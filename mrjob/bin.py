# -*- coding: utf-8 -*-
# Copyright 2009-2017 Yelp and Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Abstract base class for all runners that execute binaries/scripts
(that is, everything but inline mode).
"""
import json
import logging
import os
import pipes
import re
import sys
from subprocess import Popen
from subprocess import PIPE

import mrjob.step
from mrjob.compat import translate_jobconf
from mrjob.conf import combine_dicts
from mrjob.conf import combine_local_envs
from mrjob.py2 import PY2
from mrjob.py2 import string_types
from mrjob.runner import MRJobRunner
from mrjob.step import _is_spark_step_type
from mrjob.step import STEP_TYPES
from mrjob.util import cmd_line
from mrjob.util import shlex_split

log = logging.getLogger(__name__)

# no need to escape arguments that only include these characters
_HADOOP_SAFE_ARG_RE = re.compile(r'^[\w\./=-]*$')


class MRJobBinRunner(MRJobRunner):

    OPT_NAMES = MRJobRunner.OPT_NAMES | {
        'interpreter',
        'python_bin',
        'sh_bin',
        'spark_args',
        'spark_submit_bin',
        'steps_interpreter',
        'steps_python_bin',
        'task_python_bin',
    }

    def __init__(self, **kwargs):
        super(MRJobBinRunner, self).__init__(**kwargs)

        # we'll create the wrapper script later
        self._setup_wrapper_script_path = None

    def _default_opts(self):
        return combine_dicts(
            super(MRJobBinRunner, self)._default_opts(),
            dict(
                sh_bin=['sh', '-ex'],
            )
        )

    def _load_steps(self):
        if not self._script_path:
            return []

        args = (self._executable(True) + ['--steps'] +
                self._mr_job_extra_args(local=True))
        log.debug('> %s' % cmd_line(args))

        # add . to PYTHONPATH (in case mrjob isn't actually installed)
        env = combine_local_envs(os.environ,
                                 {'PYTHONPATH': os.path.abspath('.')})
        steps_proc = Popen(args, stdout=PIPE, stderr=PIPE, env=env)
        stdout, stderr = steps_proc.communicate()

        if steps_proc.returncode != 0:
            raise Exception(
                'error getting step information: \n%s' % stderr)

        # on Python 3, convert stdout to str so we can json.loads() it
        if not isinstance(stdout, str):
            stdout = stdout.decode('utf_8')

        try:
            steps = json.loads(stdout)
        except ValueError:
            raise ValueError("Bad --steps response: \n%s" % stdout)

        # verify that this is a proper step description
        if not steps or not stdout:
            raise ValueError('step description is empty!')
        for step in steps:
            if step['type'] not in STEP_TYPES:
                raise ValueError(
                    'unexpected step type %r in steps %r' % (
                        step['type'], stdout))

        return steps

    ### interpreter/python binary ###

    def _interpreter(self, steps=False):
        if steps:
            return (self._opts['steps_interpreter'] or
                    self._opts['interpreter'] or
                    self._steps_python_bin())
        else:
            return (self._opts['interpreter'] or
                    self._task_python_bin())

    def _executable(self, steps=False):
        if steps:
            return self._interpreter(steps=True) + [self._script_path]
        else:
            return self._interpreter() + [
                self._working_dir_mgr.name('file', self._script_path)]

    def _python_bin(self):
        """Python binary used for everything other than invoking the job.
        For invoking jobs with ``--steps``, see :py:meth:`_steps_python_bin`,
        and for everything else (e.g. ``--mapper``, ``--spark``), see
        :py:meth:`_task_python_bin`, which defaults to this method if
        :mrjob-opt:`task_python_bin` isn't set.

        Other ways mrjob uses Python:
         * file locking in setup wrapper scripts
         * finding site-packages dir to bootstrap mrjob on clusters
         * invoking ``cat.py`` in local mode
         * the Python binary for Spark (``$PYSPARK_PYTHON``)
        """
        # python_bin isn't an option for inline runners
        return self._opts['python_bin'] or self._default_python_bin()

    def _steps_python_bin(self):
        """Python binary used to invoke job with ``--steps``"""
        return (self._opts['steps_python_bin'] or
                self._default_python_bin(local=True))

    def _task_python_bin(self):
        """Python binary used to invoke job with ``--mapper``,
        ``--reducer``, ``--spark``, etc."""
        return (self._opts['task_python_bin'] or
                self._python_bin())

    def _default_python_bin(self, local=False):
        """The default python command. If local is true, try to use
        sys.executable. Otherwise use 'python' or 'python3' as appropriate.

        This returns a single-item list (because it's a command).
        """
        if local and sys.executable:
            return [sys.executable]
        elif PY2:
            return ['python']
        else:
            # e.g. python3
            return ['python%d' % sys.version_info[0]]

    ### running MRJob scripts ###

    def _script_args_for_step(self, step_num, mrc):
        args = self._executable() + self._args_for_task(step_num, mrc)

        if self._setup_wrapper_script_path:
            return (self._sh_bin() +
                    [self._working_dir_mgr.name(
                        'file', self._setup_wrapper_script_path)] +
                    args)
        else:
            return args

    def _substep_args(self, step_num, mrc):
        step = self._get_step(step_num)

        if step[mrc]['type'] == 'command':
            cmd = step[mrc]['command']

            # never wrap custom hadoop streaming commands in bash
            if isinstance(cmd, string_types):
                return shlex_split(cmd)
            else:
                return cmd

        elif step[mrc]['type'] == 'script':
            script_args = self._script_args_for_step(step_num, mrc)

            if 'pre_filter' in step[mrc]:
                return self._sh_wrap(
                    '%s | %s' % (step[mrc]['pre_filter'],
                                 cmd_line(script_args)))
            else:
                return script_args
        else:
            raise ValueError("Invalid %s step %d: %r" % (
                mrc, step_num, step[mrc]))

    ### hadoop streaming ###

    def _render_substep(self, step_num, mrc):
        step = self._get_step(step_num)

        if mrc in step:
            # cmd_line() does things that shell is fine with but
            # Hadoop Streaming finds confusing.
            return _hadoop_cmd_line(self._substep_args(step_num, mrc))
        else:
            if mrc == 'mapper':
                return 'cat'
            else:
                return None

    def _hadoop_args_for_step(self, step_num):
        """Build a list of extra arguments to the hadoop binary.

        This handles *cmdenv*, *hadoop_extra_args*, *hadoop_input_format*,
        *hadoop_output_format*, *jobconf*, and *partitioner*.

        This doesn't handle input, output, mappers, reducers, or uploading
        files.
        """
        args = []

        # -libjars, -D
        args.extend(self._hadoop_generic_args_for_step(step_num))

        # hadoop_extra_args (if defined; it's not for sim runners)
        # this has to come after -D because it may include streaming-specific
        # args (see #1332).
        args.extend(self._opts.get('hadoop_extra_args', ()))

        # partitioner
        partitioner = self._partitioner or self._sort_values_partitioner()
        if partitioner:
            args.extend(['-partitioner', partitioner])

        # cmdenv
        for key, value in sorted(self._opts['cmdenv'].items()):
            args.append('-cmdenv')
            args.append('%s=%s' % (key, value))

        # hadoop_input_format
        if (step_num == 0 and self._hadoop_input_format):
            args.extend(['-inputformat', self._hadoop_input_format])

        # hadoop_output_format
        if (step_num == self._num_steps() - 1 and self._hadoop_output_format):
            args.extend(['-outputformat', self._hadoop_output_format])

        return args

    def _hadoop_streaming_jar_args(self, step_num):
        """The arguments that come after ``hadoop jar <streaming jar path>``
        when running a Hadoop streaming job."""
        args = []

        # get command for each part of the job
        mapper, combiner, reducer = (
            self._hadoop_streaming_commands(step_num))

        # set up uploading from HDFS/cloud storage to the working dir
        args.extend(self._upload_args())

        # if no reducer, shut off reducer tasks. This has to come before
        # extra hadoop args, which could contain jar-specific args
        # (e.g. -outputformat). See #1331.
        #
        # might want to just integrate this into _hadoop_args_for_step?
        if not reducer:
            args.extend(['-D', ('%s=0' % translate_jobconf(
                'mapreduce.job.reduces', self.get_hadoop_version()))])

        # Add extra hadoop args first as hadoop args could be a hadoop
        # specific argument which must come before job
        # specific args.
        args.extend(self._hadoop_args_for_step(step_num))

        # set up input
        for input_uri in self._step_input_uris(step_num):
            args.extend(['-input', input_uri])

        # set up output
        args.append('-output')
        args.append(self._step_output_uri(step_num))

        args.append('-mapper')
        args.append(mapper)

        if combiner:
            args.append('-combiner')
            args.append(combiner)

        if reducer:
            args.append('-reducer')
            args.append(reducer)

        return args

    def _hadoop_streaming_commands(self, step_num):
        return (
            self._render_substep(step_num, 'mapper'),
            self._render_substep(step_num, 'combiner'),
            self._render_substep(step_num, 'reducer'),
        )

    def _hadoop_generic_args_for_step(self, step_num):
        """Arguments like -D and -libjars that apply to every Hadoop
        subcommand."""
        args = []

        # libjars (#198)
        libjar_paths = self._libjar_paths()
        if libjar_paths:
            args.extend(['-libjars', ','.join(libjar_paths)])

        # jobconf (-D)
        jobconf = self._jobconf_for_step(step_num)

        for key, value in sorted(jobconf.items()):
            if value is not None:
                args.extend(['-D', '%s=%s' % (key, value)])

        return args

    def _libjar_paths(self):
        """Paths or URIs of libjars, from Hadoop/Spark's point of view.

        Override this for non-local libjars (e.g. on EMR).
        """
        return self._opts['libjars']

    ### setup scripts ###

    def _create_setup_wrapper_script(
            self, dest='setup-wrapper.sh', local=False):
        """Create the wrapper script, and write it into our local temp
        directory (by default, to a file named wrapper.sh).

        This will set ``self._setup_wrapper_script_path``, and add it to
        ``self._working_dir_mgr``

        This will do nothing if ``self._setup`` is empty or
        this method has already been called.

        If *local* is true, use local line endings (e.g. Windows). Otherwise,
        use UNIX line endings (see #1071).
        """
        if self._setup_wrapper_script_path:
            return

        setup = self._setup

        if self._bootstrap_mrjob() and self._BOOTSTRAP_MRJOB_IN_SETUP:
            # patch setup to add mrjob.zip to PYTHONPATH
            mrjob_zip = self._create_mrjob_zip()
            # this is a file, not an archive, since Python can import directly
            # from .zip files
            path_dict = {'type': 'file', 'name': None, 'path': mrjob_zip}
            self._working_dir_mgr.add(**path_dict)
            setup = [['export PYTHONPATH=', path_dict, ':$PYTHONPATH']] + setup

        if not setup:
            return

        path = os.path.join(self._get_local_tmp_dir(), dest)
        log.debug('Writing wrapper script to %s' % path)

        contents = self._setup_wrapper_script_content(setup)
        for line in contents:
            log.debug('WRAPPER: ' + line.rstrip('\n'))

        if local:
            with open(path, 'w') as f:
                for line in contents:
                    f.write(line)
        else:
            with open(path, 'wb') as f:
                for line in contents:
                    f.write(line.encode('utf-8'))

        self._setup_wrapper_script_path = path
        self._working_dir_mgr.add('file', self._setup_wrapper_script_path)

    def _setup_wrapper_script_content(self, setup, mrjob_zip_name=None):
        """Return a (Bourne) shell script that runs the setup commands and then
        executes whatever is passed to it (this will be our mapper/reducer),
        as a list of strings (one for each line, including newlines).

        We obtain a file lock so that two copies of the setup commands
        cannot run simultaneously on the same machine (this helps for running
        :command:`make` on a shared source code archive, for example).
        """
        out = []

        def writeln(line=''):
            out.append(line + '\n')

        # hook for 'set -e', etc.
        pre_commands = self._sh_pre_commands()
        if pre_commands:
            for cmd in pre_commands:
                writeln(cmd)
            writeln()

        # we're always going to execute this script as an argument to
        # sh, so there's no need to add a shebang (e.g. #!/bin/sh)

        writeln('# store $PWD')
        writeln('__mrjob_PWD=$PWD')
        writeln()

        writeln('# obtain exclusive file lock')
        # Basically, we're going to tie file descriptor 9 to our lockfile,
        # use a subprocess to obtain a lock (which we somehow inherit too),
        # and then release the lock by closing the file descriptor.
        # File descriptors 10 and higher are used internally by the shell,
        # so 9 is as out-of-the-way as we can get.
        writeln('exec 9>/tmp/wrapper.lock.%s' % self._job_key)
        # would use flock(1), but it's not always available
        writeln("%s -c 'import fcntl; fcntl.flock(9, fcntl.LOCK_EX)'" %
                cmd_line(self._python_bin()))
        writeln()

        writeln('# setup commands')
        # group setup commands so we can redirect their input/output (see
        # below). Don't use parens; this would invoke a subshell, which would
        # keep us from exporting environment variables to the task.
        writeln('{')
        for cmd in setup:
            # reconstruct the command line, substituting $__mrjob_PWD/<name>
            # for path dicts
            line = '  '  # indent, since these commands are in a group
            for token in cmd:
                if isinstance(token, dict):
                    # it's a path dictionary
                    line += '$__mrjob_PWD/'
                    line += pipes.quote(self._working_dir_mgr.name(**token))
                else:
                    # it's raw script
                    line += token
            writeln(line)
        # redirect setup commands' input/output so they don't interfere
        # with the task (see Issue #803).
        writeln('} 0</dev/null 1>&2')
        writeln()

        writeln('# release exclusive file lock')
        writeln('exec 9>&-')
        writeln()

        writeln('# run task from the original working directory')
        writeln('cd $__mrjob_PWD')
        writeln('"$@"')

        return out

    def _sh_bin(self):
        """The sh binary and any arguments, as a list. Override this
        if, for example, a runner needs different default values
        depending on circumstances (see :py:class:`~mrjob.emr.EMRJobRunner`).
        """
        return self._opts['sh_bin']

    def _sh_pre_commands(self):
        """A list of lines to put at the very start of any sh script
        (e.g. ``set -e`` when ``sh -e`` wont work, see #1549)
        """
        return []

    def _sh_wrap(self, cmd_str):
        """Helper for _substep_args()

        Wrap command in sh -c '...' to allow for pipes, etc.
        Use *sh_bin* option."""
        # prepend set -e etc.
        cmd_str = '; '.join(self._sh_pre_commands() + [cmd_str])

        return self._sh_bin() + ['-c', cmd_str]

    ### spark ###

    def _args_for_spark_step(self, step_num):
        """The actual arguments used to run the spark-submit command.

        This handles both all Spark step types (``spark``, ``spark_jar``,
        and ``spark_script``).
        """
        return (
            self.get_spark_submit_bin() +
            self._spark_submit_args(step_num) +
            [self._spark_script_path(step_num)] +
            self._spark_script_args(step_num)
        )

    def _spark_script_args(self, step_num):
        """A list of args to the spark script/jar, used by
        _args_for_spark_step()."""
        step = self._get_step(step_num)

        if step['type'] == 'spark':
            args = (
                [
                    '--step-num=%d' % step_num,
                    '--spark',
                ] + self._mr_job_extra_args() + [
                    mrjob.step.INPUT,
                    mrjob.step.OUTPUT,
                ]
            )
        elif step['type'] in ('spark_jar', 'spark_script'):
            args = step['args']
        else:
            raise TypeError('Bad step type: %r' % step['type'])

        return self._interpolate_input_and_output(args, step_num)

    def get_spark_submit_bin(self):
        """The spark-submit command, as a list of args. Re-define
        this in your subclass for runner-specific behavior.
        """
        return self._opts['spark_submit_bin'] or ['spark-submit']

    def _spark_submit_arg_prefix(self):
        """Runner-specific args to spark submit (e.g. ['--master', 'yarn'])"""
        return []

    def _spark_submit_args(self, step_num):
        """Build a list of extra args to the spark-submit binary for
        the given spark or spark_script step."""
        step = self._get_step(step_num)

        if not _is_spark_step_type(step['type']):
            raise TypeError('non-Spark step: %r' % step)

        args = []

        # add runner-specific args
        args.extend(self._spark_submit_arg_prefix())

        # add --class (JAR steps)
        if step.get('main_class'):
            args.extend(['--class', step['main_class']])

        # add --jars, if any
        libjar_paths = self._libjar_paths()
        if libjar_paths:
            args.extend(['--jars', ','.join(libjar_paths)])

        # --conf arguments include python bin, cmdenv, jobconf. Make sure
        # that we can always override these manually
        jobconf = {}
        for key, value in self._spark_cmdenv(step_num).items():
            jobconf['spark.executorEnv.%s' % key] = value
            jobconf['spark.yarn.appMasterEnv.%s' % key] = value

        jobconf.update(self._jobconf_for_step(step_num))

        for key, value in sorted(jobconf.items()):
            if value is not None:
                args.extend(['--conf', '%s=%s' % (key, value)])

        # --files and --archives
        args.extend(self._spark_upload_args())

        # --py-files (Python only)
        if step['type'] in ('spark', 'spark_script'):
            py_files_arg = ','.join(self._spark_py_files())
            if py_files_arg:
                args.extend(['--py-files', py_files_arg])

        # spark_args option
        args.extend(self._opts['spark_args'])

        # step spark_args
        args.extend(step['spark_args'])

        return args

    def _spark_upload_args(self):
        return self._upload_args_helper('--files', self._spark_files,
                                        '--archives', self._spark_archives)

    def _spark_script_path(self, step_num):
        """The path of the spark script or har, used by
        _args_for_spark_step()."""
        step = self._get_step(step_num)

        if step['type'] == 'spark':
            path = self._script_path
        elif step['type'] == 'spark_jar':
            path = step['jar']
        elif step['type'] == 'spark_script':
            path = step['script']
        else:
            raise TypeError('Bad step type: %r' % step['type'])

        return self._interpolate_spark_script_path(path)

    def _interpolate_spark_script_path(self, path):
        """Redefine this in your subclass if the given path needs to be
        translated to a URI when running spark (e.g. on EMR)."""
        return path

    def _spark_cmdenv(self, step_num):
        """Returns a dictionary mapping environment variable to value,
        including mapping PYSPARK_PYTHON to self._python_bin()
        """
        step = self._get_step(step_num)

        cmdenv = {}

        if step['type'] in ('spark', 'spark_script'):  # not spark_jar
            cmdenv = dict(PYSPARK_PYTHON=cmd_line(self._python_bin()))
        cmdenv.update(self._opts['cmdenv'])
        return cmdenv

    def _spark_py_files(self):
        """The list of files to pass to spark-submit with --py-files.

        By default (cluster mode), Spark only accepts local files, so
        we pass these as-is.
        """
        py_files = []

        py_files.extend(self._opts['py_files'])

        # Spark doesn't have setup scripts; instead, we need to add
        # mrjob to py_files
        if self._bootstrap_mrjob() and self._BOOTSTRAP_MRJOB_IN_SETUP:
            py_files.append(self._create_mrjob_zip())

        return py_files


# these don't need to be methods

def _hadoop_cmd_line(args):
    """Escape args of a command line in a way that Hadoop can process
    them."""
    return ' '.join(_hadoop_escape_arg(arg) for arg in args)


def _hadoop_escape_arg(arg):
    """Escape a single command argument in a way that Hadoop can process it."""
    if _HADOOP_SAFE_ARG_RE.match(arg):
        return arg
    else:
        return "'%s'" % arg.replace("'", r"'\''")
