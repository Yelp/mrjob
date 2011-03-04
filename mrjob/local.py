# Copyright 2009-2011 Yelp and Contributors
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

"""Run an MRJob locally by forking off a bunch of processes and piping
them together. Useful for testing."""

import logging
import os
import pprint
import shutil
from subprocess import Popen, PIPE
import sys

from mrjob.conf import combine_dicts, combine_local_envs
from mrjob.parse import find_python_traceback, parse_mr_job_stderr
from mrjob.runner import MRJobRunner
from mrjob.util import cmd_line, unarchive


log = logging.getLogger('mrjob.local')


class LocalMRJobRunner(MRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` locally, for testing
    purposes.

    This is the default way of running jobs; we assume you'll spend some
    time debugging your job before you're ready to run it on EMR or
    Hadoop.

    It's rare to need to instantiate this class directly (see
    :py:meth:`~LocalMRJobRunner.__init__` for details).
    """

    alias = 'local'

    def __init__(self, **kwargs):
        """LocalMRJobRunner takes the same keyword args as :py:class:`~mrjob.runner.MRJobRunner`. However, please note:

        * *cmdenv* is combined with :py:func:`~mrjob.conf.combine_local_envs`
        * *python_bin* defaults to ``sys.executable`` (the current python interpreter)
        """
        super(LocalMRJobRunner, self).__init__(**kwargs)

        self._working_dir = None
        self._prev_outfile = None
        self._final_outfile = None
        self._counters = {}

    @classmethod
    def _default_opts(cls):
        """A dictionary giving the default value of options."""
        return combine_dicts(super(LocalMRJobRunner, cls)._default_opts(), {
            # prefer whatever interpreter we're currently using
            'python_bin': sys.executable or 'python',
        })

    @classmethod
    def _opts_combiners(cls):
        # on windows, PYTHONPATH should use ;, not :
        return combine_dicts(
            super(LocalMRJobRunner, cls)._opts_combiners(),
            {'cmdenv': combine_local_envs})

    def _run(self):
        if self._opts['bootstrap_mrjob']:
            self._add_python_archive(self._create_mrjob_tar_gz() + '#')

        self._create_wrapper_script()
        self._setup_working_dir()
        self._setup_output_dir()

        assert self._script # shouldn't be able to run if no script

        if self._opts['hadoop_extra_args']:
            log.warning('ignoring extra args to hadoop streaming: %r' %
                        (self._opts['hadoop_extra_args'],))

        wrapper_args = [self._opts['python_bin']]
        if self._wrapper_script:
            wrapper_args = [self._opts['python_bin'],
                            self._wrapper_script['name']] + wrapper_args

        # run mapper, sort, reducer for each step
        for i, step in enumerate(self._get_steps()):
            # run the mapper
            mapper_args = (wrapper_args + [self._script['name'],
                            '--step-num=%d' % i, '--mapper'] +
                           self._mr_job_extra_args())
            self._invoke_step(mapper_args, 'step-%d-mapper' % i)

            if 'R' in step:
                # sort the output
                self._invoke_step(['sort'], 'step-%d-mapper-sorted' % i,
                       env={'LC_ALL': 'C'}) # ignore locale

                # run the reducer
                reducer_args = (wrapper_args + [self._script['name'],
                                 '--step-num=%d' % i, '--reducer'] +
                                self._mr_job_extra_args())
                self._invoke_step(reducer_args, 'step-%d-reducer' % i)

        # move final output to output directory
        self._final_outfile = os.path.join(self._output_dir, 'part-00000')
        log.info('Moving %s -> %s' % (self._prev_outfile, self._final_outfile))
        shutil.move(self._prev_outfile, self._final_outfile)

    def _setup_working_dir(self):
        """Make a working directory with symlinks to our script and
        external files. Return name of the script"""
        # specify that we want to upload our script along with other files
        if self._script:
            self._script['upload'] = 'file'
        if self._wrapper_script:
            self._wrapper_script['upload'] = 'file'

        # create the working directory
        self._working_dir = os.path.join(self._get_local_tmp_dir(), 'working_dir')
        self.mkdir(self._working_dir)

        # give all our files names, and symlink or unarchive them
        self._name_files()
        for file_dict in self._files:
            path = file_dict['path']
            dest = os.path.join(self._working_dir, file_dict['name'])

            if file_dict.get('upload') == 'file':
                self._symlink_to_file_or_copy(path, dest)
            elif file_dict.get('upload') == 'archive':
                log.debug('unarchiving %s -> %s' % (path, dest))
                unarchive(path, dest)

    def _setup_output_dir(self):
        if not self._output_dir:
            self._output_dir = os.path.join(self._get_local_tmp_dir(), 'output')

        if not os.path.isdir(self._output_dir):
            log.debug('Creating output directory %s' % self._output_dir)
            self.mkdir(self._output_dir)

    def _symlink_to_file_or_copy(self, path, dest):
        """Symlink from *dest* to the absolute version of *path*.

        If symlinks aren't available, copy *path* to *dest* instead."""
        if hasattr(os, 'symlink'):
            path = os.path.abspath(path)
            log.debug('creating symlink %s <- %s' % (path, dest))
            os.symlink(path, dest)
        else:
            log.debug('copying %s -> %s' % (path, dest))
            shutil.copyfile(path, dest)

    def _stream_output(self):
        """Read output from the final outfile."""
        if self._final_outfile:
            output_file = self._final_outfile
        else:
            output_file = os.path.join(self._output_dir, 'part-00000')
        log.info('streaming final output from %s' % output_file)

        for line in open(output_file):
            yield line

    def _invoke_step(self, args, outfile_name, env=None):
        """Run the given command, outputting into outfile, and reading
        from the previous outfile (or, for the first step, from our
        original output files).

        outfile is a path relative to our local tmp dir. commands are run
        inside self._working_dir

        We'll intelligently handle stderr from the process.
        """
        # keep the current environment because we need PATH to find binaries
        # and make PYTHONPATH work
        env = combine_local_envs(
            {'PYTHONPATH': os.getcwd()},
            os.environ,
            self._get_cmdenv(),
            env or {})

        # decide where to get input
        if self._prev_outfile is not None:
            input_paths = [self._prev_outfile]
        else:
            input_paths = []
            for path in self._input_paths:
                if path == '-':
                    input_paths.append(self._dump_stdin_to_local_file())
                else:
                    input_paths.append(path)

        # add input to the command line
        for path in input_paths:
            args.append(os.path.abspath(path))

        log.info('> %s' % cmd_line(args))

        # set up outfile
        outfile = os.path.join(self._get_local_tmp_dir(), outfile_name)
        log.info('writing to %s' % outfile)
        log.debug('')

        self._prev_outfile = outfile
        write_to = open(outfile, 'w')

        # run the process
        proc = Popen(args, stdout=write_to, stderr=PIPE,
                     cwd=self._working_dir, env=env)

        # handle counters, status msgs, and other stuff on stderr
        stderr_lines = self._process_stderr_from_script(proc.stderr)
        tb_lines = find_python_traceback(stderr_lines)

        self._print_counters()

        returncode = proc.wait()
        if returncode != 0:
            # try to throw a useful exception
            if tb_lines:
                raise Exception(
                    'Command %r returned non-zero exit status %d:\n%s' %
                    (args, returncode, ''.join(tb_lines)))
            else:
                raise Exception(
                    'Command %r returned non-zero exit status %d: %s' %
                    (args, returncode))

        # flush file descriptors
        write_to.flush()

    def _process_stderr_from_script(self, stderr):
        """Handle stderr a line at time:

        - for counter lines, store counters
        - for status message, log the status change
        - for all other lines, log an error, and yield the lines
        """
        for line in stderr:
            # just pass one line at a time to parse_mr_job_stderr(),
            # so we can print error and status messages in realtime
            parsed = parse_mr_job_stderr([line], counters=self._counters)

            # in practice there's only going to be at most one line in
            # one of these lists, but the code is cleaner this way
            for status in parsed['statuses']:
                log.info('status: %s' % status)

            for line in parsed['other']:
                log.error('STDERR: %s' % line.rstrip('\n'))
                yield line

    def _print_counters(self):
        """Log the current value of counters (if any)"""
        if not self._counters:
            return

        log.info('counters: %s' % pprint.pformat(self._counters))
