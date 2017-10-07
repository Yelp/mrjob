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
"""Base class for all runners."""
import copy
import datetime
import getpass
import json
import logging
import os
import os.path
import pipes
import posixpath
import pprint
import re
import shutil
import sys
import tarfile
import tempfile
from inspect import isfunction
from inspect import ismethod
from subprocess import CalledProcessError
from subprocess import Popen
from subprocess import PIPE
from subprocess import check_call

import mrjob.step
from mrjob.compat import translate_jobconf
from mrjob.compat import translate_jobconf_dict
from mrjob.compat import translate_jobconf_for_all_versions
from mrjob.conf import combine_dicts
from mrjob.conf import combine_local_envs
from mrjob.conf import load_opts_from_mrjob_confs
from mrjob.conf import OptionStore
from mrjob.fs.composite import CompositeFilesystem
from mrjob.fs.local import LocalFilesystem
from mrjob.options import _allowed_keys
from mrjob.options import _combiners
from mrjob.options import _deprecated_aliases
from mrjob.options import CLEANUP_CHOICES
from mrjob.options import _CLEANUP_DEPRECATED_ALIASES
from mrjob.parse import is_uri
from mrjob.py2 import PY2
from mrjob.py2 import string_types
from mrjob.setup import WorkingDirManager
from mrjob.setup import name_uniquely
from mrjob.setup import parse_legacy_hash_path
from mrjob.setup import parse_setup_cmd
from mrjob.step import STEP_TYPES
from mrjob.step import _is_spark_step_type
from mrjob.util import cmd_line
from mrjob.util import zip_dir


log = logging.getLogger(__name__)

# use to detect globs and break into the part before and after the glob
GLOB_RE = re.compile(r'^(.*?)([\[\*\?].*)$')

# buffer for piping files into sort on Windows
_BUFFER_SIZE = 4096

# jobconf options for implementing SORT_VALUES
_SORT_VALUES_JOBCONF = {
    'mapreduce.partition.keypartitioner.options': '-k1,1',
    'stream.num.map.output.key.fields': 2
}

# partitioner for sort_values
_SORT_VALUES_PARTITIONER = \
    'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner'


class RunnerOptionStore(OptionStore):
    # 'base' is aritrary; if an option support all runners, it won't
    # have "runners" set in _RUNNER_OPTS at all
    ALLOWED_KEYS = _allowed_keys('base')
    COMBINERS = _combiners('base')
    DEPRECATED_ALIASES = _deprecated_aliases('base')

    def __init__(self, alias, opts, conf_paths):
        """
        :param alias: Runner alias (e.g. ``'local'``)
        :param opts: Keyword args to runner's constructor (usually from the
                     command line).
        :param conf_paths: An iterable of paths to config files
        """
        super(RunnerOptionStore, self).__init__()

        # sanitize incoming options and issue warnings for bad keys
        opts = self.validated_options(opts)

        unsanitized_opt_dicts = load_opts_from_mrjob_confs(
            alias, conf_paths=conf_paths)

        for path, mrjob_conf_opts in unsanitized_opt_dicts:
            self.cascading_dicts.append(self.validated_options(
                mrjob_conf_opts, from_where=(' from %s' % path)))

        self.cascading_dicts.append(opts)

        if (len(self.cascading_dicts) > 2 and
                all(len(d) == 0 for d in self.cascading_dicts[2:-1]) and
                (len(conf_paths or []) > 0)):
            log.warning('No configs specified for %s runner' % alias)

        self.populate_values_from_cascading_dicts()

        log.debug('Active configuration:')
        log.debug(pprint.pformat(
            dict((opt_key, self._obfuscate(opt_key, opt_value))
                 for opt_key, opt_value in self.items())))

    def default_options(self):
        super_opts = super(RunnerOptionStore, self).default_options()

        try:
            owner = getpass.getuser()
        except:
            owner = None

        return combine_dicts(super_opts, {
            'check_input_paths': True,
            'cleanup': ['ALL'],
            'cleanup_on_failure': ['NONE'],
            'local_tmp_dir': tempfile.gettempdir(),
            'owner': owner,
            'sh_bin': ['sh', '-ex'],
            'strict_protocols': True,
        })

    def validated_options(self, opts, from_where=''):
        opts = super(RunnerOptionStore, self).validated_options(
            opts, from_where)

        self._fix_cleanup_opt('cleanup', opts, from_where)
        self._fix_cleanup_opt('cleanup_on_failure', opts, from_where)

        return opts

    def _fix_cleanup_opt(self, opt_key, opts, from_where=''):
        if opts.get(opt_key) is None:
            return

        opt_list = opts[opt_key]

        # runner expects list of string, not string
        if isinstance(opt_list, string_types):
            opt_list = [opt_list]

        if 'NONE' in opt_list and len(set(opt_list)) > 1:
            raise ValueError('Cannot clean up both nothing and something!')

        def handle_cleanup_opt(opt):
            if opt in CLEANUP_CHOICES:
                return opt

            if opt in _CLEANUP_DEPRECATED_ALIASES:
                aliased_opt = _CLEANUP_DEPRECATED_ALIASES[opt]
                # TODO: don't do this when option value is None
                log.warning(
                    'Deprecated %s option %s%s has been renamed to %s' % (
                        opt_key, opt, from_where, aliased_opt))
                return aliased_opt

            raise ValueError('%s must be one of %s, not %s' % (
                opt_key, ', '.join(CLEANUP_CHOICES), opt))

        opt_list = [handle_cleanup_opt(opt) for opt in opt_list]

        opts[opt_key] = opt_list

    def _obfuscate(self, opt_key, opt_value):
        """Return value of opt to show in debug printout. Used to obfuscate
        credentials, etc."""
        return opt_value


class MRJobRunner(object):
    """Abstract base class for all runners"""

    #: alias for this runner; used for picking section of
    #: :py:mod:``mrjob.conf`` to load one of ``'local'``, ``'emr'``,
    #: or ``'hadoop'``
    alias = None

    # if this is true, when bootstrap_mrjob is true, add it through the
    # setup script
    BOOTSTRAP_MRJOB_IN_SETUP = True

    OPTION_STORE_CLASS = RunnerOptionStore

    ### methods to call from your batch script ###

    def __init__(self, mr_job_script=None, conf_paths=None,
                 extra_args=None, file_upload_args=None,
                 hadoop_input_format=None, hadoop_output_format=None,
                 input_paths=None, output_dir=None, partitioner=None,
                 sort_values=None, stdin=None, step_output_dir=None,
                 **opts):
        """All runners take the following keyword arguments:

        :type mr_job_script: str
        :param mr_job_script: the path of the ``.py`` file containing the
                              :py:class:`~mrjob.job.MRJob`. If this is None,
                              you won't actually be able to :py:meth:`run` the
                              job, but other utilities (e.g. :py:meth:`ls`)
                              will work.
        :type conf_paths: None or list
        :param conf_paths: List of config files to combine and use, or None to
                           search for mrjob.conf in the default locations.
        :type extra_args: list of str
        :param extra_args: a list of extra cmd-line arguments to pass to the
                           mr_job script. This is a hook to allow jobs to take
                           additional arguments.
        :param file_upload_args: a list of tuples of ``('--ARGNAME', path)``.
                                 The file at the given path will be uploaded
                                 to the local directory of the mr_job script
                                 when it runs, and then passed into the script
                                 with ``--ARGNAME``. Useful for passing in
                                 SQLite DBs and other configuration files to
                                 your job.
        :type hadoop_input_format: str
        :param hadoop_input_format: name of an optional Hadoop ``InputFormat``
                                    class. Passed to Hadoop along with your
                                    first step with the ``-inputformat``
                                    option. Note that if you write your own
                                    class, you'll need to include it in your
                                    own custom streaming jar (see
                                    :mrjob-opt:`hadoop_streaming_jar`).
        :type hadoop_output_format: str
        :param hadoop_output_format: name of an optional Hadoop
                                     ``OutputFormat`` class. Passed to Hadoop
                                     along with your first step with the
                                     ``-outputformat`` option. Note that if you
                                     write your own class, you'll need to
                                     include it in your own custom streaming
                                     jar (see
                                     :mrjob-opt:`hadoop_streaming_jar`).
        :type input_paths: list of str
        :param input_paths: Input files for your job. Supports globs and
                            recursively walks directories (e.g.
                            ``['data/common/', 'data/training/*.gz']``). If
                            this is left blank, we'll read from stdin
        :type output_dir: str
        :param output_dir: An empty/non-existent directory where Hadoop
                           should put the final output from the job.
                           If you don't specify an output directory, we'll
                           output into a subdirectory of this job's temporary
                           directory. You can control this from the command
                           line with ``--output-dir``. This option cannot be
                           set from configuration files. If used with the
                           hadoop runner, this path does not need to be fully
                           qualified with ``hdfs://`` URIs because it's
                           understood that it has to be on HDFS.
        :type partitioner: str
        :param partitioner: Optional name of a Hadoop partitioner class, e.g.
                            ``'org.apache.hadoop.mapred.lib.HashPartitioner'``.
                            Hadoop streaming will use this to determine how
                            mapper output should be sorted and distributed
                            to reducers.
        :type sort_values: bool
        :param sort_values: if true, set partitioners and jobconf variables
                            so that reducers to receive the values
                            associated with any key in sorted order (sorted by
                            their *encoded* value). Also known as secondary
                            sort.
        :param stdin: an iterable (can be a ``BytesIO`` or even a list) to use
                      as stdin. This is a hook for testing; if you set
                      ``stdin`` via :py:meth:`~mrjob.job.MRJob.sandbox`, it'll
                      get passed through to the runner. If for some reason
                      your lines are missing newlines, we'll add them;
                      this makes it easier to write automated tests.
        :type step_output_dir: str
        :param step_output_dir: An empty/non-existent directory where Hadoop
                                should put output from all steps other than
                                the last one (this only matters for multi-step
                                jobs). Currently ignored by local runners.
        """
        self._ran_job = False

        self._opts = self.OPTION_STORE_CLASS(self.alias, opts, conf_paths)
        self._fs = None

        # a local tmp directory that will be cleaned up when we're done
        # access/make this using self._get_local_tmp_dir()
        self._local_tmp_dir = None

        self._working_dir_mgr = WorkingDirManager()

        # mapping from dir to path for corresponding archive. we pick
        # paths during init(), but don't actually create the archives
        # until self._create_dir_archives() is called
        self._dir_to_archive_path = {}
        # dir archive names (the filename minus ".tar.gz") already taken
        self._dir_archive_names_taken = set()
        # set of dir_archives that have actually been created
        self._dir_archives_created = set()

        # track (name, path) of files and archives to upload to spark.
        # these are a subset of those in self._working_dir_mgr
        self._spark_files = []
        self._spark_archives = []

        self._upload_mgr = None  # define in subclasses that use this

        self._script_path = mr_job_script
        if self._script_path:
            self._working_dir_mgr.add('file', self._script_path)

        # give this job a unique name
        self._job_key = self._make_unique_job_key(
            label=self._opts['label'], owner=self._opts['owner'])

        # we'll create the wrapper script later
        self._setup_wrapper_script_path = None

        # extra args to our job
        self._extra_args = list(extra_args) if extra_args else []

        # extra file arguments to our job
        self._file_upload_args = []
        if file_upload_args:
            for arg, path in file_upload_args:
                arg_file = parse_legacy_hash_path('file', path)
                self._working_dir_mgr.add(**arg_file)
                self._file_upload_args.append((arg, arg_file))
                self._spark_files.append((arg_file['name'], arg_file['path']))

        # set up uploading
        for hash_path in self._opts['upload_files']:
            uf = parse_legacy_hash_path('file', hash_path,
                                        must_name='upload_files')
            self._working_dir_mgr.add(**uf)
            self._spark_files.append((uf['name'], uf['path']))

        for hash_path in self._opts['upload_archives']:
            ua = parse_legacy_hash_path('archive', hash_path,
                                        must_name='upload_archives')
            self._working_dir_mgr.add(**ua)
            self._spark_archives.append((ua['name'], ua['path']))

        for hash_path in self._opts['upload_dirs']:
            # pick name based on directory path
            ud = parse_legacy_hash_path('dir', hash_path,
                                        must_name='upload_archives')
            # but feed working_dir_mgr the archive's path
            archive_path = self._dir_archive_path(ud['path'])
            self._working_dir_mgr.add(
                'archive', archive_path, name=ud['name'])
            self._spark_archives.append((ud['name'], archive_path))

        # py_files, python_archives, setup, setup_cmds, and setup_scripts
        # self._setup is a list of shell commands with path dicts
        # interleaved; see mrjob.setup.parse_setup_cmds() for details
        self._setup = self._parse_setup()
        for cmd in self._setup:
            for token in cmd:
                if isinstance(token, dict):
                    # convert dir archives tokens to archives
                    if token['type'] == 'dir':
                        # feed the archive's path to self._working_dir_mgr
                        token['path'] = self._dir_archive_path(token['path'])
                        token['type'] = 'archive'

                    self._working_dir_mgr.add(**token)

        # Where to read input from (log files, etc.)
        self._input_paths = input_paths or ['-']  # by default read from stdin
        if PY2:
            self._stdin = stdin or sys.stdin
        else:
            self._stdin = stdin or sys.stdin.buffer
        self._stdin_path = None  # temp file containing dump from stdin

        # where a zip file of the mrjob library is stored locally
        self._mrjob_zip_path = None

        # store output_dir
        self._output_dir = output_dir

        # store partitioner
        self._partitioner = partitioner

        # store sort_values
        self._sort_values = sort_values

        # store step_output_dir
        self._step_output_dir = step_output_dir

        # store hadoop input and output formats
        self._hadoop_input_format = hadoop_input_format
        self._hadoop_output_format = hadoop_output_format

        # A cache for self._get_steps(); also useful as a test hook
        self._steps = None

        # if this is True, we have to pipe input into the sort command
        # rather than feed it multiple files
        self._sort_is_windows_sort = None

        # this variable marks whether a cleanup has happened and this runner's
        # output stream is no longer available.
        self._closed = False

    ### Filesystem object ###

    @property
    def fs(self):
        """:py:class:`~mrjob.fs.base.Filesystem` object for the local
        filesystem. Methods on :py:class:`~mrjob.fs.base.Filesystem` objects
        will be forwarded to :py:class:`~mrjob.runner.MRJobRunner` until mrjob
        0.6.0, but **this behavior is deprecated.**
        """
        if self._fs is None:
            # wrap LocalFilesystem in CompositeFilesystem to get IOError
            # on URIs (see #1185)
            self._fs = CompositeFilesystem(LocalFilesystem())
        return self._fs

    def __getattr__(self, name):
        # For backward compatibility, forward filesystem methods
        try:
            value = getattr(self.fs, name)
        except AttributeError:
            raise AttributeError(name)

        # friendly deprecation warning
        is_func = ismethod(value) or isfunction(value)
        log.warning(
            'deprecated: %s %s.fs.%s%s directly'
            ' (%s.%s is going away in v0.6.0)' % (
                'call' if is_func else 'access',
                self.__class__.__name__,
                name,
                '()' if is_func else '',
                self.__class__.__name__,
                name))

        return value

    ### Running the job and parsing output ###

    def run(self):
        """Run the job, and block until it finishes.

        Raise :py:class:`~mrjob.step.StepFailedException` if there
        are any problems (except on
        :py:class:`~mrjob.inline.InlineMRJobRunner`, where we raise the
        actual exception that caused the step to fail).
        """
        if not self._script_path:
            raise AssertionError("No script to run!")

        if self._ran_job:
            raise AssertionError("Job already ran!")

        if not self._opts['strict_protocols']:
            log.warning('\nNon-strict protocols are deprecated and will be'
                        ' removed in v0.6.0. Please run your job with'
                        ' --strict-protocols and fix any underlying'
                        ' encoding issues\n')

        self._create_dir_archives()
        self._run()
        self._ran_job = True

    def stream_output(self):
        """Stream raw lines from the job's output. You can parse these
        using the read() method of the appropriate HadoopStreamingProtocol
        class."""
        output_dir = self.get_output_dir()
        if output_dir is None:
            raise AssertionError('Run the job before streaming output')

        if self._closed is True:
            log.warning(
                'WARNING! Trying to stream output from a closed runner, output'
                ' will probably be empty.')

        log.info('Streaming final output from %s...' % output_dir)

        def split_path(path):
            while True:
                base, name = os.path.split(path)

                # no more elements
                if not name:
                    break

                yield name

                path = base

        # TODO - mtai @ davidmarin - why aren't we using self.fs.cat ?
        for filename in self.fs.ls(output_dir):
            subpath = filename[len(output_dir):]
            if not any(name.startswith('_') for name in split_path(subpath)):
                for line in self.fs._cat_file(filename):
                    yield line

    def _cleanup_mode(self, mode=None):
        """Actual cleanup action to take based on various options"""
        if self._script_path and not self._ran_job:
            return mode or self._opts['cleanup_on_failure']
        else:
            return mode or self._opts['cleanup']

    def _cleanup_cloud_tmp(self):
        """Cleanup any files/directories on cloud storage (e.g. S3) we created
        while running this job. Should be safe to run this at any time, or
        multiple times.
        """
        pass  # only EMR runner does this

    def _cleanup_hadoop_tmp(self):
        """Cleanup any files/directories on HDFS we created
        while running this job. Should be safe to run this at any time, or
        multiple times.
        """
        pass  # only Hadoop runner does this

    def _cleanup_local_tmp(self):
        """Cleanup any files/directories on the local machine we created while
        running this job. Should be safe to run this at any time, or multiple
        times.

        This particular function removes any local tmp directories
        added to the list self._local_tmp_dirs

        This won't remove output_dir if it's outside of our tmp dir.
        """
        if self._local_tmp_dir:
            log.info('Removing temp directory %s...' % self._local_tmp_dir)
            try:
                shutil.rmtree(self._local_tmp_dir)
            except OSError as e:
                log.exception(e)

        self._local_tmp_dir = None

    def _cleanup_cluster(self):
        """Terminate the cluster if there is one."""
        pass  # this only happens on EMR

    def _cleanup_logs(self):
        """Cleanup any log files that are created as a side-effect of the job.
        """
        pass  # this only happens on EMR

    def _cleanup_job(self):
        """Stop any jobs that we created that are still running."""
        pass  # currently disabled (see #1241)

    def cleanup(self, mode=None):
        """Clean up running jobs, temp files, and logs, subject to the
        *cleanup* option passed to the constructor.

        If you create your runner in a :keyword:`with` block,
        :py:meth:`cleanup` will be called automatically::

            with mr_job.make_runner() as runner:
                ...

            # cleanup() called automatically here

        :param mode: override *cleanup* passed into the constructor. Should be
                     a list of strings from :py:data:`CLEANUP_CHOICES`
        """
        mode = self._cleanup_mode(mode)

        def mode_has(*args):
            return any((choice in mode) for choice in args)

        if self._script_path and not self._ran_job:
            if mode_has('CLUSTER', 'ALL'):
                self._cleanup_cluster()

            if mode_has('JOB', 'ALL'):
                self._cleanup_job()

        if mode_has('ALL', 'TMP', 'CLOUD_TMP'):
            self._cleanup_cloud_tmp()

        if mode_has('ALL', 'TMP', 'HADOOP_TMP'):
            self._cleanup_hadoop_tmp()

        if mode_has('ALL', 'TMP', 'LOCAL_TMP'):
            self._cleanup_local_tmp()

        if mode_has('ALL', 'LOGS'):
            self._cleanup_logs()

        self._closed = True

    def counters(self):
        """Get counters associated with this run in this form::

            [{'group name': {'counter1': 1, 'counter2': 2}},
             {'group name': ...}]

        The list contains an entry for every step of the current job.
        """
        raise NotImplementedError

    ### hooks for the with statement ###

    def __enter__(self):
        """Don't do anything special at start of with block"""
        return self

    def __exit__(self, type, value, traceback):
        """Call self.cleanup() at end of with block."""
        self.cleanup()

    ### more runner information ###

    def get_opts(self):
        """Get options set for this runner, as a dict."""
        return copy.deepcopy(self._opts)

    def get_job_key(self):
        """Get the unique key for the job run by this runner.
        This has the format ``label.owner.date.time.microseconds``
        """
        return self._job_key

    def get_job_name(self):
        """Alias for :py:meth:`get_job_key`. Will be removed in v0.6.0.

        .. deprecated:: 0.5.0
        """
        log.warning('get_job_name() has been renamed to get_job_key().'
                    ' get_job_name() will be removed in v0.6.0')
        return self.get_job_key()

    def get_output_dir(self):
        """Find the directory containing the job output. If the job hasn't
        run yet, returns None"""
        if self._script_path and not self._ran_job:
            return None

        return self._output_dir

    ### other methods you need to implement in your subclass ###

    def get_hadoop_version(self):
        """Return the version number of the Hadoop environment as a string if
        Hadoop is being used or simulated. Return None if not applicable.

        :py:class:`~mrjob.emr.EMRJobRunner` infers this from the cluster.
        :py:class:`~mrjob.hadoop.HadoopJobRunner` gets this from
        ``hadoop version``. :py:class:`~mrjob.local.LocalMRJobRunner` has an
        additional `hadoop_version` option to specify which version it
        simulates.
        :py:class:`~mrjob.inline.InlineMRJobRunner` does not simulate Hadoop at
        all.
        """
        return None

    # you'll probably wan't to add your own __init__() and cleanup() as well

    def _run(self):
        """Run the job."""
        raise NotImplementedError

    ### internal utilities for implementing MRJobRunners ###

    def _get_local_tmp_dir(self):
        """Create a tmp directory on the local filesystem that will be
        cleaned up by self.cleanup()"""
        if not self._local_tmp_dir:
            path = os.path.join(self._opts['local_tmp_dir'], self._job_key)
            log.info('Creating temp directory %s' % path)
            if os.path.isdir(path):
                shutil.rmtree(path)
            os.makedirs(path)
            self._local_tmp_dir = path

        return self._local_tmp_dir

    def _make_unique_job_key(self, label=None, owner=None):
        """Come up with a useful unique ID for this job.

        We use this to choose the output directory, etc. for the job.
        """
        # use the name of the script if one wasn't explicitly
        # specified
        if not label:
            if self._script_path:
                label = os.path.basename(self._script_path).split('.')[0]
            else:
                label = 'no_script'

        if not owner:
            owner = 'no_user'

        now = datetime.datetime.utcnow()
        return '%s.%s.%s.%06d' % (
            label, owner,
            now.strftime('%Y%m%d.%H%M%S'), now.microsecond)

    def _get_steps(self):
        """Call the job script to find out how many steps it has, and whether
        there are mappers and reducers for each step. Validate its
        output.

        Returns output as described in :ref:`steps-format`.

        Results are cached, so call this as many times as you want.
        """
        if self._steps is None:
            if not self._script_path:
                self._steps = []
            else:
                args = (self._executable(True) + ['--steps'] +
                        self._mr_job_extra_args(local=True))
                log.debug('> %s' % cmd_line(args))
                # add . to PYTHONPATH (in case mrjob isn't actually installed)
                env = _fix_env(combine_local_envs(
                    os.environ, {'PYTHONPATH': os.path.abspath('.')}))
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

                self._steps = steps

        return self._steps

    def _get_step(self, step_num):
        """Get a single step (calls :py:meth:`_get_steps`)."""
        return self._get_steps()[step_num]

    def _num_steps(self):
        """Get the number of steps (calls :py:meth:`get_steps`)."""
        return len(self._get_steps())

    def _has_streaming_steps(self):
        """Are any of our steps Hadoop streaming steps?"""
        return any(step['type'] == 'streaming'
                   for step in self._get_steps())

    def _has_spark_steps(self):
        """Are any of our steps Spark steps (either spark or spark_script)"""
        return any(_is_spark_step_type(step['type'])
                   for step in self._get_steps())

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

    def _script_args_for_step(self, step_num, mrc):
        assert self._script_path

        args = self._executable() + [
            '--step-num=%d' % step_num,
            '--%s' % mrc,
        ] + self._mr_job_extra_args()

        if self._setup_wrapper_script_path:
            return (self._sh_bin() +
                    [self._working_dir_mgr.name(
                        'file', self._setup_wrapper_script_path)] +
                    args)
        else:
            return args

    def _substep_cmd_line(self, step_num, mrc):
        step = self._get_step(step_num)

        if step[mrc]['type'] == 'command':
            # never wrap custom hadoop streaming commands in bash
            return step[mrc]['command']

        elif step[mrc]['type'] == 'script':
            cmd_str = cmd_line(self._script_args_for_step(step_num, mrc))

            # filter input and pipe for great speed, if user asks
            # but we have to wrap the command in sh -c
            if 'pre_filter' in step[mrc]:
                return self._sh_wrap(
                    '%s | %s' % (step[mrc]['pre_filter'], cmd_str))
            else:
                return cmd_str
        else:
            raise ValueError("Invalid %s step %d: %r" % (
                mrc, step_num, step[mrc]))

    def _render_substep(self, step_num, mrc):
        step = self._get_step(step_num)

        if mrc in step:
            return self._substep_cmd_line(step_num, mrc)
        else:
            if mrc == 'mapper':
                return 'cat'
            else:
                return None

    def _hadoop_streaming_commands(self, step_num):
        return (
            self._render_substep(step_num, 'mapper'),
            self._render_substep(step_num, 'combiner'),
            self._render_substep(step_num, 'reducer'),
        )

    def _sh_wrap(self, cmd_str):
        """Wrap command in sh -c '...' to allow for pipes, etc.
        Use *sh_bin* option."""
        # prepend set -e etc.
        cmd_str = '; '.join(self._sh_pre_commands() + [cmd_str])

        return "%s -c '%s'" % (
            cmd_line(self._sh_bin()),
            cmd_str.replace("'", "'\\''"))

    def _mr_job_extra_args(self, local=False):
        """Return arguments to add to every invocation of MRJob.

        :type local: boolean
        :param local: if this is True, use files' local paths rather than
            the path they'll have inside Hadoop streaming
        """
        return (self._get_file_upload_args(local=local) +
                self._get_strict_protocols_args() +
                self._extra_args)

    def _get_file_upload_args(self, local=False):
        """Arguments used to pass through config files, etc from the job
        runner through to the local directory where the script is run.

        :type local: boolean
        :param local: if this is True, use files' local paths rather than
            the path they'll have inside Hadoop streaming
        """
        args = []
        for arg, path_dict in self._file_upload_args:
            args.append(arg)
            if local:
                args.append(path_dict['path'])
            else:
                args.append(self._working_dir_mgr.name(**path_dict))
        return args

    def _get_strict_protocols_args(self):
        """Arguments used to control protocol behavior in the job.

        This just adds --no-strict-protocols when strict_protocols
        is false.
        """
        # These are only in the runner so that we can default them from
        # mrjob.conf, which will allow us to eventually remove them.
        # See issue #726.
        if not self._opts['strict_protocols']:
            return ['--no-strict-protocols']
        else:
            return []

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

        if self._bootstrap_mrjob() and self.BOOTSTRAP_MRJOB_IN_SETUP:
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

    def _parse_setup(self):
        """Parse the *setup* option with
        :py:func:`mrjob.setup.parse_setup_cmd()`.

        If *bootstrap_mrjob* and ``self.BOOTSTRAP_MRJOB_IN_SETUP`` are both
        true, create mrjob.zip (if it doesn't exist already) and
        prepend a setup command that adds it to PYTHONPATH.

        Patch in *py_files*.

        Also patch in the deprecated
        options *python_archives*, *setup_cmd*, and *setup_script*
        as setup commands.
        """
        setup = []

        # py_files
        for path in self._opts['py_files']:
            # Spark (at least v1.3.1) doesn't work with # and --py-files,
            # see #1375
            if '#' in path:
                raise ValueError("py_files cannot contain '#'")
            path_dict = parse_legacy_hash_path('file', path)
            setup.append(['export PYTHONPATH=', path_dict, ':$PYTHONPATH'])

        # python_archives
        if self._opts['python_archives']:
            log.warning('python_archives is deprecated and will be removed'
                        ' in v0.6.0. Try py_files instead')
            for path in self._opts['python_archives']:
                path_dict = parse_legacy_hash_path('archive', path)
                setup.append(['export PYTHONPATH=', path_dict, ':$PYTHONPATH'])

        # setup
        for cmd in self._opts['setup']:
            setup.append(parse_setup_cmd(cmd))

        # setup_cmds
        if self._opts['setup_cmds']:
            log.warning(
                "setup_cmds is deprecated since v0.4.2 and will be removed"
                " in v0.6.0. Consider using setup instead.")

        for cmd in self._opts['setup_cmds']:
            if not isinstance(cmd, string_types):
                cmd = cmd_line(cmd)
            setup.append([cmd])

        # setup_scripts
        if self._opts['setup_scripts']:
            log.warning(
                "setup_scripts is deprecated since v0.4.2 and will be removed"
                " in v0.6.0. Consider using setup instead.")

        for path in self._opts['setup_scripts']:
            path_dict = parse_legacy_hash_path('file', path)
            setup.append([path_dict])

        return setup

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

    def _dir_archive_path(self, dir_path):
        """Assign a path for the archive of *dir_path* but don't
        actually create anything."""
        if dir_path not in self._dir_to_archive_path:
            # we can check local paths now
            if not (is_uri(dir_path) or os.path.isdir(dir_path)):
                raise OSError('%s is not a directory!' % dir_path)

            name = name_uniquely(
                dir_path, names_taken=self._dir_archive_names_taken)
            self._dir_archive_names_taken.add(name)

            self._dir_to_archive_path[dir_path] = os.path.join(
                self._get_local_tmp_dir(), 'archives', name + '.tar.gz')

        return self._dir_to_archive_path[dir_path]

    def _create_dir_archives(self):
        """Call this to create all dir archives"""
        for dir_path in sorted(set(self._dir_to_archive_path)):
            self._create_dir_archive(dir_path)

    def _create_dir_archive(self, dir_path):
        """Helper for :py:meth:`archive_dir`"""
        if not self.fs.exists(dir_path):
            raise OSError('%s does not exist')

        tar_gz_path = self._dir_archive_path(dir_path)

        if tar_gz_path in self._dir_archives_created:
            return  # already created

        if not os.path.isdir(os.path.dirname(tar_gz_path)):
            os.makedirs(os.path.dirname(tar_gz_path))

        log.info('Archiving %s -> %s' % (dir_path, tar_gz_path))

        tar_gz = tarfile.open(tar_gz_path, mode='w:gz')

        # for remote files
        tmp_download_path = os.path.join(
            self._get_local_tmp_dir(), 'tmp-download')

        try:
            for path in self.fs.ls(dir_path):
                # fs.ls() only lists files
                if path == dir_path:
                    raise OSError('%s is a file, not a directory!' % dir_path)

                # TODO: do we need this?
                if os.path.realpath(path) == os.path.realpath(tar_gz_path):
                    raise OSError(
                        'attempted to archive %s into itself!' % tar_gz_path)

                if is_uri(path):
                    path_in_tar_gz = path[len(dir_path):].lstrip('/')

                    log.info('  downloading %s -> %s' % (
                        path, tmp_download_path))
                    with open(tmp_download_path, 'wb') as f:
                        for chunk in self.fs.cat(path):
                            f.write(chunk)
                    local_path = tmp_download_path
                else:
                    path_in_tar_gz = path[len(dir_path):].lstrip(os.sep)
                    local_path = path

                log.debug('  adding %s to %s' % (path, tar_gz_path))
                tar_gz.add(local_path, path_in_tar_gz, recursive=False)
        finally:
            tar_gz.close()

        self._dir_archives_created.add(tar_gz_path)

    def _bootstrap_mrjob(self):
        """Should we bootstrap mrjob?"""
        if self._opts['bootstrap_mrjob'] is None:
            return self._opts['interpreter'] is None
        else:
            return bool(self._opts['bootstrap_mrjob'])

    def _get_input_paths(self):
        """Get the paths to input files, dumping STDIN to a local
        file if need be."""
        if '-' in self._input_paths:
            if self._stdin_path is None:
                # prompt user, so they don't think the process has stalled
                log.info('reading from STDIN')

                stdin_path = os.path.join(self._get_local_tmp_dir(), 'STDIN')
                log.debug('dumping stdin to local file %s' % stdin_path)
                with open(stdin_path, 'wb') as stdin_file:
                    for line in self._stdin:
                        # catch missing newlines (often happens with test data)
                        if not line.endswith(b'\n'):
                            line += b'\n'
                        stdin_file.write(line)

                self._stdin_path = stdin_path

        return [self._stdin_path if p == '-' else p for p in self._input_paths]

    def _intermediate_output_uri(self, step_num):
        """A URI for intermediate output for the given step number."""
        # TODO: if we enable this for local runners, use os.path.join()
        # for them.
        return posixpath.join(
            self._step_output_dir or self._default_step_output_dir(),
            '%04d' % step_num)

    def _default_step_output_dir(self):
        """Where to put output for steps other than the last one,
        if not specified by the *output_dir* constructor keyword.
        Usually you want this to be on HDFS (most efficient).

        Define this in your runner subclass.
        """
        raise NotImplementedError

    def _step_input_uris(self, step_num):
        """A list of URIs to use as input for the given step. For all
        except the first step, this list will have a single item (a
        directory)."""
        if step_num == 0:
            return [self._upload_mgr.uri(path)
                    for path in self._get_input_paths()]
        else:
            return [self._intermediate_output_uri(step_num - 1)]

    def _step_output_uri(self, step_num):
        """URI to use as output for the given step. This is either an
        intermediate dir (see :py:meth:`intermediate_output_uri`) or
        ``self._output_dir`` for the final step."""
        if step_num == len(self._get_steps()) - 1:
            return self._output_dir
        else:
            return self._intermediate_output_uri(step_num)

    def _interpolate_input_and_output(self, args, step_num):
        """Replace :py:data:`~mrjob.step.INPUT` and
        :py:data:`~mrjob.step.OUTPUT` in arguments to a jar or Spark
        step.

        If there are multiple input paths (i.e. on the first step), they'll
        be joined with a comma.
        """

        def interpolate(arg):
            if arg == mrjob.step.INPUT:
                return ','.join(self._step_input_uris(step_num))
            elif arg == mrjob.step.OUTPUT:
                return self._step_output_uri(step_num)
            else:
                return arg

        return [interpolate(arg) for arg in args]

    def _create_mrjob_zip(self):
        """Make a zip of the mrjob library, without .pyc or .pyo files,
        This will also set ``self._mrjob_zip_path`` and return it.

        Typically called from
        :py:meth:`_create_setup_wrapper_script`.

        It's safe to call this method multiple times (we'll only create
        the zip file once.)
        """
        if not self._mrjob_zip_path:
            # find mrjob library
            import mrjob

            if not os.path.basename(mrjob.__file__).startswith('__init__.'):
                raise Exception(
                    "Bad path for mrjob library: %s; can't bootstrap mrjob",
                    mrjob.__file__)

            mrjob_dir = os.path.dirname(mrjob.__file__) or '.'

            zip_path = os.path.join(self._get_local_tmp_dir(), 'mrjob.zip')

            def filter_path(path):
                filename = os.path.basename(path)
                return not(filename.lower().endswith('.pyc') or
                           filename.lower().endswith('.pyo') or
                           # filter out emacs backup files
                           filename.endswith('~') or
                           # filter out emacs lock files
                           filename.startswith('.#') or
                           # filter out MacFuse resource forks
                           filename.startswith('._'))

            log.debug('archiving %s -> %s as %s' % (
                mrjob_dir, zip_path, os.path.join('mrjob', '')))
            zip_dir(mrjob_dir, zip_path, filter=filter_path, prefix='mrjob')

            self._mrjob_zip_path = zip_path

        return self._mrjob_zip_path

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

    def _jobconf_for_step(self, step_num):
        """Get the jobconf dictionary, optionally including step-specific
        jobconf info.

        Also translate jobconfs to the current Hadoop version, if necessary.
        """

        step = self._get_step(step_num)

        # _sort_values_jobconf() isn't relevant to Spark,
        # but it doesn't do any harm either

        jobconf = combine_dicts(self._sort_values_jobconf(),
                                self._opts['jobconf'],
                                step.get('jobconf'))

        # if user is using the wrong jobconfs, add in the correct ones
        # and log a warning
        hadoop_version = self.get_hadoop_version()
        if hadoop_version:
            jobconf = translate_jobconf_dict(jobconf, hadoop_version)

        return jobconf

    def _sort_values_jobconf(self):
        """Jobconf dictionary to enable sorting by value.
        """
        if not self._sort_values:
            return {}

        # translate _SORT_VALUES_JOBCONF to the correct Hadoop version,
        # without logging a warning
        hadoop_version = self.get_hadoop_version()

        jobconf = {}
        for k, v in _SORT_VALUES_JOBCONF.items():
            if hadoop_version:
                jobconf[translate_jobconf(k, hadoop_version)] = v
            else:
                for j in translate_jobconf_for_all_versions(k):
                    jobconf[j] = v

        return jobconf

    def _sort_values_partitioner(self):
        """Partitioner to use with *sort_values* keyword to the constructor."""
        if self._sort_values:
            return _SORT_VALUES_PARTITIONER
        else:
            return None

    # TODO: this is only used by non-local runners, and could
    # conceivably be moved to some intermediary class (RealMRJobRunner?)
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

    def _spark_script_args(self, step_num):
        """A list of args to the spark script/jar, used by
        _args_for_spark_step()."""
        step = self._get_step(step_num)

        if step['type'] == 'spark':
            # TODO: add in passthrough options
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

    def _spark_py_files(self):
        """The list of files to pass to spark-submit with --py-files.

        By default (cluster mode), Spark only accepts local files, so
        we pass these as-is.
        """
        py_files = []

        py_files.extend(self._opts['py_files'])

        # Spark doesn't have setup scripts; instead, we need to add
        # mrjob to
        if self._bootstrap_mrjob() and self.BOOTSTRAP_MRJOB_IN_SETUP:
            py_files.append(self._create_mrjob_zip())

        return py_files

    def _libjar_paths(self):
        """Paths or URIs of libjars, from Hadoop/Spark's point of view.

        Override this for non-local libjars (e.g. on EMR).
        """
        return self._opts['libjars']

    def _upload_args(self):
        # just upload every file and archive in the working dir manager
        return self._upload_args_helper('-files', None, '-archives', None)

    def _upload_args_helper(
            self, files_opt_str, files, archives_opt_str, archives):
        args = []

        file_hash_paths = list(self._arg_hash_paths('file', files))
        if file_hash_paths:
            args.append(files_opt_str)
            args.append(','.join(file_hash_paths))

        archive_hash_paths = list(self._arg_hash_paths('archive', archives))
        if archive_hash_paths:
            args.append(archives_opt_str)
            args.append(','.join(archive_hash_paths))

        return args

    def _arg_hash_paths(self, type, named_paths=None):
        """Helper function for the *upload_args methods."""
        if named_paths is None:
            # just return everything managed by _working_dir_mgr
            named_paths = sorted(
                self._working_dir_mgr.name_to_path(type).items())

        for name, path in named_paths:
            if not name:
                name = self._working_dir_mgr.name(type, path)
            uri = self._upload_mgr.uri(path)
            yield '%s#%s' % (uri, name)

    def _invoke_sort(self, input_paths, output_path):
        """Use the local sort command to sort one or more input files. Raise
        an exception if there is a problem.

        This is is just a wrapper to handle limitations of Windows sort
        (see Issue #288).

        :type input_paths: list of str
        :param input_paths: paths of one or more input files
        :type output_path: str
        :param output_path: where to pipe sorted output into
        """
        if not input_paths:
            raise ValueError('Must specify at least one input path.')

        # ignore locale when sorting
        env = os.environ.copy()
        env['LC_ALL'] = 'C'

        # Make sure that the tmp dir environment variables are changed if
        # the default is changed. (Make sure unicode is converted to str
        # for Windows)
        env['TMP'] = self._opts['local_tmp_dir']
        env['TMPDIR'] = self._opts['local_tmp_dir']
        env['TEMP'] = self._opts['local_tmp_dir']

        env = _fix_env(env)

        log.debug('Writing to %s' % output_path)

        err_path = os.path.join(self._get_local_tmp_dir(), 'sort-stderr')

        # assume we're using UNIX sort unless we know otherwise
        if (not self._sort_is_windows_sort) or len(input_paths) == 1:
            with open(output_path, 'wb') as output:
                with open(err_path, 'wb') as err:
                    args = ['sort'] + list(input_paths)
                    log.debug('> %s' % cmd_line(args))
                    try:
                        check_call(args, stdout=output, stderr=err, env=env)
                        return
                    except CalledProcessError:
                        pass

        # Looks like we're using Windows sort
        self._sort_is_windows_sort = True

        log.debug('Piping files into sort for Windows compatibility')
        with open(output_path, 'wb') as output:
            with open(err_path, 'wb') as err:
                args = ['sort']
                log.debug('> %s' % cmd_line(args))
                proc = Popen(args, stdin=PIPE, stdout=output, stderr=err,
                             env=env)

                # shovel bytes into the sort process
                for input_path in input_paths:
                    with open(input_path, 'rb') as input:
                        while True:
                            buf = input.read(_BUFFER_SIZE)
                            if not buf:
                                break
                            proc.stdin.write(buf)

                proc.stdin.close()
                proc.wait()

                if proc.returncode == 0:
                    return

        # looks like there was a problem. log it and raise an error
        with open(err_path) as err:
            for line in err:
                log.error('STDERR: %s' % line.rstrip('\r\n'))
        raise CalledProcessError(proc.returncode, args)


def _fix_env(env):
    """Convert environment dictionary to strings (Python 2.7 on Windows
    doesn't allow unicode)."""
    def _to_str(s):
        if isinstance(s, string_types) and not isinstance(s, str):
            return s.encode('utf_8')
        else:
            return s

    return dict((_to_str(k), _to_str(v)) for k, v in env.items())
