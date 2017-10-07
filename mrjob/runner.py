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
import logging
import os
import os.path
import posixpath
import pprint
import re
import shutil
import sys
import tarfile
import tempfile

import mrjob.step
from mrjob.compat import translate_jobconf
from mrjob.compat import translate_jobconf_dict
from mrjob.compat import translate_jobconf_for_all_versions
from mrjob.conf import combine_dicts
from mrjob.conf import combine_opts
from mrjob.conf import load_opts_from_mrjob_confs
from mrjob.fs.composite import CompositeFilesystem
from mrjob.fs.local import LocalFilesystem
from mrjob.options import _combiners
from mrjob.options import _deprecated_aliases
from mrjob.options import CLEANUP_CHOICES
from mrjob.parse import is_uri
from mrjob.py2 import PY2
from mrjob.py2 import string_types
from mrjob.setup import WorkingDirManager
from mrjob.setup import name_uniquely
from mrjob.setup import parse_legacy_hash_path
from mrjob.setup import parse_setup_cmd
from mrjob.step import _is_spark_step_type
from mrjob.util import to_lines
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


class MRJobRunner(object):
    """Abstract base class for all runners"""

    # this class handles the basic runner framework, options and config files,
    # arguments to mrjobs, and setting up job working dirs and environments.
    # this will put files from setup scripts, py_files, and bootstrap_mrjob
    # into the job's working dir, but won't actually run/import them
    #
    # command lines to run substeps (including Spark) are handled by
    # mrjob.bin.MRJobBinRunner

    #: alias for this runner; used for picking section of
    #: :py:mod:``mrjob.conf`` to load one of ``'local'``, ``'emr'``,
    #: or ``'hadoop'``
    alias = None

    # libjars is only here because the job can set it; might want to
    # handle this with a warning from the launcher instead
    OPT_NAMES = {
        'bootstrap_mrjob',
        'check_input_paths',
        'cleanup',
        'cleanup_on_failure',
        'cmdenv',
        'jobconf',
        'label',
        'libjars',
        'local_tmp_dir',
        'owner',
        'py_files',
        'setup',
        'upload_archives',
        'upload_dirs',
        'upload_files'
    }

    # if this is true, when bootstrap_mrjob is true, add it through the
    # setup script
    _BOOTSTRAP_MRJOB_IN_SETUP = True

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

        # opts are made from:
        #
        # empty defaults (everything set to None)
        # runner-specific defaults
        # opts from config file(s)
        # opts from command line
        self._opts = self._combine_confs(
            [(None, {key: None for key in self.OPT_NAMES})] +
            [(None, self._default_opts())] +
            load_opts_from_mrjob_confs(self.alias, conf_paths) +
            [('the command line', opts)]
        )

        log.debug('Active configuration:')
        log.debug(pprint.pformat({
            opt_key: self._obfuscate_opt(opt_key, opt_value)
            for opt_key, opt_value in self._opts.items()
        }))

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

        # extra args to our job
        self._extra_args = list(extra_args) if extra_args else []
        for extra_arg in self._extra_args:
            if isinstance(extra_arg, dict):
                if extra_arg.get('type') != 'file':
                    raise NotImplementedError
                self._working_dir_mgr.add(**extra_arg)
                self._spark_files.append(
                    (extra_arg['name'], extra_arg['path']))

        # extra file arguments to our job
        if file_upload_args:
            log.warning('file_upload_args is deprecated and will be removed'
                        ' in v0.6.0. Pass dicts to extra_args instead.')
            for arg, path in file_upload_args:
                arg_file = parse_legacy_hash_path('file', path)
                self._working_dir_mgr.add(**arg_file)
                self._extra_args.extend([arg, arg_file])
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

        # py_files

        # self._setup is a list of shell commands with path dicts
        # interleaved; see mrjob.setup.parse_setup_cmd() for details
        self._setup = self._parse_setup_and_py_files()
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

        # this variable marks whether a cleanup has happened and this runner's
        # output stream is no longer available.
        self._closed = False

    ### Options ####

    def _default_opts(self):
        try:
            owner = getpass.getuser()
        except:
            owner = None

        return dict(
            check_input_paths=True,
            cleanup=['ALL'],
            cleanup_on_failure=['NONE'],
            local_tmp_dir=tempfile.gettempdir(),
            owner=owner,
        )

    def _combine_confs(self, source_and_opt_list):
        """Combine several opt dictionaries into one.

        *source_and_opt_list* is a list of tuples of *source*,
        *opts* where *opts* is a dictionary and *source* is either
        None or a description of where the opts came from (usually a path).

        Only override this if you need truly fine-grained control,
        including knowledge of the options' source.
        """
        opt_list = [
            self._fix_opts(opts, source)
            for source, opts in source_and_opt_list
        ]

        return self._combine_opts(opt_list)

    def _combine_opts(self, opt_list):
        """Combine several opt dictionaries into one. *opt_list*
        is a list of dictionaries containing validated options

        Override this if you need to base options off the values of
        other options, but don't need to issue warnings etc.
        about the options' source.
        """
        return combine_opts(self._opt_combiners(), *opt_list)

    def _opt_combiners(self):
        """A dictionary mapping opt name to combiner funciton. This
        won't necessarily include every opt name (we default to
        :py:func:`~mrjob.conf.combine_value`).
        """
        return _combiners(self.OPT_NAMES)

    def _fix_opts(self, opts, source=None):
        """Take an options dictionary, and either return a sanitized
        version of it, or raise an exception.

        *source* is either a string describing where the opts came from
        or None.

        This ensures that opt dictionaries are really dictionaries
        and handles deprecated options.
        """
        if source is None:
            source = 'defaults'  # defaults shouldn't trigger warnings

        if not isinstance(opts, dict):
            raise TypeError(
                'options for %s (from %s) must be a dict' %
                self.runner_alias, source)

        deprecated_aliases = _deprecated_aliases(self.OPT_NAMES)

        results = {}

        for k, v in sorted(opts.items()):

            # rewrite deprecated aliases
            if k in deprecated_aliases:
                if v is None:  # don't care
                    continue

                aliased_opt = deprecated_aliases

                log.warning('Deprecated option %s (from %s) has been renamed'
                            ' to %s and will be removed in v0.7.0' % (
                                k, source, aliased_opt))

                if opts.get(aliased_opt) is not None:
                    return  # don't overwrite non-aliased opt

                k = aliased_opt

            if k in self.OPT_NAMES:
                results[k] = None if v is None else self._fix_opt(k, v, source)
            else:
                log.warning('Unexpected option %s (from %s)' % (k, source))

        return results

    def _fix_opt(self, opt_key, opt_value, source):
        """Fix a single option, returning its correct value or raising
        an exception. This is not called for options that are ``None``.

        This currently handles cleanup opts.

        Override this if you require additional opt validation or cleanup.
        """
        if opt_key in ('cleanup', 'cleanup_on_failure'):
            return self._fix_cleanup_opt(opt_key, opt_value, source)
        else:
            return opt_value

    def _fix_cleanup_opt(self, opt_key, opt_value, source):
        """Fix a cleanup option, or raise ValueError."""
        if isinstance(opt_value, string_types):
            opt_value = [opt_value]

        if 'NONE' in opt_value and len(set(opt_value)) > 1:
            raise ValueError(
                'Cannot clean up both nothing and something!'
                ' (%s option from %s)' % (opt_key, source))

        for cleanup_type in opt_value:
            if cleanup_type not in CLEANUP_CHOICES:
                raise ValueError(
                    '%s must be one of %s, not %s (from %s)' % (
                        opt_key, ', '.join(CLEANUP_CHOICES), opt_value,
                        source))

        return opt_value

    def _obfuscate_opt(self, opt_key, opt_value):
        """Return value of opt to show in debug printout. Used to obfuscate
        credentials, etc."""
        return opt_value

    ### Filesystem object ###

    @property
    def fs(self):
        """:py:class:`~mrjob.fs.base.Filesystem` object for the local
        filesystem.
        """
        if self._fs is None:
            # wrap LocalFilesystem in CompositeFilesystem to get IOError
            # on URIs (see #1185)
            self._fs = CompositeFilesystem(LocalFilesystem())
        return self._fs

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

        self._create_dir_archives()
        self._check_input_paths()
        self._run()
        self._ran_job = True

    def cat_output(self):
        """Stream the jobs output, as a stream of ``bytes``. If there are
        multiple output files, there will be an empty bytestring
        (``b''``) between them.

        .. versionadded:: 0.6.0

           In previous versions, you'd use :py:meth:`stream_output`.
        """
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

        def ls_output():
            for filename in self.fs.ls(output_dir):
                subpath = filename[len(output_dir):]
                if not (any(name.startswith('_')
                            for name in split_path(subpath))):
                    yield filename

        for i, filename in enumerate(ls_output()):
            if i > 0:
                yield b''  # EOF of previous file

            for chunk in self.fs._cat_file(filename):
                yield chunk

    def stream_output(self):
        """Like :py:meth:`cat_output` except that it groups bytes into
        lines. Equivalent to ``mrjob.util.to_lines(runner.stream_output())``.

        .. deprecated:: 0.6.0
        """
        log.warning('stream_output() is deprecated and will be removed in'
                    ' v0.7.0. use mrjob.util.to_lines(runner.cat_output())'
                    ' instead.')

        return to_lines(self.cat_output())

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
        log.warning('get_opts() is deprecated and will be removed in v0.7.0')
        return copy.deepcopy(self._opts)

    def get_job_key(self):
        """Get the unique key for the job run by this runner.
        This has the format ``label.owner.date.time.microseconds``
        """
        return self._job_key

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
            self._steps = self._load_steps()

        return self._steps

    def _load_steps(self):
        """Ask job how many steps it has, and whether
        there are mappers and reducers for each step.

        Returns output as described in :ref:`steps-format`.
        """
        raise NotImplementedError

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

    def _args_for_task(self, step_num, mrc):
        return [
            '--step-num=%d' % step_num,
            '--%s' % mrc,
        ] + self._mr_job_extra_args()

    def _mr_job_extra_args(self, local=False):
        """Return arguments to add to every invocation of MRJob.

        :type local: boolean
        :param local: if this is True, use files' local paths rather than
            the path they'll have inside Hadoop streaming
        """
        result = []

        for extra_arg in self._extra_args:
            if isinstance(extra_arg, dict):
                if local:
                    result.append(extra_arg['path'])
                else:
                    result.append(self._working_dir_mgr.name(**extra_arg))
            else:
                result.append(extra_arg)

        return result

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

        # for remote files
        tmp_download_path = os.path.join(
            self._get_local_tmp_dir(), 'tmp-download')

        log.info('Archiving %s -> %s' % (dir_path, tar_gz_path))

        with tarfile.open(tar_gz_path, mode='w:gz') as tar_gz:
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

    def _check_input_paths(self):
        """Check that input exists prior to running the job, if the
        `check_input_paths` option is true."""
        if not self._opts['check_input_paths']:
            return

        for path in self._input_paths:
            if path == '-':
                    continue  # STDIN always exists

            if not self.fs.can_handle_path(path):
                continue  # e.g. non-S3 URIs on EMR

            if not self.fs.exists(path):
                raise IOError(
                    'Input path %s does not exist!' % (path,))

    def _intermediate_output_uri(self, step_num, local=False):
        """A URI for intermediate output for the given step number."""
        join = os.path.join if local else posixpath.join

        return join(
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

    def _parse_setup_and_py_files(self):
        """Parse the *setup* option with
        :py:func:`mrjob.setup.parse_setup_cmd()`, and patch in *py_files*.
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

        # setup
        for cmd in self._opts['setup']:
            setup.append(parse_setup_cmd(cmd))

        return setup

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


def _fix_env(env):
    """Convert environment dictionary to strings (Python 2.7 on Windows
    doesn't allow unicode)."""
    def _to_str(s):
        if isinstance(s, string_types) and not isinstance(s, str):
            return s.encode('utf_8')
        else:
            return s

    return dict((_to_str(k), _to_str(v)) for k, v in env.items())
