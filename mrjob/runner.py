# Copyright 2009-2012 Yelp and Contributors
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
from __future__ import with_statement

"""Base class for all runners."""

import copy
import datetime
import getpass
import logging
import os
import random
import re
import shutil
import sys
from subprocess import CalledProcessError
from subprocess import Popen
from subprocess import PIPE
from subprocess import check_call
import tempfile

try:
    from cStringIO import StringIO
    StringIO  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    from StringIO import StringIO

try:
    import simplejson as json
    json
except:
    import json

from mrjob import compat
from mrjob.conf import combine_cmds
from mrjob.conf import combine_dicts
from mrjob.conf import combine_envs
from mrjob.conf import combine_local_envs
from mrjob.conf import combine_lists
from mrjob.conf import combine_paths
from mrjob.conf import combine_path_lists
from mrjob.conf import load_opts_from_mrjob_confs
from mrjob.conf import OptionStore
from mrjob.fs.local import LocalFilesystem
from mrjob.step import STEP_TYPES
from mrjob.util import bash_wrap
from mrjob.util import cmd_line
from mrjob.util import file_ext
from mrjob.util import tar_and_gzip


log = logging.getLogger('mrjob.runner')

# use to detect globs and break into the part before and after the glob
GLOB_RE = re.compile(r'^(.*?)([\[\*\?].*)$')

#: cleanup options:
#:
#: * ``'ALL'``: delete local scratch, remote scratch, and logs; stop job if
#:   running on EMR and job flow is not to be terminated
#: * ``'LOCAL_SCRATCH'``: delete local scratch only
#: * ``'LOGS'``: delete logs only
#: * ``'NONE'``: delete nothing
#: * ``'REMOTE_SCRATCH'``: delete remote scratch only
#: * ``'SCRATCH'``: delete local and remote scratch, but not logs
#:   ``cleanup_on_failure``.
CLEANUP_CHOICES = ['ALL', 'LOCAL_SCRATCH', 'LOGS', 'NONE', 'REMOTE_SCRATCH',
                   'SCRATCH']

# buffer for piping files into sort on Windows
_BUFFER_SIZE = 4096


class RunnerOptionStore(OptionStore):

    # Test cases for this class live in tests.test_option_store rather than
    # tests.test_runner.

    ALLOWED_KEYS = OptionStore.ALLOWED_KEYS.union(set([
        'base_tmp_dir',
        'bootstrap_mrjob',
        'cleanup',
        'cleanup_on_failure',
        'cmdenv',
        'hadoop_extra_args',
        'hadoop_streaming_jar',
        'hadoop_version',
        'interpreter',
        'jobconf',
        'label',
        'owner',
        'python_archives',
        'python_bin',
        'setup_cmds',
        'setup_scripts',
        'steps_python_bin',
        'upload_archives',
        'upload_files',
    ]))

    COMBINERS = combine_dicts(OptionStore.COMBINERS, {
        'base_tmp_dir': combine_paths,
        'cmdenv': combine_envs,
        'hadoop_extra_args': combine_lists,
        'interpreter': combine_cmds,
        'jobconf': combine_dicts,
        'python_archives': combine_path_lists,
        'python_bin': combine_cmds,
        'setup_cmds': combine_lists,
        'setup_scripts': combine_path_lists,
        'steps_python_bin': combine_cmds,
        'upload_archives': combine_path_lists,
        'upload_files': combine_path_lists,
    })

    DEFAULT_ALIASES = {
        'interpreter': 'python_bin',
    }

    def __init__(self, alias, opts, conf_paths):
        """
        :param alias: Runner alias (e.g. ``'local'``)
        :param opts: Options from the command line
        :param conf_paths: Either a file path or an iterable of paths to config
                           files
        """
        super(RunnerOptionStore, self).__init__()

        # sanitize incoming options and issue warnings for bad keys
        opts = self.validated_options(
            opts, 'Got unexpected keyword arguments: %s')

        unsanitized_opt_dicts = load_opts_from_mrjob_confs(
            alias, conf_paths=conf_paths)

        for path, mrjob_conf_opts in unsanitized_opt_dicts:
            self.cascading_dicts.append(self.validated_options(
                mrjob_conf_opts,
                'Got unexpected opts from %s: %%s' % path))

        self.cascading_dicts.append(opts)

        if (len(self.cascading_dicts) > 2 and
            all(len(d) == 0 for d in self.cascading_dicts[2:-1]) and
            (len(conf_paths or []) > 0 or len(opts) == 0)):
            log.warning('No configs specified for %s runner' % alias)

        self.populate_values_from_cascading_dicts()

        self._validate_cleanup()

    def default_options(self):
        super_opts = super(RunnerOptionStore, self).default_options()

        try:
            owner = getpass.getuser()
        except:
            owner = None

        return combine_dicts(super_opts, {
            'base_tmp_dir': tempfile.gettempdir(),
            'bootstrap_mrjob': True,
            'cleanup': ['ALL'],
            'cleanup_on_failure': ['NONE'],
            'hadoop_version': '0.20',
            'owner': owner,
            'python_bin': ['python'],
            'steps_python_bin': [sys.executable or 'python'],
        })

    def _validate_cleanup(self):
        # old API accepts strings for cleanup
        # new API wants lists
        for opt_key in ('cleanup', 'cleanup_on_failure'):
            if isinstance(self[opt_key], basestring):
                self[opt_key] = [self[opt_key]]

        def validate_cleanup(error_str, opt_list):
            for choice in opt_list:
                if choice not in CLEANUP_CHOICES:
                    raise ValueError(error_str % choice)
            if 'NONE' in opt_list and len(set(opt_list)) > 1:
                raise ValueError(
                    'Cannot clean up both nothing and something!')

        cleanup_error = ('cleanup must be one of %s, not %%s' %
                         ', '.join(CLEANUP_CHOICES))
        validate_cleanup(cleanup_error, self['cleanup'])

        cleanup_failure_error = (
            'cleanup_on_failure must be one of %s, not %%s' %
            ', '.join(CLEANUP_CHOICES))
        validate_cleanup(cleanup_failure_error,
                         self['cleanup_on_failure'])


class MRJobRunner(object):
    """Abstract base class for all runners"""

    #: alias for this runner; used for picking section of
    #: :py:mod:``mrjob.conf`` to load one of ``'local'``, ``'emr'``,
    #: or ``'hadoop'``
    alias = None

    OPTION_STORE_CLASS = RunnerOptionStore

    ### methods to call from your batch script ###

    def __init__(self, mr_job_script=None, conf_path=None,
                 extra_args=None, file_upload_args=None,
                 hadoop_input_format=None, hadoop_output_format=None,
                 input_paths=None, output_dir=None, partitioner=None,
                 stdin=None, conf_paths=None, **opts):
        """All runners take the following keyword arguments:

        :type mr_job_script: str
        :param mr_job_script: the path of the ``.py`` file containing the
                              :py:class:`~mrjob.job.MRJob`. If this is None,
                              you won't actually be able to :py:meth:`run` the
                              job, but other utilities (e.g. :py:meth:`ls`)
                              will work.
        :type conf_path: str, None, or False
        :param conf_path: Deprecated. Alternate path to read configs from, or
                          ``False`` to ignore all config files. Use
                          *conf_paths* instead.
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
                                    *hadoop_streaming_jar*).
        :type hadoop_output_format: str
        :param hadoop_output_format: name of an optional Hadoop
                                     ``OutputFormat`` class. Passed to Hadoop
                                     along with your first step with the
                                     ``-outputformat`` option. Note that if you
                                     write your own class, you'll need to
                                     include it in your own custom streaming
                                     jar (see *hadoop_streaming_jar*).
        :type input_paths: list of str
        :param input_paths: Input files for your job. Supports globs and
                            recursively walks directories (e.g.
                            ``['data/common/', 'data/training/*.gz']``). If
                            this is left blank, we'll read from stdin
        :type output_dir: str
        :param output_dir: An empty/non-existent directory where Hadoop
                           streaming should put the final output from the job.
                           If you don't specify an output directory, we'll
                           output into a subdirectory of this job's temporary
                           directory. You can control this from the command
                           line with ``--output-dir``. This option cannot be
                           set from configuration files. If used with the
                           hadoop runner, this path does not need to be fully
                           qualified with ``hdfs://`` URIs because it's
                           understood that it has to be on HDFS.
        :type partitioner: str
        :param partitioner: Optional name of a Hadoop partitoner class, e.g.
                            ``'org.apache.hadoop.mapred.lib.HashPartitioner'``.
                            Hadoop streaming will use this to determine how
                            mapper output should be sorted and distributed
                            to reducers.
        :param stdin: an iterable (can be a ``StringIO`` or even a list) to use
                      as stdin. This is a hook for testing; if you set
                      ``stdin`` via :py:meth:`~mrjob.job.MRJob.sandbox`, it'll
                      get passed through to the runner. If for some reason
                      your lines are missing newlines, we'll add them;
                      this makes it easier to write automated tests.
        """
        if conf_path is not None:
            if conf_paths is not None:
                raise ValueError("Can't specify both conf_path and conf_paths")
            else:
                log.warn("The conf_path argument to MRJobRunner() is"
                         " deprecated. Use conf_paths instead.")
                if conf_path is False:
                    conf_paths = []
                else:
                    conf_paths = [conf_path]
        self._opts = self.OPTION_STORE_CLASS(self.alias, opts, conf_paths)
        self._fs = None

        # we potentially have a lot of files to copy, so we keep track
        # of them as a list of dictionaries, with the following keys:
        #
        # 'path': the path to the file on the local system
        # 'name': a unique name for the file when we copy it into HDFS etc.
        # if this is blank, we'll pick one
        # 'cache': if 'file', copy into mr_job_script's working directory
        # on the Hadoop nodes. If 'archive', uncompress the file
        self._files = []

        # add the script to our list of files (don't actually commit to
        # uploading it)
        if mr_job_script:
            self._script = {'path': mr_job_script}
            self._files.append(self._script)
            self._ran_job = False
        else:
            self._script = None
            self._ran_job = True  # don't allow user to call run()

        # setup cmds and wrapper script
        self._setup_scripts = []
        for path in self._opts['setup_scripts']:
            file_dict = self._add_file_for_upload(path)
            self._setup_scripts.append(file_dict)

        # we'll create the wrapper script later
        self._wrapper_script = None

        # extra args to our job
        self._extra_args = list(extra_args) if extra_args else []

        # extra file arguments to our job
        self._file_upload_args = []
        if file_upload_args:
            for arg, path in file_upload_args:
                file_dict = self._add_file_for_upload(path)
                self._file_upload_args.append((arg, file_dict))

        # set up uploading
        for path in self._opts['upload_archives']:
            self._add_archive_for_upload(path)
        for path in self._opts['upload_files']:
            self._add_file_for_upload(path)

        # set up python archives
        self._python_archives = []
        for path in self._opts['python_archives']:
            self._add_python_archive(path)

        # where to read input from (log files, etc.)
        self._input_paths = input_paths or ['-']  # by default read from stdin
        self._stdin = stdin or sys.stdin
        self._stdin_path = None  # temp file containing dump from stdin

        # where a tarball of the mrjob library is stored locally
        self._mrjob_tar_gz_path = None

        # store output_dir
        self._output_dir = output_dir

        # store partitioner
        self._partitioner = partitioner

        # store hadoop input and output formats
        self._hadoop_input_format = hadoop_input_format
        self._hadoop_output_format = hadoop_output_format

        # give this job a unique name
        self._job_name = self._make_unique_job_name(
            label=self._opts['label'], owner=self._opts['owner'])

        # a local tmp directory that will be cleaned up when we're done
        # access/make this using self._get_local_tmp_dir()
        self._local_tmp_dir = None

        # info about our steps. this is basically a cache for self._get_steps()
        self._steps = None

        # if this is True, we have to pipe input into the sort command
        # rather than feed it multiple files
        self._sort_is_windows_sort = None

    ### Filesystem object ###

    @property
    def fs(self):
        """:py:class:`~mrjob.fs.base.Filesystem` object for the local
        filesystem. Methods on :py:class:`~mrjob.fs.base.Filesystem` objects
        will be forwarded to :py:class:`~mrjob.runner.MRJobRunner` until mrjob
        0.5, but **this behavior is deprecated.**
        """
        if self._fs is None:
            self._fs = LocalFilesystem()
        return self._fs

    def __getattr__(self, name):
        # For backward compatibility, forward filesystem methods
        try:
            return getattr(self.fs, name)
        except AttributeError:
            raise AttributeError(name)

    ### Running the job and parsing output ###

    def run(self):
        """Run the job, and block until it finishes.

        Raise an exception if there are any problems.
        """
        assert not self._ran_job
        self._run()
        self._ran_job = True

    def stream_output(self):
        """Stream raw lines from the job's output. You can parse these
        using the read() method of the appropriate HadoopStreamingProtocol
        class."""
        assert self._ran_job

        output_dir = self.get_output_dir()
        log.info('Streaming final output from %s' % output_dir)

        def split_path(path):
            while True:
                base, name = os.path.split(path)

                # no more elements
                if not name:
                    break

                yield name

                path = base

        for filename in self.ls(output_dir):
            subpath = filename[len(output_dir):]
            if not any(name.startswith('_') for name in split_path(subpath)):
                for line in self._cat_file(filename):
                    yield line

    def _cleanup_mode(self, mode=None):
        """Actual cleanup action to take based on various options"""
        if self._ran_job:
            return mode or self._opts['cleanup']
        else:
            return mode or self._opts['cleanup_on_failure']

    def _cleanup_local_scratch(self):
        """Cleanup any files/directories on the local machine we created while
        running this job. Should be safe to run this at any time, or multiple
        times.

        This particular function removes any local tmp directories
        added to the list self._local_tmp_dirs

        This won't remove output_dir if it's outside of our scratch dir.
        """
        if self._local_tmp_dir:
            log.info('removing tmp directory %s' % self._local_tmp_dir)
            try:
                shutil.rmtree(self._local_tmp_dir)
            except OSError, e:
                log.exception(e)

        self._local_tmp_dir = None

    def _cleanup_remote_scratch(self):
        """Cleanup any files/directories on the remote machine (S3) we created
        while running this job. Should be safe to run this at any time, or
        multiple times.
        """
        pass  # this only happens on EMR

    def _cleanup_logs(self):
        """Cleanup any log files that are created as a side-effect of the job.
        """
        pass  # this only happens on EMR

    def _cleanup_job(self):
        """Stop any jobs that we created that are still running."""
        pass  # this only happens on EMR

    def cleanup(self, mode=None):
        """Clean up running jobs, scratch dirs, and logs, subject to the
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

        if mode_has('ALL', 'JOB'):
            self._cleanup_job()

        if mode_has('ALL', 'SCRATCH', 'LOCAL_SCRATCH'):
            self._cleanup_local_scratch()

        if mode_has('ALL', 'SCRATCH', 'REMOTE_SCRATCH'):
            self._cleanup_remote_scratch()

        if mode_has('ALL', 'LOGS'):
            self._cleanup_logs()

    def counters(self):
        """Get counters associated with this run in this form::

            [{'group name': {'counter1': 1, 'counter2': 2}},
             {'group name': ...}]

        The list contains an entry for every step of the current job, ignoring
        earlier steps in the same job flow.
        """
        raise NotImplementedError

    def print_counters(self, limit_to_steps=None):
        """Display this run's counters in a user-friendly way.

        :type first_step_num: int
        :param first_step_num: Display step number of the counters from the
                               first step
        :type limit_to_steps: list of int
        :param limit_to_steps: List of step numbers *relative to this job* to
                               print, indexed from 1
        """
        for step_num, step_counters in enumerate(self.counters()):
            step_num = step_num + 1
            if limit_to_steps is None or step_num in limit_to_steps:
                log.info('Counters from step %d:' % step_num)
                if step_counters.keys():
                    for group_name in sorted(step_counters.keys()):
                        log.info('  %s:' % group_name)
                        group_counters = step_counters[group_name]
                        for counter_name in sorted(group_counters.keys()):
                            log.info('    %s: %d' % (
                                counter_name, group_counters[counter_name]))
                else:
                    log.info('  (no counters found)')

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

    def get_job_name(self):
        """Get the unique name for the job run by this runner.
        This has the format ``label.owner.date.time.microseconds``
        """
        return self._job_name

    def get_output_dir(self):
        """Find the directory containing the job output. If the job hasn't
        run yet, returns None"""
        if not self._ran_job:
            return None

        return self._output_dir

    ### other methods you need to implement in your subclass ###

    def get_hadoop_version(self):
        """Return the version number of the Hadoop environment as a string if
        Hadoop is being used or simulated. Return None if not applicable.

        :py:class:`~mrjob.emr.EMRJobRunner` infers this from the job flow.
        :py:class:`~mrjob.hadoop.HadoopJobRunner` gets this from
        ``hadoop version``. :py:class:`~mrjob.local.LocalMRJobRunner` has an
        additional `hadoop_version` option to specify which version it
        simulates, with a default of 0.20.
        :py:class:`~mrjob.inline.InlineMRJobRunner` does not simulate Hadoop at
        all.
        """
        return None

    # you'll probably wan't to add your own __init__() and cleanup() as well

    def _run(self):
        """Run the job."""
        raise NotImplementedError

    ### internal utilities for implementing MRJobRunners ###

    def _split_path(self, path):
        """Split a path like /foo/bar.py#baz.py into (path, name)
        (in this case: '/foo/bar.py', 'baz.py').

        It's valid to specify no name with something like '/foo/bar.py#'
        In practice this means that we'll pick a name.
        """
        if '#' in path:
            path, name = path.split('#', 1)
            if '/' in name or '#' in name:
                raise ValueError('Bad name %r; must not contain # or /' % name)
            # empty names are okay
        else:
            name = os.path.basename(path)

        return name, path

    def _add_file(self, path):
        """Add a file that's uploaded, but not added to the working
        dir for *mr_job_script*.

        You probably want _add_for_upload() in most cases
        """
        name, path = self._split_path(path)
        file_dict = {'path': path, 'name': name}
        self._files.append(file_dict)
        return file_dict

    def _add_for_upload(self, path, what):
        """Add a file to our list of files to copy into the working
        dir for *mr_job_script*.

        path -- path to the file on the local filesystem. Normally
            we just use the file's name as it's remote name. You can
            use a  # character to pick a different name for the file:

            /foo/bar#baz -> upload /foo/bar as baz
            /foo/bar# -> upload /foo/bar, pick any name for it

        upload -- either 'file' (just copy) or 'archive' (uncompress)

        Returns:
        The internal dictionary representing the file (in case we
        want to point to it).
        """
        name, path = self._split_path(path)

        file_dict = {'path': path, 'name': name, 'upload': what}
        self._files.append(file_dict)

        return file_dict

    def _add_file_for_upload(self, path):
        return self._add_for_upload(path, 'file')

    def _add_archive_for_upload(self, path):
        return self._add_for_upload(path, 'archive')

    def _add_python_archive(self, path):
        file_dict = self._add_archive_for_upload(path)
        self._python_archives.append(file_dict)

    def _get_cmdenv(self):
        """Get the environment variables to use inside Hadoop.

        These should be `self._opts['cmdenv']` combined with python
        archives added to :envvar:`PYTHONPATH`.

        This function calls :py:meth:`MRJobRunner._name_files`
        (since we need to know where each python archive ends up in the job's
        working dir)
        """
        self._name_files()
        # on Windows, PYTHONPATH should be separated by ;, not :
        # so LocalJobRunner and EMRJobRunner use different combiners for cmdenv
        cmdenv_combiner = self.OPTION_STORE_CLASS.COMBINERS['cmdenv']
        envs_to_combine = ([{'PYTHONPATH': file_dict['name']}
                            for file_dict in self._python_archives] +
                           [self._opts['cmdenv']])

        return cmdenv_combiner(*envs_to_combine)

    def _assign_unique_names_to_files(self, name_field, prefix='', match=None):
        """Go through self._files, and fill in name_field for all files where
        it's not already filled, so that every file has a unique value for
        name_field. We'll try to give the file the same name as its local path
        (and we'll definitely keep the extension the same).

        Args:
        name_field -- field to fill in (e.g. 'name', 's3_uri', hdfs_uri')
        prefix -- prefix to prepend to each name (e.g. a path to a tmp dir)
        match -- a function that returns a true value if the path should
            just be copied verbatim to the name (for example if we're
            assigning HDFS uris and the path starts with 'hdfs://').
        """
        # handle files that are already on S3, HDFS, etc.
        if match:
            for file_dict in self._files:
                path = file_dict['path']
                if match(path) and not file_dict.get(name_field):
                    file_dict[name_field] = path

        # check for name collisions
        name_to_path = {}

        for file_dict in self._files:
            name = file_dict.get(name_field)
            if name:
                path = file_dict['path']
                if name in name_to_path and path != name_to_path[name]:
                    raise ValueError("Can't copy both %s and %s to %s" %
                                     (path, name_to_path[name], name))
                name_to_path[name] = path

        # give names to files that don't have them
        for file_dict in self._files:
            if not file_dict.get(name_field):
                path = file_dict['path']
                basename = os.path.basename(path)
                name = prefix + basename
                # if name is taken, prepend some random stuff to it
                while name in name_to_path:
                    name = prefix + '%08x-%s' % (
                        random.randint(0, 2 ** 32 - 1), basename)
                file_dict[name_field] = name
                name_to_path[name] = path  # reserve this name

    def _name_files(self):
        """Fill in the 'name' field for every file in self._files so
        that they all have unique names.

        It's safe to run this method as many times as you want.
        """
        self._assign_unique_names_to_files('name')

    def _get_local_tmp_dir(self):
        """Create a tmp directory on the local filesystem that will be
        cleaned up by self.cleanup()"""
        if not self._local_tmp_dir:
            path = os.path.join(self._opts['base_tmp_dir'], self._job_name)
            log.info('creating tmp directory %s' % path)
            os.makedirs(path)
            self._local_tmp_dir = path

        return self._local_tmp_dir

    def _make_unique_job_name(self, label=None, owner=None):
        """Come up with a useful unique ID for this job.

        We use this to choose the output directory, etc. for the job.
        """
        # use the name of the script if one wasn't explicitly
        # specified
        if not label:
            if self._script:
                label = os.path.basename(
                    self._script['path']).split('.')[0]
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

        Returns output as described in :ref:`steps-format`. Results are
        cached to avoid round trips to a subprocess.
        """
        if self._steps is None:
            if not self._script:
                self._steps = []
            else:
                args = (self._opts['steps_python_bin'] +
                        [self._script['path'], '--steps'] +
                        self._mr_job_extra_args(local=True))
                log.debug('> %s' % cmd_line(args))
                # add . to PYTHONPATH (in case mrjob isn't actually installed)
                env = combine_local_envs(os.environ,
                                   {'PYTHONPATH': os.path.abspath('.')})
                steps_proc = Popen(args, stdout=PIPE, stderr=PIPE, env=env)
                stdout, stderr = steps_proc.communicate()

                if steps_proc.returncode != 0:
                    raise Exception(
                        'error getting step information: %s', stderr)

                try:
                    steps = json.loads(stdout)
                except json.JSONDecodeError:
                    raise ValueError("Bad --steps response: \n%s" % stdout)

                # verify that this is a proper step description
                if not steps or not stdout:
                    raise ValueError('step description is empty!')
                for step in steps:
                    if step['type'] not in STEP_TYPES:
                        raise ValueError(
                            'unexpected step type %r in steps %r' %
                                         (step['type'], stdout))

                self._steps = steps

        return self._steps

    def _executable(self):
        # default behavior is to always use an interpreter. local, emr, and
        # hadoop runners check for executable script paths and prepend the
        # working_dir, discarding the interpreter if possible.
        return self._opts['interpreter'] + [self._script['name']]

    def _script_args_for_step(self, step_num, mrc):
        assert self._script

        args = self._executable() + [
            '--step-num=%d' % step_num,
            '--%s' % mrc,
        ] + self._mr_job_extra_args()
        if self._wrapper_script:
            return (
                self._opts['python_bin'] +
                [self._wrapper_script['name']] +
                args)
        else:
            return args

    def _substep_cmd_line(self, step, step_num, mrc):
        if step[mrc]['type'] == 'command':
            # never wrap custom hadoop streaming commands in bash
            return step[mrc]['command'], False

        elif step[mrc]['type'] == 'script':
            cmd = cmd_line(self._script_args_for_step(step_num, mrc))

            # filter input and pipe for great speed, if user asks
            # but we have to wrap the command in bash
            if 'pre_filter' in step[mrc]:
                return '%s | %s' % (step[mrc]['pre_filter'], cmd), True
            else:
                return cmd, False
        else:
            raise ValueError("Invalid %s step %d: %r" % (
                mrc, step_num, step[mrc]))

    def _render_substep(self, step, step_num, mrc):
        if mrc in step:
            return self._substep_cmd_line(
                step, step_num, mrc)
        else:
            if mrc == 'mapper':
                return 'cat', False
            else:
                return None, False

    def _hadoop_streaming_commands(self, step, step_num):
        version = self.get_hadoop_version()

        # Hadoop streaming stuff
        mapper, bash_wrap_mapper = self._render_substep(
            step, step_num, 'mapper')

        combiner, bash_wrap_combiner = self._render_substep(
            step, step_num, 'combiner')

        reducer, bash_wrap_reducer = self._render_substep(
            step, step_num, 'reducer')

        if (combiner is not None and
            not compat.supports_combiners_in_hadoop_streaming(version)):

            # krazy hack to support combiners on hadoop <0.20
            bash_wrap_mapper = True
            mapper = "%s | sort | %s" % (mapper, combiner)

            # take the combiner away, hadoop will just be confused
            combiner = None
            bash_wrap_combiner = False

        if bash_wrap_mapper:
            mapper = bash_wrap(mapper)

        if bash_wrap_combiner:
            combiner = bash_wrap(combiner)

        if bash_wrap_reducer:
            reducer = bash_wrap(reducer)

        return mapper, combiner, reducer

    def _mr_job_extra_args(self, local=False):
        """Return arguments to add to every invocation of MRJob.

        :type local: boolean
        :param local: if this is True, use files' local paths rather than
            the path they'll have inside Hadoop streaming
        """
        return self._get_file_upload_args(local=local) + self._extra_args

    def _get_file_upload_args(self, local=False):
        """Arguments used to pass through config files, etc from the job
        runner through to the local directory where the script is run.

        :type local: boolean
        :param local: if this is True, use files' local paths rather than
            the path they'll have inside Hadoop streaming
        """
        args = []
        for arg, file_dict in self._file_upload_args:
            args.append(arg)
            if local:
                args.append(file_dict['path'])
            else:
                args.append(file_dict['name'])
        return args

    def _wrapper_script_content(self):
        """Output a python script to the given file descriptor that runs
        setup_cmds and setup_scripts, and then runs its arguments.

        This will give names to our files if they don't already have names.
        """
        self._name_files()

        out = StringIO()

        def writeln(line=''):
            out.write(line + '\n')

        # imports
        writeln('from fcntl import flock, LOCK_EX, LOCK_UN')
        writeln('from subprocess import check_call, PIPE')
        writeln('import sys')
        writeln()

        # make lock file and lock it
        writeln("lock_file = open('/tmp/wrapper.lock.%s', 'a')" %
                self._job_name)
        writeln('flock(lock_file, LOCK_EX)')
        writeln()

        # run setup cmds
        if self._opts['setup_cmds']:
            writeln('# run setup cmds:')
            for cmd in self._opts['setup_cmds']:
                # only use the shell for strings, not for lists of arguments
                # redir stdout to /dev/null so that it won't get confused
                # with the mapper/reducer's output
                writeln(
                    "check_call(%r, shell=%r, stdout=open('/dev/null', 'w'))"
                    % (cmd, bool(isinstance(cmd, basestring))))
            writeln()

        # run setup scripts
        if self._setup_scripts:
            writeln('# run setup scripts:')
            for file_dict in self._setup_scripts:
                writeln("check_call(%r, stdout=open('/dev/null', 'w'))" % (
                    ['./' + file_dict['name']],))
            writeln()

        # unlock the lock file
        writeln('flock(lock_file, LOCK_UN)')
        writeln()

        # run the real script
        writeln('# run the real mapper/reducer')
        writeln('check_call(sys.argv[1:])')

        return out.getvalue()

    def _create_wrapper_script(self, dest='wrapper.py'):
        """Create the wrapper script, and write it into our local temp
        directory (by default, to a file named wrapper.py).

        This will set self._wrapper_script, and append it to self._files

        This will do nothing if setup_cmds and setup_scripts are
        empty, or _create_wrapper_script() has already been called.
        """
        if not (self._opts['setup_cmds'] or self._setup_scripts):
            return

        if self._wrapper_script:
            return

        path = os.path.join(self._get_local_tmp_dir(), dest)
        log.info('writing wrapper script to %s' % path)

        contents = self._wrapper_script_content()
        for line in StringIO(contents):
            log.debug('WRAPPER: ' + line.rstrip('\r\n'))

        f = open(path, 'w')
        f.write(contents)
        f.close()

        self._wrapper_script = {'path': path}
        self._files.append(self._wrapper_script)

    def _dump_stdin_to_local_file(self):
        """Dump STDIN to a file in our local dir, and set _stdin_path
        to point at it.

        You can safely call this multiple times; it'll only read from
        stdin once.
        """
        if self._stdin_path is None:
            # prompt user, so they don't think the process has stalled
            log.info('reading from STDIN')

            stdin_path = os.path.join(self._get_local_tmp_dir(), 'STDIN')
            log.debug('dumping stdin to local file %s' % stdin_path)
            with open(stdin_path, 'w') as stdin_file:
                for line in self._stdin:
                    # catch missing newlines (this often happens with test data)
                    if not line.endswith('\n'):
                        line += '\n'
                    stdin_file.write(line)

            self._stdin_path = stdin_path

        return self._stdin_path

    def _create_mrjob_tar_gz(self):
        """Make a tarball of the mrjob library, without .pyc or .pyo files,
        and return its path. This will also set self._mrjob_tar_gz_path

        It's safe to call this method multiple times (we'll only create
        the tarball once.)
        """
        if self._mrjob_tar_gz_path is None:
            # find mrjob library
            import mrjob

            if not os.path.basename(mrjob.__file__).startswith('__init__.'):
                raise Exception(
                    "Bad path for mrjob library: %s; can't bootstrap mrjob",
                    mrjob.__file__)

            mrjob_dir = os.path.dirname(mrjob.__file__) or '.'

            tar_gz_path = os.path.join(
                self._get_local_tmp_dir(), 'mrjob.tar.gz')

            def filter_path(path):
                filename = os.path.basename(path)
                return not(file_ext(filename).lower() in ('.pyc', '.pyo') or
                    # filter out emacs backup files
                    filename.endswith('~') or
                    # filter out emacs lock files
                    filename.startswith('.#') or
                    # filter out MacFuse resource forks
                    filename.startswith('._'))

            log.debug('archiving %s -> %s as %s' % (
                mrjob_dir, tar_gz_path, os.path.join('mrjob', '')))
            tar_and_gzip(
                mrjob_dir, tar_gz_path, filter=filter_path, prefix='mrjob')
            self._mrjob_tar_gz_path = tar_gz_path

        return self._mrjob_tar_gz_path

    def _hadoop_conf_args(self, step_num, num_steps):
        """Build a list of extra arguments to the hadoop binary.

        This handles *cmdenv*, *hadoop_extra_args*, *hadoop_input_format*,
        *hadoop_output_format*, *jobconf*, and *partitioner*.

        This doesn't handle input, output, mappers, reducers, or uploading
        files.
        """
        assert 0 <= step_num < num_steps

        args = []

        # hadoop_extra_args
        args.extend(self._opts['hadoop_extra_args'])

        # new-style jobconf
        version = self.get_hadoop_version()
        if compat.uses_generic_jobconf(version):
            for key, value in sorted(self._opts['jobconf'].iteritems()):
                args.extend(['-D', '%s=%s' % (key, value)])

        # partitioner
        if self._partitioner:
            args.extend(['-partitioner', self._partitioner])

        # cmdenv
        for key, value in sorted(self._get_cmdenv().iteritems()):
            args.append('-cmdenv')
            args.append('%s=%s' % (key, value))

        # hadoop_input_format
        if (step_num == 0 and self._hadoop_input_format):
            args.extend(['-inputformat', self._hadoop_input_format])

        # hadoop_output_format
        if (step_num == num_steps - 1 and self._hadoop_output_format):
            args.extend(['-outputformat', self._hadoop_output_format])

        # old-style jobconf
        if not compat.uses_generic_jobconf(version):
            for key, value in sorted(self._opts['jobconf'].iteritems()):
                args.extend(['-jobconf', '%s=%s' % (key, value)])

        return args

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

        # Make sure that the base tmp dir environment variables are changed if
        # the default is changed.
        env['TMP'] = self._opts['base_tmp_dir']
        env['TMPDIR'] = self._opts['base_tmp_dir']
        env['TEMP'] = self._opts['base_tmp_dir']

        log.info('writing to %s' % output_path)

        err_path = os.path.join(self._get_local_tmp_dir(), 'sort-stderr')

        # assume we're using UNIX sort unless we know otherwise
        if (not self._sort_is_windows_sort) or len(input_paths) == 1:
            with open(output_path, 'w') as output:
                with open(err_path, 'w') as err:
                    args = ['sort'] + list(input_paths)
                    log.info('> %s' % cmd_line(args))
                    try:
                        check_call(args, stdout=output, stderr=err, env=env)
                        return
                    except CalledProcessError:
                        pass

        # Looks like we're using Windows sort
        self._sort_is_windows_sort = True

        log.info('Piping files into sort for Windows compatibility')
        with open(output_path, 'w') as output:
            with open(err_path, 'w') as err:
                args = ['sort']
                log.info('> %s' % cmd_line(args))
                proc = Popen(args, stdin=PIPE, stdout=output, stderr=err,
                             env=env)

                # shovel bytes into the sort process
                for input_path in input_paths:
                    with open(input_path, 'r') as input:
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
