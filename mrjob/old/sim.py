# -*- coding: utf-8 -*-
# Copyright 2009-2013 Yelp and Contributors
# Copyright 2015-2017 Yelp
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
import itertools
import logging
import os
import shutil
import stat

from mrjob.compat import jobconf_from_dict
from mrjob.compat import translate_jobconf
from mrjob.compat import translate_jobconf_for_all_versions
from mrjob.conf import combine_local_envs
from mrjob.logs.counters import _format_counters
from mrjob.options import _allowed_keys
from mrjob.options import _combiners
from mrjob.options import _deprecated_aliases
from mrjob.runner import MRJobRunner
from mrjob.runner import RunnerOptionStore
from mrjob.util import read_input
from mrjob.util import unarchive


log = logging.getLogger(__name__)


class SimRunnerOptionStore(RunnerOptionStore):
    # these are the same for 'local' and 'inline' runners
    ALLOWED_KEYS = _allowed_keys('local')
    COMBINERS = _combiners('local')
    DEPRECATED_ALIASES = _deprecated_aliases('local')


class SimMRJobRunner(MRJobRunner):
    """Abstract base class for runners for testing jobs in development

    The inline and local runners inherit from this class so functionality
    common to them has been moved here.:py:method:`_run_step` must be overriden
    by classes that extend SimMRJobRunner

    :py:class:`LocalMRJobRunner` and :py:class:`InlineMRJobRunner` simulate
    the following jobconf variables (and their Hadoop 1 equivalents):

    * ``mapreduce.job.cache.archives`` (``mapred.cache.archives``)
    * ``mapreduce.job.cache.files`` (``mapred.cache.files``)
    * ``mapreduce.job.cache.local.archives`` (``mapred.cache.localArchives``)
    * ``mapreduce.job.cache.local.files`` (``mapred.cache.localFiles``)
    * ``mapreduce.job.id`` (``mapred.job.id``)
    * ``mapreduce.job.local.dir`` (``job.local.dir``)
    * ``mapreduce.map.input.file`` (``map.input.file``)
    * ``mapreduce.map.input.length`` (``map.input.length``)
    * ``mapreduce.map.input.start`` (``map.input.start``)
    * ``mapreduce.task.attempt.id`` (``mapred.task.id``)
    * ``mapreduce.task.id`` (``mapred.tip.id``)
    * ``mapreduce.task.ismap`` (``mapred.task.is.map``)
    * ``mapreduce.task.output.dir`` (``mapred.work.output.dir``)
    * ``mapreduce.task.partition`` (``mapred.task.partition``)

    Your job can read these from the environment using
    :py:func:`~mrjob.compat.jobconf_from_env()`.

    If you specify *hadoop_version*, we'll only simulate environment variables
    for that version of Hadoop.
    """
    OPTION_STORE_CLASS = SimRunnerOptionStore

    # try to run at least two tasks to catch bugs
    _DEFAULT_MAP_TASKS = 2
    _DEFAULT_REDUCE_TASKS = 2

    # keyword arguments that we ignore because they require real Hadoop.
    # We look directly at self._<kwarg_name> because they aren't in
    # self._opts
    _IGNORED_HADOOP_ATTRS = [
        '_hadoop_input_format',
        '_hadoop_output_format',
        '_partitioner',
        '_step_output_dir',
    ]

    # options that we ignore becaue they require real Hadoop.
    _IGNORED_HADOOP_OPTS = [
        'libjars',
    ]

    def __init__(self, **kwargs):
        super(SimMRJobRunner, self).__init__(**kwargs)
        self._prev_outfiles = []
        self._counters = []

    def _warn_ignored_opts(self):
        """ If the user has provided options that are not supported
        by the dev runners log warnings for each of the ignored options
        """
        for ignored_attr in self._IGNORED_HADOOP_ATTRS:
            value = getattr(self, ignored_attr)
            if value is not None:
                log.warning(
                    'ignoring %s keyword arg (requires real Hadoop): %r' %
                    (ignored_attr[1:], value))

        for ignored_opt in self._IGNORED_HADOOP_OPTS:
            value = self._opts.get(ignored_opt)
            if value:  # ignore [], the default value of libjars
                log.warning(
                    'ignoring %s option (requires real Hadoop): %r' %
                    (ignored_opt, value))

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

    def _setup_working_dir(self, working_dir):
        """Make a working directory with symlinks to our script and
        external files. Return name of the script"""
        log.debug('setting up working dir in %s' % working_dir)

        # create the working directory
        self.fs.mkdir(working_dir)

        files = self._working_dir_mgr.name_to_path('file').items()
        # give all our files names, and symlink or unarchive them
        for name, path in files:
            dest = os.path.join(working_dir, name)
            self._symlink_to_file_or_copy(path, dest)

        archives = self._working_dir_mgr.name_to_path('archive').items()
        for name, path in archives:
            dest = os.path.join(working_dir, name)
            log.debug('unarchiving %s -> %s' % (path, dest))
            unarchive(path, dest)

    def _setup_output_dir(self):
        if not self._output_dir:
            self._output_dir = os.path.join(
                self._get_local_tmp_dir(), 'output')

        if not os.path.isdir(self._output_dir):
            log.debug('Creating output directory %s' % self._output_dir)
            self.fs.mkdir(self._output_dir)

    def _check_step_works_with_runner(self, step_dict):
        """ Raise an exception if the runner cannot run this step

        Implemented by :py:class:`mrjob.inline.InlineMRJobRunner`
        """
        pass

    def _run(self):
        self._warn_ignored_opts()
        _error_on_bad_paths(self.fs, self._input_paths)
        self._create_setup_wrapper_script(local=True)
        self._setup_output_dir()

        # run mapper, combiner, sort, reducer for each step
        for step_num, step in enumerate(self._get_steps()):
            log.info('Running step %d of %d...' % (
                step_num + 1, self._num_steps()))

            self._check_step_works_with_runner(step)
            self._counters.append({})

            self._invoke_step(step_num, 'mapper')

            if 'reducer' in step:
                # sort the output. Treat this as a mini-step for the purpose
                # of self._prev_outfiles
                sort_output_path = os.path.join(
                    self._get_local_tmp_dir(),
                    'step-%04d-mapper-sorted' % step_num)

                self._invoke_sort(self._step_input_paths(), sort_output_path)
                self._prev_outfiles = [sort_output_path]

                # run the reducer
                self._invoke_step(step_num, 'reducer')

        # move final output to output directory
        for i, outfile in enumerate(self._prev_outfiles):
            final_outfile = os.path.join(self._output_dir, 'part-%05d' % i)
            log.debug('Moving %s -> %s' % (outfile, final_outfile))
            shutil.move(outfile, final_outfile)

    def _invoke_step(self, step_num, step_type):
        """Run the mapper or reducer for the given step.
        """
        step = self._get_step(step_num)

        if step['type'] != 'streaming':
            raise Exception("LocalMRJobRunner cannot run %s steps" %
                            step['type'])

        jobconf = self._jobconf_for_step(step_num)

        outfile_prefix = 'step-%04d-%s' % (step_num, step_type)

        # allow setting number of tasks from jobconf
        if step_type == 'reducer':
            num_tasks = int(jobconf_from_dict(
                jobconf, 'mapreduce.job.reduces', self._DEFAULT_REDUCE_TASKS))
        else:
            num_tasks = int(jobconf_from_dict(
                jobconf, 'mapreduce.job.maps', self._DEFAULT_MAP_TASKS))

        # get file splits for mappers and reducers
        keep_sorted = (step_type == 'reducer')
        file_splits = self._get_file_splits(
            self._step_input_paths(), num_tasks, keep_sorted=keep_sorted)

        # since we have grapped the files from the _prev_outfiles as input
        # to this step reset _prev_outfiles
        self._prev_outfiles = []

        # Start the tasks associated with the step:
        # if we need to sort, then just sort all input files into one file
        # otherwise, split the files needed for mappers and reducers
        # and setup the task environment for each

        # The correctly-ordered list of task_num, file_name pairs
        file_tasks = sorted([
            (t['task_num'], file_name) for file_name, t
            in file_splits.items()], key=lambda t: t[0])

        for task_num, input_path in file_tasks:
            # make a new working_dir for each task
            working_dir = os.path.join(
                self._get_local_tmp_dir(),
                'job_local_dir', str(step_num), step_type, str(task_num))
            self._setup_working_dir(working_dir)

            log.debug("File name %s" % input_path)
            # setup environment variables
            split_kwargs = {}
            if step_type == 'mapper':
                # mappers have extra file split info
                split_kwargs = dict(
                    input_file=file_splits[input_path]['orig_name'],
                    input_start=file_splits[input_path]['start'],
                    input_length=file_splits[input_path]['length'])

            env = self._subprocess_env(
                step_num, step_type, task_num, working_dir, **split_kwargs)

            output_path = os.path.join(
                self._get_local_tmp_dir(),
                outfile_prefix + '_part-%05d' % task_num)
            log.debug('Writing to %s' % output_path)

            self._run_step(step_num, step_type, input_path, output_path,
                           working_dir, env)

            self._prev_outfiles.append(output_path)

        self._per_step_runner_finish(step_num)
        counters = self._counters[step_num]
        if counters:
            log.info(_format_counters(counters))

    def _run_step(self, step_num, step_type, input_path, output_path,
                  working_dir, env):
        """ Runner specific per step method
        Inline and local runners override this method
        """
        raise NotImplementedError("Subclass must implement this method")

    def _per_step_runner_finish(self, step_num):
        """ Runner specific method to be executed to mark the step completion.
        Only the local runner implements this method
        """
        pass

    def _get_file_splits(self, input_paths, num_splits, keep_sorted=False):
        """ Split the input files into (roughly) *num_splits* files. Gzipped
        files are not split, but each gzipped file counts as one split.

        :param input_paths: Iterable of paths to be split
        :param num_splits: Number of splits to target
        :param keep_sorted: If True, group lines by key

        Returns a dictionary that maps split_file names to a dictionary of
        properties:

        * *orig_name*: the original name of the file whose data is in
                          the split
        * *start*: where the split starts
        * *length*: the length of the split
        """
        # sanity check: if keep_sorted is True, we should only have one file
        assert(not keep_sorted or len(input_paths) == 1)

        file_names = {}
        input_paths_to_split = []

        # Each file is assigned a 'task number' as if coming from some previous
        # task. The task number is used to choose the split file name, and
        # sometimes the file name of the sorted split. This is done so that
        # when the output files are combined after the final step, they are in
        # sorted order due to already being lexicographically sorted.

        for input_path in input_paths:
            for path in self.fs.ls(input_path):
                if path.endswith('.gz'):
                    # do not split compressed files
                    absolute_path = os.path.abspath(path)
                    file_names[absolute_path] = {
                        'orig_name': absolute_path,
                        'start': 0,
                        'task_num': len(file_names),
                        'length': os.stat(absolute_path)[stat.ST_SIZE],
                    }
                    # this counts as "one split"
                    num_splits -= 1
                else:
                    # do split uncompressed files
                    input_paths_to_split.append(path)

        # exit early if no uncompressed files given
        if not input_paths_to_split:
            return file_names

        # account for user giving fewer splits than there are compressed files
        num_splits = max(num_splits, 1)

        # determine the size of each file split
        total_size = 0
        for input_path in input_paths_to_split:
            for path in self.fs.ls(input_path):
                total_size += os.stat(path)[stat.ST_SIZE]
        split_size = total_size / num_splits

        # we want each file split to be as close to split_size as possible
        # we also want different input files to be in different splits
        tmp_directory = self._get_local_tmp_dir()

        # Helper functions:
        def create_outfile(orig_name='', start=''):
            # create a new output file and initialize its properties dict
            task_num = len(file_names)
            outfile_name = os.path.join(tmp_directory,
                                        'input_part-%05d' % task_num)
            new_file = {
                'orig_name': orig_name,
                'start': start,
                'task_num': task_num,
            }
            file_names[outfile_name] = new_file
            return outfile_name

        def line_group_generator(input_path):
            # Generate lines from a given input_path, if keep_sorted is True,
            # group lines by key; otherwise have one line per group
            # concatenate all lines with the same key and yield them
            # together
            if keep_sorted:
                def reducer_key(line):
                    return line.split(b'\t')[0]

                # assume that input is a collection of key <tab> value pairs
                # match all non-tab characters
                for _, lines in itertools.groupby(
                        read_input(input_path), key=reducer_key):
                    yield lines
            else:
                for line in read_input(input_path):
                    yield (line,)

        for path in input_paths_to_split:
            # create a new split file for each new path

            # initialize file and accumulators
            outfile_name = create_outfile(path, 0)
            bytes_written = 0
            total_bytes = 0
            outfile = None

            try:
                outfile = open(outfile_name, 'wb')

                # write each line to a file as long as we are within the limit
                # (split_size)
                for line_group in line_group_generator(path):
                    if bytes_written >= split_size:
                        # new split file if we exceeded the limit
                        file_names[outfile_name]['length'] = bytes_written
                        total_bytes += bytes_written

                        outfile_name = create_outfile(path, total_bytes)
                        outfile.close()
                        outfile = open(outfile_name, 'wb')

                        bytes_written = 0

                    for line in line_group:
                        outfile.write(line)
                        bytes_written += len(line)

                file_names[outfile_name]['length'] = bytes_written
            finally:
                if outfile is not None:
                    outfile.close()

        return file_names

    def _subprocess_env(self, step_num, step_type, task_num, working_dir,
                        **split_kwargs):
        """Set up environment variables for a subprocess (mapper, etc.)

        This combines, in decreasing order of priority:

        * environment variables set by the **cmdenv** option
        * **jobconf** environment variables set by our job (e.g.
          ``mapreduce.task.ismap`)
        * environment variables from **jobconf** options, translated to
          whatever version of Hadoop we're emulating
        * the current environment
        * PYTHONPATH set to current working directory

        We use :py:func:`~mrjob.conf.combine_local_envs`, so ``PATH``
        environment variables are handled specially.
        """
        user_jobconf = self._jobconf_for_step(step_num)

        simulated_jobconf = self._simulate_jobconf_for_step(
            step_num, step_type, task_num, working_dir, **split_kwargs)

        def to_env(jobconf):
            return dict((k.replace('.', '_'), str(v))
                        for k, v in jobconf.items())

        # keep the current environment because we need PATH to find binaries
        # and make PYTHONPATH work
        return combine_local_envs(os.environ,
                                  to_env(user_jobconf),
                                  to_env(simulated_jobconf),
                                  self._opts['cmdenv'])

    def _simulate_jobconf_for_step(
            self, step_num, step_type, task_num, working_dir,
            input_file=None, input_start=None, input_length=None):
        """Simulate jobconf variables set by Hadoop to indicate input
        files, files uploaded, working directory, etc. for a particular step.

        Returns a dictionary mapping jobconf variable name
        (e.g. ``'mapreduce.map.input.file'``) to its value, which is always
        a string.
        """
        # By convention, we use the newer (Hadoop 2) jobconf names and
        # translate them at the very end.
        j = {}

        j['mapreduce.job.id'] = self._job_key
        j['mapreduce.task.output.dir'] = self._output_dir
        j['mapreduce.job.local.dir'] = working_dir
        # archives and files for jobconf
        cache_archives = []
        cache_files = []
        cache_local_archives = []
        cache_local_files = []

        files = self._working_dir_mgr.name_to_path('file').items()
        for name, path in files:
            cache_files.append('%s#%s' % (path, name))
            cache_local_files.append(os.path.join(working_dir, name))

        archives = self._working_dir_mgr.name_to_path('archive').items()
        for name, path in archives:
            cache_archives.append('%s#%s' % (path, name))
            cache_local_archives.append(os.path.join(working_dir, name))

        # TODO: could add mtime info here too (e.g.
        # mapreduce.job.cache.archives.timestamps) here too
        j['mapreduce.job.cache.files'] = (','.join(cache_files))
        j['mapreduce.job.cache.local.files'] = (','.join(cache_local_files))
        j['mapreduce.job.cache.archives'] = (','.join(cache_archives))
        j['mapreduce.job.cache.local.archives'] = (
            ','.join(cache_local_archives))

        # task and attempt IDs
        # TODO: these are a crappy imitation of task/attempt IDs (see #1254)
        j['mapreduce.task.id'] = 'task_%s_%s_%04d%d' % (
            self._job_key, step_type.lower(), step_num, task_num)
        # (we only have one attempt)
        j['mapreduce.task.attempt.id'] = 'attempt_%s_%s_%04d%d_0' % (
            self._job_key, step_type.lower(), step_num, task_num)

        # not actually sure what's correct for combiners here. It'll definitely
        # be true if we're just using pipes to simulate a combiner though
        j['mapreduce.task.ismap'] = str(
            step_type in ('mapper', 'combiner')).lower()

        j['mapreduce.task.partition'] = str(task_num)

        if input_file is not None:
            j['mapreduce.map.input.file'] = input_file
        if input_start is not None:
            j['mapreduce.map.input.start'] = str(input_start)
        if input_length is not None:
            j['mapreduce.map.input.length'] = str(input_length)

        version = self.get_hadoop_version()
        if version:
            # translate to correct version
            j = dict((translate_jobconf(k, version), v) for k, v in j.items())
        else:
            # use all versions
            j = dict((variant, v)
                     for k, v in j.items()
                     for variant in translate_jobconf_for_all_versions(k))

        return j

    def get_hadoop_version(self):
        return self._opts['hadoop_version']

    def _step_input_paths(self):
        """Decide where to get input for a step. Dump stdin to a temp file
        if need be."""
        if self._prev_outfiles:
            return self._prev_outfiles
        else:
            return self._get_input_paths()

    def counters(self):
        return self._counters


def _error_on_bad_paths(fs, paths):
    """Raise an exception if there is not at least one valid path.

    :param fs: a filesystem used to check if a path exists
    :type fs: :class:`mrjob.fs.base.Filesystem`
    :param paths: a list of file paths or globs
    :type paths: list of strings
    :raises ValueError: if there are no valid paths
    """
    for path in paths:
        if path == '-':
            return
        if fs.exists(path):
            return

    raise ValueError("At least one valid path is required. "
                     "None found in %s" % paths)
