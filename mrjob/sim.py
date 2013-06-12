# -*- coding: utf-8 -*-
# Copyright 2009-2013 Yelp and Contributors
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
from __future__ import with_statement

import itertools
import logging
import os
import shutil
import stat

from mrjob.compat import translate_jobconf
from mrjob.conf import combine_dicts
from mrjob.conf import combine_local_envs
from mrjob.runner import MRJobRunner
from mrjob.runner import RunnerOptionStore
from mrjob.util import read_input
from mrjob.util import unarchive


log = logging.getLogger(__name__)


class SimRunnerOptionStore(RunnerOptionStore):

    COMBINERS = combine_dicts(RunnerOptionStore.COMBINERS, {
        'cmdenv': combine_local_envs,
    })

    def default_options(self):
        # don't bootstrap mrjob by default when running locally
        super_opts = super(SimRunnerOptionStore, self).default_options()
        return combine_dicts(super_opts, {
            'bootstrap_mrjob': False,
        })


class SimMRJobRunner(MRJobRunner):
    """Abstract base class for runners for testing jobs in development

    The inline and local runners inherit from this class so functionality
    common to them has been moved here.:py:method:`run_step` must be overriden
    by classes that extend SimMRJobRunner

    :py:class:`LocalMRJobRunner` and :py:class:`InlineMRJobRunner` simulate
    the following jobconf variables:

    * ``mapreduce.job.cache.archives``
    * ``mapreduce.job.cache.files``
    * ``mapreduce.job.cache.local.archives``
    * ``mapreduce.job.cache.local.files``
    * ``mapreduce.job.id``
    * ``mapreduce.job.local.dir``
    * ``mapreduce.map.input.file``
    * ``mapreduce.map.input.length``
    * ``mapreduce.map.input.start``
    * ``mapreduce.task.attempt.id``
    * ``mapreduce.task.id``
    * ``mapreduce.task.ismap``
    * ``mapreduce.task.output.dir``
    * ``mapreduce.task.partition``

    If you specify *hadoop_version* <= 0.21, the simulated environment
    variables will change to use the names corresponding with the older Hadoop
    version.

    """

    # options that we ignore because they require real Hadoop
    IGNORED_HADOOP_OPTS = [
        'hadoop_extra_args',
        'hadoop_streaming_jar',
    ]

    # keyword arguments that we ignore that are stored directly in
    # self._<kwarg_name> because they aren't configurable from mrjob.conf
    # use the version with the underscore to better support grepping our code
    IGNORED_HADOOP_ATTRS = [
        '_hadoop_input_format',
        '_hadoop_output_format',
        '_partitioner',
    ]

    def __init__(self, **kwargs):
        super(SimMRJobRunner, self).__init__(**kwargs)
        self._working_dir = None
        self._prev_outfiles = []
        self._counters = []

    def warn_ignored_opts(self):
        """ If the user has provided options that are not supported
        by the dev runners log warnings for each of the ignored options
        """
        for ignored_opt in self.IGNORED_HADOOP_OPTS:
            if self._opts[ignored_opt]:
                log.warning('ignoring %s option (requires real Hadoop): %r' %
                            (ignored_opt, self._opts[ignored_opt]))

        for ignored_attr in self.IGNORED_HADOOP_ATTRS:
            value = getattr(self, ignored_attr)
            if value is not None:
                log.warning(
                    'ignoring %s keyword arg (requires real Hadoop): %r' %
                    (ignored_attr[1:], value))

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

    def _setup_working_dir(self):
        """Make a working directory with symlinks to our script and
        external files. Return name of the script"""
        # create the working directory
        if not self._working_dir:
            self._working_dir = os.path.join(
                self._get_local_tmp_dir(), 'working_dir')
            self.mkdir(self._working_dir)

        files = self._working_dir_mgr.name_to_path('file').iteritems()
        # give all our files names, and symlink or unarchive them
        for name, path in files:
            dest = os.path.join(self._working_dir, name)
            self._symlink_to_file_or_copy(path, dest)

        archives = self._working_dir_mgr.name_to_path('archive').iteritems()
        for name, path in archives:
            dest = os.path.join(self._working_dir, name)
            log.debug('unarchiving %s -> %s' % (path, dest))
            unarchive(path, dest)

    def _setup_output_dir(self):
        if not self._output_dir:
            self._output_dir = os.path.join(
                self._get_local_tmp_dir(), 'output')

        if not os.path.isdir(self._output_dir):
            log.debug('Creating output directory %s' % self._output_dir)
            self.mkdir(self._output_dir)

    def _check_step_works_with_runner(self, step_dict):
        """ Raise an exception if the runner cannot run this step

        Implemented by :py:class:`mrjob.inline.InlineMRJobRunner`
        """
        pass

    def _run(self):

        self.warn_ignored_opts()
        self._create_setup_wrapper_script()
        self._setup_working_dir()
        self._setup_output_dir()

         # process jobconf arguments
        jobconf = self._opts['jobconf']
        self._process_jobconf_args(jobconf)

        # run mapper, combiner, sort, reducer for each step
        for step_num, step in enumerate(self._get_steps()):
            self._check_step_works_with_runner(step)
            self._counters.append({})

            self._invoke_step(
                step, 'step-%d-mapper' % step_num, step_num, 'mapper',
                num_tasks=self._map_tasks)

            if 'reducer' in step:
                # sort the output. Treat this as a mini-step for the purpose
                # of self._prev_outfiles
                sort_output_path = os.path.join(
                    self._get_local_tmp_dir(),
                    'step-%d-mapper-sorted' % step_num)

                self._invoke_sort(self._step_input_paths(), sort_output_path)
                self._prev_outfiles = [sort_output_path]

                # run the reducer
                self._invoke_step(
                    step, 'step-%d-reducer' % step_num, step_num, 'reducer',
                    num_tasks=self._reduce_tasks)

        # move final output to output directory
        for i, outfile in enumerate(self._prev_outfiles):
            final_outfile = os.path.join(self._output_dir, 'part-%05d' % i)
            log.info('Moving %s -> %s' % (outfile, final_outfile))
            shutil.move(outfile, final_outfile)

    def _invoke_step(self, step_dict, outfile_name, step_num, step_type,
                     num_tasks=1):
        """Run the given command, outputting into outfile, and reading
        from the previous outfile (or, for the first step, from our
        original output files).

        outfile is a path relative to our local tmp dir. commands are run
        inside self._working_dir

        We'll intelligently handle stderr from the process.
        :param step_dict: description of a step see step.py description
        :param outfile_name: name of the output file to store the results
        of executing this step
        :param step_num: number of the step
        :step_type: mapper/reducer
        :num_tasks: number of mapper or reducer tasks to be launched to execute
        this step
        """

        if step_dict['type'] != 'streaming':
            raise Exception("LocalMRJobRunner cannot run %s steps" %
                            step_dict['type'])

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

        for task_num, file_name in file_tasks:
            log.debug("File name %s" % file_name)
            # setup environment variables
            if step_type == 'mapper':
                env = self._subprocess_env(
                    step_type, step_num, task_num,
                    # mappers have extra file split info
                    input_file=file_splits[file_name]['orig_name'],
                    input_start=file_splits[file_name]['start'],
                    input_length=file_splits[file_name]['length'])
            else:
                env = self._subprocess_env(step_type, step_num, task_num)

            task_outfile = outfile_name + '_part-%05d' % task_num

            log.info('writing to %s' % task_outfile)
            outfile = os.path.join(self._get_local_tmp_dir(), task_outfile)
            self._prev_outfiles.append(outfile)

            self.run_step(step_dict, file_name,
                          outfile, step_num, step_type, env)

        self.per_step_runner_finish(step_num)
        self.print_counters([step_num + 1])

    def run_step(self, step_dict, file_name,
                 outfile_name, step_num, step_type, env):
        """ Runner specific per step method
        Inline and local runners override this method
        """
        raise NotImplementedError("Subclass must implement this method")

    def per_step_runner_finish(self, step_num):
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
            for path in self.ls(input_path):
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
            for path in self.ls(input_path):
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
                    return line.split('\t')[0]

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
                outfile = open(outfile_name, 'w')

                # write each line to a file as long as we are within the limit
                # (split_size)
                for line_group in line_group_generator(path):
                    if bytes_written >= split_size:
                        # new split file if we exceeded the limit
                        file_names[outfile_name]['length'] = bytes_written
                        total_bytes += bytes_written

                        outfile_name = create_outfile(path, total_bytes)
                        outfile.close()
                        outfile = open(outfile_name, 'w')

                        bytes_written = 0

                    for line in line_group:
                        outfile.write(line)
                        bytes_written += len(line)

                file_names[outfile_name]['length'] = bytes_written
            finally:
                if not outfile is None:
                    outfile.close()

        return file_names

    def _process_jobconf_args(self, jobconf):
        if jobconf:
            for (conf_arg, value) in jobconf.iteritems():
                # Internally, use one canonical Hadoop version
                canon_arg = translate_jobconf(conf_arg, '0.21')

                if canon_arg == 'mapreduce.job.maps':
                    self._map_tasks = int(value)
                    if self._map_tasks < 1:
                        raise ValueError(
                            '%s should be at least 1' % conf_arg)
                elif canon_arg == 'mapreduce.job.reduces':
                    self._reduce_tasks = int(value)
                    if self._reduce_tasks < 1:
                        raise ValueError('%s should be at least 1' % conf_arg)
                elif canon_arg == 'mapreduce.job.local.dir':
                    # Hadoop supports multiple direcories. Sticking with only
                    # one here
                    if not os.path.isdir(value):
                        raise IOError("Directory %s does not exist" % value)
                    self._working_dir = value

    def _subprocess_env(self, step_type, step_num, task_num, input_file=None,
                        input_start=None, input_length=None):
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
        version = self.get_hadoop_version()

        jobconf_env = dict(
            (translate_jobconf(k, version).replace('.', '_'), str(v))
            for (k, v) in self._opts['jobconf'].iteritems())

        internal_jobconf = self._simulate_jobconf_for_step(
            step_type, step_num, task_num, input_file=input_file,
            input_start=input_start, input_length=input_length)

        internal_jobconf_env = dict(
            (translate_jobconf(k, version).replace('.', '_'), str(v))
            for (k, v) in internal_jobconf.iteritems())

        # keep the current environment because we need PATH to find binaries
        # and make PYTHONPATH work
        return combine_local_envs(os.environ,
                                  jobconf_env,
                                  internal_jobconf_env,
                                  self._opts['cmdenv'])

    def _simulate_jobconf_for_step(self, step_type, step_num, task_num,
        input_file=None, input_start=None, input_length=None):
        """Simulate jobconf variables set by Hadoop to indicate input
        files, files uploaded, working directory, etc. for a particular step.

        Returns a dictionary mapping jobconf variable name
        (e.g. ``'mapreduce.map.input.file'``) to its value, which is always
        a string.

        We use the newer (Hadoop 0.21+) jobconf names; these will be
        translated to the correct Hadoop version elsewhere.
        """
        j = {}  # our final results

        j['mapreduce.job.id'] = self._job_name
        j['mapreduce.task.output.dir'] = self._output_dir
        j['mapreduce.job.local.dir'] = self._working_dir
        # archives and files for jobconf
        cache_archives = []
        cache_files = []
        cache_local_archives = []
        cache_local_files = []

        files = self._working_dir_mgr.name_to_path('file').iteritems()
        for name, path in files:
            cache_files.append('%s#%s' % (path, name))
            cache_local_files.append(os.path.join(self._working_dir, name))

        archives = self._working_dir_mgr.name_to_path('archive').iteritems()
        for name, path in archives:
            cache_archives.append('%s#%s' % (path, name))
            cache_local_archives.append(os.path.join(self._working_dir, name))

        # TODO: could add mtime info here too (e.g.
        # mapreduce.job.cache.archives.timestamps) here too
        j['mapreduce.job.cache.files'] = (','.join(cache_files))
        j['mapreduce.job.cache.local.files'] = (','.join(cache_local_files))
        j['mapreduce.job.cache.archives'] = (','.join(cache_archives))
        j['mapreduce.job.cache.local.archives'] = (
            ','.join(cache_local_archives))

        # task and attempt IDs
        j['mapreduce.task.id'] = 'task_%s_%s_%05d%d' % (
            self._job_name, step_type.lower(), step_num, task_num)
        # (we only have one attempt)
        j['mapreduce.task.attempt.id'] = 'attempt_%s_%s_%05d%d_0' % (
            self._job_name, step_type.lower(), step_num, task_num)

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
