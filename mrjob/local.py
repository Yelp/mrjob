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
"""Run an MRJob locally by forking off a bunch of processes and piping
them together. Useful for testing."""
from __future__ import with_statement

import itertools
import logging
import os
import shutil
import stat
from subprocess import Popen
from subprocess import PIPE
import sys

from mrjob.compat import translate_jobconf
from mrjob.conf import combine_dicts
from mrjob.conf import combine_local_envs
from mrjob.parse import find_python_traceback
from mrjob.parse import parse_mr_job_stderr
from mrjob.runner import MRJobRunner
from mrjob.util import cmd_line
from mrjob.util import read_input
from mrjob.util import unarchive


log = logging.getLogger('mrjob.local')

DEFAULT_MAP_TASKS = 2
DEFAULT_REDUCE_TASKS = 2


class LocalMRJobRunner(MRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` locally, for testing
    purposes.

    This is the default way of running jobs; we assume you'll spend some
    time debugging your job before you're ready to run it on EMR or
    Hadoop.

    It's rare to need to instantiate this class directly (see
    :py:meth:`~LocalMRJobRunner.__init__` for details).

    :py:class:`LocalMRJobRunner` simulates the following jobconf variables:

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

    :py:class:`LocalMRJobRunner` adds the current working directory to the
    subprocesses' :envvar:`PYTHONPATH`, so if you're using it to test an EMR
    job locally, be aware that it may see more Python modules than will
    actaully be uploaded. This behavior may change in the future.
    """

    alias = 'local'

    def __init__(self, **kwargs):
        """Arguments to this constructor may also appear in :file:`mrjob.conf`
        under ``runners/local``.

        :py:class:`~mrjob.local.LocalMRJobRunner`'s constructor takes the
        same keyword args as
        :py:class:`~mrjob.runner.MRJobRunner`. However, please note:

        * *cmdenv* is combined with :py:func:`~mrjob.conf.combine_local_envs`
        * *python_bin* defaults to ``sys.executable`` (the current python
          interpreter)
        * *hadoop_extra_args*, *hadoop_input_format*, *hadoop_output_format*,
          *hadoop_streaming_jar*, and *partitioner* are ignored because they
          require Java. If you need to test these, consider starting up a
          standalone Hadoop instance and running your job with ``-r hadoop``.
        """
        super(LocalMRJobRunner, self).__init__(**kwargs)

        self._working_dir = None
        self._prev_outfiles = []
        self._counters = []

        self._map_tasks = DEFAULT_MAP_TASKS
        self._reduce_tasks = DEFAULT_REDUCE_TASKS

        # jobconf variables set by our own job (e.g. files "uploaded")
        #
        # By convention, we use the Hadoop 0.21 (newer) versions of the
        # jobconf variables internally (they get auto-translated before
        # running the job)
        self._internal_jobconf = {}

    @classmethod
    def _default_opts(cls):
        """A dictionary giving the default value of options."""
        return combine_dicts(super(LocalMRJobRunner, cls)._default_opts(), {
            # prefer whatever interpreter we're currently using
            'python_bin': [sys.executable or 'python'],
        })

    @classmethod
    def _opts_combiners(cls):
        # on windows, PYTHONPATH should use ;, not :
        return combine_dicts(
            super(LocalMRJobRunner, cls)._opts_combiners(),
            {'cmdenv': combine_local_envs})

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

    def _run(self):
        if self._opts['bootstrap_mrjob']:
            self._add_python_archive(self._create_mrjob_tar_gz() + '#')

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

        self._create_wrapper_script()
        self._setup_working_dir()
        self._setup_output_dir()

         # process jobconf arguments
        jobconf = self._opts['jobconf']
        self._process_jobconf_args(jobconf)

        assert self._script  # shouldn't be able to run if no script

        wrapper_args = self._opts['python_bin']
        if self._wrapper_script:
            wrapper_args = (self._opts['python_bin'] +
                            [self._wrapper_script['name']] +
                            wrapper_args)

        # run mapper, combiner, sort, reducer for each step
        for i, step in enumerate(self._get_steps()):
            self._counters.append({})
            # run the mapper
            mapper_args = (wrapper_args + [self._script['name'],
                            '--step-num=%d' % i, '--mapper'] +
                           self._mr_job_extra_args())
            combiner_args = []
            if 'C' in step:
                combiner_args = (wrapper_args + [self._script['name'],
                                 '--step-num=%d' % i, '--combiner'] +
                                 self._mr_job_extra_args())

            self._invoke_step(mapper_args, 'step-%d-mapper' % i,
                              step_num=i, step_type='M',
                              num_tasks=self._map_tasks,
                              combiner_args=combiner_args)

            if 'R' in step:
                # sort the output. Treat this as a mini-step for the purpose
                # of self._prev_outfiles
                sort_output_path = os.path.join(
                    self._get_local_tmp_dir(), 'step-%d-mapper-sorted' % i)
                self._invoke_sort(self._step_input_paths(), sort_output_path)
                self._prev_outfiles = [sort_output_path]

                # run the reducer
                reducer_args = (wrapper_args + [self._script['name'],
                                 '--step-num=%d' % i, '--reducer'] +
                                self._mr_job_extra_args())
                self._invoke_step(reducer_args, 'step-%d-reducer' % i,
                                  step_num=i, step_type='R',
                                  num_tasks=self._reduce_tasks)

        # move final output to output directory
        for i, outfile in enumerate(self._prev_outfiles):
            final_outfile = os.path.join(self._output_dir, 'part-%05d' % i)
            log.info('Moving %s -> %s' % (outfile, final_outfile))
            shutil.move(outfile, final_outfile)

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

    def _setup_working_dir(self):
        """Make a working directory with symlinks to our script and
        external files. Return name of the script"""
        # specify that we want to upload our script along with other files
        if self._script:
            self._script['upload'] = 'file'
        if self._wrapper_script:
            self._wrapper_script['upload'] = 'file'

        # create the working directory
        if not self._working_dir:
            self._working_dir = os.path.join(
                self._get_local_tmp_dir(), 'working_dir')
            self.mkdir(self._working_dir)

        # give all our files names, and symlink or unarchive them
        self._name_files()
        for file_dict in self._files:
            path = file_dict['path']
            name = file_dict['name']
            dest = os.path.join(self._working_dir, name)

            if file_dict.get('upload') == 'file':
                self._symlink_to_file_or_copy(path, dest)
            elif file_dict.get('upload') == 'archive':
                log.debug('unarchiving %s -> %s' % (path, dest))
                unarchive(path, dest)

    def _setup_output_dir(self):
        if not self._output_dir:
            self._output_dir = os.path.join(
                self._get_local_tmp_dir(), 'output')

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

        for input_path in input_paths:
            for path in self.ls(input_path):
                if path.endswith('.gz'):
                    # do not split compressed files
                    file_names[path] = {
                        'orig_name': path,
                        'start': 0,
                        'length': os.stat(path)[stat.ST_SIZE],
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
            outfile_name = os.path.join(tmp_directory,
                                        'input_part-%05d' % len(file_names))
            new_file = {
                'orig_name': orig_name,
                'start': start,
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

    def _step_input_paths(self):
        """Decide where to get input for a step. Dump stdin to a temp file
        if need be."""
        if self._prev_outfiles:
            return self._prev_outfiles
        else:
            input_paths = []
            for path in self._input_paths:
                if path == '-':
                    input_paths.append(self._dump_stdin_to_local_file())
                else:
                    input_paths.append(path)
            return input_paths

    def _invoke_step(self, args, outfile_name, step_num=0, num_tasks=1,
                     step_type='M', combiner_args=None):
        """Run the given command, outputting into outfile, and reading
        from the previous outfile (or, for the first step, from our
        original output files).

        outfile is a path relative to our local tmp dir. commands are run
        inside self._working_dir

        We'll intelligently handle stderr from the process.

        :param combiner_args: If this mapper has a combiner, we need to do
                              some extra shell wrangling, so pass the combiner
                              arguments in separately.
        """

        # get file splits for mappers and reducers
        keep_sorted = (step_type == 'R')
        file_splits = self._get_file_splits(
            self._step_input_paths(), num_tasks, keep_sorted=keep_sorted)

        # Start the tasks associated with the step:
        # if we need to sort, then just sort all input files into one file
        # otherwise, split the files needed for mappers and reducers
        # and setup the task environment for each
        all_proc_dicts = []
        self._prev_outfiles = []

        for task_num, file_name in enumerate(file_splits):

            # setup environment variables
            if step_type == 'M':
                env = self._subprocess_env(
                    step_type, step_num, task_num,
                    # mappers have extra file split info
                    input_file=file_splits[file_name]['orig_name'],
                    input_start=file_splits[file_name]['start'],
                    input_length=file_splits[file_name]['length'])
            else:
                env = self._subprocess_env(step_type, step_num, task_num)

            task_outfile = outfile_name + '_part-%05d' % task_num

            proc_dicts = self._invoke_process(args + [file_name], task_outfile,
                                              env=env,
                                              combiner_args=combiner_args)
            all_proc_dicts.extend(proc_dicts)

        for proc_dict in all_proc_dicts:
            self._wait_for_process(proc_dict, step_num)

        self.print_counters([step_num + 1])

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
        return combine_local_envs({'PYTHONPATH': os.getcwd()},
                                  os.environ,
                                  jobconf_env,
                                  internal_jobconf_env,
                                  self._get_cmdenv())

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
        j['mapreduce.job.local.dir'] = self._working_dir
        j['mapreduce.task.output.dir'] = self._output_dir

        # archives and files for jobconf
        cache_archives = []
        cache_files = []
        cache_local_archives = []
        cache_local_files = []

        for file_dict in self._files:
            path = file_dict['path']
            name = file_dict['name']
            dest = os.path.join(self._working_dir, name)

            if file_dict.get('upload') == 'file':
                cache_files.append('%s#%s' % (path, name))
                cache_local_files.append(dest)

            elif file_dict.get('upload') == 'archive':
                cache_archives.append('%s#%s' % (path, name))
                cache_local_archives.append(dest)

        # could add mtime info here too (e.g.
        # mapreduce.job.cache.archives.timestamps) here too, though we should
        # probably cache that in self._files
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
        j['mapreduce.task.ismap'] = str(step_type in ('M', 'C')).lower()

        j['mapreduce.task.partition'] = str(task_num)

        if input_file is not None:
            j['mapreduce.map.input.file'] = input_file
        if input_start is not None:
            j['mapreduce.map.input.start'] = str(input_start)
        if input_length is not None:
            j['mapreduce.map.input.length'] = str(input_length)

        return j

    def _invoke_process(self, args, outfile_name, env, combiner_args=None):
        """invoke the process described by *args* and write to *outfile_name*

        :param combiner_args: If this mapper has a combiner, we need to do
                              some extra shell wrangling, so pass the combiner
                              arguments in separately.

        :return: dict(proc=Popen, args=[process args], write_to=file)
        """
        if combiner_args:
            log.info('> %s | sort | %s' %
                     (cmd_line(args), cmd_line(combiner_args)))
        else:
            log.info('> %s' % cmd_line(args))

        # set up outfile
        outfile = os.path.join(self._get_local_tmp_dir(), outfile_name)
        log.info('writing to %s' % outfile)

        self._prev_outfiles.append(outfile)

        with open(outfile, 'w') as write_to:
            if combiner_args:
                # set up a pipeline: mapper | sort | combiner
                mapper_proc = Popen(args, stdout=PIPE, stderr=PIPE,
                                    cwd=self._working_dir, env=env)

                sort_proc = Popen(['sort'], stdin=mapper_proc.stdout,
                                  stdout=PIPE, stderr=PIPE,
                                  cwd=self._working_dir, env=env)

                combiner_proc = Popen(combiner_args, stdin=sort_proc.stdout,
                                      stdout=write_to, stderr=PIPE,
                                      cwd=self._working_dir, env=env)

                # this process shouldn't read from the pipes
                mapper_proc.stdout.close()
                sort_proc.stdout.close()

                return [
                    {'proc': mapper_proc, 'args': args},
                    {'proc': sort_proc, 'args': ['sort']},
                    {'proc': combiner_proc, 'args': combiner_args},
                ]
            else:
                # just run the mapper process
                proc = Popen(args, stdout=write_to, stderr=PIPE,
                             cwd=self._working_dir, env=env)
                return [{'proc': proc, 'args': args}]

    def _wait_for_process(self, proc_dict, step_num):
        # handle counters, status msgs, and other stuff on stderr
        stderr_lines = self._process_stderr_from_script(
            proc_dict['proc'].stderr, step_num=step_num)
        tb_lines = find_python_traceback(stderr_lines)

        returncode = proc_dict['proc'].wait()

        if returncode != 0:
            self.print_counters([step_num + 1])
            # try to throw a useful exception
            if tb_lines:
                raise Exception(
                    'Command %r returned non-zero exit status %d:\n%s' %
                    (proc_dict['args'], returncode, ''.join(tb_lines)))
            else:
                raise Exception(
                    'Command %r returned non-zero exit status %d' %
                    (proc_dict['args'], returncode))

    def _process_stderr_from_script(self, stderr, step_num=0):
        """Handle stderr a line at time:

        * for counter lines, store counters
        * for status message, log the status change
        * for all other lines, log an error, and yield the lines
        """
        for line in stderr:
            # just pass one line at a time to parse_mr_job_stderr(),
            # so we can print error and status messages in realtime
            parsed = parse_mr_job_stderr(
                [line], counters=self._counters[step_num])

            # in practice there's only going to be at most one line in
            # one of these lists, but the code is cleaner this way
            for status in parsed['statuses']:
                log.info('status: %s' % status)

            for line in parsed['other']:
                log.error('STDERR: %s' % line.rstrip('\r\n'))
                yield line

    def counters(self):
        return self._counters

    def get_hadoop_version(self):
        return self._opts['hadoop_version']
