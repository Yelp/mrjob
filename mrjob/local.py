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

from collections import defaultdict
import itertools
import logging
import os
import pprint
import re
import shutil
import stat
from subprocess import Popen, PIPE
import sys

from mrjob import compat
from mrjob.conf import combine_dicts, combine_local_envs
from mrjob.parse import find_python_traceback, parse_mr_job_stderr
from mrjob.runner import MRJobRunner
from mrjob.util import cmd_line, read_input, unarchive


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
        """:py:class:`~mrjob.local.LocalMRJobRunner` takes the same keyword args as :py:class:`~mrjob.runner.MRJobRunner`. However, please note:

        * *cmdenv* is combined with :py:func:`~mrjob.conf.combine_local_envs`
        * *python_bin* defaults to ``sys.executable`` (the current python interpreter)
        * *hadoop_extra_args*, *hadoop_input_format*, *hadoop_output_format*, and *hadoop_streaming_jar* are ignored because they require Java. If you need to test these, consider starting up a standalone Hadoop instance and running your job with ``-r hadoop``.
        """
        super(LocalMRJobRunner, self).__init__(**kwargs)

        self._working_dir = None
        self._prev_outfiles = []
        self._counters = []

        self._map_tasks = 1
        self._reduce_tasks = 1

        self._running_env = defaultdict(str)

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
    IGNORED_OPTS = [
        'hadoop_extra_args',
        'hadoop_input_format',
        'hadoop_output_format',
        'hadoop_streaming_jar',
    ]

    def _run(self):
        if self._opts['bootstrap_mrjob']:
            self._add_python_archive(self._create_mrjob_tar_gz() + '#')

        for ignored_opt in self.IGNORED_OPTS:
            if self._opts[ignored_opt]:
                log.warning('ignoring %s option (requires real Hadoop): %r' %
                            (ignored_opt, self._opts[ignored_opt]))

        self._create_wrapper_script()
        self._setup_working_dir()
        self._setup_output_dir()

         # process jobconf arguments
        jobconf = self._opts['jobconf']
        self._process_jobconf_args(jobconf)

        assert self._script # shouldn't be able to run if no script

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

            self._invoke_step(mapper_args, 'step-%d-mapper' % i, step_num=i,
                              env=self._get_running_env(), step_type='M',
                              num_tasks=self._map_tasks,
                              combiner_args=combiner_args)

            if 'R' in step:
                # sort the output
                self._invoke_step(['sort'], 'step-%d-mapper-sorted' % i,
                       env={'LC_ALL': 'C'}, step_num=i, step_type='S', num_tasks=1) # ignore locale

                # run the reducer
                reducer_args = (wrapper_args + [self._script['name'],
                                 '--step-num=%d' % i, '--reducer'] +
                                self._mr_job_extra_args())
                self._invoke_step(reducer_args, 'step-%d-reducer' % i, step_num=i,
                        env=self._get_running_env(), num_tasks = self._reduce_tasks, step_type='R')

        # move final output to output directory
        for i, outfile in enumerate(self._prev_outfiles):
            final_outfile = os.path.join(self._output_dir, 'part-%05d' % i)
            log.info('Moving %s -> %s' % (outfile, final_outfile))
            shutil.move(outfile, final_outfile)

    def _process_jobconf_args(self, jobconf):
        version = self.get_hadoop_version()
        if jobconf:
            for (conf_arg, value) in jobconf.iteritems():
                # Internally, use one canonical Hadoop version
                try:
                    canon_arg = compat.translate_jobconf('0.21', conf_arg)
                except KeyError:
                    # probably user-defined
                    canon_arg = conf_arg

                if canon_arg == 'mapreduce.job.maps':
                    self._map_tasks = int(value)
                    if self._map_tasks < 1:
                        raise ValueError("%s should be greater than 1" % conf_arg)
                elif canon_arg == 'mapreduce.job.reduces':
                    self._reduce_tasks = int(value)
                    if self._reduce_tasks < 1:
                        raise ValueError("%s should be greater than 1" % conf_arg)
                elif canon_arg == 'mapreduce.job.local.dir':
                    # hadoop supports multiple direcories - sticking with only one here
                    if not os.path.isdir(value):
                        raise IOError("Directory %s does not exist" % value)
                    self._working_dir = value
                else:
                    # catch all - convert . to _ and add to running env
                    name = conf_arg.replace('.', '_')
                    self._running_env[name] = value

        job_id_var = compat.translate_jobconf(version, 'mapreduce.job.id')
        self._running_env[job_id_var] = self._job_name

        archives_var = compat.translate_jobconf(version, 'mapreduce.job.cache.local.archives')
        self._running_env[archives_var] = str(self._mrjob_tar_gz_path)

    def _get_running_env(self):
        """ Converts . to _ in self._running_env and returns it
        """
        env = {}
        for (key, value) in self._running_env.iteritems():
            env[key.replace('.','_')] = value
        return env

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
            self._working_dir = os.path.join(self._get_local_tmp_dir(), 'working_dir')
            self.mkdir(self._working_dir)

        version = self.get_hadoop_version()
        local_dir_var = compat.translate_jobconf(version, 'mapreduce.job.local.dir')
        self._running_env[local_dir_var] = self._working_dir

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

        version = self.get_hadoop_version()
        output_dir_var = compat.translate_jobconf(version, 'mapreduce.task.output.dir')
        self._running_env[output_dir_var] = self._output_dir

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
        """ Split the input files into (roughly) *num_splits* files

            returns a dictionary that maps split_file names to a dictionary of
            properties:
                - original_name: the original name of the file whose data is in the split
                - start: where the split starts
                - length: the length of the split
        """
        # sanity check, if keep_sorted=True, then we should only have one file here
        assert(not keep_sorted or len(input_paths) == 1)

        # determine the size of each file split
        total_size = 0
        for input_path in input_paths:
            for path in self.ls(input_path):
                total_size += os.stat(path)[stat.ST_SIZE]
        split_size = total_size / num_splits

        # we want each file split to be as close to split_size as possible
        # we also want different input files to be in different splits
        tmp_directory = self._get_local_tmp_dir()
        file_names = defaultdict(str)

        # Helper functions:
        def create_outfile(original_name = '', start = ''):
            # create a new ouput file and initialize its dictionary of properties
            outfile_name = tmp_directory + '/input_part-%05d' % len(file_names)
            new_file = defaultdict(str)
            new_file['original_name'] = original_name
            new_file['start'] = start
            file_names[outfile_name] = new_file
            return outfile_name

        def line_generator(input_path):
            # generates lines from a given input_path, if keep_sorted is true then
            # we concatinate all lines with the same key before and yield them together
            if keep_sorted:
                # assume that input is a collection of key <tab> value pairs
                # match all non-tab characters
                re_pattern = re.compile("^(\S*)")
                for key, lines in itertools.groupby(read_input(input_path),
                                key=lambda(line): re_pattern.search(line).group(1)):
                    yield ''.join(lines)
            else:
                for line in read_input(input_path):
                    yield line

        for path in input_paths:
            # create a new split file for each new path

            # initialize file and accumulators
            outfile_name = create_outfile(path, 0)
            outfile = open(outfile_name, 'w')
            bytes_written = 0
            total_bytes = 0

            # write each line to a file as long as we are within the limit (split_size)
            for line in line_generator(path):
                if bytes_written >= split_size:
                    # new split file if we exceeded the limit
                    file_names[outfile_name]['length'] = bytes_written
                    total_bytes += bytes_written
                    outfile_name = create_outfile(path, total_bytes)
                    outfile = open(outfile_name, 'w')
                    bytes_written = 0

                outfile.write(line)
                bytes_written += len(line)

            file_names[outfile_name]['length'] = bytes_written

        return file_names

    def _subprocess_env(self, step_type, step_num, env=None):
        """Set up environment variables for a subprocess (mapper, etc.)"""
        version = self.get_hadoop_version()
        # keep the current environment because we need PATH to find binaries
        # and make PYTHONPATH work
        ismap_var = compat.translate_env(version, 'mapreduce_task_ismap')
        partition_var = compat.translate_env(version, 'mapreduce_task_partition')
        hadoop_env = {
            ismap_var: str(step_type=='M'),
            partition_var: str(step_num),
        }
        return combine_local_envs({'PYTHONPATH': os.getcwd()},
                                  os.environ,
                                  self._get_cmdenv(),
                                  env or {},
                                  hadoop_env)


    def _invoke_step(self, args, outfile_name, env=None, step_num=0,
                     num_tasks=1, step_type='M', combiner_args=None):
        """Run the given command, outputting into outfile, and reading
        from the previous outfile (or, for the first step, from our
        original output files).

        outfile is a path relative to our local tmp dir. commands are run
        inside self._working_dir

        We'll intelligently handle stderr from the process.

        :param combiner_args: If this mapper has a combiner, we need to do some extra shell wrangling, so pass the combiner arguments in separately.
        """
        env = self._subprocess_env(step_type, step_num, env)

        # decide where to get input
        if self._prev_outfiles:
            input_paths = self._prev_outfiles
        else:
            input_paths = []
            for path in self._input_paths:
                if path == '-':
                    input_paths.append(self._dump_stdin_to_local_file())
                else:
                    input_paths.append(path)

        # Start the tasks associated with the step:
        # if we need to sort, then just sort all input files into one file
        # otherwise, split the files needed for mappers and reducers
        # and setup the task environment for each
        procs = []
        self._prev_outfiles = []

        if step_type == 'S':
            # sort all the files into one main file
            # no need to split the input here
            for path in input_paths:
                args.append(os.path.abspath(path))
            proc = self._invoke_process(args, outfile_name, env)
            procs.append(proc)
        else:
            # get file splits for mappers and reducers
            keep_sorted = (step_type == 'R')
            file_splits = self._get_file_splits(input_paths, num_tasks, keep_sorted=keep_sorted)

            version = self.get_hadoop_version()
            task_id_var = compat.translate_env(version, 'mapreduce_task_id')
            task_attempt_id_var = compat.translate_env(version, 'mapreduce_task_attempt_id')

            input_file_var = compat.translate_env(version, 'mapreduce_map_input_file')
            input_start_var = compat.translate_env(version, 'mapreduce_map_input_start')
            input_length_var = compat.translate_env(version, 'mapreduce_map_input_length')

            # run the tasks
            for (task_num, file_name) in enumerate(file_splits):
                # set the task env
                # generate a task id
                mapreduce_task_id = 'task_%s_%s_%05d%d' % (self._job_name, step_type, step_num, task_num)
                mapreduce_task_attempt_id = 'attempt_%s_%s_%05d%d_0' % (self._job_name, step_type, step_num, task_num) # we only have one attempt

                task_vars = {
                    task_id_var: mapreduce_task_id,
                    task_attempt_id_var: mapreduce_task_attempt_id,
                }
                if step_type == 'M':
                    # map only jobconf environment variables
                    input_vars = {
                        input_file_var: file_splits[file_name]['original_name'],
                        input_start_var: str(file_splits[file_name]['start']),
                        input_length_var: str(file_splits[file_name]['length'])
                    }
                else:
                    input_vars = {}

                task_env = combine_local_envs(env, task_vars, input_vars)

                task_outfile = outfile_name + '_part-%05d' % task_num

                proc = self._invoke_process(args + [file_name], task_outfile,
                                            env=task_env,
                                            combiner_args=combiner_args)
                procs.append(proc)

        for proc in procs:
            self._wait_for_process(proc, step_num)

        self.print_counters([step_num+1])


    def _invoke_process(self, args, outfile_name, env, combiner_args=None):
        """invokes the process described by *args* and which writes to *outfile_name*

        :param combiner_args: If this mapper has a combiner, we need to do some extra shell wrangling, so pass the combiner arguments in separately.

        :return: dict(proc=Popen, args=[process args], write_to=file)
        """
        if combiner_args:
            log.info('> %s | sort | %s' % (cmd_line(args), cmd_line(combiner_args)))
        else:
            log.info('> %s' % cmd_line(args))

        # set up outfile
        outfile = os.path.join(self._get_local_tmp_dir(), outfile_name)
        log.info('writing to %s' % outfile)
        log.debug('')

        self._prev_outfiles.append(outfile)
        write_to = open(outfile, 'w')

        # run the process
        if combiner_args:
            command = '%s | sort | %s' % (cmd_line(args), cmd_line(combiner_args))
            proc = Popen(['-c', command],
                         stdout=write_to, stderr=PIPE,
                         cwd=self._working_dir, env=env,
                         shell=True)
        else:
            proc = Popen(args, stdout=write_to, stderr=PIPE,
                         cwd=self._working_dir, env=env)
        return {'proc': proc, 'args': args, 'write_to': write_to}

    def _wait_for_process(self, proc, step_num):
        # handle counters, status msgs, and other stuff on stderr
        stderr_lines = self._process_stderr_from_script(proc['proc'].stderr, step_num=step_num)
        tb_lines = find_python_traceback(stderr_lines)

        returncode = proc['proc'].wait()

        if returncode != 0:
            self.print_counters([step_num+1])
            # try to throw a useful exception
            if tb_lines:
                raise Exception(
                    'Command %r returned non-zero exit status %d:\n%s' %
                    (proc['args'], returncode, ''.join(tb_lines)))
            else:
                raise Exception(
                    'Command %r returned non-zero exit status %d: %s' %
                    (proc['args'], returncode))

        # flush file descriptors
        proc['write_to'].flush()

    def _process_stderr_from_script(self, stderr, step_num=0):
        """Handle stderr a line at time:

        - for counter lines, store counters
        - for status message, log the status change
        - for all other lines, log an error, and yield the lines
        """
        for line in stderr:
            # just pass one line at a time to parse_mr_job_stderr(),
            # so we can print error and status messages in realtime
            parsed = parse_mr_job_stderr([line], counters=self._counters[step_num])

            # in practice there's only going to be at most one line in
            # one of these lists, but the code is cleaner this way
            for status in parsed['statuses']:
                log.info('status: %s' % status)

            for line in parsed['other']:
                log.error('STDERR: %s' % line.rstrip('\n'))
                yield line

    def counters(self):
        return self._counters

    def get_hadoop_version(self):
        return self._opts['hadoop_version']
