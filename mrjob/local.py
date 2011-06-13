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

from mrjob.conf import combine_dicts, combine_local_envs
from mrjob.parse import find_python_traceback, parse_mr_job_stderr
from mrjob.runner import MRJobRunner
from mrjob.util import cmd_line, read_file, unarchive


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

        # run mapper, sort, reducer for each step
        for i, step in enumerate(self._get_steps()):
            self._counters.append({})
            # run the mapper
            mapper_args = (wrapper_args + [self._script['name'],
                            '--step-num=%d' % i, '--mapper'] +
                           self._mr_job_extra_args())
            self._invoke_step(mapper_args, 'step-%d-mapper' % i, step_num=i, 
                        env=self._running_env, step_type='M', num_tasks=self._map_tasks)

            if 'R' in step:
                # sort the output
                self._invoke_step(['sort'], 'step-%d-mapper-sorted' % i,
                       env={'LC_ALL': 'C'}, step_num=i, step_type='S', num_tasks=1) # ignore locale

                # run the reducer
                reducer_args = (wrapper_args + [self._script['name'],
                                 '--step-num=%d' % i, '--reducer'] +
                                self._mr_job_extra_args())
                self._invoke_step(reducer_args, 'step-%d-reducer' % i, step_num=i, 
                        env=self._running_env, num_tasks = self._reduce_tasks, step_type='R')

        # move final output to output directory
        for i, outfile in enumerate(self._prev_outfiles):
            final_outfile = os.path.join(self._output_dir, 'part-%05d' % i)
            log.info('Moving %s -> %s' % (outfile, final_outfile))
            shutil.move(outfile, final_outfile)

    def _process_jobconf_args(self, jobconf):
        if jobconf:
            for (conf_arg, value) in jobconf.iteritems():
                if conf_arg == 'mapred.map.tasks' or conf_arg == 'mapreduce.job.maps':
                    self._map_tasks = int(value)
                    if self._map_tasks < 1:
                        raise ValueError("%s should be greater than 1" % conf_arg)
                elif conf_arg == 'mapred.reduce.tasks' or conf_arg == 'mapreduce.job.reduces':
                    self._reduce_tasks = int(value)
                    if self._reduce_tasks < 1:
                        raise ValueError("%s should be greater than 1" % conf_arg)
                elif conf_arg == 'mapred.job.local.dir' or conf_arg == 'mapreduce.job.local.dir':
                    # hadoop supports multiple direcories - sticking with only one here
                    if not os.path.isdir(value):
                        raise IOError("Directory %s does not exist" % value)
                    self._working_dir = value
                else:
                    # catch all - convert . to _ and add to running env
                    name = ""
                    for c in conf_arg:
                        if c == '.':
                            c = '_'
                        name = name + c
                    self._running_env[name] = value
                
        self._running_env['mapreduce_job_id'] = self._job_name
        self._running_env['mapreduce_job_cache_local_archives'] = str(self._mrjob_tar_gz_path)
                
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
        
        self._running_env["mapreduce_job_local_dir"] = self._working_dir

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
        
        self._running_env['mapreduce_task_output_dir'] = self._output_dir

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
            
            returns a dictionary that maps split_file names to a dictionary of properties
        """
        total_size = sum([os.stat(path)[stat.ST_SIZE] for path in input_paths])
        split_size = total_size / num_splits
         
        # we want each file split to be as close to split_size as possible
        # we also want different input files to be in different splits 
        tmp_directory = self._get_local_tmp_dir()
        file_names = defaultdict(str)
        
        def create_outfile(original_name = '', start = ''):
            outfile_name = tmp_directory + '/input_part-%05d' % len(file_names)
            new_file = defaultdict(str) 
            new_file['original_name'] = original_name
            new_file['start'] = start
            file_names[outfile_name] = new_file 
            return outfile_name
        
        if keep_sorted:
            # merge all input files into one
            if len(input_paths) == 1:
                input_file = input_paths[0]
            else:
                input_file = tmp_directory + '/sorted_input'
                args = ['sort', '-m'] + input_paths
                
                outfile = open(input_file, 'w')
                proc = Popen(args, stdout=outfile, stderr=PIPE,
                             cwd=self._working_dir, env={'LC_ALL': 'C'})
                returncode = proc.wait()
                if returncode != 0:
                    raise Exception(
                        'Command %r returned non-zero exit status %d: %s' %
                        (args, returncode))
                
            files = []
            for i in xrange(num_splits):
                outfile_name = create_outfile()
                files.append(open(outfile_name, 'w'))
            # assume that input is a collection of key <tab> value pairs
            # match all non-tab characters
            re_pattern = re.compile("^(\S*)")
            
            current_file = 0
            for key, lines in itertools.groupby(read_file(input_file), 
                            key=lambda(line): re_pattern.search(line).group(1)):
                for line in lines:
                    files[current_file].write(line)
                current_file = (current_file + 1) % num_splits
        else:
            for path in input_paths:
                # create a new split file for each new path
                outfile_name = create_outfile(path, 0)
                outfile = open(outfile_name, 'w')
                total_bytes = 0
                bytes_written = 0
                for line in read_file(path):
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

    def _invoke_step(self, args, outfile_name, env=None, step_num=0, num_tasks=1, step_type='M'):
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
            env or {}, 
            {'mapreduce_task_ismap': str(step_type=='M'), 
             'mapreduce_task_partition': str(step_num),
            })
            
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

        # get file splits
        keep_sorted = (step_type == 'R')
        file_splits = self._get_file_splits(input_paths, num_tasks, keep_sorted=keep_sorted)
        
        # run the tasks
        procs = []
        self._prev_outfiles = []
        for (task_num, file_name) in enumerate(file_splits):
            # args - one file_split per process
            proc_args = args + [file_name]
            log.info('> %s' % cmd_line(proc_args))
            
            # set the task env
            # generate a task id
            mapreduce_task_id = 'task_%s_%s_%05d%d' % (self._job_name, step_type, step_num, task_num) 
            mapreduce_task_attempt_id = 'attempt_%s_%s_%05d%d_0' % (self._job_name, step_type, step_num, task_num) # we only have one attempt
            
            task_env = combine_local_envs(
                env,
                {'mapreduce_task_id': mapreduce_task_id, 
                 'mapreduce_task_attempt_id': mapreduce_task_attempt_id,
                 })
                 
            if step_type == 'M':
                # map only jobconf environment variables
                task_env = combine_local_envs(
                    task_env, 
                    {'mapreduce_map_input_file': file_splits[file_name]['original_name'], 
                     'mapreduce_map_input_start': str(file_splits[file_name]['start']),
                     'mapreduce_map_input_length': str(file_splits[file_name]['length'])
                     })
            
            # set up outfile
            outfile = os.path.join(self._get_local_tmp_dir(), outfile_name + '_part-%05d' % task_num)
            log.info('writing to %s' % outfile)
            log.debug('')

            self._prev_outfiles.append(outfile)
            write_to = open(outfile, 'w')

            # run the process
            proc = Popen(proc_args, stdout=write_to, stderr=PIPE,
                         cwd=self._working_dir, env=task_env)
            procs.append(proc)

        for task_num in xrange(len(file_splits)):
            proc = procs[task_num]
            # handle counters, status msgs, and other stuff on stderr
            stderr_lines = self._process_stderr_from_script(proc.stderr, step_num=step_num)
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

    def _process_stderr_from_script(self, stderr, step_num=0):
        """Handle stderr a line at time:

        - for counter lines, store counters
        - for status message, log the status change
        - for all other lines, log an error, and yield the lines
        """
        for line in stderr:
            # just pass one line at a time to parse_mr_job_stderr(),
            # so we can print error and status messages in realtime
            parsed = parse_mr_job_stderr([line], counters=self._counters[step_num-1])

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
