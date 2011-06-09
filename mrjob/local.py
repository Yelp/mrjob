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
        
        # process jobconf arguments
        jobconf = self._opts['jobconf']
        if jobconf:
            self._process_jobconf_args(jobconf)
       
        self._create_wrapper_script()
        self._setup_working_dir()
        self._setup_output_dir()

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
        for (conf_arg, value) in jobconf.iteritems():
            if conf_arg == 'mapred.map.tasks':
                self._map_tasks = int(value)
                if self._map_tasks < 1:
                    raise ValueError("mapred.map.tasks should be greater than 1")
            elif conf_arg == 'mapred.reduce.tasks':
                self._reduce_tasks = int(value)
                if self._reduce_tasks < 1:
                    raise ValueError("mapred.reduce.tasks should be greater than 1")
            elif conf_arg == 'mapred.job.local.dir':
                # hadoop supports multiple direcories - sticking with only one here
                if not os.path.isdir(value):
                    raise IOError("Directory %s does not exist" % value)
                self._working_dir = value
            else:
                # convert . to _ and add to running env
                name = ""
                for c in conf_arg:
                    if c == '.':
                        c = '_'
                    name = name + c
                self._running_env[name] = value
                
        self._running_env['mapred_job_id'] = self._job_name
        self._running_env['mapred_cache_localArchives'] = self._mrjob_tar_gz_path
                
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
        
        self._running_env["mapred_job_local_dir"] = self._working_dir

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
        
        self._running_env['mapred_work_output_dir'] = self._output_dir

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
        """Split the input files into *num_splits* files
        """
        # there must be a better way to do this?
        tmp_directory = self._get_local_tmp_dir()
        files = []
        file_names = []
        for i in xrange(num_splits):
            outfile = tmp_directory + '/input_part-%05d' % i
            file_names.append(outfile)
            files.append(open(outfile, 'w'))

        if keep_sorted:
            # assume that input is a collection of key <tab> value pairs
            re_pattern = re.compile("^(.*?)\t")
            try:
                lines = []
                for path in input_paths:
                    for line in read_file(path):
                        key = re_pattern.search(line).group(1)
                        lines.append((key, line))

                current_file = 0
                for key, kv_pairs in itertools.groupby(sorted(lines), key=lambda(k, v): k):
                    for key, line in kv_pairs:
                        files[current_file].write(line)
                    current_file = (current_file + 1) % num_splits
            except:
                # fall back to unsorted case
                return self._get_file_splits(input_paths, num_splits)
        else:
            current_file = 0
            for path in input_paths:
                for line in read_file(path):
                    files[current_file].write(line)
                    current_file = (current_file + 1) % num_splits

        return file_names

    def _invoke_step(self, args, outfile_name, env=None, step_num=0, num_tasks=1, step_type='M'):
        """Run the given command, outputting into outfile, and reading
        from the previous outfile (or, for the first step, from our
        original output files).

        outfile is a path relative to our local tmp dir. commands are run
        inside self._working_dir

        We'll intelligently handle stderr from the process.
        """
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
        if step_type == 'R':
            file_splits = self._get_file_splits(input_paths, num_tasks, keep_sorted=True)
        else:
            file_splits = self._get_file_splits(input_paths, num_tasks, keep_sorted=False)
        
        # generate a task id
        mapred_tip_id = 'task_%s_%s_%05d%d' % (self._job_name, step_type, step_num, 0) # TODO: fix when merging
        mapred_task_id = 'attempt_%s_%s_%05d%d_0' % (self._job_name, step_type, step_num, 0) # we only have one attempt

        # keep the current environment because we need PATH to find binaries
        # and make PYTHONPATH work
        env = combine_local_envs(
            {'PYTHONPATH': os.getcwd()},
            os.environ,
            self._get_cmdenv(),
            env or {}, 
            {#'map_input_file': input_paths[0], # TODO: fix when merging
             'mapred_task_is_map': str(step_type=='M'), 
             'mapred_task_partition': str(step_num), 
             'mapred_tip_id': mapred_tip_id, # TODO: fix when merging 
             'mapred_task_id': mapred_task_id, # TODO: fix when merging 
             })
        
        # run the tasks
        procs = []
        self._prev_outfiles = []
        for task_num in xrange(num_tasks):
            # args - one file_split per process
            proc_args = args + [file_splits[task_num]]
            log.info('> %s' % cmd_line(proc_args))
            
            # set up outfile
            outfile = os.path.join(self._get_local_tmp_dir(), outfile_name + '_part-%05d' % task_num)
            log.info('writing to %s' % outfile)
            log.debug('')

            self._prev_outfiles.append(outfile)
            write_to = open(outfile, 'w')

            # run the process
            proc = Popen(proc_args, stdout=write_to, stderr=PIPE,
                         cwd=self._working_dir, env=env)
            procs.append(proc)

        for task_num in xrange(num_tasks):
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
