# -*- coding: utf-8 -*-
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


import logging
import os
from subprocess import Popen
from subprocess import PIPE
import sys

from mrjob.conf import combine_dicts
from mrjob.sim import SimMRJobRunner
from mrjob.sim import SimRunnerOptionStore
from mrjob.parse import find_python_traceback
from mrjob.parse import parse_mr_job_stderr
from mrjob.util import cmd_line
from mrjob.util import shlex_split


log = logging.getLogger(__name__)


DEFAULT_MAP_TASKS = 2
DEFAULT_REDUCE_TASKS = 2


def _chain_procs(procs_args, **kwargs):
    """Input: List of lists of command line arguments.

    These arg lists will be turned into Popen objects with the keyword
    arguments specified as kwargs to this function. For procs X, Y, and Z, X
    stdout will go to Y stdin and Y stdout will go to Z stdin. So for
    P[i < |procs|-1], stdout is replaced with a pipe to the next process. For
    P[i > 0], stdin is replaced with a pipe from the previous process.
    Otherwise, the kwargs are passed through to the Popen constructor without
    modification, so you can specify stdin/stdout/stderr file objects and have
    them behave as expected.

    The return value is a list of Popen objects created, in the same order as
    *procs_args*.

    In most ways, this function makes several processes that act as one in
    terms of input and output.
    """
    last_stdout = None

    procs = []
    for i, args in enumerate(procs_args):
        proc_kwargs = kwargs.copy()

        # first proc shouldn't override any kwargs
        # other procs should get stdin from last proc's stdout
        if i > 0:
            proc_kwargs['stdin'] = last_stdout

        # last proc shouldn't override stdout
        # other procs should have stdout sent to next proc
        if i < len(procs_args) - 1:
            proc_kwargs['stdout'] = PIPE

        proc = Popen(args, **proc_kwargs)
        last_stdout = proc.stdout
        procs.append(proc)

    return procs


class LocalRunnerOptionStore(SimRunnerOptionStore):

    def default_options(self):
        super_opts = super(LocalRunnerOptionStore, self).default_options()
        return combine_dicts(super_opts, {
            # prefer whatever interpreter we're currently using
            'python_bin': [sys.executable or 'python'],
        })


class LocalMRJobRunner(SimMRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` locally, for testing purposes.

    This is NOT the default way of running jobs; we assume you'll spend some
    time debugging your job before you're ready to run it on EMR or Hadoop.

    It's rare to need to instantiate this class directly (see
    :py:meth:`~LocalMRJobRunner.__init__` for details).

    :py:class:`LocalMRJobRunner` adds the current working directory to the
    subprocesses' :envvar:`PYTHONPATH`, so if you're using it to test an EMR
    job locally, be aware that it may see more Python modules than will
    actaully be uploaded. This behavior may change in the future.
    """

    alias = 'local'

    OPTION_STORE_CLASS = LocalRunnerOptionStore

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

        self._map_tasks = DEFAULT_MAP_TASKS
        self._reduce_tasks = DEFAULT_REDUCE_TASKS
        self._all_proc_dicts = None

        # jobconf variables set by our own job (e.g. files "uploaded")
        #
        # By convention, we use the Hadoop 0.21 (newer) versions of the
        # jobconf variables internally (they get auto-translated before
        # running the job)
        self._internal_jobconf = {}

    def run_step(self, step_dict, input_file,
                 outfile_name, step_num, step_type, env):

        if self._all_proc_dicts is None:
            self._all_proc_dicts = []
        if step_type == 'mapper':
            procs_args = self._mapper_arg_chain(
                step_dict, step_num, input_file)
        elif step_type == 'reducer':
            procs_args = self._reducer_arg_chain(
                step_dict, step_num, input_file)

        proc_dicts = self._invoke_processes(
            procs_args, outfile_name, env=env)
        self._all_proc_dicts.extend(proc_dicts)

    def per_step_runner_finish(self, step_num):
        for proc_dict in self._all_proc_dicts:
            self._wait_for_process(proc_dict, step_num)

        self.all_proc_dicts = None

    def _filter_if_any(self, substep_dict):
        if substep_dict['type'] == 'script':
            if 'pre_filter' in substep_dict:
                return shlex_split(substep_dict['pre_filter'])
        return None

    def _executable(self, steps=False):
        # detect executable files so we can discard the explicit interpreter if
        # possible
        if os.access(self._script_path, os.X_OK):
            return [os.path.join(
                self._working_dir,
                self._working_dir_mgr.name('file', self._script_path))]
        else:
            return super(LocalMRJobRunner, self)._executable(steps)

    def _substep_args(self, step_dict, step_num, mrc, input_path=None):
        if step_dict['type'] != 'streaming':
            raise Exception("LocalMRJobRunner cannot run %s steps." %
                            step_dict['type'])
        if step_dict[mrc]['type'] == 'command':
            if input_path is None:
                return [shlex_split(step_dict[mrc]['command'])]
            else:
                return [
                    ['cat', input_path],
                    shlex_split(step_dict[mrc]['command'])]
        if step_dict[mrc]['type'] == 'script':
            args = self._script_args_for_step(step_num, mrc)
            if input_path is None:
                return [args]
            else:
                return [args + [input_path]]

    def _substep_arg_chain(self, mrc, step_dict, step_num, input_file):
        procs_args = []

        filter_args = self._filter_if_any(step_dict[mrc])
        if filter_args:
            procs_args.append(['cat', input_file])
            procs_args.append(filter_args)
            # _substep_args may return more than one process
            procs_args.extend(
                self._substep_args(step_dict, step_num, mrc))
        else:
            # _substep_args may return more than one process
            procs_args.extend(
                self._substep_args(step_dict, step_num, mrc, input_file))
        return procs_args

    def _mapper_arg_chain(self, step_dict, step_num, input_file):
        # sometimes the mapper isn't actually there, so if it isn't, use cat
        if 'mapper' not in step_dict:
            new_step_dict = {
                'mapper': {
                    'type': 'command',
                    'command': 'cat',
                }
            }
            new_step_dict.update(step_dict)
            step_dict = new_step_dict

        procs_args = self._substep_arg_chain(
            'mapper', step_dict, step_num, input_file)

        if 'combiner' in step_dict:
            procs_args.append(['sort'])
            # _substep_args may return more than one process
            procs_args.extend(self._combiner_arg_chain(step_dict, step_num))

        return procs_args

    def _combiner_arg_chain(self, step_dict, step_num):
        # simpler than mapper or reducer arg logic because it never takes an
        # input file, always reads from stdin
        procs_args = []

        filter_args = self._filter_if_any(step_dict['combiner'])
        if filter_args:
            procs_args.append(filter_args)
        # _substep_args may return more than one process
        procs_args.extend(
            self._substep_args(step_dict, step_num, 'combiner'))
        return procs_args

    def _reducer_arg_chain(self, step_dict, step_num, input_file):
        return self._substep_arg_chain(
            'reducer', step_dict, step_num, input_file)

    def _invoke_processes(self, procs_args, outfile, env):
        """invoke the process described by *args* and write to *outfile_name*

        :param combiner_args: If this mapper has a combiner, we need to do
                              some extra shell wrangling, so pass the combiner
                              arguments in separately.

        :return: dict(proc=Popen, args=[process args], write_to=file)
        """
        log.info('> %s > %s' % (' | '.join(
            args if isinstance(args, basestring) else cmd_line(args)
            for args in procs_args), outfile))

        with open(outfile, 'w') as write_to:
            procs = _chain_procs(procs_args, stdout=write_to, stderr=PIPE,
                                cwd=self._working_dir, env=env)
            return [{'args': args, 'proc': proc, 'write_to': write_to}
                    for args, proc in zip(procs_args, procs)]

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
