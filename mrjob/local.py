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
import inspect
import logging
from subprocess import CalledProcessError
from subprocess import Popen
from subprocess import PIPE

import mrjob.cat
from mrjob.logs.counters import _format_counters
from mrjob.parse import _find_python_traceback
from mrjob.parse import parse_mr_job_stderr
from mrjob.py2 import string_types
from mrjob.runner import _fix_env
from mrjob.sim import SimMRJobRunner
from mrjob.step import StepFailedException
from mrjob.util import cmd_line
from mrjob.util import shlex_split


log = logging.getLogger(__name__)


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

        proc_kwargs['env'] = _fix_env(proc_kwargs['env'])

        proc = Popen(args, **proc_kwargs)
        last_stdout = proc.stdout
        procs.append(proc)

    return procs


class LocalMRJobRunner(SimMRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` locally, for testing purposes.
    Invoked when you run your job with ``-r local``.

    Unlike :py:class:`~mrjob.job.InlineMRJobRunner`, this actually spawns
    multiple subprocesses for each task.

    This is fairly inefficient and *not* a substitute for Hadoop; it's
    main purpose is to help you test out :mrjob-opt:`setup` commands.

    It's rare to need to instantiate this class directly (see
    :py:meth:`~LocalMRJobRunner.__init__` for details).

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
        * *hadoop_input_format*, *hadoop_output_format*,
          and *partitioner* are ignored because they
          require Java. If you need to test these, consider starting up a
          standalone Hadoop instance and running your job with ``-r hadoop``.
        """
        super(LocalMRJobRunner, self).__init__(**kwargs)

        self._all_proc_dicts = []

        # jobconf variables set by our own job (e.g. files "uploaded")
        #
        # By convention, we use the Hadoop 2 versions of the
        # jobconf variables internally (they get auto-translated before
        # running the job)
        self._internal_jobconf = {}

        # add mrjob/cat.py to working dir so we can invoke it to uncompress
        # input files (see #1540)
        self._working_dir_mgr.add('file', self._cat_py())

    def _run_step(self, step_num, step_type, input_path, output_path,
                  working_dir, env):
        step = self._get_step(step_num)

        if step_type == 'mapper':
            procs_args = self._mapper_arg_chain(
                step, step_num, input_path)
        elif step_type == 'reducer':
            procs_args = self._reducer_arg_chain(
                step, step_num, input_path)

        proc_dicts = self._invoke_processes(
            procs_args, output_path, working_dir, env)
        self._all_proc_dicts.extend(proc_dicts)

    def _per_step_runner_finish(self, step_num):
        for proc_dict in self._all_proc_dicts:
            self._wait_for_process(proc_dict, step_num)

        self._all_proc_dicts = []

    def _cat_py(self):
        """Return the path of mrjob.cat."""
        return inspect.getsourcefile(mrjob.cat)

    def _cat_args(self, input_path):
        """Return a command line that can call mrjob's internal "cat" script
        from any working directory, without mrjob in PYTHONPATH"""
        return self._python_bin() + [
            self._working_dir_mgr.name('file', self._cat_py()),
            input_path
        ]

    def _mapper_arg_chain(self, step_dict, step_num, input_path):
        procs_args = []

        procs_args.append(self._cat_args(input_path))

        if 'mapper' in step_dict:
            procs_args.append(shlex_split(
                self._substep_cmd_line(step_num, 'mapper')))

        if 'combiner' in step_dict:
            procs_args.append(['sort'])
            # _substep_args may return more than one process
            procs_args.append(shlex_split(
                self._substep_cmd_line(step_num, 'combiner')))

        return procs_args

    def _reducer_arg_chain(self, step_dict, step_num, input_path):
        if 'reducer' not in step_dict:
            return []

        procs_args = []

        procs_args.append(self._cat_args(input_path))
        procs_args.append(shlex_split(
            self._substep_cmd_line(step_num, 'reducer')))

        return procs_args

    def _invoke_processes(self, procs_args, output_path, working_dir, env):
        """invoke the process described by *args* and write to *output_path*

        :param combiner_args: If this mapper has a combiner, we need to do
                              some extra shell wrangling, so pass the combiner
                              arguments in separately.

        :return: dict(proc=Popen, args=[process args], write_to=file)
        """
        log.debug('> %s > %s' % (' | '.join(
            args if isinstance(args, string_types) else cmd_line(args)
            for args in procs_args), output_path))

        with open(output_path, 'wb') as write_to:
            procs = _chain_procs(procs_args, stdout=write_to, stderr=PIPE,
                                 cwd=working_dir, env=env)
            return [{'args': a, 'proc': proc, 'write_to': write_to}
                    for a, proc in zip(procs_args, procs)]

    def _wait_for_process(self, proc_dict, step_num):
        # handle counters, status msgs, and other stuff on stderr
        proc = proc_dict['proc']

        stderr_lines = self._process_stderr_from_script(
            proc.stderr, step_num=step_num)
        tb_lines = _find_python_traceback(stderr_lines)

        # proc.stdout isn't always defined
        if proc.stdout:
            proc.stdout.close()
        proc.stderr.close()

        returncode = proc.wait()

        if returncode != 0:
            # show counters before raising exception
            counters = self._counters[step_num]
            if counters:
                log.info(_format_counters(counters))

            # try to throw a useful exception
            if tb_lines:
                for line in tb_lines:
                    log.error(line.rstrip('\r\n'))

            reason = str(
                CalledProcessError(returncode, proc_dict['args']))
            raise StepFailedException(
                reason=reason, step_num=step_num,
                num_steps=len(self._get_steps()))

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
                log.info('Status: %s' % status)

            for line in parsed['other']:
                log.debug('STDERR: %s' % line.rstrip('\r\n'))
                yield line

    def _default_python_bin(self, local=False):
        return super(LocalMRJobRunner, self)._default_python_bin(
            local=True)
