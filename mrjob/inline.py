# -*- coding: utf-8 -*-
# Copyright 2011 Matthew Tai and Yelp
# Copyright 2012-2016 Yelp and Contributors
# Copyright 2017 Yelp
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
"""Run an MRJob inline by running all mappers and reducers through the same
process. Useful for debugging."""
import logging
import os

from mrjob.job import MRJob
from mrjob.options import _allowed_keys
from mrjob.options import _combiners
from mrjob.options import _deprecated_aliases
#from mrjob.parse import parse_mr_job_stderr
from mrjob.runner import RunnerOptionStore
from mrjob.sim import SimMRJobRunner
from mrjob.util import save_current_environment
from mrjob.util import save_cwd

log = logging.getLogger(__name__)


class InlineRunnerOptionStore(RunnerOptionStore):
    ALLOWED_KEYS = _allowed_keys('inline')
    COMBINERS = _combiners('inline')
    DEPRECATED_ALIASES = _deprecated_aliases('inline')


class InlineMRJobRunner(SimMRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` in the same process, so it's easy
    to attach a debugger.

    This is the default way to run jobs (we assume you'll spend some time
    debugging your job before you're ready to run it on EMR or Hadoop).

    Unlike other runners, ``InlineMRJobRunner``\ 's ``run()`` method
    raises the actual exception that caused a step to fail (rather than
    :py:class:`~mrjob.step.StepFailedException`).

    To more accurately simulate your environment prior to running on
    Hadoop/EMR, use ``-r local`` (see
    :py:class:`~mrjob.local.LocalMRJobRunner`).
    """
    OPTION_STORE_CLASS = InlineRunnerOptionStore

    alias = 'inline'

    def __init__(self, mrjob_cls=None, **kwargs):
        """:py:class:`~mrjob.inline.InlineMRJobRunner` takes the same keyword
        args as :py:class:`~mrjob.runner.MRJobRunner`. However, please note
        that
        *hadoop_input_format*, *hadoop_output_format*, and *partitioner*
        are ignored
        because they require Java. If you need to test these, consider
        starting up a standalone Hadoop instance and running your job with
        ``-r hadoop``."""
        super(InlineMRJobRunner, self).__init__(**kwargs)
        if not (mrjob_cls is None or issubclass(mrjob_cls, MRJob)):
            raise TypeError

        self._mrjob_cls = mrjob_cls

        # used to explain exceptions
        self._error_while_reading_from = None

    def _invoke_task_func(self, task_type, step_num, task_num):
        """Just run tasks in the same process."""

        # Don't care about pickleability since this runs in the same process
        def invoke_task(stdin, stdout, stderr, wd, env):
            with save_current_environment(), save_cwd():
                os.environ.update(env)
                os.chdir(wd)

                try:
                    task = self._mrjob_cls(
                        args=self._args_for_task(step_num, task_type))
                    task.sandbox(stdin=stdin, stdout=stdout, stderr=stderr)

                    task.execute()
                except:
                    # so users can figure out where the exception came from;
                    # see _log_cause_of_error(). we can't wrap the exception
                    # because then we lose the stacktrace (which is the whole
                    # point of the inline runner)

                    # TODO: could write this to a file instead
                    self._error_while_reading_from = self._task_input_path(
                        task_type, step_num, task_num)
                    raise

        return invoke_task

    def _log_cause_of_error(self, ex):
        """Just tell what file we were reading from (since they'll see
        the stacktrace from the actual exception)"""
        if self._error_while_reading_from:
            log.error('Error while reading from %s:\n' %
                      self._error_while_reading_from)

    # avoid subprocesses

    # TODO: won't need this once we break this out into MRJobBinRunner
    # (see #1617)
    def _create_setup_wrapper_script(self, local=False):
        # inline mode doesn't need this (no subprocesses)
        pass

    def _get_steps(self):
        """Redefine this so that we can get step descriptions without
        calling a subprocess."""
        if self._steps is None:
            job_args = ['--steps'] + self._mr_job_extra_args(local=True)
            self._steps = self._mrjob_cls(args=job_args)._steps_desc()

        return self._steps
