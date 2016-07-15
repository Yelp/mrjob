# -*- coding: utf-8 -*-
# Copyright 2011 Matthew Tai and Yelp
# Copyright 2012-2016 Yelp and Contributors
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
from io import BytesIO
from shutil import copyfile

from mrjob.job import MRJob
from mrjob.parse import parse_mr_job_stderr
from mrjob.sim import SimMRJobRunner
from mrjob.util import save_current_environment
from mrjob.util import save_cwd

__author__ = 'Matthew Tai <taim@google.com>'

log = logging.getLogger(__name__)


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
    alias = 'inline'

    def __init__(self, mrjob_cls=None, **kwargs):
        """:py:class:`~mrjob.inline.InlineMRJobRunner` takes the same keyword
        args as :py:class:`~mrjob.runner.MRJobRunner`. However, please note:

        * *hadoop_input_format*, *hadoop_output_format*, and *partitioner*
          are ignored
          because they require Java. If you need to test these, consider
          starting up a standalone Hadoop instance and running your job with
          ``-r hadoop``.
        * *python_bin*, *setup*, *setup_cmds*, *setup_scripts* and
          *steps_python_bin* are ignored because we don't invoke
          subprocesses.
        """
        super(InlineMRJobRunner, self).__init__(**kwargs)
        assert ((mrjob_cls) is None or issubclass(mrjob_cls, MRJob))

        self._mrjob_cls = mrjob_cls

    # options that we ignore because they involve running subprocesses
    _IGNORED_LOCAL_OPTS = [
        'bootstrap_mrjob',
        'python_bin',
        'setup',
        'setup_cmds',
        'setup_scripts',
        'steps_python_bin',
    ]

    def _check_step_works_with_runner(self, step_dict):
        for key in ('mapper', 'combiner', 'reducer'):
            if key in step_dict:
                substep = step_dict[key]
                if substep['type'] != 'script':
                    raise Exception(
                        "InlineMRJobRunner cannot run %s steps." %
                        substep['type'])
                if 'pre_filter' in substep:
                    raise Exception(
                        "InlineMRJobRunner cannot run filters.")

    def _create_setup_wrapper_script(self, local=False):
        # Inline mode does not use a wrapper script (no subprocesses)
        pass

    def _warn_ignored_opts(self):
        """ Warn the user of opts being ignored by this runner.
        """
        super(InlineMRJobRunner, self)._warn_ignored_opts()
        for ignored_opt in self._IGNORED_LOCAL_OPTS:
            if ((not self._opts.is_default(ignored_opt)) and
                    self._opts[ignored_opt]):
                log.warning('ignoring %s option (use -r local instead): %r' %
                            (ignored_opt, self._opts[ignored_opt]))

    def _get_steps(self):
        """Redefine this so that we can get step descriptions without
        calling a subprocess."""
        if self._steps is None:
            job_args = ['--steps'] + self._mr_job_extra_args(local=True)
            self._steps = self._mrjob_cls(args=job_args)._steps_desc()

        return self._steps

    def _run_step(self, step_num, step_type, input_path, output_path,
                  working_dir, env, child_stdin=None):
        step = self._get_step(step_num)

        # if no mapper, just pass the data through (see #1141)
        if step_type == 'mapper' and not step.get('mapper'):
            copyfile(input_path, output_path)
            return

        # Passing local=False ensures the job uses proper names for file
        # options (see issue #851 on github)
        common_args = (['--step-num=%d' % step_num] +
                       self._mr_job_extra_args(local=False))

        if step_type == 'mapper':
            child_args = (
                ['--mapper'] + [input_path] + common_args)
        elif step_type == 'reducer':
            child_args = (
                ['--reducer'] + [input_path] + common_args)
        elif step_type == 'combiner':
            child_args = ['--combiner'] + common_args + ['-']

        has_combiner = (step_type == 'mapper' and 'combiner' in step)

        try:
            # Use custom stdout
            if has_combiner:
                child_stdout = BytesIO()
            else:
                child_stdout = open(output_path, 'wb')

            with save_current_environment():
                with save_cwd():
                    os.environ.update(env)
                    os.chdir(working_dir)

                    child_instance = self._mrjob_cls(args=child_args)
                    child_instance.sandbox(stdin=child_stdin,
                                           stdout=child_stdout)
                    child_instance.execute()

            if has_combiner:
                sorted_lines = sorted(child_stdout.getvalue().splitlines())
                combiner_stdin = BytesIO(b'\n'.join(sorted_lines))
            else:
                child_stdout.flush()
        finally:
            child_stdout.close()

        while len(self._counters) <= step_num:
            self._counters.append({})
        parse_mr_job_stderr(child_instance.stderr.getvalue(),
                            counters=self._counters[step_num])

        if has_combiner:
            self._run_step(step_num, 'combiner', None, output_path,
                           working_dir, env, child_stdin=combiner_stdin)

            combiner_stdin.close()
