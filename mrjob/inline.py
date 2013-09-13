# -*- coding: utf-8 -*-
# Copyright 2011 Matthew Tai and Yelp
# Copyright 2012-2013 Yelp and Contributors
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
from __future__ import with_statement

__author__ = 'Matthew Tai <mtai@adku.com>'

import logging
import os

try:
    from cStringIO import StringIO
    StringIO  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    from StringIO import StringIO

from mrjob.sim import SimMRJobRunner
from mrjob.sim import SimRunnerOptionStore
from mrjob.job import MRJob
from mrjob.util import save_current_environment
from mrjob.util import save_cwd

log = logging.getLogger(__name__)


# Deprecated in favor of class variables, remove in v0.5.0
DEFAULT_MAP_TASKS = 1
DEFAULT_REDUCE_TASKS = 1


class InlineMRJobRunner(SimMRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` without invoking the job as
    a subprocess, so it's easy to attach a debugger.

    This is the default way of testing jobs; We assume you'll spend some time
    debugging your job before you're ready to run it on EMR or Hadoop.

    To more accurately simulate your environment prior to running on
    Hadoop/EMR, use ``-r local``.
    """
    alias = 'inline'

    OPTION_STORE_CLASS = SimRunnerOptionStore

    # stick to a single split for efficiency
    _DEFAULT_MAP_TASKS = 1
    _DEFAULT_REDUCE_TASKS = 1

    def __init__(self, mrjob_cls=None, **kwargs):
        """:py:class:`~mrjob.inline.InlineMRJobRunner` takes the same keyword
        args as :py:class:`~mrjob.runner.MRJobRunner`. However, please note:

        * *hadoop_extra_args*, *hadoop_input_format*, *hadoop_output_format*,
          and *hadoop_streaming_jar*, and *partitioner* are ignored
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
    IGNORED_LOCAL_OPTS = [
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

    def _create_setup_wrapper_script(self):
        # Inline mode does not use a wrapper script (no subprocesses)
        pass

    def warn_ignored_opts(self):
        """ Warn the user of opts being ignored by this runner.
        """
        super(InlineMRJobRunner, self).warn_ignored_opts()
        for ignored_opt in self.IGNORED_LOCAL_OPTS:
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

        common_args = (['--step-num=%d' % step_num] +
                       self._mr_job_extra_args(local=True))

        if step_type == 'mapper':
            child_args = (
                ['--mapper'] + [input_path] + common_args)
        elif step_type == 'reducer':
            child_args = (
                ['--reducer'] + [input_path] + common_args)
        elif step_type == 'combiner':
            child_args = ['--combiner'] + common_args + ['-']

        child_instance = self._mrjob_cls(args=child_args)

        has_combiner = (step_type == 'mapper' and 'combiner' in step)

        # Use custom stdin
        if has_combiner:
            child_stdout = StringIO()
        else:
            child_stdout = open(output_path, 'w')

        with save_current_environment():
            with save_cwd():
                os.environ.update(env)
                os.chdir(working_dir)

                child_instance.sandbox(stdin=child_stdin, stdout=child_stdout)
                child_instance.execute()

        if has_combiner:
            sorted_lines = sorted(child_stdout.getvalue().splitlines())
            combiner_stdin = StringIO('\n'.join(sorted_lines))
        else:
            child_stdout.flush()

        child_stdout.close()

        while len(self._counters) <= step_num:
            self._counters.append({})
        child_instance.parse_counters(self._counters[step_num])

        if has_combiner:
            self._run_step(step_num, 'combiner', None, output_path,
                           working_dir, env, child_stdin=combiner_stdin)

            combiner_stdin.close()
