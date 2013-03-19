# -*- coding: utf-8 -*-
# Copyright 2011 Matthew Tai
# Copyright 2012 Yelp
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

log = logging.getLogger('mrjob.inline')

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

    def __init__(self, mrjob_cls=None, **kwargs):
        """:py:class:`~mrjob.inline.InlineMRJobRunner` takes the same keyword
        args as :py:class:`~mrjob.runner.MRJobRunner`. However, please note:

        * *hadoop_extra_args*, *hadoop_input_format*, *hadoop_output_format*,
          and *hadoop_streaming_jar*, *jobconf*, and *partitioner* are ignored
          because they require Java. If you need to test these, consider
          starting up a standalone Hadoop instance and running your job with
          ``-r hadoop``.
        * *cmdenv*, *python_bin*, *setup_cmds*, *setup_scripts*,
          *steps_python_bin*, *upload_archives*, and *upload_files* are ignored
          because we don't invoke the job as a subprocess or run it in its own
          directory.
        """
        super(InlineMRJobRunner, self).__init__(**kwargs)
        assert ((mrjob_cls) is None or issubclass(mrjob_cls, MRJob))

        self._mrjob_cls = mrjob_cls
        self._map_tasks = DEFAULT_MAP_TASKS
        self._reduce_tasks = DEFAULT_REDUCE_TASKS

    # options that we ignore because they involve running subprocesses
    IGNORED_LOCAL_OPTS = [
        'bootstrap_mrjob',
        'python_bin',
        'setup_cmds',
        'setup_scripts',
        'steps_python_bin',
        'upload_archives',
        'upload_files',
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
        # Inline mode does not use a wrapper script
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
        job_args = ['--steps'] + self._mr_job_extra_args(local=True)
        return self._mrjob_cls(args=job_args)._steps_desc()

    def run_step(self, step_dict, input_file, outfile_name,
                 step_number, step_type, env,
                child_stdin=None):
        common_args = (['--step-num=%d' % step_number] +
                       self._mr_job_extra_args(local=True))

        if step_type == 'mapper':
            child_args = (
                ['--mapper'] + [input_file] + common_args)
        elif step_type == 'reducer':
            child_args = (
                ['--reducer'] + [input_file] + common_args)
        elif step_type == 'combiner':
            child_args = ['--combiner'] + common_args + ['-']

        child_instance = self._mrjob_cls(args=child_args)

        has_combiner = (step_type == 'mapper' and 'combiner' in step_dict)

        # Use custom stdin
        if has_combiner:
            child_stdout = StringIO()
        else:
            child_stdout = open(outfile_name, 'w')

        with save_current_environment():
            os.environ.update(env)
            child_instance.sandbox(stdin=child_stdin, stdout=child_stdout)
            child_instance.execute()

        if has_combiner:
            sorted_lines = sorted(child_stdout.getvalue().splitlines())
            combiner_stdin = StringIO('\n'.join(sorted_lines))
        else:
            child_stdout.flush()

        child_stdout.close()

        while len(self._counters) <= step_number:
            self._counters.append({})
        child_instance.parse_counters(self._counters[step_number - 1])

        if has_combiner:
            self.run_step(step_dict, "", outfile_name, step_number, 'combiner',
                          env=env, child_stdin=combiner_stdin)

            combiner_stdin.close()
