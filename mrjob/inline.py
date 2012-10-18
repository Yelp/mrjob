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
import shutil
import subprocess
import sys

try:
    from cStringIO import StringIO
    StringIO  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    from StringIO import StringIO

from mrjob.conf import combine_dicts
from mrjob.conf import combine_local_envs
from mrjob.runner import MRJobRunner
from mrjob.runner import RunnerOptionStore
from mrjob.job import MRJob
from mrjob.util import save_current_environment

log = logging.getLogger('mrjob.inline')


class InlineRunnerOptionStore(RunnerOptionStore):

    COMBINERS = combine_dicts(RunnerOptionStore.COMBINERS, {
        'cmdenv': combine_local_envs,
    })


class InlineMRJobRunner(MRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` without invoking the job as
    a subprocess, so it's easy to attach a debugger.

    This is the default way of testing jobs; We assume you'll spend some time
    debugging your job before you're ready to run it on EMR or Hadoop.

    To more accurately simulate your environment prior to running on
    Hadoop/EMR, use ``-r local``.
    """

    alias = 'inline'

    OPTION_STORE_CLASS = InlineRunnerOptionStore

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
        self._prev_outfile = None
        self._final_outfile = None
        self._counters = []

    # options that we ignore because they require real Hadoop
    IGNORED_HADOOP_OPTS = [
        'hadoop_extra_args',
        'hadoop_streaming_jar',
        'jobconf'
    ]

    # keyword arguments that we ignore that are stored directly in
    # self._<kwarg_name> because they aren't configurable from mrjob.conf
    # use the version with the underscore to better support grepping our code
    IGNORED_HADOOP_ATTRS = [
        '_hadoop_input_format',
        '_hadoop_output_format',
        '_partitioner',
    ]

    # options that we ignore because they involve running subprocesses
    IGNORED_LOCAL_OPTS = [
        'python_bin',
        'setup_cmds',
        'setup_scripts',
        'steps_python_bin',
        'upload_archives',
        'upload_files',
    ]

    def _check_step_is_mrjob_only(self, step_dict):
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

    def _run(self):
        self._setup_output_dir()

        for ignored_opt in self.IGNORED_HADOOP_OPTS:
            if ((not self._opts.is_default(ignored_opt)) and
                self._opts[ignored_opt]):
                log.warning('ignoring %s option (requires real Hadoop): %r' %
                            (ignored_opt, self._opts[ignored_opt]))

        for ignored_attr in self.IGNORED_HADOOP_ATTRS:
            value = getattr(self, ignored_attr)
            if value is not None:
                log.warning(
                    'ignoring %s keyword arg (requires real Hadoop): %r' %
                    (ignored_attr[1:], value))

        for ignored_opt in self.IGNORED_LOCAL_OPTS:
            if ((not self._opts.is_default(ignored_opt)) and
                self._opts[ignored_opt]):
                log.warning('ignoring %s option (use -r local instead): %r' %
                            (ignored_opt, self._opts[ignored_opt]))

        with save_current_environment():
            # set cmdenv variables
            os.environ.update(self._get_cmdenv())

            steps = self._get_steps()

            for step_dict in steps:
                self._check_step_is_mrjob_only(step_dict)

            # run mapper, sort, reducer for each step
            for step_number, step_dict in enumerate(steps):

                self._invoke_inline_mrjob(
                    step_number, step_dict, 'step-%d-mapper' % step_number,
                    'mapper')

                if 'reducer' in step_dict:
                    mapper_output_path = self._prev_outfile
                    sorted_mapper_output_path = self._decide_output_path(
                        'step-%d-mapper-sorted' % step_number)
                    with open(sorted_mapper_output_path, 'w') as sort_out:
                        proc = subprocess.Popen(
                            ['sort', mapper_output_path],
                            stdout=sort_out, env={'LC_ALL': 'C'})
                    proc.wait()

                    # This'll read from sorted_mapper_output_path
                    self._invoke_inline_mrjob(
                        step_number, step_dict,
                        'step-%d-reducer' % step_number, 'reducer')

        # move final output to output directory
        self._final_outfile = os.path.join(self._output_dir, 'part-00000')
        log.info('Moving %s -> %s' % (self._prev_outfile, self._final_outfile))
        shutil.move(self._prev_outfile, self._final_outfile)

    def _get_steps(self):
        """Redefine this so that we can get step descriptions without
        calling a subprocess."""
        job_args = ['--steps'] + self._mr_job_extra_args(local=True)
        return self._mrjob_cls(args=job_args)._steps_desc()

    def _invoke_inline_mrjob(self, step_number, step_dict, outfile_name,
                             substep_to_run, child_stdin=None):
        child_stdin = child_stdin or sys.stdin
        common_args = (['--step-num=%d' % step_number] +
                       self._mr_job_extra_args(local=True))

        if substep_to_run == 'mapper':
            child_args = (
                ['--mapper'] + self._decide_input_paths() + common_args)
        elif substep_to_run == 'reducer':
            child_args = (
                ['--reducer'] + self._decide_input_paths() + common_args)
        elif substep_to_run == 'combiner':
            child_args = ['--combiner'] + common_args + ['-']

        child_instance = self._mrjob_cls(args=child_args)

        has_combiner = (substep_to_run == 'mapper' and 'combiner' in step_dict)

        # Use custom stdin
        if has_combiner:
            child_stdout = StringIO()
        else:
            outfile = self._decide_output_path(outfile_name)
            child_stdout = open(outfile, 'w')

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
        self.print_counters([step_number + 1])

        if has_combiner:
            self._invoke_inline_mrjob(step_number, step_dict, outfile_name,
                                      'combiner', child_stdin=combiner_stdin)
            combiner_stdin.close()

    def counters(self):
        return self._counters

    def _decide_input_paths(self):
        # decide where to get input
        if self._prev_outfile is not None:
            return [self._prev_outfile]
        else:
            return self._get_input_paths()

    def _decide_output_path(self, outfile_name):
        # run the mapper
        outfile = os.path.join(self._get_local_tmp_dir(), outfile_name)
        log.info('writing to %s' % outfile)
        log.debug('')

        self._prev_outfile = outfile
        return outfile

    def _setup_output_dir(self):
        if not self._output_dir:
            self._output_dir = os.path.join(
                self._get_local_tmp_dir(), 'output')

        if not os.path.isdir(self._output_dir):
            log.debug('Creating output directory %s' % self._output_dir)
            self.mkdir(self._output_dir)
