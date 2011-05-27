# Copyright 2011 Matthew Tai
# Copyright 2011 Yelp
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
"""Run an MRJob inline by running all mappers and reducers through the same process.  Useful for debugging."""
from __future__ import with_statement

__author__ = 'Matthew Tai <mtai@adku.com>'

import logging
import os
import pprint
import shutil
import subprocess
import sys

from mrjob.conf import combine_dicts, combine_local_envs
from mrjob.runner import MRJobRunner
from mrjob.job import MRJob

log = logging.getLogger('mrjob.inline')


class InlineMRJobRunner(MRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` without invoking the job as
    a subprocess, so it's easy to attach a debugger.

    This is NOT the default way of testing jobs; to more accurately
    simulate your environment prior to running on Hadoop/EMR, use ``-r local``.

    It's rare to need to instantiate this class directly (see
    :py:meth:`~InlineMRJobRunner.__init__` for details).
    """

    alias = 'inline'

    def __init__(self, mrjob_cls=None, **kwargs):
        """:py:class:`~mrjob.inline.InlineMRJobRunner` takes the same keyword args as :py:class:`~mrjob.runner.MRJobRunner`. However, please note:

        * *hadoop_extra_args*, *hadoop_input_format*, *hadoop_output_format*, and *hadoop_streaming_jar*, and *jobconf* are ignored because they require Java. If you need to test these, consider starting up a standalone Hadoop instance and running your job with ``-r hadoop``.
        * *cmdenv*, *python_bin*, *setup_cmds*, *setup_scripts*, *steps_python_bin*, *upload_archives*, and *upload_files* are ignored because we don't invoke the job as a subprocess or run it in its own directory.
        """
        super(InlineMRJobRunner, self).__init__(**kwargs)
        assert issubclass(mrjob_cls, MRJob)

        self._mrjob_cls = mrjob_cls
        self._prev_outfile = None
        self._final_outfile = None

    @classmethod
    def _opts_combiners(cls):
        # on windows, PYTHONPATH should use ;, not :
        return combine_dicts(
            super(InlineMRJobRunner, cls)._opts_combiners(),
            {'cmdenv': combine_local_envs})

    # options that we ignore because they require real Hadoop
    IGNORED_HADOOP_OPTS = [
        'hadoop_extra_args',
        'hadoop_input_format',
        'hadoop_output_format',
        'hadoop_streaming_jar',
        'jobconf',
    ]

    # options that we ignore because they involve running subprocesses
    IGNORED_LOCAL_OPTS = [
        'cmdenv',
        'python_bin',
        'setup_cmds',
        'setup_scripts',
        'steps_python_bin',
        'upload_archives',
        'upload_files',
    ]

    def _run(self):
        self._setup_output_dir()

        assert self._script # shouldn't be able to run if no script

        default_opts = self.get_default_opts()

        for ignored_opt in self.IGNORED_HADOOP_OPTS:
            if self._opts[ignored_opt] != default_opts[ignored_opt]:
                log.warning('ignoring %s option (requires real Hadoop): %r' %
                            (ignored_opt, self._opts[ignored_opt]))

        for ignored_opt in self.IGNORED_LOCAL_OPTS:
            if self._opts[ignored_opt] != default_opts[ignored_opt]:
                log.warning('ignoring %s option (use -r local instead): %r' %
                            (ignored_opt, self._opts[ignored_opt]))

        # run mapper, sort, reducer for each step
        for step_number, step_name in enumerate(self._get_steps()):
            self._invoke_inline_mrjob(step_number, 'step-%d-mapper' %
                                      step_number, is_mapper=True)

            if 'R' in step_name:
                mapper_output_path = self._prev_outfile
                sorted_mapper_output_path = self._decide_output_path(
                    'step-%d-mapper-sorted' % step_number)
                with open(sorted_mapper_output_path, 'w') as sort_out:
                    proc = subprocess.Popen(
                        ['sort', mapper_output_path],
                        stdout=sort_out, env={'LC_ALL': 'C'})
                proc.wait()

                # This'll read from sorted_mapper_output_path
                self._invoke_inline_mrjob(step_number, 'step-%d-reducer' %
                                          step_number, is_reducer=True)

        # move final output to output directory
        self._final_outfile = os.path.join(self._output_dir, 'part-00000')
        log.info('Moving %s -> %s' % (self._prev_outfile, self._final_outfile))
        shutil.move(self._prev_outfile, self._final_outfile)

    def _invoke_inline_mrjob(self, step_number, outfile_name, is_mapper=False, is_reducer=False):
        common_args = (['--step-num=%d' % step_number] +
                       self._mr_job_extra_args(local=True) +
                       self._decide_input_paths())
        if is_mapper:
            child_args = ['--mapper'] + common_args
        elif is_reducer:
            child_args = ['--reducer'] + common_args

        outfile = self._decide_output_path(outfile_name)

        child_instance = self._mrjob_cls(args=child_args)

        # Tweak IO
        child_stdout = open(outfile, 'w')
        child_instance.sandbox(stdin=sys.stdin, stdout=child_stdout)
        child_instance.execute()
        child_stdout.flush()
        child_stdout.close()

        counters = child_instance.parse_counters()
        if counters:
            log.info('counters: ' + pprint.pformat(counters))

    def _decide_input_paths(self):
        # decide where to get input
        if self._prev_outfile is not None:
            input_paths = [self._prev_outfile]
        else:
            input_paths = []
            for path in self._input_paths:
                if path == '-':
                    input_paths.append(self._dump_stdin_to_local_file())
                else:
                    input_paths.append(path)

        return input_paths

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

    def _stream_output(self):
        """Read output from the final outfile."""
        if self._final_outfile:
            output_file = self._final_outfile
        else:
            output_file = os.path.join(self._output_dir, 'part-00000')
        log.info('streaming final output from %s' % output_file)

        for line in self.cat(output_file):
            yield line
