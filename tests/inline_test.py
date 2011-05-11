# Copyright 2011 Matthew Tai
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
"""Tests for InlineMRJobRunner"""

from __future__ import with_statement

from StringIO import StringIO
import gzip
import mrjob
import os
import shutil
import signal
from testify import TestCase, assert_in, assert_equal, assert_not_equal, assert_not_in, setup, teardown
import tempfile

from mrjob.conf import dump_mrjob_conf
from mrjob.inline import InlineMRJobRunner
from tests.mr_job_where_are_you import MRJobWhereAreYou
from tests.mr_two_step_job import MRTwoStepJob
from tests.mr_verbose_job import MRVerboseJob
from tests.quiet import no_handlers_for_logger


class InlineMRJobRunnerEndToEndTestCase(TestCase):

    @setup
    def make_tmp_dir_and_mrjob_conf(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.mrjob_conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')
        dump_mrjob_conf({'runners': {'inline': {}}},
                        open(self.mrjob_conf_path, 'w'))

    @teardown
    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_end_to_end(self):
        # read from STDIN, a regular file, and a .gz
        stdin = StringIO('foo\nbar\n')

        input_path = os.path.join(self.tmp_dir, 'input')
        with open(input_path, 'w') as input_file:
            input_file.write('bar\nqux\n')

        input_gz_path = os.path.join(self.tmp_dir, 'input.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'w')
        input_gz.write('foo\n')
        input_gz.close()

        mr_job = MRTwoStepJob(['--runner', 'inline', '-c', self.mrjob_conf_path,
                               '-', input_path, input_gz_path])
        mr_job.sandbox(stdin=stdin)

        local_tmp_dir = None
        results = []

        with mr_job.make_runner() as runner:
            assert isinstance(runner, InlineMRJobRunner)
            runner.run()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

            local_tmp_dir = runner._get_local_tmp_dir()
            assert os.path.exists(local_tmp_dir)

        # make sure cleanup happens
        assert not os.path.exists(local_tmp_dir)

        assert_equal(sorted(results),
                     [(1, 'qux'), (2, 'bar'), (2, 'foo'), (5, None)])

class TimeoutException(Exception):
    pass
