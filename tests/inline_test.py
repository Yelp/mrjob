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
import os
import shutil
from testify import TestCase
from testify import assert_equal
from testify import setup
from testify import teardown
import tempfile

from mrjob.conf import dump_mrjob_conf
from mrjob.job import MRJob
from mrjob.inline import InlineMRJobRunner
from mrjob.protocol import JSONValueProtocol
from tests.mr_cmdenv_test import MRCmdenvTest
from tests.mr_two_step_job import MRTwoStepJob


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

        mr_job = MRTwoStepJob(
            ['--runner', 'inline', '-c', self.mrjob_conf_path,
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


class InlineMRJobRunnerCmdenvTest(TestCase):

    @setup
    def make_tmp_dir_and_mrjob_conf(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.mrjob_conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')
        dump_mrjob_conf({'runners': {'inline': {}}},
                         open(self.mrjob_conf_path, 'w'))

    @teardown
    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_cmdenv(self):
        input_path = os.path.join(self.tmp_dir, 'input')
        with open(input_path, 'w') as input_file:
            input_file.write('foo\n')

        # make sure previous environment is preserved
        os.environ['SOMETHING'] = 'foofoofoo'
        old_env = os.environ.copy()

        mr_job = MRCmdenvTest(['--runner', 'inline',
                               '-c', self.mrjob_conf_path,
                               '--cmdenv=FOO=bar', input_path])
        mr_job.sandbox()

        results = []

        with mr_job.make_runner() as runner:
            assert isinstance(runner, InlineMRJobRunner)
            runner.run()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

        assert_equal(sorted(results),
                     [('FOO', 'bar'), ('SOMETHING', 'foofoofoo')])

        # make sure we revert back
        assert_equal(old_env, os.environ)


# this doesn't need to be in its own file because it'll be run inline
class MRIncrementerJob(MRJob):
    """A terribly silly way to add a positive integer to values."""

    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol

    def configure_options(self):
        super(MRIncrementerJob, self).configure_options()

        self.add_passthrough_option('--times', type='int', default=1)

    def mapper(self, _, value):
        yield None, value + 1

    def steps(self):
        return [self.mr(self.mapper)] * self.options.times


class InlineRunnerStepsTestCase(TestCase):
    # make sure file options get passed to --steps in inline mode

    def test_adding_2(self):
        stdin = ['0\n', '1\n', '2\n']
        mr_job = MRIncrementerJob(
            ['--no-conf', '-r', 'inline', '--times', '2'])
        mr_job.sandbox(stdin=stdin)

        assert_equal(len(mr_job.steps()), 2)

        with mr_job.make_runner() as runner:
            assert isinstance(runner, InlineMRJobRunner)
            assert_equal(runner._get_steps(), ['M', 'M'])

            runner.run()

            output = sorted(mr_job.parse_output_line(line)[1]
                            for line in runner.stream_output())

            assert_equal(output, [2, 3, 4])
