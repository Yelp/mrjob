# -*- coding: utf-8 -*-
# Copyright 2011 Matthew Tai
# Copyright 2012 Yelp
# Copyright 2013 Yelp and Lyft
# Copyright 2014 Marc Abramowitz
# Copyright 2015-2017 Yelp
# Copyright 2018 Yelp
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
import gzip
import os
import os.path
from os.path import exists
from os.path import join
from io import BytesIO

from warcio.warcwriter import WARCWriter

from mrjob.examples.mr_phone_to_url import MRPhoneToURL
from mrjob.inline import InlineMRJobRunner
from mrjob.job import MRJob

from tests.examples.test_mr_phone_to_url import write_conversion_record
from tests.mr_test_cmdenv import MRTestCmdenv
from tests.mr_two_step_job import MRTwoStepJob
from tests.sandbox import EmptyMrjobConfTestCase
from tests.sandbox import SandboxedTestCase
from tests.job import run_job
from tests.py2 import patch
from tests.test_sim import MRIncrementerJob


# the inline runner is extensively used in test_sim.py, so there are not
# many inline-specific tests

class InlineMRJobRunnerEndToEndTestCase(SandboxedTestCase):

    def test_end_to_end(self):
        # read from STDIN, a regular file, and a .gz
        stdin = BytesIO(b'foo\nbar\n')

        input_path = join(self.tmp_dir, 'input')
        with open(input_path, 'w') as input_file:
            input_file.write('bar\nqux\n')

        input_gz_path = join(self.tmp_dir, 'input.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'wb')
        input_gz.write(b'foo\n')
        input_gz.close()

        mr_job = MRTwoStepJob(
            ['--runner', 'inline', '-', input_path, input_gz_path])
        mr_job.sandbox(stdin=stdin)

        local_tmp_dir = None
        results = []

        with mr_job.make_runner() as runner:
            assert isinstance(runner, InlineMRJobRunner)
            runner.run()

            results.extend(mr_job.parse_output(runner.cat_output()))

            local_tmp_dir = runner._get_local_tmp_dir()
            assert exists(local_tmp_dir)

        # make sure cleanup happens
        assert not exists(local_tmp_dir)

        self.assertEqual(sorted(results),
                         [(1, 'qux'), (2, 'bar'), (2, 'foo'), (5, None)])

    def test_missing_input(self):
        runner = InlineMRJobRunner(input_paths=['/some/bogus/file/path'])
        self.assertRaises(Exception, runner._run)


class InlineMRJobRunnerCmdenvTest(EmptyMrjobConfTestCase):

    def test_cmdenv(self):
        import logging
        logging.basicConfig()
        # make sure previous environment is preserved
        os.environ['SOMETHING'] = 'foofoofoo'
        old_env = os.environ.copy()

        mr_job = MRTestCmdenv(['--runner', 'inline', '--cmdenv=FOO=bar'])
        mr_job.sandbox(stdin=BytesIO(b'foo\n'))

        results = []

        with mr_job.make_runner() as runner:
            assert isinstance(runner, InlineMRJobRunner)
            runner.run()

            results.extend(mr_job.parse_output(runner.cat_output()))

        self.assertEqual(sorted(results),
                         [('FOO', 'bar'), ('SOMETHING', 'foofoofoo')])

        # make sure we revert back
        self.assertEqual(old_env, os.environ)


class InlineRunnerStepsTestCase(EmptyMrjobConfTestCase):
    # make sure file options get passed to --steps in inline mode

    def test_adding_2(self):
        mr_job = MRIncrementerJob(['-r', 'inline', '--times', '2'])
        mr_job.sandbox(stdin=BytesIO(b'0\n1\n2\n'))

        self.assertEqual(len(mr_job.steps()), 2)

        with mr_job.make_runner() as runner:
            assert isinstance(runner, InlineMRJobRunner)
            self.assertEqual(runner._get_steps(), [
                {
                    'type': 'streaming',
                    'mapper': {
                        'type': 'script',
                    }
                },
                {
                    'type': 'streaming',
                    'mapper': {
                        'type': 'script',
                    }
                },
            ])

            runner.run()

            output = sorted(
                v for k, v in mr_job.parse_output(runner.cat_output()))

            self.assertEqual(output, [2, 3, 4])


class InlineInputManifestTestCase(SandboxedTestCase):

    RUNNER = 'inline'

    EXPECTED_OUTPUT = {
        '+12018675309': 'https://jseventplanning.biz/',
        '+16127779311': 'https://big.directory/',
    }

    def test_input_manifest(self):
        wet1 = BytesIO()
        writer1 = WARCWriter(wet1, gzip=False)

        write_conversion_record(
            writer1, 'https://nophonenumbershere.info',
            b'THIS-IS-NOT-A-NUMBER')
        write_conversion_record(
            writer1, 'https://big.directory/',
            b'The Time: (612) 777-9311\nJenny: (201) 867-5309\n')

        wet2_gz_path = join(self.tmp_dir, 'wet2.warc.wet.gz')
        with open(wet2_gz_path, 'wb') as wet2:
            writer2 = WARCWriter(wet2, gzip=True)

            write_conversion_record(
                writer2, 'https://jseventplanning.biz/',
                b'contact us at +1 201 867 5309')

        self.assertEqual(
            run_job(MRPhoneToURL(['-r', self.RUNNER, wet2_gz_path, '-']),
                    raw_input=wet1.getvalue()),
            self.EXPECTED_OUTPUT)


class MRNope(MRJob):
    def mapper_init(self):
        raise NotImplementedError


class MRManifestNope(MRJob):
    def mapper_raw(self, input_path, input_uri):
        raise NotImplementedError


class WhileReadingFromTestCase(SandboxedTestCase):
    # mostly a regression test for #1758

    def _test_reading_from(self, job_class, expect_input_path):
        # check that we report the actual input file and not the manifest file
        input_path = self.makefile('input.txt')

        job = job_class([input_path])
        job.sandbox()

        log = self.start(patch('mrjob.inline.log'))

        with job.make_runner() as runner:
            self.assertRaises(NotImplementedError, runner.run)

        error_log = ''.join(a[0][0] for a in log.error.call_args_list)

        if expect_input_path:
            self.assertIn(input_path, error_log)
        else:
            self.assertNotIn(input_path, error_log)

    def test_regular_job(self):
        self._test_reading_from(MRNope, expect_input_path=False)

    def test_input_manifest(self):
        self._test_reading_from(MRManifestNope, expect_input_path=True)
