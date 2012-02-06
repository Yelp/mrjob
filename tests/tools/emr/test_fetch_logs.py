# Copyright 2009-2012 Yelp
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

"""Test the log flow fetcher"""

from __future__ import with_statement

from optparse import OptionError
from StringIO import StringIO
import sys

from mrjob.tools.emr.fetch_logs import main as fetch_logs_main
from mrjob.tools.emr.fetch_logs import parse_args
from mrjob.tools.emr.fetch_logs import runner_kwargs

from tests.quiet import no_handlers_for_logger
from tests.test_emr import MockEMRAndS3TestCase


class LogFetchingTestCase(MockEMRAndS3TestCase):

    def setUp(self):
        super(LogFetchingTestCase, self).setUp()
        self._original_argv = sys.argv
        self._original_stdout = sys.stdout
        self.stdout = StringIO()

    def tearDown(self):
        super(LogFetchingTestCase, self).tearDown()
        sys.argv = self._original_argv
        sys.stdout = self._original_stdout

    def monkey_patch_argv(self, *args):
        sys.argv = [sys.argv[0]] + list(args)

    def monkey_patch_stdout(self):
        sys.stdout = self.stdout

    def test_bad_args(self):
        self.monkey_patch_argv()
        self.assertRaises(OptionError, parse_args)

    def test_runner_kwargs(self):
        self.monkey_patch_argv('--verbose', 'j-MOCKJOBFLOW0')
        self.assertEqual(
            runner_kwargs(parse_args()),
            {'conf_path': None,
             'ec2_key_pair_file': None,
             'emr_job_flow_id': 'j-MOCKJOBFLOW0'})

    def test_create_job_flow(self):
        self.add_mock_s3_data({'walrus': {}})
        self.monkey_patch_argv(
            '--quiet', '--no-conf',
            '--s3-sync-wait-time', '0',
            '--s3-scratch-uri', 's3://walrus/tmp',
            'j-MOCKJOBFLOW0')
