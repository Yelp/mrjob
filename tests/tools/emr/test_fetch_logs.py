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

from mrjob.tools.emr.fetch_logs import main as fetch_logs_main
from mrjob.tools.emr.fetch_logs import make_option_parser
from mrjob.tools.emr.fetch_logs import parse_args
from mrjob.tools.emr.fetch_logs import runner_kwargs

from tests.tools.emr import ToolTestCase


class LogFetchingTestCase(ToolTestCase):

    def test_bad_args(self):
        self.monkey_patch_argv()
        self.assertRaises(OptionError, parse_args, (make_option_parser(),))

    def test_runner_kwargs(self):
        self.monkey_patch_argv('--quiet', 'j-MOCKJOBFLOW0')
        self.assertEqual(
            runner_kwargs(parse_args(make_option_parser())),
            {'conf_path': None,
             'ec2_key_pair_file': None,
             'emr_job_flow_id': 'j-MOCKJOBFLOW0'})

    def test_find_failure(self):
        self.make_job_flow()
        self.monkey_patch_argv(
            '--quiet', '--no-conf',
            '--f',
            'j-MOCKJOBFLOW0')
        self.monkey_patch_stdout()

        fetch_logs_main()

        self.assertEqual(self.stdout.getvalue(),
                         'No probable cause of failure found.\n')

    def test_list(self):
        self.make_job_flow()
        self.monkey_patch_argv(
            '--quiet', '--no-conf',
            '-l',
            'j-MOCKJOBFLOW0')

        self.monkey_patch_stdout()

        fetch_logs_main()

        self.assertEqual(self.stdout.getvalue(),
                         'SSH error: Cannot ssh to master; job flow is not'
                         ' waiting or running\n'
                         'Task attempts:\n\nSteps:\n\nJobs:\n\nNodes:\n\n')

    def test_list_all(self):
        self.make_job_flow()
        self.monkey_patch_argv(
            '--quiet', '--no-conf',
            '-L',
            'j-MOCKJOBFLOW0')

        self.monkey_patch_stdout()

        fetch_logs_main()

        self.assertEqual(self.stdout.getvalue(),
                         'SSH error: Cannot ssh to master; job flow is not'
                         ' waiting or running\n\n')

    def test_fetch_counters(self):
        self.make_job_flow()
        self.monkey_patch_argv(
            '--quiet', '--no-conf',
            '--counters',
            'j-MOCKJOBFLOW0')
        self.monkey_patch_stdout()
        fetch_logs_main()
        self.assertEqual(self.stdout.getvalue(), '')
