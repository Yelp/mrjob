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

from mrjob.emr import EMRJobRunner
from mrjob.tools.emr.fetch_logs import main as fetch_logs_main
from mrjob.tools.emr.fetch_logs import make_option_parser
from mrjob.tools.emr.fetch_logs import parse_args
from mrjob.tools.emr.fetch_logs import perform_actions
from mrjob.tools.emr.fetch_logs import runner_kwargs

from tests.tools.emr import ToolTestCase


class Args(object):

    ATTRS = (
        'step_num', 'list_relevant', 'list_all', 'cat_relevant', 'cat_all',
        'get_counters', 'find_failure')

    def __init__(self, *args):
        for attr, arg in zip(self.ATTRS, args):
            setattr(self, attr, arg)


def make_args(step_num=1, list_relevant=False, list_all=False,
              cat_relevant=False, cat_all=True, get_counters=False,
              find_failure=False):
    return Args(step_num, list_relevant, list_all, cat_relevant, cat_all,
                get_counters, find_failure)


class LogFetchingTestCase(ToolTestCase):

    def setUp(self):
        super(LogFetchingTestCase, self).setUp()

        self.runner = EMRJobRunner(conf_paths=[],
                                   s3_sync_wait_time=0,
                                   emr_job_flow_id='j-MOCKJOBFLOW0')

    def test_bad_args(self):
        self.monkey_patch_argv()
        self.assertRaises(OptionError, parse_args, (make_option_parser(),))

    def test_runner_kwargs(self):
        self.monkey_patch_argv('--quiet', 'j-MOCKJOBFLOW0')
        self.assertEqual(
            runner_kwargs(parse_args(make_option_parser())),
            {'conf_paths': None,
             'ec2_key_pair_file': None,
             's3_sync_wait_time': None,
             'emr_job_flow_id': 'j-MOCKJOBFLOW0'})

    def test_find_failure(self):
        self.make_job_flow()
        self.monkey_patch_stdout()

        perform_actions(make_args(find_failure=True), self.runner)

        self.assertEqual(self.stdout.getvalue(),
                         'No probable cause of failure found.\n')

    def test_list(self):
        self.make_job_flow()
        self.monkey_patch_argv(
            '--quiet', '--no-conf',
            '-l',
            '--s3-sync-wait-time=0',
            'j-MOCKJOBFLOW0')

        self.monkey_patch_stdout()

        fetch_logs_main()

        self.assertEqual(self.stdout.getvalue(),
                         'Task attempts:\n\nSteps:\n\nJobs:\n\nNodes:\n\n')

    def test_list_all(self):
        self.make_job_flow()
        self.monkey_patch_argv(
            '--quiet', '--no-conf',
            '-L',
            '--s3-sync-wait-time=0',
            'j-MOCKJOBFLOW0')

        self.monkey_patch_stdout()

        fetch_logs_main()

        self.assertEqual(self.stdout.getvalue(), '\n')

    def test_fetch_counters(self):
        self.make_job_flow()
        self.monkey_patch_argv(
            '--quiet', '--no-conf',
            '--counters',
            '--s3-sync-wait-time=0',
            'j-MOCKJOBFLOW0')
        self.monkey_patch_stdout()
        fetch_logs_main()
        self.assertEqual(self.stdout.getvalue(), '')
