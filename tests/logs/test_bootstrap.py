# Copyright 2016 Yelp
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
from tests.sandbox import PatcherTestCase
from tests.py2 import Mock
from tests.py2 import TestCase
from tests.py2 import patch

from mrjob.logs.bootstrap import _check_for_nonzero_return_code
from mrjob.logs.bootstrap import _match_emr_bootstrap_stderr_path


class CheckForNonzeroReturnCodeTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(
            _check_for_nonzero_return_code(''),
            None)

    def test_nonzero_return_code_on_master_instance(self):
        self.assertEqual(
            _check_for_nonzero_return_code(
                'On the master instance (i-96c21a39), bootstrap action 2'
                ' returned a non-zero return code'),
            # action_num is 0-indexed
            dict(action_num=1, node_id='i-96c21a39'),
        )

    def test_nonzero_return_code_on_two_slave_instances(self):
        self.assertEqual(
            _check_for_nonzero_return_code(
                'On 2 slave instances (including i-105af6bf and i-b659f519),'
                ' bootstrap action 1 returned a non-zero return code'),
            dict(action_num=0, node_id='i-105af6bf')
        )

    def test_failed_to_download_bootstrap_action(self):
        self.assertEqual(
            _check_for_nonzero_return_code(
                'Master instance (i-ec41ed43) failed attempting to download'
                ' bootstrap action 1 file from S3'),
            None)


class MatchEMRBootstrapStderrPathTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(
            _match_emr_bootstrap_stderr_path(''),
            None)

    def test_stderr(self):
        self.assertEqual(
            _match_emr_bootstrap_stderr_path(
                's3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/i-e647eb49/'
                'bootstrap-actions/2/stderr'),
            dict(action_num=1, node_id='i-e647eb49')
        )

    def test_stderr_gz(self):
        self.assertEqual(
            _match_emr_bootstrap_stderr_path(
                's3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/i-e647eb49/'
                'bootstrap-actions/1/stderr.gz'),
            dict(action_num=0, node_id='i-e647eb49')
        )

    def test_syslog(self):
        self.assertEqual(
            _match_emr_bootstrap_stderr_path(
                's3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/i-e647eb49/'
                'bootstrap-actions/1/syslog'),
            None
        )
