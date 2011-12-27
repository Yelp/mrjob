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
"""Very basic tests for the audit_usage script"""
from StringIO import StringIO
import sys

from mrjob import boto_2_1_1_83aae37b
from mrjob.emr import EMRJobRunner
from mrjob.tools.emr.audit_usage import main
from tests.test_emr import MockEMRAndS3TestCase


class AuditUsageTestCase(MockEMRAndS3TestCase):

    def setUp(self):
        super(AuditUsageTestCase, self).setUp()
        # redirect print statements to self.stdout
        self._real_stdout = sys.stdout
        self.stdout = StringIO()
        sys.stdout = self.stdout

    def tearDown(self):
        sys.stdout = self._real_stdout
        super(AuditUsageTestCase, self).tearDown()

    def test_with_no_job_flows(self):
        main(['-q', '--no-conf'])  # just make sure it doesn't crash

    def test_with_one_job_flow(self):
        emr_conn = boto_2_1_1_83aae37b.EmrConnection()
        emr_conn.run_jobflow('no name', log_uri=None)

        main(['-q', '--no-conf'])
        self.assertIn('j-MOCKJOBFLOW0', self.stdout.getvalue())
