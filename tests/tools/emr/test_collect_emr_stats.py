# Copyright 2014 Yelp
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
"""Basic tests for the count_emr_list_active script"""
from datetime import date
from datetime import datetime
from datetime import timedelta
from StringIO import StringIO
import sys

import boto.emr.connection
from mrjob.tools.emr.count_active_emr import 
#from mrjob.tools.emr.audit_usage import job_flow_to_full_summary
#from mrjob.tools.emr.audit_usage import subdivide_interval_by_date
#from mrjob.tools.emr.audit_usage import subdivide_interval_by_hour
#from mrjob.tools.emr.audit_usage import main
#from mrjob.tools.emr.audit_usage import percent

from tests.mockboto import MockEmrObject
from tests.test_emr import MockEMRAndS3TestCase

try:
    import unittest2 as unittest
    unittest  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import unittest


# setUp / tearDown ??

class CountEMRListActiveTestCase(MockEMRAndS3TestCase):

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
        emr_conn = boto.emr.connection.EmrConnection()
        emr_conn.run_jobflow('no name', log_uri=None)

        main(['-q', '--no-conf'])
        self.assertIn('j-MOCKJOBFLOW0', self.stdout.getvalue())


# state names: [BOOTSTRAPPING, STARTING, TERMINATED, COMPLETED, WAITING, FAILED, SHUTTING_DOWN, RUNNING] 
# active : [STARTING, BOOTSTRAPPING, WAITING]


#    def test_emr_list_active(self):
        



