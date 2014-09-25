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
"""Basic tests for collect_emr_stats script"""
from datetime import date
from datetime import datetime
from StringIO import StringIO
import sys

from mock import Mock, call, patch
from mrjob.tools.emr.collect_emr_stats import main
from mrjob.tools.emr.collect_emr_stats import collect_active_job_flows
from mrjob.tools.emr.collect_emr_stats import job_flows_to_stats
from tests.mockboto import MockEmrObject, MockEmrConnection
from tests.test_emr import MockEMRAndS3TestCase

try:
    import unittest2 as unittest
    unittest  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import unittest


class CollectEMRStatsTestCase(MockEMRAndS3TestCase):

    def setUp(self):
        super(CollectEMRStatsTestCase, self).setUp()
        # redirect print statements to self.stdout
        self._real_stdout = sys.stdout
        self.stdout = StringIO()
        sys.stdout = self.stdout

    def tearDown(self):
        sys.stdout = self._real_stdout
        super(CollectEMRStatsTestCase, self).tearDown()

    @patch('mrjob.tools.emr.collect_emr_stats.describe_all_job_flows')
    @patch('mrjob.tools.emr.collect_emr_stats.EMRJobRunner')
    def test_collect_active_job_flows(self, mock_job_runner, mock_describe_jobflows):
        job_flows = collect_active_job_flows(conf_paths=[])
        assert mock_job_runner.called
        self.assertEqual(mock_job_runner.call_args_list, [call(conf_paths=[])])
        assert mock_describe_jobflows.called
        states=['STARTING', 'BOOTSTRAPPING', 'WAITING', 'RUNNING']
        args, kwargs = mock_describe_jobflows.call_args
        self.assertEqual(states, kwargs['states'])

