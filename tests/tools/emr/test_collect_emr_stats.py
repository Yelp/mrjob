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

from mock import call, patch
#from mrjob.tools.emr.collect_emr_stats import main
from mrjob.tools.emr.collect_emr_stats import collect_active_job_flows
from mrjob.tools.emr.collect_emr_stats import job_flows_to_stats
from tests.mockboto import MockEmrObject
#from tests.test_emr import MockEMRAndS3TestCase

try:
    import unittest2 as unittest
    unittest  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import unittest


class CollectEMRStatsTestCase(unittest.TestCase):

    @patch('mrjob.tools.emr.collect_emr_stats.describe_all_job_flows')
    @patch('mrjob.tools.emr.collect_emr_stats.EMRJobRunner')
    def test_collect_active_job_flows(self, mock_job_runner, mock_describe_jobflows):

        job_flows = collect_active_job_flows(conf_paths=[])

        assert mock_job_runner.called
        self.assertEqual(mock_job_runner.call_args_list, [call(conf_paths=[])])
        assert mock_describe_jobflows.called
        active_states = ['STARTING', 'BOOTSTRAPPING', 'WAITING', 'RUNNING']
        args, kwargs = mock_describe_jobflows.call_args
        self.assertEqual(active_states, kwargs['states'])


    def test_job_flows_to_stats(self):

        NUM_JOB_FLOWS = 30

        # fake jobflows
        job_flows = []
        for i in range(NUM_JOB_FLOWS):
            job_flow_id = 'j-%04d' % i
            job_flows.append(MockEmrObject(
                jobflowid=job_flow_id,
                instancecount=i, # each jobflow has instance count i
            ))

        stats = job_flows_to_stats(job_flows)

        self.assertEqual(stats['num_jobflows'], NUM_JOB_FLOWS)
        self.assertEqual(stats['total_instance_count'], sum(range(NUM_JOB_FLOWS)))
