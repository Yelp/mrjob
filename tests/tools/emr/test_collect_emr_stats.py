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
from mrjob.tools.emr.collect_emr_stats import main
from mrjob.tools.emr.collect_emr_stats import collect_active_job_flows
from mrjob.tools.emr.collect_emr_stats import job_flows_to_stats

from tests.mockboto import MockEmrObject
from tests.py2 import TestCase
from tests.py2 import call
from tests.py2 import patch


class CollectEMRStatsTestCase(TestCase):

    @patch('mrjob.tools.emr.collect_emr_stats.describe_all_job_flows')
    @patch('mrjob.tools.emr.collect_emr_stats.EMRJobRunner')
    def test_collect_active_job_flows(self, mock_job_runner, mock_describe_jobflows):

        collect_active_job_flows(conf_paths=[])

        # check if args for calling describe_jobflows are correct
        assert (mock_job_runner.call_count == 1)
        self.assertEqual(mock_job_runner.call_args_list, [call(conf_paths=[])])
        assert (mock_describe_jobflows.call_count == 1)

        # check if args for calling describe_jobflows are correct
        active_states = ['STARTING', 'BOOTSTRAPPING', 'WAITING', 'RUNNING']
        args, kwargs = mock_describe_jobflows.call_args
        self.assertEqual(active_states, kwargs['states'])


    def test_job_flows_to_stats(self):

        # mock jobflows
        NUM_JOB_FLOWS = 30
        job_flows = []
        for i in range(NUM_JOB_FLOWS):
            job_flow_id = 'j-%04d' % i
            job_flows.append(MockEmrObject(
                jobflowid=job_flow_id,
                instancecount=i, # each jobflow has different instance count
            ))

        stats = job_flows_to_stats(job_flows)

        self.assertEqual(stats['num_jobflows'], NUM_JOB_FLOWS)
        self.assertEqual(stats['total_instance_count'], sum(range(NUM_JOB_FLOWS)))


    @patch('mrjob.tools.emr.collect_emr_stats.job_flows_to_stats')
    @patch('mrjob.tools.emr.collect_emr_stats.collect_active_job_flows')
    def test_main_no_conf(self, mock_collect_active_jobflows, mock_job_flows_to_stats):

        mock_collect_active_jobflows.return_value = []
        mock_job_flows_to_stats.return_value = {}
        main(['-q', '--no-conf'])

        # check if args for calling collect_active_jobflows are correct
        assert (mock_collect_active_jobflows.call_count == 1)
        self.assertEqual(mock_collect_active_jobflows.call_args_list, [call([])])
        assert (mock_job_flows_to_stats.call_count == 1)
