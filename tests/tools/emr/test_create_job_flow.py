# Copyright 2009-2013 Yelp and Contributors
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
"""Test the create-job-flow script"""

from __future__ import with_statement

from mrjob.tools.emr.create_job_flow import main as create_job_flow_main
from mrjob.tools.emr.create_job_flow import runner_kwargs

from tests.tools.emr import ToolTestCase


class JobFlowInspectionTestCase(ToolTestCase):

    def test_runner_kwargs(self):
        self.monkey_patch_argv('--quiet')
        self.assertEqual(
            runner_kwargs(),
            {'additional_emr_info': None,
             'ami_version': None,
             'aws_availability_zone': None,
             'aws_region': None,
             'bootstrap_actions': [],
             'bootstrap_cmds': [],
             'bootstrap_files': [],
             'bootstrap_mrjob': None,
             'bootstrap_python_packages': [],
             'conf_paths': None,
             'ec2_core_instance_bid_price': None,
             'ec2_core_instance_type': None,
             'ec2_instance_type': None,
             'ec2_key_pair': None,
             'ec2_master_instance_bid_price': None,
             'ec2_master_instance_type': None,
             'ec2_task_instance_bid_price': None,
             'ec2_task_instance_type': None,
             'emr_endpoint': None,
             'emr_job_flow_pool_name': None,
             'enable_emr_debugging': None,
             'hadoop_version': None,
             'label': None,
             'mins_to_end_of_hour': None,
             'max_hours_idle': None,
             'num_ec2_core_instances': None,
             'num_ec2_instances': None,
             'num_ec2_task_instances': None,
             'owner': None,
             'pool_emr_job_flows': None,
             's3_endpoint': None,
             's3_log_uri': None,
             's3_scratch_uri': None,
             's3_sync_wait_time': None})

    def test_create_job_flow(self):
        self.add_mock_s3_data({'walrus': {}})
        self.monkey_patch_argv(
            '--quiet', '--no-conf',
            '--s3-sync-wait-time', '0',
            '--s3-scratch-uri', 's3://walrus/tmp')
        self.monkey_patch_stdout()
        create_job_flow_main()
        self.assertEqual(list(self.mock_emr_job_flows.keys()),
                         ['j-MOCKJOBFLOW0'])
        self.assertEqual(self.stdout.getvalue(), 'j-MOCKJOBFLOW0\n')
