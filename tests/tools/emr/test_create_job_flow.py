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

"""Test the idle job flow terminator"""

from __future__ import with_statement

from StringIO import StringIO
from datetime import datetime
from datetime import timedelta
import sys

from mrjob.tools.emr.create_job_flow import main as create_job_flow_main
from mrjob.tools.emr.create_job_flow import runner_kwargs

from tests.quiet import no_handlers_for_logger
from tests.test_emr import MockEMRAndS3TestCase


class JobFlowInspectionTestCase(MockEMRAndS3TestCase):

    def test_runner_kwargs(self):
        self.assertEqual(
            runner_kwargs(['--verbose']),
            {'additional_emr_info': None,
             'aws_availability_zone': None,
             'aws_region': None,
             'bootstrap_actions': [],
             'bootstrap_cmds': [],
             'bootstrap_files': [],
             'bootstrap_mrjob': None,
             'bootstrap_python_packages': [],
             'conf_path': None,
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
        create_job_flow_main(['--quiet'],
                             {'conf_path': False,
                              's3_sync_wait_time': 0,
                              's3_scratch_uri': 's3://walrus/tmp'},
                              print_job_flow_id=False)
        self.assertEqual(list(self.mock_emr_job_flows.keys()), ['j-MOCKJOBFLOW0'])
