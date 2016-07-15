# Copyright 2009-2013 Yelp and Contributors
# Copyright 2015-2016 Yelp
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
"""Test the create-cluster script"""
import sys

from mrjob.tools.emr.create_cluster import main as create_cluster_main
from mrjob.tools.emr.create_cluster import _runner_kwargs

from tests.mockboto import MockEmrObject
from tests.tools.emr import ToolTestCase


class ClusterInspectionTestCase(ToolTestCase):

    maxDiff = None

    def test_runner_kwargs(self):
        self.monkey_patch_argv('--quiet')

        self.assertEqual(
            _runner_kwargs(),
            {'additional_emr_info': None,
             'ami_version': None,
             'aws_availability_zone': None,
             'aws_region': None,
             'bootstrap': None,
             'bootstrap_actions': [],
             'bootstrap_cmds': [],
             'bootstrap_files': [],
             'bootstrap_mrjob': None,
             'bootstrap_python': None,
             'bootstrap_python_packages': [],
             'bootstrap_scripts': [],
             'conf_paths': None,
             'cluster_id': None,
             'ec2_core_instance_bid_price': None,
             'ec2_core_instance_type': None,
             'ec2_instance_type': None,
             'ec2_key_pair': None,
             'ec2_master_instance_bid_price': None,
             'ec2_master_instance_type': None,
             'ec2_task_instance_bid_price': None,
             'ec2_task_instance_type': None,
             'emr_api_params': {},
             'emr_applications': [],
             'emr_configurations': [],
             'emr_endpoint': None,
             'emr_tags': {},
             'enable_emr_debugging': None,
             'iam_endpoint': None,
             'iam_instance_profile': None,
             'iam_service_role': None,
             'label': None,
             'mins_to_end_of_hour': None,
             'max_hours_idle': None,
             'num_ec2_core_instances': None,
             'num_ec2_instances': None,
             'num_ec2_task_instances': None,
             'owner': None,
             'pool_clusters': None,
             'pool_emr_job_flows': None,
             'pool_name': None,
             'release_label': None,
             's3_endpoint': None,
             's3_log_uri': None,
             's3_scratch_uri': None,
             's3_sync_wait_time': None,
             's3_tmp_dir': None,
             's3_upload_part_size': None,
             'subnet': None,
             'visible_to_all_users': None,
             })

    def test_create_cluster(self):
        self.add_mock_s3_data({'walrus': {}})
        self.monkey_patch_argv(
            '--quiet', '--no-conf',
            '--s3-sync-wait-time', '0',
            '--s3-tmp-dir', 's3://walrus/tmp')
        self.monkey_patch_stdout()
        create_cluster_main()
        self.assertEqual(list(self.mock_emr_clusters.keys()),
                         ['j-MOCKCLUSTER0'])
        self.assertEqual(sys.stdout.getvalue(), b'j-MOCKCLUSTER0\n')

    # emr_tags was supported as a switch but not actually being applied
    # to the cluster; see #1085
    def test_emr_tags(self):
        self.add_mock_s3_data({'walrus': {}})
        self.monkey_patch_argv(
            '--quiet', '--no-conf',
            '--s3-sync-wait-time', '0',
            '--s3-scratch-uri', 's3://walrus/tmp',
            '--emr-tag', 'tag_one=foo',
            '--emr-tag', 'tag_two=bar',
        )
        self.monkey_patch_stdout()
        create_cluster_main()
        self.assertEqual(list(self.mock_emr_clusters.keys()),
                         ['j-MOCKCLUSTER0'])

        mock_cluster = self.mock_emr_clusters['j-MOCKCLUSTER0']
        self.assertEqual(mock_cluster.tags, [
            MockEmrObject(key='tag_one', value='foo'),
            MockEmrObject(key='tag_two', value='bar'),
        ])
