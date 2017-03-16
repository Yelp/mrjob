# Copyright 2009-2013 Yelp and Contributors
# Copyright 2015-2017 Yelp
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
            _runner_kwargs(), {
                'additional_emr_info': None,
                'applications': [],
                'bootstrap': [],
                'bootstrap_actions': [],
                'bootstrap_cmds': [],
                'bootstrap_files': [],
                'bootstrap_mrjob': None,
                'bootstrap_python': None,
                'bootstrap_python_packages': [],
                'bootstrap_scripts': [],
                'bootstrap_spark': None,
                'cloud_fs_sync_secs': None,
                'cloud_log_dir': None,
                'cloud_tmp_dir': None,
                'cloud_upload_part_size': None,
                'conf_paths': None,
                'core_instance_bid_price': None,
                'core_instance_type': None,
                'ec2_key_pair': None,
                'emr_api_params': None,
                'emr_configurations': None,
                'emr_endpoint': None,
                'enable_emr_debugging': None,
                'iam_endpoint': None,
                'iam_instance_profile': None,
                'iam_service_role': None,
                'image_version': None,
                'instance_type': None,
                'label': None,
                'master_instance_bid_price': None,
                'master_instance_type': None,
                'max_hours_idle': None,
                'mins_to_end_of_hour': None,
                'num_core_instances': None,
                'num_ec2_instances': None,
                'num_task_instances': None,
                'owner': None,
                'pool_clusters': None,
                'pool_name': None,
                'region': None,
                'release_label': None,
                's3_endpoint': None,
                'subnet': None,
                'tags': None,
                'task_instance_bid_price': None,
                'task_instance_type': None,
                'visible_to_all_users': None,
                'zone': None,
            })

    def test_create_cluster(self):
        self.add_mock_s3_data({'walrus': {}})
        self.monkey_patch_argv(
            '--quiet', '--no-conf',
            '--cloud-fs-sync-secs', '0',
            '--cloud-tmp-dir', 's3://walrus/tmp')
        self.monkey_patch_stdout()
        create_cluster_main()
        self.assertEqual(list(self.mock_emr_clusters.keys()),
                         ['j-MOCKCLUSTER0'])
        self.assertEqual(sys.stdout.getvalue(), b'j-MOCKCLUSTER0\n')

    # --tag was supported as a switch but not actually being applied
    # to the cluster; see #1085
    def test_tags(self):
        self.add_mock_s3_data({'walrus': {}})
        self.monkey_patch_argv(
            '--quiet', '--no-conf',
            '--cloud-fs-sync-secs', '0',
            '--cloud-tmp-dir', 's3://walrus/tmp',
            '--tag', 'tag_one=foo',
            '--tag', 'tag_two=bar',
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
