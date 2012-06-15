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

"""Test the job flow pooling tool"""

from __future__ import with_statement

from optparse import OptionError

from mrjob.emr import EMRJobRunner
from mrjob.tools.emr.job_flow_pool import main as pool_main
from mrjob.tools.emr.job_flow_pool import make_option_parser
from mrjob.tools.emr.job_flow_pool import parse_args
from mrjob.tools.emr.job_flow_pool import runner_kwargs

from tests.tools.emr import ToolTestCase


class PoolingToolTestCase(ToolTestCase):

    def test_bad_args(self):
        self.monkey_patch_argv('bad_arg')
        self.assertRaises(OptionError, parse_args, make_option_parser())

    def test_runner_kwargs(self):
        self.monkey_patch_argv('--verbose')
        self.assertEqual(
            runner_kwargs(parse_args(make_option_parser())),
            {'aws_availability_zone': None,
             'bootstrap_actions': [],
             'bootstrap_cmds': [],
             'bootstrap_files': [],
             'bootstrap_mrjob': None,
             'bootstrap_python_packages': [],
             'conf_paths': None,
             'ec2_core_instance_type': None,
             'ec2_instance_type': None,
             'ec2_key_pair': None,
             'ec2_key_pair_file': None,
             'ec2_master_instance_type': None,
             'emr_endpoint': None,
             'emr_job_flow_pool_name': None,
             'hadoop_version': None,
             'label': None,
             'num_ec2_instances': None,
             'owner': None})

    def test_list_job_flows(self):
        self.make_job_flow(pool_emr_job_flows=True)
        self.monkey_patch_argv(
            '--quiet', '--no-conf',
            '-a')
        self.monkey_patch_stdout()

        pool_main()

        value = self.stdout.getvalue()
        self.assertIn('j-MOCKJOBFLOW0', value)
        self.assertIn('--\ndefault\n--', value)

    def test_find_job_flow(self):
        jf_id = self.make_job_flow(pool_emr_job_flows=True)
        emr_conn = EMRJobRunner(conf_paths=[]).make_emr_conn()

        for i in range(3):
            emr_conn.simulate_progress(jf_id)

        self.monkey_patch_argv(
            '--quiet', '--no-conf',
            '-f')
        self.monkey_patch_stdout()

        pool_main()

        value = self.stdout.getvalue()
        self.assertIn('j-MOCKJOBFLOW0', value)

    def test_terminate_pool(self):
        jf_id = self.make_job_flow(pool_emr_job_flows=True)
        emr_conn = EMRJobRunner(conf_paths=[]).make_emr_conn()

        for i in range(3):
            emr_conn.simulate_progress(jf_id)

        self.monkey_patch_argv(
            '--quiet', '--no-conf',
            '-t', jf_id)
        self.monkey_patch_stdout()

        pool_main()

        value = self.stdout.getvalue()
        self.assertIn('j-MOCKJOBFLOW0', value)
