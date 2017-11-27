# -*- coding: utf-8 -*-
# Copyright 2009-2016 Yelp and Contributors
# Copyright 2017 Yelp
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
"""Tests for EMRJobRunner.

Tests for cluster pooling are in test_emr_pooling.py
"""
import copy
import getpass
import json
import os
import os.path
import posixpath
import sys
import time
from io import BytesIO
from shutil import make_archive

import boto3
from botocore.exceptions import ClientError
from botocore.exceptions import ParamValidationError

import mrjob
import mrjob.emr
from mrjob.aws import _boto3_paginate
from mrjob.compat import version_gte
from mrjob.emr import EMRJobRunner
from mrjob.emr import _3_X_SPARK_BOOTSTRAP_ACTION
from mrjob.emr import _3_X_SPARK_SUBMIT
from mrjob.emr import _4_X_COMMAND_RUNNER_JAR
from mrjob.emr import _BAD_BASH_IMAGE_VERSION
from mrjob.emr import _DEFAULT_IMAGE_VERSION
from mrjob.emr import _HUGE_PART_THRESHOLD
from mrjob.emr import _MAX_MINS_IDLE_BOOTSTRAP_ACTION_PATH
from mrjob.emr import _PRE_4_X_STREAMING_JAR
from mrjob.job import MRJob
from mrjob.parse import parse_s3_uri
from mrjob.pool import _extract_tags
from mrjob.py2 import PY2
from mrjob.py2 import StringIO
from mrjob.step import INPUT
from mrjob.step import OUTPUT
from mrjob.step import StepFailedException
from mrjob.tools.emr.audit_usage import _JOB_KEY_RE
from mrjob.util import cmd_line
from mrjob.util import log_to_stream

import tests.mock_boto3
import tests.mock_boto3.emr
import tests.mock_boto3.s3
from tests.mock_boto3 import MockBoto3TestCase
from tests.mockssh import mock_ssh_dir
from tests.mockssh import mock_ssh_file
from tests.mr_hadoop_format_job import MRHadoopFormatJob
from tests.mr_jar_and_streaming import MRJarAndStreaming
from tests.mr_just_a_jar import MRJustAJar
from tests.mr_null_spark import MRNullSpark
from tests.mr_no_mapper import MRNoMapper
from tests.mr_sort_values import MRSortValues
from tests.mr_spark_jar import MRSparkJar
from tests.mr_spark_script import MRSparkScript
from tests.mr_streaming_and_spark import MRStreamingAndSpark
from tests.mr_two_step_job import MRTwoStepJob
from tests.mr_word_count import MRWordCount
from tests.py2 import Mock
from tests.py2 import call
from tests.py2 import patch
from tests.quiet import logger_disabled
from tests.quiet import no_handlers_for_logger
from tests.sandbox import SandboxedTestCase
from tests.sandbox import mrjob_conf_patcher
from tests.test_hadoop import HadoopExtraArgsTestCase
from tests.test_local import _bash_wrap

# used to match command lines
if PY2:
    PYTHON_BIN = 'python'
    # prior to AMI 4.3.0, we use python2.7
else:
    PYTHON_BIN = 'python3'

# EMR configurations used for testing
# from http://docs.aws.amazon.com/ElasticMapReduce/latest/ReleaseGuide/emr-configure-apps.html  # noqa

# simple configuration
CORE_SITE_EMR_CONFIGURATION = dict(
    Classification='core-site',
    Properties={
        'hadoop.security.groups.cache.secs': '250',
    },
)

# nested configuration
HADOOP_ENV_EMR_CONFIGURATION = dict(
    Classification='hadoop-env',
    Configurations=[
        dict(
            Classification='export',
            Properties={
                'HADOOP_DATANODE_HEAPSIZE': '2048',
                'HADOOP_NAMENODE_OPTS': '-XX:GCTimeRatio=19',
            },
        ),
    ],
    Properties={},
)

# non-normalized version of HADOOP_ENV_EMR_CONFIGURATION
HADOOP_ENV_EMR_CONFIGURATION_VARIANT = dict(
    Classification='hadoop-env',
    Configurations=[
        dict(
            Classification='export',
            Configurations=[],
            Properties={
                'HADOOP_DATANODE_HEAPSIZE': 2048,
                'HADOOP_NAMENODE_OPTS': '-XX:GCTimeRatio=19',
            },
        ),
    ],
)


def _list_all_bootstrap_actions(runner):
    """Get bootstrap action for the runner's cluster as a list."""
    return list(_boto3_paginate(
        'BootstrapActions', runner.make_emr_client(), 'list_bootstrap_actions',
        ClusterId=runner.get_cluster_id()))


def _list_all_steps(runner):
    """Get steps for the runner's cluster as a list (in forward order)"""
    return list(reversed(list(_boto3_paginate(
        'Steps', runner.make_emr_client(), 'list_steps',
        ClusterId=runner.get_cluster_id()))))


def _extract_non_mrjob_tags(cluster):
    """get tags from a cluster as a dict, excluding tags starting with
    ``__mrjob_``"""
    return {k: v for k, v in _extract_tags(cluster).items()
            if not k.startswith('__mrjob_')}


class EMRJobRunnerEndToEndTestCase(MockBoto3TestCase):

    MRJOB_CONF_CONTENTS = dict(
        runners=dict(
            emr=dict(
                additional_emr_info=dict(key='value'),
                check_cluster_every=0.00,
                cloud_fs_sync_secs=0.00,
                pool_clusters=False,  # so we can test cleanup
            ),
        ),
    )

    def test_end_to_end(self):
        # read from STDIN, a local file, and a remote file
        stdin = BytesIO(b'foo\nbar\n')

        local_input_path = os.path.join(self.tmp_dir, 'input')
        with open(local_input_path, 'wb') as local_input_file:
            local_input_file.write(b'bar\nqux\n')

        remote_input_path = 's3://walrus/data/foo'
        self.add_mock_s3_data({'walrus': {'data/foo': b'foo\n'}})

        # setup fake output
        self.mock_emr_output = {('j-MOCKCLUSTER0', 1): [
            b'1\t"qux"\n2\t"bar"\n', b'2\t"foo"\n5\tnull\n']}

        mr_job = MRHadoopFormatJob(['-r', 'emr', '-v',
                                    '-', local_input_path, remote_input_path,
                                    '--jobconf', 'x=y'])
        mr_job.sandbox(stdin=stdin)

        local_tmp_dir = None
        results = []

        mock_s3_fs_snapshot = copy.deepcopy(self.mock_s3_fs)

        with mr_job.make_runner() as runner:
            self.assertIsInstance(runner, EMRJobRunner)

            # make sure that initializing the runner doesn't affect S3
            # (Issue #50)
            self.assertEqual(mock_s3_fs_snapshot, self.mock_s3_fs)

            # make sure AdditionalInfo was JSON-ified from the config file.
            # checked now because you can't actually read it from the cluster
            # on real EMR.
            self.assertEqual(runner._opts['additional_emr_info'],
                             '{"key": "value"}')

            # keep track of which steps we waited for
            runner._wait_for_step_to_complete = Mock(
                wraps=runner._wait_for_step_to_complete)

            runner.run()

            results.extend(mr_job.parse_output(runner.cat_output()))

            local_tmp_dir = runner._get_local_tmp_dir()
            # make sure cleanup hasn't happened yet
            self.assertTrue(os.path.exists(local_tmp_dir))
            self.assertTrue(any(runner.fs.ls(runner.get_output_dir())))

            cluster = runner._describe_cluster()

            name_match = _JOB_KEY_RE.match(cluster['Name'])
            self.assertEqual(name_match.group(1), 'mr_hadoop_format_job')
            self.assertEqual(name_match.group(2), getpass.getuser())

            # make sure our input and output formats are attached to
            # the correct steps
            steps = _list_all_steps(runner)

            step_0_args = steps[0]['Config']['Args']
            step_1_args = steps[1]['Config']['Args']

            self.assertIn('-inputformat', step_0_args)
            self.assertNotIn('-outputformat', step_0_args)
            self.assertNotIn('-inputformat', step_1_args)
            self.assertIn('-outputformat', step_1_args)

            # make sure jobconf got through
            self.assertIn('-D', step_0_args)
            self.assertIn('x=y', step_0_args)
            self.assertIn('-D', step_1_args)
            # job overrides jobconf in step 1
            self.assertIn('x=z', step_1_args)

            # make sure mrjob.zip is created and uploaded as a bootstrap file
            self.assertTrue(os.path.exists(runner._mrjob_zip_path))
            self.assertIn(runner._mrjob_zip_path,
                          runner._upload_mgr.path_to_uri())
            self.assertIn(runner._mrjob_zip_path,
                          runner._bootstrap_dir_mgr.paths())

        self.assertEqual(sorted(results),
                         [(1, 'qux'), (2, 'bar'), (2, 'foo'), (5, None)])

        # make sure cleanup happens
        self.assertFalse(os.path.exists(local_tmp_dir))
        self.assertFalse(any(runner.fs.ls(runner.get_output_dir())))

        # job should get terminated
        for _ in range(10):
            self.simulate_emr_progress(runner.get_cluster_id())

        cluster = runner._describe_cluster()
        self.assertEqual(cluster['Status']['State'], 'TERMINATED')

        # did we wait for steps in correct order? (regression test for #1316)
        step_ids = [
            c[0][0] for c in runner._wait_for_step_to_complete.call_args_list]
        self.assertEqual(step_ids, [step['Id'] for step in steps])

    def test_failed_job(self):
        mr_job = MRTwoStepJob(['-r', 'emr', '-v'])
        mr_job.sandbox()

        self.add_mock_s3_data({'walrus': {}})
        self.mock_emr_failures = set([('j-MOCKCLUSTER0', 0)])

        with no_handlers_for_logger('mrjob.emr'):
            stderr = StringIO()
            log_to_stream('mrjob.emr', stderr)

            with mr_job.make_runner() as runner:
                self.assertIsInstance(runner, EMRJobRunner)

                self.assertRaises(StepFailedException, runner.run)
                self.assertIn('\n  FAILED\n',
                              stderr.getvalue())

                for _ in range(10):
                    self.simulate_emr_progress(runner.get_cluster_id())

                cluster = runner._describe_cluster()
                self.assertEqual(cluster['Status']['State'],
                                 'TERMINATED_WITH_ERRORS')

            # job should get terminated on cleanup
            cluster_id = runner.get_cluster_id()
            for _ in range(10):
                self.simulate_emr_progress(cluster_id)

        cluster = runner._describe_cluster()
        self.assertEqual(cluster['Status']['State'], 'TERMINATED_WITH_ERRORS')

    def _test_cloud_tmp_cleanup(self, mode, tmp_len, log_len):
        self.add_mock_s3_data({'walrus': {'logs/j-MOCKCLUSTER0/1': b'1\n'}})
        stdin = BytesIO(b'foo\nbar\n')

        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '--cloud-log-dir', 's3://walrus/logs',
                               '-', '--cleanup', mode])
        mr_job.sandbox(stdin=stdin)

        with mr_job.make_runner() as runner:
            cloud_tmp_dir = runner._opts['cloud_tmp_dir']
            tmp_bucket, _ = parse_s3_uri(cloud_tmp_dir)

            runner.run()

            # this is set and unset before we can get at it unless we do this
            log_bucket, _ = parse_s3_uri(runner._s3_log_dir())

            list(runner.cat_output())

        bucket = runner.fs.get_bucket(tmp_bucket)
        self.assertEqual(len(list(bucket.objects.all())), tmp_len)

        bucket = runner.fs.get_bucket(log_bucket)
        self.assertEqual(len(list(bucket.objects.all())), log_len)

    def test_cleanup_all(self):
        self._test_cloud_tmp_cleanup('ALL', 0, 0)

    def test_cleanup_tmp(self):
        self._test_cloud_tmp_cleanup('TMP', 0, 1)

    def test_cleanup_remote(self):
        self._test_cloud_tmp_cleanup('CLOUD_TMP', 0, 1)

    def test_cleanup_local(self):
        self._test_cloud_tmp_cleanup('LOCAL_TMP', 5, 1)

    def test_cleanup_logs(self):
        self._test_cloud_tmp_cleanup('LOGS', 5, 0)

    def test_cleanup_none(self):
        self._test_cloud_tmp_cleanup('NONE', 5, 1)

    def test_cleanup_combine(self):
        self._test_cloud_tmp_cleanup('LOGS,CLOUD_TMP', 0, 0)

    def test_cleanup_error(self):
        self.assertRaises(ValueError, self._test_cloud_tmp_cleanup,
                          'NONE,LOGS,CLOUD_TMP', 0, 0)
        self.assertRaises(ValueError, self._test_cloud_tmp_cleanup,
                          'GARBAGE', 0, 0)


class ExistingClusterTestCase(MockBoto3TestCase):

    def test_attach_to_existing_cluster(self):
        emr_client = EMRJobRunner(conf_paths=[]).make_emr_client()
        # create cluster without LogUri, to test Issue #112
        cluster_id = emr_client.run_job_flow(
            Instances=dict(
                InstanceCount=1,
                KeepJobFlowAliveWhenNoSteps=True,
                MasterInstanceType='m1.medium',
            ),
            JobFlowRole='fake-instance-profile',
            Name='Development Cluster',
            ReleaseLabel='emr-5.0.0',
            ServiceRole='fake-service-role',
        )['JobFlowId']

        stdin = BytesIO(b'foo\nbar\n')
        self.mock_emr_output = {(cluster_id, 1): [
            b'1\t"bar"\n1\t"foo"\n2\tnull\n']}

        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '--cluster-id', cluster_id])
        mr_job.sandbox(stdin=stdin)

        results = []
        with mr_job.make_runner() as runner:
            runner.run()

            # Issue 182: don't create the bootstrap script when
            # attaching to another cluster
            self.assertIsNone(runner._master_bootstrap_script_path)

            results.extend(mr_job.parse_output(runner.cat_output()))

        self.assertEqual(sorted(results),
                         [(1, 'bar'), (1, 'foo'), (2, None)])

    def test_dont_take_down_cluster_on_failure(self):
        emr_client = EMRJobRunner(conf_paths=[]).make_emr_client()
        # create cluster without LogUri, to test Issue #112
        cluster_id = emr_client.run_job_flow(
            Instances=dict(
                InstanceCount=1,
                KeepJobFlowAliveWhenNoSteps=True,
                MasterInstanceType='m1.medium',
            ),
            JobFlowRole='fake-instance-profile',
            Name='Development Cluster',
            ReleaseLabel='emr-5.0.0',
            ServiceRole='fake-service-role',
        )['JobFlowId']

        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '--cluster-id', cluster_id])
        mr_job.sandbox()

        self.add_mock_s3_data({'walrus': {}})
        self.mock_emr_failures = set([('j-MOCKCLUSTER0', 0)])

        with mr_job.make_runner() as runner:
            self.assertIsInstance(runner, EMRJobRunner)
            self.prepare_runner_for_ssh(runner)
            with logger_disabled('mrjob.emr'):
                self.assertRaises(StepFailedException, runner.run)

            for _ in range(10):
                self.simulate_emr_progress(runner.get_cluster_id())

            cluster = runner._describe_cluster()
            self.assertEqual(cluster['Status']['State'], 'WAITING')

        # job shouldn't get terminated by cleanup
        for _ in range(10):
            self.simulate_emr_progress(runner.get_cluster_id())

        cluster = runner._describe_cluster()
        self.assertEqual(cluster['Status']['State'], 'WAITING')


class VisibleToAllUsersTestCase(MockBoto3TestCase):

    def test_defaults(self):
        cluster = self.run_and_get_cluster()
        self.assertEqual(cluster['VisibleToAllUsers'], True)

    def test_no_visible(self):
        cluster = self.run_and_get_cluster('--no-visible-to-all-users')
        self.assertEqual(cluster['VisibleToAllUsers'], False)

    def test_visible(self):
        cluster = self.run_and_get_cluster('--visible-to-all-users')
        self.assertEqual(cluster['VisibleToAllUsers'], True)

    def test_force_to_bool(self):
        VISIBLE_MRJOB_CONF = {'runners': {'emr': {
            'check_cluster_every': 0.00,
            'cloud_fs_sync_secs': 0.00,
            'visible_to_all_users': 1,  # should be True
        }}}

        with mrjob_conf_patcher(VISIBLE_MRJOB_CONF):
            cluster = self.run_and_get_cluster()
            self.assertEqual(cluster['VisibleToAllUsers'], True)

    def test_mock_boto3_does_not_force_to_bool(self):
        # make sure that mrjob is converting to bool, not mock_boto3
        self.assertRaises(ParamValidationError,
                          self.run_and_get_cluster,
                          '--extra-cluster-param', 'VisibleToAllUsers=1')


class SubnetTestCase(MockBoto3TestCase):

    def test_defaults(self):
        cluster = self.run_and_get_cluster()
        self.assertEqual(cluster['Ec2InstanceAttributes'].get('Ec2SubnetId'),
                         None)

    def test_subnet_option(self):
        cluster = self.run_and_get_cluster('--subnet', 'subnet-ffffffff')
        self.assertEqual(cluster['Ec2InstanceAttributes'].get('Ec2SubnetId'),
                         'subnet-ffffffff')

    def test_empty_string_means_no_subnet(self):
        cluster = self.run_and_get_cluster('--subnet', '')
        self.assertEqual(cluster['Ec2InstanceAttributes'].get('Ec2SubnetId'),
                         None)

    def test_subnets_option(self):
        instance_fleets = [dict(
            InstanceFleetType='MASTER',
            InstanceTypeConfigs=[dict(InstanceType='m1.medium')],
            TargetOnDemandCapacity=1)]

        cluster = self.run_and_get_cluster(
            '--subnets', 'subnet-ffffffff,subnet-eeeeeeee',
            '--instance-fleets', json.dumps(instance_fleets))

        self.assertIn(cluster['Ec2InstanceAttributes'].get('Ec2SubnetId'),
                      ['subnet-ffffffff', 'subnet-eeeeeeee'])


class IAMTestCase(MockBoto3TestCase):

    def setUp(self):
        super(IAMTestCase, self).setUp()

        # wrap boto3.client() so we can see if it was called
        self.start(patch('boto3.client', wraps=boto3.client))

    def test_role_auto_creation(self):
        cluster = self.run_and_get_cluster()
        self.assertTrue(boto3.client.called)

        # check instance_profile
        instance_profile_name = (
            cluster['Ec2InstanceAttributes']['IamInstanceProfile'])
        self.assertIsNotNone(instance_profile_name)
        self.assertTrue(instance_profile_name.startswith('mrjob-'))
        self.assertIn(instance_profile_name, self.mock_iam_instance_profiles)
        self.assertIn(instance_profile_name, self.mock_iam_roles)
        self.assertIn(instance_profile_name,
                      self.mock_iam_role_attached_policies)

        # check service_role
        service_role_name = cluster['ServiceRole']
        self.assertIsNotNone(service_role_name)
        self.assertTrue(service_role_name.startswith('mrjob-'))
        self.assertIn(service_role_name, self.mock_iam_roles)
        self.assertIn(service_role_name,
                      self.mock_iam_role_attached_policies)

        # instance_profile and service_role should be distinct
        self.assertNotEqual(instance_profile_name, service_role_name)

        # run again, and see if we reuse the roles
        cluster2 = self.run_and_get_cluster()
        self.assertEqual(
            cluster2['Ec2InstanceAttributes']['IamInstanceProfile'],
            instance_profile_name)
        self.assertEqual(cluster2['ServiceRole'], service_role_name)

    def test_iam_instance_profile_option(self):
        cluster = self.run_and_get_cluster(
            '--iam-instance-profile', 'EMR_EC2_DefaultRole')
        self.assertTrue(boto3.client.called)

        self.assertEqual(
            cluster['Ec2InstanceAttributes']['IamInstanceProfile'],
            'EMR_EC2_DefaultRole')

    def test_iam_service_role_option(self):
        cluster = self.run_and_get_cluster(
            '--iam-service-role', 'EMR_DefaultRole')
        self.assertTrue(boto3.client.called)

        self.assertEqual(cluster['ServiceRole'], 'EMR_DefaultRole')

    def test_both_iam_options(self):
        cluster = self.run_and_get_cluster(
            '--iam-instance-profile', 'EMR_EC2_DefaultRole',
            '--iam-service-role', 'EMR_DefaultRole')

        # users with limited access may not be able to connect to the IAM API.
        # This gives them a plan B
        self.assertFalse(any(args == ('iam',)
                             for args, kwargs in boto3.client.call_args_list))

        self.assertEqual(
            cluster['Ec2InstanceAttributes']['IamInstanceProfile'],
            'EMR_EC2_DefaultRole')
        self.assertEqual(cluster['ServiceRole'], 'EMR_DefaultRole')

    def test_no_iam_access(self):
        boto3_client = boto3.client

        def forbidding_boto3_client(service_name, **kwargs):
            if service_name == 'iam':
                raise ClientError(
                    dict(
                        Error=dict(),
                        ResponseMetadata=dict(HTTPStatusCode=403)
                    ), 'WhateverApiCall')
            else:
                # pass through other services
                return boto3_client(service_name, **kwargs)

        self.start(patch('boto3.client', side_effect=forbidding_boto3_client))

        with logger_disabled('mrjob.emr'):
            cluster = self.run_and_get_cluster()

        self.assertTrue(any(args == ('iam',)
                            for args, kwargs in boto3.client.call_args_list))

        self.assertEqual(
            cluster['Ec2InstanceAttributes']['IamInstanceProfile'],
            'EMR_EC2_DefaultRole')
        self.assertEqual(cluster['ServiceRole'], 'EMR_DefaultRole')


class ExtraClusterParamsTestCase(MockBoto3TestCase):

    def test_set_unsupported_json_param(self):
        cluster = self.run_and_get_cluster(
            '--image-version', '3.7.0',
            '--extra-cluster-param', 'SupportedProducts=["mapr-m3"]')

        self.assertIn('mapr-m3', [a['Name'] for a in cluster['Applications']])

    def test_set_unsupported_string_param(self):
        cluster = self.run_and_get_cluster(
            '--extra-cluster-param', 'AutoScalingRole=HankPym')

        self.assertEqual(cluster['AutoScalingRole'], 'HankPym')

    def test_bad_json_param_is_not_a_string(self):
        self.assertRaises(
            ValueError,
            self.run_and_get_cluster,
            '--image-version', '3.7.0',
            '--extra-cluster-param', 'SupportedProducts=[mapr-m3]')

    def test_set_existing_param(self):
        cluster = self.run_and_get_cluster(
            '--extra-cluster-param', 'Name=Dave')

        self.assertEqual(cluster['Name'], 'Dave')

    def test_unset_existing_param(self):
        cluster = self.run_and_get_cluster(
            '--extra-cluster-param', 'LogUri=null')

        self.assertNotIn('LogUri', cluster)

    def test_set_multiple_params(self):
        cluster = self.run_and_get_cluster(
            '--extra-cluster-param', 'AutoScalingRole=HankPym',
            '--extra-cluster-param', 'Name=Dave')

        self.assertEqual(cluster['AutoScalingRole'], 'HankPym')
        self.assertEqual(cluster['Name'], 'Dave')


class DeprecatedEMRAPIParamsTestCase(MockBoto3TestCase):

    # emr_api_param is completely disabled

    def setUp(self):
        super(DeprecatedEMRAPIParamsTestCase, self).setUp()

        self.log = self.start(patch('mrjob.emr.log'))

    def test_param_set(self):
        cluster = self.run_and_get_cluster('--emr-api-param', 'name=Dave')

        self.assertTrue(self.log.warning.called)
        self.assertNotEqual(cluster['Name'], 'Dave')

    def test_param_unset(self):
        cluster = self.run_and_get_cluster('--no-emr-api-param', 'log_uri')

        self.assertTrue(self.log.warning.called)
        self.assertIn('LogUri', cluster)


class AMIAndHadoopVersionTestCase(MockBoto3TestCase):

    def test_default(self):
        with self.make_runner() as runner:
            runner.run()
            self.assertEqual(runner.get_image_version(),
                             _DEFAULT_IMAGE_VERSION)
            self.assertEqual(runner.get_hadoop_version(), '2.7.3')

    def test_ami_version_1_0_no_longer_supported(self):
        with self.make_runner('--image-version', '1.0') as runner:
            self.assertRaises(ClientError, runner._launch)

    def test_ami_version_2_0(self):
        with self.make_runner('--image-version', '2.0') as runner:
            runner.run()
            self.assertEqual(runner.get_image_version(), '2.0.6')
            self.assertEqual(runner.get_hadoop_version(), '0.20.205')

    def test_ami_version_3_0(self):
        with self.make_runner('--image-version', '3.0') as runner:
            runner.run()
            self.assertEqual(runner.get_image_version(), '3.0.4')
            self.assertEqual(runner.get_hadoop_version(), '2.2.0')

    def test_ami_version_3_8_0(self):
        with self.make_runner('--image-version', '3.8.0') as runner:
            runner.run()
            self.assertEqual(runner.get_image_version(), '3.8.0')
            self.assertEqual(runner.get_hadoop_version(), '2.4.0')

    def test_ami_version_4_0_0_via_release_label_option(self):
        # the way EMR wants us to set 4.x AMI versions
        with self.make_runner('--release-label', 'emr-4.0.0') as runner:
            runner.run()
            self.assertEqual(runner.get_image_version(), '4.0.0')
            self.assertEqual(runner.get_hadoop_version(), '2.6.0')

            cluster = runner._describe_cluster()
            self.assertEqual(cluster.get('ReleaseLabel'), 'emr-4.0.0')
            self.assertEqual(cluster.get('RequestedAmiVersion'), None)
            self.assertEqual(cluster.get('RunningAmiVersion'), None)

    def test_ami_version_4_0_0_via_image_version_option(self):
        # mrjob should also be smart enough to handle this
        with self.make_runner('--image-version', '4.0.0') as runner:
            runner.run()
            self.assertEqual(runner.get_image_version(), '4.0.0')
            self.assertEqual(runner.get_hadoop_version(), '2.6.0')

            cluster = runner._describe_cluster()
            self.assertEqual(cluster.get('ReleaseLabel'), 'emr-4.0.0')
            self.assertEqual(cluster.get('RequestedAmiVersion'), None)
            self.assertEqual(cluster.get('RunningAmiVersion'), None)

    def test_hadoop_version_option_does_nothing(self):
        with logger_disabled('mrjob.emr'):
            with self.make_runner('--hadoop-version', '1.2.3.4') as runner:
                runner.run()
                self.assertEqual(runner.get_image_version(),
                                 _DEFAULT_IMAGE_VERSION)
                self.assertEqual(runner.get_hadoop_version(), '2.7.3')


class AvailabilityZoneTestCase(MockBoto3TestCase):

    MRJOB_CONF_CONTENTS = {'runners': {'emr': {
        'check_cluster_every': 0.00,
        'cloud_fs_sync_secs': 0.00,
        'zone': 'PUPPYLAND',
    }}}

    def test_availability_zone_config(self):
        with self.make_runner() as runner:
            runner.run()

            cluster = runner._describe_cluster()
            self.assertEqual(
                cluster['Ec2InstanceAttributes'].get('Ec2AvailabilityZone'),
                'PUPPYLAND')


class EnableDebuggingTestCase(MockBoto3TestCase):

    def test_debugging_works(self):
        with self.make_runner('--enable-emr-debugging') as runner:
            runner.run()

            steps = _list_all_steps(runner)
            self.assertEqual(steps[0]['Name'], 'Setup Hadoop Debugging')


class RegionTestCase(MockBoto3TestCase):

    def test_default(self):
        runner = EMRJobRunner()
        self.assertEqual(runner._opts['region'], 'us-west-2')

    def test_explicit_region(self):
        runner = EMRJobRunner(region='us-east-1')
        self.assertEqual(runner._opts['region'], 'us-east-1')

    def test_cannot_be_empty(self):
        runner = EMRJobRunner(region='')
        self.assertEqual(runner._opts['region'], 'us-west-2')


class TmpBucketTestCase(MockBoto3TestCase):

    def assert_new_tmp_bucket(self, location, **runner_kwargs):
        """Assert that if we create an EMRJobRunner with the given keyword
        args, it'll create a new tmp bucket with the given location
        constraint.
        """
        existing_bucket_names = set(self.mock_s3_fs)

        runner = EMRJobRunner(conf_paths=[], **runner_kwargs)
        runner._create_s3_tmp_bucket_if_needed()

        bucket_name, path = parse_s3_uri(runner._opts['cloud_tmp_dir'])

        self.assertTrue(bucket_name.startswith('mrjob-'))
        self.assertNotIn(bucket_name, existing_bucket_names)
        self.assertEqual(path, 'tmp/')

        self.assertEqual(self.mock_s3_fs[bucket_name]['location'], location)

    def test_default(self):
        self.assert_new_tmp_bucket('us-west-2')

    def test_us_west_1(self):
        self.assert_new_tmp_bucket('us-west-1',
                                   region='us-west-1')

    def test_us_east_1(self):
        # location should be blank
        self.assert_new_tmp_bucket('',
                                   region='us-east-1')

    def test_reuse_mrjob_bucket_in_same_region(self):
        self.add_mock_s3_data({'mrjob-1': {}}, location='us-west-2')

        runner = EMRJobRunner()
        self.assertEqual(runner._opts['cloud_tmp_dir'],
                         's3://mrjob-1/tmp/')

    def test_ignore_mrjob_bucket_in_different_region(self):
        # this tests 687
        self.add_mock_s3_data({'mrjob-1': {}}, location='')

        self.assert_new_tmp_bucket('us-west-2')

    def test_ignore_non_mrjob_bucket_in_different_region(self):
        self.add_mock_s3_data({'walrus': {}}, location='us-west-2')

        self.assert_new_tmp_bucket('us-west-2')

    def test_reuse_mrjob_bucket_in_us_east_1(self):
        # us-east-1 is special because the location "constraint" for its
        # buckets is '', not 'us-east-1'
        self.add_mock_s3_data({'mrjob-1': {}}, location='')

        runner = EMRJobRunner(region='us-east-1')

        self.assertEqual(runner._opts['cloud_tmp_dir'],
                         's3://mrjob-1/tmp/')

    def test_explicit_tmp_uri(self):
        self.add_mock_s3_data({'walrus': {}}, location='us-west-2')

        runner = EMRJobRunner(cloud_tmp_dir='s3://walrus/tmp/')

        self.assertEqual(runner._opts['cloud_tmp_dir'],
                         's3://walrus/tmp/')

    def test_cross_region_explicit_tmp_uri(self):
        self.add_mock_s3_data({'walrus': {}}, location='us-west-2')

        runner = EMRJobRunner(region='us-west-1',
                              cloud_tmp_dir='s3://walrus/tmp/')

        self.assertEqual(runner._opts['cloud_tmp_dir'],
                         's3://walrus/tmp/')

        # tmp bucket shouldn't influence region (it did in 0.4.x)
        self.assertEqual(runner._opts['region'], 'us-west-1')


# TODO: some of this should be moved to tests/test_cloud.py
class InstanceGroupAndFleetTestCase(MockBoto3TestCase):

    maxDiff = None

    def _test_instance_groups(self, opts, **expected):
        """Run a job with the given option dictionary, and check for
        for instance, number, and optional bid price for each instance role.

        Specify expected instance group info like:

        <role>=(num_instances, instance_type, bid_price)
        """
        runner = EMRJobRunner(**opts)
        cluster_id = runner.make_persistent_cluster()

        emr_client = runner.make_emr_client()
        instance_groups = _boto3_paginate(
            'InstanceGroups', emr_client, 'list_instance_groups',
            ClusterId=cluster_id)

        # convert actual instance groups to dicts. (This gets around any
        # assumptions about the order the API returns instance groups in;
        # see #1316)
        role_to_actual = {}
        for ig in instance_groups:
            info = dict(
                (field, ig.get(field))
                for field in ('BidPrice', 'InstanceType',
                              'Market', 'RequestedInstanceCount'))

            role_to_actual[ig['InstanceGroupType']] = info

        # convert expected to dicts
        role_to_expected = {}
        for role, (num, instance_type, bid_price) in expected.items():
            expected = dict(
                BidPrice=(bid_price if bid_price else None),
                InstanceType=instance_type,
                Market=(u'SPOT' if bid_price else u'ON_DEMAND'),
                RequestedInstanceCount=num,
            )

            role_to_expected[role.upper()] = expected

        self.assertEqual(role_to_actual, role_to_expected)

    def _test_uses_instance_fleets(self, opts):
        runner = EMRJobRunner(**opts)
        cluster_id = runner.make_persistent_cluster()

        emr_client = runner.make_emr_client()
        cluster = emr_client.describe_cluster(ClusterId=cluster_id)['Cluster']
        self.assertEqual(cluster['InstanceCollectionType'], 'INSTANCE_FLEET')

    def set_in_mrjob_conf(self, **kwargs):
        emr_opts = copy.deepcopy(self.MRJOB_CONF_CONTENTS)
        emr_opts['runners']['emr'].update(kwargs)
        patcher = mrjob_conf_patcher(emr_opts)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_defaults(self):
        self._test_instance_groups(
            {},
            master=(1, 'm1.medium', None))

    def test_instance_type_single_instance(self):
        self._test_instance_groups(
            {'instance_type': 'c1.xlarge'},
            master=(1, 'c1.xlarge', None))

    def test_instance_type_multiple_instances(self):
        self._test_instance_groups(
            {'instance_type': 'c1.xlarge', 'num_core_instances': 2},
            core=(2, 'c1.xlarge', None),
            master=(1, 'm1.medium', None))

    def test_explicit_master_and_core_instance_types(self):
        self._test_instance_groups(
            {'master_instance_type': 'm1.large'},
            master=(1, 'm1.large', None))

        self._test_instance_groups(
            {'core_instance_type': 'm2.xlarge',
             'num_core_instances': 2},
            core=(2, 'm2.xlarge', None),
            master=(1, 'm1.medium', None))

        self._test_instance_groups(
            {'master_instance_type': 'm1.large',
             'core_instance_type': 'm2.xlarge',
             'num_core_instances': 2},
            core=(2, 'm2.xlarge', None),
            master=(1, 'm1.large', None))

    def test_2_x_ami_defaults_single_node(self):
        # m1.small still works with Hadoop 1, and it's cheaper
        self._test_instance_groups(
            dict(image_version='2.4.11'),
            master=(1, 'm1.small', None))

    def test_2_x_ami_defaults_multiple_nodes(self):
        self._test_instance_groups(
            dict(image_version='2.4.11', num_core_instances=2),
            core=(2, 'm1.small', None),
            master=(1, 'm1.small', None))

    def test_release_label_hides_image_version(self):
        self._test_instance_groups(
            dict(release_label='emr-4.0.0', image_version='2.4.11'),
            master=(1, 'm1.medium', None))

    def test_spark_defaults_single_node(self):
        # Spark needs at least m1.large
        self._test_instance_groups(
            dict(image_version='4.0.0', applications=['Spark']),
            master=(1, 'm1.large', None))

    def test_spark_defaults_multiple_nodes(self):
        # Spark can get away with m1.medium for the resource manager
        self._test_instance_groups(
            dict(image_version='4.0.0',
                 applications=['Spark'],
                 num_core_instances=2),
            core=(2, 'm1.large', None),
            master=(1, 'm1.medium', None))

    def test_explicit_instance_types_take_precedence(self):
        self._test_instance_groups(
            {'instance_type': 'c1.xlarge',
             'master_instance_type': 'm1.large'},
            master=(1, 'm1.large', None))

        self._test_instance_groups(
            {'instance_type': 'c1.xlarge',
             'master_instance_type': 'm1.large',
             'core_instance_type': 'm2.xlarge',
             'num_core_instances': 2},
            core=(2, 'm2.xlarge', None),
            master=(1, 'm1.large', None))

    def test_cmd_line_opts_beat_mrjob_conf(self):
        # set instance_type in mrjob.conf, 1 instance
        self.set_in_mrjob_conf(instance_type='c1.xlarge')

        self._test_instance_groups(
            {},
            master=(1, 'c1.xlarge', None))

        self._test_instance_groups(
            {'master_instance_type': 'm1.large'},
            master=(1, 'm1.large', None))

        # set instance_type in mrjob.conf, 3 instances
        self.set_in_mrjob_conf(instance_type='c1.xlarge',
                               num_core_instances=2)

        self._test_instance_groups(
            {},
            core=(2, 'c1.xlarge', None),
            master=(1, 'm1.medium', None))

        self._test_instance_groups(
            {'master_instance_type': 'm1.large',
             'core_instance_type': 'm2.xlarge'},
            core=(2, 'm2.xlarge', None),
            master=(1, 'm1.large', None))

        # set master in mrjob.conf, 1 instance
        self.set_in_mrjob_conf(master_instance_type='m1.large')

        self._test_instance_groups(
            {},
            master=(1, 'm1.large', None))

        self._test_instance_groups(
            {'instance_type': 'c1.xlarge'},
            master=(1, 'c1.xlarge', None))

        # set master and worker in mrjob.conf, 2 instances
        self.set_in_mrjob_conf(master_instance_type='m1.large',
                               core_instance_type='m2.xlarge',
                               num_core_instances=2)

        self._test_instance_groups(
            {},
            core=(2, 'm2.xlarge', None),
            master=(1, 'm1.large', None))

        self._test_instance_groups(
            {'instance_type': 'c1.xlarge'},
            core=(2, 'c1.xlarge', None),
            master=(1, 'm1.large', None))

    def test_zero_core_instances(self):
        self._test_instance_groups(
            {'master_instance_type': 'c1.medium',
             'num_core_instances': 0},
            master=(1, 'c1.medium', None))

    def test_core_spot_instances(self):
        self._test_instance_groups(
            {'master_instance_type': 'm1.large',
             'core_instance_type': 'c1.medium',
             'core_instance_bid_price': '0.20',
             'num_core_instances': 5},
            core=(5, 'c1.medium', '0.20'),
            master=(1, 'm1.large', None))

    def test_core_on_demand_instances(self):
        self._test_instance_groups(
            {'master_instance_type': 'm1.large',
             'core_instance_type': 'c1.medium',
             'num_core_instances': 5},
            core=(5, 'c1.medium', None),
            master=(1, 'm1.large', None))

    def test_core_and_task_on_demand_instances(self):
        self._test_instance_groups(
            {'master_instance_type': 'm1.large',
             'core_instance_type': 'c1.medium',
             'num_core_instances': 5,
             'task_instance_type': 'm2.xlarge',
             'num_task_instances': 20,
             },
            core=(5, 'c1.medium', None),
            master=(1, 'm1.large', None),
            task=(20, 'm2.xlarge', None))

    def test_core_and_task_spot_instances(self):
        self._test_instance_groups(
            {'master_instance_type': 'm1.large',
             'core_instance_type': 'c1.medium',
             'core_instance_bid_price': '0.20',
             'num_core_instances': 10,
             'task_instance_type': 'm2.xlarge',
             'task_instance_bid_price': '1.00',
             'num_task_instances': 20,
             },
            core=(10, 'c1.medium', '0.20'),
            master=(1, 'm1.large', None),
            task=(20, 'm2.xlarge', '1.00'))

        self._test_instance_groups(
            {'master_instance_type': 'm1.large',
             'core_instance_type': 'c1.medium',
             'num_core_instances': 10,
             'task_instance_type': 'm2.xlarge',
             'task_instance_bid_price': '1.00',
             'num_task_instances': 20,
             },
            core=(10, 'c1.medium', None),
            master=(1, 'm1.large', None),
            task=(20, 'm2.xlarge', '1.00'))

    def test_master_and_core_spot_instances(self):
        self._test_instance_groups(
            {'master_instance_type': 'm1.large',
             'master_instance_bid_price': '0.50',
             'core_instance_type': 'c1.medium',
             'core_instance_bid_price': '0.20',
             'num_core_instances': 10,
             },
            core=(10, 'c1.medium', '0.20'),
            master=(1, 'm1.large', '0.50'))

    def test_master_spot_instance(self):
        self._test_instance_groups(
            {'master_instance_type': 'm1.large',
             'master_instance_bid_price': '0.50',
             },
            master=(1, 'm1.large', '0.50'))

    def test_zero_or_blank_bid_price_means_on_demand(self):
        self._test_instance_groups(
            {'master_instance_bid_price': '0',
             },
            master=(1, 'm1.medium', None))

        self._test_instance_groups(
            {'num_core_instances': 3,
             'core_instance_bid_price': '0.00',
             },
            core=(3, 'm1.medium', None),
            master=(1, 'm1.medium', None))

        self._test_instance_groups(
            {'num_core_instances': 3,
             'num_task_instances': 5,
             'task_instance_bid_price': '',
             },
            core=(3, 'm1.medium', None),
            master=(1, 'm1.medium', None),
            task=(5, 'm1.medium', None))

    def test_pass_invalid_bid_prices_through_to_emr(self):
        self.assertRaises(
            ClientError,
            self._test_instance_groups,
            {'master_instance_bid_price': 'all the gold in California'})

    def test_task_type_defaults_to_core_type(self):
        self._test_instance_groups(
            {'core_instance_type': 'c1.medium',
             'num_core_instances': 5,
             'num_task_instances': 20,
             },
            core=(5, 'c1.medium', None),
            master=(1, 'm1.medium', None),
            task=(20, 'c1.medium', None))

    def test_explicit_instance_groups(self):
        self._test_instance_groups(
            dict(
                instance_groups=[dict(
                    InstanceRole='MASTER',
                    InstanceCount=1,
                    InstanceType='c1.medium',
                )],
            ),
            master=(1, 'c1.medium', None))

    def test_explicit_instance_groups_beats_implicit(self):
        # instance_groups overrides specific instance group configs

        self._test_instance_groups(
            dict(
                instance_groups=[dict(
                    InstanceRole='MASTER',
                    InstanceCount=1,
                    InstanceType='c1.medium',
                )],
                master_instance_type='m1.large',
                num_core_instances=3,
            ),
            master=(1, 'c1.medium', None))

    def test_explicit_instance_fleets(self):
        self._test_uses_instance_fleets(
            dict(
                instance_fleets=[dict(
                    InstanceFleetType='MASTER',
                    InstanceTypeConfigs=[dict(InstanceType='m1.medium')],
                    TargetOnDemandCapacity=1)]
            )
        )

    def test_explicit_instance_fleets_beats_instance_groups(self):
        self._test_uses_instance_fleets(
            dict(
                instance_fleets=[dict(
                    InstanceFleetType='MASTER',
                    InstanceTypeConfigs=[dict(InstanceType='m1.medium')],
                    TargetOnDemandCapacity=1)],
                instance_groups=[dict(
                    InstanceRole='MASTER',
                    InstanceCount=1,
                    InstanceType='c1.medium',
                )],
                master_instance_type='m1.large',
                num_core_instances=3,
            ),
        )

    def test_command_line_beats_instance_groups_in_config_file(self):
        self.set_in_mrjob_conf(
            instance_fleets=[dict(
                InstanceFleetType='MASTER',
                InstanceTypeConfigs=[dict(InstanceType='m1.medium')],
                TargetOnDemandCapacity=1)])

        self._test_uses_instance_fleets({})

        self._test_instance_groups(
            dict(num_core_instances=3),
            master=(1, 'm1.medium', None),
            core=(3, 'm1.medium', None)
        )

    def test_command_line_beats_instance_fleets_in_config_file(self):
        self.set_in_mrjob_conf(
            instance_groups=[dict(
                InstanceRole='MASTER',
                InstanceCount=1,
                InstanceType='c1.medium')])

        self._test_instance_groups(
            {},
            master=(1, 'c1.medium', None)
        )

        self._test_instance_groups(
            dict(num_core_instances=3),
            master=(1, 'm1.medium', None),
            core=(3, 'm1.medium', None)
        )


### tests for error parsing ###

BUCKET = 'walrus'
BUCKET_URI = 's3://' + BUCKET + '/'

LOG_DIR = 'j-CLUSTERID/'

GARBAGE = b'GarbageGarbageGarbage\n'

TRACEBACK_START = b'Traceback (most recent call last):\n'

PY_EXCEPTION = b"""  File "<string>", line 1, in <module>
TypeError: 'int' object is not iterable
"""

CHILD_ERR_LINE = (
    b'2010-07-27 18:25:48,397 WARN'
    b' org.apache.hadoop.mapred.TaskTracker (main): Error running child\n')

JAVA_STACK_TRACE = b"""java.lang.OutOfMemoryError: Java heap space
        at org.apache.hadoop.mapred.IFile$Reader.readNextBlock(IFile.java:270)
        at org.apache.hadoop.mapred.IFile$Reader.next(IFile.java:332)
"""

HADOOP_ERR_LINE_PREFIX = (b'2010-07-27 19:53:35,451 ERROR'
                          b' org.apache.hadoop.streaming.StreamJob (main): ')

USEFUL_HADOOP_ERROR = (
    b'Error launching job , Output path already exists :'
    b' Output directory s3://yourbucket/logs/2010/07/23/ already exists'
    b' and is not empty')

BORING_HADOOP_ERROR = b'Job not Successful!'
TASK_ATTEMPTS_DIR = LOG_DIR + 'task-attempts/'

ATTEMPT_0_DIR = TASK_ATTEMPTS_DIR + 'attempt_201007271720_0001_m_000126_0/'
ATTEMPT_1_DIR = TASK_ATTEMPTS_DIR + 'attempt_201007271720_0001_m_000126_0/'


def make_input_uri_line(input_uri):
    return ("2010-07-27 17:55:29,400 INFO"
            " org.apache.hadoop.fs.s3native.NativeS3FileSystem (main):"
            " Opening '%s' for reading\n" % input_uri).encode('utf_8')


class EMREndpointTestCase(MockBoto3TestCase):

    # back when we used boto 2, mrjob used to figure out endpoints itself
    # Now we leave that to boto3, so this is more a benchmark that makes sure
    # mock_boto3 matches boto3 1.4.4

    def test_default_region(self):
        runner = EMRJobRunner(conf_paths=[])
        self.assertEqual(runner._opts['region'], 'us-west-2')
        self.assertEqual(runner.make_emr_client().meta.endpoint_url,
                         'https://us-west-2.elasticmapreduce.amazonaws.com')

    def test_none_region(self):
        # blank region should be treated the same as no region
        runner = EMRJobRunner(conf_paths=[], region=None)
        self.assertEqual(runner._opts['region'], 'us-west-2')
        self.assertEqual(runner.make_emr_client().meta.endpoint_url,
                         'https://us-west-2.elasticmapreduce.amazonaws.com')

    def test_blank_region(self):
        # blank region should be treated the same as no region
        runner = EMRJobRunner(conf_paths=[], region='')
        self.assertEqual(runner._opts['region'], 'us-west-2')
        self.assertEqual(runner.make_emr_client().meta.endpoint_url,
                         'https://us-west-2.elasticmapreduce.amazonaws.com')

    def test_us_east_1(self):
        runner = EMRJobRunner(conf_paths=[], region='us-east-1')
        self.assertEqual(runner._opts['region'], 'us-east-1')
        # boto3 has a special case for us-east-1 for whatever reason
        self.assertEqual(runner.make_emr_client().meta.endpoint_url,
                         'https://elasticmapreduce.us-east-1.amazonaws.com')

    def test_us_west_1(self):
        runner = EMRJobRunner(conf_paths=[], region='us-west-1')
        self.assertEqual(runner._opts['region'], 'us-west-1')
        self.assertEqual(runner.make_emr_client().meta.endpoint_url,
                         'https://us-west-1.elasticmapreduce.amazonaws.com')

    def test_ap_southeast_1(self):
        runner = EMRJobRunner(conf_paths=[], region='ap-southeast-1')
        self.assertEqual(runner._opts['region'], 'ap-southeast-1')
        self.assertEqual(
            runner.make_emr_client().meta.endpoint_url,
            'https://ap-southeast-1.elasticmapreduce.amazonaws.com')

    def test_explicit_endpoint(self):
        runner = EMRJobRunner(conf_paths=[], region='eu-west-2',
                              emr_endpoint='emr-proxy')
        self.assertEqual(runner._opts['region'], 'eu-west-2')
        self.assertEqual(runner.make_emr_client().meta.endpoint_url,
                         'https://emr-proxy')


class TestSSHLs(MockBoto3TestCase):

    def setUp(self):
        super(TestSSHLs, self).setUp()
        self.make_runner()

    def tearDown(self):
        super(TestSSHLs, self).tearDown()

    def make_runner(self):
        self.runner = EMRJobRunner(conf_paths=[])
        self.prepare_runner_for_ssh(self.runner)

    def test_ssh_ls(self):
        self.add_worker()

        mock_ssh_dir('testmaster', 'test')
        mock_ssh_file('testmaster', posixpath.join('test', 'one'), b'')
        mock_ssh_file('testmaster', posixpath.join('test', 'two'), b'')
        mock_ssh_dir('testmaster!testworker0', 'test')
        mock_ssh_file('testmaster!testworker0',
                      posixpath.join('test', 'three'), b'')

        self.assertEqual(
            sorted(self.runner.fs.ls('ssh://testmaster/test')),
            ['ssh://testmaster/test/one', 'ssh://testmaster/test/two'])

        self.assertEqual(
            list(self.runner.fs.ls('ssh://testmaster!testworker0/test')),
            ['ssh://testmaster!testworker0/test/three'])

        # ls() is a generator, so the exception won't fire until we list() it
        self.assertRaises(IOError, list,
                          self.runner.fs.ls('ssh://testmaster/does_not_exist'))


class NoBoto3TestCase(SandboxedTestCase):

    def setUp(self):
        self.start(patch('mrjob.emr.boto3', None))
        self.start(patch('mrjob.fs.s3.boto3', None))

    def test_init(self):
        # merely creating an EMRJobRunner should raise an exception
        # because it'll need to connect to S3 to set cloud_tmp_dir
        self.assertRaises(ImportError, EMRJobRunner, conf_paths=[])


class MasterBootstrapScriptTestCase(MockBoto3TestCase):

    def test_usr_bin_env(self):
        runner = EMRJobRunner(conf_paths=[],
                              bootstrap_mrjob=True,
                              sh_bin='bash -e')

        runner._add_bootstrap_files_for_upload()

        self.assertIsNotNone(runner._master_bootstrap_script_path)
        self.assertTrue(os.path.exists(runner._master_bootstrap_script_path))

        with open(runner._master_bootstrap_script_path) as f:
            lines = [line.rstrip() for line in f]

        self.assertEqual(lines[0], '#!/usr/bin/env bash -e')

    def _test_create_master_bootstrap_script(
            self, image_version=None, expected_python_bin=PYTHON_BIN):

        # create a fake src tarball
        foo_py_path = os.path.join(self.tmp_dir, 'foo.py')
        with open(foo_py_path, 'w'):
            pass

        # test bootstrap with a file, bootstrap_mrjob
        runner = EMRJobRunner(conf_paths=[],
                              image_version=image_version,
                              bootstrap=[
                                  expected_python_bin + ' ' +
                                  foo_py_path + '#bar.py',
                                  's3://walrus/scripts/ohnoes.sh#'],
                              bootstrap_mrjob=True)

        runner._add_bootstrap_files_for_upload()

        self.assertIsNotNone(runner._master_bootstrap_script_path)
        self.assertTrue(os.path.exists(runner._master_bootstrap_script_path))

        with open(runner._master_bootstrap_script_path) as f:
            lines = [line.rstrip() for line in f]

        if version_gte(image_version or _DEFAULT_IMAGE_VERSION,
                       _BAD_BASH_IMAGE_VERSION):
            self.assertEqual(lines[0], '#!/bin/sh -x')
            self.assertEqual(lines[1], 'set -e')
        else:
            self.assertEqual(lines[0], '#!/bin/sh -ex')

        # check PWD gets stored
        self.assertIn('__mrjob_PWD=$PWD', lines)

        # check for stdout -> stderr redirect
        self.assertIn('{', lines)
        self.assertIn('} 1>&2', lines)

        def assertScriptDownloads(path, name=None):
            uri = runner._upload_mgr.uri(path)
            name = runner._bootstrap_dir_mgr.name('file', path, name=name)

            if image_version and not version_gte(image_version, '4'):
                self.assertIn(
                    '  hadoop fs -copyToLocal %s $__mrjob_PWD/%s' % (
                        uri, name),
                    lines)
            else:
                self.assertIn(
                    '  aws s3 cp %s $__mrjob_PWD/%s' % (uri, name),
                    lines)

            self.assertIn(
                '  chmod u+rx $__mrjob_PWD/%s' % (name,),
                lines)

        # check files get downloaded
        assertScriptDownloads(foo_py_path, 'bar.py')
        assertScriptDownloads('s3://walrus/scripts/ohnoes.sh')
        assertScriptDownloads(runner._mrjob_zip_path)

        # check scripts get run

        # bootstrap
        self.assertIn('  ' + expected_python_bin + ' $__mrjob_PWD/bar.py',
                      lines)
        self.assertIn('  $__mrjob_PWD/ohnoes.sh', lines)
        # bootstrap_mrjob
        mrjob_zip_name = runner._bootstrap_dir_mgr.name(
            'file', runner._mrjob_zip_path)
        self.assertIn("  __mrjob_PYTHON_LIB=$(" + expected_python_bin +
                      " -c 'from distutils.sysconfig import get_python_lib;"
                      " print(get_python_lib())')", lines)
        self.assertIn('  sudo unzip $__mrjob_PWD/' + mrjob_zip_name +
                      ' -d $__mrjob_PYTHON_LIB', lines)
        self.assertIn('  sudo ' + expected_python_bin + ' -m compileall -q -f'
                      ' $__mrjob_PYTHON_LIB/mrjob && true', lines)

    def test_create_master_bootstrap_script(self):
        # this tests 4.x
        self._test_create_master_bootstrap_script()

    def test_create_master_bootstrap_script_on_3_11_0_ami(self):
        self._test_create_master_bootstrap_script(
            expected_python_bin=('python2.7' if PY2 else PYTHON_BIN),
            image_version='3.11.0')

    def test_create_master_bootstrap_script_on_2_4_11_ami(self):
        self._test_create_master_bootstrap_script(
            image_version='2.4.11',
            expected_python_bin=('python2.7' if PY2 else PYTHON_BIN))

    def test_no_bootstrap_script_if_not_needed(self):
        runner = EMRJobRunner(conf_paths=[], bootstrap_mrjob=False,
                              bootstrap_python=False,
                              pool_clusters=False)

        runner._add_bootstrap_files_for_upload()
        self.assertIsNone(runner._master_bootstrap_script_path)

        # bootstrap actions don't figure into the master bootstrap script
        runner = EMRJobRunner(conf_paths=[],
                              bootstrap_mrjob=False,
                              bootstrap_actions=['foo', 'bar baz'],
                              bootstrap_python=False,
                              pool_clusters=False)

        runner._add_bootstrap_files_for_upload()
        self.assertIsNone(runner._master_bootstrap_script_path)

    def test_pooling_does_not_require_bootstrap_script(self):
        # now we use tags for this (see #1086)
        runner = EMRJobRunner(conf_paths=[],
                              bootstrap_mrjob=False,
                              bootstrap_python=False,
                              pool_clusters=True)

        runner._add_bootstrap_files_for_upload()
        self.assertIsNone(runner._master_bootstrap_script_path)

    def test_bootstrap_actions_get_added(self):
        bootstrap_actions = [
            ('s3://elasticmapreduce/bootstrap-actions/configure-hadoop'
             ' -m,mapred.tasktracker.map.tasks.maximum=1'),
            's3://foo/bar',
        ]

        runner = EMRJobRunner(conf_paths=[],
                              bootstrap_actions=bootstrap_actions,
                              cloud_fs_sync_secs=0.00)

        runner.make_persistent_cluster()

        actions = _list_all_bootstrap_actions(runner)

        self.assertEqual(len(actions), 4)

        self.assertEqual(
            actions[0]['ScriptPath'],
            's3://elasticmapreduce/bootstrap-actions/configure-hadoop')
        self.assertEqual(
            actions[0]['Args'][0],
            '-m,mapred.tasktracker.map.tasks.maximum=1')
        self.assertEqual(actions[0]['Name'], 'action 0')

        self.assertEqual(actions[1]['ScriptPath'], 's3://foo/bar')
        self.assertEqual(actions[1]['Args'], [])
        self.assertEqual(actions[1]['Name'], 'action 1')

        # check for master bootstrap script
        self.assertTrue(actions[2]['ScriptPath'].startswith('s3://mrjob-'))
        self.assertTrue(actions[2]['ScriptPath'].endswith('b.sh'))
        self.assertEqual(actions[2]['Args'], [])
        self.assertEqual(actions[2]['Name'], 'master')

        # check for idle timeout script
        self.assertTrue(actions[3]['ScriptPath'].startswith('s3://mrjob-'))
        self.assertTrue(actions[3]['ScriptPath'].endswith(
            'terminate_idle_cluster.sh'))
        self.assertEqual(actions[3]['Args'], ['600'])
        self.assertEqual(actions[3]['Name'], 'idle timeout')

        # make sure master bootstrap script is on S3
        self.assertTrue(runner.fs.exists(actions[2]['ScriptPath']))

    def test_bootstrap_mrjob_uses_python_bin(self):
        # use all the bootstrap options
        runner = EMRJobRunner(conf_paths=[],
                              bootstrap_mrjob=True,
                              python_bin=['anaconda'])

        runner._add_bootstrap_files_for_upload()
        self.assertIsNotNone(runner._master_bootstrap_script_path)
        with open(runner._master_bootstrap_script_path, 'r') as f:
            content = f.read()

        self.assertIn('sudo anaconda -m compileall -q -f', content)

    def test_local_bootstrap_action(self):
        # make sure that local bootstrap action scripts get uploaded to S3
        action_path = os.path.join(self.tmp_dir, 'apt-install.sh')
        with open(action_path, 'w') as f:
            f.write('for $pkg in $@; do sudo apt-get install $pkg; done\n')

        bootstrap_actions = [
            action_path + ' python-scipy mysql-server']

        runner = EMRJobRunner(conf_paths=[],
                              bootstrap_actions=bootstrap_actions,
                              cloud_fs_sync_secs=0.00,
                              pool_clusters=False)

        runner.make_persistent_cluster()

        actions = _list_all_bootstrap_actions(runner)

        self.assertEqual(len(actions), 3)

        self.assertTrue(actions[0]['ScriptPath'].startswith('s3://mrjob-'))
        self.assertTrue(actions[0]['ScriptPath'].endswith('/apt-install.sh'))
        self.assertEqual(actions[0]['Name'], 'action 0')
        self.assertEqual(actions[0]['Args'][0], 'python-scipy')
        self.assertEqual(actions[0]['Args'][1], 'mysql-server')

        # check for master bootstrap script
        self.assertTrue(actions[1]['ScriptPath'].startswith('s3://mrjob-'))
        self.assertTrue(actions[1]['ScriptPath'].endswith('b.sh'))
        self.assertEqual(actions[1]['Args'], [])
        self.assertEqual(actions[1]['Name'], 'master')

        # check for idle timeout script
        self.assertTrue(actions[2]['ScriptPath'].startswith('s3://mrjob-'))
        self.assertTrue(actions[2]['ScriptPath'].endswith(
            'terminate_idle_cluster.sh'))
        self.assertEqual(actions[2]['Args'], ['600'])
        self.assertEqual(actions[2]['Name'], 'idle timeout')

        # make sure scripts are on S3
        self.assertTrue(runner.fs.exists(actions[1]['ScriptPath']))
        self.assertTrue(runner.fs.exists(actions[2]['ScriptPath']))

    def test_bootstrap_archive(self):
        foo_dir = self.makedirs('foo')
        foo_tar_gz = make_archive(
            os.path.join(self.tmp_dir, 'foo'), 'gztar', foo_dir)

        runner = EMRJobRunner(conf_paths=[],
                              bootstrap=['cd ' + foo_tar_gz + '#/'])

        runner._add_bootstrap_files_for_upload()

        self.assertIsNotNone(runner._master_bootstrap_script_path)
        self.assertTrue(os.path.exists(runner._master_bootstrap_script_path))

        with open(runner._master_bootstrap_script_path) as f:
            lines = [line.rstrip() for line in f]

        self.assertIn('  __mrjob_TMP=$(mktemp -d)', lines)

        self.assertIn(('  aws s3 cp %s $__mrjob_TMP/foo.tar.gz' %
                       runner._upload_mgr.uri(foo_tar_gz)),
                      lines)

        self.assertIn(
            '  mkdir $__mrjob_PWD/foo.tar.gz;'
            ' tar xfz $__mrjob_TMP/foo.tar.gz -C $__mrjob_PWD/foo.tar.gz',
            lines)

        self.assertIn(
            '  chmod u+rx -R $__mrjob_PWD/foo.tar.gz',
            lines)

        self.assertIn(
            '  cd $__mrjob_PWD/foo.tar.gz/',
            lines)

    def test_bootstrap_dir(self):
        foo_dir = self.makedirs('foo')

        runner = EMRJobRunner(conf_paths=[],
                              bootstrap=['cd ' + foo_dir + '/#'])

        runner._add_bootstrap_files_for_upload()

        self.assertIsNotNone(runner._master_bootstrap_script_path)
        self.assertTrue(os.path.exists(runner._master_bootstrap_script_path))

        self.assertIn(foo_dir, runner._dir_to_archive_path)

        with open(runner._master_bootstrap_script_path) as f:
            lines = [line.rstrip() for line in f]

        self.assertIn('  __mrjob_TMP=$(mktemp -d)', lines)

        self.assertIn(('  aws s3 cp %s $__mrjob_TMP/foo.tar.gz' %
                       runner._upload_mgr.uri(
                           runner._dir_archive_path(foo_dir))),
                      lines)

        self.assertIn(
            '  mkdir $__mrjob_PWD/foo.tar.gz;'
            ' tar xfz $__mrjob_TMP/foo.tar.gz -C $__mrjob_PWD/foo.tar.gz',
            lines)

        self.assertIn(
            '  chmod u+rx -R $__mrjob_PWD/foo.tar.gz',
            lines)

        self.assertIn(
            '  cd $__mrjob_PWD/foo.tar.gz/',
            lines)


class MasterNodeSetupScriptTestCase(MockBoto3TestCase):

    def setUp(self):
        super(MasterNodeSetupScriptTestCase, self).setUp()
        self.start(patch('mrjob.emr.log'))

    def test_no_script_needed(self):
        runner = EMRJobRunner()

        runner._add_master_node_setup_files_for_upload()
        self.assertIsNone(runner._master_node_setup_script_path)
        self.assertEqual(runner._master_node_setup_mgr.paths(), set())

    def test_libjars(self):
        runner = EMRJobRunner(libjars=[
            'cookie.jar',
            's3://pooh/honey.jar',
            'file:///left/dora.jar',
        ])

        runner._add_master_node_setup_files_for_upload()
        self.assertIsNotNone(runner._master_node_setup_script_path)
        # don't need to manage file:/// URI
        self.assertEqual(
            runner._master_node_setup_mgr.paths(),
            set(['cookie.jar', 's3://pooh/honey.jar']))

        uploaded_paths = set(runner._upload_mgr.path_to_uri())
        self.assertIn('cookie.jar', uploaded_paths)

        with open(runner._master_node_setup_script_path, 'rb') as f:
            contents = f.read()

        self.assertTrue(contents.startswith(b'#!/bin/sh -x\n'))
        self.assertIn(b'set -e', contents)
        self.assertIn(b'aws s3 cp ', contents)
        self.assertNotIn(b'hadoop fs -copyToLocal ', contents)
        self.assertIn(b'chmod u+rx ', contents)
        self.assertIn(b'cookie.jar', contents)
        self.assertIn(b's3://pooh/honey.jar', contents)
        self.assertNotIn(b'dora.jar', contents)

    def test_3_x_ami(self):
        runner = EMRJobRunner(libjars=['cookie.jar'],
                              image_version='3.11.0')

        runner._add_master_node_setup_files_for_upload()
        self.assertIsNotNone(runner._master_node_setup_script_path)

        with open(runner._master_node_setup_script_path, 'rb') as f:
            contents = f.read()

        self.assertIn(b'hadoop fs -copyToLocal ', contents)
        self.assertNotIn(b'aws s3 cp ', contents)
        self.assertIn(b'cookie.jar', contents)

    def test_usr_bin_env(self):
        runner = EMRJobRunner(libjars=['cookie.jar'],
                              sh_bin=['bash'])

        runner._add_master_node_setup_files_for_upload()
        self.assertIsNotNone(runner._master_node_setup_script_path)

        with open(runner._master_node_setup_script_path, 'rb') as f:
            contents = f.read()

        self.assertTrue(contents.startswith(b'#!/usr/bin/env bash\n'))


class EMRNoMapperTestCase(MockBoto3TestCase):

    def test_no_mapper(self):
        # read from STDIN, a local file, and a remote file
        stdin = BytesIO(b'foo\nbar\n')

        local_input_path = os.path.join(self.tmp_dir, 'input')
        with open(local_input_path, 'wb') as local_input_file:
            local_input_file.write(b'one fish two fish\nred fish blue fish\n')

        remote_input_path = 's3://walrus/data/foo'
        self.add_mock_s3_data({'walrus': {'data/foo': b'foo\n'}})

        # setup fake output
        self.mock_emr_output = {('j-MOCKCLUSTER0', 1): [
            b'1\t["blue", "one", "red", "two"]\n',
            b'4\t["fish"]\n']}

        mr_job = MRNoMapper(['-r', 'emr', '-v',
                             '-', local_input_path, remote_input_path])
        mr_job.sandbox(stdin=stdin)

        results = []

        with mr_job.make_runner() as runner:
            runner.run()

            results.extend(mr_job.parse_output(runner.cat_output()))

        self.assertEqual(sorted(results),
                         [(1, ['blue', 'one', 'red', 'two']),
                          (4, ['fish'])])


class MaxMinsIdleTestCase(MockBoto3TestCase):

    def assertRanIdleTimeoutScriptWith(self, runner, args):
        actions = _list_all_bootstrap_actions(runner)
        action = actions[-1]

        self.assertEqual(action['Name'], 'idle timeout')
        self.assertEqual(
            action['ScriptPath'],
            runner._upload_mgr.uri(_MAX_MINS_IDLE_BOOTSTRAP_ACTION_PATH))
        self.assertEqual(action['Args'], args)

    def assertDidNotUseIdleTimeoutScript(self, runner):
        actions = _list_all_bootstrap_actions(runner)
        action_names = [a['Name'] for a in actions]

        self.assertNotIn('idle timeout', action_names)
        # idle timeout script should not even be uploaded
        self.assertNotIn(_MAX_MINS_IDLE_BOOTSTRAP_ACTION_PATH,
                         runner._upload_mgr.path_to_uri())

    def test_default(self):
        mr_job = MRWordCount(['-r', 'emr'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.run()
            self.assertDidNotUseIdleTimeoutScript(runner)

    def test_pooling(self):
        mr_job = MRWordCount(['-r', 'emr', '--pool-clusters'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.run()
            self.assertRanIdleTimeoutScriptWith(runner, ['600'])

    def test_custom_max_mins_idle(self):
        mr_job = MRWordCount(['-r', 'emr', '--max-mins-idle', '0.6'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.make_persistent_cluster()
            self.assertRanIdleTimeoutScriptWith(runner, ['36'])

    def test_deprecated_max_hours_idle(self):
        mr_job = MRWordCount(['-r', 'emr', '--max-mins-idle', '0.6'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.make_persistent_cluster()
            self.assertRanIdleTimeoutScriptWith(runner, ['36'])

    def test_deprecated_mins_to_end_of_hour_does_nothing(self):
        mr_job = MRWordCount(['-r', 'emr', '--mins-to-end-of-hour', '10'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.make_persistent_cluster()
            self.assertRanIdleTimeoutScriptWith(runner, ['600'])

    def test_use_integer(self):
        mr_job = MRWordCount(['-r', 'emr', '--max-mins-idle', '60.00006'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.make_persistent_cluster()
            self.assertRanIdleTimeoutScriptWith(runner, ['3600'])

    def test_bootstrap_script_is_actually_installed(self):
        self.assertTrue(os.path.exists(_MAX_MINS_IDLE_BOOTSTRAP_ACTION_PATH))

    def test_deprecated_max_hours_idle_works(self):
        mr_job = MRWordCount(['-r', 'emr', '--max-hours-idle', '1'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.make_persistent_cluster()
            self.assertRanIdleTimeoutScriptWith(runner, ['3600'])


class TestCatFallback(MockBoto3TestCase):

    def test_s3_cat(self):
        self.add_mock_s3_data(
            {'walrus': {'one': b'one_text',
                        'two': b'two_text',
                        'three': b'three_text'}})

        runner = EMRJobRunner(cloud_tmp_dir='s3://walrus/tmp',
                              conf_paths=[])

        self.assertEqual(list(runner.fs.cat('s3://walrus/one')), [b'one_text'])

    def test_ssh_cat(self):
        runner = EMRJobRunner(conf_paths=[])
        self.prepare_runner_for_ssh(runner)
        mock_ssh_file('testmaster', 'etc/init.d', b'meow')

        ssh_cat_gen = runner.fs.cat(
            'ssh://testmaster/etc/init.d')
        self.assertEqual(list(ssh_cat_gen)[0].rstrip(), b'meow')
        self.assertRaises(
            IOError, list,
            runner.fs.cat('ssh://testmaster/does_not_exist'))

    def test_ssh_cat_errlog(self):
        # A file *containing* an error message shouldn't cause an error.
        runner = EMRJobRunner(conf_paths=[])
        self.prepare_runner_for_ssh(runner)

        error_message = b'cat: logs/err.log: No such file or directory\n'
        mock_ssh_file('testmaster', 'logs/err.log', error_message)
        self.assertEqual(
            list(runner.fs.cat('ssh://testmaster/logs/err.log')),
            [error_message])


class CleanupOptionsTestCase(MockBoto3TestCase):

    def setUp(self):
        super(CleanupOptionsTestCase, self).setUp()

        self.start(patch.object(EMRJobRunner, '_cleanup_cloud_tmp'))
        self.start(patch.object(EMRJobRunner, '_cleanup_cluster'))
        self.start(patch.object(EMRJobRunner, '_cleanup_hadoop_tmp'))
        self.start(patch.object(EMRJobRunner, '_cleanup_job'))
        self.start(patch.object(EMRJobRunner, '_cleanup_local_tmp'))
        self.start(patch.object(EMRJobRunner, '_cleanup_logs'))

    def test_cleanup_all(self):
        r = EMRJobRunner(conf_paths=[])
        r.cleanup(mode='ALL')

        self.assertTrue(EMRJobRunner._cleanup_cloud_tmp.called)
        self.assertFalse(EMRJobRunner._cleanup_cluster.called)
        self.assertTrue(EMRJobRunner._cleanup_hadoop_tmp.called)
        self.assertFalse(EMRJobRunner._cleanup_job.called)
        self.assertTrue(EMRJobRunner._cleanup_local_tmp.called)
        self.assertTrue(EMRJobRunner._cleanup_logs.called)

    def test_cleanup_job(self):
        r = EMRJobRunner(conf_paths=[])
        r.cleanup(mode='JOB')

        self.assertFalse(EMRJobRunner._cleanup_cloud_tmp.called)
        self.assertFalse(EMRJobRunner._cleanup_cluster.called)
        self.assertFalse(EMRJobRunner._cleanup_hadoop_tmp.called)
        self.assertFalse(EMRJobRunner._cleanup_job.called)  # only on failure
        self.assertFalse(EMRJobRunner._cleanup_local_tmp.called)
        self.assertFalse(EMRJobRunner._cleanup_logs.called)

    def test_cleanup_none(self):
        r = EMRJobRunner(conf_paths=[])
        r.cleanup(mode='NONE')

        self.assertFalse(EMRJobRunner._cleanup_cloud_tmp.called)
        self.assertFalse(EMRJobRunner._cleanup_cluster.called)
        self.assertFalse(EMRJobRunner._cleanup_hadoop_tmp.called)
        self.assertFalse(EMRJobRunner._cleanup_job.called)  # only on failure
        self.assertFalse(EMRJobRunner._cleanup_local_tmp.called)
        self.assertFalse(EMRJobRunner._cleanup_logs.called)


class CleanupClusterTestCase(MockBoto3TestCase):

    def _quick_runner(self):
        r = EMRJobRunner(conf_paths=[], ec2_key_pair_file='fake.pem',
                         pool_clusters=True)
        r._cluster_id = 'j-ESSEOWENS'
        r._address = 'Albuquerque, NM'
        r._ran_job = False
        return r

    def test_kill_cluster(self):
        with no_handlers_for_logger('mrjob.emr'):
            r = self._quick_runner()
            with patch.object(EMRJobRunner, 'make_emr_client') as m:
                r._cleanup_cluster()
                self.assertTrue(m().terminate_job_flows.called)

    def test_kill_cluster_if_successful(self):
        # If they are setting up the cleanup to kill the cluster, mrjob should
        # kill the cluster independent of job success.
        with no_handlers_for_logger('mrjob.emr'):
            r = self._quick_runner()
            with patch.object(EMRJobRunner, 'make_emr_client') as m:
                r._ran_job = True
                r._cleanup_cluster()
                self.assertTrue(m().terminate_job_flows.called)

    def test_kill_persistent_cluster(self):
        with no_handlers_for_logger('mrjob.emr'):
            r = self._quick_runner()
            with patch.object(EMRJobRunner, 'make_emr_client') as m:
                r._opts['cluster_id'] = 'j-MOCKCLUSTER0'
                r._cleanup_cluster()
                self.assertTrue(m().terminate_job_flows.called)


class JobWaitTestCase(MockBoto3TestCase):

    # A list of job ids that hold booleans of whether or not the job can
    # acquire a lock. Helps simulate mrjob.emr._attempt_to_acquire_lock.
    JOB_ID_LOCKS = {
        'j-fail-lock': False,
        'j-successful-lock': True,
        'j-brown': True,
        'j-epic-fail-lock': False
    }

    def setUp(self):
        super(JobWaitTestCase, self).setUp()
        self.future_mock_cluster_ids = []
        self.mock_cluster_ids = []
        self.sleep_counter = 0

        def side_effect_lock_uri(*args):
            return args[0]  # Return the only arg given to it.

        def side_effect_acquire_lock(*args):
            cluster_id = args[1]
            return self.JOB_ID_LOCKS[cluster_id]

        def side_effect_usable_clusters(*args, **kwargs):
            return [
                (cluster_id, 0) for cluster_id in self.mock_cluster_ids
                if cluster_id not in kwargs['exclude']]

        def side_effect_time_sleep(*args):
            self.sleep_counter += 1
            if self.future_mock_cluster_ids:
                cluster_id = self.future_mock_cluster_ids.pop(0)
                self.mock_cluster_ids.append(cluster_id)

        self.start(patch.object(EMRJobRunner, '_usable_clusters',
                                side_effect=side_effect_usable_clusters))
        self.start(patch.object(EMRJobRunner, '_lock_uri',
                                side_effect=side_effect_lock_uri))
        self.start(patch.object(mrjob.emr, '_attempt_to_acquire_lock',
                                side_effect=side_effect_acquire_lock))
        self.start(patch.object(time, 'sleep',
                                side_effect=side_effect_time_sleep))

    def tearDown(self):
        super(JobWaitTestCase, self).tearDown()
        self.mock_cluster_ids = []
        self.future_mock_cluster_ids = []

    def test_no_waiting_for_job_pool_fail(self):
        self.mock_cluster_ids.append('j-fail-lock')

        runner = EMRJobRunner(conf_paths=[], pool_wait_minutes=0)
        cluster_id = runner._find_cluster()

        self.assertEqual(cluster_id, None)
        # sleep once after creating temp bucket
        self.assertEqual(self.sleep_counter, 1)

    def test_no_waiting_for_job_pool_success(self):
        self.mock_cluster_ids.append('j-fail-lock')
        runner = EMRJobRunner(conf_paths=[], pool_wait_minutes=0)
        cluster_id = runner._find_cluster()

        self.assertEqual(cluster_id, None)

    def test_acquire_lock_on_first_attempt(self):
        self.mock_cluster_ids.append('j-successful-lock')
        runner = EMRJobRunner(conf_paths=[], pool_wait_minutes=1)
        cluster_id = runner._find_cluster()

        self.assertEqual(cluster_id, 'j-successful-lock')
        self.assertEqual(self.sleep_counter, 1)

    def test_sleep_then_acquire_lock(self):
        self.mock_cluster_ids.append('j-fail-lock')
        self.future_mock_cluster_ids.append('j-successful-lock')
        runner = EMRJobRunner(conf_paths=[], pool_wait_minutes=1)
        cluster_id = runner._find_cluster()

        self.assertEqual(cluster_id, 'j-successful-lock')
        self.assertEqual(self.sleep_counter, 1)

    def test_timeout_waiting_for_cluster(self):
        self.mock_cluster_ids.append('j-fail-lock')
        self.future_mock_cluster_ids.append('j-epic-fail-lock')
        runner = EMRJobRunner(conf_paths=[], pool_wait_minutes=1)
        cluster_id = runner._find_cluster()

        self.assertEqual(cluster_id, None)
        self.assertEqual(self.sleep_counter, 3)


class BuildStreamingStepTestCase(MockBoto3TestCase):

    def setUp(self):
        super(BuildStreamingStepTestCase, self).setUp()

        self.start(patch(
            'mrjob.emr.EMRJobRunner._step_input_uris',
            return_value=['input']))

        self.start(patch(
            'mrjob.emr.EMRJobRunner._step_output_uri',
            return_value='output'))

        self.start(patch(
            'mrjob.emr.EMRJobRunner.get_image_version',
            return_value='4.8.2'))

        self.start(patch(
            'mrjob.emr.EMRJobRunner.get_hadoop_version',
            return_value='2.7.3'))

        self.start(patch(
            'mrjob.emr.EMRJobRunner._get_streaming_jar_and_step_arg_prefix',
            return_value=('mockstreaming.jar', [])))

    def _runner_with_steps(self, steps, **kwargs):
        if isinstance(steps, dict):
            raise TypeError

        runner = EMRJobRunner(
            mr_job_script='my_job.py',
            conf_paths=[],
            stdin=BytesIO(),
            **kwargs)

        runner._steps = steps

        runner._add_job_files_for_upload()
        runner._add_master_node_setup_files_for_upload()

        return runner

    def test_basic_mapper(self):
        runner = self._runner_with_steps(
            [dict(type='streaming', mapper=dict(type='script'))])

        step = runner._build_step(0)

        self.assertEqual(step['HadoopJarStep']['Jar'], 'mockstreaming.jar')

        self.assertEqual(
            step['HadoopJarStep']['Args'], [
                '-files',
                '%s#my_job.py' % runner._upload_mgr.uri('my_job.py'),
                '-D', 'mapreduce.job.reduces=0',
                '-input', 'input', '-output', 'output',
                '-mapper',
                '%s my_job.py --step-num=0 --mapper' % PYTHON_BIN,
            ])

    def test_basic_reducer(self):
        runner = self._runner_with_steps(
            [dict(type='streaming', reducer=dict(type='script'))])

        step = runner._build_step(0)

        self.assertEqual(
            step['HadoopJarStep']['Args'], [
                '-files',
                '%s#my_job.py' % runner._upload_mgr.uri('my_job.py'),
                '-input', 'input', '-output', 'output',
                '-mapper', 'cat',
                '-reducer',
                '%s my_job.py --step-num=0 --reducer' % PYTHON_BIN,
            ])

    def test_pre_filters(self):
        runner = self._runner_with_steps([
            dict(type='streaming',
                 mapper=dict(
                     type='script',
                     pre_filter='grep anything'),
                 combiner=dict(
                     type='script',
                     pre_filter='grep nothing'),
                 reducer=dict(
                     type='script',
                     pre_filter='grep something'))])

        step = runner._build_step(0)

        self.assertEqual(
            step['HadoopJarStep']['Args'], [
                '-files',
                '%s#my_job.py' % runner._upload_mgr.uri('my_job.py'),
                '-input', 'input', '-output', 'output',
                '-mapper',
                "/bin/sh -x -c 'set -e; grep anything | %s"
                " my_job.py --step-num=0 --mapper'" % PYTHON_BIN,
                '-combiner',
                "/bin/sh -x -c 'set -e; grep nothing | %s"
                " my_job.py --step-num=0 --combiner'" % PYTHON_BIN,
                '-reducer',
                "/bin/sh -x -c 'set -e; grep something | %s"
                " my_job.py --step-num=0 --reducer'" % PYTHON_BIN,
            ])

    def test_pre_filter_escaping(self):
        runner = self._runner_with_steps(
            [dict(type='streaming',
                  mapper=dict(
                      type='script',
                      pre_filter=_bash_wrap("grep 'anything'")))])

        step = runner._build_step(0)

        self.assertEqual(
            step['HadoopJarStep']['Args'], [
                '-files',
                '%s#my_job.py' % runner._upload_mgr.uri('my_job.py'),
                '-D', 'mapreduce.job.reduces=0',
                '-input', 'input', '-output', 'output',
                '-mapper',
                "/bin/sh -x -c 'set -e; bash -c '\\''grep"
                " '\\''\\'\\'''\\''anything'\\''\\'\\'''\\'''\\'' | %s"
                " my_job.py --step-num=0 --mapper'" % PYTHON_BIN,
            ])

    def test_custom_streaming_jar_and_step_arg_prefix(self):
        # this tests integration with custom jar options. See
        # StreamingJarAndStepArgPrefixTestCase below.
        #
        # compare to test_basic_mapper()
        EMRJobRunner._get_streaming_jar_and_step_arg_prefix.return_value = (
            ('launch.jar', ['streaming', '-v']))

        runner = self._runner_with_steps(
            [dict(type='streaming', mapper=dict(type='script'))])

        step = runner._build_step(0)

        self.assertEqual(step['HadoopJarStep']['Jar'], 'launch.jar')

        self.assertEqual(
            step['HadoopJarStep']['Args'], [
                'streaming', '-v',
                '-files',
                '%s#my_job.py' % runner._upload_mgr.uri('my_job.py'),
                '-D', 'mapreduce.job.reduces=0',
                '-input', 'input', '-output', 'output',
                '-mapper',
                '%s my_job.py --step-num=0 --mapper' % PYTHON_BIN,
            ])

    def test_hadoop_args_for_step(self):
        self.start(patch(
            'mrjob.emr.EMRJobRunner._hadoop_args_for_step',
            return_value=['-libjars', '/home/hadoop/dora.jar',
                          '-D', 'foo=bar']))

        runner = self._runner_with_steps(
            [dict(type='streaming', mapper=dict(type='script'))])

        step = runner._build_step(0)

        self.assertEqual(
            step['HadoopJarStep']['Args'], [
                '-files',
                '%s#my_job.py' % runner._upload_mgr.uri('my_job.py'),
                '-D', 'mapreduce.job.reduces=0',
                '-libjars', '/home/hadoop/dora.jar',
                '-D', 'foo=bar',
                '-input', 'input', '-output', 'output',
                '-mapper',
                '%s my_job.py --step-num=0 --mapper' % PYTHON_BIN,
            ])


class LibjarPathsTestCase(MockBoto3TestCase):

    def test_no_libjars(self):
        runner = EMRJobRunner()
        runner._add_master_node_setup_files_for_upload()

        self.assertEqual(runner._libjar_paths(), [])

    def test_libjars(self):
        runner = EMRJobRunner(libjars=[
            'cookie.jar',
            's3://pooh/honey.jar',
            'file:///left/dora.jar',
        ])
        runner._add_master_node_setup_files_for_upload()

        working_dir = runner._master_node_setup_working_dir()

        self.assertEqual(
            runner._libjar_paths(),
            [
                working_dir + '/cookie.jar',
                working_dir + '/honey.jar',
                '/left/dora.jar',
            ]
        )


class DefaultPythonBinTestCase(MockBoto3TestCase):

    def test_default_ami(self):
        # this tests 4.x AMIs
        runner = EMRJobRunner()
        self.assertTrue(runner._opts['image_version'].startswith('5.'))
        self.assertEqual(runner._default_python_bin(), [PYTHON_BIN])

    def test_4_x_release_label(self):
        runner = EMRJobRunner(release_label='emr-4.0.0')
        self.assertEqual(runner._default_python_bin(),
                         ['python2.7'] if PY2 else [PYTHON_BIN])

    def test_3_11_0_ami(self):
        runner = EMRJobRunner(image_version='3.11.0')
        self.assertEqual(runner._default_python_bin(),
                         ['python2.7'] if PY2 else [PYTHON_BIN])

    def test_2_4_3_ami(self):
        runner = EMRJobRunner(image_version='2.4.3')
        if PY2:
            self.assertEqual(runner._default_python_bin(), ['python2.7'])
        else:
            self.assertEqual(runner._default_python_bin(), ['python3'])

    def test_local_python_bin(self):
        # just make sure we don't break this
        runner = EMRJobRunner()
        self.assertEqual(runner._default_python_bin(local=True),
                         [sys.executable])


class StreamingJarAndStepArgPrefixTestCase(MockBoto3TestCase):

    def launch_runner(self, *args):
        """make and launch runner, so cluster is created and files
        are uploaded."""
        runner = self.make_runner(*args)
        runner._launch()
        return runner

    def test_default(self):
        runner = self.launch_runner()
        self.assertEqual(runner._get_streaming_jar_and_step_arg_prefix(),
                         (_4_X_COMMAND_RUNNER_JAR, ['hadoop-streaming']))

    def test_pre_4_x_ami(self):
        runner = self.launch_runner('--image-version', '3.8.0')
        self.assertEqual(runner._get_streaming_jar_and_step_arg_prefix(),
                         (_PRE_4_X_STREAMING_JAR, []))

    def test_4_x_ami(self):
        runner = self.launch_runner('--image-version', '4.0.0')
        self.assertEqual(runner._get_streaming_jar_and_step_arg_prefix(),
                         (_4_X_COMMAND_RUNNER_JAR, ['hadoop-streaming']))

    def test_local_hadoop_streaming_jar(self):
        jar_path = os.path.join(self.tmp_dir, 'righteousness.jar')
        open(jar_path, 'w').close()

        runner = self.launch_runner(
            '--hadoop-streaming-jar', jar_path)

        jar_uri = runner._upload_mgr.uri(jar_path)
        self.assertEqual(runner._get_streaming_jar_and_step_arg_prefix(),
                         (jar_uri, []))

    def test_hadoop_streaming_jar_on_emr_absolute_path(self):
        runner = self.launch_runner(
            '--hadoop-streaming-jar', 'file:///path/to/victory.jar')
        self.assertEqual(runner._get_streaming_jar_and_step_arg_prefix(),
                         ('/path/to/victory.jar', []))

    def test_hadoop_streaming_jar_on_emr_relative_path(self):
        runner = self.launch_runner(
            '--hadoop-streaming-jar', 'file://justice.jar')
        self.assertEqual(runner._get_streaming_jar_and_step_arg_prefix(),
                         ('justice.jar', []))


class JarStepTestCase(MockBoto3TestCase):

    MRJOB_CONF_CONTENTS = {'runners': {'emr': {
        'check_cluster_every': 0.00,
        'cloud_fs_sync_secs': 0.00,
    }}}

    def test_local_jar_gets_uploaded(self):
        fake_jar = os.path.join(self.tmp_dir, 'fake.jar')
        with open(fake_jar, 'w'):
            pass

        job = MRJustAJar(['-r', 'emr', '--jar', fake_jar])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            self.assertIn(fake_jar, runner._upload_mgr.path_to_uri())
            jar_uri = runner._upload_mgr.uri(fake_jar)
            self.assertTrue(runner.fs.ls(jar_uri))

            steps = _list_all_steps(runner)

            self.assertEqual(len(steps), 1)
            self.assertEqual(steps[0]['Config']['Jar'], jar_uri)

    def test_with_libjar(self):
        fake_jar = os.path.join(self.tmp_dir, 'fake.jar')
        with open(fake_jar, 'w'):
            pass

        fake_libjar = os.path.join(self.tmp_dir, 'libfake.jar')
        with open(fake_libjar, 'w'):
            pass

        job = MRJustAJar(
            ['-r', 'emr', '--jar', fake_jar, '--libjar', fake_libjar])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            self.assertIn(fake_jar, runner._upload_mgr.path_to_uri())
            jar_uri = runner._upload_mgr.uri(fake_jar)
            self.assertTrue(runner.fs.ls(jar_uri))

            self.assertIn(fake_libjar, runner._upload_mgr.path_to_uri())
            libjar_uri = runner._upload_mgr.uri(fake_libjar)
            self.assertTrue(runner.fs.ls(libjar_uri))

            steps = _list_all_steps(runner)

            self.assertEqual(len(steps), 2)  # adds master node setup

            jar_step = steps[1]
            self.assertEqual(jar_step['Config']['Jar'], jar_uri)
            step_args = jar_step['Config']['Args']

            working_dir = runner._master_node_setup_working_dir()

            self.assertEqual(step_args,
                             ['-libjars', working_dir + '/libfake.jar'])

    def test_jar_on_s3(self):
        self.add_mock_s3_data({'dubliners': {'whiskeyinthe.jar': b''}})
        JAR_URI = 's3://dubliners/whiskeyinthe.jar'

        job = MRJustAJar(['-r', 'emr', '--jar', JAR_URI])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            steps = _list_all_steps(runner)

            self.assertEqual(len(steps), 1)
            self.assertEqual(steps[0]['Config']['Jar'], JAR_URI)

            # for comparison with test_main_class()
            self.assertNotIn('MainClass', steps[0]['Config'])

    def test_jar_inside_emr(self):
        job = MRJustAJar(['-r', 'emr', '--jar',
                          'file:///home/hadoop/hadoop-examples.jar'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            steps = _list_all_steps(runner)

            self.assertEqual(len(steps), 1)
            self.assertEqual(steps[0]['Config']['Jar'],
                             '/home/hadoop/hadoop-examples.jar')

    def test_input_output_interpolation(self):
        fake_jar = os.path.join(self.tmp_dir, 'fake.jar')
        open(fake_jar, 'w').close()
        input1 = os.path.join(self.tmp_dir, 'input1')
        open(input1, 'w').close()
        input2 = os.path.join(self.tmp_dir, 'input2')
        open(input2, 'w').close()

        job = MRJarAndStreaming(
            ['-r', 'emr', '--jar', fake_jar, input1, input2])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            steps = _list_all_steps(runner)

            self.assertEqual(len(steps), 2)
            jar_step, streaming_step = steps

            # on EMR, the jar gets uploaded
            self.assertEqual(jar_step['Config']['Jar'],
                             runner._upload_mgr.uri(fake_jar))

            jar_args = jar_step['Config']['Args']
            self.assertEqual(len(jar_args), 3)
            self.assertEqual(jar_args[0], 'stuff')

            # check input is interpolated
            input_arg = ','.join(
                runner._upload_mgr.uri(path) for path in (input1, input2))
            self.assertEqual(jar_args[1], input_arg)

            # check output of jar is input of next step
            jar_output_arg = jar_args[2]

            streaming_args = streaming_step['Config']['Args']
            streaming_input_arg = streaming_args[
                streaming_args.index('-input') + 1]
            self.assertEqual(jar_output_arg, streaming_input_arg)

    def test_main_class(self):
        # don't forget to make sure main_class is respected. tests #1572
        self.add_mock_s3_data({'dubliners': {'whiskeyinthe.jar': b''}})
        JAR_URI = 's3://dubliners/whiskeyinthe.jar'

        job = MRJustAJar(['-r', 'emr', '--jar', JAR_URI,
                          '--main-class', 'ThingAnalyzer'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            steps = _list_all_steps(runner)

            self.assertEqual(len(steps), 1)
            self.assertEqual(steps[0]['Config']['Jar'], JAR_URI)
            self.assertEqual(steps[0]['Config']['MainClass'], 'ThingAnalyzer')


class SparkStepTestCase(MockBoto3TestCase):

    def setUp(self):
        super(SparkStepTestCase, self).setUp()

        # _spark_submit_args() is tested elsewhere
        self.start(patch(
            'mrjob.bin.MRJobBinRunner._spark_submit_args',
            return_value=['<spark submit args>']))

    # TODO: test warning for for AMIs prior to 3.8.0, which don't offer Spark

    def test_3_x_ami(self):
        job = MRNullSpark(['-r', 'emr', '--image-version', '3.11.0'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()
            steps = _list_all_steps(runner)

            self.assertEqual(len(steps), 1)
            self.assertEqual(
                steps[0]['Config']['Jar'], runner._script_runner_jar_uri())
            self.assertEqual(
                steps[0]['Config']['Args'][0], _3_X_SPARK_SUBMIT)

    def test_4_x_ami(self):
        job = MRNullSpark(['-r', 'emr', '--image-version', '4.7.2'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()
            steps = _list_all_steps(runner)

            self.assertEqual(len(steps), 1)
            self.assertEqual(
                steps[0]['Config']['Jar'], _4_X_COMMAND_RUNNER_JAR)
            self.assertEqual(
                steps[0]['Config']['Args'][0], 'spark-submit')

    def test_input_and_output_args(self):
        input1 = os.path.join(self.tmp_dir, 'input1')
        open(input1, 'w').close()
        input2 = os.path.join(self.tmp_dir, 'input2')
        open(input2, 'w').close()

        job = MRNullSpark(['-r', 'emr', input1, input2])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            script_uri = runner._upload_mgr.uri(runner._script_path)
            input1_uri = runner._upload_mgr.uri(input1)
            input2_uri = runner._upload_mgr.uri(input2)

            steps = _list_all_steps(runner)

            step_args = steps[0]['Config']['Args']
            # the first arg is spark-submit and varies by AMI
            self.assertEqual(
                step_args[1:],
                [
                    '<spark submit args>',
                    script_uri,
                    '--step-num=0',
                    '--spark',
                    input1_uri + ',' + input2_uri,
                    runner._output_dir,
                ])


class SparkJarStepTestCase(MockBoto3TestCase):

    def setUp(self):
        super(SparkJarStepTestCase, self).setUp()

        self.fake_jar = self.makefile('fake.jar')

        # _spark_submit_args() is tested elsewhere
        self.start(patch(
            'mrjob.bin.MRJobBinRunner._spark_submit_args',
            return_value=['<spark submit args>']))

    def test_jar_gets_uploaded(self):
        job = MRSparkJar(['-r', 'emr', '--jar', self.fake_jar,
                          '--jar-main-class', 'fake.Main'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            self.assertIn(self.fake_jar, runner._upload_mgr.path_to_uri())
            jar_uri = runner._upload_mgr.uri(self.fake_jar)
            self.assertTrue(runner.fs.ls(jar_uri))

            steps = _list_all_steps(runner)

            self.assertEqual(len(steps), 1)
            step_args = steps[0]['Config']['Args']
            # the first arg is spark-submit and varies by AMI
            self.assertEqual(
                step_args[1:],
                ['<spark submit args>', jar_uri])


class SparkScriptStepTestCase(MockBoto3TestCase):
    # a lot of this is already tested in test_runner.py

    def setUp(self):
        super(SparkScriptStepTestCase, self).setUp()

        self.fake_script = self.makefile('fake_script.py')

        # _spark_submit_args() is tested elsewhere
        self.start(patch(
            'mrjob.bin.MRJobBinRunner._spark_submit_args',
            return_value=['<spark submit args>']))

    def test_script_gets_uploaded(self):
        job = MRSparkScript(['-r', 'emr', '--script', self.fake_script])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            self.assertIn(self.fake_script, runner._upload_mgr.path_to_uri())
            script_uri = runner._upload_mgr.uri(self.fake_script)
            self.assertTrue(runner.fs.ls(script_uri))

            steps = _list_all_steps(runner)

            self.assertEqual(len(steps), 1)
            step_args = steps[0]['Config']['Args']
            # the first arg is spark-submit and varies by AMI
            self.assertEqual(
                step_args[1:],
                ['<spark submit args>', script_uri])

    def test_3_x_ami(self):
        job = MRSparkScript(['-r', 'emr', '--script', self.fake_script,
                             '--image-version', '3.11.0'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            steps = _list_all_steps(runner)

            self.assertEqual(len(steps), 1)
            self.assertEqual(
                steps[0]['Config']['Jar'], runner._script_runner_jar_uri())
            self.assertEqual(
                steps[0]['Config']['Args'][0],
                _3_X_SPARK_SUBMIT)

    def test_4_x_ami(self):
        job = MRSparkScript(['-r', 'emr', '--script', self.fake_script,
                             '--image-version', '4.7.2'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            steps = _list_all_steps(runner)

            self.assertEqual(len(steps), 1)
            self.assertEqual(
                steps[0]['Config']['Jar'], _4_X_COMMAND_RUNNER_JAR)
            self.assertEqual(
                steps[0]['Config']['Args'][0], 'spark-submit')

    def test_arg_interpolation(self):
        input1 = os.path.join(self.tmp_dir, 'input1')
        open(input1, 'w').close()
        input2 = os.path.join(self.tmp_dir, 'input2')
        open(input2, 'w').close()

        job = MRSparkScript(['-r', 'emr', '--script', self.fake_script,
                             '--script-arg', INPUT,
                             '--script-arg=-o',
                             '--script-arg', OUTPUT,
                             input1, input2])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            script_uri = runner._upload_mgr.uri(self.fake_script)
            input1_uri = runner._upload_mgr.uri(input1)
            input2_uri = runner._upload_mgr.uri(input2)

            steps = _list_all_steps(runner)

            step_args = steps[0]['Config']['Args']
            # the first arg is spark-submit and varies by AMI
            self.assertEqual(
                step_args[1:],
                [
                    '<spark submit args>',
                    script_uri,
                    input1_uri + ',' + input2_uri,
                    '-o',
                    runner._output_dir,
                ]
            )


class BuildMasterNodeSetupStepTestCase(MockBoto3TestCase):

    def test_build_master_node_setup_step(self):
        runner = EMRJobRunner(libjars=['cookie.jar'])
        runner._add_master_node_setup_files_for_upload()

        self.assertIsNotNone(runner._master_node_setup_script_path)
        master_node_setup_uri = runner._upload_mgr.uri(
            runner._master_node_setup_script_path)

        step = runner._build_master_node_setup_step()

        self.assertTrue(step['Name'].endswith(': Master node setup'))
        self.assertEqual(step['HadoopJarStep']['Jar'],
                         runner._script_runner_jar_uri())
        self.assertEqual(step['HadoopJarStep']['Args'],
                         [master_node_setup_uri])
        self.assertEqual(step['ActionOnFailure'],
                         runner._action_on_failure())


class ActionOnFailureTestCase(MockBoto3TestCase):

    def test_default(self):
        runner = EMRJobRunner()
        self.assertEqual(runner._action_on_failure(),
                         'TERMINATE_CLUSTER')

    def test_default_with_pooling(self):
        runner = EMRJobRunner(pool_clusters=True)
        self.assertEqual(runner._action_on_failure(),
                         'CANCEL_AND_WAIT')

    def test_default_with_cluster_id(self):
        runner = EMRJobRunner(cluster_id='j-CLUSTER')
        self.assertEqual(runner._action_on_failure(),
                         'CANCEL_AND_WAIT')

    def test_option(self):
        runner = EMRJobRunner(emr_action_on_failure='CONTINUE')
        self.assertEqual(runner._action_on_failure(),
                         'CONTINUE')

    def test_switch(self):
        mr_job = MRWordCount(
            ['-r', 'emr', '--emr-action-on-failure', 'CONTINUE'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            self.assertEqual(runner._action_on_failure(), 'CONTINUE')


class MultiPartUploadTestCase(MockBoto3TestCase):

    DEFAULT_PART_SIZE = 100 * 1024 * 1024

    PART_SIZE_IN_MB = 50.0 / 1024 / 1024
    TEST_BUCKET = 'walrus'
    TEST_FILENAME = 'data.dat'
    TEST_S3_URI = 's3://%s/%s' % (TEST_BUCKET, TEST_FILENAME)

    def setUp(self):
        super(MultiPartUploadTestCase, self).setUp()
        # create the walrus bucket
        self.add_mock_s3_data({self.TEST_BUCKET: {}})

        self.upload_file = self.start(patch(
            'tests.mock_boto3.s3.MockS3Object.upload_file',
            side_effect=tests.mock_boto3.s3.MockS3Object.upload_file,
            autospec=True))

    def upload_data(self, runner, data):
        """Upload some bytes to S3"""
        data_path = os.path.join(self.tmp_dir, self.TEST_FILENAME)
        with open(data_path, 'wb') as fp:
            fp.write(data)

        runner._upload_contents(self.TEST_S3_URI, data_path)

    def assert_upload_succeeds(self, runner, data, expected_part_size):
        """Write the data to a temp file, and then upload it to (mock) S3,
        checking that the data successfully uploaded."""
        self.upload_file.reset_mock()

        data_path = os.path.join(self.tmp_dir, self.TEST_FILENAME)
        with open(data_path, 'wb') as fp:
            fp.write(data)

        runner._upload_contents(self.TEST_S3_URI, data_path)

        s3_key = runner.fs._get_s3_key(self.TEST_S3_URI)
        self.assertEqual(s3_key.get()['Body'].read(), data)

        self.assertTrue(self.upload_file.called)

        upload_file_args, upload_file_kwargs = self.upload_file.call_args

        self.assertEqual(upload_file_args[1:], (data_path,))

        self.assertIn('Config', upload_file_kwargs)
        config = upload_file_kwargs['Config']
        self.assertEqual(config.multipart_chunksize, expected_part_size)
        self.assertEqual(config.multipart_threshold, expected_part_size)

    def test_default_part_size(self):
        runner = EMRJobRunner()
        data = b'beavers mate for life'

        self.assert_upload_succeeds(runner, data, 100 * 1024 * 1024)

    def test_custom_part_size(self):
        # this test used to simulate multipart upload, but now we leave
        # that to boto3
        runner = EMRJobRunner(cloud_upload_part_size=50.0 / 1024 / 1024)

        data = b'Mew' * 20
        self.assert_upload_succeeds(runner, data, 50)

    def test_disable_multipart(self):
        runner = EMRJobRunner(cloud_upload_part_size=0)

        data = b'Mew' * 20
        self.assert_upload_succeeds(runner, data, _HUGE_PART_THRESHOLD)


class AWSSessionTokenTestCase(MockBoto3TestCase):

    def setUp(self):
        super(AWSSessionTokenTestCase, self).setUp()

        self.mock_client = self.start(patch('boto3.client'))
        self.mock_resource = self.start(patch('boto3.resource'))

    def assert_conns_use_session_token(self, runner, session_token):
        runner.make_emr_client()

        self.assertTrue(self.mock_client.called)
        emr_kwargs = self.mock_client.call_args[1]
        self.assertIn('aws_session_token', emr_kwargs)
        self.assertEqual(emr_kwargs['aws_session_token'], session_token)

        self.mock_client.reset_mock()
        runner.make_iam_client()

        self.assertTrue(self.mock_client.called)
        iam_args, iam_kwargs = self.mock_client.call_args
        self.assertEqual(iam_args, ('iam',))
        self.assertIn('aws_session_token', iam_kwargs)
        self.assertEqual(iam_kwargs['aws_session_token'], session_token)

        self.mock_client.reset_mock()
        runner.fs.make_s3_client()

        self.assertTrue(self.mock_client.called)
        s3_client_args, s3_client_kwargs = self.mock_client.call_args
        self.assertEqual(s3_client_args, ('s3',))
        self.assertIn('aws_session_token', s3_client_kwargs)
        self.assertEqual(s3_client_kwargs['aws_session_token'], session_token)

        runner.fs.make_s3_resource()
        self.assertTrue(self.mock_client.called)
        s3_resource_args, s3_resource_kwargs = self.mock_client.call_args
        self.assertEqual(s3_resource_args, ('s3',))
        self.assertIn('aws_session_token', s3_resource_kwargs)
        self.assertEqual(s3_resource_kwargs['aws_session_token'],
                         session_token)

    def test_connections_without_session_token(self):
        runner = EMRJobRunner()

        self.assert_conns_use_session_token(runner, None)

    def test_connections_with_session_token(self):
        runner = EMRJobRunner(aws_session_token='meow')

        self.assert_conns_use_session_token(runner, 'meow')


class BootstrapPythonTestCase(MockBoto3TestCase):

    if PY2:
        EXPECTED_BOOTSTRAP = []
    else:
        EXPECTED_BOOTSTRAP = [
            ['sudo yum install -y python34 python34-devel python34-pip']]

    def _assert_installs_python3_on_py3(self, *args):
        mr_job = MRTwoStepJob(['-r', 'emr'] + list(args))
        with mr_job.make_runner() as runner:
            self.assertEqual(runner._bootstrap_python(),
                             self.EXPECTED_BOOTSTRAP)
            self.assertEqual(runner._bootstrap,
                             self.EXPECTED_BOOTSTRAP)

    def _assert_tries_to_install_python3_on_py3(self, *args):
        mr_job = MRTwoStepJob(['-r', 'emr'] + list(args))

        with no_handlers_for_logger('mrjob.emr'):
            stderr = StringIO()
            log_to_stream('mrjob.emr', stderr)

            with mr_job.make_runner() as runner:
                self.assertEqual(runner._bootstrap_python(),
                                 self.EXPECTED_BOOTSTRAP)
                self.assertEqual(runner._bootstrap,
                                 self.EXPECTED_BOOTSTRAP)

                if not PY2:
                    self.assertIn('will probably not work', stderr.getvalue())

    def _assert_never_installs_python3(self, *args):
        mr_job = MRTwoStepJob(['-r', 'emr'] + list(args))
        with mr_job.make_runner() as runner:
            self.assertEqual(runner._bootstrap_python(), [])
            self.assertEqual(runner._bootstrap, [])

    def test_default(self):
        self._assert_never_installs_python3()  # pre-installed on 4.8.2 AMI

    def test_bootstrap_python_switch(self):
        self._assert_installs_python3_on_py3('--bootstrap-python')

    def test_no_bootstrap_python_switch(self):
        self._assert_never_installs_python3('--no-bootstrap-python')

    def test_ami_version_2_4_11(self):
        # this *really, really* probably won't work, but what can we do?
        self._assert_tries_to_install_python3_on_py3(
            '--image-version', '2.4.11')

    def test_ami_version_3_6_0(self):
        self._assert_tries_to_install_python3_on_py3(
            '--image-version', '3.6.0')

    def test_ami_version_3_7_0(self):
        # the first version where Python 3 is available
        self._assert_installs_python3_on_py3(
            '--image-version', '3.7.0')

    def test_ami_version_4_5_0(self):
        # the last version where Python 3 is not pre-installed
        self._assert_installs_python3_on_py3(
            '--image-version', '4.5.0')

    def test_ami_version_4_6_0(self):
        # from this point on, Python 3 is already installed
        self._assert_never_installs_python3(
            '--image-version', '4.6.0')

    def test_release_label_emr_4_6_0(self):
        self._assert_never_installs_python3(
            '--release-label', 'emr-4.6.0')

    def test_release_label_overrides_image_version(self):
        self._assert_never_installs_python3(
            '--release-label', 'emr-4.6.0',
            '--image-version', '3.11.0',
        )

    def test_force_booststrap_python(self):
        self._assert_installs_python3_on_py3(
            '--bootstrap-python', '--image-version', '4.6.0')

    def test_force_no_bootstrap_python(self):
        self._assert_never_installs_python3(
            '--no-bootstrap-python', '--image-version', '3.7.0')

    def test_bootstrap_python_comes_before_bootstrap(self):
        mr_job = MRTwoStepJob(['-r', 'emr',
                               '--bootstrap', 'true',
                               '--bootstrap-python'])

        with mr_job.make_runner() as runner:
            self.assertEqual(
                runner._bootstrap,
                self.EXPECTED_BOOTSTRAP + [['true']])


class BootstrapSparkTestCase(MockBoto3TestCase):

    def setUp(self):
        super(BootstrapSparkTestCase, self).setUp()

        self.log = self.start(patch('mrjob.emr.log'))

    def get_cluster(self, *args):
        job = MRNullSpark(['-r', 'emr'] + list(args))
        job.sandbox()

        with job.make_runner() as runner:
            self.assertTrue(runner._should_bootstrap_spark())
            runner.run()
            # include bootstrap actions
            return self.mock_emr_clusters[runner.get_cluster_id()]

    def ran_spark_bootstrap_action(
            self, cluster, uri=_3_X_SPARK_BOOTSTRAP_ACTION):

        return any(ba['ScriptPath'] == uri
                   for ba in cluster['_BootstrapActions'])

    def installed_spark_application(self, cluster, name='Spark'):
        return any(a['Name'] == name for a in cluster['Applications'])

    def test_default_ami(self):
        cluster = self.get_cluster()

        self.assertTrue(self.installed_spark_application(cluster))
        self.assertFalse(self.ran_spark_bootstrap_action(cluster))

    def test_3_11_0_ami(self):
        cluster = self.get_cluster('--image-version', '3.11.0')

        self.assertTrue(self.ran_spark_bootstrap_action(cluster))
        self.assertFalse(self.installed_spark_application(cluster))

    def test_3_7_0_ami(self):
        cluster = self.get_cluster('--image-version', '3.7.0')
        self.assertTrue(self.log.warning.called)
        self.assertTrue(
            any('Spark' in args[0]
                for args, kwargs in self.log.warning.call_args_list))

        # try bootstrapping Spark anyway
        self.assertTrue(self.ran_spark_bootstrap_action(cluster))
        self.assertFalse(self.installed_spark_application(cluster))

    def test_4_7_2_ami(self):
        cluster = self.get_cluster('--image-version', '4.7.2')

        self.assertTrue(self.installed_spark_application(cluster))
        self.assertFalse(self.ran_spark_bootstrap_action(cluster))

    def test_dont_run_install_spark_twice(self):

        cluster = self.get_cluster(
            '--image-version', '3.11.0',
            '--bootstrap-action', 's3://bucket/install-spark')

        # should run the custom bootstrap action but not the default one
        self.assertTrue(self.ran_spark_bootstrap_action(
            cluster, 's3://bucket/install-spark'))
        self.assertFalse(
            self.ran_spark_bootstrap_action(
                cluster, _3_X_SPARK_BOOTSTRAP_ACTION))
        self.assertFalse(self.installed_spark_application(cluster))

    def test_dont_add_two_spark_applications(self):

        cluster = self.get_cluster(
            '--image-version', '4.7.2',
            '--application', 'spark')

        # shouldn't add "Spark" application on top of "spark"
        self.assertTrue(self.installed_spark_application(cluster, 'spark'))
        self.assertFalse(self.installed_spark_application(cluster, 'Spark'))
        self.assertFalse(self.ran_spark_bootstrap_action(cluster))


class ShouldBootstrapSparkTestCase(MockBoto3TestCase):

    def test_default(self):
        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._should_bootstrap_spark(), False)

    def test_explicit_true(self):
        job = MRTwoStepJob(['-r', 'emr', '--bootstrap-spark'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._should_bootstrap_spark(), True)

    def test_spark_job(self):
        job = MRNullSpark(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._should_bootstrap_spark(), True)

    def test_spark_script_job(self):
        job = MRSparkScript(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._should_bootstrap_spark(), True)

    def test_explicit_false(self):
        job = MRNullSpark(['-r', 'emr', '--no-bootstrap-spark'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._should_bootstrap_spark(), False)


class EMRTagsTestCase(MockBoto3TestCase):

    def test_default_tags(self):
        cluster = self.run_and_get_cluster()

        tags = _extract_tags(cluster)

        self.assertNotIn('__mrjob_pool_hash', tags)
        self.assertNotIn('__mrjob_pool_name', tags)
        self.assertEqual(tags['__mrjob_version'], mrjob.__version__)

    def test_pooling(self):
        cluster = self.run_and_get_cluster('--pool-clusters')

        tags = _extract_tags(cluster)

        self.assertEqual(tags['__mrjob_pool_name'], 'default')
        self.assertEqual(tags['__mrjob_version'], mrjob.__version__)
        self.assertIn('__mrjob_pool_hash', tags)

    def test_tags_option_dict(self):
        job = MRWordCount([
            '-r', 'emr',
            '--tag', 'tag_one=foo',
            '--tag', 'tag_two=bar'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner._opts['tags'],
                             {'tag_one': 'foo', 'tag_two': 'bar'})

    def test_command_line_overrides_config(self):
        TAGS_MRJOB_CONF = {'runners': {'emr': {
            'check_cluster_every': 0.00,
            'cloud_fs_sync_secs': 0.00,
            'tags': {
                'tag_one': 'foo',
                'tag_two': None,
                'tag_three': 'bar',
            },
        }}}

        job = MRWordCount(['-r', 'emr', '--tag', 'tag_two=qwerty'])
        job.sandbox()

        with mrjob_conf_patcher(TAGS_MRJOB_CONF):
            with job.make_runner() as runner:
                self.assertEqual(runner._opts['tags'],
                                 {'tag_one': 'foo',
                                  'tag_two': 'qwerty',
                                  'tag_three': 'bar'})

    def test_tags_get_created(self):
        cluster = self.run_and_get_cluster('--tag', 'tag_one=foo',
                                           '--tag', 'tag_two=bar')

        self.assertEqual(
            _extract_non_mrjob_tags(cluster),
            dict(tag_one='foo', tag_two='bar'))

    def test_blank_tag_value(self):
        cluster = self.run_and_get_cluster('--tag', 'tag_one=foo',
                                           '--tag', 'tag_two=')

        self.assertEqual(
            _extract_non_mrjob_tags(cluster),
            dict(tag_one='foo', tag_two=''))

    def test_tag_values_can_be_none(self):
        runner = EMRJobRunner(conf_paths=[], tags={'tag_one': None},
                              pool_clusters=False)
        cluster_id = runner.make_persistent_cluster()
        cluster = self.mock_emr_clusters[cluster_id]

        self.assertEqual(
            _extract_non_mrjob_tags(cluster),
            dict(tag_one=''))

    def test_persistent_cluster(self):
        args = ['--tag', 'tag_one=foo',
                '--tag', 'tag_two=bar']

        with self.make_runner(*args) as runner:
            cluster_id = runner.make_persistent_cluster()
            cluster = self.mock_emr_clusters[cluster_id]

        self.assertEqual(
            _extract_non_mrjob_tags(cluster),
            dict(tag_one='foo', tag_two='bar'))


# this isn't actually enough to support GovCloud; see:
# http://docs.aws.amazon.com/govcloud-us/latest/UserGuide/using-govcloud-arns.html  # noqa
class IAMEndpointTestCase(MockBoto3TestCase):

    def test_default(self):
        runner = EMRJobRunner()

        iam_client = runner.make_iam_client()
        self.assertEqual(iam_client.meta.endpoint_url,
                         'https://iam.amazonaws.com')

    def test_explicit_iam_endpoint(self):
        runner = EMRJobRunner(iam_endpoint='https://iam.us-gov.amazonaws.com')

        iam_client = runner.make_iam_client()
        self.assertEqual(iam_client.meta.endpoint_url,
                         'https://iam.us-gov.amazonaws.com')

    def test_iam_endpoint_option(self):
        # also test hostname without scheme
        mr_job = MRJob(
            ['-r', 'emr', '--iam-endpoint', 'iam.us-gov.amazonaws.com'])

        with mr_job.make_runner() as runner:
            iam_client = runner.make_iam_client()
            self.assertEqual(iam_client.meta.endpoint_url,
                             'https://iam.us-gov.amazonaws.com')


class SetupLineEncodingTestCase(MockBoto3TestCase):

    def test_setup_wrapper_script_uses_local_line_endings(self):
        job = MRTwoStepJob(['-r', 'emr', '--setup', 'true'])
        job.sandbox(stdin=BytesIO(b'foo\nbar\n'))

        # tests #1071. Unfortunately, we mostly run these tests on machines
        # that use unix line endings anyway. So monitor open() instead
        with patch(
                'mrjob.bin.open', create=True, side_effect=open) as m_open:
            with logger_disabled('mrjob.emr'):
                with job.make_runner() as runner:
                    runner.run()

                    self.assertIn(
                        call(runner._setup_wrapper_script_path, 'wb'),
                        m_open.mock_calls)


class WaitForLogsOnS3TestCase(MockBoto3TestCase):

    def setUp(self):
        super(WaitForLogsOnS3TestCase, self).setUp()

        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox(stdin=BytesIO(b'foo\nbar\n'))

        self.runner = job.make_runner()
        self.runner._launch()

        self.cluster = self.mock_emr_clusters[self.runner._cluster_id]

        self.mock_log = self.start(patch('mrjob.emr.log'))

        self.mock_sleep = self.start(patch('time.sleep'))

    def assert_waits_ten_minutes(self):
        waited = set(self.runner._waited_for_logs_on_s3)
        step_num = len(self.runner._log_interpretations)

        self.runner._wait_for_logs_on_s3()

        self.assertTrue(self.mock_log.info.called)
        self.mock_sleep.assert_called_once_with(600)

        self.assertEqual(
            self.runner._waited_for_logs_on_s3,
            waited | set([step_num]))

    def assert_silently_exits(self):
        state = self.cluster['Status']['State']
        waited = set(self.runner._waited_for_logs_on_s3)

        self.runner._wait_for_logs_on_s3()

        self.assertFalse(self.mock_log.info.called)
        self.assertEqual(waited, self.runner._waited_for_logs_on_s3)
        self.assertEqual(self.runner._describe_cluster()['Status']['State'],
                         state)

    def test_starting(self):
        self.cluster['Status']['State'] = 'STARTING'
        self.assert_waits_ten_minutes()

    def test_bootstrapping(self):
        self.cluster['Status']['State'] = 'BOOTSTRAPPING'
        self.assert_waits_ten_minutes()

    def test_running(self):
        self.cluster['Status']['State'] = 'RUNNING'
        self.assert_waits_ten_minutes()

    def test_waiting(self):
        self.cluster['Status']['State'] = 'WAITING'
        self.assert_waits_ten_minutes()

    def test_terminating(self):
        self.cluster['Status']['State'] = 'TERMINATING'
        self.cluster['_DelayProgressSimulation'] = 1

        self.runner._wait_for_logs_on_s3()

        self.assertEqual(self.runner._describe_cluster()['Status']['State'],
                         'TERMINATED')
        self.assertTrue(self.mock_log.info.called)

    def test_terminated(self):
        self.cluster['Status']['State'] = 'TERMINATED'
        self.assert_silently_exits()

    def test_terminated_with_errors(self):
        self.cluster['Status']['State'] = 'TERMINATED_WITH_ERRORS'
        self.assert_silently_exits()

    def test_ctrl_c(self):
        self.mock_sleep.side_effect = KeyboardInterrupt

        self.assertEqual(self.runner._waited_for_logs_on_s3, set())

        self.runner._wait_for_logs_on_s3()

        self.assertTrue(self.mock_log.info.called)
        self.mock_sleep.assert_called_once_with(600)

        # still shouldn't make user ctrl-c again
        self.assertEqual(self.runner._waited_for_logs_on_s3, set([0]))

    def test_already_waited_ten_minutes(self):
        self.runner._waited_for_logs_on_s3.add(0)
        self.assert_silently_exits()

    def test_waited_for_previous_step(self):
        self.runner._waited_for_logs_on_s3.add(0)
        self.runner._log_interpretations.append({})

        self.assert_waits_ten_minutes()


class StreamLogDirsTestCase(MockBoto3TestCase):

    def setUp(self):
        super(StreamLogDirsTestCase, self).setUp()

        self.log = self.start(patch('mrjob.emr.log'))

        self._address_of_master = self.start(patch(
            'mrjob.emr.EMRJobRunner._address_of_master',
            return_value='master'))

        self.get_image_version = self.start(patch(
            'mrjob.emr.EMRJobRunner.get_image_version',
            return_value=_DEFAULT_IMAGE_VERSION))

        self.get_hadoop_version = self.start(patch(
            'mrjob.emr.EMRJobRunner.get_hadoop_version',
            return_value='2.4.0'))

        self.ssh_worker_hosts = self.start(patch(
            'mrjob.emr.EMRJobRunner._ssh_worker_hosts',
            return_value=['core1', 'core2', 'task1']))

        self._s3_log_dir = self.start(patch(
            'mrjob.emr.EMRJobRunner._s3_log_dir',
            return_value='s3://bucket/logs/j-CLUSTERID'))

        self._wait_for_logs_on_s3 = self.start(patch(
            'mrjob.emr.EMRJobRunner'
            '._wait_for_logs_on_s3'))

    def _test_stream_bootstrap_log_dirs(
            self, ssh=False,
            action_num=0, node_id='i-b659f519',
            expected_s3_dir_name='node/i-b659f519/bootstrap-actions/1'):

        # ssh doesn't matter, but let's test it
        ec2_key_pair_file = '/path/to/EMR.pem' if ssh else None
        runner = EMRJobRunner(ec2_key_pair_file=ec2_key_pair_file)

        results = runner._stream_bootstrap_log_dirs(
            action_num=action_num, node_id=node_id)

        self.log.info.reset_mock()

        self.assertEqual(next(results), [
            's3://bucket/logs/j-CLUSTERID/' + expected_s3_dir_name,
        ])
        self.assertTrue(
            self._wait_for_logs_on_s3.called)
        self.log.info.assert_called_once_with(
            'Looking for bootstrap logs in'
            ' s3://bucket/logs/j-CLUSTERID/' +
            expected_s3_dir_name + '...')

        self.assertRaises(StopIteration, next, results)

    def test_stream_history_log_dirs_without_ssh(self):
        self._test_stream_bootstrap_log_dirs()

    def test_stream_history_log_dirs_with_ssh(self):
        # shouldn't make a difference
        self._test_stream_bootstrap_log_dirs(ssh=True)

    def test_stream_history_log_dirs_without_action_num(self):
        self._test_stream_bootstrap_log_dirs(
            action_num=None, expected_s3_dir_name='node')

    def test_stream_history_log_dirs_without_node_id(self):
        self._test_stream_bootstrap_log_dirs(
            action_num=None, expected_s3_dir_name='node')

    def _test_stream_history_log_dirs(
            self, ssh, image_version=_DEFAULT_IMAGE_VERSION,
            expected_dir_name='hadoop/history',
            expected_s3_dir_name='jobs'):
        ec2_key_pair_file = '/path/to/EMR.pem' if ssh else None
        runner = EMRJobRunner(ec2_key_pair_file=ec2_key_pair_file)
        self.get_image_version.return_value = image_version

        results = runner._stream_history_log_dirs()

        if ssh:
            self.log.info.reset_mock()

            self.assertEqual(next(results), [
                'ssh://master/mnt/var/log/' + expected_dir_name,
            ])
            self.assertFalse(
                self._wait_for_logs_on_s3.called)
            self.log.info.assert_called_once_with(
                'Looking for history log in /mnt/var/log/' +
                expected_dir_name + ' on master...')

        self.log.info.reset_mock()

        self.assertEqual(next(results), [
            's3://bucket/logs/j-CLUSTERID/' + expected_s3_dir_name,
        ])
        self.assertTrue(
            self._wait_for_logs_on_s3.called)
        self.log.info.assert_called_once_with(
            'Looking for history log in'
            ' s3://bucket/logs/j-CLUSTERID/' +
            expected_s3_dir_name + '...')

        self.assertRaises(StopIteration, next, results)

    def test_stream_history_log_dirs_from_2_x_amis_with_ssh(self):
        self._test_stream_history_log_dirs(
            image_version='2.4.11', ssh=True)

    def test_stream_history_log_dirs_from_2_x_amis_without_ssh(self):
        self._test_stream_history_log_dirs(
            image_version='2.4.11', ssh=False)

    def test_cant_stream_history_log_dirs_from_3_x_amis(self):
        runner = EMRJobRunner(image_version='3.11.0')
        results = runner._stream_history_log_dirs()
        self.assertRaises(StopIteration, next, results)

    def test_stream_history_log_dirs_from_4_x_amis(self):
        # history log fetching is disabled until we fix #1253
        runner = EMRJobRunner(image_version='4.3.0')
        results = runner._stream_history_log_dirs()
        self.assertRaises(StopIteration, next, results)
        #self._test_stream_history_log_dirs(
        #    ssh=True, image_version='4.3.0',
        #    expected_dir_name='hadoop-mapreduce/history',
        #    expected_s3_dir_name='hadoop-mapreduce/history')

    def _test_stream_step_log_dirs(self, ssh):
        ec2_key_pair_file = '/path/to/EMR.pem' if ssh else None
        runner = EMRJobRunner(ec2_key_pair_file=ec2_key_pair_file)
        self.get_hadoop_version.return_value = '1.0.3'

        results = runner._stream_step_log_dirs('s-STEPID')

        if ssh:
            self.log.info.reset_mock()

            self.assertEqual(next(results), [
                'ssh://master/mnt/var/log/hadoop/steps/s-STEPID',
            ])
            self.assertFalse(
                self._wait_for_logs_on_s3.called)
            self.log.info.assert_called_once_with(
                'Looking for step log in /mnt/var/log/hadoop/steps/s-STEPID'
                ' on master...')

        self.log.info.reset_mock()

        self.assertEqual(next(results), [
            's3://bucket/logs/j-CLUSTERID/steps/s-STEPID',
        ])
        self.assertTrue(
            self._wait_for_logs_on_s3.called)
        self.log.info.assert_called_once_with(
            'Looking for step log in'
            ' s3://bucket/logs/j-CLUSTERID/steps/s-STEPID...')

        self.assertRaises(StopIteration, next, results)

    def test_stream_step_log_dirs_with_ssh(self):
        self._test_stream_step_log_dirs(ssh=True)

    def test_stream_step_log_dirs_without_ssh(self):
        self._test_stream_step_log_dirs(ssh=False)

    def _test_stream_task_log_dirs(
        self, ssh, application_id=None,
        image_version=_DEFAULT_IMAGE_VERSION,
        expected_local_path='/mnt/var/log/hadoop/userlogs',
        expected_dir_name='hadoop-yarn/containers',
        expected_s3_dir_name='containers',
    ):
        ec2_key_pair_file = '/path/to/EMR.pem' if ssh else None
        runner = EMRJobRunner(ec2_key_pair_file=ec2_key_pair_file)
        self.get_hadoop_version.return_value = '1.0.3'
        self.get_image_version.return_value = image_version

        results = runner._stream_task_log_dirs(application_id=application_id)

        if ssh:
            self.log.reset_mock()

            local_path = '/mnt/var/log/hadoop/userlogs'
            if application_id:
                local_path = posixpath.join(local_path, application_id)

            self.assertEqual(next(results), [
                'ssh://master/mnt/var/log/' + expected_dir_name,
                'ssh://master!core1/mnt/var/log/' + expected_dir_name,
                'ssh://master!core2/mnt/var/log/' + expected_dir_name,
                'ssh://master!task1/mnt/var/log/' + expected_dir_name,
            ])
            self.assertFalse(self.log.warning.called)
            self.log.info.assert_called_once_with(
                'Looking for task logs in /mnt/var/log/' +
                expected_dir_name + ' on master and task/core nodes...')

            self.assertFalse(
                self._wait_for_logs_on_s3.called)

        self.log.reset_mock()

        self.assertEqual(next(results), [
            's3://bucket/logs/j-CLUSTERID/' + expected_s3_dir_name,
        ])
        self.assertTrue(
            self._wait_for_logs_on_s3.called)
        self.log.info.assert_called_once_with(
            'Looking for task logs in'
            ' s3://bucket/logs/j-CLUSTERID/' +
            expected_s3_dir_name + '...')

        self.assertRaises(StopIteration, next, results)

    def test_stream_task_log_dirs_with_ssh(self):
        self._test_stream_task_log_dirs(ssh=True)

    def test_stream_task_log_dirs_without_ssh(self):
        self._test_stream_task_log_dirs(ssh=False)

    def test_stream_task_log_dirs_with_application_id(self):
        self._test_stream_task_log_dirs(
            ssh=True, application_id='application_1',
            expected_dir_name='hadoop-yarn/containers/application_1',
            expected_s3_dir_name='containers/application_1')

    def test_stream_task_log_dirs_from_3_x_amis(self):
        self._test_stream_task_log_dirs(
            ssh=True,
            image_version='3.11.0',
            expected_dir_name='hadoop/userlogs',
            expected_s3_dir_name='task-attempts')


class LsStepSyslogsTestCase(MockBoto3TestCase):

    def setUp(self):
        super(LsStepSyslogsTestCase, self).setUp()

        self.log = self.start(patch('mrjob.emr.log'))

        self._ls_emr_step_syslogs = self.start(patch(
            'mrjob.emr._ls_emr_step_syslogs'))
        self._stream_step_log_dirs = self.start(patch(
            'mrjob.emr.EMRJobRunner._stream_step_log_dirs'))

    def test_basic(self):
        # just verify that the keyword args get passed through and
        # that logging happens in the right order

        self._ls_emr_step_syslogs.return_value = [
            dict(path='s3://bucket/logs/steps/syslog'),
        ]

        runner = EMRJobRunner()

        self.log.info.reset_mock()

        results = runner._ls_step_syslogs(step_id='s-STEPID')

        self.assertFalse(self.log.info.called)

        self.assertEqual(next(results),
                         dict(path='s3://bucket/logs/steps/syslog'))

        self._stream_step_log_dirs.assert_called_once_with(
            step_id='s-STEPID')
        self._ls_emr_step_syslogs.assert_called_once_with(
            runner.fs,
            self._stream_step_log_dirs.return_value,
            step_id='s-STEPID')

        self.assertEqual(self.log.info.call_count, 1)
        self.assertIn('s3://bucket/logs/steps/syslog',
                      self.log.info.call_args[0][0])

        self.assertRaises(StopIteration, next, results)


class LsStepStderrLogsTestCase(MockBoto3TestCase):

    def setUp(self):
        super(LsStepStderrLogsTestCase, self).setUp()

        self.log = self.start(patch('mrjob.emr.log'))

        self._ls_emr_step_stderr_logs = self.start(patch(
            'mrjob.emr._ls_emr_step_stderr_logs'))
        self._stream_step_log_dirs = self.start(patch(
            'mrjob.emr.EMRJobRunner._stream_step_log_dirs'))

    def test_basic(self):
        # just verify that the keyword args get passed through and
        # that logging happens in the right order

        self._ls_emr_step_stderr_logs.return_value = [
            dict(path='s3://bucket/logs/steps/stderr'),
        ]

        runner = EMRJobRunner()

        self.log.info.reset_mock()

        results = runner._ls_step_stderr_logs(step_id='s-STEPID')

        self.assertFalse(self.log.info.called)

        self.assertEqual(next(results),
                         dict(path='s3://bucket/logs/steps/stderr'))

        self._stream_step_log_dirs.assert_called_once_with(
            step_id='s-STEPID')
        self._ls_emr_step_stderr_logs.assert_called_once_with(
            runner.fs,
            self._stream_step_log_dirs.return_value,
            step_id='s-STEPID')

        self.assertEqual(self.log.info.call_count, 1)
        self.assertIn('s3://bucket/logs/steps/stderr',
                      self.log.info.call_args[0][0])

        self.assertRaises(StopIteration, next, results)


class GetStepLogInterpretationTestCase(MockBoto3TestCase):

    def setUp(self):
        super(GetStepLogInterpretationTestCase, self).setUp()

        self.log = self.start(patch('mrjob.emr.log'))

        self._interpret_emr_step_syslog = self.start(patch(
            'mrjob.emr._interpret_emr_step_syslog'))
        self._ls_step_syslogs = self.start(patch(
            'mrjob.emr.EMRJobRunner._ls_step_syslogs'))

        self._interpret_emr_step_stderr = self.start(patch(
            'mrjob.emr._interpret_emr_step_stderr'))
        self._ls_step_stderr_logs = self.start(patch(
            'mrjob.emr.EMRJobRunner._ls_step_stderr_logs'))

    def test_basic(self):
        runner = EMRJobRunner()

        log_interpretation = dict(step_id='s-STEPID')

        self.log.reset_mock()

        self.assertEqual(
            runner._get_step_log_interpretation(
                log_interpretation, 'streaming'),
            self._interpret_emr_step_syslog.return_value)

        self.assertFalse(self.log.warning.called)
        self._ls_step_syslogs.assert_called_once_with(step_id='s-STEPID')
        self._interpret_emr_step_syslog.assert_called_once_with(
            runner.fs, self._ls_step_syslogs.return_value)
        self.assertFalse(self._ls_step_stderr_logs.called)
        self.assertFalse(self._interpret_emr_step_stderr.called)

    def test_no_step_id(self):
        runner = EMRJobRunner()

        log_interpretation = {}

        self.log.reset_mock()

        self.assertEqual(
            runner._get_step_log_interpretation(
                log_interpretation, 'streaming'),
            None)

        self.assertTrue(self.log.warning.called)
        self.assertFalse(self._ls_step_syslogs.called)
        self.assertFalse(self._interpret_emr_step_syslog.called)
        self.assertFalse(self._ls_step_stderr_logs.called)
        self.assertFalse(self._interpret_emr_step_stderr.called)

    def test_fallback_to_stderr(self):
        runner = EMRJobRunner()

        log_interpretation = dict(step_id='s-STEPID')

        self.log.reset_mock()

        self._interpret_emr_step_syslog.return_value = {}

        self.assertEqual(
            runner._get_step_log_interpretation(
                log_interpretation, 'streaming'),
            self._interpret_emr_step_stderr.return_value)

        self.assertFalse(self.log.warning.called)
        self._ls_step_syslogs.assert_called_once_with(step_id='s-STEPID')
        self._interpret_emr_step_syslog.assert_called_once_with(
            runner.fs, self._ls_step_syslogs.return_value)
        self._ls_step_stderr_logs.assert_called_once_with(step_id='s-STEPID')
        self._interpret_emr_step_stderr.assert_called_once_with(
            runner.fs, self._ls_step_stderr_logs.return_value)

    def test_spark(self):
        runner = EMRJobRunner()

        log_interpretation = dict(step_id='s-STEPID')

        self.log.reset_mock()

        self.assertEqual(
            runner._get_step_log_interpretation(
                log_interpretation, 'spark'),
            self._interpret_emr_step_syslog.return_value)

        self.assertFalse(self.log.warning.called)
        self.assertFalse(self._ls_step_syslogs.called)
        self._ls_step_stderr_logs.assert_called_once_with(step_id='s-STEPID')
        self._interpret_emr_step_syslog.assert_called_once_with(
            runner.fs, self._ls_step_stderr_logs.return_value)
        self.assertFalse(self._interpret_emr_step_stderr.called)


# this basically just checks that hadoop_extra_args is an option
# for the EMR runner
class HadoopExtraArgsOnEMRTestCase(HadoopExtraArgsTestCase, MockBoto3TestCase):

    def setUp(self):
        super(HadoopExtraArgsTestCase, self).setUp()

        self.start(patch(
            'mrjob.emr.EMRJobRunner.get_hadoop_version',
            return_value='2.4.0'))

    RUNNER = 'emr'


# make sure we don't override the partitioner on EMR (tests #1294)
class PartitionerTestCase(MockBoto3TestCase):

    def setUp(self):
        super(PartitionerTestCase, self).setUp()
        # _hadoop_args_for_step() needs this
        self.start(patch(
            'mrjob.emr.EMRJobRunner.get_hadoop_version',
            return_value='1.2.0'))

    def test_sort_values(self):
        job = MRSortValues(['-r', 'emr'])

        with job.make_runner() as runner:
            self.assertEqual(
                runner._hadoop_args_for_step(0), [
                    '-D', 'mapred.text.key.partitioner.options=-k1,1',
                    '-D', 'stream.num.map.output.key.fields=2',
                    '-partitioner',
                    'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner',
                ])


class EMRApplicationsTestCase(MockBoto3TestCase):

    def test_default_on_3_x_ami(self):
        job = MRTwoStepJob(['-r', 'emr', '--image-version', '3.11.0'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner._applications(), set())

            runner._launch()
            cluster = runner._describe_cluster()

            applications = set(a['Name'] for a in cluster['Applications'])
            self.assertEqual(applications, set(['hadoop']))

    def test_default_on_4_x_ami(self):
        job = MRTwoStepJob(['-r', 'emr', '--image-version', '4.3.0'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner._applications(), set())

            runner._launch()
            cluster = runner._describe_cluster()

            applications = set(a['Name'] for a in cluster['Applications'])
            self.assertEqual(applications, set(['Hadoop']))

    def test_applications_requires_4_x_ami(self):
        job = MRTwoStepJob(
            ['-r', 'emr',
             '--image-version', '3.11.0',
             '--application', 'Hadoop',
             '--application', 'Mahout'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertRaises(ClientError, runner._launch)

    def test_explicit_hadoop(self):
        job = MRTwoStepJob(
            ['-r', 'emr',
             '--application', 'Hadoop',
             '--application', 'Mahout'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner._applications(),
                             set(['Hadoop', 'Mahout']))

            runner._launch()
            cluster = runner._describe_cluster()

            applications = set(a['Name'] for a in cluster['Applications'])
            self.assertEqual(applications,
                             set(['Hadoop', 'Mahout']))

    def test_implicit_hadoop(self):
        job = MRTwoStepJob(
            ['-r', 'emr',
             '--application', 'Mahout'])
        job.sandbox()

        with job.make_runner() as runner:
            # we explicitly add Hadoop so we can see Hadoop version in
            # the cluster description from the API
            self.assertEqual(runner._applications(),
                             set(['Hadoop', 'Mahout']))

            runner._launch()
            cluster = runner._describe_cluster()

            applications = set(a['Name'] for a in cluster['Applications'])
            self.assertEqual(applications,
                             set(['Hadoop', 'Mahout']))


class EMRConfigurationsTestCase(MockBoto3TestCase):

    # example from:
    # http://docs.aws.amazon.com/ElasticMapReduce/latest/ReleaseGuide/emr-configure-apps.html  # noqa

    def test_default(self):
        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()
            cluster = runner._describe_cluster()

            self.assertFalse(hasattr(cluster, 'configurations'))

    def test_requires_4_x_ami(self):
        self.start(mrjob_conf_patcher(dict(runners=dict(emr=dict(
            emr_configurations=[CORE_SITE_EMR_CONFIGURATION])))))

        job = MRTwoStepJob(['-r', 'emr', '--image-version', '3.11.0'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertRaises(ClientError, runner._launch)

    def _test_normalized_emr_configurations(self, emr_configurations):

        self.start(mrjob_conf_patcher(dict(runners=dict(emr=dict(
            image_version='4.3.0',
            emr_configurations=emr_configurations)))))

        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner._opts['emr_configurations'],
                             emr_configurations)

            runner._launch()
            cluster = runner._describe_cluster()

            self.assertEqual(cluster['Configurations'], emr_configurations)

    def test_basic_emr_configuration(self, raw=None):
        self._test_normalized_emr_configurations(
            [CORE_SITE_EMR_CONFIGURATION])

    def test_complex_emr_configurations(self):
        self._test_normalized_emr_configurations(
            [CORE_SITE_EMR_CONFIGURATION, HADOOP_ENV_EMR_CONFIGURATION])

    def test_normalization(self):
        self.start(mrjob_conf_patcher(dict(runners=dict(emr=dict(
            image_version='4.3.0',
            emr_configurations=[
                HADOOP_ENV_EMR_CONFIGURATION_VARIANT])))))

        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner._opts['emr_configurations'],
                             [HADOOP_ENV_EMR_CONFIGURATION])

    def test_command_line_switch(self):
        job = MRTwoStepJob(
            ['-r', 'emr',
             '--image-version', '4.3.0',
             '--emr-configuration', json.dumps(CORE_SITE_EMR_CONFIGURATION),
             '--emr-configuration', json.dumps(HADOOP_ENV_EMR_CONFIGURATION),
             ])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner._opts['emr_configurations'],
                             [CORE_SITE_EMR_CONFIGURATION,
                              HADOOP_ENV_EMR_CONFIGURATION])

    def test_combine_command_line_with_conf(self):
        self.start(mrjob_conf_patcher(dict(runners=dict(emr=dict(
            image_version='4.3.0',
            emr_configurations=[
                CORE_SITE_EMR_CONFIGURATION])))))

        job = MRTwoStepJob(
            ['-r', 'emr',
             '--emr-configuration', json.dumps(HADOOP_ENV_EMR_CONFIGURATION),
             ])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner._opts['emr_configurations'],
                             [CORE_SITE_EMR_CONFIGURATION,
                              HADOOP_ENV_EMR_CONFIGURATION])


class GetJobStepsTestCase(MockBoto3TestCase):

    def test_empty(self):
        runner = EMRJobRunner()
        runner.make_persistent_cluster()

        self.assertEqual(runner.get_job_steps(), [])

    def test_own_cluster(self):
        job = MRTwoStepJob(['-r', 'emr']).sandbox()

        with job.make_runner() as runner:
            runner._launch()

            all_steps = _list_all_steps(runner)

            # ensure that steps appear in correct order (see #1316)
            self.assertIn('Step 1', all_steps[0]['Name'])
            self.assertIn('Step 2', all_steps[1]['Name'])

            self.assertEqual(runner.get_job_steps(), all_steps[:2])

    def test_shared_cluster(self):
        cluster_id = EMRJobRunner().make_persistent_cluster()

        def add_other_steps(runner, n):
            emr_client = runner.make_emr_client()
            emr_client.add_job_flow_steps(
                JobFlowId=runner.get_cluster_id(),
                Steps=[dict(Name='dummy step',
                            HadoopJarStep=(dict(Jar='dummy.jar')))] * n,
            )

        job = MRTwoStepJob(['-r', 'emr', '--cluster-id', cluster_id]).sandbox()

        with job.make_runner() as runner:
            add_other_steps(runner, 50)
            runner._launch()
            add_other_steps(runner, 3)

            all_steps = _list_all_steps(runner)

            # make sure these are our steps, and they are in the right order
            # (see #1316)
            self.assertIn(runner._job_key, all_steps[50]['Name'])
            self.assertIn('Step 1', all_steps[50]['Name'])
            self.assertIn(runner._job_key, all_steps[51]['Name'])
            self.assertIn('Step 2', all_steps[51]['Name'])

            self.assertEqual(runner.get_job_steps(), all_steps[50:52])


class WaitForStepsToCompleteTestCase(MockBoto3TestCase):

    # TODO: test more functionality. This currently
    # mostly ensures that we open the SSH tunnel at an appropriate time

    class StopTest(Exception):
        pass

    def setUp(self):
        super(WaitForStepsToCompleteTestCase, self).setUp()

        # mock out setting up SSH tunnel
        self.start(patch.object(EMRJobRunner, '_set_up_ssh_tunnel'))

        # mock out logging
        self.start(patch('mrjob.emr.log'))

        # track number of calls to _wait_for_step_to_complete()
        #
        # need to keep a ref to the mock; apparently, when side_effect/autospec
        # is used, we can read mock attributes of
        # EMRJobRunner._wait_for_step_to_complete but not write them
        self._wait_for_step_to_complete = self.start(patch.object(
            EMRJobRunner, '_wait_for_step_to_complete',
            side_effect=EMRJobRunner._wait_for_step_to_complete,
            autospec=True))

    def make_runner(self, *extra_args):
        """Make a runner for a two step job and launch it."""
        job = MRTwoStepJob(['-r', 'emr'] + list(extra_args)).sandbox()
        runner = job.make_runner()
        runner._launch()
        return runner

    def test_basic(self):
        runner = self.make_runner()

        runner._wait_for_steps_to_complete()

        self.assertEqual(EMRJobRunner._wait_for_step_to_complete.call_count, 2)
        self.assertTrue(EMRJobRunner._set_up_ssh_tunnel.called)
        self.assertEqual(len(runner._log_interpretations), 2)
        self.assertIsNone(runner._mns_log_interpretation)

    def test_master_node_setup(self):
        fake_jar = os.path.join(self.tmp_dir, 'fake.jar')
        with open(fake_jar, 'w'):
            pass

        # --libjar is currently the only way to create the master
        # node setup script
        runner = self.make_runner('--libjar', fake_jar)

        runner._add_master_node_setup_files_for_upload()
        runner._wait_for_steps_to_complete()

        self.assertIsNotNone(runner._master_node_setup_script_path)

        self.assertEqual(EMRJobRunner._wait_for_step_to_complete.call_count, 3)
        self.assertTrue(EMRJobRunner._set_up_ssh_tunnel.called)
        self.assertEqual(len(runner._log_interpretations), 2)
        self.assertIsNotNone(runner._mns_log_interpretation)
        self.assertEqual(runner._mns_log_interpretation['no_job'], True)

    def test_blanks_out_log_interpretations(self):
        runner = self.make_runner()

        runner._log_interpretations = ['foo', 'bar', 'baz']
        runner._mns_log_interpretation = 'qux'

        self._wait_for_step_to_complete.side_effect = self.StopTest

        self.assertRaises(self.StopTest, runner._wait_for_steps_to_complete)
        self.assertEqual(runner._log_interpretations, [])
        self.assertEqual(runner._mns_log_interpretation, None)

    def test_open_ssh_tunnel_when_first_step_runs(self):
        # normally, we'll open the SSH tunnel when the first step
        # is RUNNING

        # stop the test as soon as SSH tunnel is set up
        EMRJobRunner._set_up_ssh_tunnel.side_effect = self.StopTest

        runner = self.make_runner()

        self.assertRaises(self.StopTest, runner._wait_for_steps_to_complete)

        self.assertEqual(EMRJobRunner._wait_for_step_to_complete.call_count, 1)

        mock_cluster = self.mock_emr_clusters[runner._cluster_id]
        mock_steps = mock_cluster['_Steps']

        self.assertEqual(len(mock_steps), 2)
        self.assertEqual(mock_steps[0]['Status']['State'], 'RUNNING')

    def test_open_ssh_tunnel_if_cluster_running(self):
        # tests #1115

        # stop the test as soon as SSH tunnel is set up
        EMRJobRunner._set_up_ssh_tunnel.side_effect = self.StopTest

        runner = self.make_runner()
        mock_cluster = self.mock_emr_clusters[runner._cluster_id]
        mock_cluster['Status']['State'] = 'RUNNING'
        mock_cluster['MasterPublicDnsName'] = 'mockmaster'

        # run until SSH tunnel is set up
        self.assertRaises(self.StopTest, runner._wait_for_steps_to_complete)

        self.assertFalse(EMRJobRunner._wait_for_step_to_complete.called)

    def test_open_ssh_tunnel_if_cluster_waiting(self):
        # tests #1115

        # stop the test as soon as SSH tunnel is set up
        EMRJobRunner._set_up_ssh_tunnel.side_effect = self.StopTest

        runner = self.make_runner()
        mock_cluster = self.mock_emr_clusters[runner._cluster_id]
        mock_cluster['Status']['State'] = 'WAITING'
        mock_cluster['MasterPublicDnsName'] = 'mockmaster'

        # run until SSH tunnel is set up
        self.assertRaises(self.StopTest, runner._wait_for_steps_to_complete)

        self.assertFalse(EMRJobRunner._wait_for_step_to_complete.called)

    def test_open_ssh_tunnel_when_step_pending_but_cluster_running(self):
        # tests #1115

        # stop the test as soon as SSH tunnel is set up
        EMRJobRunner._set_up_ssh_tunnel.side_effect = self.StopTest

        # put steps from previous job on cluster
        previous_runner = self.make_runner()

        runner = self.make_runner(
            '--cluster-id', previous_runner.get_cluster_id())

        mock_cluster = self.mock_emr_clusters[runner._cluster_id]
        mock_steps = mock_cluster['_Steps']

        # sanity-check: are steps from the previous cluster on there?
        self.assertEqual(len(mock_steps), 4)

        # run until ssh tunnel is called
        self.assertRaises(self.StopTest, runner._wait_for_steps_to_complete)

        # should have only waited for first step
        self.assertEqual(EMRJobRunner._wait_for_step_to_complete.call_count, 1)

        # cluster should be running, step should still be pending
        self.assertEqual(mock_cluster['Status']['State'], 'RUNNING')
        self.assertIn(runner._job_key, mock_steps[2]['Name'])
        self.assertEqual(mock_steps[2]['Status']['State'], 'PENDING')

    def test_terminated_cluster(self):
        runner = self.make_runner()

        self.start(patch(
            'tests.mock_boto3.emr.MockEMRClient.describe_step',
            return_value=dict(
                Step=dict(
                    Status=dict(
                        State='CANCELLED',
                        StateChangeReason={},
                    ),
                ),
            ),
        ))

        self.start(patch(
            'tests.mock_boto3.emr.MockEMRClient.describe_cluster',
            return_value=dict(
                Cluster=dict(
                    Id='j-CLUSTERID',
                    Status=dict(
                        State='TERMINATING',
                        StateChangeReason={},
                    ),
                ),
            ),
        ))

        self.start(patch.object(
            runner, '_check_for_missing_default_iam_roles'))
        self.start(patch.object(
            runner, '_check_for_failed_bootstrap_action',
            side_effect=self.StopTest))

        self.assertRaises(self.StopTest,
                          runner._wait_for_steps_to_complete)

        self.assertTrue(runner._check_for_missing_default_iam_roles.called)
        self.assertTrue(runner._check_for_failed_bootstrap_action.called)


class LsBootstrapStderrLogsTestCase(MockBoto3TestCase):

    def setUp(self):
        super(LsBootstrapStderrLogsTestCase, self).setUp()

        self.runner = EMRJobRunner()
        self.log = self.start(patch('mrjob.emr.log'))

        self._ls_emr_bootstrap_stderr_logs = self.start(
            patch('mrjob.emr._ls_emr_bootstrap_stderr_logs'))
        self.runner._stream_bootstrap_log_dirs = Mock()

    def test_basic(self):
        # _ls_bootstrap_stderr_logs() is a very thin wrapper. Just
        # verify that the keyword args get passed through and
        # that logging happens in the right order

        stderr_path = ('s3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/i-e647eb49/'
                       'bootstrap-actions/1/stderr.gz')

        self._ls_emr_bootstrap_stderr_logs.return_value = [
            dict(
                action_num=0,
                node_id='i-e647eb49',
                path=stderr_path,
            ),
        ]

        results = self.runner._ls_bootstrap_stderr_logs(
            action_num=0,
            node_id='i-e647eb49',
        )

        self.assertFalse(self.log.info.called)

        self.assertEqual(next(results), dict(
            action_num=0,
            node_id='i-e647eb49',
            path=stderr_path,
        ))
        self._ls_emr_bootstrap_stderr_logs.assert_called_once_with(
            self.runner.fs,
            self.runner._stream_bootstrap_log_dirs.return_value,
            action_num=0,
            node_id='i-e647eb49',
        )

        self.assertEqual(self.log.info.call_count, 1)
        self.assertIn(stderr_path, self.log.info.call_args[0][0])

        self.assertRaises(StopIteration, next, results)


class CheckForFailedBootstrapActionTestCase(MockBoto3TestCase):

    def setUp(self):
        super(CheckForFailedBootstrapActionTestCase, self).setUp()

        self.runner = EMRJobRunner()

        self.start(patch('mrjob.emr._get_reason'))
        self._check_for_nonzero_return_code = self.start(
            patch('mrjob.emr._check_for_nonzero_return_code',
                  return_value=None))
        self.log = self.start(patch('mrjob.emr.log'))
        self._ls_bootstrap_stderr_logs = self.start(
            patch('mrjob.emr.EMRJobRunner._ls_bootstrap_stderr_logs'))
        self._interpret_emr_bootstrap_stderr = self.start(
            patch('mrjob.emr._interpret_emr_bootstrap_stderr',
                  return_value={}))

    def test_failed_for_wrong_reason(self):
        self.runner._check_for_failed_bootstrap_action(cluster=Mock())

        self.assertFalse(self._interpret_emr_bootstrap_stderr.called)
        self.assertFalse(self.log.error.called)

    def test_empty_interpretation(self):
        self._check_for_nonzero_return_code.return_value = dict(
            action_num=0, node_id='i-e647eb49')

        self.runner._check_for_failed_bootstrap_action(cluster=Mock())

        self._ls_bootstrap_stderr_logs.assert_called_once_with(
            action_num=0, node_id='i-e647eb49')
        self.assertTrue(self._interpret_emr_bootstrap_stderr.called)

        self.assertFalse(self.log.error.called)

    def test_error(self):
        self._check_for_nonzero_return_code.return_value = dict(
            action_num=0, node_id='i-e647eb49')

        stderr_path = ('s3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/'
                       'i-e647eb49/bootstrap-actions/1/stderr.gz')

        self._interpret_emr_bootstrap_stderr.return_value = dict(
            errors=[
                dict(
                    action_num=0,
                    node_id='i-e647eb49',
                    task_error=dict(
                        message='BOOM!\n',
                        path=stderr_path,
                    ),
                ),
            ],
            partial=True,
        )

        self.runner._check_for_failed_bootstrap_action(cluster=Mock())

        self._ls_bootstrap_stderr_logs.assert_called_once_with(
            action_num=0, node_id='i-e647eb49')
        self.assertTrue(self._interpret_emr_bootstrap_stderr.called)

        self.assertTrue(self.log.error.called)
        self.assertIn('BOOM!', self.log.error.call_args[0][0])
        self.assertIn(stderr_path, self.log.error.call_args[0][0])


class UseSudoOverSshTestCase(MockBoto3TestCase):

    def test_ami_4_3_0_with_ssh_fs(self):
        job = MRTwoStepJob(
            ['-r', 'emr', '--ec2-key-pair-file', '/path/to/EMR.pem',
             '--image-version', '4.3.0']).sandbox()

        with job.make_runner() as runner:
            self.assertIsNotNone(runner._ssh_fs)
            self.assertFalse(runner._ssh_fs._sudo)

            runner._launch()

            self.assertTrue(runner._ssh_fs._sudo)

    def test_ami_4_2_0_with_ssh_fs(self):

        job = MRTwoStepJob(
            ['-r', 'emr', '--ec2-key-pair-file', '/path/to/EMR.pem',
             '--image-version', '4.2.0']).sandbox()

        with job.make_runner() as runner:
            self.assertIsNotNone(runner._ssh_fs)
            self.assertFalse(runner._ssh_fs._sudo)

            runner._launch()

            self.assertFalse(runner._ssh_fs._sudo)

    def test_ami_4_3_0_without_ssh_fs(self):
        # just make sure we don't cause an error trying to set up sudo
        # on a nonexistent filesystem
        job = MRTwoStepJob(
            ['-r', 'emr', '--image-version', '4.3.0']).sandbox()

        with job.make_runner() as runner:
            self.assertIsNone(runner._ssh_fs)

            runner._launch()

            self.assertIsNone(runner._ssh_fs)


class MasterPrivateIPTestCase(MockBoto3TestCase):

    # logic for runner._master_private_ip()

    def test_master_private_ip(self):
        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            # no cluster yet
            self.assertRaises(AssertionError, runner._master_private_ip)

            runner._launch()

            self.assertIsNone(runner._master_private_ip())

            self.simulate_emr_progress(runner.get_cluster_id())
            self.assertIsNotNone(runner._master_private_ip())


class SetUpSSHTunnelTestCase(MockBoto3TestCase):

    def setUp(self, *args):
        super(SetUpSSHTunnelTestCase, self).setUp()

        self.mock_Popen = self.start(patch('mrjob.emr.Popen'))
        # simulate successfully binding port
        self.mock_Popen.return_value.returncode = None
        self.mock_Popen.return_value.pid = 99999

        self.start(patch('os.kill'))  # don't clean up fake SSH proc

    def get_ssh_args(self, *args, **kwargs):
        job_args = [
            '-r', 'emr',
            '--ssh-tunnel',
            '--ec2-key-pair', 'EMR',
            '--ec2-key-pair-file', '/path/to/EMR.pem'] + list(args)

        job = MRTwoStepJob(job_args)
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()

            cluster_id = runner.get_cluster_id()

            cluster = self.mock_emr_clusters[cluster_id]
            while cluster['Status']['State'] in ('STARTING', 'BOOTSTRAPPING'):
                self.simulate_emr_progress(cluster_id)

            runner._set_up_ssh_tunnel()

            ssh_args = self.mock_Popen.call_args[0][0]

            if kwargs.get('assert_tunnel_failed'):
                self.assertFalse(runner._ssh_proc)

            return ssh_args

    def parse_ssh_args(self, ssh_args):
        local_port, remote_host, remote_port = (
            ssh_args[ssh_args.index('-L') + 1].split(':'))

        local_port = int(local_port)
        remote_port = int(remote_port)

        user, host = ssh_args[-1].split('@')

        return dict(
            host=host,
            local_port=local_port,
            remote_host=remote_host,
            remote_port=remote_port,
            user=user)

    def test_basic(self):
        # test things that don't depend on AMIs
        ssh_args = self.get_ssh_args()
        params = self.parse_ssh_args(ssh_args)

        self.assertEqual(params['user'], 'hadoop')
        self.assertNotEqual(params['host'], params['remote_host'])

        self.assertEqual(self.mock_Popen.call_count, 1)

        self.assertNotIn('-g', ssh_args)
        self.assertNotIn('-4', ssh_args)

    def test_2_x_ami(self):
        ssh_args = self.get_ssh_args('--image-version', '2.4.11')
        params = self.parse_ssh_args(ssh_args)

        self.assertEqual(params['remote_port'], 9100)
        self.assertEqual(params['remote_host'], 'localhost')

    def test_3_x_ami(self):
        ssh_args = self.get_ssh_args('--image-version', '3.11.0')
        params = self.parse_ssh_args(ssh_args)

        self.assertEqual(params['remote_port'], 9026)
        self.assertEqual(len(params['remote_host'].split('.')), 4)

    def test_4_x_ami(self):
        ssh_args = self.get_ssh_args('--image-version', '4.7.2')
        params = self.parse_ssh_args(ssh_args)

        self.assertEqual(params['remote_port'], 8088)
        self.assertEqual(len(params['remote_host'].split('.')), 4)

    def test_ssh_tunnel_is_open(self):
        # this is the same on all AMIs
        ssh_args = self.get_ssh_args('--ssh-tunnel-is-open')

        self.assertIn('-g', ssh_args)
        self.assertIn('-4', ssh_args)

    def test_ssh_bind_ports(self):
        ssh_args = self.get_ssh_args('--ssh-bind-ports', '12345')
        params = self.parse_ssh_args(ssh_args)

        self.assertEqual(params['local_port'], 12345)

    def test_retry_ports(self):
        returncodes = [None, 255, 255]  # in reverse order

        def popen_side_effect(*args, **kwargs):
            return_value = Mock()
            return_value.pid = 99999
            return_value.returncode = returncodes.pop()
            return return_value

        self.mock_Popen.side_effect = popen_side_effect

        self.start(patch('mrjob.emr.EMRJobRunner._pick_ssh_bind_ports',
                   return_value=[10001, 10002, 10003, 10004]))

        ssh_args = self.get_ssh_args()
        params = self.parse_ssh_args(ssh_args)

        self.assertEqual(self.mock_Popen.call_count, 3)
        self.assertEqual(params['local_port'], 10003)

    def test_missing_ssh_binary(self):
        # tests #1474
        self.mock_Popen.side_effect = OSError(2, 'No such file or directory')

        self.get_ssh_args(assert_tunnel_failed=True)

    def test_other_popen_error(self):
        self.mock_Popen.side_effect = NotImplementedError()

        self.assertRaises(NotImplementedError, self.get_ssh_args)


class UsesSparkTestCase(MockBoto3TestCase):

    def test_default(self):
        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertFalse(runner._uses_spark())
            self.assertFalse(runner._has_spark_steps())
            self.assertFalse(runner._has_spark_install_bootstrap_action())
            self.assertFalse(runner._has_spark_application())
            self.assertFalse(runner._opts['bootstrap_spark'])

    def test_spark_step(self):
        job = MRNullSpark(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertTrue(runner._uses_spark())
            self.assertTrue(runner._has_spark_steps())

    def test_spark_script_step(self):
        job = MRSparkScript(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertTrue(runner._uses_spark())
            self.assertTrue(runner._has_spark_steps())

    def test_streaming_and_spark_steps(self):
        job = MRStreamingAndSpark(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertTrue(runner._uses_spark())
            self.assertTrue(runner._has_spark_steps())

    def test_s3_spark_install_bootstrap_action(self):
        job = MRTwoStepJob([
            '-r', 'emr',
            '--bootstrap-action',
            's3://support.elasticmapreduce/spark/install-spark',
        ])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertTrue(runner._uses_spark())
            self.assertTrue(runner._has_spark_install_bootstrap_action())

    def test_file_spark_install_bootstrap_action(self):
        job = MRTwoStepJob([
            '-r', 'emr',
            '--bootstrap-action',
            'file:///usr/share/aws/emr/install-spark/install-spark',
        ])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertTrue(runner._uses_spark())
            self.assertTrue(runner._has_spark_install_bootstrap_action())

    def test_s3_ganglia_install_bootstrap_action(self):
        job = MRTwoStepJob([
            '-r', 'emr',
            '--bootstrap-action',
            's3://beta.elasticmapreduce/bootstrap-actions/install-ganglia',
        ])

        job.sandbox()

        with job.make_runner() as runner:
            self.assertFalse(runner._uses_spark())
            self.assertFalse(runner._has_spark_install_bootstrap_action())

    def test_s3_ganglia_and_spark_bootstrap_actions(self):
        job = MRTwoStepJob([
            '-r', 'emr',
            '--bootstrap-action',
            's3://support.elasticmapreduce/spark/install-spark',
            's3://beta.elasticmapreduce/bootstrap-actions/install-ganglia',
        ])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertTrue(runner._uses_spark())
            self.assertTrue(runner._has_spark_install_bootstrap_action())

    def test_spark_application(self):
        job = MRTwoStepJob(['-r', 'emr',
                            '--image-version', '4.0.0',
                            '--application', 'Spark'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertTrue(runner._uses_spark())
            self.assertTrue(runner._has_spark_application())

    def test_spark_application_lowercase(self):
        job = MRTwoStepJob(['-r', 'emr',
                            '--image-version', '4.0.0',
                            '--application', 'spark'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertTrue(runner._uses_spark())
            self.assertTrue(runner._has_spark_application())

    def test_other_application(self):
        job = MRTwoStepJob(['-r', 'emr',
                            '--image-version', '4.0.0',
                            '--application', 'Mahout'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertFalse(runner._uses_spark())
            self.assertFalse(runner._has_spark_application())

    def test_spark_and_other_application(self):
        job = MRTwoStepJob(['-r', 'emr',
                            '--image-version', '4.0.0',
                            '--application', 'Mahout',
                            '--application', 'Spark'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertTrue(runner._uses_spark())
            self.assertTrue(runner._has_spark_application())

    def test_bootstrap_spark(self):
        # tests #1465
        job = MRTwoStepJob(['-r', 'emr', '--bootstrap-spark'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertTrue(runner._uses_spark())
            self.assertTrue(runner._opts['bootstrap_spark'])

    def test_ignores_new_supported_products_api_param(self):
        job = MRTwoStepJob(['-r', 'emr',
                            '--emr-api-param',
                            'NewSupportedProducts.member.1.Name=spark'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertFalse(runner._uses_spark())
            self.assertFalse(runner._has_spark_application())

    def test_ignores_application_api_param(self):
        job = MRTwoStepJob(['-r', 'emr',
                            '--image-version', '4.0.0',
                            '--emr-api-param',
                            'Application.member.1.Name=Spark'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertFalse(runner._uses_spark())
            self.assertFalse(runner._has_spark_application())


class SparkPyFilesTestCase(MockBoto3TestCase):

    def test_eggs(self):
        egg1_path = self.makefile('dragon.egg')
        egg2_path = self.makefile('horton.egg')

        job = MRNullSpark([
            '-r', 'emr',
            '--py-file', egg1_path, '--py-file', egg2_path])
        job.sandbox()

        with job.make_runner() as runner:
            runner._add_job_files_for_upload()

            # in the cloud, we need to upload py_files to cloud storage
            self.assertIn(egg1_path, runner._upload_mgr.path_to_uri())
            self.assertIn(egg2_path, runner._upload_mgr.path_to_uri())

            self.assertEqual(
                runner._spark_py_files(),
                [runner._upload_mgr.uri(egg1_path),
                 runner._upload_mgr.uri(egg2_path)]
            )


class TestClusterSparkSupportWarning(MockBoto3TestCase):

    def test_okay(self):
        job = MRNullSpark(['-r', 'emr', '--image-version', '4.0.0'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()

            message = runner._cluster_spark_support_warning()
            self.assertIsNone(message)

    def test_no_python_3(self):
        job = MRNullSpark(['-r', 'emr', '--image-version', '3.11.0'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()

            message = runner._cluster_spark_support_warning()
            self.assertIsNotNone(message)
            # this version of Spark is bad for different reasons, depending
            # on Python version
            if PY2:
                self.assertIn('old version of Spark', message)
            else:
                self.assertIn('Python 3', message)
            self.assertIn('4.0.0', message)

    def test_too_old(self):
        job = MRNullSpark(['-r', 'emr', '--image-version', '3.7.0'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()

            message = runner._cluster_spark_support_warning()
            self.assertIsNotNone(message)
            self.assertIn('support Spark', message)
            self.assertNotIn('Python 3', message)
            # should suggest an AMI that works with this version of Python
            if PY2:
                self.assertIn('3.8.0', message)
            else:
                self.assertIn('4.0.0', message)

    def test_master_instance_too_small(self):
        job = MRNullSpark(['-r', 'emr', '--image-version', '4.0.0',
                           '--num-core-instances', '2',
                           '--core-instance-type', 'm1.medium'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()

            message = runner._cluster_spark_support_warning()
            self.assertIsNotNone(message)
            self.assertIn('too small', message)
            self.assertIn('stall', message)

    def test_core_instances_too_small(self):
        job = MRNullSpark(['-r', 'emr', '--image-version', '4.0.0',
                           '--num-core-instances', '2',
                           '--core-instance-type', 'm1.medium'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()

            message = runner._cluster_spark_support_warning()
            self.assertIsNotNone(message)
            self.assertIn('too small', message)
            self.assertIn('stall', message)

    def test_task_instances_too_small(self):
        job = MRNullSpark(['-r', 'emr', '--image-version', '4.0.0',
                           '--num-core-instances', '2',
                           '--core-instance-type', 'm1.large',
                           '--num-task-instances', '2',
                           '--task-instance-type', 'm1.medium'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()

            message = runner._cluster_spark_support_warning()
            self.assertIsNotNone(message)
            self.assertIn('too small', message)
            self.assertIn('stall', message)

    def test_sole_master_instance_too_small(self):
        job = MRNullSpark(['-r', 'emr', '--image-version', '4.0.0',
                           '--master-instance-type', 'm1.medium'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()

            message = runner._cluster_spark_support_warning()
            self.assertIsNotNone(message)
            self.assertIn('too small', message)
            self.assertIn('stall', message)

    def test_okay_with_core_instances_and_small_master(self):
        job = MRNullSpark(['-r', 'emr', '--image-version', '4.0.0',
                           '--num-core-instances', '2',
                           '--master-instance-type', 'm1.medium'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()

            message = runner._cluster_spark_support_warning()
            self.assertIsNone(message)

    def test_instance_fleet_okay(self):
        instance_fleets = [dict(
            InstanceFleetType='MASTER',
            InstanceTypeConfigs=[dict(InstanceType='m1.large')],
            TargetOnDemandCapacity=1)]

        job = MRNullSpark(
            ['-r', 'emr', '--instance-fleets', json.dumps(instance_fleets)])
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()

            message = runner._cluster_spark_support_warning()
            self.assertIsNone(message)

    def test_instance_fleet_sole_master_too_small(self):
        instance_fleets = [dict(
            InstanceFleetType='MASTER',
            InstanceTypeConfigs=[dict(InstanceType='m1.medium')],
            TargetOnDemandCapacity=1)]

        job = MRNullSpark(
            ['-r', 'emr', '--instance-fleets', json.dumps(instance_fleets)])
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()

            message = runner._cluster_spark_support_warning()
            self.assertIsNotNone(message)
            self.assertIn('too small', message)
            self.assertIn('stall', message)

    def test_instance_fleet_core_instances_too_small(self):
        instance_fleets = [
            dict(
                InstanceFleetType='MASTER',
                InstanceTypeConfigs=[dict(InstanceType='m1.large')],
                TargetOnDemandCapacity=1,
            ),
            dict(
                InstanceFleetType='CORE',
                InstanceTypeConfigs=[dict(InstanceType='m1.medium')],
                TargetOnDemandCapacity=1,
            ),
        ]

        job = MRNullSpark(
            ['-r', 'emr', '--instance-fleets', json.dumps(instance_fleets)])
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()

            message = runner._cluster_spark_support_warning()
            self.assertIsNotNone(message)
            self.assertIn('too small', message)
            self.assertIn('stall', message)

    def test_instance_fleet_some_instances_too_small(self):
        instance_fleets = [dict(
            InstanceFleetType='MASTER',
            InstanceTypeConfigs=[
                dict(InstanceType='m1.medium'),
                dict(InstanceType='m1.large'),
                dict(InstanceType='c1.xlarge'),
            ],
            TargetOnDemandCapacity=1)]

        job = MRNullSpark(
            ['-r', 'emr', '--instance-fleets', json.dumps(instance_fleets)])
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()

            message = runner._cluster_spark_support_warning()
            self.assertIsNotNone(message)
            self.assertIn('too small', message)
            self.assertIn('stall', message)

    def test_instance_fleet_okay_with_core_instances_and_small_master(self):
        instance_fleets = [
            dict(
                InstanceFleetType='MASTER',
                InstanceTypeConfigs=[dict(InstanceType='m1.medium')],
                TargetOnDemandCapacity=1,
            ),
            dict(
                InstanceFleetType='CORE',
                InstanceTypeConfigs=[dict(InstanceType='m1.large')],
                TargetOnDemandCapacity=1,
            ),
        ]

        job = MRNullSpark(
            ['-r', 'emr', '--instance-fleets', json.dumps(instance_fleets)])
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()

            message = runner._cluster_spark_support_warning()
            self.assertIsNone(message)


class ImageVersionGteTestCase(MockBoto3TestCase):

    def test_image_version(self):
        runner = EMRJobRunner(image_version='3.7.0')

        self.assertTrue(runner._image_version_gte('3'))
        self.assertTrue(runner._image_version_gte('3.7.0'))
        self.assertFalse(runner._image_version_gte('3.8.0'))

    def test_release_label(self):
        runner = EMRJobRunner(release_label='emr-4.8.2')

        self.assertTrue(runner._image_version_gte('4'))
        self.assertTrue(runner._image_version_gte('4.6.0'))
        self.assertTrue(runner._image_version_gte('4.8.2'))
        self.assertFalse(runner._image_version_gte('4.9.0'))
        self.assertFalse(runner._image_version_gte('5'))

    def test_release_label_hides_image_version(self):
        runner = EMRJobRunner(image_version='3.7.0',
                              release_label='emr-4.8.2')

        self.assertTrue(runner._image_version_gte('4'))
        self.assertTrue(runner._image_version_gte('4.6.0'))
        self.assertTrue(runner._image_version_gte('4.8.2'))
        self.assertFalse(runner._image_version_gte('5'))


class SparkSubmitArgPrefixTestCase(MockBoto3TestCase):

    def test_default(self):
        # these are hard-coded and always the same
        runner = EMRJobRunner()

        self.assertEqual(
            runner._spark_submit_arg_prefix(),
            ['--master', 'yarn', '--deploy-mode', 'cluster'])


class SSHWorkerHostsTestCase(MockBoto3TestCase):

    def _ssh_worker_hosts(self, *args):
        mr_job = MRTwoStepJob(['-r', 'emr'] + list(args))
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.run()

            return runner._ssh_worker_hosts()

    def test_ignore_master(self):
        self.assertEqual(len(self._ssh_worker_hosts()), 0)

    def test_include_core_nodes(self):
        self.assertEqual(
            len(self._ssh_worker_hosts('--num-core-instances', '2')),
            2)

    def test_include_task_nodes(self):
        self.assertEqual(
            len(self._ssh_worker_hosts('--num-core-instances', '2',
                                       '--num-task-instances', '3')),
            5)


class BadBashWorkaroundTestCase(MockBoto3TestCase):
    # regression test for 1548

    def _test_sh_bin(self, image_version, expected_bin, expected_pre_commands):
        cookie_jar = self.makefile('cookie.jar')

        job = MRTwoStepJob(['-r', 'emr', '--image-version', image_version,
                            '--libjar', cookie_jar])
        job.sandbox()

        def check_script(path):
            self.assertTrue(path)
            with open(path) as script:
                lines = list(script)
                self.assertEqual(lines[0].strip(),
                                 '#!%s' % cmd_line(expected_bin))
                # everything up to first newline is a pre-command
                pre_commands = []
                for line in lines[1:]:
                    cmd = line.strip()
                    if not cmd:
                        break
                    pre_commands.append(cmd)

                self.assertEqual(pre_commands, expected_pre_commands)

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(runner._sh_bin(), expected_bin)
            self.assertEqual(runner._sh_pre_commands(), expected_pre_commands)

            check_script(runner._master_bootstrap_script_path)
            check_script(runner._master_node_setup_script_path)

    def test_good_bash(self):
        self._test_sh_bin('5.0.0', ['/bin/sh', '-ex'], [])

    def test_bad_bash(self):
        self._test_sh_bin('5.2.0', ['/bin/sh', '-x'], ['set -e'])


class LogProgressTestCase(MockBoto3TestCase):

    def setUp(self):
        super(LogProgressTestCase, self).setUp()

        self._progress_html_from_tunnel = self.start(patch(
            'mrjob.emr.EMRJobRunner._progress_html_from_tunnel'))
        self._progress_html_over_ssh = self.start(patch(
            'mrjob.emr.EMRJobRunner._progress_html_over_ssh'))

        self._parse_progress_from_job_tracker = self.start(patch(
            'mrjob.emr._parse_progress_from_job_tracker',
            return_value=(100, 50)))

        self._parse_progress_from_resource_manager = self.start(patch(
            'mrjob.emr._parse_progress_from_resource_manager',
            return_value=61.3))

        self.log = self.start(patch('mrjob.emr.log'))

        # don't clean up our mock cluster; this causes unwanted logging
        self.start(patch('mrjob.emr.EMRJobRunner.cleanup'))

    def _launch_and_log_progress(self, *args):
        job = MRTwoStepJob(['-r', 'emr'] + list(args))
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()
            self.log.info.reset_mock()

            runner._log_step_progress()

    def test_default(self):
        # by default, we fetch progress from the resource manager, through
        # the SSH tunnel
        self._launch_and_log_progress()

        self.log.info.assert_called_once_with('    61.3% complete')

        self.assertTrue(self._progress_html_from_tunnel.called)
        self.assertFalse(self._progress_html_over_ssh.called)

        self.assertFalse(self._parse_progress_from_job_tracker.called)
        self._parse_progress_from_resource_manager.assert_called_once_with(
            self._progress_html_from_tunnel.return_value)

    def test_fallback_to_direct_ssh(self):
        self._progress_html_from_tunnel.return_value = None

        self._launch_and_log_progress()

        self.log.info.assert_called_once_with('    61.3% complete')

        self.assertTrue(self._progress_html_from_tunnel.called)
        self.assertTrue(self._progress_html_over_ssh.called)

        self.assertFalse(self._parse_progress_from_job_tracker.called)
        self._parse_progress_from_resource_manager.assert_called_once_with(
            self._progress_html_over_ssh.return_value)

    def test_no_progress_available(self):
        self._progress_html_from_tunnel.return_value = None
        self._progress_html_over_ssh.return_value = None

        self._launch_and_log_progress()

        self.assertFalse(self.log.info.called)

        self.assertTrue(self._progress_html_from_tunnel.called)
        self.assertTrue(self._progress_html_over_ssh.called)

        self.assertFalse(self._parse_progress_from_job_tracker.called)
        self.assertFalse(self._parse_progress_from_resource_manager.called)

    def test_use_job_tracker_on_2_x_amis(self):
        # by default, we fetch progress from the resource manager, through
        # the SSH tunnel
        self._launch_and_log_progress('--image-version', '2.4.9')

        self.log.info.assert_called_once_with('   map 100% reduce  50%')

        self.assertTrue(self._progress_html_from_tunnel.called)
        self.assertFalse(self._progress_html_over_ssh.called)

        self._parse_progress_from_job_tracker.assert_called_once_with(
            self._progress_html_from_tunnel.return_value)
        self.assertFalse(self._parse_progress_from_resource_manager.called)


class ProgressHtmlFromTunnelTestCase(MockBoto3TestCase):

    MOCK_TUNNEL_URL = 'http://foohost:12345/cluster'

    def setUp(self):
        super(ProgressHtmlFromTunnelTestCase, self).setUp()

        self.urlopen = self.start(patch('mrjob.emr.urlopen'))

        self.log = self.start(patch('mrjob.emr.log'))

        # don't clean up our mock cluster; this causes unwanted logging
        self.start(patch('mrjob.emr.EMRJobRunner.cleanup'))

    def _launch_and_get_progress_html(self, ssh_tunnel=True):
        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()
            if ssh_tunnel:
                runner._ssh_tunnel_url = self.MOCK_TUNNEL_URL

            self.log.debug.reset_mock()

            return runner._progress_html_from_tunnel()

    def test_no_tunnel(self):
        self.assertIsNone(self._launch_and_get_progress_html(ssh_tunnel=False))

        self.assertFalse(self.urlopen.called)

    def test_tunnel(self):
        html = self._launch_and_get_progress_html()

        self.urlopen.assert_called_once_with(self.MOCK_TUNNEL_URL)
        self.assertTrue(self.urlopen.return_value.read.called)

        self.assertEqual(html, self.urlopen.return_value.read.return_value)

        self.assertTrue(self.urlopen.return_value.close.called)

    def test_urlopen_exception(self):
        self.urlopen.side_effect = Exception('BOOM')

        self.assertIsNone(self._launch_and_get_progress_html())

        self.urlopen.assert_called_once_with(self.MOCK_TUNNEL_URL)
        self.assertFalse(self.urlopen.return_value.read.called)

        self.assertFalse(self.urlopen.return_value.close.called)

    def test_urlopen_read_exception(self):
        self.urlopen.return_value.read.side_effect = Exception('BOOM')

        self.assertIsNone(self._launch_and_get_progress_html())

        self.urlopen.assert_called_once_with(self.MOCK_TUNNEL_URL)
        self.assertTrue(self.urlopen.return_value.read.called)

        self.assertTrue(self.urlopen.return_value.close.called)


class ProgressHtmlOverSshTestCase(MockBoto3TestCase):

    MOCK_MASTER = 'mockmaster'
    MOCK_JOB_TRACKER_URL = 'http://1.2.3.4:8088/cluster'

    MOCK_EC2_KEY_PAIR_FILE = 'mock.pem'

    def setUp(self):
        super(ProgressHtmlOverSshTestCase, self).setUp()

        self._ssh_run = self.start(patch('mrjob.fs.ssh.SSHFilesystem._ssh_run',
                                         return_value=(Mock(), Mock())))

        self._address_of_master = self.start(patch(
            'mrjob.emr.EMRJobRunner._address_of_master',
            return_value=self.MOCK_MASTER))

        self._job_tracker_url = self.start(patch(
            'mrjob.emr.EMRJobRunner._job_tracker_url',
            return_value=self.MOCK_JOB_TRACKER_URL))

    def _launch_and_get_progress_html(self, *args):
        job = MRTwoStepJob(
            ['-r', 'emr', '--ec2-key-pair-file', self.MOCK_EC2_KEY_PAIR_FILE] +
            list(args))
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()
            return runner._progress_html_over_ssh()

    def test_default(self):
        html = self._launch_and_get_progress_html()

        self.assertIsNotNone(html)
        self._ssh_run.assert_called_once_with(
            self.MOCK_MASTER,
            ['curl', self.MOCK_JOB_TRACKER_URL])

        self.assertEqual(html, self._ssh_run.return_value[0])

    def test_no_master_node(self):
        self._address_of_master.return_value = None

        self.assertIsNone(self._launch_and_get_progress_html())

        self.assertFalse(self._ssh_run.called)

    def test_no_ssh_bin(self):
        self.assertIsNone(self._launch_and_get_progress_html('--ssh-bin', ''))

        self.assertFalse(self._ssh_run.called)

    def test_no_key_pair_file(self):
        self.assertIsNone(self._launch_and_get_progress_html(
            '--ec2-key-pair-file', ''))

        self.assertFalse(self._ssh_run.called)

    def test_ssh_run_exception(self):
        self._ssh_run.side_effect = Exception('BOOM')

        self.assertIsNone(self._launch_and_get_progress_html())

        self.assertTrue(self._ssh_run.called)


class EMRCredentialsObfuscationTestCase(MockBoto3TestCase):

    def get_debug_printout(self, **opts):
        stderr = StringIO()

        with no_handlers_for_logger():
            log_to_stream('mrjob.runner', stderr, debug=True)

            # debug printout happens in constructor
            EMRJobRunner(**opts)

        return stderr.getvalue()

    def test_non_obfuscated_option_on_emr(self):
        printout = self.get_debug_printout(owner='dave')

        self.assertIn("'owner'", printout)
        self.assertIn("'dave'", printout)

    def test_aws_access_key_id(self):
        printout = self.get_debug_printout(
            aws_access_key_id='AKIATOPQUALITYSALESEVENT')

        self.assertIn("'aws_access_key_id'", printout)
        self.assertIn("'...VENT'", printout)

    def test_aws_access_key_id_with_wrong_type(self):
        printout = self.get_debug_printout(
            aws_access_key_id=['AKIATOPQUALITYSALESEVENT'])

        self.assertIn("'aws_access_key_id'", printout)
        self.assertNotIn('VENT', printout)
        self.assertIn("'...'", printout)

    def test_aws_secret_access_key(self):
        printout = self.get_debug_printout(
            aws_secret_access_key='PASSWORD')

        self.assertIn("'aws_secret_access_key'", printout)
        self.assertNotIn('PASSWORD', printout)
        self.assertIn("'...'", printout)

    def test_aws_session_token(self):
        printout = self.get_debug_printout(aws_session_token='TOKEN')

        self.assertIn("'aws_session_token'", printout)
        self.assertNotIn('TOKEN', printout)
        self.assertIn("'...'", printout)

    def test_dont_obfuscate_empty_opts(self):
        printout = self.get_debug_printout()

        self.assertNotIn("'...'", printout)
        self.assertIn("'aws_access_key_id'", printout)
        self.assertIn("'aws_secret_access_key'", printout)
        self.assertIn("'aws_session_token'", printout)


class CheckInputPathsTestCase(MockBoto3TestCase):

    def setUp(self):
        super(CheckInputPathsTestCase, self).setUp()

        # stop at _run()
        self.start(patch('mrjob.emr.EMRJobRunner._run',
                         side_effect=StopIteration))

        # this assumes we really ran the job
        self.cleanup = self.start(patch('mrjob.emr.EMRJobRunner.cleanup'))

    def test_existing_s3_path(self):
        self.add_mock_s3_data({'walrus': {'data/foo': b'foo\n'}})

        job = MRTwoStepJob(['-r', 'emr', 's3://walrus/data/foo'])

        with job.make_runner() as runner:
            self.assertRaises(StopIteration, runner.run)

    def test_nonexistent_s3_path(self):
        job = MRTwoStepJob(['-r', 'emr', 's3://walrus/data/foo'])

        with job.make_runner() as runner:
            self.assertRaises(IOError, runner.run)

    def test_dont_check_input_path(self):
        job = MRTwoStepJob(['-r', 'emr', 's3://walrus/data/foo',
                            '--no-check-input-paths'])

        with job.make_runner() as runner:
            self.assertRaises(StopIteration, runner.run)

    def test_other_uri(self):
        job = MRTwoStepJob(['-r', 'emr', 'hdfs:///path/to/input/'])

        # no way to check hdfs://, ignore
        with job.make_runner() as runner:
            self.assertRaises(StopIteration, runner.run)


class CheckClusterEveryTestCase(MockBoto3TestCase):

    def test_command_line_option(self):
        # regression test for #1664
        job = MRTwoStepJob(['-r', 'emr', '--check-cluster-every', '5.0'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()
