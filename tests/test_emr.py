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
"""Tests for EMRJobRunner"""
import copy
import getpass
import json
import os
import os.path
import posixpath
import sys
import time
from datetime import datetime
from datetime import timedelta
from io import BytesIO
from unittest import TestCase
from unittest import skipIf

import mrjob
import mrjob.emr
from mrjob.compat import version_gte
from mrjob.emr import EMRJobRunner
from mrjob.emr import _3_X_SPARK_BOOTSTRAP_ACTION
from mrjob.emr import _3_X_SPARK_SUBMIT
from mrjob.emr import _4_X_COMMAND_RUNNER_JAR
from mrjob.emr import _DEFAULT_IMAGE_VERSION
from mrjob.emr import _MAX_HOURS_IDLE_BOOTSTRAP_ACTION_PATH
from mrjob.emr import _PRE_4_X_STREAMING_JAR
from mrjob.emr import _attempt_to_acquire_lock
from mrjob.emr import _decode_configurations_from_api
from mrjob.emr import _lock_acquire_step_1
from mrjob.emr import _lock_acquire_step_2
from mrjob.emr import _list_all_steps
from mrjob.emr import _yield_all_bootstrap_actions
from mrjob.emr import _yield_all_clusters
from mrjob.emr import _yield_all_instance_groups
from mrjob.emr import filechunkio
from mrjob.job import MRJob
from mrjob.parse import parse_s3_uri
from mrjob.pool import _pool_hash_and_name
from mrjob.py2 import PY2
from mrjob.py2 import StringIO
from mrjob.step import INPUT
from mrjob.step import OUTPUT
from mrjob.step import StepFailedException
from mrjob.tools.emr.audit_usage import _JOB_KEY_RE
from mrjob.util import bash_wrap
from mrjob.util import cmd_line
from mrjob.util import log_to_stream
from mrjob.util import tar_and_gzip

from tests.mockboto import DEFAULT_MAX_STEPS_RETURNED
from tests.mockboto import MockBotoTestCase
from tests.mockboto import MockEmrConnection
from tests.mockboto import MockEmrObject
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
from tests.sandbox import mrjob_conf_patcher
from tests.test_hadoop import HadoopExtraArgsTestCase

try:
    import boto
    import boto.emr
    import boto.emr.connection
    import boto.exception
    boto  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    boto = None

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

if boto:
    INSTANCE_FLEETS_ERROR = boto.exception.EmrResponseError(
        400, 'BadRequest',
        '<ErrorResponse xmlns="http://elasticmapreduce.amazonaws.com/doc/2009-03-31">\n  <Error>\n    <Type>Sender</Type>\n    <Code>InvalidRequestException</Code>\n    <Message>Instance fleets and instance groups are mutually exclusive. The EMR cluster specified in the request uses instance fleets. The ListInstanceGroups operation does not support clusters that use instance fleets. Use the ListInstanceFleets operation instead.</Message>\n  </Error>\n  <RequestId>68f8aaaf-8c3d-11e7-b4d2-c345838a0f11</RequestId>\n</ErrorResponse>\n')  # noqa


class EMRJobRunnerEndToEndTestCase(MockBotoTestCase):

    MRJOB_CONF_CONTENTS = {'runners': {'emr': {
        'check_cluster_every': 0.00,
        'cloud_fs_sync_secs': 0.00,
        'additional_emr_info': {'key': 'value'}
    }}}

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

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

            local_tmp_dir = runner._get_local_tmp_dir()
            # make sure cleanup hasn't happened yet
            self.assertTrue(os.path.exists(local_tmp_dir))
            self.assertTrue(any(runner.fs.ls(runner.get_output_dir())))

            cluster = runner._describe_cluster()

            name_match = _JOB_KEY_RE.match(cluster.name)
            self.assertEqual(name_match.group(1), 'mr_hadoop_format_job')
            self.assertEqual(name_match.group(2), getpass.getuser())

            # make sure our input and output formats are attached to
            # the correct steps
            emr_conn = runner.make_emr_conn()
            steps = _list_all_steps(emr_conn, runner.get_cluster_id())

            step_0_args = [a.value for a in steps[0].config.args]
            step_1_args = [a.value for a in steps[1].config.args]

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
        emr_conn = runner.make_emr_conn()
        cluster_id = runner.get_cluster_id()
        for _ in range(10):
            emr_conn.simulate_progress(cluster_id)

        cluster = runner._describe_cluster()
        self.assertEqual(cluster.status.state, 'TERMINATED')

        # did we wait for steps in correct order? (regression test for #1316)
        step_ids = [
            c[0][0] for c in runner._wait_for_step_to_complete.call_args_list]
        self.assertEqual(step_ids, [step.id for step in steps])

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

                emr_conn = runner.make_emr_conn()
                cluster_id = runner.get_cluster_id()
                for _ in range(10):
                    emr_conn.simulate_progress(cluster_id)

                cluster = runner._describe_cluster()
                self.assertEqual(cluster.status.state,
                                 'TERMINATED_WITH_ERRORS')

            # job should get terminated on cleanup
            cluster_id = runner.get_cluster_id()
            for _ in range(10):
                emr_conn.simulate_progress(cluster_id)

        cluster = runner._describe_cluster()
        self.assertEqual(cluster.status.state, 'TERMINATED_WITH_ERRORS')

    def _test_cloud_tmp_cleanup(self, mode, tmp_len, log_len):
        self.add_mock_s3_data({'walrus': {'logs/j-MOCKCLUSTER0/1': b'1\n'}})
        stdin = BytesIO(b'foo\nbar\n')

        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '--s3-log-uri', 's3://walrus/logs',
                               '-', '--cleanup', mode])
        mr_job.sandbox(stdin=stdin)

        with mr_job.make_runner() as runner:
            cloud_tmp_dir = runner._opts['cloud_tmp_dir']
            tmp_bucket, _ = parse_s3_uri(cloud_tmp_dir)

            runner.run()

            # this is set and unset before we can get at it unless we do this
            log_bucket, _ = parse_s3_uri(runner._s3_log_dir())

            list(runner.stream_output())

        conn = runner.fs.make_s3_conn()
        bucket = conn.get_bucket(tmp_bucket)
        self.assertEqual(len(list(bucket.list())), tmp_len)

        bucket = conn.get_bucket(log_bucket)
        self.assertEqual(len(list(bucket.list())), log_len)

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


class ExistingClusterTestCase(MockBotoTestCase):

    def test_attach_to_existing_cluster(self):
        emr_conn = EMRJobRunner(conf_paths=[]).make_emr_conn()
        # set log_uri to None, so that when we describe the cluster, it
        # won't have the loguri attribute, to test Issue #112
        cluster_id = emr_conn.run_jobflow(
            name='Development Cluster', log_uri=None,
            keep_alive=True, job_flow_role='fake-instance-profile',
            service_role='fake-service-role')

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

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

        self.assertEqual(sorted(results),
                         [(1, 'bar'), (1, 'foo'), (2, None)])

    def test_dont_take_down_cluster_on_failure(self):
        emr_conn = EMRJobRunner(conf_paths=[]).make_emr_conn()
        # set log_uri to None, so that when we describe the cluster, it
        # won't have the loguri attribute, to test Issue #112
        cluster_id = emr_conn.run_jobflow(
            name='Development Cluster', log_uri=None,
            keep_alive=True, job_flow_role='fake-instance-profile',
            service_role='fake-service-role')

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

            emr_conn = runner.make_emr_conn()
            cluster_id = runner.get_cluster_id()
            for _ in range(10):
                emr_conn.simulate_progress(cluster_id)

            cluster = runner._describe_cluster()
            self.assertEqual(cluster.status.state, 'WAITING')

        # job shouldn't get terminated by cleanup
        emr_conn = runner.make_emr_conn()
        cluster_id = runner.get_cluster_id()
        for _ in range(10):
            emr_conn.simulate_progress(cluster_id)

        cluster = runner._describe_cluster()
        self.assertEqual(cluster.status.state, 'WAITING')


class VisibleToAllUsersTestCase(MockBotoTestCase):

    def test_defaults(self):
        cluster = self.run_and_get_cluster()
        self.assertEqual(cluster.visibletoallusers, 'true')

    def test_no_visible(self):
        cluster = self.run_and_get_cluster('--no-visible-to-all-users')
        self.assertEqual(cluster.visibletoallusers, 'false')

    def test_force_to_bool(self):
        # make sure mockboto doesn't always convert to bool
        self.assertRaises(boto.exception.EmrResponseError,
                          self.run_and_get_cluster,
                          '--emr-api-param', 'VisibleToAllUsers=1')

    def test_visible(self):
        cluster = self.run_and_get_cluster('--visible-to-all-users')
        self.assertTrue(cluster.visibletoallusers, 'true')

        VISIBLE_MRJOB_CONF = {'runners': {'emr': {
            'check_cluster_every': 0.00,
            'cloud_fs_sync_secs': 0.00,
            'visible_to_all_users': 1,  # should be True
        }}}

        with mrjob_conf_patcher(VISIBLE_MRJOB_CONF):
            visible_cluster = self.run_and_get_cluster()
            self.assertEqual(visible_cluster.visibletoallusers, 'true')


class SubnetTestCase(MockBotoTestCase):

    def test_defaults(self):
        cluster = self.run_and_get_cluster()
        self.assertEqual(
            getattr(cluster.ec2instanceattributes, 'ec2subnetid', None),
            None)

    def test_subnet_option(self):
        cluster = self.run_and_get_cluster('--subnet', 'subnet-ffffffff')
        self.assertEqual(
            getattr(cluster.ec2instanceattributes, 'ec2subnetid', None),
            'subnet-ffffffff')

    def test_empty_string_means_no_subnet(self):
        cluster = self.run_and_get_cluster('--subnet', '')
        self.assertEqual(
            getattr(cluster.ec2instanceattributes, 'ec2subnetid', None),
            None)


class IAMTestCase(MockBotoTestCase):

    def setUp(self):
        super(IAMTestCase, self).setUp()

        # wrap connect_iam() so we can see if it was called
        p_iam = patch.object(boto, 'connect_iam', wraps=boto.connect_iam)
        self.addCleanup(p_iam.stop)
        p_iam.start()

    def test_role_auto_creation(self):
        cluster = self.run_and_get_cluster()
        self.assertTrue(boto.connect_iam.called)

        # check instance_profile
        instance_profile_name = (
            cluster.ec2instanceattributes.iaminstanceprofile)
        self.assertIsNotNone(instance_profile_name)
        self.assertTrue(instance_profile_name.startswith('mrjob-'))
        self.assertIn(instance_profile_name, self.mock_iam_instance_profiles)
        self.assertIn(instance_profile_name, self.mock_iam_roles)
        self.assertIn(instance_profile_name,
                      self.mock_iam_role_attached_policies)

        # check service_role
        service_role_name = cluster.servicerole
        self.assertIsNotNone(service_role_name)
        self.assertTrue(service_role_name.startswith('mrjob-'))
        self.assertIn(service_role_name, self.mock_iam_roles)
        self.assertIn(service_role_name,
                      self.mock_iam_role_attached_policies)

        # instance_profile and service_role should be distinct
        self.assertNotEqual(instance_profile_name, service_role_name)

        # run again, and see if we reuse the roles
        cluster2 = self.run_and_get_cluster()
        self.assertEqual(cluster2.ec2instanceattributes.iaminstanceprofile,
                         instance_profile_name)
        self.assertEqual(cluster2.servicerole, service_role_name)

    def test_iam_instance_profile_option(self):
        cluster = self.run_and_get_cluster(
            '--iam-instance-profile', 'EMR_EC2_DefaultRole')
        self.assertTrue(boto.connect_iam.called)

        self.assertEqual(cluster.ec2instanceattributes.iaminstanceprofile,
                         'EMR_EC2_DefaultRole')

    def test_iam_service_role_option(self):
        cluster = self.run_and_get_cluster(
            '--iam-service-role', 'EMR_DefaultRole')
        self.assertTrue(boto.connect_iam.called)

        self.assertEqual(cluster.servicerole, 'EMR_DefaultRole')

    def test_both_iam_options(self):
        cluster = self.run_and_get_cluster(
            '--iam-instance-profile', 'EMR_EC2_DefaultRole',
            '--iam-service-role', 'EMR_DefaultRole')

        # users with limited access may not be able to connect to the IAM API.
        # This gives them a plan B
        self.assertFalse(boto.connect_iam.called)

        self.assertEqual(cluster.ec2instanceattributes.iaminstanceprofile,
                         'EMR_EC2_DefaultRole')
        self.assertEqual(cluster.servicerole, 'EMR_DefaultRole')

    def test_no_iam_access(self):
        ex = boto.exception.BotoServerError(403, 'Forbidden')
        self.assertIsInstance(boto.connect_iam, Mock)
        boto.connect_iam.side_effect = ex

        with logger_disabled('mrjob.emr'):
            cluster = self.run_and_get_cluster()

        self.assertTrue(boto.connect_iam.called)

        self.assertEqual(cluster.ec2instanceattributes.iaminstanceprofile,
                         'EMR_EC2_DefaultRole')
        self.assertEqual(cluster.servicerole, 'EMR_DefaultRole')


class EMRAPIParamsTestCase(MockBotoTestCase):

    def test_param_set(self):
        cluster = self.run_and_get_cluster(
            '--emr-api-param', 'Test.API=a', '--emr-api-param', 'Test.API2=b')
        self.assertTrue('Test.API' in cluster._api_params)
        self.assertTrue('Test.API2' in cluster._api_params)
        self.assertEqual(cluster._api_params['Test.API'], 'a')
        self.assertEqual(cluster._api_params['Test.API2'], 'b')

    def test_param_unset(self):
        cluster = self.run_and_get_cluster(
            '--no-emr-api-param', 'Test.API',
            '--no-emr-api-param', 'Test.API2')
        self.assertTrue('Test.API' in cluster._api_params)
        self.assertTrue('Test.API2' in cluster._api_params)
        self.assertIsNone(cluster._api_params['Test.API'])
        self.assertIsNone(cluster._api_params['Test.API2'])

    def test_invalid_param(self):
        self.assertRaises(
            ValueError, self.run_and_get_cluster,
            '--emr-api-param', 'Test.API')

    def test_overrides(self):
        cluster = self.run_and_get_cluster(
            '--emr-api-param', 'VisibleToAllUsers=false',
            '--visible-to-all-users')
        self.assertEqual(cluster.visibletoallusers, 'false')

    def test_no_emr_api_param_command_line_switch(self):
        job = MRWordCount([
            '-r', 'emr',
            '--emr-api-param', 'Instances.Ec2SubnetId=someID',
            '--no-emr-api-param', 'VisibleToAllUsers'])

        with job.make_runner() as runner:
            self.assertEqual(runner._opts['emr_api_params'],
                             {'Instances.Ec2SubnetId': 'someID',
                              'VisibleToAllUsers': None})

    def test_no_emr_api_params_is_not_a_real_option(self):
        job = MRWordCount([
            '-r', 'emr',
            '--no-emr-api-param', 'VisibleToAllUsers'])

        self.assertNotIn('no_emr_api_params',
                         sorted(job.emr_job_runner_kwargs()))
        self.assertNotIn('no_emr_api_param',
                         sorted(job.emr_job_runner_kwargs()))

        with job.make_runner() as runner:
            self.assertNotIn('no_emr_api_params', sorted(runner._opts))
            self.assertNotIn('no_emr_api_param', sorted(runner._opts))
            self.assertEqual(runner._opts['emr_api_params'],
                             {'VisibleToAllUsers': None})

    def test_command_line_overrides_config(self):
        # want to make sure a nulled-out param in the config file
        # can't override a param set on the command line

        API_PARAMS_MRJOB_CONF = {'runners': {'emr': {
            'check_cluster_every': 0.00,
            'cloud_fs_sync_secs': 0.00,
            'emr_api_params': {
                'Instances.Ec2SubnetId': 'someID',
                'VisibleToAllUsers': None,
                'Name': 'eaten_by_a_whale',
            },
        }}}

        job = MRWordCount([
            '-r', 'emr',
            '--no-emr-api-param', 'Instances.Ec2SubnetId',
            '--emr-api-param', 'VisibleToAllUsers=true'])

        with mrjob_conf_patcher(API_PARAMS_MRJOB_CONF):
            with job.make_runner() as runner:
                self.assertEqual(runner._opts['emr_api_params'],
                                 {'Instances.Ec2SubnetId': None,
                                  'VisibleToAllUsers': 'true',
                                  'Name': 'eaten_by_a_whale'})

    def test_serialization(self):
        # we can now serialize data structures from mrjob.conf

        API_PARAMS_MRJOB_CONF = {'runners': {'emr': {
            'check_cluster_every': 0.00,
            'cloud_fs_sync_secs': 0.00,
            'emr_api_params': {
                'Foo': {'Bar': ['Baz', {'Qux': ['Quux', 'Quuux']}]}
            },
        }}}

        job = MRWordCount(['-r', 'emr'])
        job.sandbox()

        with mrjob_conf_patcher(API_PARAMS_MRJOB_CONF):
            with job.make_runner() as runner:
                runner._launch()

                api_params = runner._describe_cluster()._api_params
                self.assertEqual(
                    api_params.get('Foo.Bar.member.1'), 'Baz')
                self.assertEqual(
                    api_params.get('Foo.Bar.member.2.Qux.member.1'), 'Quux')
                self.assertEqual(
                    api_params.get('Foo.Bar.member.2.Qux.member.2'), 'Quuux')


class AMIAndHadoopVersionTestCase(MockBotoTestCase):

    def test_default(self):
        with self.make_runner() as runner:
            runner.run()
            self.assertEqual(runner.get_image_version(),
                             _DEFAULT_IMAGE_VERSION)
            self.assertEqual(runner.get_hadoop_version(), '2.7.3')

    def test_ami_version_1_0_no_longer_supported(self):
        with self.make_runner('--image-version', '1.0') as runner:
            self.assertRaises(boto.exception.EmrResponseError,
                              runner._launch)

    def test_ami_version_2_0(self):
        with self.make_runner('--image-version', '2.0') as runner:
            runner.run()
            self.assertEqual(runner.get_image_version(), '2.0.6')
            self.assertEqual(runner.get_hadoop_version(), '0.20.205')

    def test_latest_ami_version(self):
        # "latest" is no longer actually the latest version
        with self.make_runner('--image-version', 'latest') as runner:
            # we should translate "latest" ourselves (see #1269)
            self.assertEqual(runner._opts['image_version'], '2.4.2')
            runner.run()
            self.assertEqual(runner.get_image_version(), '2.4.2')
            self.assertEqual(runner.get_hadoop_version(), '1.0.3')

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
            self.assertEqual(getattr(cluster, 'releaselabel', ''),
                             'emr-4.0.0')
            self.assertEqual(getattr(cluster, 'requestedamiversion', ''), '')
            self.assertEqual(getattr(cluster, 'runningamiversion', ''), '')

    def test_ami_version_4_0_0_via_image_version_option(self):
        # mrjob should also be smart enough to handle this
        with self.make_runner('--image-version', '4.0.0') as runner:
            runner.run()
            self.assertEqual(runner.get_image_version(), '4.0.0')
            self.assertEqual(runner.get_hadoop_version(), '2.6.0')

            cluster = runner._describe_cluster()
            self.assertEqual(getattr(cluster, 'releaselabel', ''),
                             'emr-4.0.0')
            self.assertEqual(getattr(cluster, 'requestedamiversion', ''), '')
            self.assertEqual(getattr(cluster, 'runningamiversion', ''), '')

    def test_hadoop_version_option_does_nothing(self):
        with logger_disabled('mrjob.emr'):
            with self.make_runner('--hadoop-version', '1.2.3.4') as runner:
                runner.run()
                self.assertEqual(runner.get_image_version(),
                                 _DEFAULT_IMAGE_VERSION)
                self.assertEqual(runner.get_hadoop_version(), '2.7.3')


class AvailabilityZoneTestCase(MockBotoTestCase):

    MRJOB_CONF_CONTENTS = {'runners': {'emr': {
        'check_cluster_every': 0.00,
        'cloud_fs_sync_secs': 0.00,
        'zone': 'PUPPYLAND',
    }}}

    def test_availability_zone_config(self):
        with self.make_runner() as runner:
            runner.run()

            cluster = runner._describe_cluster()
            self.assertEqual(cluster.ec2instanceattributes.ec2availabilityzone,
                             'PUPPYLAND')


class EnableDebuggingTestCase(MockBotoTestCase):

    def test_debugging_works(self):
        with self.make_runner('--enable-emr-debugging') as runner:
            runner.run()

            emr_conn = runner.make_emr_conn()
            steps = _list_all_steps(emr_conn, runner.get_cluster_id())

            self.assertEqual(steps[0].name, 'Setup Hadoop Debugging')


class RegionTestCase(MockBotoTestCase):

    def test_default(self):
        runner = EMRJobRunner()
        self.assertEqual(runner._opts['region'], 'us-west-2')

    def test_explicit_region(self):
        runner = EMRJobRunner(region='us-east-1')
        self.assertEqual(runner._opts['region'], 'us-east-1')

    def test_cannot_be_empty(self):
        runner = EMRJobRunner(region='')
        self.assertEqual(runner._opts['region'], 'us-west-2')


class TmpBucketTestCase(MockBotoTestCase):

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


class EC2InstanceGroupTestCase(MockBotoTestCase):

    maxDiff = None

    def _test_instance_groups(self, opts, **expected):
        """Run a job with the given option dictionary, and check for
        for instance, number, and optional bid price for each instance role.

        Specify expected instance group info like:

        <role>=(num_instances, instance_type, bid_price)
        """
        runner = EMRJobRunner(**opts)
        cluster_id = runner.make_persistent_cluster()

        emr_conn = runner.make_emr_conn()
        instance_groups = list(
            _yield_all_instance_groups(emr_conn, cluster_id))

        # convert actual instance groups to dicts. (This gets around any
        # assumptions about the order the API returns instance groups in;
        # see #1316)
        role_to_actual = {}
        for ig in instance_groups:
            info = dict(
                (field, getattr(ig, field, None))
                for field in ('bidprice', 'instancetype',
                              'market', 'requestedinstancecount'))

            role_to_actual[ig.instancegrouptype] = info

        # convert expected to dicts
        role_to_expected = {}
        for role, (num, instance_type, bid_price) in expected.items():
            info = dict(
                bidprice=(bid_price if bid_price else None),
                instancetype=instance_type,
                market=(u'SPOT' if bid_price else u'ON_DEMAND'),
                requestedinstancecount=str(num),
            )

            role_to_expected[role.upper()] = info

        self.assertEqual(role_to_actual, role_to_expected)

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

        # set master and slave in mrjob.conf, 2 instances
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
            boto.exception.EmrResponseError,
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

    def test_deprecated_num_ec2_instances(self):
        self._test_instance_groups(
            {'num_ec2_instances': 3},
            core=(2, 'm1.medium', None),
            master=(1, 'm1.medium', None))

    def test_deprecated_num_ec2_instances_conflict_on_cmd_line(self):
        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.emr', stderr)
            self._test_instance_groups(
                {'num_ec2_instances': 4,
                 'num_core_instances': 10},
                core=(10, 'm1.medium', None),
                master=(1, 'm1.medium', None))

        self.assertIn('does not make sense', stderr.getvalue())

    def test_deprecated_num_ec2_instances_conflict_in_mrjob_conf(self):
        self.set_in_mrjob_conf(num_ec2_instances=3,
                               num_core_instances=5,
                               num_task_instances=9)

        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.emr', stderr)
            self._test_instance_groups(
                {},
                core=(5, 'm1.medium', None),
                master=(1, 'm1.medium', None),
                task=(9, 'm1.medium', None))

        self.assertIn('does not make sense', stderr.getvalue())

    def test_deprecated_num_ec2_instances_cmd_line_beats_mrjob_conf(self):
        self.set_in_mrjob_conf(num_core_instances=5,
                               num_task_instances=9)

        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.emr', stderr)
            self._test_instance_groups(
                {'num_ec2_instances': 3},
                core=(2, 'm1.medium', None),
                master=(1, 'm1.medium', None))

        self.assertNotIn('does not make sense', stderr.getvalue())


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


class TestEMREndpoints(MockBotoTestCase):

    def test_default_region(self):
        runner = EMRJobRunner(conf_paths=[])
        self.assertEqual(runner.make_emr_conn().host,
                         'elasticmapreduce.us-west-2.amazonaws.com')
        self.assertEqual(runner._opts['region'], 'us-west-2')

    def test_none_region(self):
        # blank region should be treated the same as no region
        runner = EMRJobRunner(conf_paths=[], region=None)
        self.assertEqual(runner.make_emr_conn().host,
                         'elasticmapreduce.us-west-2.amazonaws.com')
        self.assertEqual(runner._opts['region'], 'us-west-2')

    def test_blank_region(self):
        # blank region should be treated the same as no region
        runner = EMRJobRunner(conf_paths=[], region='')
        self.assertEqual(runner.make_emr_conn().host,
                         'elasticmapreduce.us-west-2.amazonaws.com')
        self.assertEqual(runner._opts['region'], 'us-west-2')

    def test_eu(self):
        runner = EMRJobRunner(conf_paths=[], region='EU')
        self.assertEqual(runner.make_emr_conn().host,
                         'elasticmapreduce.eu-west-1.amazonaws.com')

    def test_eu_case_insensitive(self):
        runner = EMRJobRunner(conf_paths=[], region='eu')
        self.assertEqual(runner.make_emr_conn().host,
                         'elasticmapreduce.eu-west-1.amazonaws.com')

    def test_us_east_1(self):
        runner = EMRJobRunner(conf_paths=[], region='us-east-1')
        self.assertEqual(runner.make_emr_conn().host,
                         'elasticmapreduce.us-east-1.amazonaws.com')

    def test_us_west_1(self):
        runner = EMRJobRunner(conf_paths=[], region='us-west-1')
        self.assertEqual(runner.make_emr_conn().host,
                         'elasticmapreduce.us-west-1.amazonaws.com')

    def test_us_west_1_case_insensitive(self):
        runner = EMRJobRunner(conf_paths=[], region='US-West-1')
        self.assertEqual(runner.make_emr_conn().host,
                         'elasticmapreduce.us-west-1.amazonaws.com')

    def test_ap_southeast_1(self):
        runner = EMRJobRunner(conf_paths=[], region='ap-southeast-1')
        self.assertEqual(runner.make_emr_conn().host,
                         'elasticmapreduce.ap-southeast-1.amazonaws.com')

    def test_previously_unknown_region(self):
        runner = EMRJobRunner(conf_paths=[], region='lolcatnia-1')
        self.assertEqual(runner.make_emr_conn().host,
                         'elasticmapreduce.lolcatnia-1.amazonaws.com')

    def test_explicit_endpoints(self):
        runner = EMRJobRunner(conf_paths=[], region='EU',
                              s3_endpoint='s3-proxy', emr_endpoint='emr-proxy')
        self.assertEqual(runner.make_emr_conn().host, 'emr-proxy')

    def test_ssl_fallback_host(self):
        runner = EMRJobRunner(conf_paths=[], region='us-west-1')

        with patch.object(MockEmrConnection, 'STRICT_SSL', True):
            emr_conn = runner.make_emr_conn()
            self.assertEqual(emr_conn.host,
                             'elasticmapreduce.us-west-1.amazonaws.com')
            # this should still work
            self.assertEqual(list(_yield_all_clusters(emr_conn)), [])
            # but it's only because we've switched to the alternate hostname
            self.assertEqual(emr_conn.host,
                             'us-west-1.elasticmapreduce.amazonaws.com')

        # without SSL issues, we should stay on the same endpoint
        emr_conn = runner.make_emr_conn()
        self.assertEqual(emr_conn.host,
                         'elasticmapreduce.us-west-1.amazonaws.com')

        self.assertEqual(list(_yield_all_clusters(emr_conn)), [])
        self.assertEqual(emr_conn.host,
                         'elasticmapreduce.us-west-1.amazonaws.com')


class TestSSHLs(MockBotoTestCase):

    def setUp(self):
        super(TestSSHLs, self).setUp()
        self.make_runner()

    def tearDown(self):
        super(TestSSHLs, self).tearDown()

    def make_runner(self):
        self.runner = EMRJobRunner(conf_paths=[])
        self.prepare_runner_for_ssh(self.runner)

    def test_ssh_ls(self):
        self.add_slave()

        mock_ssh_dir('testmaster', 'test')
        mock_ssh_file('testmaster', posixpath.join('test', 'one'), b'')
        mock_ssh_file('testmaster', posixpath.join('test', 'two'), b'')
        mock_ssh_dir('testmaster!testslave0', 'test')
        mock_ssh_file('testmaster!testslave0',
                      posixpath.join('test', 'three'), b'')

        self.assertEqual(
            sorted(self.runner.fs.ls('ssh://testmaster/test')),
            ['ssh://testmaster/test/one', 'ssh://testmaster/test/two'])

        self.assertEqual(
            list(self.runner.fs.ls('ssh://testmaster!testslave0/test')),
            ['ssh://testmaster!testslave0/test/three'])

        # ls() is a generator, so the exception won't fire until we list() it
        self.assertRaises(IOError, list,
                          self.runner.fs.ls('ssh://testmaster/does_not_exist'))


class TestNoBoto(TestCase):

    def setUp(self):
        self.blank_out_boto()

    def tearDown(self):
        self.restore_boto()

    def blank_out_boto(self):
        self._real_boto = mrjob.emr.boto
        mrjob.emr.boto = None
        mrjob.fs.s3.boto = None

    def restore_boto(self):
        mrjob.emr.boto = self._real_boto
        mrjob.fs.s3.boto = self._real_boto

    def test_init(self):
        # merely creating an EMRJobRunner should raise an exception
        # because it'll need to connect to S3 to set cloud_tmp_dir
        self.assertRaises(ImportError, EMRJobRunner, conf_paths=[])


class MasterBootstrapScriptTestCase(MockBotoTestCase):

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

        yelpy_tar_gz_path = os.path.join(self.tmp_dir, 'yelpy.tar.gz')
        tar_and_gzip(self.tmp_dir, yelpy_tar_gz_path, prefix='yelpy')

        # use all the bootstrap options
        runner = EMRJobRunner(conf_paths=[],
                              image_version=image_version,
                              bootstrap=[
                                  expected_python_bin + ' ' +
                                  foo_py_path + '#bar.py',
                                  's3://walrus/scripts/ohnoes.sh#'],
                              bootstrap_cmds=['echo "Hi!"', 'true', 'ls'],
                              bootstrap_files=['/tmp/quz'],
                              bootstrap_mrjob=True,
                              bootstrap_python_packages=[yelpy_tar_gz_path],
                              bootstrap_scripts=['speedups.sh', '/tmp/s.sh'])

        runner._add_bootstrap_files_for_upload()

        self.assertIsNotNone(runner._master_bootstrap_script_path)
        self.assertTrue(os.path.exists(runner._master_bootstrap_script_path))

        with open(runner._master_bootstrap_script_path) as f:
            lines = [line.rstrip() for line in f]

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
                '  chmod a+x $__mrjob_PWD/%s' % (name,),
                lines)

        # check files get downloaded
        assertScriptDownloads(foo_py_path, 'bar.py')
        assertScriptDownloads('s3://walrus/scripts/ohnoes.sh')
        assertScriptDownloads('/tmp/quz', 'quz')
        assertScriptDownloads(runner._mrjob_zip_path)
        assertScriptDownloads('speedups.sh')
        assertScriptDownloads('/tmp/s.sh')
        if PY2:
            assertScriptDownloads(yelpy_tar_gz_path)

        # check scripts get run

        # bootstrap
        self.assertIn('  ' + expected_python_bin + ' $__mrjob_PWD/bar.py',
                      lines)
        self.assertIn('  $__mrjob_PWD/ohnoes.sh', lines)
        # bootstrap_cmds
        self.assertIn('  echo "Hi!"', lines)
        self.assertIn('  true', lines)
        self.assertIn('  ls', lines)
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
        # bootstrap_python_packages
        self.assertIn(('  sudo ' + expected_python_bin +
                       ' -m pip install $__mrjob_PWD/yelpy.tar.gz'), lines)
        # bootstrap_scripts
        self.assertIn('  $__mrjob_PWD/speedups.sh', lines)
        self.assertIn('  $__mrjob_PWD/s.sh', lines)

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
                              bootstrap_python=False)

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

    def test_pooling_requires_bootstrap_script(self):
        # using pooling currently requires us to create a bootstrap script;
        # see #1503
        runner = EMRJobRunner(conf_paths=[],
                              bootstrap_mrjob=False,
                              bootstrap_python=False,
                              pool_clusters=True)

        runner._add_bootstrap_files_for_upload()
        self.assertIsNotNone(runner._master_bootstrap_script_path)

    def test_bootstrap_actions_get_added(self):
        bootstrap_actions = [
            ('s3://elasticmapreduce/bootstrap-actions/configure-hadoop'
             ' -m,mapred.tasktracker.map.tasks.maximum=1'),
            's3://foo/bar',
        ]

        runner = EMRJobRunner(conf_paths=[],
                              bootstrap_actions=bootstrap_actions,
                              cloud_fs_sync_secs=0.00)

        cluster_id = runner.make_persistent_cluster()

        emr_conn = runner.make_emr_conn()
        actions = list(_yield_all_bootstrap_actions(emr_conn, cluster_id))

        self.assertEqual(len(actions), 3)

        self.assertEqual(
            actions[0].scriptpath,
            's3://elasticmapreduce/bootstrap-actions/configure-hadoop')
        self.assertEqual(
            actions[0].args[0].value,
            '-m,mapred.tasktracker.map.tasks.maximum=1')
        self.assertEqual(actions[0].name, 'action 0')

        self.assertEqual(actions[1].scriptpath, 's3://foo/bar')
        self.assertEqual(actions[1].args, [])
        self.assertEqual(actions[1].name, 'action 1')

        # check for master bootstrap script
        self.assertTrue(actions[2].scriptpath.startswith('s3://mrjob-'))
        self.assertTrue(actions[2].scriptpath.endswith('b.sh'))
        self.assertEqual(actions[2].args, [])
        self.assertEqual(actions[2].name, 'master')

        # make sure master bootstrap script is on S3
        self.assertTrue(runner.fs.exists(actions[2].scriptpath))

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
                              cloud_fs_sync_secs=0.00)

        cluster_id = runner.make_persistent_cluster()

        emr_conn = runner.make_emr_conn()
        actions = list(_yield_all_bootstrap_actions(emr_conn, cluster_id))

        self.assertEqual(len(actions), 2)

        self.assertTrue(actions[0].scriptpath.startswith('s3://mrjob-'))
        self.assertTrue(actions[0].scriptpath.endswith('/apt-install.sh'))
        self.assertEqual(actions[0].name, 'action 0')
        self.assertEqual(actions[0].args[0].value, 'python-scipy')
        self.assertEqual(actions[0].args[1].value, 'mysql-server')

        # check for master bootstrap script
        self.assertTrue(actions[1].scriptpath.startswith('s3://mrjob-'))
        self.assertTrue(actions[1].scriptpath.endswith('b.sh'))
        self.assertEqual(actions[1].args, [])
        self.assertEqual(actions[1].name, 'master')

        # make sure master bootstrap script is on S3
        self.assertTrue(runner.fs.exists(actions[1].scriptpath))


class MasterNodeSetupScriptTestCase(MockBotoTestCase):

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

        self.assertTrue(contents.startswith(b'#!/bin/sh -ex\n'))
        self.assertIn(b'aws s3 cp ', contents)
        self.assertNotIn(b'hadoop fs -copyToLocal ', contents)
        self.assertIn(b'chmod a+x ', contents)
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


class EMRNoMapperTestCase(MockBotoTestCase):

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

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

        self.assertEqual(sorted(results),
                         [(1, ['blue', 'one', 'red', 'two']),
                          (4, ['fish'])])


class PoolMatchingTestCase(MockBotoTestCase):

    def make_pooled_cluster(self, name=None, minutes_ago=0, **kwargs):
        """Returns ``(runner, cluster_id)``. Set minutes_ago to set
        ``cluster.startdatetime`` to seconds before
        ``datetime.datetime.now()``."""
        runner = EMRJobRunner(pool_clusters=True,
                              pool_name=name,
                              **kwargs)
        cluster_id = runner.make_persistent_cluster()
        mock_cluster = self.mock_emr_clusters[cluster_id]

        mock_cluster.status.state = 'WAITING'
        start = datetime.now() - timedelta(minutes=minutes_ago)
        mock_cluster.status.timeline.creationdatetime = (
            start.strftime(boto.utils.ISO8601))
        return runner, cluster_id

    def get_cluster(self, job_args, job_class=MRTwoStepJob):
        mr_job = job_class(job_args)
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            self.prepare_runner_for_ssh(runner)
            runner.run()

            return runner.get_cluster_id()

    def assertJoins(self, cluster_id, job_args, job_class=MRTwoStepJob):
        actual_cluster_id = self.get_cluster(job_args, job_class=job_class)

        self.assertEqual(actual_cluster_id, cluster_id)

    def assertDoesNotJoin(self, cluster_id, job_args, job_class=MRTwoStepJob):

        actual_cluster_id = self.get_cluster(job_args, job_class=job_class)

        self.assertNotEqual(actual_cluster_id, cluster_id)

        # terminate the cluster created by this assert, to avoid
        # very confusing behavior (see Issue #331)
        emr_conn = EMRJobRunner(conf_paths=[]).make_emr_conn()
        emr_conn.terminate_jobflow(actual_cluster_id)

    def make_simple_runner(self, pool_name):
        """Make an EMRJobRunner that is ready to try to find a pool to join"""
        mr_job = MRTwoStepJob([
            '-r', 'emr', '-v', '--pool-clusters',
            '--pool-name', pool_name])
        mr_job.sandbox()
        runner = mr_job.make_runner()
        self.prepare_runner_for_ssh(runner)
        runner._prepare_for_launch()
        return runner

    def test_make_new_pooled_cluster(self):
        mr_job = MRTwoStepJob(['-r', 'emr', '-v', '--pool-clusters'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            self.prepare_runner_for_ssh(runner)
            runner.run()

            # Make sure that the runner made a pooling-enabled cluster
            emr_conn = runner.make_emr_conn()
            bootstrap_actions = list(_yield_all_bootstrap_actions(
                emr_conn, runner.get_cluster_id()))

            jf_hash, jf_name = _pool_hash_and_name(bootstrap_actions)
            self.assertEqual(jf_hash, runner._pool_hash())
            self.assertEqual(jf_name, runner._opts['pool_name'])

            emr_conn.simulate_progress(runner.get_cluster_id())
            cluster = runner._describe_cluster()
            self.assertEqual(cluster.status.state, 'WAITING')

    def test_join_pooled_cluster(self):
        _, cluster_id = self.make_pooled_cluster()

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters'])

    def test_join_named_pool(self):
        _, cluster_id = self.make_pooled_cluster('pool1')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--pool-name', 'pool1'])

    def test_join_anyway_if_i_say_so(self):
        _, cluster_id = self.make_pooled_cluster(image_version='2.0')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--cluster-id', cluster_id,
            '--image-version', '2.2'])

    def test_pooling_with_image_version(self):
        _, cluster_id = self.make_pooled_cluster(image_version='2.0')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--image-version', '2.0'])

    def test_pooling_with_image_version_prefix_major_minor(self):
        _, cluster_id = self.make_pooled_cluster(image_version='2.0.0')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--image-version', '2.0'])

    def test_pooling_with_image_version_prefix_major(self):
        _, cluster_id = self.make_pooled_cluster(image_version='2.0.0')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--image-version', '2'])

    def test_dont_join_pool_with_wrong_image_version(self):
        _, cluster_id = self.make_pooled_cluster(image_version='2.2')

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--image-version', '2.0'])

    def test_pooling_with_4_x_ami_version(self):
        # this actually uses release label internally
        _, cluster_id = self.make_pooled_cluster(image_version='4.0.0')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--image-version', '4.0.0'])

    def test_pooling_with_release_label(self):
        _, cluster_id = self.make_pooled_cluster(release_label='emr-4.0.0')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--release-label', 'emr-4.0.0'])

    def test_dont_join_pool_with_wrong_release_label(self):
        _, cluster_id = self.make_pooled_cluster(release_label='emr-4.0.1')

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--release-label', 'emr-4.0.0'])

    def test_dont_join_pool_without_release_label(self):
        _, cluster_id = self.make_pooled_cluster(image_version='2.2')

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--release-label', 'emr-4.0.0'])

    def test_matching_release_label_and_ami_version(self):
        _, cluster_id = self.make_pooled_cluster(release_label='emr-4.0.0')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--image-version', '4.0.0'])

    def test_non_matching_release_label_and_ami_version(self):
        _, cluster_id = self.make_pooled_cluster(release_label='emr-4.0.0')

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--image-version', '2.2'])

    def test_release_label_hides_ami_version(self):
        _, cluster_id = self.make_pooled_cluster(release_label='emr-4.0.0')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--release-label', 'emr-4.0.0',
            '--image-version', '1.0.0'])

    def test_matching_applications(self):
        _, cluster_id = self.make_pooled_cluster(
            image_version='4.0.0', applications=['Mahout'])

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--image-version', '4.0.0',
            '--application', 'Mahout'])

    def test_extra_applications_okay(self):
        _, cluster_id = self.make_pooled_cluster(
            image_version='4.0.0', applications=['Ganglia', 'Mahout'])

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--image-version', '4.0.0',
            '--application', 'Mahout'])

    def test_missing_applications_not_okay(self):
        _, cluster_id = self.make_pooled_cluster(
            image_version='4.0.0', applications=['Mahout'])

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--image-version', '4.0.0',
            '--application', 'Ganglia',
            '--application', 'Mahout'])

    def test_application_matching_is_case_insensitive(self):
        _, cluster_id = self.make_pooled_cluster(
            image_version='4.0.0', applications=['Mahout'])

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--image-version', '4.0.0',
            '--application', 'mahout'])

    def test_matching_emr_configurations(self):
        _, cluster_id = self.make_pooled_cluster(
            image_version='4.0.0',
            emr_configurations=[HADOOP_ENV_EMR_CONFIGURATION])

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--image-version', '4.0.0',
            '--emr-configuration', json.dumps(HADOOP_ENV_EMR_CONFIGURATION),
        ])

    def test_missing_emr_configurations(self):
        _, cluster_id = self.make_pooled_cluster(
            image_version='4.0.0',
            emr_configurations=[HADOOP_ENV_EMR_CONFIGURATION])

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--image-version', '4.0.0',
        ])

    def test_extra_emr_configuration(self):
        _, cluster_id = self.make_pooled_cluster(
            image_version='4.0.0')

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--image-version', '4.0.0',
            '--emr-configuration', json.dumps(HADOOP_ENV_EMR_CONFIGURATION),
        ])

    def test_wrong_emr_configuration(self):
        _, cluster_id = self.make_pooled_cluster(
            image_version='4.0.0',
            emr_configurations=[HADOOP_ENV_EMR_CONFIGURATION])

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--image-version', '4.0.0',
            '--emr-configuration', json.dumps(CORE_SITE_EMR_CONFIGURATION),
        ])

    def test_wrong_emr_configuration_ordering(self):
        _, cluster_id = self.make_pooled_cluster(
            image_version='4.0.0',
            emr_configurations=[CORE_SITE_EMR_CONFIGURATION,
                                HADOOP_ENV_EMR_CONFIGURATION])

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--image-version', '4.0.0',
            '--emr-configuration', json.dumps(HADOOP_ENV_EMR_CONFIGURATION),
            '--emr-configuration', json.dumps(CORE_SITE_EMR_CONFIGURATION),
        ])

    def test_matching_subnet(self):
        _, cluster_id = self.make_pooled_cluster(
            subnet='subnet-ffffffff')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--subnet', 'subnet-ffffffff'])

    def test_other_subnet(self):
        _, cluster_id = self.make_pooled_cluster(
            subnet='subnet-ffffffff')

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--subnet', 'subnet-eeeeeeee'])

    def test_require_subnet(self):
        _, cluster_id = self.make_pooled_cluster()

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--subnet', 'subnet-ffffffff'])

    def test_require_no_subnet(self):
        _, cluster_id = self.make_pooled_cluster(
            subnet='subnet-ffffffff')

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters'])

    def test_empty_string_subnet(self):
        # same as no subnet
        _, cluster_id = self.make_pooled_cluster()

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--subnet', ''])

    def test_pooling_with_additional_emr_info(self):
        info = '{"tomatoes": "actually a fruit!"}'
        _, cluster_id = self.make_pooled_cluster(
            additional_emr_info=info)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--additional-emr-info', info])

    def test_dont_join_pool_with_wrong_additional_emr_info(self):
        info = '{"tomatoes": "actually a fruit!"}'
        _, cluster_id = self.make_pooled_cluster()

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--additional-emr-info', info])

    def test_join_pool_with_same_instance_type_and_count(self):
        _, cluster_id = self.make_pooled_cluster(
            instance_type='m2.4xlarge',
            num_core_instances=20)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-type', 'm2.4xlarge',
            '--num-core-instances', '20'])

    def test_join_pool_with_more_of_same_instance_type(self):
        _, cluster_id = self.make_pooled_cluster(
            instance_type='m2.4xlarge',
            num_core_instances=20)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-type', 'm2.4xlarge',
            '--num-core-instances', '5'])

    def test_join_cluster_with_bigger_instances(self):
        _, cluster_id = self.make_pooled_cluster(
            instance_type='m2.4xlarge',
            num_core_instances=20)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-type', 'm1.medium',
            '--num-core-instances', '20'])

    def test_join_cluster_with_enough_cpu_and_memory(self):
        _, cluster_id = self.make_pooled_cluster(
            instance_type='c1.xlarge',
            num_core_instances=3)

        # join the pooled cluster even though it has less instances total,
        # since they're have enough memory and CPU
        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-type', 'm1.medium',
            '--num-core-instances', '10'])

    def test_dont_join_cluster_with_instances_with_too_little_memory(self):
        _, cluster_id = self.make_pooled_cluster(
            instance_type='c1.xlarge',
            num_core_instances=20)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-type', 'm2.4xlarge',
            '--num-core-instances', '2'])

    def test_master_instance_has_to_be_big_enough(self):
        _, cluster_id = self.make_pooled_cluster(
            instance_type='c1.xlarge',
            num_core_instances=10)

        # We implicitly want a MASTER instance with c1.xlarge. The pooled
        # cluster has an m1.medium master instance and 9 c1.xlarge core
        # instances, which doesn't match.
        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-type', 'c1.xlarge'])

    def test_unknown_instance_type_against_matching_pool(self):
        _, cluster_id = self.make_pooled_cluster(
            instance_type='a1.sauce',
            num_core_instances=10)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-type', 'a1.sauce',
            '--num-core-instances', '10'])

    def test_unknown_instance_type_against_pool_with_more_instances(self):
        _, cluster_id = self.make_pooled_cluster(
            instance_type='a1.sauce',
            num_core_instances=20)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-type', 'a1.sauce',
            '--num-core-instances', '10'])

    def test_unknown_instance_type_against_pool_with_less_instances(self):
        _, cluster_id = self.make_pooled_cluster(
            instance_type='a1.sauce',
            num_core_instances=5)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-type', 'a1.sauce',
            '--num-core-instances', '10'])

    def test_unknown_instance_type_against_other_instance_types(self):
        _, cluster_id = self.make_pooled_cluster(
            instance_type='m2.4xlarge',
            num_core_instances=100)

        # for all we know, "a1.sauce" instances have even more memory and CPU
        # than m2.4xlarge
        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-type', 'a1.sauce',
            '--num-core-instances', '2'])

    def test_can_join_cluster_with_same_bid_price(self):
        _, cluster_id = self.make_pooled_cluster(
            master_instance_bid_price='0.25')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--master-instance-bid-price', '0.25'])

    def test_can_join_cluster_with_higher_bid_price(self):
        _, cluster_id = self.make_pooled_cluster(
            master_instance_bid_price='25.00')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--master-instance-bid-price', '0.25'])

    def test_cant_join_cluster_with_lower_bid_price(self):
        _, cluster_id = self.make_pooled_cluster(
            master_instance_bid_price='0.25',
            num_core_instances=100)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--master-instance-bid-price', '25.00'])

    def test_on_demand_satisfies_any_bid_price(self):
        _, cluster_id = self.make_pooled_cluster()

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--master-instance-bid-price', '25.00'])

    def test_no_bid_price_satisfies_on_demand(self):
        _, cluster_id = self.make_pooled_cluster(
            master_instance_bid_price='25.00')

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters'])

    def test_core_and_task_instance_types(self):
        # a tricky test that mixes and matches different criteria
        _, cluster_id = self.make_pooled_cluster(
            core_instance_bid_price='0.25',
            task_instance_bid_price='25.00',
            task_instance_type='c1.xlarge',
            num_core_instances=2,
            num_task_instances=3)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--num-ec2-core-instances', '2',
            '--num-ec2-task-instances', '10',  # more instances, but smaller
            '--core-instance-bid-price', '0.10',
            '--master-instance-bid-price', '77.77',
            '--task-instance-bid-price', '22.00'])

    def test_dont_join_full_cluster(self):
        dummy_runner, cluster_id = self.make_pooled_cluster()

        # fill the cluster
        self.mock_emr_clusters[cluster_id]._steps = 999 * [
            MockEmrObject(
                actiononfailure='CANCEL_AND_WAIT',
                config=MockEmrObject(args=[]),
                id='s-FAKE',
                name='dummy',
                status=MockEmrObject(
                    state='COMPLETED',
                    timeline=MockEmrObject(
                        enddatetime='definitely not none')))
        ]

        # a two-step job shouldn't fit
        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters'],
            job_class=MRTwoStepJob)

    def test_join_almost_full_cluster(self):
        dummy_runner, cluster_id = self.make_pooled_cluster()

        # fill the cluster
        self.mock_emr_clusters[cluster_id]._steps = 999 * [
            MockEmrObject(
                actiononfailure='CANCEL_AND_WAIT',
                config=MockEmrObject(args=[]),
                id='s-FAKE',
                name='dummy',
                status=MockEmrObject(
                    state='COMPLETED',
                    timeline=MockEmrObject(
                        enddatetime='definitely not none')))
        ]

        # a one-step job should fit
        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters'],
            job_class=MRWordCount)

    def test_dont_join_full_cluster_256_step_limit(self):
        dummy_runner, cluster_id = self.make_pooled_cluster(
            image_version='2.4.7')

        # fill the cluster
        self.mock_emr_clusters[cluster_id]._steps = 255 * [
            MockEmrObject(
                actiononfailure='CANCEL_AND_WAIT',
                config=MockEmrObject(args=[]),
                id='s-FAKE',
                name='dummy',
                status=MockEmrObject(
                    state='COMPLETED',
                    timeline=MockEmrObject(
                        enddatetime='definitely not none')))
        ]

        # a two-step job shouldn't fit
        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--image-version', '2.4.7'],
            job_class=MRTwoStepJob)

    def test_join_almost_full_2_x_ami_cluster(self):
        dummy_runner, cluster_id = self.make_pooled_cluster(
            image_version='2.4.7')

        # fill the cluster
        self.mock_emr_clusters[cluster_id]._steps = 255 * [
            MockEmrObject(
                actiononfailure='CANCEL_AND_WAIT',
                config=MockEmrObject(args=[]),
                id='s-FAKE',
                name='dummy',
                status=MockEmrObject(
                    state='COMPLETED',
                    timeline=MockEmrObject(
                        enddatetime='definitely not none')))
        ]

        # a one-step job should fit
        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters', '--image-version', '2.4.7'],
            job_class=MRWordCount)

    def test_no_space_for_master_node_setup(self):
        dummy_runner, cluster_id = self.make_pooled_cluster()

        # fill the cluster
        self.mock_emr_clusters[cluster_id]._steps = 999 * [
            MockEmrObject(
                actiononfailure='CANCEL_AND_WAIT',
                config=MockEmrObject(args=[]),
                id='s-FAKE',
                name='dummy',
                status=MockEmrObject(
                    state='COMPLETED',
                    timeline=MockEmrObject(
                        enddatetime='definitely not none')))
        ]

        # --libjar makes this a two-step job, which won't fit
        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--libjar', 's3:///poohs-house/HUNNY.jar'],
            job_class=MRWordCount)

    def test_bearly_space_for_master_node_setup(self):
        dummy_runner, cluster_id = self.make_pooled_cluster()

        # fill the cluster
        self.mock_emr_clusters[cluster_id]._steps = 998 * [
            MockEmrObject(
                actiononfailure='CANCEL_AND_WAIT',
                config=MockEmrObject(args=[]),
                id='s-FAKE',
                name='dummy',
                status=MockEmrObject(
                    state='COMPLETED',
                    timeline=MockEmrObject(
                        enddatetime='definitely not none')))
        ]

        # now there's space for two steps
        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--libjar', 's3://poohs-house/HUNNY.jar'],
            job_class=MRWordCount)

    def test_dont_join_idle_with_pending_steps(self):
        dummy_runner, cluster_id = self.make_pooled_cluster()

        cluster = self.mock_emr_clusters[cluster_id]

        cluster._steps = [
            MockEmrObject(
                actiononfailure='CANCEL_AND_WAIT',
                config=MockEmrObject(args=[]),
                name='dummy',
                status=MockEmrObject(state='PENDING'))]
        cluster.delay_progress_simulation = 100  # keep step PENDING

        self.assertDoesNotJoin(cluster_id,
                               ['-r', 'emr', '--pool-clusters'])

    def test_do_join_idle_with_cancelled_steps(self):
        dummy_runner, cluster_id = self.make_pooled_cluster()

        self.mock_emr_clusters[cluster_id].steps = [
            MockEmrObject(
                state='FAILED',
                name='step 1 of 2',
                actiononfailure='CANCEL_AND_WAIT',
                enddatetime='sometime in the past',
                args=[]),
            # step 2 never ran, so its enddatetime is not set
            MockEmrObject(
                state='CANCELLED',
                name='step 2 of 2',
                actiononfailure='CANCEL_AND_WAIT',
                args=[])
        ]

        self.assertJoins(cluster_id,
                         ['-r', 'emr', '--pool-clusters'])

    def test_dont_join_wrong_named_pool(self):
        _, cluster_id = self.make_pooled_cluster('pool1')

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--pool-name', 'not_pool1'])

    def test_dont_join_wrong_mrjob_version(self):
        _, cluster_id = self.make_pooled_cluster()

        old_version = mrjob.__version__

        try:
            mrjob.__version__ = 'OVER NINE THOUSAAAAAND'

            self.assertDoesNotJoin(cluster_id, [
                '-r', 'emr', '--pool-clusters'])
        finally:
            mrjob.__version__ = old_version

    def test_dont_join_wrong_python_bin(self):
        _, cluster_id = self.make_pooled_cluster()

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--python-bin', 'snake'])

    def test_versions_dont_matter_if_no_bootstrap_mrjob(self):
        _, cluster_id = self.make_pooled_cluster(
            bootstrap_mrjob=False)

        old_version = mrjob.__version__

        try:
            mrjob.__version__ = 'OVER NINE THOUSAAAAAND'

            self.assertJoins(cluster_id, [
                '-r', 'emr', '--pool-clusters',
                '--no-bootstrap-mrjob',
                '--python-bin', 'snake'])
        finally:
            mrjob.__version__ = old_version

    def test_join_similarly_bootstrapped_pool(self):
        local_input_path = os.path.join(self.tmp_dir, 'input')
        with open(local_input_path, 'w') as input_file:
            input_file.write('bar\nfoo\n')

        _, cluster_id = self.make_pooled_cluster(
            bootstrap_files=[local_input_path])

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--bootstrap-file', local_input_path])

    def test_dont_join_differently_bootstrapped_pool(self):
        local_input_path = os.path.join(self.tmp_dir, 'input')
        with open(local_input_path, 'w') as input_file:
            input_file.write('bar\nfoo\n')

        _, cluster_id = self.make_pooled_cluster()

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--bootstrap-file', local_input_path])

    def test_dont_join_differently_bootstrapped_pool_2(self):
        local_input_path = os.path.join(self.tmp_dir, 'input')
        with open(local_input_path, 'w') as input_file:
            input_file.write('bar\nfoo\n')

        bootstrap_path = os.path.join(self.tmp_dir, 'go.sh')
        with open(bootstrap_path, 'w') as f:
            f.write('#!/usr/bin/sh\necho "hi mom"\n')

        _, cluster_id = self.make_pooled_cluster()

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--bootstrap-action', bootstrap_path + ' a b c'])

    def test_pool_contention(self):
        _, cluster_id = self.make_pooled_cluster('robert_downey_jr')

        def runner_plz():
            mr_job = MRTwoStepJob([
                '-r', 'emr', '-v', '--pool-clusters',
                '--pool-name', 'robert_downey_jr'])
            mr_job.sandbox()
            runner = mr_job.make_runner()
            runner._prepare_for_launch()
            return runner

        runner_1 = runner_plz()
        runner_2 = runner_plz()

        self.assertEqual(runner_1._find_cluster(), cluster_id)
        self.assertEqual(runner_2._find_cluster(), None)

    def test_sorting_by_time(self):
        _, cluster_id_1 = self.make_pooled_cluster('pool1', minutes_ago=20)
        _, cluster_id_2 = self.make_pooled_cluster('pool1', minutes_ago=40)

        runner_1 = self.make_simple_runner('pool1')
        runner_2 = self.make_simple_runner('pool1')

        self.assertEqual(runner_1._find_cluster(), cluster_id_1)
        self.assertEqual(runner_2._find_cluster(), cluster_id_2)

    def test_sorting_by_cpu_hours(self):
        _, cluster_id_1 = self.make_pooled_cluster('pool1',
                                                   minutes_ago=40,
                                                   num_core_instances=2)
        _, cluster_id_2 = self.make_pooled_cluster('pool1',
                                                   minutes_ago=20,
                                                   num_core_instances=1)

        runner_1 = self.make_simple_runner('pool1')
        runner_2 = self.make_simple_runner('pool1')

        self.assertEqual(runner_1._find_cluster(), cluster_id_1)
        self.assertEqual(runner_2._find_cluster(), cluster_id_2)

    def test_dont_destroy_own_pooled_cluster_on_failure(self):
        # Issue 242: job failure shouldn't kill the pooled clusters
        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '--pool-clusters'])
        mr_job.sandbox()

        self.mock_emr_failures = set([('j-MOCKCLUSTER0', 0)])

        with mr_job.make_runner() as runner:
            self.assertIsInstance(runner, EMRJobRunner)
            self.prepare_runner_for_ssh(runner)
            with logger_disabled('mrjob.emr'):
                self.assertRaises(StepFailedException, runner.run)

            emr_conn = runner.make_emr_conn()
            cluster_id = runner.get_cluster_id()
            for _ in range(10):
                emr_conn.simulate_progress(cluster_id)

            cluster = runner._describe_cluster()
            self.assertEqual(cluster.status.state, 'WAITING')

        # job shouldn't get terminated by cleanup
        emr_conn = runner.make_emr_conn()
        cluster_id = runner.get_cluster_id()
        for _ in range(10):
            emr_conn.simulate_progress(cluster_id)

        cluster = runner._describe_cluster()
        self.assertEqual(cluster.status.state, 'WAITING')

    def test_dont_destroy_other_pooled_cluster_on_failure(self):
        # Issue 242: job failure shouldn't kill the pooled clusters
        _, cluster_id = self.make_pooled_cluster()

        self.mock_emr_failures = set([(cluster_id, 0)])

        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '--pool-clusters'])
        mr_job.sandbox()

        self.mock_emr_failures = set([('j-MOCKCLUSTER0', 0)])

        with mr_job.make_runner() as runner:
            self.assertIsInstance(runner, EMRJobRunner)
            self.prepare_runner_for_ssh(runner)
            with logger_disabled('mrjob.emr'):
                self.assertRaises(StepFailedException, runner.run)

            self.assertEqual(runner.get_cluster_id(), cluster_id)

            emr_conn = runner.make_emr_conn()
            for _ in range(10):
                emr_conn.simulate_progress(cluster_id)

            cluster = runner._describe_cluster()
            self.assertEqual(cluster.status.state, 'WAITING')

        # job shouldn't get terminated by cleanup
        emr_conn = runner.make_emr_conn()
        cluster_id = runner.get_cluster_id()
        for _ in range(10):
            emr_conn.simulate_progress(cluster_id)

        cluster = runner._describe_cluster()
        self.assertEqual(cluster.status.state, 'WAITING')

    def test_max_hours_idle_doesnt_affect_pool_hash(self):
        # max_hours_idle uses a bootstrap action, but it's not included
        # in the pool hash
        _, cluster_id = self.make_pooled_cluster()

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters', '--max-hours-idle', '1'])

    def test_can_join_cluster_started_with_max_hours_idle(self):
        _, cluster_id = self.make_pooled_cluster(max_hours_idle=1)

        self.assertJoins(cluster_id, ['-r', 'emr', '--pool-clusters'])

    def test_can_join_cluster_with_same_key_pair(self):
        _, cluster_id = self.make_pooled_cluster(ec2_key_pair='EMR')

        self.assertJoins(
            cluster_id,
            ['-r', 'emr', '--ec2-key-pair', 'EMR', '--pool-clusters'])

    def test_cant_join_cluster_with_different_key_pair(self):
        _, cluster_id = self.make_pooled_cluster(ec2_key_pair='EMR')

        self.assertDoesNotJoin(
            cluster_id,
            ['-r', 'emr', '--ec2-key-pair', 'EMR2', '--pool-clusters'])

    def test_cant_join_cluster_with_missing_key_pair(self):
        _, cluster_id = self.make_pooled_cluster()

        self.assertDoesNotJoin(
            cluster_id,
            ['-r', 'emr', '--ec2-key-pair', 'EMR2', '--pool-clusters'])

    def test_ignore_key_pair_if_we_have_none(self):
        _, cluster_id = self.make_pooled_cluster(ec2_key_pair='EMR')

        self.assertJoins(
            cluster_id,
            ['-r', 'emr', '--pool-clusters'])

    def test_dont_join_cluster_without_spark(self):
        _, cluster_id = self.make_pooled_cluster()

        self.assertDoesNotJoin(
            cluster_id,
            ['-r', 'emr', '--pool-clusters'],
            job_class=MRNullSpark)

    def test_join_cluster_with_spark_3_x_ami(self):
        _, cluster_id = self.make_pooled_cluster(
            image_version='3.11.0',
            bootstrap_actions=[_3_X_SPARK_BOOTSTRAP_ACTION])

        self.assertJoins(
            cluster_id,
            ['-r', 'emr', '--pool-clusters', '--image-version', '3.11.0'],
            job_class=MRNullSpark)

    def test_join_cluster_with_spark_4_x_ami(self):
        _, cluster_id = self.make_pooled_cluster(
            image_version='4.7.2',
            applications=['Spark'])

        self.assertJoins(
            cluster_id,
            ['-r', 'emr', '--pool-clusters', '--image-version', '4.7.2'],
            job_class=MRNullSpark)

    def test_ignore_spark_bootstrap_action_on_4_x_ami(self):
        _, cluster_id = self.make_pooled_cluster(
            image_version='4.7.2',
            bootstrap_actions=[_3_X_SPARK_BOOTSTRAP_ACTION])

        self.assertDoesNotJoin(
            cluster_id,
            ['-r', 'emr', '--pool-clusters', '--image-version', '4.7.2'],
            job_class=MRNullSpark)

    def test_other_install_spark_bootstrap_action_on_3_x_ami(self):
        # has to be exactly the install-spark bootstrap action we expected
        _, cluster_id = self.make_pooled_cluster(
            image_version='3.11.0',
            bootstrap_actions=['s3://bucket/install-spark'])

        self.assertDoesNotJoin(
            cluster_id,
            ['-r', 'emr', '--pool-clusters', '--image-version', '3.11.0'],
            job_class=MRNullSpark)

    def test_ignore_instance_fleets(self):
        _, cluster_id = self.make_pooled_cluster()

        with patch.object(MockEmrConnection, 'list_instance_groups',
                          side_effect=INSTANCE_FLEETS_ERROR):
            self.assertDoesNotJoin(
                cluster_id,
                ['-r', 'emr', '--pool-clusters'])


class PoolingRecoveryTestCase(MockBotoTestCase):

    MRJOB_CONF_CONTENTS = {'runners': {'emr': {'pool_clusters': True}}}

    # for multiple failover test
    MAX_EMR_CONNECTIONS = 1000

    def make_pooled_cluster(self, **kwargs):
        cluster_id = EMRJobRunner(**kwargs).make_persistent_cluster()

        mock_cluster = self.mock_emr_clusters[cluster_id]
        mock_cluster.status.state = 'WAITING'

        return cluster_id

    def num_steps(self, cluster_id):
        return len(self.mock_emr_clusters[cluster_id]._steps)

    def test_join_healthy_cluster(self):
        cluster_id = self.make_pooled_cluster()

        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(self.num_steps(cluster_id), 2)
            self.assertEqual(runner.get_cluster_id(), cluster_id)

    def test_launch_new_cluster_after_self_termination(self):
        cluster_id = self.make_pooled_cluster()
        self.mock_emr_self_termination.add(cluster_id)

        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            # tried to add steps to pooled cluster, had to try again
            self.assertEqual(self.num_steps(cluster_id), 2)

            self.assertNotEqual(runner.get_cluster_id(), cluster_id)
            self.assertEqual(self.num_steps(runner.get_cluster_id()), 2)

    def test_restart_ssh_tunnel_on_launch(self):
        # regression test for #1549
        ssh_tunnel_cluster_ids = []

        def _set_up_ssh_tunnel(self):
            if self._ssh_proc is None:
                ssh_tunnel_cluster_ids.append(self._cluster_id)
                self._ssh_proc = Mock()

        self.start(patch(
            'mrjob.emr.EMRJobRunner._set_up_ssh_tunnel',
            side_effect=_set_up_ssh_tunnel, autospec=True))

        def _kill_ssh_tunnel(self):
            self._ssh_proc = None

        mock_kill_ssh_tunnel = self.start(patch(
            'mrjob.emr.EMRJobRunner._kill_ssh_tunnel',
            side_effect=_kill_ssh_tunnel,
            autospec=True))

        cluster_id = self.make_pooled_cluster()
        self.mock_emr_self_termination.add(cluster_id)

        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            # tried to add steps to pooled cluster, had to try again
            self.assertEqual(self.num_steps(cluster_id), 2)

            self.assertNotEqual(runner.get_cluster_id(), cluster_id)
            self.assertEqual(self.num_steps(runner.get_cluster_id()), 2)

            self.assertEqual(len(mock_kill_ssh_tunnel.call_args_list), 1)
            self.assertEqual(ssh_tunnel_cluster_ids,
                             [cluster_id, runner.get_cluster_id()])

    def test_join_pooled_cluster_after_self_termination(self):
        # cluster 1 should be preferable
        cluster1_id = self.make_pooled_cluster(num_core_instances=20)
        self.mock_emr_self_termination.add(cluster1_id)
        cluster2_id = self.make_pooled_cluster()

        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(self.num_steps(cluster1_id), 2)

            self.assertEqual(runner.get_cluster_id(), cluster2_id)
            self.assertEqual(self.num_steps(cluster2_id), 2)

    def test_multiple_failover(self):
        cluster_ids = []
        for _ in range(10):
            cluster_id = self.make_pooled_cluster()
            self.mock_emr_self_termination.add(cluster_id)

        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            for cluster_id in cluster_ids:
                self.assertEqual(self.num_steps(cluster_id), 2)

            self.assertNotIn(runner.get_cluster_id(), cluster_ids)
            self.assertEqual(self.num_steps(runner.get_cluster_id()), 2)

    def test_dont_recover_with_explicit_cluster_id(self):
        cluster_id = self.make_pooled_cluster()
        self.mock_emr_self_termination.add(cluster_id)

        job = MRTwoStepJob(['-r', 'emr', '--cluster-id', cluster_id])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertRaises(StepFailedException, runner.run)

            self.assertEqual(self.num_steps(cluster_id), 2)

    def test_dont_recover_from_user_termination(self):
        cluster_id = self.make_pooled_cluster()

        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            # don't have a mockboto hook for termination of cluster
            # during run(), so running the two halves of run() separately
            runner._launch()

            self.assertEqual(runner.get_cluster_id(), cluster_id)
            self.assertEqual(self.num_steps(cluster_id), 2)

            self.connect_emr().terminate_jobflow(cluster_id)

            self.assertRaises(StepFailedException, runner._finish_run)

    def test_dont_recover_from_created_cluster_self_terminating(self):
        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()

            cluster_id = runner.get_cluster_id()
            self.assertEqual(self.num_steps(cluster_id), 2)
            self.mock_emr_self_termination.add(cluster_id)

            self.assertRaises(StepFailedException, runner._finish_run)

    def test_dont_recover_from_step_failure(self):
        cluster_id = self.make_pooled_cluster()
        self.mock_emr_failures = set([(cluster_id, 0)])

        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertRaises(StepFailedException, runner.run)

            self.assertEqual(runner.get_cluster_id(), cluster_id)

    def test_cluster_info_cache_gets_cleared(self):
        cluster_id = self.make_pooled_cluster()

        self.mock_emr_self_termination.add(cluster_id)

        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()

            self.assertEqual(runner.get_cluster_id(), cluster_id)
            addr = runner._address_of_master()

            runner._finish_run()

            self.assertNotEqual(runner.get_cluster_id(), cluster_id)
            self.assertNotEqual(runner._address_of_master(), addr)


class PoolingDisablingTestCase(MockBotoTestCase):

    MRJOB_CONF_CONTENTS = {'runners': {'emr': {
        'check_cluster_every': 0.00,
        'cloud_fs_sync_secs': 0.00,
        'pool_clusters': True,
    }}}

    def test_can_turn_off_pooling_from_cmd_line(self):
        mr_job = MRTwoStepJob(['-r', 'emr', '-v', '--no-pool-clusters'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            self.prepare_runner_for_ssh(runner)
            runner.run()

            cluster = runner._describe_cluster()
            self.assertEqual(cluster.autoterminate, 'true')


class S3LockTestCase(MockBotoTestCase):

    def setUp(self):
        super(S3LockTestCase, self).setUp()
        self.make_buckets()

    def make_buckets(self):
        self.add_mock_s3_data({'locks': {
            'expired_lock': b'x',
        }}, datetime.utcnow() - timedelta(minutes=30))
        self.lock_uri = 's3://locks/some_lock'
        self.expired_lock_uri = 's3://locks/expired_lock'

    def test_lock(self):
        # Most basic test case
        runner = EMRJobRunner(conf_paths=[])

        self.assertEqual(
            True,
            _attempt_to_acquire_lock(runner.fs, self.lock_uri, 0, 'jf1'))

        self.assertEqual(
            False,
            _attempt_to_acquire_lock(runner.fs, self.lock_uri, 0, 'jf2'))

    def test_lock_expiration(self):
        runner = EMRJobRunner(conf_paths=[])

        did_lock = _attempt_to_acquire_lock(
            runner.fs, self.expired_lock_uri, 0, 'jf1',
            mins_to_expiration=5)
        self.assertEqual(True, did_lock)

    def test_key_race_condition(self):
        # Test case where one attempt puts the key in existence
        runner = EMRJobRunner(conf_paths=[])

        key = _lock_acquire_step_1(runner.fs, self.lock_uri, 'jf1')
        self.assertNotEqual(key, None)

        key2 = _lock_acquire_step_1(runner.fs, self.lock_uri, 'jf2')
        self.assertEqual(key2, None)

    def test_read_race_condition(self):
        # test case where both try to create the key
        runner = EMRJobRunner(conf_paths=[])

        key = _lock_acquire_step_1(runner.fs, self.lock_uri, 'jf1')
        self.assertNotEqual(key, None)

        # acquire the key by subversive means to simulate contention
        bucket_name, key_prefix = parse_s3_uri(self.lock_uri)
        bucket = runner.fs.get_bucket(bucket_name)
        key2 = bucket.get_key(key_prefix)

        # and take the lock!
        key2.set_contents_from_string(b'jf2')

        self.assertFalse(_lock_acquire_step_2(key, 'jf1'), 'Lock should fail')


class MaxHoursIdleTestCase(MockBotoTestCase):

    def assertRanIdleTimeoutScriptWith(self, runner, args):
        emr_conn = runner.make_emr_conn()
        cluster_id = runner.get_cluster_id()

        actions = list(_yield_all_bootstrap_actions(emr_conn, cluster_id))
        action = actions[-1]

        self.assertEqual(action.name, 'idle timeout')
        self.assertEqual(
            action.scriptpath,
            runner._upload_mgr.uri(_MAX_HOURS_IDLE_BOOTSTRAP_ACTION_PATH))
        self.assertEqual([arg.value for arg in action.args], args)

    def assertDidNotUseIdleTimeoutScript(self, runner):
        emr_conn = runner.make_emr_conn()
        cluster_id = runner.get_cluster_id()

        actions = list(_yield_all_bootstrap_actions(emr_conn, cluster_id))
        action_names = [a.name for a in actions]

        self.assertNotIn('idle timeout', action_names)
        # idle timeout script should not even be uploaded
        self.assertNotIn(_MAX_HOURS_IDLE_BOOTSTRAP_ACTION_PATH,
                         runner._upload_mgr.path_to_uri())

    def test_default(self):
        mr_job = MRWordCount(['-r', 'emr'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.run()
            self.assertDidNotUseIdleTimeoutScript(runner)

    def test_non_persistent_cluster(self):
        mr_job = MRWordCount(['-r', 'emr', '--max-hours-idle', '1'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.run()
            self.assertDidNotUseIdleTimeoutScript(runner)

    def test_persistent_cluster(self):
        mr_job = MRWordCount(['-r', 'emr', '--max-hours-idle', '0.01'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.make_persistent_cluster()
            self.assertRanIdleTimeoutScriptWith(runner, ['36', '3600'])

    def test_mins_to_end_of_hour(self):
        mr_job = MRWordCount(['-r', 'emr', '--max-hours-idle', '1',
                              '--mins-to-end-of-hour', '10'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.make_persistent_cluster()
            self.assertRanIdleTimeoutScriptWith(runner, ['3600', '600'])

    def test_mins_to_end_of_hour_does_nothing_without_max_hours_idle(self):
        mr_job = MRWordCount(['-r', 'emr', '--mins-to-end-of-hour', '10'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.make_persistent_cluster()
            self.assertDidNotUseIdleTimeoutScript(runner)

    def test_use_integers(self):
        mr_job = MRWordCount(['-r', 'emr', '--max-hours-idle', '1.000001',
                              '--mins-to-end-of-hour', '10.000001'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.make_persistent_cluster()
            self.assertRanIdleTimeoutScriptWith(runner, ['3600', '600'])

    def pooled_clusters(self):
        mr_job = MRWordCount(['-r', 'emr', '--pool-clusters',
                              '--max-hours-idle', '0.5'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.run()
            self.assertRanIdleTimeoutScriptWith(runner, ['1800', '300'])

    def test_bootstrap_script_is_actually_installed(self):
        self.assertTrue(os.path.exists(_MAX_HOURS_IDLE_BOOTSTRAP_ACTION_PATH))


class TestCatFallback(MockBotoTestCase):

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


class CleanupOptionsTestCase(MockBotoTestCase):

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


class CleanupClusterTestCase(MockBotoTestCase):

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
            with patch.object(EMRJobRunner, 'make_emr_conn') as m:
                r._cleanup_cluster()
                self.assertTrue(m().terminate_jobflow.called)

    def test_kill_cluster_if_successful(self):
        # If they are setting up the cleanup to kill the cluster, mrjob should
        # kill the cluster independent of job success.
        with no_handlers_for_logger('mrjob.emr'):
            r = self._quick_runner()
            with patch.object(mrjob.emr.EMRJobRunner, 'make_emr_conn') as m:
                r._ran_job = True
                r._cleanup_cluster()
                self.assertTrue(m().terminate_jobflow.called)

    def test_kill_persistent_cluster(self):
        with no_handlers_for_logger('mrjob.emr'):
            r = self._quick_runner()
            with patch.object(mrjob.emr.EMRJobRunner, 'make_emr_conn') as m:
                r._opts['cluster_id'] = 'j-MOCKCLUSTER0'
                r._cleanup_cluster()
                self.assertTrue(m().terminate_jobflow.called)


class JobWaitTestCase(MockBotoTestCase):

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

        self.start(patch.object(EMRJobRunner, 'make_emr_conn'))
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


class PoolWaitMinutesOptionTestCase(MockBotoTestCase):

    def test_default_pool_wait_minutes(self):
        runner = self.make_runner('--no-conf')
        self.assertEqual(runner._opts['pool_wait_minutes'], 0)

    def test_pool_wait_minutes_from_mrjob_conf(self):
        # tests issue #1070
        MRJOB_CONF_WITH_POOL_WAIT_MINUTES = {'runners': {'emr': {
            'check_cluster_every': 0.00,
            'cloud_fs_sync_secs': 0.00,
            'pool_wait_minutes': 11,
        }}}

        with mrjob_conf_patcher(MRJOB_CONF_WITH_POOL_WAIT_MINUTES):
            runner = self.make_runner()
            self.assertEqual(runner._opts['pool_wait_minutes'], 11)

    def test_pool_wait_minutes_from_command_line(self):
        runner = self.make_runner('--pool-wait-minutes', '12')
        self.assertEqual(runner._opts['pool_wait_minutes'], 12)


class BuildStreamingStepTestCase(MockBotoTestCase):

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
            return_value='3.7.0'))

        self.start(patch(
            'mrjob.emr.EMRJobRunner.get_hadoop_version',
            return_value='2.4.0'))

        self.start(patch(
            'mrjob.emr.EMRJobRunner._get_streaming_jar_and_step_arg_prefix',
            return_value=('streaming.jar', [])))

    def _get_streaming_step(self, step, **kwargs):
        runner = EMRJobRunner(
            mr_job_script='my_job.py',
            conf_paths=[],
            stdin=BytesIO(),
            **kwargs)

        runner._steps = [step]

        runner._add_job_files_for_upload()
        runner._add_master_node_setup_files_for_upload()

        with patch('boto.emr.StreamingStep', dict):
            return runner._build_streaming_step(0)

    def test_basic_mapper(self):
        ss = self._get_streaming_step(
            dict(type='streaming', mapper=dict(type='script')))

        self.assertEqual(ss['mapper'],
                         PYTHON_BIN + ' my_job.py --step-num=0 --mapper')
        self.assertEqual(ss['combiner'], None)
        self.assertEqual(ss['reducer'], None)

    def test_basic_reducer(self):
        ss = self._get_streaming_step(
            dict(type='streaming', reducer=dict(type='script')))

        self.assertEqual(ss['mapper'], 'cat')
        self.assertEqual(ss['combiner'], None)
        self.assertEqual(ss['reducer'],
                         PYTHON_BIN + ' my_job.py --step-num=0 --reducer')

        self.assertEqual(ss['jar'], 'streaming.jar')
        self.assertEqual(ss['step_args'][:1], ['-files'])  # no prefix

    def test_pre_filters(self):
        ss = self._get_streaming_step(
            dict(type='streaming',
                 mapper=dict(
                     type='script',
                     pre_filter='grep anything'),
                 combiner=dict(
                     type='script',
                     pre_filter='grep nothing'),
                 reducer=dict(
                     type='script',
                     pre_filter='grep something')))

        self.assertEqual(ss['mapper'],
                         "/bin/sh -ex -c 'grep anything | " +
                         PYTHON_BIN +
                         " my_job.py --step-num=0 --mapper'")
        self.assertEqual(ss['combiner'],
                         "/bin/sh -ex -c 'grep nothing | " +
                         PYTHON_BIN +
                         " my_job.py --step-num=0 --combiner'")
        self.assertEqual(ss['reducer'],
                         "/bin/sh -ex -c 'grep something | " +
                         PYTHON_BIN +
                         " my_job.py --step-num=0 --reducer'")

    def test_pre_filter_escaping(self):
        ss = self._get_streaming_step(
            dict(type='streaming',
                 mapper=dict(
                     type='script',
                     pre_filter=bash_wrap("grep 'anything'"))))

        self.assertEqual(
            ss['mapper'],
            "/bin/sh -ex -c 'bash -c '\\''grep"
            " '\\''\\'\\'''\\''anything'\\''\\'\\'''\\'''\\'' | " +
            PYTHON_BIN +
            " my_job.py --step-num=0 --mapper'")
        self.assertEqual(
            ss['combiner'], None)
        self.assertEqual(
            ss['reducer'], None)

    def test_default_streaming_jar_and_step_arg_prefix(self):
        ss = self._get_streaming_step(
            dict(type='streaming', mapper=dict(type='script')))

        self.assertEqual(ss['jar'], 'streaming.jar')

        # step_args should be -files script_uri#script_name
        self.assertEqual(len(ss['step_args']), 2)
        self.assertEqual(ss['step_args'][0], '-files')
        self.assertTrue(ss['step_args'][1].endswith('#my_job.py'))

    def test_custom_streaming_jar_and_step_arg_prefix(self):
        # test integration with custom jar options. See
        # StreamingJarAndStepArgPrefixTestCase below.
        EMRJobRunner._get_streaming_jar_and_step_arg_prefix.return_value = (
            ('launch.jar', ['streaming', '-v']))

        ss = self._get_streaming_step(
            dict(type='streaming', mapper=dict(type='script')))

        self.assertEqual(ss['jar'], 'launch.jar')

        # step_args should be -files script_uri#script_name
        self.assertEqual(len(ss['step_args']), 4)
        self.assertEqual(ss['step_args'][:2], ['streaming', '-v'])
        self.assertEqual(ss['step_args'][2], '-files')
        self.assertTrue(ss['step_args'][3].endswith('#my_job.py'))

    def test_hadoop_args_for_step(self):
        self.start(patch(
            'mrjob.emr.EMRJobRunner._hadoop_args_for_step',
            return_value=['-libjars', '/home/hadoop/dora.jar',
                          '-D', 'foo=bar']))

        ss = self._get_streaming_step(
            dict(type='streaming', mapper=dict(type='script')))

        self.assertEqual(ss['jar'], 'streaming.jar')

        # step_args should be -files script_uri#script_name
        self.assertEqual(len(ss['step_args']), 6)
        self.assertEqual(ss['step_args'][0], '-files')
        self.assertTrue(ss['step_args'][1].endswith('#my_job.py'))
        self.assertEqual(
            ss['step_args'][2:],
            ['-libjars', '/home/hadoop/dora.jar',
             '-D', 'foo=bar'])


class LibjarPathsTestCase(MockBotoTestCase):

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


class DefaultPythonBinTestCase(MockBotoTestCase):

    def test_default_ami(self):
        # this tests 4.x AMIs
        runner = EMRJobRunner()
        self.assertTrue(runner._opts['image_version'].startswith('4.'))
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

    def test_2_4_2_ami(self):
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


class StreamingJarAndStepArgPrefixTestCase(MockBotoTestCase):

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


class DeprecatedHadoopStreamingJarOnEMROptionTestCase(MockBotoTestCase):

    def setUp(self):
        super(DeprecatedHadoopStreamingJarOnEMROptionTestCase, self).setUp()
        self.log = self.start(patch('mrjob.emr.log'))

    def assert_deprecation_warning(self):
        self.assertTrue(self.log.warning.called)
        self.assertIn('hadoop_streaming_jar_on_emr is deprecated',
                      self.log.warning.call_args[0][0])

    def test_absolute_path(self):
        runner = EMRJobRunner(hadoop_streaming_jar_on_emr='/fridge/pickle.jar')
        self.assert_deprecation_warning()
        self.assertEqual(runner._opts['hadoop_streaming_jar'],
                         'file:///fridge/pickle.jar')

    def test_relative_path(self):
        runner = EMRJobRunner(hadoop_streaming_jar_on_emr='mason.jar')
        self.assert_deprecation_warning()
        self.assertEqual(runner._opts['hadoop_streaming_jar'],
                         'file://mason.jar')

    def test_dont_override_hadoop_streaming_jar(self):
        runner = EMRJobRunner(hadoop_streaming_jar='s3://bucket/nice.jar',
                              hadoop_streaming_jar_on_emr='/path/to/bad.jar')
        self.assert_deprecation_warning()
        self.assertEqual(runner._opts['hadoop_streaming_jar'],
                         's3://bucket/nice.jar')


class JarStepTestCase(MockBotoTestCase):

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

            emr_conn = runner.make_emr_conn()
            steps = _list_all_steps(emr_conn, runner.get_cluster_id())

            self.assertEqual(len(steps), 1)
            self.assertEqual(steps[0].config.jar, jar_uri)

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

            emr_conn = runner.make_emr_conn()
            steps = _list_all_steps(emr_conn, runner.get_cluster_id())

            self.assertEqual(len(steps), 2)  # adds master node setup

            jar_step = steps[1]
            self.assertEqual(jar_step.config.jar, jar_uri)
            step_args = [a.value for a in jar_step.config.args]

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

            emr_conn = runner.make_emr_conn()
            steps = _list_all_steps(emr_conn, runner.get_cluster_id())

            self.assertEqual(len(steps), 1)
            self.assertEqual(steps[0].config.jar, JAR_URI)

    def test_jar_inside_emr(self):
        job = MRJustAJar(['-r', 'emr', '--jar',
                          'file:///home/hadoop/hadoop-examples.jar'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            emr_conn = runner.make_emr_conn()
            steps = _list_all_steps(emr_conn, runner.get_cluster_id())

            self.assertEqual(len(steps), 1)
            self.assertEqual(steps[0].config.jar,
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

            emr_conn = runner.make_emr_conn()
            steps = _list_all_steps(emr_conn, runner.get_cluster_id())

            self.assertEqual(len(steps), 2)
            jar_step, streaming_step = steps

            # on EMR, the jar gets uploaded
            self.assertEqual(jar_step.config.jar,
                             runner._upload_mgr.uri(fake_jar))

            jar_args = [a.value for a in jar_step.config.args]
            self.assertEqual(len(jar_args), 3)
            self.assertEqual(jar_args[0], 'stuff')

            # check input is interpolated
            input_arg = ','.join(
                runner._upload_mgr.uri(path) for path in (input1, input2))
            self.assertEqual(jar_args[1], input_arg)

            # check output of jar is input of next step
            jar_output_arg = jar_args[2]

            streaming_args = [a.value for a in streaming_step.config.args]
            streaming_input_arg = streaming_args[
                streaming_args.index('-input') + 1]
            self.assertEqual(jar_output_arg, streaming_input_arg)


class SparkStepTestCase(MockBotoTestCase):

    def setUp(self):
        super(SparkStepTestCase, self).setUp()

        # _spark_submit_args() is tested elsewhere
        self.start(patch(
            'mrjob.runner.MRJobRunner._spark_submit_args',
            return_value=['<spark submit args>']))

    # TODO: test warning for for AMIs prior to 3.8.0, which don't offer Spark

    def test_3_x_ami(self):
        job = MRNullSpark(['-r', 'emr', '--ami-version', '3.11.0'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            emr_conn = runner.make_emr_conn()
            steps = _list_all_steps(emr_conn, runner.get_cluster_id())

            self.assertEqual(len(steps), 1)
            self.assertEqual(
                steps[0].config.jar, runner._script_runner_jar_uri())
            self.assertEqual(
                steps[0].config.args[0].value,
                _3_X_SPARK_SUBMIT)

    def test_4_x_ami(self):
        job = MRNullSpark(['-r', 'emr', '--ami-version', '4.7.2'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            emr_conn = runner.make_emr_conn()
            steps = _list_all_steps(emr_conn, runner.get_cluster_id())

            self.assertEqual(len(steps), 1)
            self.assertEqual(
                steps[0].config.jar, _4_X_COMMAND_RUNNER_JAR)
            self.assertEqual(
                steps[0].config.args[0].value, 'spark-submit')

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

            emr_conn = runner.make_emr_conn()
            steps = _list_all_steps(emr_conn, runner.get_cluster_id())

            step_args = [a.value for a in steps[0].config.args]
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


class SparkJarStepTestCase(MockBotoTestCase):

    def setUp(self):
        super(SparkJarStepTestCase, self).setUp()

        self.fake_jar = self.makefile('fake.jar')

        # _spark_submit_args() is tested elsewhere
        self.start(patch(
            'mrjob.runner.MRJobRunner._spark_submit_args',
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

            emr_conn = runner.make_emr_conn()
            steps = _list_all_steps(emr_conn, runner.get_cluster_id())

            self.assertEqual(len(steps), 1)
            step_args = [a.value for a in steps[0].config.args]
            # the first arg is spark-submit and varies by AMI
            self.assertEqual(
                step_args[1:],
                ['<spark submit args>', jar_uri])


class SparkScriptStepTestCase(MockBotoTestCase):
    # a lot of this is already tested in test_runner.py

    def setUp(self):
        super(SparkScriptStepTestCase, self).setUp()

        self.fake_script = self.makefile('fake_script.py')

        # _spark_submit_args() is tested elsewhere
        self.start(patch(
            'mrjob.runner.MRJobRunner._spark_submit_args',
            return_value=['<spark submit args>']))

    def test_script_gets_uploaded(self):
        job = MRSparkScript(['-r', 'emr', '--script', self.fake_script])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            self.assertIn(self.fake_script, runner._upload_mgr.path_to_uri())
            script_uri = runner._upload_mgr.uri(self.fake_script)
            self.assertTrue(runner.fs.ls(script_uri))

            emr_conn = runner.make_emr_conn()
            steps = _list_all_steps(emr_conn, runner.get_cluster_id())

            self.assertEqual(len(steps), 1)
            step_args = [a.value for a in steps[0].config.args]
            # the first arg is spark-submit and varies by AMI
            self.assertEqual(
                step_args[1:],
                ['<spark submit args>', script_uri])

    def test_3_x_ami(self):
        job = MRSparkScript(['-r', 'emr', '--script', self.fake_script,
                             '--ami-version', '3.11.0'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            emr_conn = runner.make_emr_conn()
            steps = _list_all_steps(emr_conn, runner.get_cluster_id())

            self.assertEqual(len(steps), 1)
            self.assertEqual(
                steps[0].config.jar, runner._script_runner_jar_uri())
            self.assertEqual(
                steps[0].config.args[0].value,
                _3_X_SPARK_SUBMIT)

    def test_4_x_ami(self):
        job = MRSparkScript(['-r', 'emr', '--script', self.fake_script,
                             '--ami-version', '4.7.2'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            emr_conn = runner.make_emr_conn()
            steps = _list_all_steps(emr_conn, runner.get_cluster_id())

            self.assertEqual(len(steps), 1)
            self.assertEqual(
                steps[0].config.jar, _4_X_COMMAND_RUNNER_JAR)
            self.assertEqual(
                steps[0].config.args[0].value, 'spark-submit')

    def test_arg_interpolation(self):
        input1 = os.path.join(self.tmp_dir, 'input1')
        open(input1, 'w').close()
        input2 = os.path.join(self.tmp_dir, 'input2')
        open(input2, 'w').close()

        job = MRSparkScript(['-r', 'emr', '--script', self.fake_script,
                             '--script-arg', INPUT,
                             '--script-arg', '-o',
                             '--script-arg', OUTPUT,
                             input1, input2])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            script_uri = runner._upload_mgr.uri(self.fake_script)
            input1_uri = runner._upload_mgr.uri(input1)
            input2_uri = runner._upload_mgr.uri(input2)

            emr_conn = runner.make_emr_conn()
            steps = _list_all_steps(emr_conn, runner.get_cluster_id())

            step_args = [a.value for a in steps[0].config.args]
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


class BuildMasterNodeSetupStep(MockBotoTestCase):

    def test_build_master_node_setup_step(self):
        runner = EMRJobRunner(libjars=['cookie.jar'])
        runner._add_master_node_setup_files_for_upload()

        self.assertIsNotNone(runner._master_node_setup_script_path)
        master_node_setup_uri = runner._upload_mgr.uri(
            runner._master_node_setup_script_path)

        with patch('boto.emr.JarStep', dict):
            step = runner._build_master_node_setup_step()

        self.assertTrue(step['name'].endswith(': Master node setup'))
        self.assertEqual(step['jar'], runner._script_runner_jar_uri())
        self.assertEqual(step['step_args'], [master_node_setup_uri])
        self.assertEqual(step['action_on_failure'],
                         runner._action_on_failure())


class ActionOnFailureTestCase(MockBotoTestCase):

    def test_default(self):
        runner = EMRJobRunner()
        self.assertEqual(runner._action_on_failure(),
                         'TERMINATE_CLUSTER')

    def test_default_with_cluster_id(self):
        runner = EMRJobRunner(cluster_id='j-CLUSTER')
        self.assertEqual(runner._action_on_failure(),
                         'CANCEL_AND_WAIT')

    def test_default_with_pooling(self):
        runner = EMRJobRunner(pool_clusters=True)
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


class MultiPartUploadTestCase(MockBotoTestCase):

    PART_SIZE_IN_MB = 50.0 / 1024 / 1024
    TEST_BUCKET = 'walrus'
    TEST_FILENAME = 'data.dat'
    TEST_S3_URI = 's3://%s/%s' % (TEST_BUCKET, TEST_FILENAME)

    def setUp(self):
        super(MultiPartUploadTestCase, self).setUp()
        # create the walrus bucket
        self.add_mock_s3_data({self.TEST_BUCKET: {}})

    def upload_data(self, runner, data):
        """Upload some bytes to S3"""
        data_path = os.path.join(self.tmp_dir, self.TEST_FILENAME)
        with open(data_path, 'wb') as fp:
            fp.write(data)

        runner._upload_contents(self.TEST_S3_URI, data_path)

    def assert_upload_succeeds(self, runner, data, expect_multipart):
        """Write the data to a temp file, and then upload it to (mock) S3,
        checking that the data successfully uploaded."""
        with patch.object(runner, '_upload_parts', wraps=runner._upload_parts):
            self.upload_data(runner, data)

            s3_key = runner.fs.get_s3_key(self.TEST_S3_URI)
            self.assertEqual(s3_key.get_contents_as_string(), data)
            self.assertEqual(runner._upload_parts.called, expect_multipart)

    def test_small_file(self):
        runner = EMRJobRunner()
        data = b'beavers mate for life'

        self.assert_upload_succeeds(runner, data, expect_multipart=False)

    @skipIf(filechunkio is None, 'need filechunkio')
    def test_large_file(self):
        # Real S3 has a minimum chunk size of 5MB, but I'd rather not
        # store that in memory (in our mock S3 filesystem)
        runner = EMRJobRunner(cloud_upload_part_size=self.PART_SIZE_IN_MB)
        self.assertEqual(runner._get_upload_part_size(), 50)

        data = b'Mew' * 20
        self.assert_upload_succeeds(runner, data, expect_multipart=True)

    def test_file_size_equals_part_size(self):
        runner = EMRJobRunner(cloud_upload_part_size=self.PART_SIZE_IN_MB)
        self.assertEqual(runner._get_upload_part_size(), 50)

        data = b'o' * 50
        self.assert_upload_succeeds(runner, data, expect_multipart=False)

    def test_disable_multipart(self):
        runner = EMRJobRunner(cloud_upload_part_size=0)
        self.assertEqual(runner._get_upload_part_size(), 0)

        data = b'Mew' * 20
        self.assert_upload_succeeds(runner, data, expect_multipart=False)

    def test_no_filechunkio(self):
        with patch.object(mrjob.emr, 'filechunkio', None):
            runner = EMRJobRunner(cloud_upload_part_size=self.PART_SIZE_IN_MB)
            self.assertEqual(runner._get_upload_part_size(), 50)

            data = b'Mew' * 20
            with logger_disabled('mrjob.emr'):
                self.assert_upload_succeeds(runner, data,
                                            expect_multipart=False)

    @skipIf(filechunkio is None, 'need filechunkio')
    def test_exception_while_uploading_large_file(self):

        runner = EMRJobRunner(cloud_upload_part_size=self.PART_SIZE_IN_MB)
        self.assertEqual(runner._get_upload_part_size(), 50)

        data = b'Mew' * 20

        with patch.object(runner, '_upload_parts', side_effect=IOError):
            self.assertRaises(IOError, self.upload_data, runner, data)

            s3_key = runner.fs.get_s3_key(self.TEST_S3_URI)
            self.assertTrue(s3_key.mock_multipart_upload_was_cancelled())


class SessionTokenTestCase(MockBotoTestCase):

    def setUp(self):
        super(SessionTokenTestCase, self).setUp()

        self.mock_emr = self.start(patch('boto.emr.connection.EmrConnection'))
        self.mock_iam = self.start(patch('boto.connect_iam'))

        # runner needs to do stuff with S3 on initialization
        self.mock_s3 = self.start(patch('boto.connect_s3',
                                        wraps=boto.connect_s3))

    def assert_conns_use_session_token(self, runner, session_token):
        runner.make_emr_conn()

        self.assertTrue(self.mock_emr.called)
        emr_kwargs = self.mock_emr.call_args[1]
        self.assertIn('security_token', emr_kwargs)
        self.assertEqual(emr_kwargs['security_token'], session_token)

        runner.make_iam_conn()

        self.assertTrue(self.mock_iam.called)
        iam_kwargs = self.mock_iam.call_args[1]
        self.assertIn('security_token', iam_kwargs)
        self.assertEqual(iam_kwargs['security_token'], session_token)

        runner.fs.make_s3_conn()

        self.assertTrue(self.mock_s3.called)
        s3_kwargs = self.mock_s3.call_args[1]
        self.assertIn('security_token', s3_kwargs)
        self.assertEqual(s3_kwargs['security_token'], session_token)

    def test_connections_without_session_token(self):
        runner = EMRJobRunner()

        self.assert_conns_use_session_token(runner, None)

    def test_connections_with_session_token(self):
        runner = EMRJobRunner(aws_session_token='meow')

        self.assert_conns_use_session_token(runner, 'meow')


class BootstrapPythonTestCase(MockBotoTestCase):

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


class BootstrapSparkTestCase(MockBotoTestCase):

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

        return any(ba.scriptpath == uri for ba in cluster._bootstrapactions)

    def installed_spark_application(self, cluster, name='Spark'):
        return any(a.name == name for a in cluster.applications)

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


class ShouldBootstrapSparkTestCase(MockBotoTestCase):

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


class EMRTagsTestCase(MockBotoTestCase):

    def test_tags_option_dict(self):
        job = MRWordCount([
            '-r', 'emr',
            '--tag', 'tag_one=foo',
            '--tag', 'tag_two=bar'])

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

        with mrjob_conf_patcher(TAGS_MRJOB_CONF):
            with job.make_runner() as runner:
                self.assertEqual(runner._opts['tags'],
                                 {'tag_one': 'foo',
                                  'tag_two': 'qwerty',
                                  'tag_three': 'bar'})

    def test_tags_get_created(self):
        cluster = self.run_and_get_cluster('--tag', 'tag_one=foo',
                                           '--tag', 'tag_two=bar')

        # tags should be in alphabetical order by key
        self.assertEqual(cluster.tags, [
            MockEmrObject(key='tag_one', value='foo'),
            MockEmrObject(key='tag_two', value='bar'),
        ])

    def test_blank_tag_value(self):
        cluster = self.run_and_get_cluster('--tag', 'tag_one=foo',
                                           '--tag', 'tag_two=')

        # tags should be in alphabetical order by key
        self.assertEqual(cluster.tags, [
            MockEmrObject(key='tag_one', value='foo'),
            MockEmrObject(key='tag_two', value=''),
        ])

    def test_tag_values_can_be_none(self):
        runner = EMRJobRunner(conf_paths=[], tags={'tag_one': None})
        cluster_id = runner.make_persistent_cluster()

        mock_cluster = self.mock_emr_clusters[cluster_id]
        self.assertEqual(mock_cluster.tags, [
            MockEmrObject(key='tag_one', value=''),
        ])

    def test_persistent_cluster(self):
        args = ['--tag', 'tag_one=foo',
                '--tag', 'tag_two=bar']

        with self.make_runner(*args) as runner:
            cluster_id = runner.make_persistent_cluster()

        mock_cluster = self.mock_emr_clusters[cluster_id]
        self.assertEqual(mock_cluster.tags, [
            MockEmrObject(key='tag_one', value='foo'),
            MockEmrObject(key='tag_two', value='bar'),
        ])


class IAMEndpointTestCase(MockBotoTestCase):

    def test_default(self):
        runner = EMRJobRunner()

        iam_conn = runner.make_iam_conn()
        self.assertEqual(iam_conn.host, 'iam.amazonaws.com')

    def test_explicit_iam_endpoint(self):
        runner = EMRJobRunner(iam_endpoint='iam.us-gov.amazonaws.com')

        iam_conn = runner.make_iam_conn()
        self.assertEqual(iam_conn.host, 'iam.us-gov.amazonaws.com')

    def test_iam_endpoint_option(self):
        mr_job = MRJob(
            ['-r', 'emr', '--iam-endpoint', 'iam.us-gov.amazonaws.com'])

        with mr_job.make_runner() as runner:
            iam_conn = runner.make_iam_conn()
            self.assertEqual(iam_conn.host, 'iam.us-gov.amazonaws.com')


class SetupLineEncodingTestCase(MockBotoTestCase):

    def test_setup_wrapper_script_uses_local_line_endings(self):
        job = MRTwoStepJob(['-r', 'emr', '--setup', 'true'])
        job.sandbox(stdin=BytesIO(b'foo\nbar\n'))

        # tests #1071. Unfortunately, we mostly run these tests on machines
        # that use unix line endings anyway. So monitor open() instead
        with patch(
                'mrjob.runner.open', create=True, side_effect=open) as m_open:
            with logger_disabled('mrjob.emr'):
                with job.make_runner() as runner:
                    runner.run()

                    self.assertIn(
                        call(runner._setup_wrapper_script_path, 'wb'),
                        m_open.mock_calls)


class WaitForLogsOnS3TestCase(MockBotoTestCase):

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
        state = self.cluster.status.state
        waited = set(self.runner._waited_for_logs_on_s3)

        self.runner._wait_for_logs_on_s3()

        self.assertFalse(self.mock_log.info.called)
        self.assertEqual(waited, self.runner._waited_for_logs_on_s3)
        self.assertEqual(self.runner._describe_cluster().status.state, state)

    def test_starting(self):
        self.cluster.status.state = 'STARTING'
        self.assert_waits_ten_minutes()

    def test_bootstrapping(self):
        self.cluster.status.state = 'BOOTSTRAPPING'
        self.assert_waits_ten_minutes()

    def test_running(self):
        self.cluster.status.state = 'RUNNING'
        self.assert_waits_ten_minutes()

    def test_waiting(self):
        self.cluster.status.state = 'WAITING'
        self.assert_waits_ten_minutes()

    def test_terminating(self):
        self.cluster.status.state = 'TERMINATING'
        self.cluster.delay_progress_simulation = 1

        self.runner._wait_for_logs_on_s3()

        self.assertEqual(self.runner._describe_cluster().status.state,
                         'TERMINATED')
        self.assertTrue(self.mock_log.info.called)

    def test_terminated(self):
        self.cluster.status.state = 'TERMINATED'
        self.assert_silently_exits()

    def test_terminated_with_errors(self):
        self.cluster.status.state = 'TERMINATED_WITH_ERRORS'
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


class StreamLogDirsTestCase(MockBotoTestCase):

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


class LsStepSyslogsTestCase(MockBotoTestCase):

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


class LsStepStderrLogsTestCase(MockBotoTestCase):

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


class GetStepLogInterpretationTestCase(MockBotoTestCase):

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
class HadoopExtraArgsOnEMRTestCase(HadoopExtraArgsTestCase, MockBotoTestCase):

    def setUp(self):
        super(HadoopExtraArgsTestCase, self).setUp()

        self.start(patch(
            'mrjob.emr.EMRJobRunner.get_hadoop_version',
            return_value='2.4.0'))

    RUNNER = 'emr'


# make sure we don't override the partitioner on EMR (tests #1294)
class PartitionerTestCase(MockBotoTestCase):

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

    def test_switch_overrides_sort_values(self):
        job = MRSortValues(['-r', 'emr', '--partitioner', 'java.lang.Object'])

        with job.make_runner() as runner:
            self.assertEqual(
                runner._hadoop_args_for_step(0), [
                    '-D', 'mapred.text.key.partitioner.options=-k1,1',
                    '-D', 'stream.num.map.output.key.fields=2',
                    '-partitioner', 'java.lang.Object',
                ])


class EMRApplicationsTestCase(MockBotoTestCase):

    def test_default_on_3_x_ami(self):
        job = MRTwoStepJob(['-r', 'emr', '--image-version', '3.11.0'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner._applications(), set())

            runner._launch()
            cluster = runner._describe_cluster()

            applications = set(a.name for a in cluster.applications)
            self.assertEqual(applications, set(['hadoop']))

    def test_default_on_4_x_ami(self):
        job = MRTwoStepJob(['-r', 'emr', '--image-version', '4.3.0'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner._applications(), set())

            runner._launch()
            cluster = runner._describe_cluster()

            applications = set(a.name for a in cluster.applications)
            self.assertEqual(applications, set(['Hadoop']))

    def test_applications_requires_4_x_ami(self):
        job = MRTwoStepJob(
            ['-r', 'emr',
             '--image-version', '3.11.0',
             '--application', 'Hadoop',
             '--application', 'Mahout'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertRaises(boto.exception.EmrResponseError, runner._launch)

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

            applications = set(a.name for a in cluster.applications)
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

            applications = set(a.name for a in cluster.applications)
            self.assertEqual(applications,
                             set(['Hadoop', 'Mahout']))

    def test_api_param_serialization(self):
        job = MRTwoStepJob(
            ['-r', 'emr',
             '--application', 'Hadoop',
             '--application', 'Mahout'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()
            cluster = runner._describe_cluster()

            self.assertIn('Applications.member.1.Name', cluster._api_params)
            self.assertIn('Applications.member.2.Name', cluster._api_params)
            self.assertNotIn('Applications.member.0.Name', cluster._api_params)


class EMRConfigurationsTestCase(MockBotoTestCase):

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
            self.assertRaises(boto.exception.EmrResponseError, runner._launch)

    def _test_normalized_emr_configurations(
            self, emr_configurations, expected_api_response=None):

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

            if expected_api_response:
                self.assertEqual(cluster.configurations, expected_api_response)

            self.assertEqual(
                _decode_configurations_from_api(cluster.configurations),
                emr_configurations)

    def test_basic_emr_configuration(self, raw=None):
        self._test_normalized_emr_configurations(
            [CORE_SITE_EMR_CONFIGURATION],
            [
                MockEmrObject(
                    classification='core-site',
                    properties=[
                        MockEmrObject(
                            key='hadoop.security.groups.cache.secs',
                            value='250',
                        ),
                    ],
                ),
            ])

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


class JobStepsTestCase(MockBotoTestCase):

    def setUp(self):
        super(JobStepsTestCase, self).setUp()
        self.start(patch.object(MockEmrConnection, 'list_steps',
                                side_effect=MockEmrConnection.list_steps,
                                autospec=True))

    def test_empty(self):
        runner = EMRJobRunner()
        runner.make_persistent_cluster()

        self.assertEqual(runner._job_steps(max_steps=0), [])
        self.assertEqual(MockEmrConnection.list_steps.call_count, 0)

    def test_own_cluster(self):
        job = MRTwoStepJob(['-r', 'emr']).sandbox()

        with job.make_runner() as runner:
            runner._launch()

            job_steps = runner._job_steps(max_steps=2)

            self.assertEqual(len(job_steps), 2)

            # ensure that steps appear in correct order (see #1316)
            self.assertIn('Step 1', job_steps[0].name)
            self.assertIn('Step 2', job_steps[1].name)

            self.assertEqual(MockEmrConnection.list_steps.call_count, 1)

    def test_shared_cluster(self):
        cluster_id = EMRJobRunner().make_persistent_cluster()

        def add_other_steps(n):
            for _ in range(n):
                self.mock_emr_clusters[cluster_id]._steps.append(
                    MockEmrObject(id='s-NONE', name=''))

        job = MRTwoStepJob(['-r', 'emr', '--cluster-id', cluster_id]).sandbox()

        with job.make_runner() as runner:
            add_other_steps(n=DEFAULT_MAX_STEPS_RETURNED)
            runner._launch()
            add_other_steps(n=3)

            # this test won't work if pages of steps are really small
            assert(DEFAULT_MAX_STEPS_RETURNED >= 5)

            job_steps = runner._job_steps(max_steps=2)

            self.assertEqual(len(job_steps), 2)

            # ensure that steps appear in correct order (see #1316)
            self.assertIn('Step 1', job_steps[0].name)
            self.assertIn('Step 2', job_steps[1].name)

            # ensure that steps are for correct job
            self.assertTrue(job_steps[0].name.startswith(runner._job_key))
            self.assertTrue(job_steps[1].name.startswith(runner._job_key))

            # this should have only taken one call to list_steps(),
            # thanks to pagination
            self.assertEqual(MockEmrConnection.list_steps.call_count, 1)

            # would take two calls to list all the steps
            MockEmrConnection.list_steps.reset_mock()

            job_steps = runner._job_steps()
            self.assertEqual(len(job_steps), 2)
            self.assertEqual(MockEmrConnection.list_steps.call_count, 2)


class WaitForStepsToCompleteTestCase(MockBotoTestCase):

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

        mock_cluster = runner._describe_cluster()
        mock_steps = mock_cluster._steps

        self.assertEqual(len(mock_steps), 2)
        self.assertEqual(mock_steps[0].status.state, 'RUNNING')

    def test_open_ssh_tunnel_if_cluster_running(self):
        # tests #1115

        # stop the test as soon as SSH tunnel is set up
        EMRJobRunner._set_up_ssh_tunnel.side_effect = self.StopTest

        runner = self.make_runner()
        mock_cluster = runner._describe_cluster()
        mock_cluster.status.state = 'RUNNING'

        # run until SSH tunnel is set up
        self.assertRaises(self.StopTest, runner._wait_for_steps_to_complete)

        self.assertFalse(EMRJobRunner._wait_for_step_to_complete.called)

    def test_open_ssh_tunnel_if_cluster_waiting(self):
        # tests #1115

        # stop the test as soon as SSH tunnel is set up
        EMRJobRunner._set_up_ssh_tunnel.side_effect = self.StopTest

        runner = self.make_runner()
        mock_cluster = runner._describe_cluster()
        mock_cluster.status.state = 'WAITING'

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

        mock_cluster = runner._describe_cluster()
        mock_steps = mock_cluster._steps

        # sanity-check: are steps from the previous cluster on there?
        self.assertEqual(len(mock_steps), 4)

        # run until ssh tunnel is called
        self.assertRaises(self.StopTest, runner._wait_for_steps_to_complete)

        # should have only waited for first step
        self.assertEqual(EMRJobRunner._wait_for_step_to_complete.call_count, 1)

        # cluster should be running, step should still be pending
        self.assertEqual(mock_cluster.status.state, 'RUNNING')
        self.assertIn(runner._job_key, mock_steps[2].name)
        self.assertEqual(mock_steps[2].status.state, 'PENDING')

    def test_terminated_cluster(self):
        runner = self.make_runner()

        self.start(patch(
            'mrjob.emr._patched_describe_step',
            return_value=MockEmrObject(
                status=MockEmrObject(
                    state='CANCELLED',
                ),
            ),
        ))

        self.start(patch(
            'mrjob.emr._patched_describe_cluster',
            return_value=MockEmrObject(
                id='j-CLUSTERID',
                status=MockEmrObject(
                    state='TERMINATING',
                ),
            ),
        ))

        self.start(patch.object(
            runner, '_check_for_missing_default_iam_roles'))
        self.start(patch.object(
            runner, '_check_for_key_pair_from_wrong_region'))
        self.start(patch.object(
            runner, '_check_for_failed_bootstrap_action',
            side_effect=self.StopTest))

        self.assertRaises(self.StopTest,
                          runner._wait_for_steps_to_complete)

        self.assertTrue(runner._check_for_missing_default_iam_roles.called)
        self.assertTrue(runner._check_for_key_pair_from_wrong_region.called)
        self.assertTrue(runner._check_for_failed_bootstrap_action.called)


class LsBootstrapStderrLogsTestCase(MockBotoTestCase):

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


class CheckForFailedBootstrapActionTestCase(MockBotoTestCase):

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


class UseSudoOverSshTestCase(MockBotoTestCase):

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


class MasterPrivateIPTestCase(MockBotoTestCase):

    # logic for runner._master_private_ip()

    def test_master_private_ip(self):
        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            # no cluster yet
            self.assertRaises(AssertionError, runner._master_private_ip)

            runner._launch()

            self.assertIsNone(runner._master_private_ip())

            self.connect_emr().simulate_progress(runner.get_cluster_id())
            self.assertIsNotNone(runner._master_private_ip())


class SetUpSSHTunnelTestCase(MockBotoTestCase):

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
            while cluster.status.state in ('STARTING', 'BOOTSTRAPPING'):
                self.connect_emr().simulate_progress(cluster_id)

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
        # test things that don't depend on AMI
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


class UsesSparkTestCase(MockBotoTestCase):

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
                            '--ami-version', '4.0.0',
                            '--application', 'Spark'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertTrue(runner._uses_spark())
            self.assertTrue(runner._has_spark_application())

    def test_spark_application_lowercase(self):
        job = MRTwoStepJob(['-r', 'emr',
                            '--ami-version', '4.0.0',
                            '--application', 'spark'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertTrue(runner._uses_spark())
            self.assertTrue(runner._has_spark_application())

    def test_other_application(self):
        job = MRTwoStepJob(['-r', 'emr',
                            '--ami-version', '4.0.0',
                            '--application', 'Mahout'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertFalse(runner._uses_spark())
            self.assertFalse(runner._has_spark_application())

    def test_spark_and_other_application(self):
        job = MRTwoStepJob(['-r', 'emr',
                            '--ami-version', '4.0.0',
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
                            '--ami-version', '4.0.0',
                            '--emr-api-param',
                            'Application.member.1.Name=Spark'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertFalse(runner._uses_spark())
            self.assertFalse(runner._has_spark_application())


class SparkPyFilesTestCase(MockBotoTestCase):

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


class DeprecatedAMIVersionKeywordOptionTestCase(MockBotoTestCase):
    # regression test for #1421

    def test_ami_version_4_0_0(self):
        runner = EMRJobRunner(ami_version='4.0.0')
        runner.make_persistent_cluster()

        self.assertEqual(runner.get_image_version(), '4.0.0')

        cluster = runner._describe_cluster()
        self.assertEqual(cluster.releaselabel, 'emr-4.0.0')
        self.assertFalse(hasattr(cluster, 'runningamiversion'))

        self.assertEqual(runner._opts['image_version'], '4.0.0')
        self.assertEqual(runner._opts['release_label'], 'emr-4.0.0')


class TestClusterSparkSupportWarning(MockBotoTestCase):

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

    def test_cant_list_instance_groups(self):
        job = MRNullSpark(['-r', 'emr', '--image-version', '4.0.0'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()

            with patch.object(MockEmrConnection, 'list_instance_groups',
                              side_effect=INSTANCE_FLEETS_ERROR):
                message = runner._cluster_spark_support_warning()
                self.assertIsNone(message)


class ImageVersionGteTestCase(MockBotoTestCase):

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


class SparkSubmitArgPrefixTestCase(MockBotoTestCase):

    def test_default(self):
        # these are hard-coded and always the same
        runner = EMRJobRunner()

        self.assertEqual(
            runner._spark_submit_arg_prefix(),
            ['--master', 'yarn', '--deploy-mode', 'cluster'])


class SSHWorkerHostsTestCase(MockBotoTestCase):

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


class BadBashWorkaroundTestCase(MockBotoTestCase):
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


class LogProgressTestCase(MockBotoTestCase):

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


class ProgressHtmlFromTunnelTestCase(MockBotoTestCase):

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


class ProgressHtmlOverSshTestCase(MockBotoTestCase):

    MOCK_MASTER = 'mockmaster'
    MOCK_JOB_TRACKER_URL = 'http://1.2.3.4:8088/cluster'

    MOCK_EC2_KEY_PAIR_FILE = 'mock.pem'

    def setUp(self):
        super(ProgressHtmlOverSshTestCase, self).setUp()

        self._ssh_run = self.start(patch('mrjob.emr._ssh_run',
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
            ['ssh'],
            self.MOCK_MASTER,
            self.MOCK_EC2_KEY_PAIR_FILE,
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
