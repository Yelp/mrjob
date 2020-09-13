# -*- coding: utf-8 -*-
# Copyright 2009-2016 Yelp and Contributors
# Copyright 2017-2019 Yelp
# Copyright 2020 Affirm, Inc.
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
"""Tests of EMRJobRunner's cluster pooling."""
import json
import os
from datetime import datetime
from datetime import timedelta
from os.path import join
from shutil import make_archive
from time import sleep

from mrjob.aws import _boto3_now
from mrjob.emr import EMRJobRunner
from mrjob.emr import PoolTimeoutException
from mrjob.emr import _3_X_SPARK_BOOTSTRAP_ACTION
from mrjob.emr import _POOLING_SLEEP_INTERVAL
from mrjob.pool import _attempt_to_lock_cluster
from mrjob.pool import _attempt_to_unlock_cluster
from mrjob.pool import _pool_name
from mrjob.step import StepFailedException

from tests.mock_boto3 import MockBoto3TestCase
from tests.mr_null_spark import MRNullSpark
from tests.mr_two_step_job import MRTwoStepJob
from tests.py2 import Mock
from tests.py2 import call
from tests.py2 import patch
from tests.sandbox import mrjob_conf_patcher
from tests.test_emr import CORE_SITE_EMR_CONFIGURATION
from tests.test_emr import HADOOP_ENV_EMR_CONFIGURATION


class PoolWaitMinutesOptionTestCase(MockBoto3TestCase):

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


class PoolMatchingBaseTestCase(MockBoto3TestCase):

    def make_pooled_cluster(self, name=None, minutes_ago=0,
                            provision=True,
                            normalized_instance_hours=None,
                            **kwargs):
        """Returns ``(runner, cluster_id)``. Set minutes_ago to set
        ``cluster.startdatetime`` to seconds before
        ``datetime.datetime.now()``."""
        runner = EMRJobRunner(pool_clusters=True,
                              pool_name=name,
                              **kwargs)
        cluster_id = runner.make_persistent_cluster()
        mock_cluster = self.mock_emr_clusters[cluster_id]

        # poor man's version of simulating cluster progress
        mock_cluster['Status']['State'] = 'WAITING'
        # minutes_ago is only used in one test, which relies on ReadyDateTime,
        # so setting CreationDateTime and ReadyDateTime both
        mock_cluster['Status']['Timeline']['CreationDateTime'] = (
            _boto3_now() - timedelta(minutes=minutes_ago))
        mock_cluster['Status']['Timeline']['ReadyDateTime'] = (
            _boto3_now() - timedelta(minutes=minutes_ago))
        mock_cluster['MasterPublicDnsName'] = 'mockmaster'

        # set normalized_instance_hours if requested
        if normalized_instance_hours is not None:
            mock_cluster['NormalizedInstanceHours'] = normalized_instance_hours

        # instance fleets cares about provisioned instances
        if provision:
            if mock_cluster['InstanceCollectionType'] == 'INSTANCE_GROUP':
                for ig in mock_cluster['_InstanceGroups']:
                    ig['RunningInstanceCount'] = ig['RequestedInstanceCount']
            elif mock_cluster['InstanceCollectionType'] == 'INSTANCE_FLEET':
                for fleet in mock_cluster['_InstanceFleets']:
                    fleet['ProvisionedOnDemandCapacity'] = fleet[
                        'TargetOnDemandCapacity']
                    fleet['ProvisionedSpotCapacity'] = fleet[
                        'TargetSpotCapacity']

        return runner, cluster_id


    def _fleet_config(
            self, role='MASTER', instance_types=None,
            weighted_capacities=None,
            ebs_device_configs=None,
            ebs_optimized=None,
            on_demand_capacity=1, spot_capacity=0, spot_spec=None):

        config = dict(InstanceFleetType=role, InstanceTypeConfigs=[])

        if not instance_types:
            instance_types = ['m1.medium']

        if not weighted_capacities:
            weighted_capacities = {}

        for instance_type in instance_types:
            instance_config = dict(InstanceType=instance_type)
            if weighted_capacities.get(instance_type):
                instance_config['WeightedCapacity'] = (
                    weighted_capacities[instance_type])

            EbsConfiguration = {}

            if ebs_device_configs is not None:
                EbsConfiguration['EbsBlockDeviceConfigs'] = ebs_device_configs

            if ebs_optimized is not None:
                EbsConfiguration['EbsOptimized'] = ebs_optimized

            if EbsConfiguration:
                instance_config['EbsConfiguration'] = EbsConfiguration

            config['InstanceTypeConfigs'].append(instance_config)

        if spot_spec:
            config['LaunchSpecifications'] = dict(SpotSpecification=spot_spec)

        if on_demand_capacity:
            config['TargetOnDemandCapacity'] = on_demand_capacity

        if spot_capacity:
            config['TargetSpotCapacity'] = spot_capacity

        return config

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
        emr_client = EMRJobRunner(conf_paths=[]).make_emr_client()
        emr_client.terminate_job_flows(JobFlowIds=[actual_cluster_id])

    def make_simple_runner(self, pool_name, *args):
        """Make an EMRJobRunner that is ready to try to find a pool to join"""
        mr_job = MRTwoStepJob([
            '-r', 'emr', '-v', '--pool-clusters',
            '--pool-name', pool_name] + list(args))
        mr_job.sandbox()
        runner = mr_job.make_runner()
        self.prepare_runner_for_ssh(runner)
        runner._prepare_for_launch()
        return runner


class BasicPoolMatchingTestCase(PoolMatchingBaseTestCase):

    def test_make_new_pooled_cluster(self):
        mr_job = MRTwoStepJob(['-r', 'emr', '-v', '--pool-clusters'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            self.prepare_runner_for_ssh(runner)
            runner.run()

            # Make sure that the runner made a pooling-enabled cluster
            cluster = runner._describe_cluster()
            self.assertEqual(_pool_name(cluster), runner._opts['pool_name'])

            self.simulate_emr_progress(runner.get_cluster_id())

            cluster = runner._describe_cluster()
            self.assertEqual(cluster['Status']['State'], 'WAITING')

    def test_join_pooled_cluster(self):
        _, cluster_id = self.make_pooled_cluster()

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters'])

    def test_join_named_pool(self):
        _, cluster_id = self.make_pooled_cluster('pool1')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--pool-name', 'pool1'])

    def test_dont_join_wrong_named_pool(self):
        _, cluster_id = self.make_pooled_cluster('pool1')

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--pool-name', 'not_pool1'])

    def test_join_anyway_if_i_say_so(self):
        _, cluster_id = self.make_pooled_cluster(image_version='2.0')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--cluster-id', cluster_id,
            '--image-version', '2.2'])


class ConcurrentStepsPoolMatchingTestCase(PoolMatchingBaseTestCase):

    def test_add_batch_in_steps_does_not_affect_pooling(self):
        _, cluster_id = self.make_pooled_cluster()

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters', '--add-steps-in-batch'])

    def test_same_max_concurrent_steps(self):
        _, cluster_id = self.make_pooled_cluster(
            max_concurrent_steps=3)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters', '--max-concurrent-steps', '3'])

    def test_dont_join_cluster_with_higher_concurrency(self):
        _, cluster_id = self.make_pooled_cluster(
            max_concurrent_steps=4)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters', '--max-concurrent-steps', '3'])

    def test_join_cluster_with_lower_concurrency(self):
        _, cluster_id = self.make_pooled_cluster(
            max_concurrent_steps=2)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters', '--max-concurrent-steps', '3'])

    def test_non_concurrent_cluster_okay(self):
        _, cluster_id = self.make_pooled_cluster(
            max_concurrent_steps=1)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters', '--max-concurrent-steps', '3'])


class AMIPoolMatchingTestCase(PoolMatchingBaseTestCase):

    def test_pooling_with_image_version(self):
        _, cluster_id = self.make_pooled_cluster(image_version='2.4.9')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--image-version', '2.4.9'])

    def test_pooling_requires_exact_image_version_match(self):
        _, cluster_id = self.make_pooled_cluster(image_version='2.4.9')

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--image-version', '2.4'])

    def test_partial_image_version_okay(self):
        # the 2.x series is over, so "2.4" is always the same
        # patch version anyhow
        _, cluster_id = self.make_pooled_cluster(image_version='2.4')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--image-version', '2.4'])

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

    def test_pooling_with_custom_ami(self):
        _, cluster_id = self.make_pooled_cluster(image_id='ami-blanchin')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--image-id', 'ami-blanchin'])

    def test_dont_join_pool_with_wrong_custom_ami(self):
        _, cluster_id = self.make_pooled_cluster(image_id='ami-blanchin')

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--image-id', 'ami-awake'])

    def test_dont_join_pool_with_non_custom_ami(self):
        _, cluster_id = self.make_pooled_cluster()

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--image-id', 'ami-blanchin'])

    def test_dont_join_pool_with_custom_ami_if_not_set(self):
        _, cluster_id = self.make_pooled_cluster(image_id='ami-blanchin')

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters'])

    def test_join_pool_with_matching_custom_ami_and_ami_version(self):
        _, cluster_id = self.make_pooled_cluster(image_id='ami-blanchin',
                                                 image_version='5.10.0')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--image-id', 'ami-blanchin', '--release-label', 'emr-5.10.0'])

    def test_dont_join_pool_with_right_custom_ami_but_wrong_version(self):
        _, cluster_id = self.make_pooled_cluster(image_id='ami-blanchin',
                                                 image_version='5.9.0')

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--image-id', 'ami-blanchin', '--image-version', '5.10.0'])


class ApplicationsPoolMatchingTestCase(PoolMatchingBaseTestCase):

    def test_matching_applications(self):
        _, cluster_id = self.make_pooled_cluster(
            image_version='4.0.0', applications=['Mahout'])

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--image-version', '4.0.0',
            '--application', 'Mahout'])

    def test_extra_applications_not_okay(self):
        _, cluster_id = self.make_pooled_cluster(
            image_version='4.0.0', applications=['Ganglia', 'Mahout'])

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--image-version', '4.0.0',
            '--application', 'Mahout'])

    def test_missing_applications_not_okay(self):
        _, cluster_id = self.make_pooled_cluster(
            image_version='4.0.0', applications=['Mahout'])

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--image-version', '4.0.0',
            '--applications', 'Ganglia,Mahout'])

    def test_application_matching_is_case_insensitive(self):
        _, cluster_id = self.make_pooled_cluster(
            image_version='4.0.0', applications=['Mahout'])

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--image-version', '4.0.0',
            '--application', 'mahout'])


class EMRConfigurationPoolMatchingTestCase(PoolMatchingBaseTestCase):

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


class SubnetPoolMatchingTestCase(PoolMatchingBaseTestCase):

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

    def test_list_of_subnets(self):
        # subnets only works with instance fleets
        fleets = [self._fleet_config()]

        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=fleets, subnet='subnet-eeeeeeee')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--instance-fleets', json.dumps(fleets),
            '--subnets', 'subnet-eeeeeeee,subnet-ffffffff'])


class AdditionalEMRInfoPoolMatchingTestCase(PoolMatchingBaseTestCase):

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


class InstancePoolMatchingTestCase(PoolMatchingBaseTestCase):

    def test_join_pool_with_same_instance_type_and_count(self):
        _, cluster_id = self.make_pooled_cluster(
            instance_type='m2.4xlarge',
            num_core_instances=20)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-type', 'm2.4xlarge',
            '--num-core-instances', '20'])

    def test_join_pool_with_same_instance_groups(self):
        INSTANCE_GROUPS = [
            dict(
                InstanceRole='MASTER',
                InstanceCount=1,
                InstanceType='m1.medium',
            ),
            dict(
                InstanceRole='CORE',
                InstanceCount=20,
                InstanceType='m2.4xlarge',
            ),
        ]

        _, cluster_id = self.make_pooled_cluster(
            instance_groups=INSTANCE_GROUPS)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(INSTANCE_GROUPS)])

    def test_instance_groups_match_instance_type_and_count(self):
        # setting instance type and count is just a shorthand
        # for --instance-groups

        INSTANCE_GROUPS = [
            dict(
                InstanceRole='MASTER',
                InstanceCount=1,
                InstanceType='m1.medium',
            ),
            dict(
                InstanceRole='CORE',
                InstanceCount=20,
                InstanceType='m2.4xlarge',
            ),
        ]

        _, cluster_id = self.make_pooled_cluster(
            instance_type='m2.4xlarge',
            num_core_instances=20)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(INSTANCE_GROUPS)])

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

    # tests of what happens when user specifies a single master and
    # pooling tries to join clusters with core and/or task instances

    def test_master_alone_joins_master_and_core(self):
        _, cluster_id = self.make_pooled_cluster(
            num_core_instances=2)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters'])

    def test_master_alone_requires_big_enough_core_instances(self):
        _, cluster_id = self.make_pooled_cluster(
            master_instance_type='c3.4xlarge',
            num_core_instances=2)  # core instances are m5.xlarge

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--master-instance-type', 'c3.4xlarge'])

    def test_master_alone_requires_big_enough_master_when_with_core(self):
        _, cluster_id = self.make_pooled_cluster(
            core_instance_type='c1.xlarge',
            num_core_instances=2)  # master instances are m5.xlarge

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--master-instance-type', 'c1.xlarge'])

    def test_master_alone_accepts_master_core_task(self):
        _, cluster_id = self.make_pooled_cluster(
            num_core_instances=2,
            num_task_instances=2)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters'])

    def test_master_alone_does_not_accept_too_small_task_instances(self):
        _, cluster_id = self.make_pooled_cluster(
            master_instance_type='c1.xlarge',
            core_instance_type='c1.xlarge',
            task_instance_type='m1.medium',
            num_core_instances=2,
            num_task_instances=2)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--master-instance-type', 'c1.xlarge'])

    def test_accept_extra_task_instances(self):
        _, cluster_id = self.make_pooled_cluster(
            instance_type='c1.xlarge',
            num_core_instances=3,
            num_task_instances=1)

        # doesn't matter that there are less than 3 task instances;
        # just has to be big enough to run tasks

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-type', 'c1.xlarge',
            '--num-core-instances', '3'])

    def test_reject_too_small_extra_task_instances(self):
        _, cluster_id = self.make_pooled_cluster(
            core_instance_type='c1.xlarge',
            task_instance_type='m1.medium',
            num_core_instances=3,
            num_task_instances=1)

        # doesn't matter that there are less than 3 task instances;
        # just has to be big enough to run tasks

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-type', 'c1.xlarge',
            '--num-core-instances', '3'])

    def test_extra_task_instances_dont_count_in_total_cpu(self):
        _, cluster_id = self.make_pooled_cluster(
            instance_type='c1.xlarge',
            num_core_instances=2,
            num_task_instances=2)

        # 4 instance total, but only core instances count

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-type', 'c1.xlarge',
            '--num-core-instances', '3'])

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


class EBSRootVolumeGBPoolMatchingTestCase(PoolMatchingBaseTestCase):

    # note that ebs_root_volume_gb is independent from EBS config on
    # instance fleets and instance groups

    def test_join_cluster_with_same_ebs_root_volume_gb(self):
        _, cluster_id = self.make_pooled_cluster(
            ebs_root_volume_gb=123)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--ebs-root-volume-gb', '123'])

    def test_join_cluster_with_larger_ebs_root_volume_gb(self):
        _, cluster_id = self.make_pooled_cluster(
            ebs_root_volume_gb=456)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--ebs-root-volume-gb', '123'])

    def test_dont_join_cluster_with_smaller_ebs_root_volume_gb(self):
        _, cluster_id = self.make_pooled_cluster(
            ebs_root_volume_gb=11)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--ebs-root-volume-gb', '123'])

    def test_dont_join_cluster_with_default_ebs_root_volume_gb(self):
        _, cluster_id = self.make_pooled_cluster()

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--ebs-root-volume-gb', '123'])

    def test_dont_join_cluster_with_non_default_ebs_root_volume_gb(self):
        _, cluster_id = self.make_pooled_cluster(
            ebs_root_volume_gb=123)

        self.assertDoesNotJoin(cluster_id, ['-r', 'emr', '--pool-clusters'])


class EBSConfigPoolMatchingTestCase(PoolMatchingBaseTestCase):

    def _ig_with_ebs_config(
            self, device_configs=(), iops=None,
            num_volumes=None,
            optimized=None, role='MASTER',
            volume_size=100, volume_type=None):
        """Build an instance group request with the given list of
        EBS device configs. Optionally turn on EBS optimization
        and specify a different instance role (``'MASTER'`` by default)."""
        if not device_configs:
            # io1 is the only volume type that accepts IOPS
            if volume_type is None:
                volume_type = 'io1' if iops else 'standard'

            volume_spec = dict(SizeInGB=volume_size, VolumeType=volume_type)
            if iops:
                volume_spec['Iops'] = iops

            if num_volumes:
                volume_spec['VolumesPerInstance'] = num_volumes

            device_configs = [dict(VolumeSpecification=volume_spec)]

        ebs_config = dict(EbsBlockDeviceConfigs=device_configs)
        if optimized is not None:
            ebs_config['EbsOptimized'] = optimized

        return dict(
            EbsConfiguration=ebs_config,
            InstanceRole=role,
            InstanceCount=1,
            InstanceType='m5.xlarge',
        )

    def test_can_join_cluster_with_same_ebs_config(self):
        igs = [self._ig_with_ebs_config()]

        _, cluster_id = self.make_pooled_cluster(
            instance_groups=igs)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(igs)])

    def test_cluster_must_have_ebs_config_if_requested(self):
        igs = [self._ig_with_ebs_config()]

        _, cluster_id = self.make_pooled_cluster()

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(igs)])

    def test_any_ebs_config_okay_if_none_requested(self):
        igs = [self._ig_with_ebs_config()]

        _, cluster_id = self.make_pooled_cluster(
            instance_groups=igs)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters'])

    def test_join_ebs_optimized_cluster(self):
        igs = [self._ig_with_ebs_config(optimized=True)]

        _, cluster_id = self.make_pooled_cluster(
            instance_groups=igs)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(igs)])

    def test_require_ebs_optimized(self):
        requested_igs = [self._ig_with_ebs_config(optimized=True)]
        actual_igs = [self._ig_with_ebs_config()]

        _, cluster_id = self.make_pooled_cluster(
            instance_groups=actual_igs)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(requested_igs)])

    def test_allow_ebs_optimized_if_not_requested(self):
        requested_igs = [self._ig_with_ebs_config()]
        actual_igs = [self._ig_with_ebs_config(optimized=True)]

        _, cluster_id = self.make_pooled_cluster(
            instance_groups=actual_igs)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(requested_igs)])

    def test_ebs_volume_must_be_same_type(self):
        requested_igs = [self._ig_with_ebs_config(volume_type='standard')]
        actual_igs = [self._ig_with_ebs_config(volume_type='gp2')]

        _, cluster_id = self.make_pooled_cluster(
            instance_groups=actual_igs)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(requested_igs)])

    def test_more_ebs_storage_okay(self):
        requested_igs = [self._ig_with_ebs_config(volume_size=100)]
        actual_igs = [self._ig_with_ebs_config(volume_size=200)]

        _, cluster_id = self.make_pooled_cluster(
            instance_groups=actual_igs)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(requested_igs)])

    def test_less_ebs_storage_not_okay(self):
        requested_igs = [self._ig_with_ebs_config(volume_size=100)]
        actual_igs = [self._ig_with_ebs_config(volume_size=50)]

        _, cluster_id = self.make_pooled_cluster(
            instance_groups=actual_igs)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(requested_igs)])

    def test_join_cluster_with_same_iops(self):
        igs = [self._ig_with_ebs_config(iops=1000)]

        _, cluster_id = self.make_pooled_cluster(
            instance_groups=igs)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(igs)])

    def test_more_iops_okay(self):
        requested_igs = [self._ig_with_ebs_config(iops=1000)]
        actual_igs = [self._ig_with_ebs_config(iops=2000)]

        _, cluster_id = self.make_pooled_cluster(
            instance_groups=actual_igs)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(requested_igs)])

    def test_less_iops_not_okay(self):
        requested_igs = [self._ig_with_ebs_config(iops=1000)]
        actual_igs = [self._ig_with_ebs_config(iops=500)]

        _, cluster_id = self.make_pooled_cluster(
            instance_groups=actual_igs)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(requested_igs)])

    def test_multiple_volumes(self):
        igs = [self._ig_with_ebs_config(num_volumes=2)]

        _, cluster_id = self.make_pooled_cluster(
            instance_groups=igs)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(igs)])

    def test_extra_volumes_okay(self):
        requested_igs = [self._ig_with_ebs_config(num_volumes=2)]
        actual_igs = [self._ig_with_ebs_config(num_volumes=3)]

        _, cluster_id = self.make_pooled_cluster(
            instance_groups=actual_igs)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(requested_igs)])

    def test_less_volumes_not_okay(self):
        requested_igs = [self._ig_with_ebs_config(num_volumes=3)]
        actual_igs = [self._ig_with_ebs_config(num_volumes=2)]

        _, cluster_id = self.make_pooled_cluster(
            instance_groups=actual_igs)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(requested_igs)])

    def test_multiple_volume_defs(self):
        volume_spec = dict(SizeInGB=100, VolumeType='standard')

        # two ways of saying the same thing
        requested_igs = [self._ig_with_ebs_config(
            [dict(VolumeSpecification=volume_spec),
             dict(VolumeSpecification=volume_spec)])]
        actual_igs = [self._ig_with_ebs_config(
            [dict(VolumeSpecification=volume_spec, VolumesPerInstance=2)])]

        _, cluster_id = self.make_pooled_cluster(
            instance_groups=actual_igs)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(requested_igs)])

    def test_specs_of_extra_volumes_ignored(self):
        shared_spec = dict(SizeInGB=100, VolumeType='standard')
        extra_spec = dict(SizeInGB=1, VolumeType='gp2')

        requested_igs = [self._ig_with_ebs_config(
            [dict(VolumeSpecification=shared_spec)])]
        actual_igs = [self._ig_with_ebs_config(
            [dict(VolumeSpecification=shared_spec),
             dict(VolumeSpecification=extra_spec)])]

        _, cluster_id = self.make_pooled_cluster(
            instance_groups=actual_igs)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(requested_igs)])

    def test_ordering_of_volumes_matters(self):
        # each volume spec corresponds to an actual volume at
        # particular mount point, so order matters

        shared_spec = dict(SizeInGB=100, VolumeType='standard')
        extra_spec = dict(SizeInGB=1, VolumeType='gp2')

        requested_igs = [self._ig_with_ebs_config(
            [dict(VolumeSpecification=shared_spec)])]
        actual_igs = [self._ig_with_ebs_config(
            [dict(VolumeSpecification=extra_spec),
             dict(VolumeSpecification=shared_spec)])]

        _, cluster_id = self.make_pooled_cluster(
            instance_groups=actual_igs)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(requested_igs)])

    def test_match_ebs_specs_on_multiple_roles(self):
        igs = [self._ig_with_ebs_config(volume_size=10),
               self._ig_with_ebs_config(role='CORE')]

        _, cluster_id = self.make_pooled_cluster(
            instance_groups=igs)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(igs)])

    def test_ebs_must_match_on_all_roles(self):
        requested_igs = [
            self._ig_with_ebs_config(volume_size=10),
            self._ig_with_ebs_config(role='CORE', volume_type='standard')]
        actual_igs = [
            self._ig_with_ebs_config(volume_size=10),
            self._ig_with_ebs_config(role='CORE', volume_type='gp2')]

        _, cluster_id = self.make_pooled_cluster(
            instance_groups=actual_igs)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-groups', json.dumps(requested_igs)])


class BidPricePoolMatchingTestCase(PoolMatchingBaseTestCase):

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
            task_instance_type='c3.4xlarge',
            num_core_instances=2,
            num_task_instances=3)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--num-core-instances', '2',
            '--num-task-instances', '10',  # more instances, but smaller
            '--core-instance-bid-price', '0.10',
            '--master-instance-bid-price', '77.77',
            '--task-instance-bid-price', '22.00'])


class InstanceFleetPoolMatchingTestCase(PoolMatchingBaseTestCase):

    def test_same_instance_fleet_config(self):
        fleets = [
            self._fleet_config(role='MASTER'),
            self._fleet_config(role='CORE',
                               instance_types=['m1.medium', 'm1.large'],
                               weighted_capacities={'m1.large': 2})
        ]

        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=fleets)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(fleets)
        ])

    def test_instance_groups_dont_satisfy_fleets(self):
        fleets = [self._fleet_config(instance_types=['m1.medium', 'm1.large'])]

        _, cluster_id = self.make_pooled_cluster(
            master_instance_type='m1.large')

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(fleets)])

    def test_weighted_capacities_must_match(self):
        actual_fleets = [
            self._fleet_config(role='MASTER'),
            self._fleet_config(role='CORE',
                               instance_types=['m1.medium', 'm1.large'],
                               weighted_capacities={'m1.large': 2})
        ]

        req_fleets = [
            self._fleet_config(role='MASTER'),
            self._fleet_config(role='CORE',
                               instance_types=['m1.medium', 'm1.large'],
                               weighted_capacities={'m1.large': 3})
        ]

        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=actual_fleets)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(req_fleets)])

    def test_matching_fleet_capacity(self):
        fleets = [
            self._fleet_config(role='MASTER'),
            self._fleet_config(
                role='CORE',
                on_demand_capacity=3, spot_capacity=4),
        ]

        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=fleets)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(fleets)])

    def test_extra_fleet_capacity(self):
        actual_fleets = [
            self._fleet_config(role='MASTER'),
            self._fleet_config(
                role='CORE',
                on_demand_capacity=4, spot_capacity=5),
        ]

        req_fleets = [
            self._fleet_config(role='MASTER'),
            self._fleet_config(
                role='CORE',
                on_demand_capacity=3, spot_capacity=4),
        ]

        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=actual_fleets)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(req_fleets)])

    def test_less_fleet_capacity(self):
        actual_fleets = [
            self._fleet_config(role='MASTER'),
            self._fleet_config(
                role='CORE',
                on_demand_capacity=2, spot_capacity=3),
        ]

        req_fleets = [
            self._fleet_config(role='MASTER'),
            self._fleet_config(
                role='CORE',
                on_demand_capacity=3, spot_capacity=4),
        ]

        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=actual_fleets)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(req_fleets)])

    def test_on_demand_can_count_for_missing_spot_capcity(self):
        actual_fleets = [
            self._fleet_config(role='MASTER'),
            self._fleet_config(
                role='CORE',
                on_demand_capacity=4, spot_capacity=3),
        ]

        req_fleets = [
            self._fleet_config(role='MASTER'),
            self._fleet_config(
                role='CORE',
                on_demand_capacity=3, spot_capacity=4),
        ]

        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=actual_fleets)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(req_fleets)])

    def test_spot_cant_count_for_missing_on_demand_capcity(self):
        actual_fleets = [
            self._fleet_config(role='MASTER'),
            self._fleet_config(
                role='CORE',
                on_demand_capacity=3, spot_capacity=4),
        ]

        req_fleets = [
            self._fleet_config(role='MASTER'),
            self._fleet_config(
                role='CORE',
                on_demand_capacity=4, spot_capacity=3),
        ]

        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=actual_fleets)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(req_fleets)])

    def test_extra_requested_fleet_instance_okay(self):
        actual_fleets = [
            self._fleet_config(instance_types=['m1.medium', 'm1.large'])]

        req_fleets = [
            self._fleet_config(
                instance_types=['m1.medium', 'm1.large', 'm1.xlarge'])]

        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=actual_fleets)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(req_fleets)])

    def test_missing_requested_fleet_instance_not_okay(self):
        actual_fleets = [
            self._fleet_config(instance_types=['m1.medium', 'm1.large'])]

        req_fleets = [
            self._fleet_config(
                instance_types=['m1.medium'])]

        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=actual_fleets)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(req_fleets)])

    def test_ebs_optimized_fleet(self):
        fleets = [
            self._fleet_config(ebs_optimized=True)]

        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=fleets)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(fleets)
        ])

    def test_unnecessary_fleet_ebs_optimization_okay(self):
        actual_fleets = [
            self._fleet_config(ebs_optimized=True)]

        req_fleets = [
            self._fleet_config(ebs_optimized=False)]

        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=actual_fleets)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(req_fleets)
        ])

    def test_missing_fleet_ebs_optimization_not_okay(self):
        actual_fleets = [
            self._fleet_config(ebs_optimized=False)]

        req_fleets = [
            self._fleet_config(ebs_optimized=True)]

        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=actual_fleets)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(req_fleets)
        ])

    # fleets use the same code for comparing EBS configs as instance
    # groups, so we don't need to test all the ways EBS configs can match

    def test_fleet_with_ebs_configs(self):
        fleets = [
            self._fleet_config(ebs_device_configs=[dict(
                VolumeSpecification=dict(VolumeType='standard', SizeInGB=200))
            ])
        ]

        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=fleets)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(fleets)
        ])

    def test_fleet_with_better_ebs_devices_okay(self):
        actual_fleets = [
            self._fleet_config(ebs_device_configs=[dict(
                VolumeSpecification=dict(VolumeType='standard', SizeInGB=200))
            ])
        ]

        req_fleets = [
            self._fleet_config(ebs_device_configs=[dict(
                VolumeSpecification=dict(VolumeType='standard', SizeInGB=100))
            ])
        ]
        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=actual_fleets)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(req_fleets)
        ])

    def test_fleet_with_worse_ebs_devices_not_okay(self):
        actual_fleets = [
            self._fleet_config(ebs_device_configs=[dict(
                VolumeSpecification=dict(VolumeType='standard', SizeInGB=50))
            ])
        ]

        req_fleets = [
            self._fleet_config(ebs_device_configs=[dict(
                VolumeSpecification=dict(VolumeType='standard', SizeInGB=100))
            ])
        ]

        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=actual_fleets)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(req_fleets)
        ])

    def test_fleet_that_terminates_on_spot_timeout(self):
        fleets = [
            self._fleet_config(
                spot_spec=dict(
                    TimeoutAction='TERMINATE_CLUSTER',
                    TimeoutDurationMinutes=1440,
                ),
            ),
        ]

        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=fleets)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(fleets)
        ])

    def test_fleet_that_might_terminate(self):
        actual_fleets = [
            self._fleet_config(
                spot_spec=dict(
                    TimeoutAction='TERMINATE_CLUSTER',
                    TimeoutDurationMinutes=1440,
                ),
                spot_capacity=1,
                on_demand_capacity=0,
            )
        ]

        req_fleets = [
            self._fleet_config(on_demand_capacity=0, spot_capacity=1)
        ]

        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=actual_fleets)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(req_fleets)
        ])

    def test_fleet_that_might_terminate_prematurely(self):
        actual_fleets = [
            self._fleet_config(
                spot_spec=dict(
                    TimeoutAction='TERMINATE_CLUSTER',
                    TimeoutDurationMinutes=1440,
                ),
                spot_capacity=1,
                on_demand_capacity=0,
            ),
        ]

        req_fleets = [
            self._fleet_config(
                spot_spec=dict(
                    TimeoutAction='TERMINATE_CLUSTER',
                    TimeoutDurationMinutes=2880,
                ),
                spot_capacity=1,
                on_demand_capacity=0,
            ),
        ]

        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=actual_fleets)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(req_fleets)
        ])

    def test_fleet_that_might_terminate_but_more_slowly(self):
        actual_fleets = [
            self._fleet_config(
                spot_spec=dict(
                    TimeoutAction='TERMINATE_CLUSTER',
                    TimeoutDurationMinutes=1440,
                ),
                spot_capacity=1,
                on_demand_capacity=0,
            ),
        ]

        req_fleets = [
            self._fleet_config(
                spot_spec=dict(
                    TimeoutAction='TERMINATE_CLUSTER',
                    TimeoutDurationMinutes=770,
                ),
                spot_capacity=1,
                on_demand_capacity=0,
            ),
        ]

        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=actual_fleets)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(req_fleets)
        ])

    def test_join_fleet_that_wont_terminate(self):
        actual_fleets = [self._fleet_config()]

        req_fleets = [
            self._fleet_config(
                spot_spec=dict(
                    TimeoutAction='TERMINATE_CLUSTER',
                    TimeoutDurationMinutes=770,
                ),
                spot_capacity=1,
                on_demand_capacity=0,
            ),
        ]

        _, cluster_id = self.make_pooled_cluster(
            instance_fleets=actual_fleets)

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(req_fleets)
        ])

    def test_dont_join_fleet_pool_without_provisioned_capacity(self):
        # make sure that we only join fleets with provisioned capacity
        fleets = [self._fleet_config()]

        _, cluster_id = self.make_pooled_cluster(provision=False,
                                                 instance_fleets=fleets)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--instance-fleets', json.dumps(fleets)])


class MrjobVersionPoolMatchingTestCase(PoolMatchingBaseTestCase):

    def test_dont_join_wrong_mrjob_version(self):
        _, cluster_id = self.make_pooled_cluster()

        self.start(patch('mrjob.__version__', 'OVER NINE THOUSAAAAAND'))

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters'])

    def test_version_matters_even_if_mrjob_not_bootstrapped(self):
        _, cluster_id = self.make_pooled_cluster(
            bootstrap_mrjob=False)

        self.start(patch('mrjob.__version__', 'OVER NINE THOUSAAAAAND'))

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters'])

    def test_python_bin_doesnt_matter(self):
        _, cluster_id = self.make_pooled_cluster()

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--python-bin', 'snake'])


class BootstrapPoolMatchingTestCase(PoolMatchingBaseTestCase):

    def test_join_similarly_bootstrapped_pool(self):
        _, cluster_id = self.make_pooled_cluster(
            bootstrap=['true'])

        self.assertJoins(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--bootstrap', 'true'])

    def test_dont_join_differently_bootstrapped_pool(self):
        _, cluster_id = self.make_pooled_cluster()

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--bootstrap', 'true'])

    def test_dont_join_differently_bootstrapped_pool_2(self):
        bootstrap_path = join(self.tmp_dir, 'go.sh')
        with open(bootstrap_path, 'w') as f:
            f.write('#!/usr/bin/sh\necho "hi mom"\n')

        _, cluster_id = self.make_pooled_cluster()

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters',
            '--bootstrap-action', bootstrap_path + ' a b c'])

    def test_bootstrap_file_contents(self):
        story_path = self.makefile('story.txt', b'Once upon a time')

        true_story = 'true %s#' % story_path

        _, cluster_id = self.make_pooled_cluster(bootstrap=[true_story])

        # same bootstrap command, same file (matches)
        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--bootstrap', true_story])

        # same command, different file path with same contents (matches)
        story_2_path = self.makefile('story-2.txt', b'Once upon a time')
        self.assertNotEqual(story_2_path, story_path)

        true_story_2 = 'true %s#story.txt' % story_2_path

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--bootstrap', true_story_2])

        # same command, same file path, different contents (does not match)
        with open(story_path, 'wb') as f:
            f.write(b'Call me Ishmael.')  # same file size, different letters

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--bootstrap', true_story])

    def test_bootstrap_archive_contents(self):
        story_dir = self.makedirs('story')
        self.makefile(join(story_dir, 'fairy.txt'), b'Once upon a time')
        self.makefile(join(story_dir, 'moby.txt'), b'Call me Ishmael.')

        empty_dir = self.makedirs('empty')

        story_path = make_archive(join(self.tmp_dir, 'story'),
                                  'gztar', story_dir)

        true_story = 'true %s#/' % story_path

        _, cluster_id = self.make_pooled_cluster(bootstrap=[true_story])

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--bootstrap', true_story])

        os.remove(story_path)

        empty_story_path = make_archive(join(self.tmp_dir, 'story'),
                                        'gztar', empty_dir)

        self.assertEqual(story_path, empty_story_path)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--bootstrap', true_story])


class MiscPoolMatchingTestCase(PoolMatchingBaseTestCase):

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

        self.assertEqual(runner_1._find_cluster()[0], cluster_id)
        self.assertEqual(runner_2._find_cluster()[0], None)

    def test_sorting_by_cpu_capacity(self):
        _, cluster_id_1 = self.make_pooled_cluster(
            'pool1',
            num_core_instances=2,
            normalized_instance_hours=48)
        _, cluster_id_2 = self.make_pooled_cluster(
            'pool1',
            num_core_instances=1,
            normalized_instance_hours=32)

        runner_1 = self.make_simple_runner(
            'pool1', '--num-core-instances', '1')
        runner_2 = self.make_simple_runner(
            'pool1', '--num-core-instances', '1')

        self.assertEqual(runner_1._find_cluster()[0], cluster_id_1)
        self.assertEqual(runner_2._find_cluster()[0], cluster_id_2)

    def test_sorting_by_cpu_capacity_divides_by_number_of_hours(self):
        _, cluster_id_1 = self.make_pooled_cluster(
            'pool1',
            num_core_instances=2,
            normalized_instance_hours=48)
        _, cluster_id_2 = self.make_pooled_cluster(
            'pool1',
            num_core_instances=1,
            minutes_ago=90,
            normalized_instance_hours=64)

        runner_1 = self.make_simple_runner(
            'pool1', '--num-core-instances', '1')
        runner_2 = self.make_simple_runner(
            'pool1', '--num-core-instances', '1')

        self.assertEqual(runner_1._find_cluster()[0], cluster_id_1)
        self.assertEqual(runner_2._find_cluster()[0], cluster_id_2)

    def test_dont_destroy_own_pooled_cluster_on_failure(self):
        # Issue 242: job failure shouldn't kill the pooled clusters
        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '--pool-clusters'])
        mr_job.sandbox()

        self.mock_emr_failures = set([('j-MOCKCLUSTER0', 0)])

        with mr_job.make_runner() as runner:
            self.assertIsInstance(runner, EMRJobRunner)
            self.prepare_runner_for_ssh(runner)
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
            self.assertRaises(StepFailedException, runner.run)

            self.assertEqual(runner.get_cluster_id(), cluster_id)

            for _ in range(10):
                self.simulate_emr_progress(runner.get_cluster_id())

            cluster = runner._describe_cluster()
            self.assertEqual(cluster['Status']['State'], 'WAITING')

        # job shouldn't get terminated by cleanup
        for _ in range(10):
            self.simulate_emr_progress(runner.get_cluster_id())

        cluster = runner._describe_cluster()
        self.assertEqual(cluster['Status']['State'], 'WAITING')

    def test_max_mins_idle_doesnt_affect_pool_hash(self):
        # max_mins_idle uses a bootstrap action, but it's not included
        # in the pool hash
        _, cluster_id = self.make_pooled_cluster()

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters', '--max-mins-idle', '60'])

    def test_can_join_cluster_started_with_max_mins_idle(self):
        _, cluster_id = self.make_pooled_cluster(max_mins_idle=60)

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

    def test_dont_join_pool_without_provisioned_instances(self):
        # test #1633
        _, cluster_id = self.make_pooled_cluster(provision=False)

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '-v', '--pool-clusters'])


class DockerPoolMatchingTestCase(PoolMatchingBaseTestCase):

    def test_same_docker_image(self):
        _, cluster_id = self.make_pooled_cluster(
            docker_image='dead-sea/scrolls:latest')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--docker-image', 'dead-sea/scrolls:latest'])

    def test_same_docker_registry(self):
        _, cluster_id = self.make_pooled_cluster(
            docker_image='dead-sea/scrolls:latest')

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--docker-image', 'dead-sea/mud'])

    def test_different_docker_registry(self):
        _, cluster_id = self.make_pooled_cluster(
            docker_image='dead-sea/scrolls:latest')

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--docker-image', 'mrjob-registry/mrjob'])

    def test_docker_image_vs_none(self):
        _, cluster_id = self.make_pooled_cluster(
            docker_image='dead-sea/scrolls:latest')

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters'])


class MinAvailableOptsPoolMatchingTestCase(PoolMatchingBaseTestCase):

    EXPECTED_CURL_ARGS = [
        'curl', '-fsS', '-m', '20',
        'http://mockmaster:8088/ws/v1/cluster/metrics',
    ]

    def setUp(self):
        super(MinAvailableOptsPoolMatchingTestCase, self).setUp()

        # update this dict to set availableMB etc.
        self.cluster_metrics = dict(
            availableMB=1,
            availableVirtualCores=4,
        )

        def _mock_ssh_run(address, cmd_args):
            return (
                json.dumps(
                    dict(clusterMetrics=self.cluster_metrics)
                ).encode('utf_8'),
                b'',
            )

        self._ssh_run = self.start(patch(
            'mrjob.fs.ssh.SSHFilesystem._ssh_run',
            side_effect=_mock_ssh_run,
        ))

        self.key_pair_file = self.makefile('EMR.pem')

    def test_join_cluster_with_requested_resources(self):
        _, cluster_id = self.make_pooled_cluster(
            ec2_key_pair='EMR')

        self.cluster_metrics['availableMB'] = 12288
        self.cluster_metrics['availableVirtualCores'] = 4

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--ec2-key-pair', 'EMR',
            '--ec2-key-pair-file', self.key_pair_file,
            '--min-available-mb', '12288',
            '--min-available-virtual-cores', '4',
        ])

        # _ssh_run() is also called to fetch the job's progress
        self._ssh_run.assert_any_call('mockmaster', self.EXPECTED_CURL_ARGS)

    def test_join_cluster_with_more_than_requested_resources(self):
        _, cluster_id = self.make_pooled_cluster(
            ec2_key_pair='EMR')

        self.cluster_metrics['availableMB'] = 24576
        self.cluster_metrics['availableVirtualCores'] = 8

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--ec2-key-pair', 'EMR',
            '--ec2-key-pair-file', self.key_pair_file,
            '--min-available-mb', '12288',
            '--min-available-virtual-cores', '4',
        ])

        self._ssh_run.assert_any_call('mockmaster', self.EXPECTED_CURL_ARGS)

    def test_dont_join_cluster_with_too_few_mb(self):
        _, cluster_id = self.make_pooled_cluster(
            ec2_key_pair='EMR')

        self.cluster_metrics['availableMB'] = 6144
        self.cluster_metrics['availableVirtualCores'] = 8

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--ec2-key-pair', 'EMR',
            '--ec2-key-pair-file', self.key_pair_file,
            '--min-available-mb', '12288',
            '--min-available-virtual-cores', '4',
        ])

        self._ssh_run.assert_any_call('mockmaster', self.EXPECTED_CURL_ARGS)

    def test_join_cluster_with_too_few_virtual_cores(self):
        _, cluster_id = self.make_pooled_cluster(
            ec2_key_pair='EMR')

        self.cluster_metrics['availableMB'] = 12288
        self.cluster_metrics['availableVirtualCores'] = 2

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--ec2-key-pair', 'EMR',
            '--ec2-key-pair-file', self.key_pair_file,
            '--min-available-mb', '12288',
            '--min-available-virtual-cores', '4',
        ])

        self._ssh_run.assert_any_call('mockmaster', self.EXPECTED_CURL_ARGS)

    def test_available_mb_only(self):
        _, cluster_id = self.make_pooled_cluster(
            ec2_key_pair='EMR')

        self.cluster_metrics['availableMB'] = 12288
        self.cluster_metrics['availableVirtualCores'] = 2

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--ec2-key-pair', 'EMR',
            '--ec2-key-pair-file', self.key_pair_file,
            '--min-available-mb', '12288',
        ])

        self._ssh_run.assert_any_call('mockmaster', self.EXPECTED_CURL_ARGS)


    def test_available_virtual_cores_only(self):
        _, cluster_id = self.make_pooled_cluster(
            ec2_key_pair='EMR')

        self.cluster_metrics['availableMB'] = 6144
        self.cluster_metrics['availableVirtualCores'] = 4

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--ec2-key-pair', 'EMR',
            '--ec2-key-pair-file', self.key_pair_file,
            '--min-available-virtual-cores', '4',
        ])

        self._ssh_run.assert_any_call('mockmaster', self.EXPECTED_CURL_ARGS)

    def test_zero_mb_disables_opt(self):
        _, cluster_id = self.make_pooled_cluster(
            ec2_key_pair='EMR')

        self.cluster_metrics['availableMB'] = 6144
        self.cluster_metrics['availableVirtualCores'] = 4

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--ec2-key-pair', 'EMR',
            '--ec2-key-pair-file', self.key_pair_file,
            '--min-available-mb', '0',
            '--min-available-virtual-cores', '4',
        ])

        self._ssh_run.assert_any_call('mockmaster', self.EXPECTED_CURL_ARGS)

    def test_zero_virtual_cores_disables_opt(self):
        _, cluster_id = self.make_pooled_cluster(
            ec2_key_pair='EMR')

        self.cluster_metrics['availableMB'] = 12288
        self.cluster_metrics['availableVirtualCores'] = 2

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--ec2-key-pair', 'EMR',
            '--ec2-key-pair-file', self.key_pair_file,
            '--min-available-mb', '12288',
            '--min-available-virtual-cores', '0',
        ])

        self._ssh_run.assert_any_call('mockmaster', self.EXPECTED_CURL_ARGS)

    def test_instance_attributes_dont_matter(self):
        _, cluster_id = self.make_pooled_cluster(
            ec2_key_pair='EMR')

        self.cluster_metrics['availableMB'] = 12288
        self.cluster_metrics['availableVirtualCores'] = 4

        self.assertJoins(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--ec2-key-pair', 'EMR',
            '--ec2-key-pair-file', self.key_pair_file,
            '--min-available-mb', '12288',
            '--min-available-virtual-cores', '4',
            '--instance-type', 'm5.24xlarge',
            '--num-core-instances', '100',
        ])

        # EBS attributes of instances (set with instance_groups or
        # instance_fleets) also won't be checked. this is by design,
        # trying to save API calls

        self._ssh_run.assert_any_call('mockmaster', self.EXPECTED_CURL_ARGS)

    def test_cluster_attributes_still_matter(self):
        _, cluster_id = self.make_pooled_cluster(
            ec2_key_pair='EMR')

        self.cluster_metrics['availableMB'] = 12288
        self.cluster_metrics['availableVirtualCores'] = 4

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--ec2-key-pair', 'EMR',
            '--ec2-key-pair-file', self.key_pair_file,
            '--min-available-mb', '12288',
            '--min-available-virtual-cores', '4',
            '--instance-type', 'm5.24xlarge',
            '--num-core-instances', '100',
            '--ebs-root-volume-gb', '1000',
        ])

        self._ssh_run.assert_any_call('mockmaster', self.EXPECTED_CURL_ARGS)

    def test_setting_both_opts_to_zero_disables_connecting_to_yarn(self):
        _, cluster_id = self.make_pooled_cluster(
            ec2_key_pair='EMR')

        self.cluster_metrics['availableMB'] = 12288
        self.cluster_metrics['availableVirtualCores'] = 4

        self.assertDoesNotJoin(cluster_id, [
            '-r', 'emr', '--pool-clusters',
            '--ec2-key-pair', 'EMR',
            '--ec2-key-pair-file', self.key_pair_file,
            '--min-available-mb', '0',
            '--min-available-virtual-cores', '0',
            '--instance-type', 'm5.24xlarge',
            '--num-core-instances', '100',
        ])

        self.assertNotIn(
            call('mockmaster', self.EXPECTED_CURL_ARGS),
            self._ssh_run.call_args_list
        )


class PoolingRecoveryTestCase(MockBoto3TestCase):

    MRJOB_CONF_CONTENTS = {'runners': {'emr': {'pool_clusters': True}}}

    # for multiple failover test
    MAX_EMR_CLIENTS = 200

    def make_pooled_cluster(self, normalized_instance_hours=None, **kwargs):
        cluster_id = EMRJobRunner(**kwargs).make_persistent_cluster()

        # simulate that instances are provisioned
        mock_cluster = self.mock_emr_clusters[cluster_id]
        mock_cluster['Status']['State'] = 'WAITING'
        mock_cluster['MasterPublicDnsName'] = 'mockmaster'
        for ig in mock_cluster['_InstanceGroups']:
            ig['RunningInstanceCount'] = ig['RequestedInstanceCount']

        # simulate NormalizedInstanceHours
        if normalized_instance_hours is not None:
            mock_cluster['NormalizedInstanceHours'] = normalized_instance_hours

        # make sure ReadyDateTime is set (needed for sorting)
        mock_cluster['Status']['Timeline']['ReadyDateTime'] = (
            mock_cluster['Status']['Timeline']['CreationDateTime'])

        return cluster_id

    def step_ids(self, cluster_id):
        return [s['Id'] for s in self.mock_emr_clusters[cluster_id]['_Steps']]

    def num_steps(self, cluster_id):
        return len(self.mock_emr_clusters[cluster_id]['_Steps'])

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

            # tried to add step to pooled cluster, had to try again
            self.assertEqual(self.num_steps(cluster_id), 1)

            self.assertNotEqual(runner.get_cluster_id(), cluster_id)
            self.assertEqual(self.num_steps(runner.get_cluster_id()), 2)

            # make sure self._step_ids got cleared
            self.assertEqual(runner._step_ids,
                             self.step_ids(runner.get_cluster_id()))

    def test_launch_new_multi_node_cluster_after_self_termination(self):
        # the error message is different when a multi-node cluster
        # self-terminates
        cluster_id = self.make_pooled_cluster(num_core_instances=1)
        self.mock_emr_self_termination.add(cluster_id)

        job = MRTwoStepJob(['-r', 'emr', '--num-core-instances', '1'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            # tried to add step to pooled cluster, had to try again
            self.assertEqual(self.num_steps(cluster_id), 1)

            self.assertNotEqual(runner.get_cluster_id(), cluster_id)
            self.assertEqual(self.num_steps(runner.get_cluster_id()), 2)

            # make sure self._step_ids got cleared
            self.assertEqual(runner._step_ids,
                             self.step_ids(runner.get_cluster_id()))

    def test_reset_ssh_tunnel_and_hadoop_fs_on_launch(self):
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

        # also test reset of _hadoop_fs
        def _address_of_master(self):
            return '%s-master' % self._cluster_id

        self.start(patch(
            'mrjob.emr.EMRJobRunner._address_of_master',
            side_effect=_address_of_master, autospec=True))

        cluster_id = self.make_pooled_cluster()
        self.mock_emr_self_termination.add(cluster_id)

        job = MRTwoStepJob(['-r', 'emr', '--ec2-key-pair-file', os.devnull])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertTrue(hasattr(runner.fs, 'hadoop'))
            self.assertEqual(runner.fs.hadoop._hadoop_bin, [])

            runner.run()

            # tried to add step to pooled cluster, had to try again
            self.assertEqual(self.num_steps(cluster_id), 1)

            self.assertNotEqual(runner.get_cluster_id(), cluster_id)
            self.assertEqual(self.num_steps(runner.get_cluster_id()), 2)

            self.assertEqual(len(mock_kill_ssh_tunnel.call_args_list), 1)
            self.assertEqual(ssh_tunnel_cluster_ids,
                             [cluster_id, runner.get_cluster_id()])

            self.assertNotEqual(runner.fs.hadoop._hadoop_bin, [])
            self.assertIn('hadoop@%s-master' % runner.get_cluster_id(),
                          runner.fs.hadoop._hadoop_bin)

    def test_join_pooled_cluster_after_self_termination(self):
        # cluster 1 should be preferable
        cluster1_id = self.make_pooled_cluster(num_core_instances=20,
                                               normalized_instance_hours=272)
        self.mock_emr_self_termination.add(cluster1_id)
        cluster2_id = self.make_pooled_cluster(num_core_instances=1,
                                               normalized_instance_hours=32)

        job = MRTwoStepJob(['-r', 'emr', '--num-core-instances', '1'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(self.num_steps(cluster1_id), 1)

            self.assertEqual(runner.get_cluster_id(), cluster2_id)
            self.assertEqual(self.num_steps(cluster2_id), 2)

            # make sure self._step_ids got cleared
            self.assertEqual(runner._step_ids, self.step_ids(cluster2_id))

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

            # steps are added one at a time
            self.assertEqual(self.num_steps(cluster_id), 1)

    def test_dont_recover_from_user_termination(self):
        cluster_id = self.make_pooled_cluster()

        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            # don't have a mock_boto3 hook for termination of cluster
            # during run(), so running the two halves of run() separately
            self.launch(runner)

            self.assertEqual(runner.get_cluster_id(), cluster_id)
            # steps are added one at a time
            self.assertEqual(self.num_steps(cluster_id), 1)

            self.client('emr').terminate_job_flows(JobFlowIds=[cluster_id])

            self.assertRaises(StepFailedException, runner._finish_run)

    def test_dont_recover_from_created_cluster_self_terminating(self):
        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            self.launch(runner)

            cluster_id = runner.get_cluster_id()
            # steps are added one at a time
            self.assertEqual(self.num_steps(cluster_id), 1)
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
            self.launch(runner)

            self.assertEqual(runner.get_cluster_id(), cluster_id)
            addr = runner._address_of_master()

            runner._finish_run()

            self.assertNotEqual(runner.get_cluster_id(), cluster_id)
            self.assertNotEqual(runner._address_of_master(), addr)


class PoolingDisablingTestCase(MockBoto3TestCase):

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
            self.assertEqual(cluster['AutoTerminate'], True)


class ClusterLockingTestCase(PoolMatchingBaseTestCase):

    # ensure that clusters get locked and unlocked at the right time

    def setUp(self):
        super(ClusterLockingTestCase, self).setUp()

        self._attempt_to_lock_cluster = self.start(patch(
            'mrjob.emr._attempt_to_lock_cluster',
            side_effect=_attempt_to_lock_cluster))

        self._attempt_to_unlock_cluster = self.start(patch(
            'mrjob.emr._attempt_to_unlock_cluster',
            side_effect=_attempt_to_unlock_cluster))

    def test_no_locking_without_pooling(self):
        job = MRTwoStepJob(['-r', 'emr'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

        self.assertFalse(self._attempt_to_lock_cluster.called)

    def test_no_locking_with_explicit_cluster_id(self):
        _, cluster_id = self.make_pooled_cluster()

        job = MRTwoStepJob(['-r', 'emr', '--cluster-id', cluster_id])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

        self.assertFalse(self._attempt_to_lock_cluster.called)

    def test_join_non_concurrent_pooled_cluster(self):
        _, cluster_id = self.make_pooled_cluster()

        job = MRTwoStepJob(['-r', 'emr', '--pool-clusters'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._add_input_files_for_upload()
            runner._launch()

            self.assertTrue(self._attempt_to_lock_cluster.called)
            self.assertFalse(self._attempt_to_unlock_cluster.called)

            self.mock_emr_failures.add((cluster_id, 0))
            self.assertRaises(StepFailedException, runner._finish_run)

            # unlocked at some point while first step was running
            self.assertTrue(self._attempt_to_unlock_cluster.called)

    def test_join_concurrent_pooled_cluster(self):
        _, cluster_id = self.make_pooled_cluster(max_concurrent_steps=2)

        job = MRTwoStepJob(['-r', 'emr', '--pool-clusters',
                            '--max-concurrent-steps', '2'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._add_input_files_for_upload()
            runner._launch()

            self.assertTrue(self._attempt_to_lock_cluster.called)
            # unlocked immediately after adding steps
            self.assertTrue(self._attempt_to_unlock_cluster.called)


class TestTimedOutException(Exception):
    pass


class FindClusterTestCase(MockBoto3TestCase):
    # test max_clusters_in_pool, pool_wait_minutes, and pool_timeout_minutes

    # bail out if we "sleep" for more than ten minutes
    MAX_TIME_SLEPT = 600

    def setUp(self):
        super(FindClusterTestCase, self).setUp()

        # mock out sleep() and now()
        #
        # also break the test out of infinite loops
        self.time_slept = 0

        def mock_sleep(time):
            self.time_slept += time
            if self.time_slept >= self.MAX_TIME_SLEPT:
                raise TestTimedOutException(
                    'Test slept for more than %d seconds' %
                    self.MAX_TIME_SLEPT)

        self.sleep = self.start(patch('time.sleep', side_effect=mock_sleep))

        now_func = datetime.now

        def mock_now():
            return now_func() + timedelta(seconds=self.time_slept)

        self.datetime = self.start(patch('mrjob.emr.datetime'))
        self.datetime.now = mock_now

        # by default, treat all available cluster IDs as valid
        def mock_yield_clusters_to_join(available_cluster_ids):
            for cluster_id in available_cluster_ids:
                mock_cluster = dict(
                    Id=cluster_id,
                    StepConcurrencyLevel=1)

                yield (mock_cluster, mock_now())

        self.yield_clusters_to_join = self.start(patch(
            'mrjob.emr.EMRJobRunner._yield_clusters_to_join',
            side_effect=mock_yield_clusters_to_join))

        # cluster locking always succeeds
        self.attempt_to_lock_cluster = self.start(patch(
            'mrjob.emr._attempt_to_lock_cluster', return_value=True))

        # change this to make the tests work differently
        self.list_cluster_ids_for_pooling = self.start(patch(
            'mrjob.emr.EMRJobRunner._list_cluster_ids_for_pooling',
            return_value=dict(
                available=[],
                in_pool=set(),
                matching=set(),
                max_created=None,
            )
        ))

        # make randint repeatable
        def mock_randint(a, b):
            return (a + b) // 2

        self.randint = self.start(patch(
            'mrjob.emr.randint', side_effect=mock_randint))

    def test_create_own_cluster_if_none_available(self):
        runner = self.make_runner(
            '--pool-clusters', '--max-clusters-in-pool', '2')

        self.assertEqual(runner._find_cluster()[0], None)

    def test_wait_forever_if_pool_full(self):
        self.list_cluster_ids_for_pooling.return_value.update(dict(
            in_pool={'j-INPOOL1', 'j-INPOOL2'},
            max_created=datetime(2020, 9, 11, 12, 13),
        ))

        runner = self.make_runner(
            '--pool-clusters', '--max-clusters-in-pool', '2')

        self.assertRaises(TestTimedOutException, runner._find_cluster)

    def test_pool_timeout_minutes(self):
        # pool is full, forever
        self.list_cluster_ids_for_pooling.return_value.update(dict(
            in_pool={'j-INPOOL1', 'j-INPOOL2'},
            max_created=datetime(2020, 9, 11, 12, 13),
        ))

        runner = self.make_runner(
            '--pool-clusters', '--max-clusters-in-pool', '2',
            '--pool-timeout-minutes', '2')

        self.assertRaises(PoolTimeoutException, runner._find_cluster)

        # pooling sleep interval is a little more than 30 seconds, so
        # four times would exceed the timeout
        self.assertEqual(self.time_slept, 3 * _POOLING_SLEEP_INTERVAL)

    def test_pool_timeout_of_zero_means_dont_time_out(self):
        self.list_cluster_ids_for_pooling.return_value.update(dict(
            in_pool={'j-INPOOL1', 'j-INPOOL2'},
            max_created=datetime(2020, 9, 11, 12, 13),
        ))

        runner = self.make_runner(
            '--pool-clusters', '--max-clusters-in-pool', '2',
            '--pool-timeout-minutes', '0')

        self.assertRaises(TestTimedOutException, runner._find_cluster)

    def test_pool_timeout_minutes_applies_to_jitter(self):
        runner = self.make_runner(
            '--pool-clusters', '--max-clusters-in-pool', '2',
            '--pool-jitter-seconds', '250',
            '--pool-timeout-minutes', '2')

        self.assertRaises(PoolTimeoutException, runner._find_cluster)

        # should attempt to wait a random number of seconds before
        # double-checking that the pool isn't full, but would have to wait
        # too long
        self.randint.assert_called_once_with(0, 250)
        self.assertEqual(self.time_slept, 0)

    def test_join_available_cluster_from_full_pool(self):
        self.list_cluster_ids_for_pooling.return_value.update(dict(
            available=['j-INPOOL2'],
            matching={'j-INPOOL2'},
            in_pool={'j-INPOOL1', 'j-INPOOL2'},
            max_created=datetime(2020, 9, 11, 12, 13),
        ))

        runner = self.make_runner(
            '--pool-clusters', '--max-clusters-in-pool', '2')

        self.assertEqual(runner._find_cluster()[0], 'j-INPOOL2')

    def test_double_check_with_random_jitter(self):
        self.list_cluster_ids_for_pooling.side_effect = [
            dict(
                available=[],
                in_pool={'j-INPOOL1'},
                matching={'j-INPOOL1'},
                max_created=datetime(2020, 9, 11, 12, 13),
            ),
            # when we check again, another cluster has started
            dict(
                available=[],
                in_pool={'j-INPOOL1', 'j-INPOOL2'},
                matching={'j-INPOOL2'},
                max_created=datetime(2020, 9, 11, 12, 13, 14),
            ),
            # and then the first one becomes available to join
            dict(
                available=['j-INPOOL1'],
                in_pool={'j-INPOOL1', 'j-INPOOL2'},
                matching={'j-INPOOL1', 'j-INPOOL2'},
                max_created=datetime(2020, 9, 11, 12, 13, 14),
            ),
        ]

        runner = self.make_runner(
            '--pool-clusters', '--max-clusters-in-pool', '2')

        self.assertEqual(runner._find_cluster()[0], 'j-INPOOL1')

        self.randint.assert_called_once_with(0, 60)  # default jitter

        self.list_cluster_ids_for_pooling.assert_any_call(
            created_after=datetime(2020, 9, 11, 12, 13))

        self.assertEqual(self.time_slept,
                         _POOLING_SLEEP_INTERVAL + self.randint(0, 60))

    def test_pool_jitter_seconds_option(self):
        runner = self.make_runner(
            '--pool-clusters', '--max-clusters-in-pool', '2',
            '--pool-jitter-seconds', '120')

        self.assertEqual(runner._find_cluster()[0], None)

        self.randint.assert_called_once_with(0, 120)

        self.list_cluster_ids_for_pooling.assert_any_call(
            created_after=None)

        self.assertEqual(self.time_slept, self.randint(0, 120))

    def test_pool_wait_minutes(self):
        self.list_cluster_ids_for_pooling.return_value.update(dict(
            available=[],
            matching={'j-NICE'},
            in_pool={'j-NICE'},
            max_created=datetime(2020, 9, 11, 12, 13),
        ))

        runner = self.make_runner(
            '--pool-clusters', '--pool-wait-minutes', '5')

        self.assertEqual(runner._find_cluster()[0], None)

        # should wait a little over 5 minutes
        self.assertGreater(self.time_slept, 5 * 60)
        self.assertLess(self.time_slept, 6 * 60)

        # should not use jitter to create new cluster, since we just timed out
        self.assertFalse(self.randint.called)

    def test_pool_wait_minutes_with_max_clusters_in_pool(self):
        self.list_cluster_ids_for_pooling.return_value.update(dict(
            available=[],
            matching={'j-NICE'},
            in_pool={'j-NICE'},
            max_created=datetime(2020, 9, 11, 12, 13),
        ))

        runner = self.make_runner(
            '--pool-clusters', '--pool-wait-minutes', '5',
            '--max-clusters-in-pool', '2')

        self.assertEqual(runner._find_cluster()[0], None)

        # should use jitter to create new cluster, since we just timed out
        self.randint.assert_called_once_with(0, 60)

        # should wait a little over 5 minutes
        self.assertGreater(self.time_slept, 5 * 60 + self.randint(0, 60))
        self.assertLess(self.time_slept, 6 * 60 + self.randint(0, 60))

    def test_pool_wait_minutes_with_no_matching_clusters(self):
        self.list_cluster_ids_for_pooling.return_value.update(dict(
            available=[],
            matching=set(),
            in_pool={'j-NOPE'},
            max_created=datetime(2020, 9, 11, 12, 13),
        ))

        runner = self.make_runner(
            '--pool-clusters', '--pool-wait-minutes', '5')

        self.assertEqual(runner._find_cluster()[0], None)

        # should not wait at all except for jitter
        self.randint.assert_called_once_with(0, 60)

        self.assertEqual(self.time_slept, self.randint(0, 60))

    def test_no_matching_clusters_but_pool_full(self):
        self.list_cluster_ids_for_pooling.return_value.update(dict(
            available=[],
            matching=set(),
            in_pool={'j-NOPE1', 'j-NOPE2'},
            max_created=datetime(2020, 9, 11, 12, 13),
        ))

        runner = self.make_runner(
            '--pool-clusters', '--pool-wait-minutes', '5',
            '--max-clusters-in-pool', '2')

        self.assertRaises(TestTimedOutException, runner._find_cluster)


class ListClusterIdsForPoolingTestCase(PoolMatchingBaseTestCase):
    # test this method separately, since we mocked it out in the above test

    def test_empty(self):
        runner = self.make_runner('--pool-clusters')

        self.assertEqual(
            runner._list_cluster_ids_for_pooling(),
            dict(
                available=[],
                in_pool=set(),
                matching=set(),
                max_created=None,
            )
        )

    def test_new_cluster(self):
        cluster_id = self.make_runner(
            '--pool-clusters').make_persistent_cluster()

        cluster = self.mock_emr_clusters[cluster_id]
        self.assertEqual(cluster['Status']['State'], 'STARTING')

        runner = self.make_runner('--pool-clusters')

        self.assertEqual(
            runner._list_cluster_ids_for_pooling(),
            dict(
                available=[],  # because still STARTING
                in_pool={cluster_id},
                matching={cluster_id},
                max_created=cluster['Status']['Timeline']['CreationDateTime'],
            )
        )

    def test_bootstrapping_cluster(self):
        cluster_id = self.make_runner(
            '--pool-clusters').make_persistent_cluster()

        cluster = self.mock_emr_clusters[cluster_id]
        cluster['Status']['State'] = 'BOOTSTRAPPING'

        runner = self.make_runner('--pool-clusters')

        self.assertEqual(
            runner._list_cluster_ids_for_pooling(),
            dict(
                available=[],
                in_pool={cluster_id},
                matching={cluster_id},
                max_created=cluster['Status']['Timeline']['CreationDateTime'],
            )
        )

    def test_waiting_cluster(self):
        cluster_id = self.make_runner(
            '--pool-clusters').make_persistent_cluster()

        cluster = self.mock_emr_clusters[cluster_id]
        cluster['Status']['State'] = 'WAITING'

        runner = self.make_runner('--pool-clusters')

        self.assertEqual(
            runner._list_cluster_ids_for_pooling(),
            dict(
                available=[cluster_id],
                in_pool={cluster_id},
                matching={cluster_id},
                max_created=cluster['Status']['Timeline']['CreationDateTime'],
            )
        )

    def test_running_cluster(self):
        cluster_id = self.make_runner(
            '--pool-clusters').make_persistent_cluster()

        cluster = self.mock_emr_clusters[cluster_id]
        cluster['Status']['State'] = 'RUNNING'

        runner = self.make_runner('--pool-clusters')

        self.assertEqual(
            runner._list_cluster_ids_for_pooling(),
            dict(
                available=[],
                in_pool={cluster_id},
                matching={cluster_id},
                max_created=cluster['Status']['Timeline']['CreationDateTime'],
            )
        )

    def test_running_cluster_with_concurrency_allowed(self):
        cluster_id = self.make_runner(
            '--pool-clusters').make_persistent_cluster()

        cluster = self.mock_emr_clusters[cluster_id]
        cluster['Status']['State'] = 'RUNNING'

        runner = self.make_runner(
            '--pool-clusters', '--max-concurrent-steps', '2')

        self.assertEqual(
            runner._list_cluster_ids_for_pooling(),
            dict(
                available=[cluster_id],
                in_pool={cluster_id},
                matching={cluster_id},
                max_created=cluster['Status']['Timeline']['CreationDateTime'],
            )
        )

    def test_terminating_cluster(self):
        cluster_id = self.make_runner(
            '--pool-clusters').make_persistent_cluster()

        cluster = self.mock_emr_clusters[cluster_id]
        cluster['Status']['State'] = 'TERMINATING'

        runner = self.make_runner('--pool-clusters')

        # TERMINATING clusters aren't queried
        self.assertEqual(
            runner._list_cluster_ids_for_pooling(),
            dict(
                available=[],
                in_pool=set(),
                matching=set(),
                max_created=None,
            )
        )

    def test_terminated_cluster(self):
        cluster_id = self.make_runner(
            '--pool-clusters').make_persistent_cluster()

        cluster = self.mock_emr_clusters[cluster_id]
        cluster['Status']['State'] = 'TERMINATED'

        runner = self.make_runner('--pool-clusters')

        self.assertEqual(
            runner._list_cluster_ids_for_pooling(),
            dict(
                available=[],
                in_pool=set(),
                matching=set(),
                max_created=None,
            )
        )

    def test_terminated_with_errors_cluster(self):
        cluster_id = self.make_runner(
            '--pool-clusters').make_persistent_cluster()

        cluster = self.mock_emr_clusters[cluster_id]
        cluster['Status']['State'] = 'TERMINATED_WITH_ERRORS'

        runner = self.make_runner('--pool-clusters')

        self.assertEqual(
            runner._list_cluster_ids_for_pooling(),
            dict(
                available=[],
                in_pool=set(),
                matching=set(),
                max_created=None,
            )
        )

    def test_non_matching_cluster(self):
        cluster_id = self.make_runner(
            '--pool-clusters', '--bootstrap', 'true').make_persistent_cluster()

        cluster = self.mock_emr_clusters[cluster_id]
        cluster['Status']['State'] = 'WAITING'

        runner = self.make_runner('--pool-clusters')

        self.assertEqual(
            runner._list_cluster_ids_for_pooling(),
            dict(
                available=[],
                in_pool={cluster_id},
                matching=set(),
                max_created=cluster['Status']['Timeline']['CreationDateTime'],
            )
        )

    def test_cluster_in_other_pool(self):
        cluster_id = self.make_runner(
            '--pool-clusters').make_persistent_cluster()

        cluster = self.mock_emr_clusters[cluster_id]
        cluster['Status']['State'] = 'WAITING'

        runner = self.make_runner('--pool-clusters', '--pool-name', 'mine')

        self.assertEqual(
            runner._list_cluster_ids_for_pooling(),
            dict(
                available=[],
                in_pool=set(),
                matching=set(),
                max_created=cluster['Status']['Timeline']['CreationDateTime'],
            )
        )

    def test_max_created(self):
        # max_created is the maximum creation time for clusters we list,
        # even if they're not in the pool

        cluster_id_1 = self.make_runner(
            '--pool-clusters').make_persistent_cluster()
        cluster_1 = self.mock_emr_clusters[cluster_id_1]

        sleep(0.0001)

        cluster_id_2 = self.make_runner(
            '--pool-clusters', '--pool-name', 'mine').make_persistent_cluster()
        cluster_2 = self.mock_emr_clusters[cluster_id_2]

        sleep(0.0001)

        cluster_id_3 = self.make_runner(
            '--pool-clusters').make_persistent_cluster()

        # TERMINATING clusters aren't even listed
        cluster_3 = self.mock_emr_clusters[cluster_id_3]
        cluster_3['Status']['State'] = 'TERMINATING'

        runner = self.make_runner('--pool-clusters')

        self.assertEqual(
            runner._list_cluster_ids_for_pooling(),
            dict(
                available=[],
                in_pool={cluster_id_1},
                matching={cluster_id_1},
                max_created=cluster_2[
                    'Status']['Timeline']['CreationDateTime'],
            )
        )

    def test_created_after(self):
        cluster_id_1 = self.make_runner(
            '--pool-clusters').make_persistent_cluster()
        cluster_1 = self.mock_emr_clusters[cluster_id_1]

        sleep(0.0001)

        cluster_id_2 = self.make_runner(
            '--pool-clusters').make_persistent_cluster()
        cluster_2 = self.mock_emr_clusters[cluster_id_2]

        created_after = (
            cluster_1['Status']['Timeline']['CreationDateTime'] +
            timedelta(microseconds=500))

        runner = self.make_runner('--pool-clusters')

        self.assertEqual(
            runner._list_cluster_ids_for_pooling(created_after=created_after),
            dict(
                available=[],
                in_pool={cluster_id_2},
                matching={cluster_id_2},
                max_created=cluster_2[
                    'Status']['Timeline']['CreationDateTime'],
            )
        )
