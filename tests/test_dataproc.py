# -*- coding: utf-8 -*-
# Copyright 2009-2017 Yelp and Contributors
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
"""Tests for DataprocJobRunner"""
import collections
import copy
import getpass
import os
import os.path

from contextlib import contextmanager
from io import BytesIO

import mrjob
import mrjob.dataproc
from mrjob.dataproc import DataprocException
from mrjob.dataproc import DataprocJobRunner
from mrjob.dataproc import _DATAPROC_API_REGION
from mrjob.dataproc import _DEFAULT_CLOUD_TMP_DIR_OBJECT_TTL_DAYS
from mrjob.dataproc import _DEFAULT_IMAGE_VERSION
from mrjob.dataproc import _MAX_HOURS_IDLE_BOOTSTRAP_ACTION_PATH
from mrjob.fs.gcs import parse_gcs_uri
from mrjob.py2 import PY2
from mrjob.py2 import StringIO
from mrjob.step import StepFailedException
from mrjob.tools.emr.audit_usage import _JOB_KEY_RE
from mrjob.util import log_to_stream

from tests.mockgoogleapiclient import MockGoogleAPITestCase
from tests.mockgoogleapiclient import _TEST_PROJECT
from tests.mr_hadoop_format_job import MRHadoopFormatJob
from tests.mr_no_mapper import MRNoMapper
from tests.mr_two_step_job import MRTwoStepJob
from tests.mr_word_count import MRWordCount
from tests.py2 import mock
from tests.py2 import patch
from tests.quiet import logger_disabled
from tests.quiet import no_handlers_for_logger
from tests.sandbox import mrjob_conf_patcher

try:
    from oauth2client.client import GoogleCredentials
    from googleapiclient import discovery
except ImportError:
    # don't require googleapiclient; MRJobs don't actually need it when running
    # inside hadoop streaming
    GoogleCredentials = None
    discovery = None

# used to match command lines
if PY2:
    PYTHON_BIN = 'python'
else:
    PYTHON_BIN = 'python3'

DEFAULT_GCE_REGION = 'us-central1'
US_EAST_GCE_REGION = 'us-east1'
EU_WEST_GCE_REGION = 'europe-west1'

DEFAULT_GCE_INSTANCE = 'n1-standard-1'
HIGHMEM_GCE_INSTANCE = 'n1-highmem-2'
HIGHCPU_GCE_INSTANCE = 'n1-highcpu-2'
MICRO_GCE_INSTANCE = 'f1-micro'


class DataprocJobRunnerEndToEndTestCase(MockGoogleAPITestCase):

    MRJOB_CONF_CONTENTS = {'runners': {'dataproc': {
        'check_cluster_every': 0.00,
        'cloud_fs_sync_secs': 0.00,
    }}}

    def test_end_to_end(self):
        # read from STDIN, a local file, and a remote file
        stdin = BytesIO(b'foo\nbar\n')

        local_input_path = os.path.join(self.tmp_dir, 'input')
        with open(local_input_path, 'wb') as local_input_file:
            local_input_file.write(b'bar\nqux\n')

        remote_input_path = 'gs://walrus/data/foo'
        self.put_gcs_multi({
            remote_input_path: b'foo\n'
        })

        mr_job = MRHadoopFormatJob(['-r', 'dataproc', '-v',
                                    '-', local_input_path, remote_input_path,
                                    '--jobconf', 'x=y'])
        mr_job.sandbox(stdin=stdin)

        results = []

        gcs_buckets_snapshot = copy.deepcopy(self._gcs_client._cache_buckets)
        gcs_objects_snapshot = copy.deepcopy(self._gcs_client._cache_objects)

        fake_gcs_output = [
            b'1\t"qux"\n2\t"bar"\n',
            b'2\t"foo"\n5\tnull\n'
        ]

        with mr_job.make_runner() as runner:
            self.assertIsInstance(runner, DataprocJobRunner)

            # make sure that initializing the runner doesn't affect GCS
            # (Issue #50)
            self.assertEqual(gcs_buckets_snapshot,
                             self._gcs_client._cache_buckets)
            self.assertEqual(gcs_objects_snapshot,
                             self._gcs_client._cache_objects)

            runner.run()

            # setup fake output
            self.put_job_output_parts(runner, fake_gcs_output)

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

            local_tmp_dir = runner._get_local_tmp_dir()
            # make sure cleanup hasn't happened yet
            self.assertTrue(os.path.exists(local_tmp_dir))
            self.assertTrue(any(runner.fs.ls(runner.get_output_dir())))

            name_match = _JOB_KEY_RE.match(runner._job_key)
            self.assertEqual(name_match.group(1), 'mr_hadoop_format_job')
            self.assertEqual(name_match.group(2), getpass.getuser())

            # make sure our input and output formats are attached to
            # the correct steps
            jobs_list = runner.api_client.jobs().list(
                projectId=runner._gcp_project,
                region=_DATAPROC_API_REGION).execute()
            jobs = jobs_list['items']

            step_0_args = jobs[0]['hadoopJob']['args']
            step_1_args = jobs[1]['hadoopJob']['args']

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

            cluster_id = runner.get_cluster_id()

        self.assertEqual(sorted(results),
                         [(1, 'qux'), (2, 'bar'), (2, 'foo'), (5, None)])

        # make sure cleanup happens
        self.assertFalse(os.path.exists(local_tmp_dir))

        # we don't clean-up the output dir as we're relying on lifecycle
        # management
        output_dirs = list(runner.fs.ls(runner.get_output_dir()))
        self.assertEqual(len(fake_gcs_output), len(output_dirs))

        # job should get terminated
        cluster = (
            self._dataproc_client._cache_clusters[_TEST_PROJECT][cluster_id])
        cluster_state = self._dataproc_client.get_state(cluster)
        self.assertEqual(cluster_state, 'DELETING')

    def test_failed_job(self):
        mr_job = MRTwoStepJob(['-r', 'dataproc', '-v'])
        mr_job.sandbox()

        with no_handlers_for_logger('mrjob.dataproc'):
            stderr = StringIO()
            log_to_stream('mrjob.dataproc', stderr)

            self._dataproc_client.job_get_advances_states = (
                collections.deque(['SETUP_DONE', 'RUNNING', 'ERROR']))

            with mr_job.make_runner() as runner:
                self.assertIsInstance(runner, DataprocJobRunner)

                self.assertRaises(StepFailedException, runner.run)

                self.assertIn(' => ERROR\n', stderr.getvalue())

                cluster_id = runner.get_cluster_id()

        # job should get terminated
        cluster = (
            self._dataproc_client._cache_clusters[_TEST_PROJECT][cluster_id])
        cluster_state = self._dataproc_client.get_state(cluster)
        self.assertEqual(cluster_state, 'DELETING')

    def _test_cloud_tmp_cleanup(self, mode, tmp_len):
        stdin = BytesIO(b'foo\nbar\n')

        mr_job = MRTwoStepJob(['-r', 'dataproc', '-v',
                               '-', '--cleanup', mode])
        mr_job.sandbox(stdin=stdin)

        with mr_job.make_runner() as runner:
            tmp_bucket, _ = parse_gcs_uri(runner._cloud_tmp_dir)

            runner.run()

            # this is set and unset before we can get at it unless we do this
            list(runner.stream_output())

        objects_in_bucket = self._gcs_fs.api_client._cache_objects[tmp_bucket]
        self.assertEqual(len(objects_in_bucket), tmp_len)

    def test_cleanup_all(self):
        self._test_cloud_tmp_cleanup('ALL', 0)

    def test_cleanup_tmp(self):
        self._test_cloud_tmp_cleanup('TMP', 0)

    def test_cleanup_remote(self):
        self._test_cloud_tmp_cleanup('CLOUD_TMP', 0)

    def test_cleanup_local(self):
        self._test_cloud_tmp_cleanup('LOCAL_TMP', 5)

    def test_cleanup_logs(self):
        self._test_cloud_tmp_cleanup('LOGS', 5)

    def test_cleanup_none(self):
        self._test_cloud_tmp_cleanup('NONE', 5)

    def test_cleanup_combine(self):
        self._test_cloud_tmp_cleanup('LOGS,CLOUD_TMP', 0)

    def test_cleanup_error(self):
        self.assertRaises(ValueError, self._test_cloud_tmp_cleanup,
                          'NONE,LOGS,CLOUD_TMP', 0)
        self.assertRaises(ValueError, self._test_cloud_tmp_cleanup,
                          'GARBAGE', 0)


class ExistingClusterTestCase(MockGoogleAPITestCase):

    def test_attach_to_existing_cluster(self):
        runner = DataprocJobRunner(conf_paths=[])

        cluster_body = runner.api_client.cluster_create()
        cluster_id = cluster_body['clusterName']

        stdin = BytesIO(b'foo\nbar\n')

        mr_job = MRTwoStepJob(['-r', 'dataproc', '-v',
                               '--cluster-id', cluster_id])
        mr_job.sandbox(stdin=stdin)

        results = []

        with mr_job.make_runner() as runner:
            runner.run()

            # Generate fake output
            self.put_job_output_parts(runner, [
                b'1\t"bar"\n1\t"foo"\n2\tnull\n'
            ])

            # Issue 182: don't create the bootstrap script when
            # attaching to another cluster
            self.assertIsNone(runner._master_bootstrap_script_path)

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

        self.assertEqual(sorted(results),
                         [(1, 'bar'), (1, 'foo'), (2, None)])

    def test_dont_take_down_cluster_on_failure(self):
        runner = DataprocJobRunner(conf_paths=[])

        cluster_body = runner.api_client.cluster_create()
        cluster_id = cluster_body['clusterName']

        mr_job = MRTwoStepJob(['-r', 'dataproc', '-v',
                               '--cluster-id', cluster_id])
        mr_job.sandbox()

        self._dataproc_client.job_get_advances_states = (
            collections.deque(['SETUP_DONE', 'RUNNING', 'ERROR']))

        with mr_job.make_runner() as runner:
            self.assertIsInstance(runner, DataprocJobRunner)

            with logger_disabled('mrjob.dataproc'):
                self.assertRaises(StepFailedException, runner.run)

            cluster = self.get_cluster_from_runner(runner, cluster_id)
            cluster_state = self._dataproc_client.get_state(cluster)
            self.assertEqual(cluster_state, 'RUNNING')

        # job shouldn't get terminated by cleanup
        cluster = (
            self._dataproc_client._cache_clusters[_TEST_PROJECT][cluster_id])
        cluster_state = self._dataproc_client.get_state(cluster)
        self.assertEqual(cluster_state, 'RUNNING')


class CloudAndHadoopVersionTestCase(MockGoogleAPITestCase):

    def test_default(self):
        with self.make_runner() as runner:
            runner.run()
            self.assertEqual(runner.get_image_version(),
                             _DEFAULT_IMAGE_VERSION)
            self.assertEqual(runner.get_hadoop_version(), '2.7.2')

    def test_image_0_1(self):
        self._assert_cloud_hadoop_version('0.1', '2.7.1')

    def test_image_0_2(self):
        self._assert_cloud_hadoop_version('0.2', '2.7.1')

    def test_image_1_0(self):
        self._assert_cloud_hadoop_version('1.0', '2.7.2')

    def test_image_1_0_11(self):
        # regression test for #1428
        self._assert_cloud_hadoop_version('1.0.11', '2.7.2')

    def test_future_proofing(self):
        self._assert_cloud_hadoop_version('5.0', '2.7.2')

    def _assert_cloud_hadoop_version(self, image_version, hadoop_version):
        with self.make_runner('--image-version', image_version) as runner:
            runner.run()
            self.assertEqual(runner.get_image_version(), image_version)
            self.assertEqual(runner.get_hadoop_version(), hadoop_version)

    def test_hadoop_version_option_does_nothing(self):
        with logger_disabled('mrjob.dataproc'):
            with self.make_runner('--hadoop-version', '1.2.3.4') as runner:
                runner.run()
                self.assertEqual(runner.get_image_version(),
                                 _DEFAULT_IMAGE_VERSION)
                self.assertEqual(runner.get_hadoop_version(), '2.7.2')


EXPECTED_ZONE = 'PUPPYLAND'


class ZoneTestCase(MockGoogleAPITestCase):

    MRJOB_CONF_CONTENTS = {'runners': {'dataproc': {
        'check_cluster_every': 0.00,
        'cloud_fs_sync_secs': 0.00,
        'zone': EXPECTED_ZONE,
    }}}

    def test_availability_zone_config(self):
        with self.make_runner() as runner:
            runner.run()

            cluster = runner._api_cluster_get(runner._cluster_id)
            self.assertIn(EXPECTED_ZONE,
                          cluster['config']['gceClusterConfig']['zoneUri'])
            self.assertIn(EXPECTED_ZONE,
                          cluster['config']['masterConfig']['machineTypeUri'])
            self.assertIn(EXPECTED_ZONE,
                          cluster['config']['workerConfig']['machineTypeUri'])


class RegionTestCase(MockGoogleAPITestCase):

    def test_default(self):
        runner = DataprocJobRunner()
        self.assertEqual(runner._gce_region, 'us-central1')

    def test_explicit_region(self):
        runner = DataprocJobRunner(region='europe-west1')
        self.assertEqual(runner._gce_region, 'europe-west1')

    def test_cannot_be_empty(self):
        runner = DataprocJobRunner(region='')
        self.assertEqual(runner._gce_region, 'us-central1')


class TmpBucketTestCase(MockGoogleAPITestCase):
    def assert_new_tmp_bucket(self, location, **runner_kwargs):
        """Assert that if we create an DataprocJobRunner with the given keyword
        args, it'll create a new tmp bucket with the given location
        constraint.
        """
        bucket_cache = self._gcs_client._cache_buckets

        existing_buckets = set(bucket_cache.keys())

        runner = DataprocJobRunner(conf_paths=[], **runner_kwargs)

        bucket_name, path = parse_gcs_uri(runner._cloud_tmp_dir)
        runner._create_fs_tmp_bucket(bucket_name, location=location)

        self.assertTrue(bucket_name.startswith('mrjob-'))
        self.assertNotIn(bucket_name, existing_buckets)
        self.assertEqual(path, 'tmp/')

        current_bucket = bucket_cache[bucket_name]
        self.assertEqual(current_bucket['location'], location)

        # Verify that we setup bucket lifecycle rules of 28-day retention
        first_lifecycle_rule = current_bucket['lifecycle']['rule'][0]
        self.assertEqual(first_lifecycle_rule['action'], dict(type='Delete'))
        self.assertEqual(first_lifecycle_rule['condition'],
                         dict(age=_DEFAULT_CLOUD_TMP_DIR_OBJECT_TTL_DAYS))

    def _make_bucket(self, name, location=None):
        self._gcs_fs.create_bucket(
            project=_TEST_PROJECT, name=name, location=location)

    def test_default(self):
        self.assert_new_tmp_bucket(DEFAULT_GCE_REGION)

    def test_us_east_1(self):
        self.assert_new_tmp_bucket(US_EAST_GCE_REGION,
                                   region=US_EAST_GCE_REGION)

    def test_europe_west_1(self):
        # location should be blank
        self.assert_new_tmp_bucket(EU_WEST_GCE_REGION,
                                   region=EU_WEST_GCE_REGION)

    def test_reuse_mrjob_bucket_in_same_region(self):
        self._make_bucket('mrjob-1', DEFAULT_GCE_REGION)

        runner = DataprocJobRunner()
        self.assertEqual(runner._cloud_tmp_dir, 'gs://mrjob-1/tmp/')

    def test_ignore_mrjob_bucket_in_different_region(self):
        # this tests 687
        self._make_bucket('mrjob-1', US_EAST_GCE_REGION)

        self.assert_new_tmp_bucket(DEFAULT_GCE_REGION)

    def test_ignore_non_mrjob_bucket_in_different_region(self):
        self._make_bucket('walrus', US_EAST_GCE_REGION)

        self.assert_new_tmp_bucket(DEFAULT_GCE_REGION)

    def test_explicit_tmp_uri(self):
        self._make_bucket('walrus', US_EAST_GCE_REGION)

        runner = DataprocJobRunner(cloud_tmp_dir='gs://walrus/tmp/')

        self.assertEqual(runner._cloud_tmp_dir, 'gs://walrus/tmp/')

    def test_cross_region_explicit_tmp_uri(self):
        self._make_bucket('walrus', EU_WEST_GCE_REGION)

        runner = DataprocJobRunner(region=US_EAST_GCE_REGION,
                                   cloud_tmp_dir='gs://walrus/tmp/')

        self.assertEqual(runner._cloud_tmp_dir, 'gs://walrus/tmp/')

        # tmp bucket shouldn't influence region (it did in 0.4.x)
        self.assertEqual(runner._gce_region, US_EAST_GCE_REGION)


class GCEInstanceGroupTestCase(MockGoogleAPITestCase):

    maxDiff = None

    def _gce_instance_group_summary(self, instance_group):
        if not instance_group:
            return (0, None)

        num_instances = instance_group['numInstances']
        instance_type = instance_group['machineTypeUri'].split('/')[-1]
        return (num_instances, instance_type)

    def _test_instance_groups(self, opts, **kwargs):
        """Run a job with the given option dictionary, and check for
        for instance, number, and optional bid price for each instance role.

        Specify expected instance group info like:

        <role>=(num_instances, instance_type, bid_price)
        """
        runner = DataprocJobRunner(**opts)

        # cluster_body = runner.api_client.cluster_create()
        fake_bootstrap_script = 'gs://fake-bucket/fake-script.sh'
        runner._master_bootstrap_script_path = fake_bootstrap_script
        runner._upload_mgr.add(fake_bootstrap_script)
        runner._upload_mgr.add(_MAX_HOURS_IDLE_BOOTSTRAP_ACTION_PATH)

        cluster_id = runner._launch_cluster()

        cluster_body = runner._api_cluster_get(cluster_id)

        conf = cluster_body['config']

        role_to_actual = dict(
            master=self._gce_instance_group_summary(conf['masterConfig']),
            core=self._gce_instance_group_summary(conf['workerConfig']),
            task=self._gce_instance_group_summary(
                conf.get('secondaryWorkerConfig'))
        )

        role_to_expected = kwargs.copy()
        role_to_expected.setdefault('master', (1, DEFAULT_GCE_INSTANCE))
        role_to_expected.setdefault('core', (2, DEFAULT_GCE_INSTANCE))
        role_to_expected.setdefault(
            'task', self._gce_instance_group_summary(dict()))
        self.assertEqual(role_to_actual, role_to_expected)

    def set_in_mrjob_conf(self, **kwargs):
        dataproc_opts = copy.deepcopy(self.MRJOB_CONF_CONTENTS)
        dataproc_opts['runners']['dataproc'].update(kwargs)
        patcher = mrjob_conf_patcher(dataproc_opts)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_defaults(self):
        self._test_instance_groups(
            {},
            master=(1, DEFAULT_GCE_INSTANCE))

        self._test_instance_groups(
            {'num_core_instances': 2},
            core=(2, DEFAULT_GCE_INSTANCE),
            master=(1, DEFAULT_GCE_INSTANCE))

    def test_multiple_instances(self):
        self._test_instance_groups(
            {'instance_type': HIGHCPU_GCE_INSTANCE, 'num_core_instances': 5},
            core=(5, HIGHCPU_GCE_INSTANCE),
            master=(1, DEFAULT_GCE_INSTANCE))

    def test_explicit_master_and_slave_instance_types(self):
        self._test_instance_groups(
            {'master_instance_type': MICRO_GCE_INSTANCE},
            master=(1, MICRO_GCE_INSTANCE))

        self._test_instance_groups(
            {'instance_type': HIGHMEM_GCE_INSTANCE,
             'num_core_instances': 2},
            core=(2, HIGHMEM_GCE_INSTANCE),
            master=(1, DEFAULT_GCE_INSTANCE))

        self._test_instance_groups(
            {'master_instance_type': MICRO_GCE_INSTANCE,
             'instance_type': HIGHMEM_GCE_INSTANCE,
             'num_core_instances': 2},
            core=(2, HIGHMEM_GCE_INSTANCE),
            master=(1, MICRO_GCE_INSTANCE))

    def test_explicit_instance_types_take_precedence(self):
        self._test_instance_groups(
            {'instance_type': HIGHCPU_GCE_INSTANCE,
             'master_instance_type': MICRO_GCE_INSTANCE},
            master=(1, MICRO_GCE_INSTANCE),
            core=(2, HIGHCPU_GCE_INSTANCE)
        )

    def test_cmd_line_opts_beat_mrjob_conf(self):
        # set instance_type in mrjob.conf, 1 instance
        self.set_in_mrjob_conf(master_instance_type=HIGHCPU_GCE_INSTANCE)

        self._test_instance_groups(
            {},
            master=(1, HIGHCPU_GCE_INSTANCE),
        )

        self._test_instance_groups(
            {'master_instance_type': MICRO_GCE_INSTANCE},
            master=(1, MICRO_GCE_INSTANCE)
        )

        # set instance_type in mrjob.conf, 3 instances
        self.set_in_mrjob_conf(instance_type=HIGHCPU_GCE_INSTANCE,
                               num_core_instances=2)

        self._test_instance_groups(
            {},
            master=(1, DEFAULT_GCE_INSTANCE),
            core=(2, HIGHCPU_GCE_INSTANCE)
        )

        self._test_instance_groups(
            {'master_instance_type': MICRO_GCE_INSTANCE,
             'instance_type': HIGHMEM_GCE_INSTANCE},
            master=(1, MICRO_GCE_INSTANCE),
            core=(2, HIGHMEM_GCE_INSTANCE)
        )

        # set master in mrjob.conf, 1 instance
        self.set_in_mrjob_conf(master_instance_type=MICRO_GCE_INSTANCE)

        self._test_instance_groups(
            {},
            master=(1, MICRO_GCE_INSTANCE))

        self._test_instance_groups(
            {'master_instance_type': HIGHCPU_GCE_INSTANCE},
            master=(1, HIGHCPU_GCE_INSTANCE))

        # set master and slave in mrjob.conf, 2 instances
        self.set_in_mrjob_conf(master_instance_type=MICRO_GCE_INSTANCE,
                               instance_type=HIGHMEM_GCE_INSTANCE,
                               num_core_instances=2)

        self._test_instance_groups(
            {},
            core=(2, HIGHMEM_GCE_INSTANCE),
            master=(1, MICRO_GCE_INSTANCE))

        self._test_instance_groups(
            {'instance_type': HIGHCPU_GCE_INSTANCE},
            core=(2, HIGHCPU_GCE_INSTANCE),
            master=(1, MICRO_GCE_INSTANCE))

        self._test_instance_groups(
            {'instance_type': HIGHMEM_GCE_INSTANCE},
            core=(2, HIGHMEM_GCE_INSTANCE),
            master=(1, MICRO_GCE_INSTANCE))

    def test_core_and_task_on_demand_instances(self):
        self.assertRaises(
            DataprocException,
            self._test_instance_groups,
            {'master_instance_type': MICRO_GCE_INSTANCE,
             'core_instance_type': HIGHCPU_GCE_INSTANCE,
             'task_instance_type': HIGHMEM_GCE_INSTANCE,
             'num_core_instances': 5,
             'num_task_instances': 20,
             },
            master=(1, MICRO_GCE_INSTANCE),
            core=(5, HIGHCPU_GCE_INSTANCE),
            task=(20, HIGHMEM_GCE_INSTANCE)
        )

    def test_task_type_defaults_to_core_type(self):
        self._test_instance_groups(
            {'instance_type': HIGHCPU_GCE_INSTANCE,
             'num_core_instances': 5,
             'num_task_instances': 20,
             },
            master=(1, DEFAULT_GCE_INSTANCE),
            core=(5, HIGHCPU_GCE_INSTANCE),
            task=(20, HIGHCPU_GCE_INSTANCE))


class MasterBootstrapScriptTestCase(MockGoogleAPITestCase):

    def test_usr_bin_env(self):
        runner = DataprocJobRunner(conf_paths=[],
                                   bootstrap_mrjob=True,
                                   sh_bin='bash -e')

        runner._add_bootstrap_files_for_upload()

        self.assertIsNotNone(runner._master_bootstrap_script_path)
        self.assertTrue(os.path.exists(runner._master_bootstrap_script_path))

        with open(runner._master_bootstrap_script_path) as f:
            lines = [line.rstrip() for line in f]

        self.assertEqual(lines[0], '#!/usr/bin/env bash -e')

    def test_create_master_bootstrap_script(self):
        # create a fake src tarball
        foo_py_path = os.path.join(self.tmp_dir, 'foo.py')
        with open(foo_py_path, 'w'):
            pass

        # use all the bootstrap options
        runner = DataprocJobRunner(
            conf_paths=[],
            bootstrap=[
                PYTHON_BIN + ' ' +
                foo_py_path + '#bar.py',
                'gs://walrus/scripts/ohnoes.sh#',
                # bootstrap_cmds
                'echo "Hi!"',
                'true',
                'ls',
                # bootstrap_scripts
                'speedups.sh',
                '/tmp/s.sh'
            ],
            bootstrap_mrjob=True)

        runner._add_bootstrap_files_for_upload()

        self.assertIsNotNone(runner._master_bootstrap_script_path)
        self.assertTrue(os.path.exists(runner._master_bootstrap_script_path))

        with open(runner._master_bootstrap_script_path) as f:
            lines = [line.rstrip() for line in f]

        self.assertEqual(lines[0], '#!/bin/sh -ex')

        # check PWD gets stored
        self.assertIn('__mrjob_PWD=$PWD', lines)

        def assertScriptDownloads(path, name=None):
            uri = runner._upload_mgr.uri(path)
            name = runner._bootstrap_dir_mgr.name('file', path, name=name)

            self.assertIn(
                'hadoop fs -copyToLocal %s $__mrjob_PWD/%s' % (uri, name),
                lines)
            self.assertIn(
                'chmod a+x $__mrjob_PWD/%s' % (name,),
                lines)

        # check files get downloaded
        assertScriptDownloads(foo_py_path, 'bar.py')
        assertScriptDownloads('gs://walrus/scripts/ohnoes.sh')
        assertScriptDownloads(runner._mrjob_zip_path)

        # check scripts get run

        # bootstrap
        self.assertIn(PYTHON_BIN + ' $__mrjob_PWD/bar.py', lines)
        self.assertIn('$__mrjob_PWD/ohnoes.sh', lines)

        self.assertIn('echo "Hi!"', lines)
        self.assertIn('true', lines)
        self.assertIn('ls', lines)

        self.assertIn('speedups.sh', lines)
        self.assertIn('/tmp/s.sh', lines)

        # bootstrap_mrjob
        mrjob_zip_name = runner._bootstrap_dir_mgr.name(
            'file', runner._mrjob_zip_path)
        self.assertIn("__mrjob_PYTHON_LIB=$(" + PYTHON_BIN + " -c 'from"
                      " distutils.sysconfig import get_python_lib;"
                      " print(get_python_lib())')", lines)
        self.assertIn('sudo unzip $__mrjob_PWD/' + mrjob_zip_name +
                      ' -d $__mrjob_PYTHON_LIB', lines)
        self.assertIn('sudo ' + PYTHON_BIN + ' -m compileall -q -f'
                      ' $__mrjob_PYTHON_LIB/mrjob && true', lines)
        # bootstrap_python
        if PY2:
            self.assertIn(
                'sudo apt-get install -y python-pip python-dev',
                lines)
        else:
            self.assertIn(
                'sudo apt-get install -y python3 python3-pip python3-dev',
                lines)

    def test_no_bootstrap_script_if_not_needed(self):
        runner = DataprocJobRunner(conf_paths=[], bootstrap_mrjob=False,
                                   bootstrap_python=False)

        runner._add_bootstrap_files_for_upload()
        self.assertIsNone(runner._master_bootstrap_script_path)

    def test_bootstrap_mrjob_uses_python_bin(self):
        # use all the bootstrap options
        runner = DataprocJobRunner(conf_paths=[],
                                   bootstrap_mrjob=True,
                                   python_bin=['anaconda'])

        runner._add_bootstrap_files_for_upload()
        self.assertIsNotNone(runner._master_bootstrap_script_path)
        with open(runner._master_bootstrap_script_path, 'r') as f:
            content = f.read()

        self.assertIn('sudo anaconda -m compileall -q -f', content)

    def test_bootstrap_script_respects_sh_bin(self):
        runner = DataprocJobRunner(conf_paths=[])

        self.start(patch('mrjob.dataproc.DataprocJobRunner._sh_bin',
                         return_value=['/bin/bash']))
        runner._add_bootstrap_files_for_upload()
        self.assertIsNotNone(runner._master_bootstrap_script_path)
        with open(runner._master_bootstrap_script_path) as f:
            lines = list(f)

        self.assertEqual(lines[0].strip(), '#!/bin/bash')

    def test_bootstrap_script_respects_sh_pre_commands(self):
        runner = DataprocJobRunner(conf_paths=[])

        self.start(patch('mrjob.dataproc.DataprocJobRunner._sh_pre_commands',
                         return_value=['garply', 'quux']))
        runner._add_bootstrap_files_for_upload()
        self.assertIsNotNone(runner._master_bootstrap_script_path)
        with open(runner._master_bootstrap_script_path) as f:
            lines = list(f)

        self.assertEqual([line.strip() for line in lines[1:3]],
                         ['garply', 'quux'])


class DataprocNoMapperTestCase(MockGoogleAPITestCase):

    def test_no_mapper(self):
        # read from STDIN, a local file, and a remote file
        stdin = BytesIO(b'foo\nbar\n')

        local_input_path = os.path.join(self.tmp_dir, 'input')
        with open(local_input_path, 'wb') as local_input_file:
            local_input_file.write(b'one fish two fish\nred fish blue fish\n')

        remote_input_path = 'gs://walrus/data/foo'
        self.put_gcs_multi({
            remote_input_path: b'foo\n'
        })

        mr_job = MRNoMapper(['-r', 'dataproc', '-v',
                             '-', local_input_path, remote_input_path])
        mr_job.sandbox(stdin=stdin)

        results = []

        with mr_job.make_runner() as runner:
            runner.run()

            # setup fake output
            self.put_job_output_parts(runner, [
                b'1\t["blue", "one", "red", "two"]\n',
                b'4\t["fish"]\n'])

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

        self.assertEqual(sorted(results),
                         [(1, ['blue', 'one', 'red', 'two']),
                          (4, ['fish'])])


class MaxHoursIdleTestCase(MockGoogleAPITestCase):

    def assertRanIdleTimeoutScriptWith(self, runner, expected_metadata):
        cluster_metadata, last_init_exec = (
            self._cluster_metadata_and_last_init_exec(runner))

        # Verify args
        for key in expected_metadata.keys():
            self.assertEqual(cluster_metadata[key], expected_metadata[key])

        expected_uri = runner._upload_mgr.uri(
            _MAX_HOURS_IDLE_BOOTSTRAP_ACTION_PATH)
        self.assertEqual(last_init_exec, expected_uri)

    def _cluster_metadata_and_last_init_exec(self, runner):
        cluster_id = runner.get_cluster_id()

        cluster = self.get_cluster_from_runner(runner, cluster_id)

        # Verify last arg
        cluster_config = cluster['config']
        last_init_action = cluster_config['initializationActions'][-1]
        last_init_exec = last_init_action['executableFile']

        cluster_metadata = cluster_config['gceClusterConfig']['metadata']
        return cluster_metadata, last_init_exec

    def test_default(self):
        mr_job = MRWordCount(['-r', 'dataproc'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.run()
            self.assertRanIdleTimeoutScriptWith(runner, {
                'mrjob-max-secs-idle': '360',
            })

    def test_persistent_cluster(self):
        mr_job = MRWordCount(['-r', 'dataproc', '--max-hours-idle', '0.01'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.run()
            self.assertRanIdleTimeoutScriptWith(runner, {
                'mrjob-max-secs-idle': '36',
            })

    def test_bootstrap_script_is_actually_installed(self):
        self.assertTrue(os.path.exists(_MAX_HOURS_IDLE_BOOTSTRAP_ACTION_PATH))


class TestCatFallback(MockGoogleAPITestCase):

    def test_gcs_cat(self):
        self.put_gcs_multi({
            'gs://walrus/one': b'one_text',
            'gs://walrus/two': b'two_text',
            'gs://walrus/three': b'three_text',
        })

        runner = DataprocJobRunner(cloud_tmp_dir='gs://walrus/tmp',
                                   conf_paths=[])

        self.assertEqual(list(runner.fs.cat('gs://walrus/one')), [b'one_text'])


class CleanUpJobTestCase(MockGoogleAPITestCase):

    @contextmanager
    def _test_mode(self, mode):
        r = DataprocJobRunner(conf_paths=[])
        with patch.multiple(r,
                            _cleanup_cluster=mock.DEFAULT,
                            _cleanup_job=mock.DEFAULT,
                            _cleanup_local_tmp=mock.DEFAULT,
                            _cleanup_logs=mock.DEFAULT,
                            _cleanup_cloud_tmp=mock.DEFAULT) as mock_dict:
            r.cleanup(mode=mode)
            yield mock_dict

    def _quick_runner(self):
        r = DataprocJobRunner(conf_paths=[])
        r._cluster_id = 'j-ESSEOWENS'
        r._ran_job = False
        return r

    def test_cleanup_all(self):
        with self._test_mode('ALL') as m:
            self.assertFalse(m['_cleanup_cluster'].called)
            self.assertFalse(m['_cleanup_job'].called)
            self.assertTrue(m['_cleanup_local_tmp'].called)
            self.assertTrue(m['_cleanup_cloud_tmp'].called)
            self.assertTrue(m['_cleanup_logs'].called)

    def test_cleanup_job(self):
        with self._test_mode('JOB') as m:
            self.assertFalse(m['_cleanup_cluster'].called)
            self.assertFalse(m['_cleanup_local_tmp'].called)
            self.assertFalse(m['_cleanup_cloud_tmp'].called)
            self.assertFalse(m['_cleanup_logs'].called)
            self.assertFalse(m['_cleanup_job'].called)  # Only on failure

    def test_cleanup_none(self):
        with self._test_mode('NONE') as m:
            self.assertFalse(m['_cleanup_cluster'].called)
            self.assertFalse(m['_cleanup_local_tmp'].called)
            self.assertFalse(m['_cleanup_cloud_tmp'].called)
            self.assertFalse(m['_cleanup_logs'].called)
            self.assertFalse(m['_cleanup_job'].called)

    def test_kill_cluster(self):
        with no_handlers_for_logger('mrjob.dataproc'):
            r = self._quick_runner()
            with patch.object(mrjob.dataproc.DataprocJobRunner,
                              '_api_cluster_delete') as m:
                r._cleanup_cluster()
                self.assertTrue(m.called)

    def test_kill_cluster_if_successful(self):
        # If they are setting up the cleanup to kill the cluster, mrjob should
        # kill the cluster independent of job success.
        with no_handlers_for_logger('mrjob.dataproc'):
            r = self._quick_runner()
            with patch.object(mrjob.dataproc.DataprocJobRunner,
                              '_api_cluster_delete') as m:
                r._ran_job = True
                r._cleanup_cluster()
                self.assertTrue(m.called)

    def test_kill_persistent_cluster(self):
        with no_handlers_for_logger('mrjob.dataproc'):
            r = self._quick_runner()
            with patch.object(mrjob.dataproc.DataprocJobRunner,
                              '_api_cluster_delete') as m:
                r._opts['cluster_id'] = 'j-MOCKCLUSTER0'
                r._cleanup_cluster()
                self.assertTrue(m.called)


class BootstrapPythonTestCase(MockGoogleAPITestCase):

    if PY2:
        EXPECTED_BOOTSTRAP = [
            ['sudo apt-get install -y python-pip python-dev'],
        ]
    else:
        EXPECTED_BOOTSTRAP = [
            ['sudo apt-get install -y python3 python3-pip python3-dev'],
        ]

    def test_default(self):
        mr_job = MRTwoStepJob(['-r', 'dataproc'])
        with mr_job.make_runner() as runner:
            self.assertEqual(runner._opts['bootstrap_python'], True)
            self.assertEqual(runner._bootstrap_python(),
                             self.EXPECTED_BOOTSTRAP)
            self.assertEqual(runner._bootstrap,
                             self.EXPECTED_BOOTSTRAP)

    def test_bootstrap_python_switch(self):
        mr_job = MRTwoStepJob(['-r', 'dataproc', '--bootstrap-python'])

        with mr_job.make_runner() as runner:
            self.assertEqual(runner._opts['bootstrap_python'], True)
            self.assertEqual(runner._bootstrap_python(),
                             self.EXPECTED_BOOTSTRAP)
            self.assertEqual(runner._bootstrap,
                             self.EXPECTED_BOOTSTRAP)

    def test_no_bootstrap_python_switch(self):
        mr_job = MRTwoStepJob(['-r', 'dataproc', '--no-bootstrap-python'])
        with mr_job.make_runner() as runner:
            self.assertEqual(runner._opts['bootstrap_python'], False)
            self.assertEqual(runner._bootstrap_python(), [])
            self.assertEqual(runner._bootstrap, [])

    def test_bootstrap_python_comes_before_bootstrap(self):
        mr_job = MRTwoStepJob(['-r', 'dataproc', '--bootstrap', 'true'])

        with mr_job.make_runner() as runner:
            self.assertEqual(
                runner._bootstrap,
                self.EXPECTED_BOOTSTRAP + [['true']])
