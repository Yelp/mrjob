# -*- coding: utf-8 -*-
# Copyright 2009-2017 Yelp and Contributors
# Copyright 2018 Google Inc.
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
import getpass
import os
import os.path
from contextlib import contextmanager
from copy import deepcopy
from io import BytesIO

import mrjob
import mrjob.dataproc
from mrjob.dataproc import DataprocException
from mrjob.dataproc import DataprocJobRunner
from mrjob.dataproc import _DEFAULT_CLOUD_TMP_DIR_OBJECT_TTL_DAYS
from mrjob.dataproc import _DEFAULT_GCE_REGION
from mrjob.dataproc import _DEFAULT_IMAGE_VERSION
from mrjob.dataproc import _MAX_MINS_IDLE_BOOTSTRAP_ACTION_PATH
from mrjob.dataproc import _cluster_state_name
from mrjob.fs.gcs import GCSFilesystem
from mrjob.fs.gcs import parse_gcs_uri
from mrjob.py2 import PY2
from mrjob.py2 import StringIO
from mrjob.step import StepFailedException
from mrjob.tools.emr.audit_usage import _JOB_KEY_RE
from mrjob.util import log_to_stream
from mrjob.util import save_current_environment

from tests.mock_google import MockGoogleTestCase
from tests.mr_hadoop_format_job import MRHadoopFormatJob
from tests.mr_no_mapper import MRNoMapper
from tests.mr_two_step_job import MRTwoStepJob
from tests.mr_word_count import MRWordCount
from tests.py2 import call
from tests.py2 import mock
from tests.py2 import patch
from tests.quiet import logger_disabled
from tests.quiet import no_handlers_for_logger
from tests.sandbox import mrjob_conf_patcher

# used to match command lines
if PY2:
    PYTHON_BIN = 'python'
else:
    PYTHON_BIN = 'python3'

US_EAST_GCE_REGION = 'us-east1'
EU_WEST_GCE_REGION = 'europe-west1'

DEFAULT_GCE_INSTANCE = 'n1-standard-1'
HIGHMEM_GCE_INSTANCE = 'n1-highmem-2'
HIGHCPU_GCE_INSTANCE = 'n1-highcpu-2'
MICRO_GCE_INSTANCE = 'f1-micro'


class DataprocJobRunnerEndToEndTestCase(MockGoogleTestCase):

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

        mock_gcs_fs_snapshot = deepcopy(self.mock_gcs_fs)

        fake_gcs_output = [
            b'1\t"qux"\n2\t"bar"\n',
            b'2\t"foo"\n5\tnull\n'
        ]

        with mr_job.make_runner() as runner:
            self.assertIsInstance(runner, DataprocJobRunner)

            # make sure that initializing the runner doesn't affect GCS
            # (Issue #50)
            self.assertEqual(self.mock_gcs_fs, mock_gcs_fs_snapshot)

            runner.run()

            # setup fake output
            self.put_job_output_parts(runner, fake_gcs_output)

            results.extend(mr_job.parse_output(runner.cat_output()))

            local_tmp_dir = runner._get_local_tmp_dir()
            # make sure cleanup hasn't happened yet
            self.assertTrue(os.path.exists(local_tmp_dir))
            self.assertTrue(any(runner.fs.ls(runner.get_output_dir())))

            name_match = _JOB_KEY_RE.match(runner._job_key)
            self.assertEqual(name_match.group(1), 'mr_hadoop_format_job')
            self.assertEqual(name_match.group(2), getpass.getuser())

            # make sure our input and output formats are attached to
            # the correct steps
            jobs = list(runner._list_jobs())
            self.assertEqual(len(jobs), 2)

            # put earliest job first
            jobs.sort(key=lambda j: j.reference.job_id)

            step_0_args = jobs[0].hadoop_job.args
            step_1_args = jobs[1].hadoop_job.args

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
        cluster = runner._get_cluster(cluster_id)
        self.assertEqual(_cluster_state_name(cluster.status.state), 'DELETING')

    def test_failed_job(self):
        mr_job = MRTwoStepJob(['-r', 'dataproc', '-v'])
        mr_job.sandbox()

        with no_handlers_for_logger('mrjob.dataproc'):
            stderr = StringIO()
            log_to_stream('mrjob.dataproc', stderr)

            self.mock_jobs_succeed = False

            with mr_job.make_runner() as runner:
                self.assertIsInstance(runner, DataprocJobRunner)

                self.assertRaises(StepFailedException, runner.run)

                self.assertIn(' => ERROR\n', stderr.getvalue())

                cluster_id = runner.get_cluster_id()

        # job should get terminated
        cluster = runner._get_cluster(cluster_id)
        self.assertEqual(_cluster_state_name(cluster.status.state), 'DELETING')

    def _test_cloud_tmp_cleanup(self, mode, tmp_len):
        stdin = BytesIO(b'foo\nbar\n')

        mr_job = MRTwoStepJob(['-r', 'dataproc', '-v',
                               '-', '--cleanup', mode])
        mr_job.sandbox(stdin=stdin)

        with mr_job.make_runner() as runner:
            tmp_bucket, _ = parse_gcs_uri(runner._cloud_tmp_dir)

            runner.run()

            # this is set and unset before we can get at it unless we do this
            list(runner.cat_output())

            fs = runner.fs

        # with statement finishes, cleanup runs

        self.assertEqual(
            len(list(fs.client.bucket(tmp_bucket).list_blobs())),
            tmp_len)

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


class ExistingClusterTestCase(MockGoogleTestCase):

    def test_attach_to_existing_cluster(self):
        runner1 = DataprocJobRunner(conf_paths=[])

        runner1._launch_cluster()
        cluster_id = runner1._cluster_id

        stdin = BytesIO(b'foo\nbar\n')

        mr_job = MRTwoStepJob(['-r', 'dataproc', '-v',
                               '--cluster-id', cluster_id])
        mr_job.sandbox(stdin=stdin)

        results = []

        with mr_job.make_runner() as runner2:
            runner2.run()

            # Generate fake output
            self.put_job_output_parts(runner2, [
                b'1\t"bar"\n1\t"foo"\n2\tnull\n'
            ])

            # Issue 182: don't create the bootstrap script when
            # attaching to another cluster
            self.assertIsNone(runner2._master_bootstrap_script_path)

            results.extend(mr_job.parse_output(runner2.cat_output()))

        self.assertEqual(sorted(results),
                         [(1, 'bar'), (1, 'foo'), (2, None)])

    def test_dont_take_down_cluster_on_failure(self):
        runner1 = DataprocJobRunner(conf_paths=[])

        runner1._launch_cluster()
        cluster_id = runner1._cluster_id

        mr_job = MRTwoStepJob(['-r', 'dataproc', '-v',
                               '--cluster-id', cluster_id])
        mr_job.sandbox()

        self.mock_jobs_succeed = False

        with mr_job.make_runner() as runner2:
            self.assertIsInstance(runner2, DataprocJobRunner)

            with logger_disabled('mrjob.dataproc'):
                self.assertRaises(StepFailedException, runner2.run)

            cluster2 = runner2._get_cluster(runner2._cluster_id)
            self.assertEqual(_cluster_state_name(cluster2.status.state),
                             'RUNNING')

        # job shouldn't get terminated by cleanup
        cluster1 = runner1._get_cluster(runner1._cluster_id)
        self.assertEqual(_cluster_state_name(cluster1.status.state),
                         'RUNNING')


class CloudAndHadoopVersionTestCase(MockGoogleTestCase):

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


class AvailabilityZoneConfigTestCase(MockGoogleTestCase):

    ZONE = 'puppy-land-1a'

    MRJOB_CONF_CONTENTS = {'runners': {'dataproc': {
        'check_cluster_every': 0.00,
        'cloud_fs_sync_secs': 0.00,
        'zone': ZONE,
    }}}

    def test_availability_zone_config(self):
        with self.make_runner('--zone', self.ZONE) as runner:
            runner.run()

            cluster = runner._get_cluster(runner._cluster_id)
            self.assertIn(self.ZONE,
                          cluster.config.gce_cluster_config.zone_uri)
            self.assertIn(self.ZONE,
                          cluster.config.master_config.machine_type_uri)
            self.assertIn(self.ZONE,
                          cluster.config.worker_config.machine_type_uri)


class ProjectIDTestCase(MockGoogleTestCase):

    def test_default(self):
        with self.make_runner() as runner:
            self.assertEqual(runner._project_id, self.mock_project_id)

    def test_project_id(self):
        with self.make_runner('--project-id', 'alan-parsons') as runner:
            self.assertEqual(runner._project_id, 'alan-parsons')


class CredentialsTestCase(MockGoogleTestCase):

    def test_credentials_are_scoped(self):
        # if we don't set scope, we'll get an error unless we're reading
        # credentials from gcloud (see #1742)
        with self.make_runner() as runner:
            self.assertTrue(runner._credentials.scopes)


class ExtraClusterParamsTestCase(MockGoogleTestCase):

    # just a basic test to make extra_cluster_params is respected.
    # more extensive tests are found in tests.test_emr

    def test_set_labels(self):
        args = ['--extra-cluster-param', 'labels={"name": "wrench"}']

        with self.make_runner(*args) as runner:
            runner.run()

            cluster = runner._get_cluster(runner._cluster_id)
            self.assertEqual(cluster.labels['name'], 'wrench')


class RegionAndZoneOptsTestCase(MockGoogleTestCase):

    def setUp(self):
        super(RegionAndZoneOptsTestCase, self).setUp()
        self.log = self.start(patch('mrjob.dataproc.log'))

    def test_default(self):
        runner = DataprocJobRunner()
        self.assertEqual(runner._opts['region'], 'us-west1')
        self.assertEqual(runner._opts['zone'], None)
        self.assertFalse(self.log.warning.called)

    def test_explicit_zone(self):
        runner = DataprocJobRunner(zone='europe-west1-a')
        self.assertEqual(runner._opts['zone'], 'europe-west1-a')

    def test_region_from_environment(self):
        with save_current_environment():
            os.environ['CLOUDSDK_COMPUTE_REGION'] = 'us-east1'
            runner = DataprocJobRunner()

        self.assertEqual(runner._opts['region'], 'us-east1')

    def test_explicit_region_beats_environment(self):
        with save_current_environment():
            os.environ['CLOUDSDK_COMPUTE_REGION'] = 'us-east1'
            runner = DataprocJobRunner(region='europe-west1-a')

        self.assertEqual(runner._opts['region'], 'europe-west1-a')

    def test_zone_from_environment(self):
        with save_current_environment():
            os.environ['CLOUDSDK_COMPUTE_ZONE'] = 'us-west1-b'
            runner = DataprocJobRunner()

        self.assertEqual(runner._opts['zone'], 'us-west1-b')

    def test_explicit_zone_beats_environment(self):
        with save_current_environment():
            os.environ['CLOUDSDK_COMPUTE_ZONE'] = 'us-west1-b'
            runner = DataprocJobRunner(zone='europe-west1-a')

        self.assertEqual(runner._opts['zone'], 'europe-west1-a')

    def test_zone_beats_region(self):
        runner = DataprocJobRunner(region='europe-west1',
                                   zone='europe-west1-a')

        self.assertTrue(self.log.warning.called)
        self.assertEqual(runner._opts['region'], None)
        self.assertEqual(runner._opts['zone'], 'europe-west1-a')

    def test_command_line_beats_config(self):
        ZONE_CONF = dict(runners=dict(dataproc=dict(zone='us-west1-a')))

        with mrjob_conf_patcher(ZONE_CONF):
            runner = DataprocJobRunner(region='europe-west1')

            # region takes precedence because it was set on the command line
            self.assertEqual(runner._opts['region'], 'europe-west1')
            self.assertEqual(runner._opts['zone'], None)
            # only a problem if you set region and zone
            # in the same config
            self.assertFalse(self.log.warning.called)


class TmpBucketTestCase(MockGoogleTestCase):
    def assert_new_tmp_bucket(self, location, **runner_kwargs):
        """Assert that if we create an DataprocJobRunner with the given keyword
        args, it'll create a new tmp bucket with the given location
        constraint.
        """
        existing_buckets = set(self.mock_gcs_fs)

        runner = DataprocJobRunner(conf_paths=[], **runner_kwargs)

        bucket_name, path = parse_gcs_uri(runner._cloud_tmp_dir)
        runner._create_fs_tmp_bucket(bucket_name, location=location)

        self.assertTrue(bucket_name.startswith('mrjob-'))
        self.assertNotIn(bucket_name, existing_buckets)
        self.assertEqual(path, 'tmp/')

        current_bucket = runner.fs.get_bucket(bucket_name)

        self.assertEqual(current_bucket.location, location.upper())

        # Verify that we setup bucket lifecycle rules of 28-day retention
        first_lifecycle_rule = current_bucket.lifecycle_rules[0]
        self.assertEqual(first_lifecycle_rule['action'], dict(type='Delete'))
        self.assertEqual(first_lifecycle_rule['condition'],
                         dict(age=_DEFAULT_CLOUD_TMP_DIR_OBJECT_TTL_DAYS))

    def _make_bucket(self, name, location=None):
        fs = GCSFilesystem()
        fs.create_bucket(name, location=location)

    def test_default(self):
        self.assert_new_tmp_bucket(_DEFAULT_GCE_REGION)

    def test_us_east_1(self):
        self.assert_new_tmp_bucket(US_EAST_GCE_REGION,
                                   region=US_EAST_GCE_REGION)

    def test_europe_west_1(self):
        # location should be blank
        self.assert_new_tmp_bucket(EU_WEST_GCE_REGION,
                                   region=EU_WEST_GCE_REGION)

    def test_reuse_mrjob_bucket_in_same_region(self):
        self._make_bucket('mrjob-1', _DEFAULT_GCE_REGION)

        runner = DataprocJobRunner()
        self.assertEqual(runner._cloud_tmp_dir, 'gs://mrjob-1/tmp/')

    def test_ignore_mrjob_bucket_in_different_region(self):
        # this tests 687
        self._make_bucket('mrjob-1', US_EAST_GCE_REGION)

        self.assert_new_tmp_bucket(_DEFAULT_GCE_REGION)

    def test_ignore_non_mrjob_bucket_in_different_region(self):
        self._make_bucket('walrus', US_EAST_GCE_REGION)

        self.assert_new_tmp_bucket(_DEFAULT_GCE_REGION)

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
        self.assertEqual(runner._region(), US_EAST_GCE_REGION)


class GCEInstanceGroupTestCase(MockGoogleTestCase):

    maxDiff = None

    def _gce_instance_group_summary(self, instance_group):
        if not instance_group:
            return (0, '')

        num_instances = instance_group.num_instances
        instance_type = instance_group.machine_type_uri.split('/')[-1]
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
        runner._upload_mgr.add(_MAX_MINS_IDLE_BOOTSTRAP_ACTION_PATH)

        cluster_id = runner._launch_cluster()

        cluster = runner._get_cluster(cluster_id)

        conf = cluster.config

        role_to_actual = dict(
            master=self._gce_instance_group_summary(conf.master_config),
            core=self._gce_instance_group_summary(conf.worker_config),
            task=self._gce_instance_group_summary(conf.secondary_worker_config)
        )

        role_to_expected = kwargs.copy()
        role_to_expected.setdefault('master', (1, DEFAULT_GCE_INSTANCE))
        role_to_expected.setdefault('core', (2, DEFAULT_GCE_INSTANCE))
        role_to_expected.setdefault(
            'task', self._gce_instance_group_summary(dict()))
        self.assertEqual(role_to_actual, role_to_expected)

    def set_in_mrjob_conf(self, **kwargs):
        dataproc_opts = deepcopy(self.MRJOB_CONF_CONTENTS)
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

    def test_explicit_master_and_worker_instance_types(self):
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

        # set master and worker in mrjob.conf, 2 instances
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


class MasterBootstrapScriptTestCase(MockGoogleTestCase):

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

        runner = DataprocJobRunner(
            conf_paths=[],
            bootstrap=[
                PYTHON_BIN + ' ' +
                foo_py_path + '#bar.py',
                'gs://walrus/scripts/ohnoes.sh#',
                'echo "Hi!"',
                'true',
                'ls',
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
                '  hadoop fs -copyToLocal %s $__mrjob_PWD/%s' % (uri, name),
                lines)
            self.assertIn(
                '  chmod u+rx $__mrjob_PWD/%s' % (name,),
                lines)

        # check files get downloaded
        assertScriptDownloads(foo_py_path, 'bar.py')
        assertScriptDownloads('gs://walrus/scripts/ohnoes.sh')
        assertScriptDownloads(runner._mrjob_zip_path)

        # check scripts get run

        # bootstrap
        self.assertIn('  ' + PYTHON_BIN + ' $__mrjob_PWD/bar.py', lines)
        self.assertIn('  $__mrjob_PWD/ohnoes.sh', lines)

        self.assertIn('  echo "Hi!"', lines)
        self.assertIn('  true', lines)
        self.assertIn('  ls', lines)

        self.assertIn('  speedups.sh', lines)
        self.assertIn('  /tmp/s.sh', lines)

        # bootstrap_mrjob
        mrjob_zip_name = runner._bootstrap_dir_mgr.name(
            'file', runner._mrjob_zip_path)
        self.assertIn("  __mrjob_PYTHON_LIB=$(" + PYTHON_BIN + " -c 'from"
                      " distutils.sysconfig import get_python_lib;"
                      " print(get_python_lib())')", lines)
        self.assertIn('  sudo unzip $__mrjob_PWD/' + mrjob_zip_name +
                      ' -d $__mrjob_PYTHON_LIB', lines)
        self.assertIn('  sudo ' + PYTHON_BIN + ' -m compileall -q -f'
                      ' $__mrjob_PYTHON_LIB/mrjob && true', lines)
        # bootstrap_python
        if PY2:
            self.assertIn(
                '  sudo apt-get install -y python-pip python-dev',
                lines)
        else:
            self.assertIn(
                '  sudo apt-get install -y python3 python3-pip python3-dev',
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


class DataprocNoMapperTestCase(MockGoogleTestCase):

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

            results.extend(mr_job.parse_output(runner.cat_output()))

        self.assertEqual(sorted(results),
                         [(1, ['blue', 'one', 'red', 'two']),
                          (4, ['fish'])])


class MaxMinsIdleTestCase(MockGoogleTestCase):

    def assertRanIdleTimeoutScriptWith(self, runner, expected_metadata):
        cluster_metadata, last_init_exec = (
            self._cluster_metadata_and_last_init_exec(runner))

        # Verify args
        for key in expected_metadata.keys():
            self.assertEqual(cluster_metadata[key], expected_metadata[key])

        expected_uri = runner._upload_mgr.uri(
            _MAX_MINS_IDLE_BOOTSTRAP_ACTION_PATH)
        self.assertEqual(last_init_exec, expected_uri)

    def _cluster_metadata_and_last_init_exec(self, runner):
        cluster = runner._get_cluster(runner.get_cluster_id())

        # Verify last arg
        last_init_action = cluster.config.initialization_actions[-1]
        last_init_exec = last_init_action.executable_file

        cluster_metadata = cluster.config.gce_cluster_config.metadata
        return cluster_metadata, last_init_exec

    def test_default(self):
        mr_job = MRWordCount(['-r', 'dataproc'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.run()
            self.assertRanIdleTimeoutScriptWith(runner, {
                'mrjob-max-secs-idle': '600',
            })

    def test_persistent_cluster(self):
        mr_job = MRWordCount(['-r', 'dataproc', '--max-mins-idle', '0.6'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.run()
            self.assertRanIdleTimeoutScriptWith(runner, {
                'mrjob-max-secs-idle': '36',
            })

    def test_bootstrap_script_is_actually_installed(self):
        self.assertTrue(os.path.exists(_MAX_MINS_IDLE_BOOTSTRAP_ACTION_PATH))


class TestCatFallback(MockGoogleTestCase):

    def test_gcs_cat(self):

        self.put_gcs_multi({
            'gs://walrus/one': b'one_text',
            'gs://walrus/two': b'two_text',
            'gs://walrus/three': b'three_text',
        })

        runner = DataprocJobRunner(cloud_tmp_dir='gs://walrus/tmp',
                                   conf_paths=[])

        self.assertEqual(list(runner.fs.cat('gs://walrus/one')), [b'one_text'])


class CleanUpJobTestCase(MockGoogleTestCase):

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
                              '_delete_cluster') as m:
                r._cleanup_cluster()
                self.assertTrue(m.called)

    def test_kill_cluster_if_successful(self):
        # If they are setting up the cleanup to kill the cluster, mrjob should
        # kill the cluster independent of job success.
        with no_handlers_for_logger('mrjob.dataproc'):
            r = self._quick_runner()
            with patch.object(mrjob.dataproc.DataprocJobRunner,
                              '_delete_cluster') as m:
                r._ran_job = True
                r._cleanup_cluster()
                self.assertTrue(m.called)

    def test_kill_persistent_cluster(self):
        with no_handlers_for_logger('mrjob.dataproc'):
            r = self._quick_runner()
            with patch.object(mrjob.dataproc.DataprocJobRunner,
                              '_delete_cluster') as m:
                r._opts['cluster_id'] = 'j-MOCKCLUSTER0'
                r._cleanup_cluster()
                self.assertTrue(m.called)


class BootstrapPythonTestCase(MockGoogleTestCase):

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


class GetNewDriverOutputLinesTestCase(MockGoogleTestCase):
    # test line parsing logic

    URI = ('gs://mock-bucket/google-cloud-dataproc-metainfo/mock-cluster-id'
           '/jobs/mock-job-name/driveroutput')

    def setUp(self):
        super(GetNewDriverOutputLinesTestCase, self).setUp()
        self.runner = DataprocJobRunner()
        self.fs = self.runner.fs

    def append_data(self, uri, new_data):
        old_data = b''.join(self.fs.cat(uri))
        self.put_gcs_multi({uri: old_data + new_data})

    def get_new_lines(self):
        return self.runner._get_new_driver_output_lines(self.URI)

    def test_no_log_files(self):
        self.assertEqual(self.get_new_lines(), [])

    def test_return_new_lines_as_available(self):
        log_uri = self.URI + '.000000000'

        self.assertEqual(self.get_new_lines(), [])

        self.append_data(log_uri, b'log line\nanother log line\n')

        self.assertEqual(self.get_new_lines(),
                         ['log line\n', 'another log line\n'])
        self.assertEqual(self.get_new_lines(), [])

        self.append_data(log_uri, b'third log line\n')

        self.assertEqual(self.get_new_lines(), ['third log line\n'])
        self.assertEqual(self.get_new_lines(), [])

    def test_partial_lines(self):
        # probably lines are going to be added atomically, but just in case

        log_uri = self.URI + '.000000000'

        self.assertEqual(self.get_new_lines(), [])

        self.append_data(log_uri, b'log line\nanother lo')

        self.assertEqual(self.get_new_lines(), ['log line\n'])
        self.assertEqual(self.get_new_lines(), [])

        self.append_data(log_uri, b'g line\na thir')

        self.assertEqual(self.get_new_lines(), ['another log line\n'])
        self.assertEqual(self.get_new_lines(), [])

        self.append_data(log_uri, b'd log line\n')

    def test_iterating_through_log_files(self):
        log0_uri = self.URI + '.000000000'
        log1_uri = self.URI + '.000000001'

        self.assertEqual(self.get_new_lines(), [])

        self.append_data(log0_uri, b'log line\n')

        self.assertEqual(self.get_new_lines(), ['log line\n'])

        self.append_data(log0_uri, b'another log line\n')
        self.append_data(log1_uri, b'a third log line\n')

        self.assertEqual(self.get_new_lines(),
                         ['another log line\n', 'a third log line\n'])

        # this shouldn't actually happen
        self.append_data(log0_uri, b'Hey where did THIS come from???\n')

        # because we've moved beyond log0
        self.assertEqual(self.get_new_lines(), [])


class UpdateStepInterpretationTestCase(MockGoogleTestCase):
    # make sure we parse status messages, counters, etc. properly

    URI = ('gs://mock-bucket/google-cloud-dataproc-metainfo/mock-cluster-id'
           '/jobs/mock-job-name/driveroutput')

    def setUp(self):
        super(UpdateStepInterpretationTestCase, self).setUp()
        self.runner = DataprocJobRunner()
        self.get_lines = self.start(patch(
            'mrjob.dataproc.DataprocJobRunner._get_new_driver_output_lines',
            return_value=[]))

        self.step_interpretation = {}

    def update_step_interpretation(self):
        self.runner._update_step_interpretation(
            self.step_interpretation, self.URI)

    def test_empty(self):
        self.update_step_interpretation()

        self.assertEqual(self.step_interpretation, {})

    def test_progress(self):
        self.get_lines.side_effect = [
            ['18/04/17 22:03:40 INFO impl.YarnClientImpl: Submitted'
             ' application application_1524002511355_0001\n'],
            ['18/04/17 22:03:54 INFO mapreduce.Job:  map 0% reduce 0%\n'],
            ['18/04/17 22:05:10 INFO mapreduce.Job:  map 52% reduce 0%\n',
             '18/04/17 22:06:15 INFO mapreduce.Job:  map 100% reduce 0%\n'],
            ['18/04/17 22:07:32 INFO mapreduce.Job:  map 100% reduce 100%\n'],
        ]


        self.update_step_interpretation()
        self.assertEqual(self.step_interpretation['application_id'],
                         'application_1524002511355_0001')

        self.update_step_interpretation()
        self.assertEqual(self.step_interpretation['progress'],
                         dict(map=0, reduce=0,
                              message=' map 0% reduce 0%'))

        # make sure we didn't overwrite application_id
        self.assertEqual(self.step_interpretation['application_id'],
                         'application_1524002511355_0001')

        self.update_step_interpretation()
        self.assertEqual(self.step_interpretation['progress'],
                         dict(map=100, reduce=0,
                              message=' map 100% reduce 0%'))

        self.update_step_interpretation()
        self.assertEqual(self.step_interpretation['progress'],
                         dict(map=100, reduce=100,
                              message=' map 100% reduce 100%'))

    def test_counters(self):
        self.get_lines.return_value = [
            '18/04/17 22:07:34 INFO mapreduce.Job: Counters: 3\n',
            '\tFile System Counters\n',
            '\t\tFILE: Number of bytes read=819\n',
            '\t\tFILE: Number of bytes written=3698122\n',
            '\tMap-Reduce Framework\n',
            '\t\tMap input records=13\n',
        ]

        self.update_step_interpretation()
        self.assertEqual(
            self.step_interpretation['counters'],
            {
                'File System Counters': {
                    'FILE: Number of bytes read': 819,
                    'FILE: Number of bytes written': 3698122,
                },
                'Map-Reduce Framework': {
                    'Map input records': 13,
                },
            },
        )


class ProgressAndCounterLoggingTestCase(MockGoogleTestCase):

    def setUp(self):
        super(ProgressAndCounterLoggingTestCase, self).setUp()

        self.get_lines = self.start(patch(
            'mrjob.dataproc.DataprocJobRunner._get_new_driver_output_lines',
            return_value=[]))

        self.log = self.start(patch('mrjob.dataproc.log'))
        self.start(patch('mrjob.logs.mixin.log', self.log))

    def test_log_messages(self):
        self.get_lines.return_value = [
            '18/04/17 22:06:15 INFO mapreduce.Job:  map 100% reduce 0%\n',
            '18/04/17 22:07:34 INFO mapreduce.Job: Counters: 1\n',
            '\tFile System Counters\n',
            '\t\tFILE: Number of bytes read=819\n',
        ]

        mr_job = MRWordCount(['-r', 'dataproc'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.run()


        self.assertIn(call('  map 100% reduce 0%'),
                      self.log.info.call_args_list)

        self.assertIn(
            call(
                'Counters: 1\n\tFile System Counters\n\t\tFILE:'
                ' Number of bytes read=819'),
            self.log.info.call_args_list)
