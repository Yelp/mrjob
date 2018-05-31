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
from subprocess import PIPE
from unittest import TestCase

from google.api_core.exceptions import InvalidArgument
from google.api_core.exceptions import NotFound
from google.api_core.exceptions import RequestRangeNotSatisfiable

import mrjob
import mrjob.dataproc
import mrjob.fs.gcs
from mrjob.dataproc import DataprocException
from mrjob.dataproc import DataprocJobRunner
from mrjob.dataproc import _CONTAINER_EXECUTOR_CLASS_NAME
from mrjob.dataproc import _DEFAULT_CLOUD_TMP_DIR_OBJECT_TTL_DAYS
from mrjob.dataproc import _DEFAULT_GCE_REGION
from mrjob.dataproc import _DEFAULT_IMAGE_VERSION
from mrjob.dataproc import _HADOOP_STREAMING_JAR_URI
from mrjob.dataproc import _cluster_state_name
from mrjob.dataproc import _fix_java_stack_trace
from mrjob.dataproc import _fix_traceback
from mrjob.examples.mr_boom import MRBoom
from mrjob.fs.gcs import GCSFilesystem
from mrjob.fs.gcs import parse_gcs_uri
from mrjob.logs.errors import _pick_error
from mrjob.parse import is_uri
from mrjob.py2 import PY2
from mrjob.py2 import StringIO
from mrjob.step import StepFailedException
from mrjob.tools.emr.audit_usage import _JOB_KEY_RE
from mrjob.util import log_to_stream
from mrjob.util import save_current_environment

from tests.mock_google import MockGoogleTestCase
from tests.mock_google.dataproc import _DEFAULT_SCOPES
from tests.mock_google.dataproc import _MANDATORY_SCOPES
from tests.mock_google.storage import MockGoogleStorageBlob
from tests.mr_hadoop_format_job import MRHadoopFormatJob
from tests.mr_jar_and_streaming import MRJarAndStreaming
from tests.mr_just_a_jar import MRJustAJar
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

DRIVER_OUTPUT_URI = (
    'gs://mock-bucket/google-cloud-dataproc-metainfo/mock-cluster-id'
    '/jobs/mock-job-name/driveroutput')

APPLICATION_ID = 'application_1525195653111_0001'
CONTAINER_ID_1 = 'container_1525195653111_0001_0001_01_000001'
CONTAINER_ID_2 = 'container_1525195653111_0001_0002_02_000002'

STACK_TRACE = (
    'Diagnostics report from attempt_1525195653111_0001_m_000000_3:'
    ' Error: java.lang.RuntimeException: PipeMapRed.waitOutputThreads():'
    ' subprocess failed with code 1\n'
    '\tat org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads'
    '(PipeMapRed.java:322)\n'
    '\tat org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:158)'
)

# newlines don't get logged for some reason
LOGGING_STACK_TRACE = STACK_TRACE.replace('\n', '')

# sample traceback from MRBoom
TRACEBACK = (
   'Traceback (most recent call last):\n'
    '  File "mr_boom.py", line 23, in <module>\n'
    '    MRBoom.run()\n'
    '  File "/usr/lib/python2.7/dist-packages/mrjob/job.py"'
    ', line 433, in run\n'
    '    mr_job.execute()\n'
    '  File "/usr/lib/python2.7/dist-packages/mrjob/job.py"'
    ', line 442, in execute\n'
    '    self.run_mapper(self.options.step_num)\n'
    '  File "/usr/lib/python2.7/dist-packages/mrjob/job.py"'
    ', line 522, in run_mapper\n'
    '    for out_key, out_value in mapper_init() or ():\n'
    '  File "mr_boom.py", line 20, in mapper_init\n'
    '    raise Exception(\'BOOM\')\n'
    'Exception: BOOM')

LOGGING_TRACEBACK = TRACEBACK.replace('\n', '')

# these very occasionally appear in the stderr log
LOG4J_WARNINGS = (
    '\n'
    'No appenders could be found for logger'
    ' (org.apache.hadoop.metrics2.impl.MetricsSystemImpl).'
    'Please initialize the log4j system properly.\n'
    'See http://logging.apache.org/log4j/1.2/faq.html#noconfig'
    ' for more info.'
)

SPLIT_URI = ('gs://mrjob-us-west1-aaaaaaaaaaaaaaaa/tmp/mr_boom'
             '.davidmarin.20180503.232439.647629/files/LICENSE.txt')
SPLIT_MESSAGE = (
    'Processing split: %s:0+28' % SPLIT_URI)
SPLIT = dict(path=SPLIT_URI, start_line=0, num_lines=28)

LOGGING_CLUSTER_NAME = 'mock-cluster-with-logging'


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
        self._test_cloud_tmp_cleanup('LOCAL_TMP', 4)

    def test_cleanup_logs(self):
        self._test_cloud_tmp_cleanup('LOGS', 4)

    def test_cleanup_none(self):
        self._test_cloud_tmp_cleanup('NONE', 4)

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


class GCEClusterConfigTestCase(MockGoogleTestCase):
    # test service_account, service_account_scopes

    def _get_gce_cluster_config(self, *args):
        job = MRWordCount(['-r', 'dataproc'] + list(args))
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()
            return runner._get_cluster(
                runner._cluster_id).config.gce_cluster_config

    def test_default(self):
        gcc = self._get_gce_cluster_config()

        self.assertFalse(gcc.service_account)
        self.assertEqual(
            set(gcc.service_account_scopes),
            _MANDATORY_SCOPES | _DEFAULT_SCOPES
        )

    def test_blank_means_default(self):
        gcc = self._get_gce_cluster_config('--service-account-scopes', '')

        self.assertEqual(
            set(gcc.service_account_scopes),
            _MANDATORY_SCOPES | _DEFAULT_SCOPES
        )

    def test_service_account(self):
        account = '12345678901-compute@developer.gserviceaccount.com'

        gcc = self._get_gce_cluster_config(
            '--service-account', account)

        self.assertEqual(gcc.service_account, account)

    def test_service_account_scopes(self):
        scope1 = 'https://www.googleapis.com/auth/scope1'
        scope2 = 'https://www.googleapis.com/auth/scope2'

        gcc = self._get_gce_cluster_config(
            '--service-account-scopes', '%s,%s' % (scope1, scope2))

        self.assertEqual(
            set(gcc.service_account_scopes),
            _MANDATORY_SCOPES | {scope1, scope2})

    def test_set_scope_by_name(self):
        scope_name = 'test.name'
        scope_uri = 'https://www.googleapis.com/auth/test.name'

        gcc = self._get_gce_cluster_config(
            '--service-account-scopes', scope_name)

        self.assertEqual(
            set(gcc.service_account_scopes),
            _MANDATORY_SCOPES | {scope_uri})





class ClusterPropertiesTestCase(MockGoogleTestCase):

    def _get_cluster_properties(self, *args):
        job = MRWordCount(['-r', 'dataproc'] + list(args))
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()
            return runner._get_cluster(
                runner._cluster_id).config.software_config.properties

    def test_default(self):
        props = self._get_cluster_properties()

        self.assertNotIn('foo:bar', props)

    def test_command_line(self):
        props = self._get_cluster_properties(
            '--cluster-property',
            'dataproc:dataproc.allow.zero.workers=true',
            '--cluster-property',
            'mapred:mapreduce.map.memory.mb=1024',
        )

        self.assertEqual(props['dataproc:dataproc.allow.zero.workers'], 'true')
        self.assertEqual(props['mapred:mapreduce.map.memory.mb'], '1024')

    def test_convert_conf_values_to_strings(self):
        conf_path = self.makefile(
            'mrjob.conf',
            b'runners:\n  dataproc:\n    cluster_properties:\n'
            b"      'dataproc:dataproc.allow.zero.workers': true\n"
            b"      'hdfs:dfs.namenode.handler.count': 40\n")

        self.mrjob_conf_patcher.stop()
        props = self._get_cluster_properties('-c', conf_path)
        self.mrjob_conf_patcher.start()

        self.assertEqual(props['dataproc:dataproc.allow.zero.workers'], 'true')
        self.assertEqual(props['hdfs:dfs.namenode.handler.count'], '40')


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


class InstanceTypeAndNumberTestCase(MockGoogleTestCase):

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


class InstanceConfigTestCase(MockGoogleTestCase):
    # test the *_instance_config options

    def _get_cluster_config(self, *args):
        job = MRWordCount(['-r', 'dataproc'] + list(args))
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()
            return runner._get_cluster(runner._cluster_id).config

    def test_default(self):
        conf = self._get_cluster_config()

        self.assertEqual(
            conf.master_config.disk_config.boot_disk_size_gb, 500)
        self.assertEqual(
            conf.worker_config.disk_config.boot_disk_size_gb, 500)

    def test_set_disk_config(self):
        conf = self._get_cluster_config(
            '--master-instance-config',
            '{"disk_config": {"boot_disk_size_gb": 100}}',
            '--core-instance-config',
            '{"disk_config": {"boot_disk_size_gb": 200, "num_local_ssds": 2}}')

        self.assertEqual(
            conf.master_config.disk_config.boot_disk_size_gb, 100)
        self.assertFalse(
            conf.master_config.disk_config.num_local_ssds)
        self.assertEqual(
            conf.worker_config.disk_config.boot_disk_size_gb, 200)
        self.assertEqual(
            conf.worker_config.disk_config.num_local_ssds, 2)

    def test_can_override_num_instances(self):
        conf = self._get_cluster_config(
            '--core-instance-config', '{"num_instances": 10}')

        self.assertEqual(
            conf.worker_config.num_instances, 10)

    def test_set_task_config(self):
        conf = self._get_cluster_config(
            '--num-task-instances', '3',
            '--task-instance-config',
            '{"disk_config": {"boot_disk_size_gb": 300}}')

        self.assertEqual(
            conf.secondary_worker_config.disk_config.boot_disk_size_gb, 300)

    def test_dont_set_task_config_if_no_task_instances(self):
        conf = self._get_cluster_config(
            '--task-instance-config',
            '{"disk_config": {"boot_disk_size_gb": 300}}')

        self.assertFalse(
            conf.secondary_worker_config.disk_config.boot_disk_size_gb)

    def test_can_set_num_instances_through_task_config(self):
        conf = self._get_cluster_config(
            '--task-instance-config',
            '{"disk_config": {"boot_disk_size_gb": 300}, "num_instances": 3}')

        self.assertEqual(
            conf.secondary_worker_config.num_instances, 3)
        self.assertEqual(
            conf.secondary_worker_config.disk_config.boot_disk_size_gb, 300)

    def test_preemtible_task_instances(self):
        conf = self._get_cluster_config(
            '--num-task-instances', '3',
            '--task-instance-config',
            '{"is_preemptible": true}')

        self.assertTrue(
            conf.secondary_worker_config.is_preemptible)





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

        # see #1601
        self.assertIn('mkdir /tmp/mrjob', lines)
        self.assertIn('cd /tmp/mrjob', lines)

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

    def test_default(self):
        mr_job = MRWordCount(['-r', 'dataproc'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.run()

            cluster = runner._get_cluster(runner._cluster_id)

            self.assertEqual(
                cluster.config.lifecycle_config.idle_delete_ttl.seconds,
                600)

    def test_persistent_cluster(self):
        mr_job = MRWordCount(['-r', 'dataproc', '--max-mins-idle', '30'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.run()

            cluster = runner._get_cluster(runner._cluster_id)

            self.assertEqual(
                cluster.config.lifecycle_config.idle_delete_ttl.seconds,
                1800)


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
        return self.runner._get_new_driver_output_lines(DRIVER_OUTPUT_URI)

    def test_no_log_files(self):
        self.assertEqual(self.get_new_lines(), [])

    def test_empty_log_file(self):
        log_uri = DRIVER_OUTPUT_URI + '.000000000'
        self.append_data(log_uri, b'')

        self.assertEqual(self.get_new_lines(), [])

    def test_return_new_lines_as_available(self):
        log_uri = DRIVER_OUTPUT_URI + '.000000000'

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

        log_uri = DRIVER_OUTPUT_URI + '.000000000'

        self.assertEqual(self.get_new_lines(), [])

        self.append_data(log_uri, b'log line\nanother lo')

        self.assertEqual(self.get_new_lines(), ['log line\n'])
        self.assertEqual(self.get_new_lines(), [])

        self.append_data(log_uri, b'g line\na thir')

        self.assertEqual(self.get_new_lines(), ['another log line\n'])
        self.assertEqual(self.get_new_lines(), [])

        self.append_data(log_uri, b'd log line\n')

    def test_iterating_through_log_files(self):
        log0_uri = DRIVER_OUTPUT_URI + '.000000000'
        log1_uri = DRIVER_OUTPUT_URI + '.000000001'

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

    def test_not_found(self):
        log_uri = DRIVER_OUTPUT_URI + '.000000000'
        self.append_data(log_uri, b'log line\nanother log line\n')

        # in some cases the blob for the log file appears but
        # raises NotFound when read from, maybe an eventual consistency
        # race condition?
        self.start(patch('tests.mock_google.storage.MockGoogleStorageBlob'
                         '.download_as_string',
                         side_effect=NotFound('race condition')))

        self.assertEqual(self.get_new_lines(), [])

    def test_request_range_not_satisfiable(self):
        log_uri = DRIVER_OUTPUT_URI + '.000000000'
        self.append_data(log_uri, b'')

        # this is normal and happens when we request a range starting
        # at or after the end of the file. Our mock Blob object imitates
        # this behavior, so this test should be redundant with
        # test_empty_log_file(), above.
        self.start(patch('tests.mock_google.storage.MockGoogleStorageBlob'
                         '.download_as_string',
                         side_effect=RequestRangeNotSatisfiable('too far')))

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
            self.step_interpretation, DRIVER_OUTPUT_URI)

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


class SetUpSSHTunnelTestCase(MockGoogleTestCase):

    def setUp(self, *args):
        super(SetUpSSHTunnelTestCase, self).setUp()

        self.mock_Popen = self.start(patch('mrjob.cloud.Popen'))
        # simulate successfully binding port
        self.mock_Popen.return_value.returncode = None
        self.mock_Popen.return_value.pid = 99999

        self.start(patch('os.kill'))  # don't clean up fake SSH proc

        self.start(patch('time.sleep'))

    def test_default(self):
        job = MRWordCount(['-r', 'dataproc'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

        self.assertFalse(self.mock_Popen.called)

    def test_default_ssh_tunnel(self):
        job = MRWordCount(['-r', 'dataproc', '--ssh-tunnel'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

        self.assertEqual(self.mock_Popen.call_count, 1)
        args_tuple, kwargs = self.mock_Popen.call_args
        args = args_tuple[0]

        self.assertEqual(kwargs, dict(stdin=PIPE, stdout=PIPE, stderr=PIPE))

        self.assertEqual(args[:3], ['gcloud', 'compute', 'ssh'])

        self.assertIn('-L', args)
        self.assertIn('-N', args)
        self.assertIn('-n', args)
        self.assertIn('-q', args)

        self.assertNotIn('-g', args)
        self.assertNotIn('-4', args)

        self.mock_Popen.stdin.called_once_with(b'\n\n')

    def test_open_ssh_tunnel(self):
        job = MRWordCount(
            ['-r', 'dataproc', '--ssh-tunnel', '--ssh-tunnel-is-open'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

        self.assertEqual(self.mock_Popen.call_count, 1)
        args = self.mock_Popen.call_args[0][0]

        self.assertIn('-L', args)
        self.assertIn('-N', args)
        self.assertIn('-n', args)
        self.assertIn('-q', args)

        self.assertIn('-g', args)
        self.assertIn('-4', args)

    def test_custom_gcloud_bin(self):
        job = MRWordCount(['-r', 'dataproc', '--ssh-tunnel',
                           '--gcloud-bin', '/path/to/gcloud -v'])

        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

        self.assertEqual(self.mock_Popen.call_count, 1)
        args = self.mock_Popen.call_args[0][0]

        self.assertEqual(args[:4], ['/path/to/gcloud', '-v', 'compute', 'ssh'])

    def test_missing_gcloud_bin(self):
        self.mock_Popen.side_effect = OSError(2, 'No such file or directory')

        job = MRWordCount(['-r', 'dataproc', '--ssh-tunnel'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

        self.assertEqual(self.mock_Popen.call_count, 1)
        self.assertTrue(runner._give_up_on_ssh_tunnel)

    def test_error_from_gcloud_bin(self):
        self.mock_Popen.return_value.returncode = 255

        job = MRWordCount(['-r', 'dataproc', '--ssh-tunnel'])

        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

        self.assertGreater(self.mock_Popen.call_count, 1)
        self.assertFalse(runner._give_up_on_ssh_tunnel)


class MockLogEntriesTestCase(MockGoogleTestCase):
    """Superclass for tests that create fake log entries."""

    def setUp(self):
        super(MockLogEntriesTestCase, self).setUp()

        self.mock_cluster_id = LOGGING_CLUSTER_NAME

    def add_container_exit(self, container_id, returncode=143):
        payload = {
            'class': _CONTAINER_EXECUTOR_CLASS_NAME,
            'message': ('Exit code from container %s is : %d' % (
                container_id, returncode)),
        }

        self.add_mock_log_entry(
            payload,
            self.log_name('yarn-yarn-nodemanager'),
            resource=self.log_resource(),
        )

    def add_split(self, container_id, cluster=LOGGING_CLUSTER_NAME):
        self.add_entry(container_id, 'syslog', SPLIT_MESSAGE)

    def add_stack_trace(self, container_id, cluster=LOGGING_CLUSTER_NAME):
        self.add_entry(container_id, 'syslog', LOGGING_STACK_TRACE)

    def add_traceback(self, container_id, cluster=LOGGING_CLUSTER_NAME):
        self.add_entry(container_id, 'stderr', LOGGING_TRACEBACK)

    def add_entry(self, container_id, logname, message):
        payload = dict(
            application=APPLICATION_ID,
            container=container_id,
            container_logname=logname,
            message=message,
        )

        self.add_mock_log_entry(
            payload,
            self.log_name('yarn-userlogs'),
            resource=self.log_resource(),
        )

    def log_name(self, name):
        return 'projects/%s/logs/%s' % (self.mock_project_id, name)

    def log_resource(self):
        return dict(
            labels=dict(
                cluster_name=LOGGING_CLUSTER_NAME,
                project_id=self.mock_project_id,
                region=_DEFAULT_GCE_REGION,
            ),
            type='cloud_dataproc_cluster',
        )


class FailedTaskContainerIDsTestCase(MockLogEntriesTestCase):

    OTHER_APP_CONTAINER_ID = 'container_1234567890111_0001_01_000001'

    def setUp(self):
        super(FailedTaskContainerIDsTestCase, self).setUp()

        self.runner = DataprocJobRunner()
        self.runner._cluster_id = LOGGING_CLUSTER_NAME

    def test_empty(self):
        self.assertEqual(
            list(self.runner._failed_task_container_ids(APPLICATION_ID)),
            [])

    def test_one_failure(self):
        self.add_container_exit(CONTAINER_ID_1)

        self.assertEqual(
            list(self.runner._failed_task_container_ids(APPLICATION_ID)),
            [CONTAINER_ID_1])

    def test_reverse_order(self):
        self.add_container_exit(CONTAINER_ID_1)
        self.add_container_exit(CONTAINER_ID_2)

        self.assertEqual(
            list(self.runner._failed_task_container_ids(APPLICATION_ID)),
            [CONTAINER_ID_2, CONTAINER_ID_1])

    def test_ignore_failures_from_other_runs(self):
        self.add_container_exit(self.OTHER_APP_CONTAINER_ID)

        self.assertEqual(
            list(self.runner._failed_task_container_ids(APPLICATION_ID)),
            [])

    def test_ignore_zero_returncode(self):
        self.add_container_exit(CONTAINER_ID_1, returncode=0)

        self.assertEqual(
            list(self.runner._failed_task_container_ids(APPLICATION_ID)),
            [])


class FixTracebackTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(_fix_traceback(''), '')

    def test_fix_traceback_with_no_newlines(self):
        self.assertEqual(_fix_traceback(LOGGING_TRACEBACK),
                         TRACEBACK)

    def test_fix_traceback_plus_log4j_warnings(self):
        self.assertEqual(
            _fix_traceback(LOGGING_TRACEBACK + LOG4J_WARNINGS),
            TRACEBACK)

    def test_no_need_to_fix(self):
        self.assertEqual(_fix_traceback(TRACEBACK), TRACEBACK)

    def test_can_strip_log4j_warnings_from_correct_traceback(self):
        self.assertEqual(
            _fix_traceback(TRACEBACK + LOG4J_WARNINGS),
            TRACEBACK)

    def test_something_else(self):
        message = 'mice in your kitchen\nare bad'

        self.assertEqual(_fix_traceback(message), message)


class FixJavaStackTraceTestCase(TestCase):

    STACK_TRACE = (
        'Diagnostics report from attempt_1525195653111_0001_m_000000_3:'
        ' Error: java.lang.RuntimeException: PipeMapRed.waitOutputThreads():'
        ' subprocess failed with code 1\n'
        '\tat org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads'
        '(PipeMapRed.java:322)\n'
        '\tat org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:158)'
    )

    LOGGING_STACK_TRACE = STACK_TRACE.replace('\n', '')

    def test_add_missing_newlines(self):
        self.assertEqual(_fix_java_stack_trace(LOGGING_STACK_TRACE),
                         STACK_TRACE)

    def test_no_need_to_fix(self):
        self.assertEqual(_fix_java_stack_trace(STACK_TRACE),
                         STACK_TRACE)

    def test_something_else(self):
        message = 'mice in your kitchen\nare bad'

        self.assertEqual(_fix_traceback(message), message)


class TaskLogInterpretationTestCase(MockLogEntriesTestCase):

    def setUp(self):
        super(TaskLogInterpretationTestCase, self).setUp()

        self.container_ids_method = self.start(patch(
            'mrjob.dataproc.DataprocJobRunner._failed_task_container_ids',
            return_value=[CONTAINER_ID_2, CONTAINER_ID_1]))

        self.runner = DataprocJobRunner()
        self.runner._cluster_id = LOGGING_CLUSTER_NAME

    def test_empty(self):
        self.assertEqual(
            self.runner._task_log_interpretation(APPLICATION_ID, 'streaming'),
            {})

    def test_find_error(self):
        self.add_split(CONTAINER_ID_1)
        self.add_stack_trace(CONTAINER_ID_1)
        self.add_traceback(CONTAINER_ID_1)

        interp = self.runner._task_log_interpretation(
            APPLICATION_ID, 'streaming')
        self.assertEqual(len(interp.get('errors', [])), 1)

        error = interp['errors'][0]

        self.assertEqual(error['container_id'], CONTAINER_ID_1)
        self.assertEqual(error['hadoop_error']['message'], STACK_TRACE)
        self.assertEqual(error['split'], SPLIT)
        self.assertEqual(error['task_error']['message'], TRACEBACK)

        self.assertTrue(interp.get('partial'))

    def test_stop_after_first_task_error(self):
        self.add_stack_trace(CONTAINER_ID_1)
        self.add_traceback(CONTAINER_ID_1)
        self.add_stack_trace(CONTAINER_ID_2)
        self.add_traceback(CONTAINER_ID_2)

        interp = self.runner._task_log_interpretation(
            APPLICATION_ID, 'streaming')

        self.assertEqual(len(interp.get('errors', [])), 1)
        error = interp['errors'][0]

        self.assertEqual(error['container_id'], CONTAINER_ID_2)
        self.assertTrue(interp.get('partial'))

    def test_keep_going_if_just_hadoop_error(self):
        self.add_stack_trace(CONTAINER_ID_1)
        self.add_traceback(CONTAINER_ID_1)
        self.add_stack_trace(CONTAINER_ID_2)

        interp = self.runner._task_log_interpretation(
            APPLICATION_ID, 'streaming')

        errors = interp.get('errors', [])
        self.assertEqual(len(errors), 2)

        self.assertEqual(errors[0]['container_id'], CONTAINER_ID_2)
        self.assertNotIn('task_error', errors[0])

        self.assertEqual(errors[1]['container_id'], CONTAINER_ID_1)
        self.assertIn('task_error', errors[1])

        self.assertTrue(interp.get('partial'))

    def test_hadoop_errors_only(self):
        self.add_stack_trace(CONTAINER_ID_1)
        self.add_stack_trace(CONTAINER_ID_2)

        interp = self.runner._task_log_interpretation(
            APPLICATION_ID, 'streaming')

        errors = interp.get('errors', [])
        self.assertEqual(len(errors), 2)

        self.assertEqual(errors[0]['container_id'], CONTAINER_ID_2)
        self.assertNotIn('task_error', errors[0])

        self.assertEqual(errors[1]['container_id'], CONTAINER_ID_1)
        self.assertNotIn('task_error', errors[1])

        self.assertFalse(interp.get('partial'))

    def test_task_error_only(self):
        self.add_traceback(CONTAINER_ID_1)

        self.assertEqual(
            self.runner._task_log_interpretation(APPLICATION_ID, 'streaming'),
            {})

    def test_not_partial(self):
        self.add_stack_trace(CONTAINER_ID_1)
        self.add_traceback(CONTAINER_ID_1)
        self.add_stack_trace(CONTAINER_ID_2)
        self.add_traceback(CONTAINER_ID_2)

        interp = self.runner._task_log_interpretation(
            APPLICATION_ID, 'streaming', partial=False)
        errors = interp.get('errors', [])

        self.assertEqual(len(errors), 2)

        self.assertFalse(interp.get('partial'))


class CauseOfErrorTestCase(MockLogEntriesTestCase):

    def setUp(self):
        super(CauseOfErrorTestCase, self).setUp()
        self.get_lines = self.start(patch(
            'mrjob.dataproc.DataprocJobRunner._get_new_driver_output_lines',
            return_value=[]))

    def test_end_to_end(self):
        # use LOGGING_CLUSTER_NAME so we can generage fake logging entries
        job = MRBoom(['-r', 'dataproc', '--cluster-id', LOGGING_CLUSTER_NAME])
        job.sandbox()

        self.mock_jobs_succeed = False

        # feed application_id into mock driver output
        self.get_lines.side_effect = [
            ['15/12/11 13:32:45 INFO impl.YarnClientImpl:'
             ' Submitted application %s' % APPLICATION_ID],
            [],
            [],
            [],
        ]

        self.add_container_exit(CONTAINER_ID_1)
        self.add_split(CONTAINER_ID_1)
        self.add_stack_trace(CONTAINER_ID_1)
        self.add_traceback(CONTAINER_ID_1)

        with job.make_runner() as runner:
            self.assertRaises(StepFailedException, runner.run)

            self.assertEqual(len(runner._log_interpretations), 1)
            interp = runner._log_interpretations[0]

            self.assertIn('step', interp)
            self.assertIn('history', interp)
            self.assertIn('task', interp)

            error = _pick_error(interp)
            self.assertIsNotNone(error)

            self.assertEqual(error['split'], SPLIT)
            self.assertEqual(error['hadoop_error']['message'], STACK_TRACE)
            self.assertEqual(error['task_error']['message'], TRACEBACK)


class CloudPartSizeTestCase(MockGoogleTestCase):

    def setUp(self):
        super(CloudPartSizeTestCase, self).setUp()

        self.upload_from_string = self.start(patch(
            'tests.mock_google.storage.MockGoogleStorageBlob'
            '.upload_from_string',
            side_effect=MockGoogleStorageBlob.upload_from_string,
            autospec=True))

    def test_default(self):
        runner = DataprocJobRunner()

        self.assertEqual(runner._fs_chunk_size(), 100 * 1024 * 1024)

    def test_float(self):
        runner = DataprocJobRunner(cloud_part_size_mb=0.25)

        self.assertEqual(runner._fs_chunk_size(), 256 * 1024)

    def test_zero(self):
        runner = DataprocJobRunner(cloud_part_size_mb=0)

        self.assertEqual(runner._fs_chunk_size(), None)

    def test_multipart_upload(self):
        job = MRWordCount(
            ['-r', 'dataproc', '--cloud-part-size-mb', '2'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._prepare_for_launch()

        # chunk size should be set on blob object used to upload
        self.assertTrue(self.upload_from_string.called)
        for call_args in self.upload_from_string.call_args_list:
            blob = call_args[0][0]
            self.assertEqual(blob.chunk_size, 2 * 1024 * 1024)


class JarStepTestCase(MockGoogleTestCase):

    def test_local_jar_gets_uploaded(self):
        fake_jar = os.path.join(self.tmp_dir, 'fake.jar')
        with open(fake_jar, 'w'):
            pass

        job = MRJustAJar(['-r', 'dataproc', '--jar', fake_jar])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            self.assertIn(fake_jar, runner._upload_mgr.path_to_uri())
            jar_uri = runner._upload_mgr.uri(fake_jar)
            self.assertTrue(runner.fs.ls(jar_uri))

            jobs = list(runner._list_jobs(cluster_name=runner._cluster_id))

            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].hadoop_job.main_jar_file_uri, jar_uri)

    def test_jar_on_gcs(self):
        jar_uri = 'gs://dubliners/whiskeyinthe.jar'
        self.put_gcs_multi({jar_uri: b''})

        job = MRJustAJar(['-r', 'dataproc', '--jar', jar_uri])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            jobs = list(runner._list_jobs(cluster_name=runner._cluster_id))

            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].hadoop_job.main_jar_file_uri, jar_uri)

            # for comparison with test_main_class()
            self.assertFalse(jobs[0].hadoop_job.main_class)

    def test_main_class(self):
        jar_uri = 'gs://dubliners/whiskeyinthe.jar'
        self.put_gcs_multi({jar_uri: b''})

        job = MRJustAJar(['-r', 'dataproc', '--jar', jar_uri,
                          '--main-class', 'ThingAnalyzer'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            jobs = list(runner._list_jobs(cluster_name=runner._cluster_id))

            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].hadoop_job.jar_file_uris, [jar_uri])
            self.assertEqual(jobs[0].hadoop_job.main_class, 'ThingAnalyzer')

            # main_jar_file_uri and main_class are mutually exclusive
            self.assertFalse(jobs[0].hadoop_job.main_jar_file_uri)

    def test_jar_inside_dataproc(self):
        jar_uri = (
            'file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar')

        job = MRJustAJar(['-r', 'dataproc', '--jar', jar_uri])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            jobs = list(runner._list_jobs(cluster_name=runner._cluster_id))

            self.assertEqual(len(jobs), 1)
            # Dataproc accepts file:// URIs as-is
            self.assertEqual(jobs[0].hadoop_job.main_jar_file_uri, jar_uri)

    def test_input_output_interpolation(self):
        fake_jar = os.path.join(self.tmp_dir, 'fake.jar')
        open(fake_jar, 'w').close()
        input1 = os.path.join(self.tmp_dir, 'input1')
        open(input1, 'w').close()
        input2 = os.path.join(self.tmp_dir, 'input2')
        open(input2, 'w').close()

        job = MRJarAndStreaming(
            ['-r', 'dataproc', '--jar', fake_jar, input1, input2])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            jobs = sorted(runner._list_jobs(cluster_name=runner._cluster_id),
                          key=lambda j: j.reference.job_id)

            self.assertEqual(len(jobs), 2)
            jar_job, streaming_job = jobs

            jar_uri = runner._upload_mgr.uri(fake_jar)

            self.assertEqual(jar_job.hadoop_job.main_jar_file_uri, jar_uri)
            jar_args = jar_job.hadoop_job.args

            self.assertEqual(len(jar_args), 3)
            self.assertEqual(jar_args[0], 'stuff')

            # check input is interpolated
            input_arg = ','.join(
                runner._upload_mgr.uri(path) for path in (input1, input2))
            self.assertEqual(jar_args[1], input_arg)

            # check output of jar is input of next step
            jar_output_arg = jar_args[2]

            streaming_args = list(streaming_job.hadoop_job.args)
            streaming_input_arg = streaming_args[
                streaming_args.index('-input') + 1]
            self.assertEqual(jar_output_arg, streaming_input_arg)


class HadoopStreamingJarTestCase(MockGoogleTestCase):

    def get_runner_and_job(self, *args):
        mr_job = MRWordCount(['-r', 'dataproc'] + list(args))
        mr_job.sandbox()

        runner = mr_job.make_runner()
        runner.run()

        jobs = list(runner._list_jobs(cluster_name=runner._cluster_id))
        self.assertEqual(len(jobs), 1)

        return runner, jobs[0]

    def test_default(self):
        runner, job = self.get_runner_and_job()

        self.assertEqual(job.hadoop_job.main_jar_file_uri,
                         _HADOOP_STREAMING_JAR_URI)

    def test_local_hadoop_streaming_jar(self):
        jar_path = os.path.join(self.tmp_dir, 'righteousness.jar')
        open(jar_path, 'w').close()

        runner, job = self.get_runner_and_job(
            '--hadoop-streaming-jar', jar_path)

        jar_uri = runner._upload_mgr.uri(jar_path)

        self.assertEqual(job.hadoop_job.main_jar_file_uri, jar_uri)

    def test_hadoop_streaming_jar_on_node(self):
        jar_uri = 'file:///path/to/victory.jar'

        runner, job = self.get_runner_and_job(
            '--hadoop-streaming-jar', jar_uri)

        self.assertEqual(job.hadoop_job.main_jar_file_uri, jar_uri)

    def test_hadoop_streaming_jar_on_gcs(self):
        jar_uri = 'gs://dubliners/whiskeyinthe.jar'
        self.put_gcs_multi({jar_uri: b''})

        runner, job = self.get_runner_and_job(
            '--hadoop-streaming-jar', jar_uri)

        self.assertEqual(job.hadoop_job.main_jar_file_uri, jar_uri)


class NetworkAndSubnetworkTestCase(MockGoogleTestCase):

    def _get_project_id_and_gce_config(self, *args):
        job = MRWordCount(['-r', 'dataproc'] + list(args))
        job.sandbox()

        with job.make_runner() as runner:
            runner._launch()
            cluster = runner._get_cluster(
                runner._cluster_id)
            return cluster.project_id, cluster.config.gce_cluster_config


    def test_default(self):
        project_id, gce_config = self._get_project_id_and_gce_config()

        self.assertEqual(
            gce_config.network_uri,
            'https://www.googleapis.com/compute/v1/projects/%s'
            '/global/networks/default' % project_id)
        self.assertFalse(gce_config.subnetwork_uri)

    def test_network_name(self):
        project_id, gce_config = self._get_project_id_and_gce_config(
            '--network', 'test')

        self.assertEqual(
            gce_config.network_uri,
            'https://www.googleapis.com/compute/v1/projects/%s'
            '/global/networks/test' % project_id)
        self.assertFalse(gce_config.subnetwork_uri)

    def test_network_path(self):
        project_id, gce_config = self._get_project_id_and_gce_config(
            '--network', 'projects/manhattan/global/networks/secret')

        self.assertEqual(
            gce_config.network_uri,
            'https://www.googleapis.com/compute/v1'
            '/projects/manhattan/global/networks/secret')
        self.assertFalse(gce_config.subnetwork_uri)

    def test_network_uri(self):
        project_id, gce_config = self._get_project_id_and_gce_config(
            '--network', 'https://www.cnn.com/')

        self.assertEqual(
            gce_config.network_uri, 'https://www.cnn.com/')
        self.assertFalse(gce_config.subnetwork_uri)

    def test_subnetwork_name(self):
        project_id, gce_config = self._get_project_id_and_gce_config(
            '--subnet', 'test')

        self.assertEqual(
            gce_config.subnetwork_uri,
            'https://www.googleapis.com/compute/v1/projects/%s'
            '/us-west1/subnetworks/test' % project_id)
        self.assertFalse(gce_config.network_uri)

    def test_subnetwork_path(self):
        project_id, gce_config = self._get_project_id_and_gce_config(
            '--subnet', 'projects/manhattan/los-alamos/networks/lanl')

        self.assertEqual(
            gce_config.subnetwork_uri,
            'https://www.googleapis.com/compute/v1'
            '/projects/manhattan/los-alamos/networks/lanl')
        self.assertFalse(gce_config.network_uri)

    def test_subnetwork_uri(self):
        project_id, gce_config = self._get_project_id_and_gce_config(
            '--subnet', 'https://www.cnn.com/specials/videos/hln')

        self.assertEqual(
            gce_config.subnetwork_uri,
            'https://www.cnn.com/specials/videos/hln')
        self.assertFalse(gce_config.network_uri)

    def test_network_and_subnet_conflict(self):
        self.assertRaises(
            InvalidArgument,
            self._get_project_id_and_gce_config,
            '--network', 'default',
            '--subnet', 'default')

    def test_network_on_cmd_line_overrides_subnet_in_config(self):
        conf_path = self.makefile(
            'mrjob.conf',
            b'runners:\n  dataproc:\n    subnet: default')

        self.mrjob_conf_patcher.stop()
        project_id, gce_config = self._get_project_id_and_gce_config(
            '-c', conf_path, '--network', 'test')
        self.mrjob_conf_patcher.start()

        self.assertEqual(
            gce_config.network_uri,
            'https://www.googleapis.com/compute/v1/projects/%s'
            '/global/networks/test' % project_id)
        self.assertFalse(gce_config.subnetwork_uri)

    def test_subnet_on_cmd_line_overrides_network_in_config(self):
        conf_path = self.makefile(
            'mrjob.conf',
            b'runners:\n  dataproc:\n    network: default')

        self.mrjob_conf_patcher.stop()
        project_id, gce_config = self._get_project_id_and_gce_config(
            '-c', conf_path, '--subnet', 'test')
        self.mrjob_conf_patcher.start()

        self.assertEqual(
            gce_config.subnetwork_uri,
            'https://www.googleapis.com/compute/v1/projects/%s'
            '/us-west1/subnetworks/test' % project_id)
        self.assertFalse(gce_config.network_uri)
