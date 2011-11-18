# Copyright 2009-2011 Yelp
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

from __future__ import with_statement

import bz2
import copy
import datetime
import getpass
import gzip
import logging
import os
import posixpath
import py_compile
import shutil
from StringIO import StringIO
import tempfile
from testify import TestCase
from testify import assert_equal
from testify import assert_gt
from testify import assert_in
from testify import assert_not_in
from testify import assert_raises
from testify import setup
from testify import teardown
from testify import assert_not_equal

import mrjob
from mrjob.conf import dump_mrjob_conf
import mrjob.emr
from mrjob.emr import EMRJobRunner
from mrjob.emr import describe_all_job_flows
from mrjob.emr import attempt_to_acquire_lock
from mrjob.emr import _lock_acquire_step_1
from mrjob.emr import _lock_acquire_step_2
from mrjob.parse import JOB_NAME_RE
from mrjob.parse import parse_s3_uri
from mrjob.ssh import SSH_LOG_ROOT
from mrjob.ssh import SSH_PREFIX
from mrjob.util import tar_and_gzip

from tests.mockboto import MockS3Connection
from tests.mockboto import MockEmrConnection
from tests.mockboto import MockEmrObject
from tests.mockboto import add_mock_s3_data
from tests.mockboto import DEFAULT_MAX_JOB_FLOWS_RETURNED
from tests.mockboto import to_iso8601
from tests.mockssh import create_mock_ssh_script
from tests.mockssh import mock_ssh_dir
from tests.mockssh import mock_ssh_file
from tests.mr_two_step_job import MRTwoStepJob
from tests.mr_hadoop_format_job import MRHadoopFormatJob
from tests.quiet import logger_disabled
from tests.quiet import no_handlers_for_logger

try:
    import boto
    import boto.emr
    from mrjob import boto_2_1_rc2
except ImportError:
    boto = None


class MockEMRAndS3TestCase(TestCase):

    @setup
    def make_mrjob_conf(self):
        _, self.mrjob_conf_path = tempfile.mkstemp(prefix='mrjob.conf.')
        with open(self.mrjob_conf_path, 'w') as f:
            dump_mrjob_conf({'runners': {'emr': {
                'check_emr_status_every': 0.01,
                's3_sync_wait_time': 0.01,
            }}}, f)

    @teardown
    def rm_mrjob_conf(self):
        os.unlink(self.mrjob_conf_path)

    @setup
    def sandbox_boto(self):
        self.mock_s3_fs = {}
        self.mock_emr_job_flows = {}
        self.mock_emr_failures = {}
        self.mock_emr_output = {}

        def mock_boto_connect_s3(*args, **kwargs):
            kwargs['mock_s3_fs'] = self.mock_s3_fs
            return MockS3Connection(*args, **kwargs)

        def mock_boto_emr_EmrConnection(*args, **kwargs):
            kwargs['mock_s3_fs'] = self.mock_s3_fs
            kwargs['mock_emr_job_flows'] = self.mock_emr_job_flows
            kwargs['mock_emr_failures'] = self.mock_emr_failures
            kwargs['mock_emr_output'] = self.mock_emr_output
            return MockEmrConnection(*args, **kwargs)

        self._real_boto_connect_s3 = boto.connect_s3
        boto.connect_s3 = mock_boto_connect_s3

        self._real_boto_2_1_rc2_EmrConnection = boto_2_1_rc2.EmrConnection
        boto_2_1_rc2.EmrConnection = mock_boto_emr_EmrConnection

        # copy the old environment just to be polite
        self._old_environ = os.environ.copy()

    @teardown
    def unsandbox_boto(self):
        boto.connect_s3 = self._real_boto_connect_s3
        boto_2_1_rc2.EmrConnection = self._real_boto_2_1_rc2_EmrConnection

    def add_mock_s3_data(self, data):
        """Update self.mock_s3_fs with a map from bucket name
        to key name to data."""
        add_mock_s3_data(self.mock_s3_fs, data)

    def prepare_runner_for_ssh(self, runner, num_slaves=0):
        # Set up environment variables
        self._old_environ = os.environ.copy()
        os.environ['MOCK_SSH_VERIFY_KEY_FILE'] = 'true'

        # Create temporary directories and add them to MOCK_SSH_ROOTS
        self.master_ssh_root = tempfile.mkdtemp(prefix='master_ssh_root.')
        os.environ['MOCK_SSH_ROOTS'] = 'testmaster=%s' % self.master_ssh_root
        mock_ssh_dir('testmaster', SSH_LOG_ROOT + '/history')

        self.slave_ssh_roots = []

        # Make the fake binary
        os.mkdir(os.path.join(self.master_ssh_root, 'bin'))
        self.ssh_bin = os.path.join(self.master_ssh_root, 'bin', 'ssh')
        create_mock_ssh_script(self.ssh_bin)

        # Make a fake keyfile so that the 'file exists' requirements are
        # satsified
        self.keyfile_path = os.path.join(self.master_ssh_root, 'key.pem')
        with open(self.keyfile_path, 'w') as f:
            f.write('I AM DEFINITELY AN SSH KEY FILE')

        # Tell the runner to use the fake binary
        runner._opts['ssh_bin'] = [self.ssh_bin]
        # Inject master node hostname so it doesn't try to 'emr --describe' it
        runner._address = 'testmaster'
        # Also pretend to have an SSH key pair file
        runner._opts['ec2_key_pair_file'] = self.keyfile_path

    def add_slave(self):
        """Add a mocked slave to the cluster. Caller is responsible for setting
        runner._opts['num_ec2_instances'] to the correct number.
        """
        slave_num = len(self.slave_ssh_roots)
        new_dir = tempfile.mkdtemp(prefix='slave_%d_ssh_root.' % slave_num)
        self.slave_ssh_roots.append(new_dir)
        os.environ['MOCK_SSH_ROOTS'] += (':testmaster!testslave%d=%s'
                                         % (slave_num, new_dir))

    def teardown_ssh(self):
        os.environ.clear()
        os.environ.update(self._old_environ)
        shutil.rmtree(self.master_ssh_root)
        for path in self.slave_ssh_roots:
            shutil.rmtree(path)


class EMRJobRunnerEndToEndTestCase(MockEMRAndS3TestCase):

    @setup
    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    @teardown
    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    @setup
    def put_additional_emr_info_in_mrjob_conf(self):
        dump_mrjob_conf({'runners': {'emr': {
            'check_emr_status_every': 0.01,
            's3_sync_wait_time': 0.01,
            'additional_emr_info': {'key': 'value'},
        }}}, open(self.mrjob_conf_path, 'w'))

    def test_end_to_end(self):
        # read from STDIN, a local file, and a remote file
        stdin = StringIO('foo\nbar\n')

        local_input_path = os.path.join(self.tmp_dir, 'input')
        with open(local_input_path, 'w') as local_input_file:
            local_input_file.write('bar\nqux\n')

        remote_input_path = 's3://walrus/data/foo'
        self.add_mock_s3_data({'walrus': {'data/foo': 'foo\n'}})

        # setup fake output
        self.mock_emr_output = {('j-MOCKJOBFLOW0', 1): [
            '1\t"qux"\n2\t"bar"\n', '2\t"foo"\n5\tnull\n']}

        mr_job = MRHadoopFormatJob(['-r', 'emr', '-v',
                                    '-c', self.mrjob_conf_path,
                                    '-', local_input_path, remote_input_path])
        mr_job.sandbox(stdin=stdin)

        local_tmp_dir = None
        results = []

        mock_s3_fs_snapshot = copy.deepcopy(self.mock_s3_fs)

        with mr_job.make_runner() as runner:
            assert isinstance(runner, EMRJobRunner)

            # make sure that initializing the runner doesn't affect S3
            # (Issue #50)
            assert_equal(mock_s3_fs_snapshot, self.mock_s3_fs)

            # make sure AdditionalInfo was JSON-ified from the config file.
            # checked now because you can't actually read it from the job flow
            # on real EMR.
            assert_equal(runner._opts['additional_emr_info'],
                         '{"key": "value"}')

            runner.run()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

            local_tmp_dir = runner._get_local_tmp_dir()
            # make sure cleanup hasn't happened yet
            assert os.path.exists(local_tmp_dir)
            assert any(runner.ls(runner.get_output_dir()))

            emr_conn = runner.make_emr_conn()
            job_flow = emr_conn.describe_jobflow(runner.get_emr_job_flow_id())
            assert_equal(job_flow.state, 'COMPLETED')
            name_match = JOB_NAME_RE.match(job_flow.name)
            assert_equal(name_match.group(1), 'mr_hadoop_format_job')
            assert_equal(name_match.group(2), getpass.getuser())

            # make sure our input and output formats are attached to
            # the correct steps
            assert_in('-inputformat', job_flow.steps[0].args())
            assert_not_in('-outputformat', job_flow.steps[0].args())
            assert_not_in('-inputformat', job_flow.steps[1].args())
            assert_in('-outputformat', job_flow.steps[1].args())

            # make sure mrjob.tar.gz is created and uploaded as
            # a bootstrap file
            assert runner._mrjob_tar_gz_path
            mrjob_tar_gz_file_dicts = [
                file_dict for file_dict in runner._files
                if file_dict['path'] == runner._mrjob_tar_gz_path]

            assert_equal(len(mrjob_tar_gz_file_dicts), 1)

            mrjob_tar_gz_file_dict = mrjob_tar_gz_file_dicts[0]
            assert mrjob_tar_gz_file_dict['name']
            assert_equal(mrjob_tar_gz_file_dict.get('bootstrap'), 'file')

            # shouldn't be in PYTHONPATH (we dump it directly in site-packages)
            pythonpath = runner._get_cmdenv().get('PYTHONPATH') or ''
            assert_not_in(mrjob_tar_gz_file_dict['name'],
                          pythonpath.split(':'))

        assert_equal(sorted(results),
                     [(1, 'qux'), (2, 'bar'), (2, 'foo'), (5, None)])

        # make sure cleanup happens
        assert not os.path.exists(local_tmp_dir)
        assert not any(runner.ls(runner.get_output_dir()))

        # job should get terminated
        emr_conn = runner.make_emr_conn()
        job_flow_id = runner.get_emr_job_flow_id()
        for _ in xrange(10):
            emr_conn.simulate_progress(job_flow_id)

        job_flow = emr_conn.describe_jobflow(job_flow_id)
        assert_equal(job_flow.state, 'TERMINATED')

    def test_failed_job(self):
        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '-c', self.mrjob_conf_path])
        mr_job.sandbox()

        self.add_mock_s3_data({'walrus': {}})
        self.mock_emr_failures = {('j-MOCKJOBFLOW0', 0): None}

        with mr_job.make_runner() as runner:
            assert isinstance(runner, EMRJobRunner)

            with logger_disabled('mrjob.emr'):
                assert_raises(Exception, runner.run)

            emr_conn = runner.make_emr_conn()
            job_flow_id = runner.get_emr_job_flow_id()
            for _ in xrange(10):
                emr_conn.simulate_progress(job_flow_id)

            job_flow = emr_conn.describe_jobflow(job_flow_id)
            assert_equal(job_flow.state, 'FAILED')

        # job should get terminated on cleanup
        emr_conn = runner.make_emr_conn()
        job_flow_id = runner.get_emr_job_flow_id()
        for _ in xrange(10):
            emr_conn.simulate_progress(job_flow_id)

        job_flow = emr_conn.describe_jobflow(job_flow_id)
        assert_equal(job_flow.state, 'TERMINATED')

    def _test_remote_scratch_cleanup(self, mode, scratch_len, log_len):
        self.add_mock_s3_data({'walrus': {'logs/j-MOCKJOBFLOW0/1': '1\n'}})
        # read from STDIN, a local file, and a remote file
        stdin = StringIO('foo\nbar\n')

        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '-c', self.mrjob_conf_path,
                               '--s3-log-uri', 's3://walrus/logs',
                               '-', '--cleanup', mode])
        mr_job.sandbox(stdin=stdin)

        with mr_job.make_runner() as runner:
            s3_scratch_uri = runner._opts['s3_scratch_uri']
            scratch_bucket, _ = parse_s3_uri(s3_scratch_uri)

            runner.run()

            # this is set and unset before we can get at it unless we do this
            log_bucket, _ = parse_s3_uri(runner._s3_job_log_uri)

            list(runner.stream_output())

        conn = runner.make_s3_conn()
        bucket = conn.get_bucket(scratch_bucket)
        assert_equal(len(list(bucket.list())), scratch_len)

        bucket = conn.get_bucket(log_bucket)
        assert_equal(len(list(bucket.list())), log_len)

    def test_cleanup_all(self):
        self._test_remote_scratch_cleanup('ALL', 0, 0)

    def test_cleanup_scratch(self):
        self._test_remote_scratch_cleanup('SCRATCH', 0, 1)

    def test_cleanup_remote(self):
        self._test_remote_scratch_cleanup('REMOTE_SCRATCH', 0, 1)

    def test_cleanup_local(self):
        self._test_remote_scratch_cleanup('LOCAL_SCRATCH', 5, 1)

    def test_cleanup_logs(self):
        self._test_remote_scratch_cleanup('LOGS', 5, 0)

    def test_cleanup_none(self):
        self._test_remote_scratch_cleanup('NONE', 5, 1)

    def test_cleanup_combine(self):
        self._test_remote_scratch_cleanup('LOGS,REMOTE_SCRATCH', 0, 0)

    def test_cleanup_error(self):
        assert_raises(ValueError, self._test_remote_scratch_cleanup,
                      'NONE,LOGS,REMOTE_SCRATCH', 0, 0)
        assert_raises(ValueError, self._test_remote_scratch_cleanup,
                      'GARBAGE', 0, 0)

    def test_args_version_018(self):
        self.add_mock_s3_data({'walrus': {'logs/j-MOCKJOBFLOW0/1': '1\n'}})
        # read from STDIN, a local file, and a remote file
        stdin = StringIO('foo\nbar\n')

        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '--hadoop-version=0.18',
                               '-c', self.mrjob_conf_path])
        mr_job.sandbox(stdin=stdin)

        with mr_job.make_runner() as runner:
            runner.run()
            assert_not_in('-files', runner._describe_jobflow().steps[0].args())
            assert_in('-cacheFile', runner._describe_jobflow().steps[0].args())
            assert_not_in('-combiner',
                          runner._describe_jobflow().steps[0].args())

    def test_args_version_020(self):
        self.add_mock_s3_data({'walrus': {'logs/j-MOCKJOBFLOW0/1': '1\n'}})
        # read from STDIN, a local file, and a remote file
        stdin = StringIO('foo\nbar\n')

        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '--hadoop-version=0.20',
                               '-c', self.mrjob_conf_path])
        mr_job.sandbox(stdin=stdin)

        with mr_job.make_runner() as runner:
            runner.run()
            assert_in('-files', runner._describe_jobflow().steps[0].args())
            assert_not_in('-cacheFile',
                          runner._describe_jobflow().steps[0].args())
            assert_in('-combiner', runner._describe_jobflow().steps[0].args())


class S3ScratchURITestCase(MockEMRAndS3TestCase):

    def test_pick_scratch_uri(self):
        self.add_mock_s3_data({'mrjob-walrus': {}, 'zebra': {}})
        runner = EMRJobRunner(conf_path=False)

        assert_equal(runner._opts['s3_scratch_uri'],
                     's3://mrjob-walrus/tmp/')

    def test_create_scratch_uri(self):
        # "walrus" bucket will be ignored; it doesn't start with "mrjob-"
        self.add_mock_s3_data({'walrus': {}, 'zebra': {}})

        runner = EMRJobRunner(conf_path=False, s3_sync_wait_time=0.01)

        # bucket name should be mrjob- plus 16 random hex digits
        s3_scratch_uri = runner._opts['s3_scratch_uri']
        assert_equal(s3_scratch_uri[:11], 's3://mrjob-')
        assert_equal(s3_scratch_uri[27:], '/tmp/')

        # bucket shouldn't actually exist yet
        scratch_bucket, _ = parse_s3_uri(s3_scratch_uri)
        assert_not_in(scratch_bucket, self.mock_s3_fs.keys())

        # need to do something to ensure that the bucket actually gets
        # created. let's launch a (mock) job flow
        job_flow_id = runner.make_persistent_job_flow()
        assert_in(scratch_bucket, self.mock_s3_fs.keys())
        runner.make_emr_conn().terminate_jobflow(job_flow_id)

        # once our scratch bucket is created, we should re-use it
        runner2 = EMRJobRunner(conf_path=False)
        s3_scratch_uri = runner._opts['s3_scratch_uri']
        assert_equal(runner2._opts['s3_scratch_uri'], s3_scratch_uri)


class BootstrapFilesTestCase(MockEMRAndS3TestCase):

    def test_bootstrap_files_only_get_uploaded_once(self):
        # just a regression test for Issue #8

        # use self.mrjob_conf_path because it's easier than making a new file
        bootstrap_file = self.mrjob_conf_path

        runner = EMRJobRunner(conf_path=False,
                              bootstrap_files=[bootstrap_file])

        matching_file_dicts = [fd for fd in runner._files
                               if fd['path'] == bootstrap_file]
        assert_equal(len(matching_file_dicts), 1)


class ExistingJobFlowTestCase(MockEMRAndS3TestCase):

    def test_attach_to_existing_job_flow(self):
        emr_conn = EMRJobRunner(conf_path=False).make_emr_conn()
        # set log_uri to None, so that when we describe the job flow, it
        # won't have the loguri attribute, to test Issue #112
        emr_job_flow_id = emr_conn.run_jobflow(
            name='Development Job Flow', log_uri=None,
            keep_alive=True)

        stdin = StringIO('foo\nbar\n')
        self.mock_emr_output = {(emr_job_flow_id, 1): [
            '1\t"bar"\n1\t"foo"\n2\tnull\n']}

        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '-c', self.mrjob_conf_path,
                               '--emr-job-flow-id', emr_job_flow_id])
        mr_job.sandbox(stdin=stdin)

        results = []
        with mr_job.make_runner() as runner:
            runner.run()

            # Issue 182: don't create the bootstrap script when
            # attaching to another job flow
            assert_equal(runner._master_bootstrap_script, None)

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

        assert_equal(sorted(results),
            [(1, 'bar'), (1, 'foo'), (2, None)])

    def test_dont_take_down_job_flow_on_failure(self):
        emr_conn = EMRJobRunner(conf_path=False).make_emr_conn()
        # set log_uri to None, so that when we describe the job flow, it
        # won't have the loguri attribute, to test Issue #112
        emr_job_flow_id = emr_conn.run_jobflow(
            name='Development Job Flow', log_uri=None,
            keep_alive=True)

        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '-c', self.mrjob_conf_path,
                               '--emr-job-flow-id', emr_job_flow_id])
        mr_job.sandbox()

        self.add_mock_s3_data({'walrus': {}})
        self.mock_emr_failures = {('j-MOCKJOBFLOW0', 0): None}

        with mr_job.make_runner() as runner:
            assert isinstance(runner, EMRJobRunner)

            with logger_disabled('mrjob.emr'):
                assert_raises(Exception, runner.run)

            emr_conn = runner.make_emr_conn()
            job_flow_id = runner.get_emr_job_flow_id()
            for _ in xrange(10):
                emr_conn.simulate_progress(job_flow_id)

            job_flow = emr_conn.describe_jobflow(job_flow_id)
            assert_equal(job_flow.state, 'WAITING')

        # job shouldn't get terminated by cleanup
        emr_conn = runner.make_emr_conn()
        job_flow_id = runner.get_emr_job_flow_id()
        for _ in xrange(10):
            emr_conn.simulate_progress(job_flow_id)

        job_flow = emr_conn.describe_jobflow(job_flow_id)
        assert_equal(job_flow.state, 'WAITING')


class HadoopVersionTestCase(MockEMRAndS3TestCase):

    def test_default_hadoop_version(self):
        stdin = StringIO('foo\nbar\n')
        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '-c', self.mrjob_conf_path])
        mr_job.sandbox(stdin=stdin)

        with mr_job.make_runner() as runner:
            runner.run()

            emr_conn = runner.make_emr_conn()
            job_flow = emr_conn.describe_jobflow(runner.get_emr_job_flow_id())

            assert_equal(job_flow.hadoopversion, '0.20')

    def test_set_hadoop_version(self):
        stdin = StringIO('foo\nbar\n')
        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '-c', self.mrjob_conf_path,
                               '--hadoop-version', '0.18'])
        mr_job.sandbox(stdin=stdin)

        with mr_job.make_runner() as runner:
            runner.run()

            emr_conn = runner.make_emr_conn()
            job_flow = emr_conn.describe_jobflow(runner.get_emr_job_flow_id())

            assert_equal(job_flow.hadoopversion, '0.18')


class AvailabilityZoneTestCase(MockEMRAndS3TestCase):

    @setup
    def put_availability_zone_in_mrjob_conf(self):
        dump_mrjob_conf({'runners': {'emr': {
            'check_emr_status_every': 0.01,
            's3_sync_wait_time': 0.01,
            'aws_availability_zone': 'PUPPYLAND',
        }}}, open(self.mrjob_conf_path, 'w'))

    def test_availability_zone_config(self):
        # Confirm that the mrjob.conf option 'aws_availability_zone' was
        #   propagated through to the job flow
        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '-c', self.mrjob_conf_path])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.run()

            emr_conn = runner.make_emr_conn()
            job_flow_id = runner.get_emr_job_flow_id()
            job_flow = emr_conn.describe_jobflow(job_flow_id)
            assert_equal(job_flow.availabilityzone, 'PUPPYLAND')

    def test_debugging_works(self):
        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                           '-c', self.mrjob_conf_path,
                           '--enable-emr-debugging'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.run()
            flow = runner.make_emr_conn().describe_jobflow(
                runner._emr_job_flow_id)
            assert_equal(flow.steps[0].name, 'Setup Hadoop Debugging')


class BucketRegionTestCase(MockEMRAndS3TestCase):

    @setup
    def make_dummy_data(self):
        self.add_mock_s3_data({'mrjob-1': {}})
        s3c = boto.connect_s3()
        self.bucket1 = s3c.get_bucket('mrjob-1')
        self.bucket1_uri = 's3://mrjob-1/tmp/'

    def test_region_nobucket_nolocation(self):
        # aws_region specified, no bucket specified, default bucket has no
        # location
        j = EMRJobRunner(aws_region='PUPPYLAND',
                         s3_endpoint='PUPPYLAND',
                         conf_path=False)
        assert_not_equal(j._opts['s3_scratch_uri'], self.bucket1_uri)

    def test_region_nobucket_nomatchexists(self):
        # aws_region specified, no bucket specified, no buckets have matching
        # region
        self.bucket1.set_location('PUPPYLAND')
        j = EMRJobRunner(aws_region='KITTYLAND',
                         s3_endpoint='KITTYLAND',
                         conf_path=False)
        assert_not_equal(j._opts['s3_scratch_uri'], self.bucket1_uri)

    def test_noregion_nobucket_nolocation(self):
        # aws_region not specified, no bucket specified, default bucket has no
        # location
        j = EMRJobRunner(conf_path=False)
        assert_equal(j._opts['s3_scratch_uri'], self.bucket1_uri)

    def test_noregion_bucket_nolocation(self):
        # aws_region not specified, bucket specified without location
        j = EMRJobRunner(conf_path=False,
                         s3_scratch_uri=self.bucket1_uri)
        assert_equal(j._opts['s3_scratch_uri'], self.bucket1_uri)

    def test_noregion_bucket_location(self):
        # aws_region not specified, bucket specified with location
        self.bucket1.set_location('PUPPYLAND')
        j = EMRJobRunner(conf_path=False)
        assert_equal(j._aws_region, 'PUPPYLAND')


class ExtraBucketRegionTestCase(MockEMRAndS3TestCase):

    @setup
    def make_dummy_data(self):
        self.add_mock_s3_data({'mrjob-1': {}})
        s3c = boto.connect_s3()
        self.bucket1 = s3c.get_bucket('mrjob-1')
        self.bucket1_uri = 's3://mrjob-1/tmp/'

        self.add_mock_s3_data({'mrjob-2': {}})
        self.bucket2 = s3c.get_bucket('mrjob-2')
        self.bucket2.set_location('KITTYLAND')
        self.bucket2_uri = 's3://mrjob-2/tmp/'

    def test_region_nobucket_matchexists(self):
        # aws_region specified, no bucket specified, bucket exists with
        # matching region
        j = EMRJobRunner(aws_region='KITTYLAND',
                         s3_endpoint='KITTYLAND',
                         conf_path=False)
        assert_equal(j._opts['s3_scratch_uri'], self.bucket2_uri)

    def test_region_bucket_match(self):
        # aws_region specified, bucket specified with matching location
        j = EMRJobRunner(aws_region='PUPPYLAND',
                         s3_endpoint='PUPPYLAND',
                         s3_scratch_uri=self.bucket1_uri,
                         conf_path=False)
        assert_equal(j._opts['s3_scratch_uri'], self.bucket1_uri)

    def test_region_bucket_does_not_match(self):
        # aws_region specified, bucket specified with incorrect location
        with no_handlers_for_logger():
            stderr = StringIO()
            log = logging.getLogger('mrjob.emr')
            log.addHandler(logging.StreamHandler(stderr))
            log.setLevel(logging.WARNING)

            EMRJobRunner(aws_region='PUPPYLAND',
                         s3_endpoint='PUPPYLAND',
                         s3_scratch_uri=self.bucket2_uri,
                         conf_path=False)

            assert_in('does not match bucket region', stderr.getvalue())


class DescribeAllJobFlowsTestCase(MockEMRAndS3TestCase):

    def test_can_get_all_job_flows(self):
        now = datetime.datetime.utcnow()

        NUM_JOB_FLOWS = 2222
        assert_gt(NUM_JOB_FLOWS, DEFAULT_MAX_JOB_FLOWS_RETURNED)

        for i in range(NUM_JOB_FLOWS):
            job_flow_id = 'j-%04d' % i
            self.mock_emr_job_flows[job_flow_id] = MockEmrObject(
                creationdatetime=to_iso8601(
                    now - datetime.timedelta(minutes=i)),
                jobflowid=job_flow_id)

        emr_conn = EMRJobRunner(conf_path=False).make_emr_conn()

        # ordinary describe_jobflows() hits the limit on number of job flows
        some_job_flows = emr_conn.describe_jobflows()
        assert_equal(len(some_job_flows), DEFAULT_MAX_JOB_FLOWS_RETURNED)

        all_job_flows = describe_all_job_flows(emr_conn)
        assert_equal(len(all_job_flows), NUM_JOB_FLOWS)
        assert_equal(sorted(jf.jobflowid for jf in all_job_flows),
                     [('j-%04d' % i) for i in range(NUM_JOB_FLOWS)])


class EC2InstanceTypeTestCase(MockEMRAndS3TestCase):

    def _test_instance_types(self, kwargs, expected_master, expected_slave):
        runner = EMRJobRunner(conf_path=self.mrjob_conf_path, **kwargs)

        job_flow_id = runner.make_persistent_job_flow()
        job_flow = runner.make_emr_conn().describe_jobflow(job_flow_id)

        assert_equal(
            (expected_master, expected_slave),
            (job_flow.masterinstancetype, job_flow.slaveinstancetype))

    def test_defaults(self):
        self._test_instance_types(
            {}, 'm1.small', 'm1.small')

        self._test_instance_types(
            {'num_ec2_instances': 2},
            'm1.small', 'm1.small')

    def test_single_instance(self):
        self._test_instance_types(
            {'ec2_instance_type': 'c1.xlarge'},
            'c1.xlarge', 'c1.xlarge')

    def test_multiple_instances(self):
        self._test_instance_types(
            {'ec2_instance_type': 'c1.xlarge', 'num_ec2_instances': 2},
            'm1.small', 'c1.xlarge')

    def test_explicit_master_and_slave_instance_types(self):
        self._test_instance_types(
            {'ec2_master_instance_type': 'm1.large'},
            'm1.large', 'm1.small')
        self._test_instance_types(
            {'ec2_slave_instance_type': 'm2.xlarge'},
            'm1.small', 'm2.xlarge')
        self._test_instance_types(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_slave_instance_type': 'm2.xlarge'},
            'm1.large', 'm2.xlarge')

    def test_ec2_instance_type_takes_precedence(self):
        self._test_instance_types(
            {'ec2_instance_type': 'c1.xlarge',
             'ec2_master_instance_type': 'm1.large',
             'ec2_slave_instance_type': 'm2.xlarge'},
            'c1.xlarge', 'c1.xlarge')
        # when there are multiple instances, ec2_instance_type only
        # sets slave instance type
        self._test_instance_types(
            {'ec2_instance_type': 'c1.xlarge',
             'ec2_master_instance_type': 'm1.large',
             'num_ec2_instances': 2,
             'ec2_slave_instance_type': 'm2.xlarge'},
            'm1.large', 'c1.xlarge')


### tests for error parsing ###

BUCKET = 'walrus'
BUCKET_URI = 's3://' + BUCKET + '/'

LOG_DIR = 'j-JOBFLOWID/'

GARBAGE = \
"""GarbageGarbageGarbage
"""

TRACEBACK_START = 'Traceback (most recent call last):\n'

PY_EXCEPTION = \
"""  File "<string>", line 1, in <module>
TypeError: 'int' object is not iterable
"""

CHILD_ERR_LINE = (
    '2010-07-27 18:25:48,397 WARN'
    ' org.apache.hadoop.mapred.TaskTracker (main): Error running child\n')

JAVA_STACK_TRACE = """java.lang.OutOfMemoryError: Java heap space
        at org.apache.hadoop.mapred.IFile$Reader.readNextBlock(IFile.java:270)
        at org.apache.hadoop.mapred.IFile$Reader.next(IFile.java:332)
"""

HADOOP_ERR_LINE_PREFIX = ('2010-07-27 19:53:35,451 ERROR'
                          ' org.apache.hadoop.streaming.StreamJob (main): ')

USEFUL_HADOOP_ERROR = (
    'Error launching job , Output path already exists :'
    ' Output directory s3://yourbucket/logs/2010/07/23/ already exists'
    ' and is not empty')

BORING_HADOOP_ERROR = 'Job not Successful!'
TASK_ATTEMPTS_DIR = LOG_DIR + 'task-attempts/'

ATTEMPT_0_DIR = TASK_ATTEMPTS_DIR + 'attempt_201007271720_0001_m_000126_0/'
ATTEMPT_1_DIR = TASK_ATTEMPTS_DIR + 'attempt_201007271720_0001_m_000126_0/'


def make_input_uri_line(input_uri):
    return ("2010-07-27 17:55:29,400 INFO"
            " org.apache.hadoop.fs.s3native.NativeS3FileSystem (main):"
            " Opening '%s' for reading\n" % input_uri)


class FindProbableCauseOfFailureTestCase(MockEMRAndS3TestCase):
    # We're mostly concerned here that the right log files are read in the
    # right order. parsing of the logs is handled by tests.parse_test

    @setup
    def make_runner(self):
        self.add_mock_s3_data({'walrus': {}})
        self.runner = EMRJobRunner(s3_sync_wait_time=0,
                                   s3_scratch_uri='s3://walrus/tmp',
                                   conf_path=False)
        self.runner._s3_job_log_uri = BUCKET_URI + LOG_DIR

    @teardown
    def cleanup_runner(self):
        self.runner.cleanup()

    def test_empty(self):
        self.add_mock_s3_data({'walrus': {}})
        assert_equal(self.runner._find_probable_cause_of_failure([1]), None)

    def test_python_exception(self):
        self.add_mock_s3_data({'walrus': {
            ATTEMPT_0_DIR + 'stderr':
                GARBAGE + TRACEBACK_START + PY_EXCEPTION + GARBAGE,
            ATTEMPT_0_DIR + 'syslog':
                make_input_uri_line(BUCKET_URI + 'input.gz'),
        }})
        assert_equal(self.runner._find_probable_cause_of_failure([1]),
                     {'lines': list(StringIO(TRACEBACK_START + PY_EXCEPTION)),
                      'log_file_uri':
                          BUCKET_URI + ATTEMPT_0_DIR + 'stderr',
                      'input_uri': BUCKET_URI + 'input.gz'})

    def test_python_exception_without_input_uri(self):
        self.add_mock_s3_data({'walrus': {
            ATTEMPT_0_DIR + 'stderr': (
                GARBAGE + TRACEBACK_START + PY_EXCEPTION + GARBAGE),
        }})
        assert_equal(self.runner._find_probable_cause_of_failure([1]),
                     {'lines': list(StringIO(TRACEBACK_START + PY_EXCEPTION)),
                      'log_file_uri':
                          BUCKET_URI + ATTEMPT_0_DIR + 'stderr',
                      'input_uri': None})

    def test_java_exception(self):
        self.add_mock_s3_data({'walrus': {
            ATTEMPT_0_DIR + 'stderr': GARBAGE + GARBAGE,
            ATTEMPT_0_DIR + 'syslog':
                make_input_uri_line(BUCKET_URI + 'input.gz') +
                GARBAGE +
                CHILD_ERR_LINE +
                JAVA_STACK_TRACE +
                GARBAGE,
        }})
        assert_equal(self.runner._find_probable_cause_of_failure([1]),
                     {'lines': list(StringIO(JAVA_STACK_TRACE)),
                      'log_file_uri':
                          BUCKET_URI + ATTEMPT_0_DIR + 'syslog',
                      'input_uri': BUCKET_URI + 'input.gz'})

    def test_java_exception_without_input_uri(self):
        self.add_mock_s3_data({'walrus': {
            ATTEMPT_0_DIR + 'syslog':
                CHILD_ERR_LINE +
                JAVA_STACK_TRACE +
                GARBAGE,
        }})
        assert_equal(self.runner._find_probable_cause_of_failure([1]),
                     {'lines': list(StringIO(JAVA_STACK_TRACE)),
                      'log_file_uri':
                          BUCKET_URI + ATTEMPT_0_DIR + 'syslog',
                      'input_uri': None})

    def test_hadoop_streaming_error(self):
        # we should look only at step 2 since the errors in the other
        # steps are boring
        #
        # we include input.gz just to test that we DON'T check for it
        self.add_mock_s3_data({'walrus': {
            LOG_DIR + 'steps/1/syslog':
                GARBAGE +
                HADOOP_ERR_LINE_PREFIX + BORING_HADOOP_ERROR + '\n',
            LOG_DIR + 'steps/2/syslog':
                GARBAGE +
                make_input_uri_line(BUCKET_URI + 'input.gz') +
                HADOOP_ERR_LINE_PREFIX + USEFUL_HADOOP_ERROR + '\n',
            LOG_DIR + 'steps/3/syslog':
                HADOOP_ERR_LINE_PREFIX + BORING_HADOOP_ERROR + '\n',
        }})

        assert_equal(self.runner._find_probable_cause_of_failure([1, 2, 3]),
                     {'lines': [USEFUL_HADOOP_ERROR + '\n'],
                      'log_file_uri':
                          BUCKET_URI + LOG_DIR + 'steps/2/syslog',
                      'input_uri': None})

    def test_later_task_attempt_steps_win(self):
        # should look at later steps first
        self.add_mock_s3_data({'walrus': {
            TASK_ATTEMPTS_DIR + 'attempt_201007271720_0001_r_000126_3/stderr':
                TRACEBACK_START + PY_EXCEPTION,
            TASK_ATTEMPTS_DIR + 'attempt_201007271720_0002_m_000004_0/syslog':
                CHILD_ERR_LINE + JAVA_STACK_TRACE,
        }})
        failure = self.runner._find_probable_cause_of_failure([1, 2])
        assert_equal(failure['log_file_uri'],
                     BUCKET_URI + TASK_ATTEMPTS_DIR +
                     'attempt_201007271720_0002_m_000004_0/syslog')

    def test_later_step_logs_win(self):
        self.add_mock_s3_data({'walrus': {
            LOG_DIR + 'steps/1/syslog':
                HADOOP_ERR_LINE_PREFIX + USEFUL_HADOOP_ERROR + '\n',
            LOG_DIR + 'steps/2/syslog':
                HADOOP_ERR_LINE_PREFIX + USEFUL_HADOOP_ERROR + '\n',
        }})
        failure = self.runner._find_probable_cause_of_failure([1, 2])
        assert_equal(failure['log_file_uri'],
                     BUCKET_URI + LOG_DIR + 'steps/2/syslog')

    def test_reducer_beats_mapper(self):
        # should look at reducers over mappers
        self.add_mock_s3_data({'walrus': {
            TASK_ATTEMPTS_DIR + 'attempt_201007271720_0001_m_000126_3/stderr':
                TRACEBACK_START + PY_EXCEPTION,
            TASK_ATTEMPTS_DIR + 'attempt_201007271720_0001_r_000126_3/syslog':
                CHILD_ERR_LINE + JAVA_STACK_TRACE,
        }})
        failure = self.runner._find_probable_cause_of_failure([1])
        assert_equal(failure['log_file_uri'],
                     BUCKET_URI + TASK_ATTEMPTS_DIR +
                     'attempt_201007271720_0001_r_000126_3/syslog')

    def test_more_attempts_win(self):
        # look at fourth attempt before looking at first attempt
        self.add_mock_s3_data({'walrus': {
            TASK_ATTEMPTS_DIR + 'attempt_201007271720_0001_m_000126_0/stderr':
                TRACEBACK_START + PY_EXCEPTION,
            TASK_ATTEMPTS_DIR + 'attempt_201007271720_0001_m_000004_3/syslog':
                CHILD_ERR_LINE + JAVA_STACK_TRACE,
        }})
        failure = self.runner._find_probable_cause_of_failure([1])
        assert_equal(failure['log_file_uri'],
                     BUCKET_URI + TASK_ATTEMPTS_DIR +
                     'attempt_201007271720_0001_m_000004_3/syslog')

    def test_py_exception_beats_java_stack_trace(self):
        self.add_mock_s3_data({'walrus': {
            ATTEMPT_0_DIR + 'stderr': TRACEBACK_START + PY_EXCEPTION,
            ATTEMPT_0_DIR + 'syslog': CHILD_ERR_LINE + JAVA_STACK_TRACE,
        }})
        failure = self.runner._find_probable_cause_of_failure([1])
        assert_equal(failure['log_file_uri'],
                     BUCKET_URI + ATTEMPT_0_DIR + 'stderr')

    def test_exception_beats_hadoop_error(self):
        self.add_mock_s3_data({'walrus': {
            TASK_ATTEMPTS_DIR + 'attempt_201007271720_0002_m_000126_0/stderr':
                TRACEBACK_START + PY_EXCEPTION,
            LOG_DIR + 'steps/1/syslog':
                HADOOP_ERR_LINE_PREFIX + USEFUL_HADOOP_ERROR + '\n',
        }})
        failure = self.runner._find_probable_cause_of_failure([1, 2])
        assert_equal(failure['log_file_uri'],
                     BUCKET_URI + TASK_ATTEMPTS_DIR +
                     'attempt_201007271720_0002_m_000126_0/stderr')

    def test_step_filtering(self):
        # same as previous test, but step 2 is filtered out
        self.add_mock_s3_data({'walrus': {
            TASK_ATTEMPTS_DIR + 'attempt_201007271720_0002_m_000126_0/stderr':
                TRACEBACK_START + PY_EXCEPTION,
            LOG_DIR + 'steps/1/syslog':
                HADOOP_ERR_LINE_PREFIX + USEFUL_HADOOP_ERROR + '\n',
        }})
        failure = self.runner._find_probable_cause_of_failure([1])
        assert_equal(failure['log_file_uri'],
                     BUCKET_URI + LOG_DIR + 'steps/1/syslog')

    def test_ignore_errors_from_steps_that_later_succeeded(self):
        # This tests the fix for Issue #31
        self.add_mock_s3_data({'walrus': {
            ATTEMPT_0_DIR + 'stderr':
                GARBAGE + TRACEBACK_START + PY_EXCEPTION + GARBAGE,
            ATTEMPT_0_DIR + 'syslog':
                make_input_uri_line(BUCKET_URI + 'input.gz'),
            ATTEMPT_1_DIR + 'stderr': '',
            ATTEMPT_1_DIR + 'syslog':
                make_input_uri_line(BUCKET_URI + 'input.gz'),
        }})
        assert_equal(self.runner._find_probable_cause_of_failure([1]), None)


class LogFetchingFallbackTestCase(MockEMRAndS3TestCase):
    # Make sure that SSH and S3 are accessed when we expect them to be

    @setup
    def make_runner(self):
        self.add_mock_s3_data({'walrus': {}})

        self.runner = EMRJobRunner(s3_sync_wait_time=0,
                                   s3_scratch_uri='s3://walrus/tmp',
                                   conf_path=False)
        self.runner._s3_job_log_uri = BUCKET_URI + LOG_DIR
        self.prepare_runner_for_ssh(self.runner)

    @teardown
    def cleanup_runner(self):
        """This method assumes ``prepare_runner_for_ssh()`` was called. That
        method isn't a "proper" setup method because it requires different
        arguments for different tests.
        """
        self.runner.cleanup()
        self.teardown_ssh()

    def test_ssh_comes_first(self):
        mock_ssh_dir('testmaster', SSH_LOG_ROOT + '/steps/1')
        mock_ssh_dir('testmaster', SSH_LOG_ROOT + '/history')
        mock_ssh_dir('testmaster', SSH_LOG_ROOT + '/userlogs')

        # Put a log file and error into SSH
        ssh_lone_log_path = posixpath.join(
            SSH_LOG_ROOT, 'steps', '1', 'syslog')
        mock_ssh_file('testmaster', ssh_lone_log_path,
                      HADOOP_ERR_LINE_PREFIX + USEFUL_HADOOP_ERROR + '\n')

        # Put a 'more interesting' error in S3 to make sure that the
        # 'less interesting' one from SSH is read and S3 is never
        # looked at. This would never happen in reality because the
        # logs should be identical, but it makes for an easy test
        # of SSH overriding S3.
        self.add_mock_s3_data({'walrus': {
            TASK_ATTEMPTS_DIR + 'attempt_201007271720_0002_m_000126_0/stderr':
                TRACEBACK_START + PY_EXCEPTION,
        }})
        failure = self.runner._find_probable_cause_of_failure([1, 2])
        assert_equal(failure['log_file_uri'],
                     SSH_PREFIX + self.runner._address + ssh_lone_log_path)

    def test_ssh_works_with_slaves(self):
        self.add_slave()

        mock_ssh_dir('testmaster', SSH_LOG_ROOT + '/steps/1')
        mock_ssh_dir('testmaster', SSH_LOG_ROOT + '/history')
        mock_ssh_dir(
            'testmaster!testslave0',
            SSH_LOG_ROOT + '/userlogs/attempt_201007271720_0002_m_000126_0')

        # Put a log file and error into SSH
        ssh_log_path = posixpath.join(SSH_LOG_ROOT, 'userlogs',
                                      'attempt_201007271720_0002_m_000126_0',
                                      'stderr')
        ssh_log_path_2 = posixpath.join(SSH_LOG_ROOT, 'userlogs',
                                        'attempt_201007271720_0002_m_000126_0',
                                        'syslog')
        mock_ssh_file('testmaster!testslave0', ssh_log_path,
                      TRACEBACK_START + PY_EXCEPTION)
        mock_ssh_file('testmaster!testslave0', ssh_log_path_2,
                      '')
        failure = self.runner._find_probable_cause_of_failure([1, 2])
        assert_equal(failure['log_file_uri'],
                     SSH_PREFIX + 'testmaster!testslave0' + ssh_log_path)

    def test_ssh_fails_to_s3(self):
        # the runner will try to use SSH and find itself unable to do so,
        # throwing a LogFetchError and triggering S3 fetching.
        self.runner._address = None

        # Put a different error into S3
        self.add_mock_s3_data({'walrus': {
            TASK_ATTEMPTS_DIR + 'attempt_201007271720_0002_m_000126_0/stderr':
                TRACEBACK_START + PY_EXCEPTION,
        }})
        failure = self.runner._find_probable_cause_of_failure([1, 2])
        assert_equal(failure['log_file_uri'],
                     BUCKET_URI + TASK_ATTEMPTS_DIR +
                     'attempt_201007271720_0002_m_000126_0/stderr')


class TestEMRandS3Endpoints(MockEMRAndS3TestCase):

    def test_no_region(self):
        runner = EMRJobRunner(conf_path=False)
        assert_equal(runner.make_emr_conn().endpoint,
                     'elasticmapreduce.amazonaws.com')
        assert_equal(runner.make_s3_conn().endpoint,
                     's3.amazonaws.com')
        assert_equal(runner._aws_region, '')

    def test_none_region(self):
        # blank region should be treated the same as no region
        runner = EMRJobRunner(conf_path=False, aws_region=None)
        assert_equal(runner.make_emr_conn().endpoint,
                     'elasticmapreduce.amazonaws.com')
        assert_equal(runner.make_s3_conn().endpoint,
                     's3.amazonaws.com')
        assert_equal(runner._aws_region, '')

    def test_blank_region(self):
        # blank region should be treated the same as no region
        runner = EMRJobRunner(conf_path=False, aws_region='')
        assert_equal(runner.make_emr_conn().endpoint,
                     'elasticmapreduce.amazonaws.com')
        assert_equal(runner.make_s3_conn().endpoint,
                     's3.amazonaws.com')
        assert_equal(runner._aws_region, '')

    def test_eu(self):
        runner = EMRJobRunner(conf_path=False, aws_region='EU')
        assert_equal(runner.make_emr_conn().endpoint,
                     'eu-west-1.elasticmapreduce.amazonaws.com')
        assert_equal(runner.make_s3_conn().endpoint,
                     's3-eu-west-1.amazonaws.com')

    def test_us_east_1(self):
        runner = EMRJobRunner(conf_path=False, aws_region='us-east-1')
        assert_equal(runner.make_emr_conn().endpoint,
                     'us-east-1.elasticmapreduce.amazonaws.com')
        assert_equal(runner.make_s3_conn().endpoint,
                     's3.amazonaws.com')

    def test_us_west_1(self):
        runner = EMRJobRunner(conf_path=False, aws_region='us-west-1')
        assert_equal(runner.make_emr_conn().endpoint,
                     'us-west-1.elasticmapreduce.amazonaws.com')
        assert_equal(runner.make_s3_conn().endpoint,
                     's3-us-west-1.amazonaws.com')

    def test_ap_southeast_1(self):
        runner = EMRJobRunner(conf_path=False, aws_region='ap-southeast-1')
        assert_equal(runner.make_s3_conn().endpoint,
                     's3-ap-southeast-1.amazonaws.com')
        assert_raises(Exception, runner.make_emr_conn)

    def test_bad_region(self):
        # should fail in the constructor because the constructor connects to S3
        assert_raises(Exception, EMRJobRunner,
                      conf_path=False, aws_region='the-moooooooon-1')

    def test_case_sensitive(self):
        assert_raises(Exception, EMRJobRunner,
                      conf_path=False, aws_region='eu')
        assert_raises(Exception, EMRJobRunner,
                      conf_path=False, aws_region='US-WEST-1')

    def test_explicit_endpoints(self):
        runner = EMRJobRunner(conf_path=False, aws_region='EU',
                              s3_endpoint='s3-proxy', emr_endpoint='emr-proxy')
        assert_equal(runner.make_emr_conn().endpoint, 'emr-proxy')
        assert_equal(runner.make_s3_conn().endpoint, 's3-proxy')


class TestS3Ls(MockEMRAndS3TestCase):

    def test_s3_ls(self):
        self.add_mock_s3_data({'walrus': {'one': '', 'two': '', 'three': ''}})

        runner = EMRJobRunner(s3_scratch_uri='s3://walrus/tmp',
                              conf_path=False)

        assert_equal(set(runner._s3_ls('s3://walrus/')),
                     set(['s3://walrus/one',
                          's3://walrus/two',
                          's3://walrus/three',
                          ]))

        assert_equal(set(runner._s3_ls('s3://walrus/t')),
                     set(['s3://walrus/two',
                          's3://walrus/three',
                          ]))

        assert_equal(set(runner._s3_ls('s3://walrus/t/')),
                     set([]))

        # if we ask for a nonexistent bucket, we should get some sort
        # of exception (in practice, buckets with random names will
        # probably be owned by other people, and we'll get some sort
        # of permissions error)
        assert_raises(Exception, set, runner._s3_ls('s3://lolcat/'))


class TestSSHLs(MockEMRAndS3TestCase):

    @setup
    def make_runner(self):
        self.runner = EMRJobRunner(conf_path=False)
        self.prepare_runner_for_ssh(self.runner)

    @teardown
    def cleanup_runner(self):
        self.teardown_ssh()

    def test_ssh_ls(self):
        self.add_slave()

        mock_ssh_dir('testmaster', 'test')
        mock_ssh_file('testmaster', posixpath.join('test', 'one'), '')
        mock_ssh_file('testmaster', posixpath.join('test', 'two'), '')
        mock_ssh_dir('testmaster!testslave0', 'test')
        mock_ssh_file('testmaster!testslave0',
                      posixpath.join('test', 'three'), '')

        assert_equal(sorted(self.runner.ls('ssh://testmaster/test')),
                     ['ssh://testmaster/test/one',
                      'ssh://testmaster/test/two'])
        assert_equal(list(self.runner.ls('ssh://testmaster!testslave0/test')),
                     ['ssh://testmaster!testslave0/test/three'])

        # ls() is a generator, so the exception won't fire until we list() it
        assert_raises(IOError, list,
                      self.runner.ls('ssh://testmaster/does_not_exist'))


class TestNoBoto(TestCase):

    @setup
    def blank_out_boto(self):
        self._real_boto = mrjob.emr.boto
        mrjob.emr.boto = None

    @teardown
    def restore_boto(self):
        mrjob.emr.boto = self._real_boto

    def test_init(self):
        # merely creating an EMRJobRunner should raise an exception
        # because it'll need to connect to S3 to set s3_scratch_uri
        assert_raises(ImportError, EMRJobRunner, conf_path=False)

    def test_init_with_s3_scratch_uri(self):
        # this also raises an exception because we have to check
        # the bucket location
        assert_raises(ImportError, EMRJobRunner,
                      conf_path=False, s3_scratch_uri='s3://foo/tmp')


class TestMasterBootstrapScript(MockEMRAndS3TestCase):

    @setup
    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    @teardown
    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_master_bootstrap_script_is_valid_python(self):
        # create a fake src tarball
        with open(os.path.join(self.tmp_dir, 'foo.py'), 'w'):
            pass

        yelpy_tar_gz_path = os.path.join(self.tmp_dir, 'yelpy.tar.gz')
        tar_and_gzip(self.tmp_dir, yelpy_tar_gz_path, prefix='yelpy')

        # use all the bootstrap options
        runner = EMRJobRunner(conf_path=False,
                              bootstrap_cmds=['echo "Hi!"', 'true', 'ls'],
                              bootstrap_files=['/tmp/quz'],
                              bootstrap_mrjob=True,
                              bootstrap_python_packages=[yelpy_tar_gz_path],
                              bootstrap_scripts=['speedups.sh', '/tmp/s.sh'])
        script_path = os.path.join(self.tmp_dir, 'b.py')
        runner._create_master_bootstrap_script(dest=script_path)

        assert os.path.exists(script_path)
        py_compile.compile(script_path)

    def test_no_bootstrap_script_if_not_needed(self):
        runner = EMRJobRunner(conf_path=False, bootstrap_mrjob=False)
        script_path = os.path.join(self.tmp_dir, 'b.py')

        runner._create_master_bootstrap_script(dest=script_path)
        assert not os.path.exists(script_path)

        # bootstrap actions don't figure into the master bootstrap script
        runner = EMRJobRunner(conf_path=False,
                              bootstrap_mrjob=False,
                              bootstrap_actions=['foo', 'bar baz'])
        runner._create_master_bootstrap_script(dest=script_path)

        assert not os.path.exists(script_path)

    def test_bootstrap_actions_get_added(self):
        bootstrap_actions = [
            ('s3://elasticmapreduce/bootstrap-actions/configure-hadoop'
             ' -m,mapred.tasktracker.map.tasks.maximum=1'),
            's3://foo/bar#xyzzy',  # use alternate name for script
        ]

        runner = EMRJobRunner(conf_path=False,
                              bootstrap_actions=bootstrap_actions,
                              s3_sync_wait_time=0.01)

        job_flow_id = runner.make_persistent_job_flow()

        emr_conn = runner.make_emr_conn()
        job_flow = emr_conn.describe_jobflow(job_flow_id)
        actions = job_flow.bootstrapactions

        assert_equal(len(actions), 3)

        assert_equal(
            actions[0].path,
            's3://elasticmapreduce/bootstrap-actions/configure-hadoop')
        assert_equal(
            actions[0].args[0].value,
            '-m,mapred.tasktracker.map.tasks.maximum=1')
        assert_equal(actions[0].name, 'configure-hadoop')

        assert_equal(actions[1].path, 's3://foo/bar')
        assert_equal(actions[1].args, [])
        assert_equal(actions[1].name, 'xyzzy')

        # check for master bootstrap script
        assert actions[2].path.startswith('s3://mrjob-')
        assert actions[2].path.endswith('b.py')
        assert_equal(actions[2].args, [])
        assert_equal(actions[2].name, 'master')

        # make sure master bootstrap script is on S3
        assert runner.path_exists(actions[2].path)

    def test_local_bootstrap_action(self):
        # make sure that local bootstrap action scripts get uploaded to S3
        action_path = os.path.join(self.tmp_dir, 'apt-install.sh')
        with open(action_path, 'w') as f:
            f.write('for $pkg in $@; do sudo apt-get install $pkg; done\n')

        bootstrap_actions = [
            action_path + ' python-scipy mysql-server']

        runner = EMRJobRunner(conf_path=False,
                              bootstrap_actions=bootstrap_actions,
                              s3_sync_wait_time=0.01)

        job_flow_id = runner.make_persistent_job_flow()

        emr_conn = runner.make_emr_conn()
        job_flow = emr_conn.describe_jobflow(job_flow_id)
        actions = job_flow.bootstrapactions

        assert_equal(len(actions), 2)

        assert actions[0].path.startswith('s3://mrjob-')
        assert actions[0].path.endswith('/apt-install.sh')
        assert_equal(actions[0].name, 'apt-install.sh')
        assert_equal(actions[0].args[0].value, 'python-scipy')
        assert_equal(actions[0].args[1].value, 'mysql-server')

        # check for master boostrap script
        assert actions[1].path.startswith('s3://mrjob-')
        assert actions[1].path.endswith('b.py')
        assert_equal(actions[1].args, [])
        assert_equal(actions[1].name, 'master')

        # make sure master bootstrap script is on S3
        assert runner.path_exists(actions[1].path)


class EMRNoMapperTest(MockEMRAndS3TestCase):
    @setup
    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    @teardown
    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_no_mapper(self):
        # read from STDIN, a local file, and a remote file
        stdin = StringIO('foo\nbar\n')

        local_input_path = os.path.join(self.tmp_dir, 'input')
        with open(local_input_path, 'w') as local_input_file:
            local_input_file.write('bar\nqux\n')

        remote_input_path = 's3://walrus/data/foo'
        self.add_mock_s3_data({'walrus': {'data/foo': 'foo\n'}})

        # setup fake output
        self.mock_emr_output = {('j-MOCKJOBFLOW0', 1): [
            '1\t"qux"\n2\t"bar"\n', '2\t"foo"\n5\tnull\n']}

        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '-c', self.mrjob_conf_path,
                               '-', local_input_path, remote_input_path])
        mr_job.sandbox(stdin=stdin)

        results = []

        with mr_job.make_runner() as runner:
            runner.run()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

        assert_equal(sorted(results),
                     [(1, 'qux'), (2, 'bar'), (2, 'foo'), (5, None)])


class TestCat(MockEMRAndS3TestCase):
    @setup
    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    @teardown
    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_cat_uncompressed(self):
        local_input_path = os.path.join(self.tmp_dir, 'input')
        with open(local_input_path, 'w') as input_file:
            input_file.write('bar\nfoo\n')

        remote_input_path = 's3://walrus/data/foo'
        self.add_mock_s3_data({'walrus': {'data/foo': 'foo\nfoo\n'}})

        with EMRJobRunner(cleanup='NONE', conf_path=False) as runner:
            local_output = []
            for line in runner.cat(local_input_path):
                local_output.append(line)

            remote_output = []
            for line in runner.cat(remote_input_path):
                remote_output.append(line)

        assert_equal(local_output, ['bar\n', 'foo\n'])
        assert_equal(remote_output, ['foo\n', 'foo\n'])

    def test_cat_compressed(self):
        input_gz_path = os.path.join(self.tmp_dir, 'input.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'w')
        input_gz.write('foo\nbar\n')
        input_gz.close()

        with EMRJobRunner(cleanup=['NONE'], conf_path=False) as runner:
            output = []
            for line in runner.cat(input_gz_path):
                output.append(line)

        assert_equal(output, ['foo\n', 'bar\n'])

        input_bz2_path = os.path.join(self.tmp_dir, 'input.bz2')
        input_bz2 = bz2.BZ2File(input_bz2_path, 'w')
        input_bz2.write('bar\nbar\nfoo\n')
        input_bz2.close()

        with EMRJobRunner(cleanup=['NONE'], conf_path=False) as runner:
            output = []
            for line in runner.cat(input_bz2_path):
                output.append(line)

        assert_equal(output, ['bar\n', 'bar\n', 'foo\n'])


class PoolingTestCase(MockEMRAndS3TestCase):

    @setup
    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    @teardown
    def rm_tmp_dir(self):
        try:
            shutil.rmtree(self.tmp_dir)
            self.teardown_ssh()
        except (OSError, AttributeError):
            pass  # didn't set up SSH

    def sorted_results_for_runner_with_args(self, job_args):
        mr_job = MRTwoStepJob(job_args)
        mr_job.sandbox()

        results = []
        with mr_job.make_runner() as runner:
            self.prepare_runner_for_ssh(runner)
            runner.run()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

        return sorted(results)

    def make_pooled_job_flow(self, name=None, minutes_ago=0, **kwargs):
        """Returns (runner, job_flow_id). Set minutes_ago to set
        jobflow.startdatetime to seconds before datetime.datetime.now()."""
        runner = EMRJobRunner(conf_path=self.mrjob_conf_path,
                              pool_emr_job_flows=True,
                              emr_job_flow_pool_name=name,
                              **kwargs)
        job_flow_id = runner.make_persistent_job_flow()
        jf = runner.make_emr_conn().describe_jobflow(job_flow_id)
        jf.state = 'WAITING'
        start = (datetime.datetime.now() -
                 datetime.timedelta(minutes=minutes_ago))
        jf.startdatetime = start.strftime(boto.utils.ISO8601)
        return runner, job_flow_id

    def make_simple_runner(self, pool_name):
        """Make an EMRJobRunner that is ready to try to find a pool to join"""
        mr_job = MRTwoStepJob([
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--pool-name', pool_name,
            '-c', self.mrjob_conf_path])
        mr_job.sandbox()
        runner = mr_job.make_runner()
        self.prepare_runner_for_ssh(runner)
        runner._prepare_for_launch()
        return runner

    def test_make_new_pooled_job_flow(self):
        mr_job = MRTwoStepJob(['-r', 'emr', '-v', '--pool-emr-job-flows',
                               '-c', self.mrjob_conf_path])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            self.prepare_runner_for_ssh(runner)
            runner.run()

            # Make sure that the runner made a pooling-enabled job flow
            emr_conn = runner.make_emr_conn()
            job_flow_id = runner.get_emr_job_flow_id()
            job_flow = emr_conn.describe_jobflow(job_flow_id)
            bootstrap_action = job_flow.bootstrapactions[0]
            runner_jobflow_args = [a.value for a in bootstrap_action.args]
            assert runner._pool_arg() in runner_jobflow_args
            assert_equal(job_flow.state, 'WAITING')

    def test_join_pooled_job_flow(self):
        _, job_flow_id = self.make_pooled_job_flow()

        self.mock_emr_output = {(job_flow_id, 1): [
            '1\t"bar"\n1\t"foo"\n2\tnull\n']}

        results = self.sorted_results_for_runner_with_args([
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '-c', self.mrjob_conf_path])
        assert_equal(results,
            [(1, 'bar'), (1, 'foo'), (2, None)])

    def test_join_named_pool(self):
        _, job_flow_id = self.make_pooled_job_flow('pool1')

        self.mock_emr_output = {(job_flow_id, 1): [
            '1\t"bar"\n1\t"foo"\n2\tnull\n']}

        results = self.sorted_results_for_runner_with_args([
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--pool-name', 'pool1',
            '-c', self.mrjob_conf_path])
        assert_equal(results,
            [(1, 'bar'), (1, 'foo'), (2, None)])

    def test_dont_join_full_job_flow(self):
        dummy_runner, job_flow_id = self.make_pooled_job_flow('pool1')

        # insert first item
        self.mock_emr_output = {(job_flow_id, 1): [
            '1\t"bar"\n1\t"foo"\n2\tnull\n']}

        results = self.sorted_results_for_runner_with_args([
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--pool-name', 'pool1',
            '-c', self.mrjob_conf_path])
        assert_equal(results,
            [(1, 'bar'), (1, 'foo'), (2, None)])

        # copy it
        jf = dummy_runner.make_emr_conn().describe_jobflow(job_flow_id)
        jf.steps = jf.steps * 128

        # make sure next attempt at joining uses new job flow
        self.mock_emr_output = {(job_flow_id, 257): [
            '1\t"bar"\n1\t"foo"\n2\tnull\n']}

        results = self.sorted_results_for_runner_with_args([
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--pool-name', 'pool1',
            '-c', self.mrjob_conf_path])
        assert_not_equal(results,
            [(1, 'bar'), (1, 'foo'), (2, None)])

    def test_dont_join_wrong_named_pool(self):
        _, job_flow_id = self.make_pooled_job_flow('pool1')

        self.mock_emr_output = {(job_flow_id, 1): [
            '1\t"bar"\n1\t"foo"\n2\tnull\n']}

        results = self.sorted_results_for_runner_with_args([
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--pool-name', 'not_pool1',
            '-c', self.mrjob_conf_path])

        assert_not_equal(results,
            [(1, 'bar'), (1, 'foo'), (2, None)])

    def test_dont_join_wrong_mrjob_version(self):
        _, job_flow_id = self.make_pooled_job_flow('pool1')
        old_version = mrjob.__version__
        mrjob.__version__ = 'OVER NINE THOUSAAAAAND'

        self.mock_emr_output = {(job_flow_id, 1): [
            '1\t"bar"\n1\t"foo"\n2\tnull\n']}

        results = self.sorted_results_for_runner_with_args([
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--pool-name', 'not_pool1',
            '-c', self.mrjob_conf_path])

        mrjob.__version__ = old_version

        assert_not_equal(results,
            [(1, 'bar'), (1, 'foo'), (2, None)])

    def test_join_similarly_bootstrapped_pool(self):
        local_input_path = os.path.join(self.tmp_dir, 'input')
        with open(local_input_path, 'w') as input_file:
            input_file.write('bar\nfoo\n')

        _, job_flow_id = self.make_pooled_job_flow(
            bootstrap_files=[local_input_path])

        self.mock_emr_output = {(job_flow_id, 1): [
            '1\t"bar"\n1\t"foo"\n2\tnull\n']}

        results = self.sorted_results_for_runner_with_args([
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--bootstrap-file', local_input_path,
            '-c', self.mrjob_conf_path])

        assert_equal(results,
            [(1, 'bar'), (1, 'foo'), (2, None)])

    def test_dont_join_differently_bootstrapped_pool(self):
        local_input_path = os.path.join(self.tmp_dir, 'input')
        with open(local_input_path, 'w') as input_file:
            input_file.write('bar\nfoo\n')

        _, job_flow_id = self.make_pooled_job_flow()

        self.mock_emr_output = {(job_flow_id, 1): [
            '1\t"bar"\n1\t"foo"\n2\tnull\n']}

        results = self.sorted_results_for_runner_with_args([
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--bootstrap-file', local_input_path,
            '-c', self.mrjob_conf_path])

        assert_not_equal(results,
            [(1, 'bar'), (1, 'foo'), (2, None)])

    def test_dont_join_differently_bootstrapped_pool_2(self):
        local_input_path = os.path.join(self.tmp_dir, 'input')
        with open(local_input_path, 'w') as input_file:
            input_file.write('bar\nfoo\n')

        bootstrap_path = os.path.join(self.tmp_dir, 'go.sh')
        with open(bootstrap_path, 'w') as f:
            f.write('#!/usr/bin/sh\necho "hi mom"\n')

        _, job_flow_id = self.make_pooled_job_flow()

        self.mock_emr_output = {(job_flow_id, 1): [
            '1\t"bar"\n1\t"foo"\n2\tnull\n']}

        results = self.sorted_results_for_runner_with_args([
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--bootstrap-action', bootstrap_path + ' a b c',
            '-c', self.mrjob_conf_path])

        assert_not_equal(results,
            [(1, 'bar'), (1, 'foo'), (2, None)])

    def test_pool_competition(self):
        _, job_flow_id = self.make_pooled_job_flow('robert_downey_jr')

        def runner_plz():
            mr_job = MRTwoStepJob([
                '-r', 'emr', '-v', '--pool-emr-job-flows',
                '--pool-name', 'robert_downey_jr',
                '-c', self.mrjob_conf_path])
            mr_job.sandbox()
            runner = mr_job.make_runner()
            runner._prepare_for_launch()
            return runner

        runner1 = runner_plz()
        runner2 = runner_plz()

        jf1 = runner1.find_job_flow()
        jf2 = runner2.find_job_flow()
        assert_equal(jf1.jobflowid, job_flow_id)
        assert_equal(jf2, None)
        jf1.status = 'COMPLETED'

    def test_sorting_by_time(self):
        _, job_flow_id_1 = self.make_pooled_job_flow('pool1', minutes_ago=20)
        _, job_flow_id_2 = self.make_pooled_job_flow('pool1', minutes_ago=40)

        runner1 = self.make_simple_runner('pool1')
        runner2 = self.make_simple_runner('pool1')

        jf1 = runner1.find_job_flow()
        jf2 = runner2.find_job_flow()
        assert_equal(jf1.jobflowid, job_flow_id_1)
        assert_equal(jf2.jobflowid, job_flow_id_2)
        jf1.status = 'COMPLETED'
        jf2.status = 'COMPLETED'

    def test_sorting_by_cpu_hours(self):
        _, job_flow_id_1 = self.make_pooled_job_flow('pool1',
                                                     minutes_ago=40,
                                                     num_ec2_instances=2)
        _, job_flow_id_2 = self.make_pooled_job_flow('pool1',
                                                     minutes_ago=20,
                                                     num_ec2_instances=1)

        runner1 = self.make_simple_runner('pool1')
        runner2 = self.make_simple_runner('pool1')

        jf1 = runner1.find_job_flow()
        jf2 = runner2.find_job_flow()
        assert_equal(jf1.jobflowid, job_flow_id_1)
        assert_equal(jf2.jobflowid, job_flow_id_2)
        jf1.status = 'COMPLETED'
        jf2.status = 'COMPLETED'

    def test_dont_destroy_own_pooled_job_flow_on_failure(self):
        # Issue 242: job failure shouldn't kill the pooled job flows
        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '-c', self.mrjob_conf_path,
                               '--pool-emr-job-flow'])
        mr_job.sandbox()

        self.mock_emr_failures = {('j-MOCKJOBFLOW0', 0): None}

        with mr_job.make_runner() as runner:
            assert isinstance(runner, EMRJobRunner)

            with logger_disabled('mrjob.emr'):
                assert_raises(Exception, runner.run)

            emr_conn = runner.make_emr_conn()
            job_flow_id = runner.get_emr_job_flow_id()
            for _ in xrange(10):
                emr_conn.simulate_progress(job_flow_id)

            job_flow = emr_conn.describe_jobflow(job_flow_id)
            assert_equal(job_flow.state, 'WAITING')

        # job shouldn't get terminated by cleanup
        emr_conn = runner.make_emr_conn()
        job_flow_id = runner.get_emr_job_flow_id()
        for _ in xrange(10):
            emr_conn.simulate_progress(job_flow_id)

        job_flow = emr_conn.describe_jobflow(job_flow_id)
        assert_equal(job_flow.state, 'WAITING')

    def test_dont_destroy_other_pooled_job_flow_on_failure(self):
        # Issue 242: job failure shouldn't kill the pooled job flows
        _, job_flow_id = self.make_pooled_job_flow()

        self.mock_emr_failures = {(job_flow_id, 0): None}

        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '-c', self.mrjob_conf_path,
                               '--pool-emr-job-flow'])
        mr_job.sandbox()

        self.mock_emr_failures = {('j-MOCKJOBFLOW0', 0): None}

        with mr_job.make_runner() as runner:
            assert isinstance(runner, EMRJobRunner)

            with logger_disabled('mrjob.emr'):
                assert_raises(Exception, runner.run)

            assert_equal(runner.get_emr_job_flow_id(), job_flow_id)

            emr_conn = runner.make_emr_conn()
            for _ in xrange(10):
                emr_conn.simulate_progress(job_flow_id)

            job_flow = emr_conn.describe_jobflow(job_flow_id)
            assert_equal(job_flow.state, 'WAITING')

        # job shouldn't get terminated by cleanup
        emr_conn = runner.make_emr_conn()
        job_flow_id = runner.get_emr_job_flow_id()
        for _ in xrange(10):
            emr_conn.simulate_progress(job_flow_id)

        job_flow = emr_conn.describe_jobflow(job_flow_id)
        assert_equal(job_flow.state, 'WAITING')


class S3LockTestCase(MockEMRAndS3TestCase):

    @setup
    def make_buckets(self):
        self.add_mock_s3_data({'locks': {}})
        self.lock_uri = 's3://locks/some_lock'

    def test_lock(self):
        # Most basic test case
        runner = EMRJobRunner(conf_path=False)
        s3_conn = runner.make_s3_conn()

        assert_equal(
            True, attempt_to_acquire_lock(s3_conn, self.lock_uri, 0, 'jf1'))

        assert_equal(
            False, attempt_to_acquire_lock(s3_conn, self.lock_uri, 0, 'jf2'))

    def test_key_race_condition(self):
        # Test case where one attempt puts the key in existence
        runner = EMRJobRunner(conf_path=False)
        s3_conn = runner.make_s3_conn()

        key = _lock_acquire_step_1(s3_conn, self.lock_uri, 'jf1')
        assert_not_equal(key, None)

        key2 = _lock_acquire_step_1(s3_conn, self.lock_uri, 'jf2')
        assert_equal(key2, None)

    def test_read_race_condition(self):
        # test case where both try to create the key
        runner = EMRJobRunner(conf_path=False)
        s3_conn = runner.make_s3_conn()

        key = _lock_acquire_step_1(s3_conn, self.lock_uri, 'jf1')
        assert_not_equal(key, None)

        # acquire the key by subversive means to simulate contention
        bucket_name, key_prefix = parse_s3_uri(self.lock_uri)
        bucket = s3_conn.get_bucket(bucket_name)
        key2 = bucket.get_key(key_prefix)

        # and take the lock!
        key2.set_contents_from_string('jf2')

        assert not _lock_acquire_step_2(key, 'jf1'), 'Lock should fail'


class TestCatFallback(MockEMRAndS3TestCase):

    def test_s3_cat(self):
        self.add_mock_s3_data(
            {'walrus': {'one': 'one_text',
                        'two': 'two_text',
                        'three': 'three_text'}})

        runner = EMRJobRunner(s3_scratch_uri='s3://walrus/tmp',
                              conf_path=False)

        assert_equal(list(runner.cat('s3://walrus/one')), ['one_text\n'])

    def test_ssh_cat(self):
        runner = EMRJobRunner(conf_path=False)
        self.prepare_runner_for_ssh(runner)
        p = mock_ssh_file('testmaster', 'etc/init.d', 'meow')
        assert_equal(
            list(runner.cat(SSH_PREFIX + runner._address + '/etc/init.d')),
            ['meow\n'])
        assert_raises(
            IOError, list,
            runner.cat(SSH_PREFIX + runner._address + '/does_not_exist'))
