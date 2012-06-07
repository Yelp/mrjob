# Copyright 2009-2012 Yelp and Contributors
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

import copy
from datetime import datetime
from datetime import timedelta
import getpass
import logging
import os
import posixpath
import py_compile
import shutil
from StringIO import StringIO
import tempfile
import time

from mock import patch

try:
    import unittest2 as unittest
    unittest  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import unittest

import mrjob
from mrjob.conf import dump_mrjob_conf
import mrjob.emr
from mrjob.emr import EMRJobRunner
from mrjob.emr import attempt_to_acquire_lock
from mrjob.emr import describe_all_job_flows
from mrjob.emr import _lock_acquire_step_1
from mrjob.emr import _lock_acquire_step_2
from mrjob.parse import JOB_NAME_RE
from mrjob.parse import parse_s3_uri
from mrjob.pool import pool_hash_and_name
from mrjob.ssh import SSH_LOG_ROOT
from mrjob.ssh import SSH_PREFIX
from mrjob.util import log_to_stream
from mrjob.util import tar_and_gzip

from tests.mockboto import DEFAULT_MAX_JOB_FLOWS_RETURNED
from tests.mockboto import MockEmrConnection
from tests.mockboto import MockEmrObject
from tests.mockboto import MockS3Connection
from tests.mockboto import add_mock_s3_data
from tests.mockboto import to_iso8601
from tests.mockssh import create_mock_ssh_script
from tests.mockssh import mock_ssh_dir
from tests.mockssh import mock_ssh_file
from tests.mr_hadoop_format_job import MRHadoopFormatJob
from tests.mr_two_step_job import MRTwoStepJob
from tests.mr_word_count import MRWordCount
from tests.quiet import log_to_buffer
from tests.quiet import logger_disabled
from tests.quiet import no_handlers_for_logger

try:
    import boto
    import boto.emr
    import boto.exception
    from mrjob import boto_2_1_1_83aae37b
    boto  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    boto = None


class MockEMRAndS3TestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.fake_mrjob_tgz_path = tempfile.mkstemp(
            prefix='fake_mrjob_', suffix='.tar.gz')[1]

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.fake_mrjob_tgz_path):
            os.remove(cls.fake_mrjob_tgz_path)

    def setUp(self):
        self.make_mrjob_conf()
        self.sandbox_boto()

        def simple_patch(obj, attr, side_effect=None, autospec=False):
            patcher = patch.object(obj, attr, side_effect=side_effect,
                                   autospec=autospec)
            patcher.start()
            self.addCleanup(patcher.stop)

        def fake_create_mrjob_tar_gz(mocked_self, *args, **kwargs):
            mocked_self._mrjob_tar_gz_path = self.fake_mrjob_tgz_path
            return self.fake_mrjob_tgz_path

        simple_patch(EMRJobRunner, '_create_mrjob_tar_gz',
                     fake_create_mrjob_tar_gz, autospec=True)

        simple_patch(EMRJobRunner, '_wait_for_s3_eventual_consistency')
        simple_patch(EMRJobRunner, '_wait_for_job_flow_termination')
        simple_patch(time, 'sleep')

    def tearDown(self):
        self.unsandbox_boto()
        self.rm_mrjob_conf()

    def mrjob_conf_contents(self):
        return {'runners': {'emr': {
                'check_emr_status_every': 0.00,
                's3_sync_wait_time': 0.00,
                'bootstrap_mrjob': False,
            }}}

    def make_mrjob_conf(self):
        _, self.mrjob_conf_path = tempfile.mkstemp(prefix='mrjob.conf.')
        with open(self.mrjob_conf_path, 'w') as f:
            dump_mrjob_conf(self.mrjob_conf_contents(), f)

    def rm_mrjob_conf(self):
        os.unlink(self.mrjob_conf_path)

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

        self._real_boto_2_1_1_83aae37b_EmrConnection = (
            boto_2_1_1_83aae37b.EmrConnection)
        boto_2_1_1_83aae37b.EmrConnection = mock_boto_emr_EmrConnection

        # copy the old environment just to be polite
        self._old_environ = os.environ.copy()

    def unsandbox_boto(self):
        boto.connect_s3 = self._real_boto_connect_s3
        boto_2_1_1_83aae37b.EmrConnection = (
            self._real_boto_2_1_1_83aae37b_EmrConnection)

    def add_mock_s3_data(self, data, time_modified=None):
        """Update self.mock_s3_fs with a map from bucket name
        to key name to data."""
        add_mock_s3_data(self.mock_s3_fs, data, time_modified)

    def prepare_runner_for_ssh(self, runner, num_slaves=0):
        # TODO: Refactor this abomination of a test harness

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

        # re-initialize fs
        runner._fs = None
        runner._ssh_fs = None
        runner._s3_fs = None
        #runner.fs

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

    def setUp(self):
        super(EMRJobRunnerEndToEndTestCase, self).setUp()
        self.make_tmp_dir()
        self.put_additional_emr_info_in_mrjob_conf()

    def tearDown(self):
        self.rm_tmp_dir()
        super(EMRJobRunnerEndToEndTestCase, self).tearDown()

    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def put_additional_emr_info_in_mrjob_conf(self):
        with open(self.mrjob_conf_path, 'w') as f:
            dump_mrjob_conf({'runners': {'emr': {
                'check_emr_status_every': 0.00,
                's3_sync_wait_time': 0.00,
                'additional_emr_info': {'key': 'value'},
            }}}, f)

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
            self.assertEqual(mock_s3_fs_snapshot, self.mock_s3_fs)

            # make sure AdditionalInfo was JSON-ified from the config file.
            # checked now because you can't actually read it from the job flow
            # on real EMR.
            self.assertEqual(runner._opts['additional_emr_info'],
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
            self.assertEqual(job_flow.state, 'COMPLETED')
            name_match = JOB_NAME_RE.match(job_flow.name)
            self.assertEqual(name_match.group(1), 'mr_hadoop_format_job')
            self.assertEqual(name_match.group(2), getpass.getuser())

            # make sure our input and output formats are attached to
            # the correct steps
            self.assertIn('-inputformat', job_flow.steps[0].args())
            self.assertNotIn('-outputformat', job_flow.steps[0].args())
            self.assertNotIn('-inputformat', job_flow.steps[1].args())
            self.assertIn('-outputformat', job_flow.steps[1].args())

            # make sure mrjob.tar.gz is created and uploaded as
            # a bootstrap file
            assert runner._mrjob_tar_gz_path
            mrjob_tar_gz_file_dicts = [
                file_dict for file_dict in runner._files
                if file_dict['path'] == runner._mrjob_tar_gz_path]

            self.assertEqual(len(mrjob_tar_gz_file_dicts), 1)

            mrjob_tar_gz_file_dict = mrjob_tar_gz_file_dicts[0]
            assert mrjob_tar_gz_file_dict['name']
            self.assertEqual(mrjob_tar_gz_file_dict.get('bootstrap'), 'file')

            # shouldn't be in PYTHONPATH (we dump it directly in site-packages)
            pythonpath = runner._get_cmdenv().get('PYTHONPATH') or ''
            self.assertNotIn(mrjob_tar_gz_file_dict['name'],
                             pythonpath.split(':'))

        self.assertEqual(sorted(results),
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
        self.assertEqual(job_flow.state, 'TERMINATED')

    def test_failed_job(self):
        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                               '-c', self.mrjob_conf_path])
        mr_job.sandbox()

        self.add_mock_s3_data({'walrus': {}})
        self.mock_emr_failures = {('j-MOCKJOBFLOW0', 0): None}

        with mr_job.make_runner() as runner:
            assert isinstance(runner, EMRJobRunner)

            with logger_disabled('mrjob.emr'):
                self.assertRaises(Exception, runner.run)

            emr_conn = runner.make_emr_conn()
            job_flow_id = runner.get_emr_job_flow_id()
            for _ in xrange(10):
                emr_conn.simulate_progress(job_flow_id)

            job_flow = emr_conn.describe_jobflow(job_flow_id)
            self.assertEqual(job_flow.state, 'FAILED')

        # job should get terminated on cleanup
        emr_conn = runner.make_emr_conn()
        job_flow_id = runner.get_emr_job_flow_id()
        for _ in xrange(10):
            emr_conn.simulate_progress(job_flow_id)

        job_flow = emr_conn.describe_jobflow(job_flow_id)
        self.assertEqual(job_flow.state, 'TERMINATED')

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
        self.assertEqual(len(list(bucket.list())), scratch_len)

        bucket = conn.get_bucket(log_bucket)
        self.assertEqual(len(list(bucket.list())), log_len)

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
        self.assertRaises(ValueError, self._test_remote_scratch_cleanup,
                          'NONE,LOGS,REMOTE_SCRATCH', 0, 0)
        self.assertRaises(ValueError, self._test_remote_scratch_cleanup,
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
            self.assertNotIn('-files',
                             runner._describe_jobflow().steps[0].args())
            self.assertIn('-cacheFile',
                          runner._describe_jobflow().steps[0].args())
            self.assertNotIn('-combiner',
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
            self.assertIn('-files', runner._describe_jobflow().steps[0].args())
            self.assertNotIn('-cacheFile',
                             runner._describe_jobflow().steps[0].args())
            self.assertIn('-combiner',
                          runner._describe_jobflow().steps[0].args())

    def test_wait_for_job_flow_termination(self):
        # Test regression from #338 where _wait_for_job_flow_termination
        # would raise an IndexError whenever the job flow wasn't already
        # finished
        mr_job = MRTwoStepJob(['-r', 'emr',
                               '-c', self.mrjob_conf_path,
                               '--check-emr-status-every=0'])
        mr_job.sandbox()
        with mr_job.make_runner() as runner:
            runner._launch_emr_job()
            jf = runner._describe_jobflow()
            jf.keepjobflowalivewhennosteps = 'false'
            runner._wait_for_job_flow_termination()


class S3ScratchURITestCase(MockEMRAndS3TestCase):

    def test_pick_scratch_uri(self):
        self.add_mock_s3_data({'mrjob-walrus': {}, 'zebra': {}})
        runner = EMRJobRunner(conf_path=False)

        self.assertEqual(runner._opts['s3_scratch_uri'],
                         's3://mrjob-walrus/tmp/')

    def test_create_scratch_uri(self):
        # "walrus" bucket will be ignored; it doesn't start with "mrjob-"
        self.add_mock_s3_data({'walrus': {}, 'zebra': {}})

        runner = EMRJobRunner(conf_path=False, s3_sync_wait_time=0.00)

        # bucket name should be mrjob- plus 16 random hex digits
        s3_scratch_uri = runner._opts['s3_scratch_uri']
        self.assertEqual(s3_scratch_uri[:11], 's3://mrjob-')
        self.assertEqual(s3_scratch_uri[27:], '/tmp/')

        # bucket shouldn't actually exist yet
        scratch_bucket, _ = parse_s3_uri(s3_scratch_uri)
        self.assertNotIn(scratch_bucket, self.mock_s3_fs.keys())

        # need to do something to ensure that the bucket actually gets
        # created. let's launch a (mock) job flow
        job_flow_id = runner.make_persistent_job_flow()
        self.assertIn(scratch_bucket, self.mock_s3_fs.keys())
        runner.make_emr_conn().terminate_jobflow(job_flow_id)

        # once our scratch bucket is created, we should re-use it
        runner2 = EMRJobRunner(conf_path=False)
        s3_scratch_uri = runner._opts['s3_scratch_uri']
        self.assertEqual(runner2._opts['s3_scratch_uri'], s3_scratch_uri)


class BootstrapFilesTestCase(MockEMRAndS3TestCase):

    def test_bootstrap_files_only_get_uploaded_once(self):
        # just a regression test for Issue #8

        # use self.mrjob_conf_path because it's easier than making a new file
        bootstrap_file = self.mrjob_conf_path

        runner = EMRJobRunner(conf_path=False,
                              bootstrap_files=[bootstrap_file])

        matching_file_dicts = [fd for fd in runner._files
                               if fd['path'] == bootstrap_file]
        self.assertEqual(len(matching_file_dicts), 1)


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
            self.assertEqual(runner._master_bootstrap_script, None)

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

        self.assertEqual(sorted(results),
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
                self.assertRaises(Exception, runner.run)

            emr_conn = runner.make_emr_conn()
            job_flow_id = runner.get_emr_job_flow_id()
            for _ in xrange(10):
                emr_conn.simulate_progress(job_flow_id)

            job_flow = emr_conn.describe_jobflow(job_flow_id)
            self.assertEqual(job_flow.state, 'WAITING')

        # job shouldn't get terminated by cleanup
        emr_conn = runner.make_emr_conn()
        job_flow_id = runner.get_emr_job_flow_id()
        for _ in xrange(10):
            emr_conn.simulate_progress(job_flow_id)

        job_flow = emr_conn.describe_jobflow(job_flow_id)
        self.assertEqual(job_flow.state, 'WAITING')


class AMIAndHadoopVersionTestCase(MockEMRAndS3TestCase):

    def run_and_get_job_flow(self, *args):
        stdin = StringIO('foo\nbar\n')
        mr_job = MRTwoStepJob(
            ['-r', 'emr', '-v', '-c', self.mrjob_conf_path] + list(args))
        mr_job.sandbox(stdin=stdin)

        with mr_job.make_runner() as runner:
            runner.run()

            emr_conn = runner.make_emr_conn()
            return emr_conn.describe_jobflow(runner.get_emr_job_flow_id())

    def test_defaults(self):
        job_flow = self.run_and_get_job_flow()
        self.assertFalse(hasattr(job_flow, 'amiversion'))
        self.assertEqual(job_flow.hadoopversion, '0.20')

    def test_hadoop_version_0_18(self):
        job_flow = self.run_and_get_job_flow('--hadoop-version', '0.18')
        self.assertFalse(hasattr(job_flow, 'amiversion'))
        self.assertEqual(job_flow.hadoopversion, '0.18')

    def test_hadoop_version_0_20(self):
        job_flow = self.run_and_get_job_flow('--hadoop-version', '0.20')
        self.assertFalse(hasattr(job_flow, 'amiversion'))
        self.assertEqual(job_flow.hadoopversion, '0.20')

    def test_bad_hadoop_version(self):
        self.assertRaises(boto.exception.EmrResponseError,
                          self.run_and_get_job_flow,
                          '--hadoop-version', '0.99')

    def test_ami_version_1_0(self):
        job_flow = self.run_and_get_job_flow('--ami-version', '1.0')
        self.assertEqual(job_flow.amiversion, '1.0')
        self.assertEqual(job_flow.hadoopversion, '0.18')

    def test_ami_version_2_0(self):
        job_flow = self.run_and_get_job_flow('--ami-version', '2.0')
        self.assertEqual(job_flow.amiversion, '2.0')
        self.assertEqual(job_flow.hadoopversion, '0.20.205')

    def test_latest_ami_version(self):
        job_flow = self.run_and_get_job_flow('--ami-version', 'latest')
        self.assertEqual(job_flow.amiversion, 'latest')
        self.assertEqual(job_flow.hadoopversion, '0.20.205')

    def test_bad_ami_version(self):
        self.assertRaises(boto.exception.EmrResponseError,
                          self.run_and_get_job_flow,
                          '--ami-version', '1.5')

    def test_ami_version_1_0_hadoop_version_0_18(self):
        job_flow = self.run_and_get_job_flow('--ami-version', '1.0',
                                             '--hadoop-version', '0.18')
        self.assertEqual(job_flow.amiversion, '1.0')
        self.assertEqual(job_flow.hadoopversion, '0.18')

    def test_ami_version_1_0_hadoop_version_0_20(self):
        job_flow = self.run_and_get_job_flow('--ami-version', '1.0',
                                             '--hadoop-version', '0.20')
        self.assertEqual(job_flow.amiversion, '1.0')
        self.assertEqual(job_flow.hadoopversion, '0.20')

    def test_mismatched_ami_and_hadoop_versions(self):
        self.assertRaises(boto.exception.EmrResponseError,
                          self.run_and_get_job_flow,
                          '--ami-version', '1.0',
                          '--hadoop-version', '0.20.205')


class AvailabilityZoneTestCase(MockEMRAndS3TestCase):

    def setUp(self):
        super(AvailabilityZoneTestCase, self).setUp()
        self.put_availability_zone_in_mrjob_conf()

    def put_availability_zone_in_mrjob_conf(self):
        dump_mrjob_conf({'runners': {'emr': {
            'check_emr_status_every': 0.00,
            's3_sync_wait_time': 0.00,
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
            self.assertEqual(job_flow.availabilityzone, 'PUPPYLAND')

    def test_debugging_works(self):
        mr_job = MRTwoStepJob(['-r', 'emr', '-v',
                           '-c', self.mrjob_conf_path,
                           '--enable-emr-debugging'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            runner.run()
            flow = runner.make_emr_conn().describe_jobflow(
                runner._emr_job_flow_id)
            self.assertEqual(flow.steps[0].name, 'Setup Hadoop Debugging')


class BucketRegionTestCase(MockEMRAndS3TestCase):

    def setUp(self):
        super(BucketRegionTestCase, self).setUp()
        self.make_dummy_data()

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
        self.assertNotEqual(j._opts['s3_scratch_uri'], self.bucket1_uri)

    def test_region_nobucket_nomatchexists(self):
        # aws_region specified, no bucket specified, no buckets have matching
        # region
        self.bucket1.set_location('PUPPYLAND')
        j = EMRJobRunner(aws_region='KITTYLAND',
                         s3_endpoint='KITTYLAND',
                         conf_path=False)
        self.assertNotEqual(j._opts['s3_scratch_uri'], self.bucket1_uri)

    def test_noregion_nobucket_nolocation(self):
        # aws_region not specified, no bucket specified, default bucket has no
        # location
        j = EMRJobRunner(conf_path=False)
        self.assertEqual(j._opts['s3_scratch_uri'], self.bucket1_uri)

    def test_noregion_bucket_nolocation(self):
        # aws_region not specified, bucket specified without location
        j = EMRJobRunner(conf_path=False,
                         s3_scratch_uri=self.bucket1_uri)
        self.assertEqual(j._opts['s3_scratch_uri'], self.bucket1_uri)

    def test_noregion_bucket_location(self):
        # aws_region not specified, bucket specified with location
        self.bucket1.set_location('PUPPYLAND')
        j = EMRJobRunner(conf_path=False)
        self.assertEqual(j._aws_region, 'PUPPYLAND')


class ExtraBucketRegionTestCase(MockEMRAndS3TestCase):

    def setUp(self):
        super(ExtraBucketRegionTestCase, self).setUp()
        self.make_dummy_data()

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
        self.assertEqual(j._opts['s3_scratch_uri'], self.bucket2_uri)

    def test_region_bucket_match(self):
        # aws_region specified, bucket specified with matching location
        j = EMRJobRunner(aws_region='PUPPYLAND',
                         s3_endpoint='PUPPYLAND',
                         s3_scratch_uri=self.bucket1_uri,
                         conf_path=False)
        self.assertEqual(j._opts['s3_scratch_uri'], self.bucket1_uri)

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

            self.assertIn('does not match bucket region', stderr.getvalue())


class DescribeAllJobFlowsTestCase(MockEMRAndS3TestCase):

    def test_can_get_more_job_flows_than_limit(self):
        now = datetime.utcnow()

        NUM_JOB_FLOWS = 2222
        self.assertGreater(NUM_JOB_FLOWS, DEFAULT_MAX_JOB_FLOWS_RETURNED)

        for i in range(NUM_JOB_FLOWS):
            job_flow_id = 'j-%04d' % i
            self.mock_emr_job_flows[job_flow_id] = MockEmrObject(
                creationdatetime=to_iso8601(
                    now - timedelta(minutes=i)),
                jobflowid=job_flow_id)

        emr_conn = EMRJobRunner(conf_path=False).make_emr_conn()

        # ordinary describe_jobflows() hits the limit on number of job flows
        some_job_flows = emr_conn.describe_jobflows()
        self.assertEqual(len(some_job_flows), DEFAULT_MAX_JOB_FLOWS_RETURNED)

        all_job_flows = describe_all_job_flows(emr_conn)
        self.assertEqual(len(all_job_flows), NUM_JOB_FLOWS)
        self.assertEqual(sorted(jf.jobflowid for jf in all_job_flows),
                         [('j-%04d' % i) for i in range(NUM_JOB_FLOWS)])

    def test_no_params_hole(self):
        # Issue #346: If we (incorrectly) include no parameters to
        # DescribeJobFlows on our initial call, we'll skip over
        # j-THREEWEEKSAGO, since it's neither currently active, nor
        # in the last 2 weeks.

        now = datetime.utcnow()

        self.mock_emr_job_flows['j-THREEWEEKSAGO'] = MockEmrObject(
            creationdatetime=to_iso8601(now - timedelta(weeks=3)),
            jobflowid='j-THREEWEEKSAGO',
            state='COMPLETED',
        )

        self.mock_emr_job_flows['j-LONGRUNNING'] = MockEmrObject(
            creationdatetime=to_iso8601(now - timedelta(weeks=4)),
            jobflowid='j-LONGRUNNING',
            state='RUNNING',
        )

        emr_conn = EMRJobRunner(conf_path=False).make_emr_conn()

        # ordinary describe_jobflows() misses j-THREEWEEKSAGO
        some_job_flows = emr_conn.describe_jobflows()
        self.assertEqual(sorted(jf.jobflowid for jf in some_job_flows),
                         ['j-LONGRUNNING'])

        # describe_all_job_flows() should work around this
        all_job_flows = describe_all_job_flows(emr_conn)
        self.assertEqual(sorted(jf.jobflowid for jf in all_job_flows),
                         ['j-LONGRUNNING', 'j-THREEWEEKSAGO'])


class EC2InstanceGroupTestCase(MockEMRAndS3TestCase):

    def _test_instance_groups(self, opts, **expected):
        """Run a job with the given option dictionary, and check for
        for instance, number, and optional bid price for each instance role.

        Specify expected instance group info like:

        <role>=(num_instances, instance_type, bid_price)
        """
        runner = EMRJobRunner(conf_path=self.mrjob_conf_path, **opts)

        job_flow_id = runner.make_persistent_job_flow()
        job_flow = runner.make_emr_conn().describe_jobflow(job_flow_id)

        # convert expected to a dict of dicts
        role_to_expected = {}
        for role, (num, instance_type, bid_price) in expected.iteritems():
            info = {
                'instancerequestcount': str(num),
                'instancetype': instance_type,
            }
            if bid_price:
                info['market'] = 'SPOT'
                info['bidprice'] = bid_price
            else:
                info['market'] = 'ON_DEMAND'

            role_to_expected[role.upper()] = info

        # convert actual instance groups to dicts
        role_to_actual = {}
        for ig in job_flow.instancegroups:
            info = {}
            for field in ('bidprice', 'instancerequestcount',
                          'instancetype', 'market'):
                if hasattr(ig, field):
                    info[field] = getattr(ig, field)
            role_to_actual[ig.instancerole] = info

        self.assertEqual(role_to_expected, role_to_actual)

        # also check master/slave and # of instance types
        # this is mostly a sanity check of mockboto
        expected_master_instance_type = role_to_expected.get(
            'MASTER', {}).get('instancetype')
        self.assertEqual(expected_master_instance_type,
                         getattr(job_flow, 'masterinstancetype', None))

        expected_slave_instance_type = role_to_expected.get(
            'CORE', {}).get('instancetype')
        self.assertEqual(expected_slave_instance_type,
                         getattr(job_flow, 'slaveinstancetype', None))

        expected_instance_count = str(sum(
            int(info['instancerequestcount'])
            for info in role_to_expected.itervalues()))
        self.assertEqual(expected_instance_count, job_flow.instancecount)

    def set_in_mrjob_conf(self, **kwargs):
        emr_opts = {'check_emr_status_every': 0.00,
                    's3_sync_wait_time': 0.00}
        emr_opts.update(kwargs)
        with open(self.mrjob_conf_path, 'w') as f:
            dump_mrjob_conf({'runners': {'emr': emr_opts}}, f)

    def test_defaults(self):
        self._test_instance_groups(
            {},
            master=(1, 'm1.small', None))

        self._test_instance_groups(
            {'num_ec2_instances': 3},
            core=(2, 'm1.small', None),
            master=(1, 'm1.small', None))

    def test_single_instance(self):
        self._test_instance_groups(
            {'ec2_instance_type': 'c1.xlarge'},
            master=(1, 'c1.xlarge', None))

    def test_multiple_instances(self):
        self._test_instance_groups(
            {'ec2_instance_type': 'c1.xlarge', 'num_ec2_instances': 3},
            core=(2, 'c1.xlarge', None),
            master=(1, 'm1.small', None))

    def test_explicit_master_and_slave_instance_types(self):
        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large'},
            master=(1, 'm1.large', None))

        self._test_instance_groups(
            {'ec2_slave_instance_type': 'm2.xlarge',
             'num_ec2_instances': 3},
            core=(2, 'm2.xlarge', None),
            master=(1, 'm1.small', None))

        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_slave_instance_type': 'm2.xlarge',
             'num_ec2_instances': 3},
            core=(2, 'm2.xlarge', None),
            master=(1, 'm1.large', None))

    def test_explicit_instance_types_take_precedence(self):
        self._test_instance_groups(
            {'ec2_instance_type': 'c1.xlarge',
             'ec2_master_instance_type': 'm1.large'},
            master=(1, 'm1.large', None))

        self._test_instance_groups(
            {'ec2_instance_type': 'c1.xlarge',
             'ec2_master_instance_type': 'm1.large',
             'ec2_slave_instance_type': 'm2.xlarge',
             'num_ec2_instances': 3},
            core=(2, 'm2.xlarge', None),
            master=(1, 'm1.large', None))

    def test_cmd_line_opts_beat_mrjob_conf(self):
        # set ec2_instance_type in mrjob.conf, 1 instance
        self.set_in_mrjob_conf(ec2_instance_type='c1.xlarge')

        self._test_instance_groups(
            {},
            master=(1, 'c1.xlarge', None))

        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large'},
            master=(1, 'm1.large', None))

        # set ec2_instance_type in mrjob.conf, 3 instances
        self.set_in_mrjob_conf(ec2_instance_type='c1.xlarge',
                               num_ec2_instances=3)

        self._test_instance_groups(
            {},
            core=(2, 'c1.xlarge', None),
            master=(1, 'm1.small', None))

        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_slave_instance_type': 'm2.xlarge'},
            core=(2, 'm2.xlarge', None),
            master=(1, 'm1.large', None))

        # set master in mrjob.conf, 1 instance
        self.set_in_mrjob_conf(ec2_master_instance_type='m1.large')

        self._test_instance_groups(
            {},
            master=(1, 'm1.large', None))

        self._test_instance_groups(
            {'ec2_instance_type': 'c1.xlarge'},
            master=(1, 'c1.xlarge', None))

        # set master and slave in mrjob.conf, 2 instances
        self.set_in_mrjob_conf(ec2_master_instance_type='m1.large',
                               ec2_slave_instance_type='m2.xlarge',
                               num_ec2_instances=3)

        self._test_instance_groups(
            {},
            core=(2, 'm2.xlarge', None),
            master=(1, 'm1.large', None))

        self._test_instance_groups(
            {'ec2_instance_type': 'c1.xlarge'},
            core=(2, 'c1.xlarge', None),
            master=(1, 'm1.large', None))

    def test_zero_core_instances(self):
        self._test_instance_groups(
            {'ec2_master_instance_type': 'c1.medium',
             'num_ec2_core_instances': 0},
            master=(1, 'c1.medium', None))

    def test_core_spot_instances(self):
        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_core_instance_type': 'c1.medium',
             'ec2_core_instance_bid_price': '0.20',
             'num_ec2_core_instances': 5},
            core=(5, 'c1.medium', '0.20'),
            master=(1, 'm1.large', None))

    def test_core_on_demand_instances(self):
        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_core_instance_type': 'c1.medium',
             'num_ec2_core_instances': 5},
            core=(5, 'c1.medium', None),
            master=(1, 'm1.large', None))

        # Test the ec2_slave_instance_type alias
        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_slave_instance_type': 'c1.medium',
             'num_ec2_instances': 6},
            core=(5, 'c1.medium', None),
            master=(1, 'm1.large', None))

    def test_core_and_task_on_demand_instances(self):
        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_core_instance_type': 'c1.medium',
             'num_ec2_core_instances': 5,
             'ec2_task_instance_type': 'm2.xlarge',
             'num_ec2_task_instances': 20,
             },
            core=(5, 'c1.medium', None),
            master=(1, 'm1.large', None),
            task=(20, 'm2.xlarge', None))

    def test_core_and_task_spot_instances(self):
        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_core_instance_type': 'c1.medium',
             'ec2_core_instance_bid_price': '0.20',
             'num_ec2_core_instances': 10,
             'ec2_task_instance_type': 'm2.xlarge',
             'ec2_task_instance_bid_price': '1.00',
             'num_ec2_task_instances': 20,
             },
            core=(10, 'c1.medium', '0.20'),
            master=(1, 'm1.large', None),
            task=(20, 'm2.xlarge', '1.00'))

        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_core_instance_type': 'c1.medium',
             'num_ec2_core_instances': 10,
             'ec2_task_instance_type': 'm2.xlarge',
             'ec2_task_instance_bid_price': '1.00',
             'num_ec2_task_instances': 20,
             },
            core=(10, 'c1.medium', None),
            master=(1, 'm1.large', None),
            task=(20, 'm2.xlarge', '1.00'))

    def test_master_and_core_spot_instances(self):
        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_master_instance_bid_price': '0.50',
             'ec2_core_instance_type': 'c1.medium',
             'ec2_core_instance_bid_price': '0.20',
             'num_ec2_core_instances': 10,
             },
            core=(10, 'c1.medium', '0.20'),
            master=(1, 'm1.large', '0.50'))

    def test_master_spot_instance(self):
        self._test_instance_groups(
            {'ec2_master_instance_type': 'm1.large',
             'ec2_master_instance_bid_price': '0.50',
             },
            master=(1, 'm1.large', '0.50'))

    def test_zero_or_blank_bid_price_means_on_demand(self):
        self._test_instance_groups(
            {'ec2_master_instance_bid_price': '0',
             },
            master=(1, 'm1.small', None))

        self._test_instance_groups(
            {'num_ec2_core_instances': 3,
             'ec2_core_instance_bid_price': '0.00',
             },
            core=(3, 'm1.small', None),
            master=(1, 'm1.small', None))

        self._test_instance_groups(
            {'num_ec2_core_instances': 3,
             'num_ec2_task_instances': 5,
             'ec2_task_instance_bid_price': '',
             },
            core=(3, 'm1.small', None),
            master=(1, 'm1.small', None),
            task=(5, 'm1.small', None))

    def test_pass_invalid_bid_prices_through_to_emr(self):
        self.assertRaises(
            boto.exception.EmrResponseError,
            self._test_instance_groups,
            {'ec2_master_instance_bid_price': 'all the gold in California'})

    def test_task_type_defaults_to_core_type(self):
        self._test_instance_groups(
            {'ec2_core_instance_type': 'c1.medium',
             'num_ec2_core_instances': 5,
             'num_ec2_task_instances': 20,
             },
            core=(5, 'c1.medium', None),
            master=(1, 'm1.small', None),
            task=(20, 'c1.medium', None))

    def test_mixing_instance_number_opts_on_cmd_line(self):
        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.emr', stderr)
            self._test_instance_groups(
                {'num_ec2_instances': 4,
                 'num_ec2_core_instances': 10},
                core=(10, 'm1.small', None),
                master=(1, 'm1.small', None))

        self.assertIn('does not make sense', stderr.getvalue())

    def test_mixing_instance_number_opts_in_mrjob_conf(self):
        self.set_in_mrjob_conf(num_ec2_instances=3,
                               num_ec2_core_instances=5,
                               num_ec2_task_instances=9)

        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.emr', stderr)
            self._test_instance_groups(
                {},
                core=(5, 'm1.small', None),
                master=(1, 'm1.small', None),
                task=(9, 'm1.small', None))

        self.assertIn('does not make sense', stderr.getvalue())

    def test_cmd_line_instance_numbers_beat_mrjob_conf(self):
        self.set_in_mrjob_conf(num_ec2_core_instances=5,
                               num_ec2_task_instances=9)

        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.emr', stderr)
            self._test_instance_groups(
                {'num_ec2_instances': 3},
                core=(2, 'm1.small', None),
                master=(1, 'm1.small', None))

        self.assertNotIn('does not make sense', stderr.getvalue())


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

    def setUp(self):
        super(FindProbableCauseOfFailureTestCase, self).setUp()
        self.make_runner()

    def tearDown(self):
        self.cleanup_runner()
        super(FindProbableCauseOfFailureTestCase, self).tearDown()

    # We're mostly concerned here that the right log files are read in the
    # right order. parsing of the logs is handled by tests.parse_test
    def make_runner(self):
        self.add_mock_s3_data({'walrus': {}})
        self.runner = EMRJobRunner(s3_sync_wait_time=0,
                                   s3_scratch_uri='s3://walrus/tmp',
                                   conf_path=False)
        self.runner._s3_job_log_uri = BUCKET_URI + LOG_DIR

    def cleanup_runner(self):
        self.runner.cleanup()

    def test_empty(self):
        self.add_mock_s3_data({'walrus': {}})
        self.assertEqual(self.runner._find_probable_cause_of_failure([1]),
                         None)

    def test_python_exception(self):
        self.add_mock_s3_data({'walrus': {
            ATTEMPT_0_DIR + 'stderr':
                GARBAGE + TRACEBACK_START + PY_EXCEPTION + GARBAGE,
            ATTEMPT_0_DIR + 'syslog':
                make_input_uri_line(BUCKET_URI + 'input.gz'),
        }})
        self.assertEqual(
            self.runner._find_probable_cause_of_failure([1]),
            {'lines': list(StringIO(TRACEBACK_START + PY_EXCEPTION)),
             'log_file_uri': BUCKET_URI + ATTEMPT_0_DIR + 'stderr',
             'input_uri': BUCKET_URI + 'input.gz'})

    def test_python_exception_without_input_uri(self):
        self.add_mock_s3_data({'walrus': {
            ATTEMPT_0_DIR + 'stderr': (
                GARBAGE + TRACEBACK_START + PY_EXCEPTION + GARBAGE),
        }})
        self.assertEqual(
            self.runner._find_probable_cause_of_failure([1]),
            {'lines': list(StringIO(TRACEBACK_START + PY_EXCEPTION)),
             'log_file_uri': BUCKET_URI + ATTEMPT_0_DIR + 'stderr',
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
        self.assertEqual(
            self.runner._find_probable_cause_of_failure([1]),
            {'lines': list(StringIO(JAVA_STACK_TRACE)),
             'log_file_uri': BUCKET_URI + ATTEMPT_0_DIR + 'syslog',
             'input_uri': BUCKET_URI + 'input.gz'})

    def test_java_exception_without_input_uri(self):
        self.add_mock_s3_data({'walrus': {
            ATTEMPT_0_DIR + 'syslog':
                CHILD_ERR_LINE +
                JAVA_STACK_TRACE +
                GARBAGE,
        }})
        self.assertEqual(
            self.runner._find_probable_cause_of_failure([1]),
            {'lines': list(StringIO(JAVA_STACK_TRACE)),
             'log_file_uri': BUCKET_URI + ATTEMPT_0_DIR + 'syslog',
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

        self.assertEqual(
            self.runner._find_probable_cause_of_failure([1, 2, 3]),
            {'lines': [USEFUL_HADOOP_ERROR + '\n'],
             'log_file_uri': BUCKET_URI + LOG_DIR + 'steps/2/syslog',
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
        self.assertEqual(failure['log_file_uri'],
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
        self.assertEqual(failure['log_file_uri'],
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
        self.assertEqual(failure['log_file_uri'],
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
        self.assertEqual(failure['log_file_uri'],
                         BUCKET_URI + TASK_ATTEMPTS_DIR +
                         'attempt_201007271720_0001_m_000004_3/syslog')

    def test_py_exception_beats_java_stack_trace(self):
        self.add_mock_s3_data({'walrus': {
            ATTEMPT_0_DIR + 'stderr': TRACEBACK_START + PY_EXCEPTION,
            ATTEMPT_0_DIR + 'syslog': CHILD_ERR_LINE + JAVA_STACK_TRACE,
        }})
        failure = self.runner._find_probable_cause_of_failure([1])
        self.assertEqual(failure['log_file_uri'],
                         BUCKET_URI + ATTEMPT_0_DIR + 'stderr')

    def test_exception_beats_hadoop_error(self):
        self.add_mock_s3_data({'walrus': {
            TASK_ATTEMPTS_DIR + 'attempt_201007271720_0002_m_000126_0/stderr':
                TRACEBACK_START + PY_EXCEPTION,
            LOG_DIR + 'steps/1/syslog':
                HADOOP_ERR_LINE_PREFIX + USEFUL_HADOOP_ERROR + '\n',
        }})
        failure = self.runner._find_probable_cause_of_failure([1, 2])
        self.assertEqual(failure['log_file_uri'],
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
        self.assertEqual(failure['log_file_uri'],
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
        self.assertEqual(self.runner._find_probable_cause_of_failure([1]),
                         None)


class CounterFetchingTestCase(MockEMRAndS3TestCase):

    COUNTER_LINE = (
        'Job JOBID="job_201106092314_0001" FINISH_TIME="1307662284564"'
        ' JOB_STATUS="SUCCESS" FINISHED_MAPS="0" FINISHED_REDUCES="0"'
        ' FAILED_MAPS="0" FAILED_REDUCES="0" COUNTERS="%s" .' % ''.join([
            '{(org\.apache\.hadoop\.mapred\.JobInProgress$Counter)',
            '(Job Counters )',
            '[(TOTAL_LAUNCHED_REDUCES)(Launched reduce tasks)(1)]}',
    ]))

    def setUp(self):
        super(CounterFetchingTestCase, self).setUp()
        self.add_mock_s3_data({'walrus': {}})
        kwargs = {
            'conf_path': False,
            's3_scratch_uri': 's3://walrus/',
            's3_sync_wait_time': 0}
        with EMRJobRunner(**kwargs) as runner:
            self.job_flow_id = runner.make_persistent_job_flow()
        self.runner = EMRJobRunner(emr_job_flow_id=self.job_flow_id, **kwargs)

    def tearDown(self):
        super(CounterFetchingTestCase, self).tearDown()
        self.runner.cleanup()

    def test_empty_counters_running_job(self):
        self.runner._describe_jobflow().state = 'RUNNING'
        with no_handlers_for_logger():
            buf = log_to_buffer('mrjob.emr', logging.INFO)
            self.runner._fetch_counters([1], True)
            self.assertIn('5 minutes', buf.getvalue())

    def test_present_counters_running_job(self):
        self.add_mock_s3_data({'walrus': {
            'logs/j-MOCKJOBFLOW0/jobs/job_0_1_hadoop_streamjob1.jar': self.COUNTER_LINE}})
        self.runner._describe_jobflow().state = 'RUNNING'
        self.runner._fetch_counters([1], True)
        self.assertEqual(self.runner.counters(),
                         [{'Job Counters ': {'Launched reduce tasks': 1}}])

    def test_present_counters_terminated_job(self):
        self.add_mock_s3_data({'walrus': {
            'logs/j-MOCKJOBFLOW0/jobs/job_0_1_hadoop_streamjob1.jar': self.COUNTER_LINE}})
        self.runner._describe_jobflow().state = 'TERMINATED'
        self.runner._fetch_counters([1], True)
        self.assertEqual(self.runner.counters(),
                         [{'Job Counters ': {'Launched reduce tasks': 1}}])


class LogFetchingFallbackTestCase(MockEMRAndS3TestCase):

    def setUp(self):
        super(LogFetchingFallbackTestCase, self).setUp()
        self.make_runner()

    def tearDown(self):
        super(LogFetchingFallbackTestCase, self).tearDown()
        self.cleanup_runner()

    # Make sure that SSH and S3 are accessed when we expect them to be
    def make_runner(self):
        self.add_mock_s3_data({'walrus': {}})

        self.runner = EMRJobRunner(s3_sync_wait_time=0,
                                   s3_scratch_uri='s3://walrus/tmp',
                                   conf_path=False)
        self.runner._s3_job_log_uri = BUCKET_URI + LOG_DIR
        self.prepare_runner_for_ssh(self.runner)

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
        self.assertEqual(failure['log_file_uri'],
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
        self.assertEqual(failure['log_file_uri'],
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
        self.assertEqual(failure['log_file_uri'],
                         BUCKET_URI + TASK_ATTEMPTS_DIR +
                         'attempt_201007271720_0002_m_000126_0/stderr')


class TestEMRandS3Endpoints(MockEMRAndS3TestCase):

    def test_no_region(self):
        runner = EMRJobRunner(conf_path=False)
        self.assertEqual(runner.make_emr_conn().endpoint,
                         'elasticmapreduce.amazonaws.com')
        self.assertEqual(runner.make_s3_conn().endpoint,
                         's3.amazonaws.com')
        self.assertEqual(runner._aws_region, '')

    def test_none_region(self):
        # blank region should be treated the same as no region
        runner = EMRJobRunner(conf_path=False, aws_region=None)
        self.assertEqual(runner.make_emr_conn().endpoint,
                         'elasticmapreduce.amazonaws.com')
        self.assertEqual(runner.make_s3_conn().endpoint,
                         's3.amazonaws.com')
        self.assertEqual(runner._aws_region, '')

    def test_blank_region(self):
        # blank region should be treated the same as no region
        runner = EMRJobRunner(conf_path=False, aws_region='')
        self.assertEqual(runner.make_emr_conn().endpoint,
                         'elasticmapreduce.amazonaws.com')
        self.assertEqual(runner.make_s3_conn().endpoint,
                         's3.amazonaws.com')
        self.assertEqual(runner._aws_region, '')

    def test_eu(self):
        runner = EMRJobRunner(conf_path=False, aws_region='EU')
        self.assertEqual(runner.make_emr_conn().endpoint,
                         'eu-west-1.elasticmapreduce.amazonaws.com')
        self.assertEqual(runner.make_s3_conn().endpoint,
                         's3-eu-west-1.amazonaws.com')

    def test_us_east_1(self):
        runner = EMRJobRunner(conf_path=False, aws_region='us-east-1')
        self.assertEqual(runner.make_emr_conn().endpoint,
                         'us-east-1.elasticmapreduce.amazonaws.com')
        self.assertEqual(runner.make_s3_conn().endpoint,
                         's3.amazonaws.com')

    def test_us_west_1(self):
        runner = EMRJobRunner(conf_path=False, aws_region='us-west-1')
        self.assertEqual(runner.make_emr_conn().endpoint,
                         'us-west-1.elasticmapreduce.amazonaws.com')
        self.assertEqual(runner.make_s3_conn().endpoint,
                         's3-us-west-1.amazonaws.com')

    def test_ap_southeast_1(self):
        runner = EMRJobRunner(conf_path=False, aws_region='ap-southeast-1')
        self.assertEqual(runner.make_s3_conn().endpoint,
                         's3-ap-southeast-1.amazonaws.com')
        self.assertRaises(Exception, runner.make_emr_conn)

    def test_bad_region(self):
        # should fail in the constructor because the constructor connects to S3
        self.assertRaises(Exception, EMRJobRunner,
                          conf_path=False, aws_region='the-moooooooon-1')

    def test_case_sensitive(self):
        self.assertRaises(Exception, EMRJobRunner,
                          conf_path=False, aws_region='eu')
        self.assertRaises(Exception, EMRJobRunner,
                          conf_path=False, aws_region='US-WEST-1')

    def test_explicit_endpoints(self):
        runner = EMRJobRunner(conf_path=False, aws_region='EU',
                              s3_endpoint='s3-proxy', emr_endpoint='emr-proxy')
        self.assertEqual(runner.make_emr_conn().endpoint, 'emr-proxy')
        self.assertEqual(runner.make_s3_conn().endpoint, 's3-proxy')


class TestS3Ls(MockEMRAndS3TestCase):

    def test_s3_ls(self):
        self.add_mock_s3_data({'walrus': {'one': '', 'two': '', 'three': ''}})

        runner = EMRJobRunner(s3_scratch_uri='s3://walrus/tmp',
                              conf_path=False)

        self.assertEqual(set(runner._s3_ls('s3://walrus/')),
                         set(['s3://walrus/one',
                              's3://walrus/two',
                              's3://walrus/three',
                              ]))

        self.assertEqual(set(runner._s3_ls('s3://walrus/t')),
                         set(['s3://walrus/two',
                              's3://walrus/three',
                              ]))

        self.assertEqual(set(runner._s3_ls('s3://walrus/t/')),
                         set([]))

        # if we ask for a nonexistent bucket, we should get some sort
        # of exception (in practice, buckets with random names will
        # probably be owned by other people, and we'll get some sort
        # of permissions error)
        self.assertRaises(Exception, set, runner._s3_ls('s3://lolcat/'))


class TestSSHLs(MockEMRAndS3TestCase):

    def setUp(self):
        super(TestSSHLs, self).setUp()
        self.make_runner()

    def tearDown(self):
        super(TestSSHLs, self).tearDown()
        self.cleanup_runner()

    def make_runner(self):
        self.runner = EMRJobRunner(conf_path=False)
        self.prepare_runner_for_ssh(self.runner)

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

        self.assertEqual(
            sorted(self.runner.ls('ssh://testmaster/test')),
            ['ssh://testmaster/test/one', 'ssh://testmaster/test/two'])

        self.runner._enable_slave_ssh_access()

        self.assertEqual(
            list(self.runner.ls('ssh://testmaster!testslave0/test')),
            ['ssh://testmaster!testslave0/test/three'])

        # ls() is a generator, so the exception won't fire until we list() it
        self.assertRaises(IOError, list,
                          self.runner.ls('ssh://testmaster/does_not_exist'))


class TestNoBoto(unittest.TestCase):

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
        # because it'll need to connect to S3 to set s3_scratch_uri
        self.assertRaises(ImportError, EMRJobRunner, conf_path=False)

    def test_init_with_s3_scratch_uri(self):
        # this also raises an exception because we have to check
        # the bucket location
        self.assertRaises(ImportError, EMRJobRunner,
                          conf_path=False, s3_scratch_uri='s3://foo/tmp')


class TestMasterBootstrapScript(MockEMRAndS3TestCase):

    def setUp(self):
        super(TestMasterBootstrapScript, self).setUp()
        self.make_tmp_dir()

    def tearDown(self):
        super(TestMasterBootstrapScript, self).tearDown()
        self.rm_tmp_dir()

    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

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
                              bootstrap_actions=['foo', 'bar baz'],
                              pool_emr_job_flows=False)
        runner._create_master_bootstrap_script(dest=script_path)

        assert not os.path.exists(script_path)

    def test_bootstrap_script_if_needed_for_pooling(self):
        runner = EMRJobRunner(conf_path=False, bootstrap_mrjob=False)
        script_path = os.path.join(self.tmp_dir, 'b.py')

        runner._create_master_bootstrap_script(dest=script_path)
        assert not os.path.exists(script_path)

        # bootstrap actions don't figure into the master bootstrap script
        runner = EMRJobRunner(conf_path=False,
                              bootstrap_mrjob=False,
                              bootstrap_actions=['foo', 'bar baz'],
                              pool_emr_job_flows=True)
        runner._create_master_bootstrap_script(dest=script_path)

        assert os.path.exists(script_path)

    def test_bootstrap_actions_get_added(self):
        bootstrap_actions = [
            ('s3://elasticmapreduce/bootstrap-actions/configure-hadoop'
             ' -m,mapred.tasktracker.map.tasks.maximum=1'),
            's3://foo/bar#xyzzy',  # use alternate name for script
        ]

        runner = EMRJobRunner(conf_path=False,
                              bootstrap_actions=bootstrap_actions,
                              s3_sync_wait_time=0.00)

        job_flow_id = runner.make_persistent_job_flow()

        emr_conn = runner.make_emr_conn()
        job_flow = emr_conn.describe_jobflow(job_flow_id)
        actions = job_flow.bootstrapactions

        self.assertEqual(len(actions), 3)

        self.assertEqual(
            actions[0].path,
            's3://elasticmapreduce/bootstrap-actions/configure-hadoop')
        self.assertEqual(
            actions[0].args[0].value,
            '-m,mapred.tasktracker.map.tasks.maximum=1')
        self.assertEqual(actions[0].name, 'configure-hadoop')

        self.assertEqual(actions[1].path, 's3://foo/bar')
        self.assertEqual(actions[1].args, [])
        self.assertEqual(actions[1].name, 'xyzzy')

        # check for master bootstrap script
        assert actions[2].path.startswith('s3://mrjob-')
        assert actions[2].path.endswith('b.py')
        self.assertEqual(actions[2].args, [])
        self.assertEqual(actions[2].name, 'master')

        # make sure master bootstrap script is on S3
        assert runner.path_exists(actions[2].path)

    def test_bootstrap_script_uses_python_bin(self):
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
                              bootstrap_scripts=['speedups.sh', '/tmp/s.sh'],
                              python_bin=['anaconda'])
        script_path = os.path.join(self.tmp_dir, 'b.py')
        runner._create_master_bootstrap_script(dest=script_path)
        with open(script_path, 'r') as f:
            content = f.read()
            self.assertIn("call(['sudo', 'anaconda', '-m', 'compileall', '-f', mrjob_dir]", content)
            self.assertIn("check_call(['sudo', 'anaconda', 'setup.py', 'install']", content)

    def test_local_bootstrap_action(self):
        # make sure that local bootstrap action scripts get uploaded to S3
        action_path = os.path.join(self.tmp_dir, 'apt-install.sh')
        with open(action_path, 'w') as f:
            f.write('for $pkg in $@; do sudo apt-get install $pkg; done\n')

        bootstrap_actions = [
            action_path + ' python-scipy mysql-server']

        runner = EMRJobRunner(conf_path=False,
                              bootstrap_actions=bootstrap_actions,
                              s3_sync_wait_time=0.00)

        job_flow_id = runner.make_persistent_job_flow()

        emr_conn = runner.make_emr_conn()
        job_flow = emr_conn.describe_jobflow(job_flow_id)
        actions = job_flow.bootstrapactions

        self.assertEqual(len(actions), 2)

        assert actions[0].path.startswith('s3://mrjob-')
        assert actions[0].path.endswith('/apt-install.sh')
        self.assertEqual(actions[0].name, 'apt-install.sh')
        self.assertEqual(actions[0].args[0].value, 'python-scipy')
        self.assertEqual(actions[0].args[1].value, 'mysql-server')

        # check for master boostrap script
        assert actions[1].path.startswith('s3://mrjob-')
        assert actions[1].path.endswith('b.py')
        self.assertEqual(actions[1].args, [])
        self.assertEqual(actions[1].name, 'master')

        # make sure master bootstrap script is on S3
        assert runner.path_exists(actions[1].path)


class EMRNoMapperTest(MockEMRAndS3TestCase):

    def setUp(self):
        super(EMRNoMapperTest, self).setUp()
        self.make_tmp_dir()

    def tearDown(self):
        super(EMRNoMapperTest, self).tearDown()
        self.rm_tmp_dir()

    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

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

        self.assertEqual(sorted(results),
                         [(1, 'qux'), (2, 'bar'), (2, 'foo'), (5, None)])


class PoolingTestCase(MockEMRAndS3TestCase):

    def setUp(self):
        super(PoolingTestCase, self).setUp()
        self.make_tmp_dir()

    def tearDown(self):
        super(PoolingTestCase, self).tearDown()
        self.rm_tmp_dir()

    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    def rm_tmp_dir(self):
        try:
            shutil.rmtree(self.tmp_dir)
            self.teardown_ssh()
        except (OSError, AttributeError):
            pass  # didn't set up SSH

    def mrjob_conf_contents(self):
        return {'runners': {'emr': {
                'check_emr_status_every': 0.00,
                's3_sync_wait_time': 0.00,
            }}}

    def make_pooled_job_flow(self, name=None, minutes_ago=0, **kwargs):
        """Returns ``(runner, job_flow_id)``. Set minutes_ago to set
        ``jobflow.startdatetime`` to seconds before
        ``datetime.datetime.now()``."""
        runner = EMRJobRunner(conf_path=self.mrjob_conf_path,
                              pool_emr_job_flows=True,
                              emr_job_flow_pool_name=name,
                              **kwargs)
        job_flow_id = runner.make_persistent_job_flow()
        jf = runner.make_emr_conn().describe_jobflow(job_flow_id)
        jf.state = 'WAITING'
        start = datetime.now() - timedelta(minutes=minutes_ago)
        jf.startdatetime = start.strftime(boto.utils.ISO8601)
        return runner, job_flow_id

    def get_job_flow_and_results(self, job_args, mock_output=(),
                                 job_class=MRTwoStepJob):
        mr_job = job_class(job_args)
        mr_job.sandbox()

        results = []
        with mr_job.make_runner() as runner:
            self.prepare_runner_for_ssh(runner)
            runner.run()

            job_flow_id = runner.get_emr_job_flow_id()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

        return job_flow_id, sorted(results)

    def assertJoins(self, job_flow_id, job_args, job_class=MRTwoStepJob,
                    check_output=True):

        if check_output:
            mock_output = ['1\t"bar"\n1\t"foo"\n2\tnull\n']

            steps_in_jf = len(self.mock_emr_job_flows[job_flow_id].steps)
            steps_in_job = len(job_class(job_args).steps())
            step_num = steps_in_jf + steps_in_job - 1

            self.mock_emr_output[(job_flow_id, step_num)] = mock_output

        actual_job_flow_id, results = self.get_job_flow_and_results(
            job_args, job_class=job_class, mock_output=mock_output)

        self.assertEqual(actual_job_flow_id, job_flow_id)

        if check_output:
            self.assertEqual(results,
                             [(1, 'bar'), (1, 'foo'), (2, None)])

    def assertDoesNotJoin(self, job_flow_id, job_args, job_class=MRTwoStepJob):

        actual_job_flow_id, _ = self.get_job_flow_and_results(
            job_args, job_class=job_class)

        self.assertNotEqual(actual_job_flow_id, job_flow_id)

        # terminate the job flow created by this assert, to avoid
        # very confusing behavior (see Issue #331)
        emr_conn = EMRJobRunner(conf_path=False).make_emr_conn()
        emr_conn.terminate_jobflow(actual_job_flow_id)

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
            jf_hash, jf_name = pool_hash_and_name(job_flow)
            self.assertEqual(jf_hash, runner._pool_hash())
            self.assertEqual(jf_name, runner._opts['emr_job_flow_pool_name'])
            self.assertEqual(job_flow.state, 'WAITING')

    def test_join_pooled_job_flow(self):
        _, job_flow_id = self.make_pooled_job_flow()

        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '-c', self.mrjob_conf_path])

    def test_join_named_pool(self):
        _, job_flow_id = self.make_pooled_job_flow('pool1')

        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--pool-name', 'pool1',
            '-c', self.mrjob_conf_path])

    def test_pooling_with_hadoop_version(self):
        _, job_flow_id = self.make_pooled_job_flow(hadoop_version='0.18')

        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--hadoop-version', '0.18',
            '-c', self.mrjob_conf_path])

    def test_dont_join_pool_with_wrong_hadoop_version(self):
        _, job_flow_id = self.make_pooled_job_flow(hadoop_version='0.18')

        self.assertDoesNotJoin(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--hadoop-version', '0.20',
            '-c', self.mrjob_conf_path])

    def test_pooling_with_ami_version(self):
        _, job_flow_id = self.make_pooled_job_flow(ami_version='2.0')

        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--ami-version', '2.0',
            '-c', self.mrjob_conf_path])

    def test_dont_join_pool_with_wrong_ami_version(self):
        _, job_flow_id = self.make_pooled_job_flow(ami_version='2.0')

        self.assertDoesNotJoin(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--ami-version', '1.0',
            '-c', self.mrjob_conf_path])

    def test_pooling_with_additional_emr_info(self):
        info = '{"tomatoes": "actually a fruit!"}'
        _, job_flow_id = self.make_pooled_job_flow(
            additional_emr_info=info)

        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--additional-emr-info', info,
            '-c', self.mrjob_conf_path])

    def test_dont_join_pool_with_wrong_additional_emr_info(self):
        info = '{"tomatoes": "actually a fruit!"}'
        _, job_flow_id = self.make_pooled_job_flow()

        self.assertDoesNotJoin(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--additional-emr-info', info,
            '-c', self.mrjob_conf_path])

    def test_join_pool_with_same_instance_type_and_count(self):
        _, job_flow_id = self.make_pooled_job_flow(
            ec2_instance_type='m2.4xlarge',
            num_ec2_instances=20)

        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--ec2-instance-type', 'm2.4xlarge',
            '--num-ec2-instances', '20',
            '-c', self.mrjob_conf_path])

    def test_join_pool_with_more_of_same_instance_type(self):
        _, job_flow_id = self.make_pooled_job_flow(
            ec2_instance_type='m2.4xlarge',
            num_ec2_instances=20)

        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--ec2-instance-type', 'm2.4xlarge',
            '--num-ec2-instances', '5',
            '-c', self.mrjob_conf_path])

    def test_join_job_flow_with_bigger_instances(self):
        _, job_flow_id = self.make_pooled_job_flow(
            ec2_instance_type='m2.4xlarge',
            num_ec2_instances=20)

        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--ec2-instance-type', 'm1.small',
            '--num-ec2-instances', '20',
            '-c', self.mrjob_conf_path])

    def test_join_job_flow_with_enough_cpu_and_memory(self):
        _, job_flow_id = self.make_pooled_job_flow(
            ec2_instance_type='c1.xlarge',
            num_ec2_instances=3)

        # join the pooled job flow even though it has less instances total,
        # since they're have enough memory and CPU
        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--ec2-instance-type', 'm1.small',
            '--num-ec2-instances', '10',
            '-c', self.mrjob_conf_path])

    def test_dont_join_job_flow_with_instances_with_too_little_memory(self):
        _, job_flow_id = self.make_pooled_job_flow(
            ec2_instance_type='c1.xlarge',
            num_ec2_instances=20)

        self.assertDoesNotJoin(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--ec2-instance-type', 'm2.4xlarge',
            '--num-ec2-instances', '2',
            '-c', self.mrjob_conf_path])

    def test_master_instance_has_to_be_big_enough(self):
        _, job_flow_id = self.make_pooled_job_flow(
            ec2_instance_type='c1.xlarge',
            num_ec2_instances=10)

        # We implicitly want a MASTER instance with c1.xlarge. The pooled
        # job flow has an m1.small master instance and 9 c1.xlarge core
        # instances, which doesn't match.
        self.assertDoesNotJoin(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--ec2-instance-type', 'c1.xlarge',
            '--num-ec2-instances', '1',
            '-c', self.mrjob_conf_path])

    def test_unknown_instance_type_against_matching_pool(self):
        _, job_flow_id = self.make_pooled_job_flow(
            ec2_instance_type='a1.sauce',
            num_ec2_instances=10)

        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--ec2-instance-type', 'a1.sauce',
            '--num-ec2-instances', '10',
            '-c', self.mrjob_conf_path])

    def test_unknown_instance_type_against_pool_with_more_instances(self):
        _, job_flow_id = self.make_pooled_job_flow(
            ec2_instance_type='a1.sauce',
            num_ec2_instances=20)

        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--ec2-instance-type', 'a1.sauce',
            '--num-ec2-instances', '10',
            '-c', self.mrjob_conf_path])

    def test_unknown_instance_type_against_pool_with_less_instances(self):
        _, job_flow_id = self.make_pooled_job_flow(
            ec2_instance_type='a1.sauce',
            num_ec2_instances=5)

        self.assertDoesNotJoin(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--ec2-instance-type', 'a1.sauce',
            '--num-ec2-instances', '10',
            '-c', self.mrjob_conf_path])

    def test_unknown_instance_type_against_other_instance_types(self):
        _, job_flow_id = self.make_pooled_job_flow(
            ec2_instance_type='m2.4xlarge',
            num_ec2_instances=100)

        # for all we know, "a1.sauce" instances have even more memory and CPU
        # than m2.4xlarge
        self.assertDoesNotJoin(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--ec2-instance-type', 'a1.sauce',
            '--num-ec2-instances', '2',
            '-c', self.mrjob_conf_path])

    def test_can_join_job_flow_with_same_bid_price(self):
        _, job_flow_id = self.make_pooled_job_flow(
            ec2_master_instance_bid_price='0.25')

        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--ec2-master-instance-bid-price', '0.25',
            '-c', self.mrjob_conf_path])

    def test_can_join_job_flow_with_higher_bid_price(self):
        _, job_flow_id = self.make_pooled_job_flow(
            ec2_master_instance_bid_price='25.00')

        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--ec2-master-instance-bid-price', '0.25',
            '-c', self.mrjob_conf_path])

    def test_cant_join_job_flow_with_lower_bid_price(self):
        _, job_flow_id = self.make_pooled_job_flow(
            ec2_master_instance_bid_price='0.25',
            num_ec2_instances=100)

        self.assertDoesNotJoin(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--ec2-master-instance-bid-price', '25.00',
            '-c', self.mrjob_conf_path])

    def test_on_demand_satisfies_any_bid_price(self):
        _, job_flow_id = self.make_pooled_job_flow()

        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--ec2-master-instance-bid-price', '25.00',
            '-c', self.mrjob_conf_path])

    def test_no_bid_price_satisfies_on_demand(self):
        _, job_flow_id = self.make_pooled_job_flow(
            ec2_master_instance_bid_price='25.00')

        self.assertDoesNotJoin(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '-c', self.mrjob_conf_path])

    def test_core_and_task_instance_types(self):
        # a tricky test that mixes and matches different criteria
        _, job_flow_id = self.make_pooled_job_flow(
            ec2_core_instance_bid_price='0.25',
            ec2_task_instance_bid_price='25.00',
            ec2_task_instance_type='c1.xlarge',
            num_ec2_core_instances=2,
            num_ec2_task_instances=3)

        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--num-ec2-core-instances', '2',
            '--num-ec2-task-instances', '10',  # more instances, but smaller
            '--ec2-core-instance-bid-price', '0.10',
            '--ec2-master-instance-bid-price', '77.77',
            '--ec2-task-instance-bid-price', '22.00',
            '-c', self.mrjob_conf_path])

    def test_can_turn_off_pooling_from_cmd_line(self):
        # turn on pooling in mrjob.conf
        with open(self.mrjob_conf_path, 'w') as f:
            dump_mrjob_conf({'runners': {'emr': {
                'check_emr_status_every': 0.00,
                's3_sync_wait_time': 0.00,
                'pool_emr_job_flows': True,
            }}}, f)

        mr_job = MRTwoStepJob([
            '-r', 'emr', '-v', '--no-pool-emr-job-flows',
            '-c', self.mrjob_conf_path])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            self.prepare_runner_for_ssh(runner)
            runner.run()

            job_flow_id = runner.get_emr_job_flow_id()
            jf = runner.make_emr_conn().describe_jobflow(job_flow_id)
            self.assertEqual(jf.keepjobflowalivewhennosteps, 'false')

    def test_dont_join_full_job_flow(self):
        dummy_runner, job_flow_id = self.make_pooled_job_flow('pool1')

        # fill the job flow
        self.mock_emr_job_flows[job_flow_id].steps = 255 * [
            MockEmrObject(
                state='COMPLETED',
                name='dummy',
                actiononfailure='CANCEL_AND_WAIT',
                args=[])]

        # a two-step job shouldn't fit
        self.assertDoesNotJoin(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--pool-name', 'pool1',
            '-c', self.mrjob_conf_path],
            job_class=MRTwoStepJob)

    def test_join_almost_full_job_flow(self):
        dummy_runner, job_flow_id = self.make_pooled_job_flow('pool1')

        # fill the job flow
        self.mock_emr_job_flows[job_flow_id].steps = 255 * [
            MockEmrObject(
                state='COMPLETED',
                name='dummy',
                actiononfailure='CANCEL_AND_WAIT',
                enddatetime='definitely not none',
                args=[])]

        # a one-step job should fit
        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--pool-name', 'pool1',
            '-c', self.mrjob_conf_path],
            job_class=MRWordCount)

    def test_dont_join_idle_with_steps(self):
        dummy_runner, job_flow_id = self.make_pooled_job_flow('pool1')

        self.mock_emr_job_flows[job_flow_id].steps = [
            MockEmrObject(
                state='WAITING',
                name='dummy',
                actiononfailure='CANCEL_AND_WAIT',
                args=[])]

        self.assertDoesNotJoin(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--pool-name', 'pool1',
            '-c', self.mrjob_conf_path],
            job_class=MRWordCount)

    def test_dont_join_wrong_named_pool(self):
        _, job_flow_id = self.make_pooled_job_flow('pool1')

        self.assertDoesNotJoin(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--pool-name', 'not_pool1',
            '-c', self.mrjob_conf_path])

    def test_dont_join_wrong_mrjob_version(self):
        _, job_flow_id = self.make_pooled_job_flow('pool1')

        old_version = mrjob.__version__

        try:
            mrjob.__version__ = 'OVER NINE THOUSAAAAAND'

            self.assertDoesNotJoin(job_flow_id, [
                '-r', 'emr', '-v', '--pool-emr-job-flows',
                '--pool-name', 'not_pool1',
                '-c', self.mrjob_conf_path])
        finally:
            mrjob.__version__ = old_version

    def test_join_similarly_bootstrapped_pool(self):
        local_input_path = os.path.join(self.tmp_dir, 'input')
        with open(local_input_path, 'w') as input_file:
            input_file.write('bar\nfoo\n')

        _, job_flow_id = self.make_pooled_job_flow(
            bootstrap_files=[local_input_path])

        self.assertJoins(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--bootstrap-file', local_input_path,
            '-c', self.mrjob_conf_path])

    def test_dont_join_differently_bootstrapped_pool(self):
        local_input_path = os.path.join(self.tmp_dir, 'input')
        with open(local_input_path, 'w') as input_file:
            input_file.write('bar\nfoo\n')

        _, job_flow_id = self.make_pooled_job_flow()

        self.assertDoesNotJoin(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--bootstrap-file', local_input_path,
            '-c', self.mrjob_conf_path])

    def test_dont_join_differently_bootstrapped_pool_2(self):
        local_input_path = os.path.join(self.tmp_dir, 'input')
        with open(local_input_path, 'w') as input_file:
            input_file.write('bar\nfoo\n')

        bootstrap_path = os.path.join(self.tmp_dir, 'go.sh')
        with open(bootstrap_path, 'w') as f:
            f.write('#!/usr/bin/sh\necho "hi mom"\n')

        _, job_flow_id = self.make_pooled_job_flow()

        self.assertDoesNotJoin(job_flow_id, [
            '-r', 'emr', '-v', '--pool-emr-job-flows',
            '--bootstrap-action', bootstrap_path + ' a b c',
            '-c', self.mrjob_conf_path])

    def test_pool_contention(self):
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
        self.assertEqual(jf1.jobflowid, job_flow_id)
        self.assertEqual(jf2, None)
        jf1.status = 'COMPLETED'

    def test_sorting_by_time(self):
        _, job_flow_id_1 = self.make_pooled_job_flow('pool1', minutes_ago=20)
        _, job_flow_id_2 = self.make_pooled_job_flow('pool1', minutes_ago=40)

        runner1 = self.make_simple_runner('pool1')
        runner2 = self.make_simple_runner('pool1')

        jf1 = runner1.find_job_flow()
        jf2 = runner2.find_job_flow()
        self.assertEqual(jf1.jobflowid, job_flow_id_1)
        self.assertEqual(jf2.jobflowid, job_flow_id_2)
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
        self.assertEqual(jf1.jobflowid, job_flow_id_1)
        self.assertEqual(jf2.jobflowid, job_flow_id_2)
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
                self.assertRaises(Exception, runner.run)

            emr_conn = runner.make_emr_conn()
            job_flow_id = runner.get_emr_job_flow_id()
            for _ in xrange(10):
                emr_conn.simulate_progress(job_flow_id)

            job_flow = emr_conn.describe_jobflow(job_flow_id)
            self.assertEqual(job_flow.state, 'WAITING')

        # job shouldn't get terminated by cleanup
        emr_conn = runner.make_emr_conn()
        job_flow_id = runner.get_emr_job_flow_id()
        for _ in xrange(10):
            emr_conn.simulate_progress(job_flow_id)

        job_flow = emr_conn.describe_jobflow(job_flow_id)
        self.assertEqual(job_flow.state, 'WAITING')

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
                self.assertRaises(Exception, runner.run)

            self.assertEqual(runner.get_emr_job_flow_id(), job_flow_id)

            emr_conn = runner.make_emr_conn()
            for _ in xrange(10):
                emr_conn.simulate_progress(job_flow_id)

            job_flow = emr_conn.describe_jobflow(job_flow_id)
            self.assertEqual(job_flow.state, 'WAITING')

        # job shouldn't get terminated by cleanup
        emr_conn = runner.make_emr_conn()
        job_flow_id = runner.get_emr_job_flow_id()
        for _ in xrange(10):
            emr_conn.simulate_progress(job_flow_id)

        job_flow = emr_conn.describe_jobflow(job_flow_id)
        self.assertEqual(job_flow.state, 'WAITING')


class S3LockTestCase(MockEMRAndS3TestCase):

    def setUp(self):
        super(S3LockTestCase, self).setUp()
        self.make_buckets()

    def make_buckets(self):
        self.add_mock_s3_data({'locks': {
            'expired_lock': 'x',
        }}, datetime.utcnow() - timedelta(minutes=30))
        self.lock_uri = 's3://locks/some_lock'
        self.expired_lock_uri = 's3://locks/expired_lock'

    def test_lock(self):
        # Most basic test case
        runner = EMRJobRunner(conf_path=False)
        s3_conn = runner.make_s3_conn()

        self.assertEqual(
            True, attempt_to_acquire_lock(s3_conn, self.lock_uri, 0, 'jf1'))

        self.assertEqual(
            False, attempt_to_acquire_lock(s3_conn, self.lock_uri, 0, 'jf2'))

    def test_lock_expiration(self):
        runner = EMRJobRunner(conf_path=False)
        s3_conn = runner.make_s3_conn()
        did_lock = attempt_to_acquire_lock(
            s3_conn, self.expired_lock_uri, 0, 'jf1',
            mins_to_expiration=5)
        self.assertEqual(True, did_lock)

    def test_key_race_condition(self):
        # Test case where one attempt puts the key in existence
        runner = EMRJobRunner(conf_path=False)
        s3_conn = runner.make_s3_conn()

        key = _lock_acquire_step_1(s3_conn, self.lock_uri, 'jf1')
        self.assertNotEqual(key, None)

        key2 = _lock_acquire_step_1(s3_conn, self.lock_uri, 'jf2')
        self.assertEqual(key2, None)

    def test_read_race_condition(self):
        # test case where both try to create the key
        runner = EMRJobRunner(conf_path=False)
        s3_conn = runner.make_s3_conn()

        key = _lock_acquire_step_1(s3_conn, self.lock_uri, 'jf1')
        self.assertNotEqual(key, None)

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

        self.assertEqual(list(runner.cat('s3://walrus/one')), ['one_text\n'])

    def test_ssh_cat(self):
        runner = EMRJobRunner(conf_path=False)
        self.prepare_runner_for_ssh(runner)
        mock_ssh_file('testmaster', 'etc/init.d', 'meow')

        self.assertEqual(
            list(runner.cat(SSH_PREFIX + runner._address + '/etc/init.d')),
            ['meow\n'])
        self.assertRaises(
            IOError, list,
            runner.cat(SSH_PREFIX + runner._address + '/does_not_exist'))
