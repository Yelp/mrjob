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
"""Test case to mock out boto3."""
import itertools
import os
import shutil
import tempfile
import time
from io import BytesIO

import boto3

from mrjob.emr import _EMR_LOG_DIR
from mrjob.emr import EMRJobRunner

from ..mockssh import create_mock_ssh_script
from ..mockssh import mock_ssh_dir
from ..mr_two_step_job import MRTwoStepJob
from ..py2 import MagicMock
from ..py2 import patch
from ..sandbox import SandboxedTestCase

from .emr import MockEMRClient
from .emr import Boto2TestSkipper
from .iam import MockIAMClient
from .s3 import MockS3Client
from .s3 import MockS3Resource
from .s3 import add_mock_s3_data

class MockBoto3TestCase(SandboxedTestCase):

    # if a test needs to create an EMR client more than this many
    # times, there's probably a problem with simulating progress
    MAX_EMR_CLIENTS = 100

    @classmethod
    def setUpClass(cls):
        # we don't care what's in this file, just want mrjob to stop creating
        # and deleting a complicated archive.
        cls.fake_mrjob_zip_path = tempfile.mkstemp(
            prefix='fake_mrjob_', suffix='.zip')[1]

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.fake_mrjob_zip_path):
            os.remove(cls.fake_mrjob_zip_path)

    def setUp(self):
        # patch boto3
        self.mock_emr_failures = set()
        self.mock_emr_self_termination = set()
        self.mock_emr_clusters = {}
        self.mock_emr_output = {}
        self.mock_iam_instance_profiles = {}
        self.mock_iam_role_attached_policies = {}
        self.mock_iam_roles = {}
        self.mock_s3_fs = {}

        self.emr_client_counter = itertools.repeat(
            None, self.MAX_EMR_CLIENTS)

        self.start(patch.object(boto3, 'client', self.client))
        self.start(patch.object(boto3, 'resource', self.resource))

        super(MockBoto3TestCase, self).setUp()

        # patch slow things
        def fake_create_mrjob_zip(mocked_self, *args, **kwargs):
            mocked_self._mrjob_zip_path = self.fake_mrjob_zip_path
            return self.fake_mrjob_zip_path

        self.start(patch.object(
            EMRJobRunner, '_create_mrjob_zip',
            fake_create_mrjob_zip))

        self.start(patch.object(
            EMRJobRunner, 'make_emr_conn',
            Boto2TestSkipper(), create=True))

        self.start(patch.object(time, 'sleep'))

    def add_mock_s3_data(self, data, age=None, location=None):
        """Update self.mock_s3_fs with a map from bucket name
        to key name to data."""
        add_mock_s3_data(self.mock_s3_fs, data, age, location)

    def add_mock_emr_cluster(self, cluster):
        if cluster.id in self.mock_emr_clusters:
            raise ValueError('mock cluster %s already exists' % cluster.id)

        for field in ('_bootstrapactions', '_instancegroups', '_steps'):
            if not hasattr(cluster, field):
                setattr(cluster, field, [])

        if not hasattr(cluster, 'name'):
            cluster.name = cluster.id[2:]

        if not hasattr(cluster, 'normalizedinstancehours'):
            cluster.normalizedinstancehours = '0'

        self.mock_emr_clusters[cluster.id] = cluster

    def prepare_runner_for_ssh(self, runner, num_slaves=0):
        # TODO: Refactor this abomination of a test harness

        # Set up environment variables
        os.environ['MOCK_SSH_VERIFY_KEY_FILE'] = 'true'

        # Create temporary directories and add them to MOCK_SSH_ROOTS
        master_ssh_root = tempfile.mkdtemp(prefix='master_ssh_root.')
        os.environ['MOCK_SSH_ROOTS'] = 'testmaster=%s' % master_ssh_root
        mock_ssh_dir('testmaster', _EMR_LOG_DIR + '/hadoop/history')

        if not hasattr(self, 'slave_ssh_roots'):
            self.slave_ssh_roots = []

        self.addCleanup(self.teardown_ssh, master_ssh_root)

        # Make the fake binary
        os.mkdir(os.path.join(master_ssh_root, 'bin'))
        self.ssh_bin = os.path.join(master_ssh_root, 'bin', 'ssh')
        create_mock_ssh_script(self.ssh_bin)

        # Make a fake keyfile so that the 'file exists' requirements are
        # satsified
        self.keyfile_path = os.path.join(master_ssh_root, 'key.pem')
        with open(self.keyfile_path, 'w') as f:
            f.write('I AM DEFINITELY AN SSH KEY FILE')

        # Tell the runner to use the fake binary
        runner._opts['ssh_bin'] = [self.ssh_bin]
        # Also pretend to have an SSH key pair file
        runner._opts['ec2_key_pair_file'] = self.keyfile_path

        # use fake hostname
        runner._address_of_master = MagicMock(return_value='testmaster')
        runner._master_private_ip = MagicMock(return_value='172.172.172.172')

        # re-initialize fs
        runner._fs = None
        #runner.fs

    # TODO: this should be replaced
    def add_slave(self):
        """Add a mocked slave to the cluster. Caller is responsible for setting
        runner._opts['num_ec2_instances'] to the correct number.
        """
        slave_num = len(self.slave_ssh_roots)
        new_dir = tempfile.mkdtemp(prefix='slave_%d_ssh_root.' % slave_num)
        self.slave_ssh_roots.append(new_dir)
        os.environ['MOCK_SSH_ROOTS'] += (':testmaster!testslave%d=%s'
                                         % (slave_num, new_dir))

    def teardown_ssh(self, master_ssh_root):
        shutil.rmtree(master_ssh_root)
        for path in self.slave_ssh_roots:
            shutil.rmtree(path)

    def make_runner(self, *args):
        """create a dummy job, and call make_runner() on it.
        Use this in a with block:

        with self.make_runner() as runner:
            ...
        """
        stdin = BytesIO(b'foo\nbar\n')
        mr_job = MRTwoStepJob(['-r', 'emr'] + list(args))
        mr_job.sandbox(stdin=stdin)

        return mr_job.make_runner()

    def run_and_get_cluster(self, *args):
        # TODO: not sure why we include -v
        with self.make_runner('-v', *args) as runner:
            runner.run()
            return runner._describe_cluster()

    # mock boto3.client()
    def client(self, service_name, **kwargs):
        if service_name == 'emr':
            try:
                next(self.emr_client_counter)
            except StopIteration:
                raise AssertionError(
                    'Too many connections to mock EMR, may be stalled')

            kwargs['mock_s3_fs'] = self.mock_s3_fs
            kwargs['mock_emr_clusters'] = self.mock_emr_clusters
            kwargs['mock_emr_failures'] = self.mock_emr_failures
            kwargs['mock_emr_self_termination'] = (
                self.mock_emr_self_termination)
            kwargs['mock_emr_output'] = self.mock_emr_output
            return MockEMRClient(**kwargs)

        elif service_name == 'iam':
            kwargs['mock_iam_instance_profiles'] = (
                self.mock_iam_instance_profiles)
            kwargs['mock_iam_roles'] = self.mock_iam_roles
            kwargs['mock_iam_role_attached_policies'] = (
                self.mock_iam_role_attached_policies)
            return MockIAMClient(**kwargs)

        elif service_name == 's3':
            kwargs['mock_s3_fs'] = self.mock_s3_fs
            return MockS3Client(**kwargs)
        else:
            raise NotImplementedError(
                'mock %s service not supported' % service_name)

    # mock boto3.resource()
    def resource(self, service_name, **kwargs):
        if service_name == 's3':
            kwargs['mock_s3_fs'] = self.mock_s3_fs
            return MockS3Resource(**kwargs)
        else:
            raise NotImplementedError(
                'mock %s resource not supported' % service_name)
