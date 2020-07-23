# Copyright 2009-2017 Yelp and Contributors
# Copyright 2018-2019 Yelp
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
"""Test case to mock out boto3."""
import itertools
import os
import shutil
import tempfile
import time
from io import BytesIO

import boto3

from mrjob.aws import _boto3_now
from mrjob.emr import _EMR_LOG_DIR
from mrjob.emr import EMRJobRunner

from tests.mockssh import create_mock_ssh_script
from tests.mockssh import mock_ssh_dir
from tests.mr_two_step_job import MRTwoStepJob
from tests.py2 import MagicMock
from tests.py2 import patch
from tests.sandbox import SandboxedTestCase

from .ec2 import MockEC2Client
from .emr import MockEMRClient
from .iam import MockIAMClient
from .s3 import MockS3Client
from .s3 import MockS3Resource
from .s3 import add_mock_s3_data


class MockBoto3TestCase(SandboxedTestCase):

    # if a test needs to create an EMR client more than this many
    # times, there's probably a problem with simulating progress
    MAX_EMR_CLIENTS = 120

    def setUp(self):
        # patch boto3
        self.mock_ec2_images = []
        self.mock_emr_failures = set()
        self.mock_emr_self_termination = set()
        self.mock_emr_clusters = {}
        self.mock_emr_output = {}
        self.mock_iam_instance_profiles = {}
        self.mock_iam_role_attached_policies = {}
        self.mock_iam_roles = {}
        self.mock_s3_fs = {}

        self.emr_client = None  # used by simulate_emr_progress()
        self.emr_client_counter = itertools.repeat(
            None, self.MAX_EMR_CLIENTS)

        self.start(patch.object(boto3, 'client', self.client))
        self.start(patch.object(boto3, 'resource', self.resource))

        super(MockBoto3TestCase, self).setUp()

        # patch slow things
        self.mrjob_zip_path = None

        def fake_create_mrjob_zip(mocked_runner, *args, **kwargs):
            if not self.mrjob_zip_path:
                self.mrjob_zip_path = self.makefile('fake_mrjob.zip')

            mocked_runner._mrjob_zip_path = self.mrjob_zip_path

            return self.mrjob_zip_path

        self.start(patch.object(
            EMRJobRunner, '_create_mrjob_zip',
            fake_create_mrjob_zip))

        self.start(patch.object(time, 'sleep'))

    def add_mock_s3_data(self, data,
                         age=None, location=None, storage_class=None,
                         restore=None):
        """Update self.mock_s3_fs with a map from bucket name
        to key name to data."""
        add_mock_s3_data(
            self.mock_s3_fs, data, age, location, storage_class, restore)

    def add_mock_ec2_image(self, image):
        """Add information about a mock EC2 Image (AMI) to be returned by
        mock :py:meth:`~tests.mock_boto3.ec2.MockEC2Client.describe_images`.

        This will automatically fill `CreationDate`. Other fields you
        might want to fill include:

        * ``Architecture`` (e.g. ``'i386'``, ``'x86_64'``)
        * ``BlockDeviceMappings`` (e.g. ``[{'DeviceName': '/dev/sda1'}]``)
        * ``ImageOwnerAlias`` (e.g. ``'amazon'``, ``'aws-marketplace'``)
        * ``Name`` (e.g. ``amzn-ami-hvm-2017.09.1.20171120-x86_64-s3``)
        * ``RootDeviceType`` (e.g. ``'ebs'``, ``'instance-store'``)
        * ``VirtualizationType (e.g. ``'hvm'``, ``'paravirtual'``)
        """
        image = dict(image)

        # TODO: will eventually need to add a mock user ID to support
        # filtering by owner == 'self'

        if not image.get('CreationDate'):
            image['CreationDate'] = _boto3_now().strftime(
                '%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        self.mock_ec2_images.append(image)

    def add_mock_emr_cluster(self, cluster):
        if cluster['Id'] in self.mock_emr_clusters:
            raise ValueError('mock cluster %s already exists' % cluster.id)

        for field in ('_BootstrapActions', '_InstanceGroups', '_Steps'):
            cluster.setdefault(field, [])

        cluster.setdefault('Name', cluster['Id'][2:])
        cluster.setdefault('NormalizedInstanceHours', 0)

        self.mock_emr_clusters[cluster['Id']] = cluster

    def simulate_emr_progress(self, cluster_id):
        if not self.emr_client:
            self.emr_client = self.client('emr')

        self.emr_client._simulate_progress(cluster_id)

    def prepare_runner_for_ssh(self, runner, num_workers=0):
        # TODO: Refactor this abomination of a test harness

        # Set up environment variables
        os.environ['MOCK_SSH_VERIFY_KEY_FILE'] = 'true'

        # Create temporary directories and add them to MOCK_SSH_ROOTS
        main_ssh_root = tempfile.mkdtemp(prefix='main_ssh_root.')
        os.environ['MOCK_SSH_ROOTS'] = 'testmain=%s' % main_ssh_root
        mock_ssh_dir('testmain', _EMR_LOG_DIR + '/hadoop/history')

        if not hasattr(self, 'worker_ssh_roots'):
            self.worker_ssh_roots = []

        self.addCleanup(self.teardown_ssh, main_ssh_root)

        # Make the fake binary
        os.mkdir(os.path.join(main_ssh_root, 'bin'))
        self.ssh_bin = os.path.join(main_ssh_root, 'bin', 'ssh')
        create_mock_ssh_script(self.ssh_bin)
        self.ssh_add_bin = os.path.join(main_ssh_root, 'bin', 'ssh-add')
        create_mock_ssh_script(self.ssh_add_bin)

        # Make a fake keyfile so that the 'file exists' requirements are
        # satsified
        self.keyfile_path = os.path.join(main_ssh_root, 'key.pem')
        with open(self.keyfile_path, 'w') as f:
            f.write('I AM DEFINITELY AN SSH KEY FILE')

        # Tell the runner to use the fake binary
        runner._opts['ssh_bin'] = [self.ssh_bin]
        runner._opts['ssh_add_bin'] = [self.ssh_add_bin]
        # Also pretend to have an SSH key pair file
        runner._opts['ec2_key_pair_file'] = self.keyfile_path

        # use fake hostname
        runner._address_of_main = MagicMock(return_value='testmain')
        runner._main_private_ip = MagicMock(return_value='172.172.172.172')

        # re-initialize fs
        runner._fs = None

    # TODO: this should be replaced
    def add_worker(self):
        """Add a mocked worker to the cluster. Caller is responsible for
        setting runner._opts['num_ec2_instances'] to the correct number.
        """
        worker_num = len(self.worker_ssh_roots)
        new_dir = tempfile.mkdtemp(prefix='worker_%d_ssh_root.' % worker_num)
        self.worker_ssh_roots.append(new_dir)
        os.environ['MOCK_SSH_ROOTS'] += (':testmain!testworker%d=%s'
                                         % (worker_num, new_dir))

    def teardown_ssh(self, main_ssh_root):
        shutil.rmtree(main_ssh_root)
        for path in self.worker_ssh_roots:
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

    def launch(self, runner):
        """Launch a job but don't wait for it to finish."""
        runner._add_input_files_for_upload()
        runner._launch()

    def run_and_get_cluster(self, *args):
        with self.make_runner(*args) as runner:
            runner.run()
            return runner._describe_cluster()

    # mock boto3.client()
    def client(self, service_name, **kwargs):
        if service_name == 'ec2':
            kwargs['mock_ec2_images'] = self.mock_ec2_images
            return MockEC2Client(**kwargs)

        elif service_name == 'emr':
            try:
                next(self.emr_client_counter)
            except StopIteration:
                raise Exception(
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
