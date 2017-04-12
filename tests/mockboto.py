# Copyright 2009-2016 Yelp and Contributors
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
"""Mercilessly taunt an Amazonian river dolphin.

This is by no means a complete mock of boto3, just what we need for tests.
"""
import hashlib
import itertools
import json
import os
import shutil
import tempfile
import time
from datetime import datetime
from datetime import timedelta
from io import BytesIO

import boto3
import botocore.config
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError
from botocore.exceptions import ParamValidationError

from mrjob.aws import _DEFAULT_AWS_REGION
from mrjob.aws import _boto3_now
from mrjob.compat import map_version
from mrjob.compat import version_gte
from mrjob.conf import combine_dicts
from mrjob.conf import combine_values
from mrjob.emr import _EMR_LOG_DIR
from mrjob.emr import EMRJobRunner
from mrjob.parse import is_s3_uri
from mrjob.parse import parse_s3_uri
from mrjob.py2 import integer_types
from mrjob.py2 import string_types

from tests.mockssh import create_mock_ssh_script
from tests.mockssh import mock_ssh_dir
from tests.mr_two_step_job import MRTwoStepJob
from tests.py2 import Mock
from tests.py2 import MagicMock
from tests.py2 import patch
from tests.sandbox import SandboxedTestCase

# list_clusters() only returns this many results at a time
DEFAULT_MAX_CLUSTERS_RETURNED = 50

# list_steps() only returns this many results at a time
DEFAULT_MAX_STEPS_RETURNED = 50

# Size of each chunk returned by the MockS3Object iterator
SIMULATED_BUFFER_SIZE = 256

# what partial versions and "latest" map to, as of 2015-07-15
AMI_VERSION_ALIASES = {
    None: '1.0.0',  # API does this for old accounts
    '2.0': '2.0.6',
    '2.1': '2.1.4',
    '2.2': '2.2.4',
    '2.3': '2.3.6',
    '2.4': '2.4.11',
    '3.0': '3.0.4',
    '3.1': '3.1.4',
    '3.2': '3.2.3',
    '3.3': '3.3.2',
    '3.4': '3.4.0',
    '3.5': '3.5.0',
    '3.6': '3.6.0',
    '3.7': '3.7.0',
    '3.8': '3.8.0',
    '3.9': '3.9.0',
    '3.10': '3.10.0',
    '3.11': '3.11.0',
    'latest': '2.4.2',
}

# versions of hadoop for each AMI
AMI_HADOOP_VERSION_UPDATES = {
    '1.0.0': '0.20',
    '2.0.0': '0.20.205',
    '2.2.0': '1.0.3',
    '3.0.0': '2.2.0',
    '3.1.0': '2.4.0',
    '4.0.0': '2.6.0',
    '4.3.0': '2.7.1',
    '4.5.0': '2.7.2',
    '4.8.2': '2.7.3',
    '5.0.0': '2.7.2',
    '5.0.3': '2.7.3',
}

# need to fill in a version for non-Hadoop applications
DUMMY_APPLICATION_VERSION = '0.0.0'


### Errors ###

def err_xml(message, type='Sender', code='ValidationError'):
    """Use this to create the body of boto response errors."""
    return """\
<ErrorResponse xmlns="http://elasticmapreduce.amazonaws.com/doc/2009-03-31">
  <Error>
    <Type>%s</Type>
    <Code>%s</Code>
    <Message>%s</Message>
  </Error>
  <RequestId>eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee</RequestId>
</ErrorResponse>""" % (type, code, message)


### Test Case ###

class MockBotoTestCase(SandboxedTestCase):

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

        super(MockBotoTestCase, self).setUp()

        # patch slow things
        def fake_create_mrjob_zip(mocked_self, *args, **kwargs):
            mocked_self._mrjob_zip_path = self.fake_mrjob_zip_path
            return self.fake_mrjob_zip_path

        self.start(patch.object(
            EMRJobRunner, '_create_mrjob_zip',
            fake_create_mrjob_zip))

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


### S3 ###

def add_mock_s3_data(mock_s3_fs, data, age=None, location=None):
    """Update mock_s3_fs with a map from bucket name to key name to data.

    :param age: a timedelta
    :param location string: the bucket's location cosntraint (a region name)
    """
    age = age or timedelta(0)
    time_modified = _boto3_now() - age

    for bucket_name, key_name_to_bytes in data.items():
        bucket = mock_s3_fs.setdefault(bucket_name,
                                       {'keys': {}, 'location': ''})

        for key_name, key_data in key_name_to_bytes.items():
            if not isinstance(key_data, bytes):
                raise TypeError('mock s3 data must be bytes')
            bucket['keys'][key_name] = (key_data, time_modified)

        if location is not None:
            bucket['location'] = location


def _invalid_request_error(operation_name, message):
    # boto3 reports this as a botocore.exceptions.InvalidRequestException,
    # but that's not something you can actually import
    return AttributeError(
        'An error occurred (InvalidRequestException) when calling the'
        ' %s operation: %s' % (operation_name, message))


def _no_such_bucket_error(bucket_name, operation_name):
    return ClientError(
        dict(
            Error=dict(
                Bucket=bucket_name,
                Code='NoSuchBucket',
                Message='The specified bucket does not exist',
            ),
            ResponseMetadata=dict(
                HTTPStatusCode=404
            ),
        ),
        operation_name)


def _no_such_key_error(key_name, operation_name):
    return ClientError(
        dict(
            Error=dict(
                Code='NoSuchKey',
                Key=key_name,
                Message='The specified key does not exist',
            ),
            ResponseMetadata=dict(
                HTTPStatusCode=404,
            ),
        ),
        operation_name,
    )


def _validation_error(operation_name, message):
    return ClientError(
        dict(
            Error=dict(
                Code='ValidationException',
                Message=message,
            ),
            ResponseMetadata=dict(
                HTTPStatusCode=404,
            ),
        ),
        operation_name,
    )

def _check_param_type(value, type_or_types):
    """quick way to raise a boto3 ParamValidationError. We don't bother
    constructing the text of the ParamValidationError."""
    if not isinstance(value, type_or_types):
        raise ParamValidationError


class MockS3Client(object):
    """Mock out boto3 S3 client

    :param mock_s3_fs: Maps bucket name to a dictionary with the keys *keys*
                       and *location*. *keys* maps key name to tuples of
                       ``(data, time_modified)``. *data* is bytes, and
                        *time_modified* is a UTC
                        :py:class:`~datetime.datetime`. *location* is an
                        optional location constraint for the bucket
                        (a region name).
    """
    def __init__(self,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 aws_session_token=None,
                 endpoint_url=None,
                 region_name=None,
                 mock_s3_fs=None):

        self.mock_s3_fs = mock_s3_fs

        region_name = region_name or _DEFAULT_AWS_REGION
        if not endpoint_url:
            if region_name == _DEFAULT_AWS_REGION:
                endpoint_url = 'https://s3.amazonaws.com'
            else:
                endpoint_url = 'https://s3-%s.amazonaws.com' % region_name

        self.meta = MockObject(
            endpoint_url=endpoint_url,
            region_name=region_name)

    def _check_bucket_exists(self, bucket_name, operation_name):
        if bucket_name not in self.mock_s3_fs:
            raise _no_such_bucket_error(bucket_name, operation_name)

    def create_bucket(self, Bucket, CreateBucketConfiguration=None):
        # boto3 doesn't seem to mind if you try to create a bucket that exists
        if Bucket not in self.mock_s3_fs:
            location = (CreateBucketConfiguration or {}).get(
                'LocationConstraint', '')
            self.mock_s3_fs[Bucket] = dict(keys={}, location=location)

        # "Location" here actually refers to the bucket name
        return dict(Location=('/' + Bucket))

    def get_bucket_location(self, Bucket):
        self._check_bucket_exists(Bucket, 'GetBucketLocation')

        location_constraint = self.mock_s3_fs[Bucket].get('location') or None

        return dict(LocationConstraint=location_constraint)


class MockS3Resource(object):
    """Mock out boto3 S3 resource"""
    def __init__(self,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 aws_session_token=None,
                 endpoint_url=None,
                 region_name=None,
                 mock_s3_fs=None):

        self.mock_s3_fs = mock_s3_fs

        self.meta = MockObject(
            client=MockS3Client(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                endpoint_url=endpoint_url,
                region_name=region_name,
                mock_s3_fs=mock_s3_fs
            )
        )

        self.buckets = MockObject(
            all=self._buckets_all,
        )

    def Bucket(self, name):
        # boto3's Bucket() doesn't care if the bucket exists
        return MockS3Bucket(self.meta.client, name)

    def _buckets_all(self):
        # technically, this only lists buckets we own, but our mock fs
        # doesn't simulate buckets owned by others
        for bucket_name in sorted(self.meta.client.mock_s3_fs):
            yield self.Bucket(bucket_name)


class MockS3Bucket(object):
    """Mock out boto3 bucket
    """
    def __init__(self, client, name):
        """Create a mock bucket with the given name and client
        """
        self.name = name
        self.meta = MockObject(client=client)

        self.objects = MockObject(
            all=self._objects_all,
            filter=self._objects_filter)

    def Object(self, key):
        return MockS3Object(self.meta.client, self.name, key)

    def _objects_all(self):
        return self._objects_filter()

    def _objects_filter(self, Prefix=None):
        self._check_bucket_exists('ListObjects')

        # there are several other keyword arguments that we don't support
        mock_s3_fs = self.meta.client.mock_s3_fs

        for key in sorted(mock_s3_fs[self.name]['keys']):
            if Prefix and not key.startswith(Prefix):
                continue

            key = self.Object(key)
            # emulate ObjectSummary by pre-filling size, e_tag, etc.
            key.get()
            yield key

    def _check_bucket_exists(self, operation_name):
        if self.name not in self.meta.client.mock_s3_fs:
            raise _no_such_bucket_error(self.name, operation_name)


class MockS3Object(object):
    """Mock out s3.Object"""

    def __init__(self, client, bucket_name, key):
        self.bucket_name = bucket_name
        self.key = key

        self.meta = MockObject(client=client)

    def delete(self):
        mock_keys = self._mock_bucket_keys('DeleteObject')

        # okay if key doesn't exist
        if self.key in mock_keys:
            del mock_keys[self.key]

        return {}

    def get(self):
        key_data, mtime = self._get_key_data_and_mtime()

        # fill in known attributes
        m = hashlib.md5()
        m.update(key_data)

        self.e_tag = '"%s"' % m.hexdigest()
        self.last_modified = mtime
        self.size = len(key_data)

        return dict(
            Body=MockStreamingBody(key_data),
            ContentLength=self.size,
            ETag=self.e_tag,
            LastModified=self.last_modified,
        )

    def put(self, Body):
        if not isinstance(Body, bytes):
            raise NotImplementedError('mock put() only support bytes')

        mock_keys = self._mock_bucket_keys('PutObject')

        if isinstance(Body, bytes):
            data = Body
        elif hasattr(Body, 'read'):
            data = Body.read()

        if not isinstance(data, bytes):
            raise TypeError('Body or Body.read() must be bytes')

        mock_keys[self.key] = (data, _boto3_now())

    def upload_file(self, path, Config=None):
        if self.bucket_name not in self.meta.client.mock_s3_fs:
            # upload_file() is a higher-order operation, has fancy errors
            raise S3UploadFailedError(
                'Failed to upload %s to %s/%s: %s' % (
                    path, self.bucket_name, self.key,
                    str(_no_such_bucket_error('PutObject'))))

        mock_keys = self._mock_bucket_keys('PutObject')
        with open(path, 'rb') as f:
            mock_keys[self.key] = (f.read(), _boto3_now())

    def __getattr__(self, key):
        if key in ('e_tag', 'last_modified', 'size'):
            try:
                self.get()
            except ClientError:
                pass

        if hasattr(self, key):
            return getattr(self, key)
        else:
            raise AttributeError(
                "'s3.Object' object has no attribute '%s'" % key)

    def _mock_bucket_keys(self, operation_name):
        self._check_bucket_exists(operation_name)

        return self.meta.client.mock_s3_fs[self.bucket_name]['keys']

    def _check_bucket_exists(self, operation_name):
        if self.bucket_name not in self.meta.client.mock_s3_fs:
            raise _no_such_bucket_error(self.bucket_name, operation_name)

    def _get_key_data_and_mtime(self):
        """Return (key_data, time_modified)."""
        mock_keys = self._mock_bucket_keys('GetBucket')

        if self.key not in mock_keys:
            raise _no_such_key_error(self.key, 'GetObject')

        return mock_keys[self.key]


class MockStreamingBody(object):
    """Mock of boto3's not-really-a-fileobj for reading from S3"""

    def __init__(self, data):
        if not isinstance(data, bytes):
            raise TypeError

        self._data = data
        self._offset = 0

    def read(self, amt=None):
        start = self._offset

        if amt is None:
            end = len(self._data)
        else:
            end = start + amt

        self._offset = end
        return self._data[start:end]


### EMR ###

class MockEMRClient(object):
    """Mock out boto3 EMR clients. This actually handles a small
    state machine that simulates EMR clusters."""

    def __init__(self,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 aws_session_token=None,
                 endpoint_url=None,
                 region_name=None,
                 mock_s3_fs=None,
                 mock_emr_clusters=None,
                 mock_emr_failures=None,
                 mock_emr_self_termination=None,
                 mock_emr_output=None,
                 max_clusters_returned=DEFAULT_MAX_CLUSTERS_RETURNED,
                 max_steps_returned=DEFAULT_MAX_STEPS_RETURNED):
        """Create a mock version boto3 EMR clients.

        By default, jobs will run to conclusion, and if their output dir
        is on S3, create a single empty output file. You can manually
        decide that some jobs will fail, or give them different output
        by setting mock_emr_failures/mock_emr_output.

        Clusters are given IDs j-MOCKCLUSTER0, j-MOCKCLUSTER1, etc.
        Step numbers are 0-indexed.

        Extra args:
        :param mock_s3_fs: a mock S3 filesystem to point to (usually you just
                            want to use an empty dictionary).
        :param mock_emr_clusters: map from cluster ID to an EMRObject, in the
                                  format returned by describe_cluster(), plus
                                 ``_bootstrapactions``, ``_instancegroups``,
                                 and ``_steps`` fields.
        :param mock_emr_failures: a set of ``(cluster ID, step_num)`` for steps
                                  that should fail.
        :param mock_emr_self_termination: a set of cluster IDs that should
                                          simulate master node termination
                                          once cluster is up
        :param mock_emr_output: a map from ``(cluster ID, step_num)`` to a
                                list of ``str``s representing file contents to
                                output when the job completes
        :type max_clusters_returned: int
        :param max_clusters_returned: the maximum number of clusters that
                                       :py:meth:`list_clusters` can return,
                                       to simulate a real limitation of EMR
        :type max_steps_returned: int
        :param max_steps_returned: the maximum number of clusters that
                                   :py:meth:`list_steps` can return,
                                   to simulate a real limitation of EMR
        :type max_days_ago: int
        :param max_days_ago: the maximum amount of days that EMR will go back
                             in time
        """
        # check this now; strs will cause problems later in Python 3
        if mock_emr_output and any(
                any(not isinstance(part, bytes) for part in parts)
                for parts in mock_emr_output.values()):
            raise TypeError('mock EMR output must be bytes')

        self.mock_s3_fs = combine_values({}, mock_s3_fs)
        self.mock_emr_clusters = combine_values({}, mock_emr_clusters)
        self.mock_emr_failures = combine_values(set(), mock_emr_failures)
        self.mock_emr_self_termination = combine_values(
            set(), mock_emr_self_termination)
        self.mock_emr_output = combine_values({}, mock_emr_output)
        self.max_clusters_returned = max_clusters_returned
        self.max_steps_returned = max_steps_returned

        region_name = region_name or _DEFAULT_AWS_REGION
        if not endpoint_url:
            # not entirely sure why boto3 1.4.4 uses a different format for
            # us-east-1, but there it is. according to AWS docs, the canonical
            # host name is elasticmapreduce.<region>.amazonaws.com.
            if endpoint_url == _DEFAULT_AWS_REGION:
                endpoint_url = (
                    'https://elasticmapreduce.us-east-1.amazonaws.com')
            else:
                endpoint_url = (
                    'https://%s.elasticmapreduce.amazonaws.com' % region_name)

        self.meta = MockObject(
            endpoint_url=endpoint_url,
            region_name=region_name)

    def run_job_flow(self, **kwargs):
        # going to pop params from kwargs as we process then, and raise
        # NotImplementedError at the end if any params are left
        now = kwargs.pop('_Now', _boto3_now())

        # our newly created cluster, as described by describe_cluster(), plus:
        #
        # _BootstrapActions: as described by list_bootstrap_actions()
        # _InstanceGroups: as described by list_instance_groups()
        # _Steps: as decribed by list_steps(), but not reversed
        #
        # TODO: at some point when we implement instance fleets,
        # _InstanceGroups will become optional
        #
        # TODO: update simulate_progress() to add MasterPublicDnsName
        cluster = dict(
            _BootstrapActions=[],
            _InstanceGroups=[],
            _Steps=[],
            Applications=[],
            AutoTerminate=True,
            Configurations=[],
            Ec2InstanceAttributes=dict(
                EmrManagedMasterSecurityGroup='sg-mockmaster',
                EmrManagedSlaveSecurityGroup='sg-mockslave',
                IamInstanceProfile='',
            ),
            Id='',
            Name='',
            NormalizedInstanceHours=0,
            ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
            ServiceRole='',
            Status=dict(
                State='STARTING',
                StateChangeReason={},
                Timeline=dict(CreationDateTime=now),
            ),
            Tags=[],
            TerminationProtected=False,
            VisibleToAllUsers=False,
        )

        def _error(message):
            return _validation_error('RunJobFlow', message)

        # Id
        cluster['Id'] = (
            kwargs.pop('_Id', None) or
            'j-MOCKCLUSTER%d' % len (self.mock_emr_clusters))

        # Name (required)
        _check_param_type(kwargs.get('Name'), string_types)
        cluster['Name'] = kwargs.pop('Name', None)

        # JobFlowRole and ServiceRole (required)
        _check_param_type(kwargs.get('JobFlowRole'), string_types)
        cluster['Ec2InstanceAttributes']['IamInstanceProfile'] = kwargs.pop(
            'JobFlowRole')

        if 'ServiceRole' not in kwargs:  # required by API, not boto3
            raise _error('ServiceRole is required for creating cluster.')
        _check_param_type(kwargs['ServiceRole'], string_types)
        cluster['ServiceRole'] = kwargs.pop('ServiceRole')

        # AmiVersion and ReleaseLabel
        for version_param in ('AmiVersion', 'ReleaseLabel'):
            if version_param in kwargs:
                _check_param_type(kwargs[version_param], string_types)

        if 'AmiVersion' in kwargs:
            if 'ReleaseLabel' in kwargs:
                raise _error(
                    'Only one AMI version and release label may be specified.'
                    ' Provided AMI: %s, release label: %s.' % (
                        kwargs['AmiVersion'], kwargs['ReleaseLabel']))

            AmiVersion = kwargs.pop('AmiVersion')

            running_ami_version = AMI_VERSION_ALIASES.get(
                AmiVersion, AmiVersion)

            if version_gte(running_ami_version, '4'):
                raise _error('The supplied ami version is invalid.')
            elif not version_gte(running_ami_version, '2'):
                raise _error(
                    'Job flow role is not compatible with the supplied'
                    ' AMI version')

            cluster['RequestedAmiVersion'] = AmiVersion
            cluster['RunningAmiVersion'] = running_ami_version

        elif 'ReleaseLabel' in kwargs:
            ReleaseLabel = kwargs.pop('ReleaseLabel')
            running_ami_version = ReleaseLabel.lstrip('emr-')

            if not version_gte(running_ami_version, '4'):
                raise _error('The supplied release label is invalid: %s.' %
                             ReleaseLabel)

            cluster['ReleaseLabel'] = ReleaseLabel
        else:
            # note: you can't actually set Hadoop version through boto3
            raise _error(
                'Must specify exactly one of the following:'
                ' release label, AMI version, or Hadoop version.')

        # Applications
        hadoop_version = map_version(
            running_ami_version, AMI_HADOOP_VERSION_UPDATES)

        if version_get(running_ami_version, '4'):
            application_names = set(
                a['Name'] for a in kwargs.get('Applications', []))

            # if Applications is set but doesn't include Hadoop, the
            # cluster description won't either! (Even though Hadoop is
            # in fact installed.)
            if not application_names:
                application_names = set(['Hadoop'])

            for app_name in sorted(application_names):
                if app_name == 'Hadoop':
                    version = hadoop_version
                else:
                    version = DUMMY_APPLICATION_VERSION

                cluster['Applications'].append(
                    dict(Name=app_name, Version=version))
        else:
             if kwargs.get('Applications'):
                raise _error(
                    'Cannot specify applications when AMI version is used.'
                    ' Specify supported products or new supported products'
                    ' instead.')

             # 'hadoop' is lowercase if AmiVersion specified
             cluster['Applications'].append(
                 dict(Name='hadoop', Version=hadoop_version))

        # Configurations
        if 'Configurations' in kwargs:
            cluster['Configurations'] = normalized_configurations(
                kwargs.pop('Configurations'))

        # VisibleToAllUsers
        if 'VisibleToAllUsers' in kwargs:
            _check_param_type(kwargs['VisibleToAllUsers'], bool)
            cluster['VisibleToAllUsers'] = kwargs.pop('VisibleToAllUsers')

        # pass BootstrapActions off to helper
        if 'BootstrapActions' in kwargs:
            self._add_bootstrap_actions(
                'RunJobFlow', kwargs.pop('BootstrapActions'), cluster)

        # pass Instances (required) off to helper
        if 'Instances' not in kwargs:
            raise ParamValidationError
        self._add_instances('RunJobFlow', kwargs.pop('Instances'), cluster)

        # pass Steps off to helper
        if 'Steps' in kwargs:
            self._add_steps('RunJobFlow', kwargs.pop('Steps'), cluster)

        # pass Tags off to helper
        if 'Tags' in kwargs:
            self._add_tags('RunJobFlow', kwargs.pop('Tags'), cluster)

        # catch extra params
        if kwargs:
            raise NotImplementedError(
                'mock RunJobFlow does not support these parameters: %s' %
                ', '.join(sorted(kwargs)))

        self.mock_emr_clusters[cluster['Id']] = cluster

        return dict(JobFlowId=cluster['Id'])

    # helper methods for run_job_flow()

    def _add_bootstrap_actions(
            self, operation_name, BootstrapActions, cluster):
        """Handle BootstrapActions param from run_job_flow().

        (there isn't any other way to add bootstrap actions)
        """
        _check_param_type(BootstrapActions, list)

        operation_name  # currently unused, quiet pyflakes

        new_actions = []  # don't update _BootstrapActions if there's an error

        for ba in BootstrapActions:
            _check_param_type(ba, dict)
            _check_param_type(ba.get('Name'), string_types)
            _check_param_type(ba.get('ScriptBootstrapAction', dict))
            _check_param_type(ba['ScriptBootstrapAction'].get('Path'),
                              string_types)

            args = []
            if 'Args' in ba['ScriptBootstrapAction']:
                _check_param_type(ba['ScriptBootstrapAction']['Args'], list)
                for arg in ba['ScriptBootstrapAction']['Args']:
                    _check_param_type(arg, string_types)
                    args.append(arg)

            new_actions.append(dict(
                Name=ba['Name'],
                ScriptPath=ba['ScriptBootstrapAction']['Path'],
                Args=args))

        cluster['_BootstrapActions'].extend(new_actions)

    def _add_instances(self, operation_name, Instances, cluster):
        """Handle Instances param from run_job_flow()"""
        _check_param_type(Instances, dict)

        Instances = dict(Instances)  # going to pop params from Instances

        def _error(message):
            return _validation_error(operation_name, message)

        # Ec2KeyName
        if 'Ec2KeyName' in Instances:
            _check_param_type(Instances['Ec2KeyName'], string_types)
            cluster['Ec2InstanceAttributes']['Ec2KeyName'] = Instances.pop(
                'Ec2KeyName')

        # Ec2SubnetId
        if 'Ec2SubnetId' in Instances:
            _check_param_type(Instances['Ec2SubnetId'], string_types)
            cluster['Ec2InstanceAttributes']['Ec2SubnetId'] = (
                Instances['Ec2SubnetId'])

        # KeepJobFlowAliveWhenNoSteps
        if 'KeepJobFlowAliveWhenNoSteps' in Instances:
            _check_param_type(Instances['KeepJobFlowAliveWhenNoSteps'], bool)
            cluster['AutoTerminate'] = (
                not Instances['KeepJobFlowAliveWhenNoSteps'])

        # Placement (availability zone)
        if 'Placement' in Instances:
            Placement = Instances.pop('Placement')
            if not isinstance(Placement, dict):
                raise ParamValidationError

            # mockboto doesn't support the 'AvailabilityZones' param
            if not isinstance(Placement.get('AvailabilityZone'), string_types):
                raise ParamValidationError

            cluster['Ec2AvailabilityZone'] = Placement['AvailabilityZone']

        if 'InstanceGroups' in Instances:
            if any(x in Instances for x in ('MasterInstanceType',
                                            'SlaveInstanceType',
                                            'InstanceCount')):
                raise _error(
                    'Please configure instances using one and only one of the'
                    ' following: instance groups; instance fleets; instance'
                    ' count, master and slave instance type.')

            self._add_instance_groups(
                operation_name, Instances.pop('InstanceGroups'), cluster)
        # TODO: will need to support instance fleets at some point
        else:
            # build our own instance groups
            instance_groups = []

            instance_count = Instances.pop('InstanceCount', 0)
            _check_param_type(instance_count, integer_types)

            # note: boto3 actually lets 'null' fall through to the API here
            _check_param_type(
                Instances.get('MasterInstanceType'), string_types)
            instance_groups.append(dict(
                InstanceRole='MASTER',
                InstanceType=Instances.pop('MasterInstanceType'),
                InstanceCount=1))

            if 'SlaveInstanceType' in Instances:
                _check_param_type(
                    Instances.get('SlaveInstanceType'), string_types)
                instance_groups.append(dict(
                    InstanceRole='CORE',
                    InstanceType=Instances.pop('SlaveInstanceType'),
                    InstanceCount=instance_count - 1))

            self._add_instance_groups(
                operation_name, instance_groups, cluster, now=now)

        if kwargs:
            raise NotImplementedError(
                'mock %s does not support these parameters: %s' % (
                    operation_name,
                    ', '.join('Instances.%s' % k for k in sorted(kwargs))))

    def _add_instance_groups(self, operation_name, InstanceGroups, cluster,
                             now=None):
        """Add instance groups from *InstanceGroups* to the mock
        cluster *cluster*.
        """
        _check_param_type(InstanceGroups, list)

        def _error(message):
            return _validation_error(operation_name, message)

        if now is None:
            now = _boto3_now()

        # currently, this is just a helper method for run_job_flow()
        if cluster.get('_InstanceGroups'):
            raise NotImplementedError(
                "mockboto doesn't support adding instance groups")

        new_igs = []  # don't update _InstanceGroups if there's an error

        roles = set()  # roles already handled

        for i, InstanceGroup in enumerate(InstanceGroups):
            _check_param_type(InstanceGroup, dict)
            InstanceGroup = dict(InstanceGroup)

            # our new mock instance group
            ig = dict(
                Configurations=[],
                EbsBlockDevices=[],
                Id='ig-FAKE',
                InstanceGroupType='',
                Market='ON_DEMAND',
                RequestedInstanceCount=0,
                RunningInstanceCount=1,
                ShrinkPolicy={},
                Status=dict(
                    State='PROVISIONING',
                    StateChangeReason=dict(Message=''),
                    Timeline=dict(CreationDateTime=now),
                ),
            )

            # InstanceRole (required)
            _check_param_type(InstanceGroup.get('InstanceRole', string_types))
            role = InstanceGroup.pop('InstanceRole')

            # bad role type
            if role not in ('MASTER', 'CORE', 'TASK'):
                raise _error(
                    "1 validation error detected: value '%s' at"
                    " 'instances.instanceGroups.%d.member.instanceRole' failed"
                    " to satify constraint: Member must satisfy enum value"
                    " set: [MASTER, TASK, CORE]" % (role, i + 1))

            # check for duplicate roles
            if role in roles:
                raise _error(
                    'Multiple %s instance groups supplied, you'
                    ' must specify exactly one %s instance group' %
                    (role.lower(), role.lower()))
            roles.add(role)

            ig['InstanceGroupType'] = role

            # InstanceType (required)
            _check_param_type(InstanceGroup.get('InstanceType', string_types))

            # 3.x AMIs (but not 4.x, etc.) reject m1.small explicitly
            if (InstanceGroup.get('InstanceType') == 'm1.small' and
                    cluster.get('RunningAmiVersion', '').startswith('3.')):
                raise _error(
                    'm1.small instance type is not supported with AMI version'
                    ' %s.' % cluster['RunningAmiVersion'])

            ig['InstanceType'] = InstanceGroup.pop('InstanceType')

            # InstanceCount (required)
            _check_param_type(InstanceGroup.get('InstanceCount'),
                              integer_types)
            InstanceCount = InstanceGroup.pop('InstanceCount')
            if InstanceCount < 1:
                raise _error(
                    'An instance group must have at least one instance')

            if role == 'MASTER' and InstanceCount != 1:
                raise _error(
                    'A master instance group must specify a single instance')
            ig['InstanceCount'] = InstanceCount

            # Name
            if 'Name' in InstanceGroup:
                _check_param_type(InstanceGroup['Name'], string_types)
                ig['Name'] = InstanceGroup.pop('Name')

            # Market (default set above)
            if 'Market' in InstanceGroup:
                _check_param_type(InstanceGroup['Market'], string_types)
                if InstanceGroup['Market'] not in ('ON_DEMAND', 'SPOT'):
                    raise _error(
                    "1 validation error detected: value '%s' at"
                    " 'instances.instanceGroups.%d.member.market' failed"
                    " to satify constraint: Member must satisfy enum value"
                    " set: [SPOT, ON_DEMAND]" % (role, i + 1))
                ig['Market'] = InstanceGroup.pop('Market')

            # BidPrice
            if 'BidPrice' in InstanceGroup:
                # not float, surprisingly
                _check_param_type(InstanceGroup['BidPrice'], string_types)

                if ig['Market'] != 'SPOT':
                    raise _error('Attempted to set bid price for on demand'
                                 ' instance group.')

                # simulate bid price validation
                try:
                    if not float(bid_price) > 0:
                        raise _error('The bid price is negative or zero.')
                except (TypeError, ValueError):
                    raise _error(
                        'The bid price supplied for an instance group is'
                        ' invalid')

                if '.' in bid_price and len(bid_price.split('.', 1)[1]) > 3:
                    raise _error('No more than 3 digits are allowed after'
                                 ' decimal place in bid price')

                ig['BidPrice'] = InstanceGroup.pop('BidPrice')

            if kwargs:
                raise NotImplementedError(
                    'mockboto does not support these InstanceGroup'
                    ' params: %s' % ', '.join(sorted(kwargs)))

            new_igs.append(ig)

        # TASK roles require CORE roles (to host HDFS)
        if 'TASK' in roles and 'CORE' not in roles:
            raise _error(
                'Clusters with task nodes must also define core nodes.')

        # MASTER role is required
        if 'MASTER' not in roles:
            raise _error('Zero master instance groups supplied, you must'
                         ' specify exactly one master instance group')

        cluster['_InstanceGroups'].extend(new_igs)

    def _add_steps(self, operation_name, Steps, cluster, now=None):
        if now is None:
            now = _boto3_now()

        if not isinstance(Steps, list):
            raise ValidationError

        new_steps = []

        # TODO: reject if more than 256 steps added at once?

        for i, Step in enumerate(Steps):
            Step = dict(Step)

            new_step = dict(
                ActionOnFailure='TERMINATE_CLUSTER',
                Config=dict(
                    Args=[],
                    Jar={},
                    Properties={},
                ),
                Id='s-MOCKSTEP%d' % (len(cluster['_Steps']) + 1),
                Name='',
                Status=dict(
                    State='PENDING',
                    Timeline=dict(CreationDateTime=now),
                ),
            )

            # Name (required)
            _check_param_type(Step.get('Name'), string_types)
            new_step['Name'] = Step.pop('Name')

            # HadoopJarStep (required)
            _check_param_type(Step.get('HadoopJarStep'), dict)
            HadoopJarStep = dict(Step.pop('HadoopJarStep'))

            _check_param_type(HadoopJarStep.get('Jar', string_types))
            new_step['Config']['Jar'] = HadoopJarStep.pop('Jar')

            if 'Args' in HadoopJarStep:
                Args = HadoopJarStep.pop('Args')
                _check_param_type(Args, list)
                for arg in Args:
                    _check_param_type(arg, string_types)
                new_step['Config']['Args'].extend(Args)

            if 'MainClass' in HadoopJarStep:
                _check_param_type(HadoopJarStep['MainClass'], string_types)
                new_step['Config']['MainClass'] = HadoopJarStep.pop(
                    'MainClass')

            # don't currently support Properties
            if HadoopJarStep:
                raise NotImplementedError(
                    "mockboto doesn't support these HadoopJarStep params: %s" %
                    ', '.join(sorted(HadoopJarStep)))

            if Step:
                raise NotImplementedError(
                    "mockboto doesn't support these step params: %s" %
                    ', '.join(sorted(Step)))

        cluster['_Steps'].extend(new_steps)

        # add_job_flow_steps() needs to return step IDs
        return [new_step['Id'] for new_step in new_steps]

    def _add_tags(self, operation_name, Tags, cluster):
        if not isinstance(Tags, list):
            raise ValidationError

        new_tags = {}

        for Tag in Tags:
            _check_param_type(Tag, dict)
            if set(Tag) > set('Key', 'Value'):
                raise ParamValidationError

            Key = Tag.get('Key')
            if not Key or not 1 <= len(Key) <= 128:
                raise _invalid_request_error(
                    operation_name,
                    "Invalid tag key: '%s'. Tag keys must be between 1 and 128"
                    " characters in length." %
                    ('null' if Key is None else Key))

            Value = Tag.get('Value') or ''
            if not 0 <= len(Value) <= 256:
                raise _invalid_request_error(
                    operation_name,
                    "Invalid tag value: '%s'. Tag values must be between 1 and"
                    " 128 characters in length." % Value)

            new_tags[Key] = Value

        tags_dict = dict((t['Key'], t['Value']) for t in cluster['_Tags'])
        tags_dict.update(new_tags)

        cluster['_Tags'] = [
            dict(Key=k, Value=v) for k, v in sorted(tags_dict.items())]

    def _get_mock_cluster(self, cluster_id):
        if not cluster_id in self.mock_emr_clusters:
            raise boto.exception.S3ResponseError(404, 'Not Found')

        return self.mock_emr_clusters[cluster_id]

    def add_jobflow_steps(self, jobflow_id, steps, now=None):
        self._enforce_strict_ssl()

        if now is None:
            now = _boto3_now()

        cluster = self._get_mock_cluster(jobflow_id)

        for step in steps:
            step_config = MockEmrObject(
                args=[MockEmrObject(value=a) for a in step.args()],
                jar=step.jar(),
                mainclass=step.main_class())
            # there's also a "properties" field, but boto doesn't handle it

            step_status = MockEmrObject(
                state='PENDING',
                timeline=MockEmrObject(
                    creationdatetime=to_iso8601(now)),
            )

            cluster._steps.append(MockEmrObject(
                actiononfailure=step.action_on_failure,
                config=step_config,
                id=('s-MOCKSTEP%d' % len(cluster._steps)),
                name=step.name,
                status=step_status,
            ))

    def add_tags(self, resource_id, tags):
        """Simulate successful creation of new metadata tags for the specified
        resource id.
        """
        self._enforce_strict_ssl()

        cluster = self._get_mock_cluster(resource_id)

        if not tags:
            raise boto.exception.EmrResponseError(
                400, 'Bad Request', body=err_xml(
                    'Tags cannot be null or empty.',
                    code='InvalidRequestException'))

        for key, value in sorted(tags.items()):
            value = value or ''

            for tag_obj in cluster.tags:
                if tag_obj.key == key:
                    tag_obj.value == value
                    break
            else:
                cluster.tags.append(MockEmrObject(
                    key=key, value=value))

        return True

    def describe_cluster(self, cluster_id):
        self._enforce_strict_ssl()

        cluster = self._get_mock_cluster(cluster_id)

        if cluster.status.state == 'TERMINATING':
            # simulate progress, to support
            # _wait_for_logs_on_s3()
            self.simulate_progress(cluster_id)

        return cluster

    def describe_step(self, cluster_id, step_id):
        self._enforce_strict_ssl()

        # simulate progress, to support _wait_for_steps_to_complete()
        self.simulate_progress(cluster_id)

        # make sure that we only call list_steps() when we've patched
        # around https://github.com/boto/boto/issues/3268
        if 'StartDateTime' not in boto.emr.emrobject.ClusterTimeline.Fields:
            raise Exception('called un-patched version of describe_step()!')

        cluster = self._get_mock_cluster(cluster_id)

        for step in cluster._steps:
            if step.id == step_id:
                return step

        raise boto.exception.EmrResponseError(
            400, 'Bad Request', body=err_xml(
                "Step id '%s' is not valid." % step_id))

    def list_bootstrap_actions(self, cluster_id, marker=None):
        self._enforce_strict_ssl()

        if marker is not None:
            raise NotImplementedError(
                'marker not simulated for ListBootstrapActions')

        cluster = self._get_mock_cluster(cluster_id)

        return MockEmrObject(actions=cluster._bootstrapactions)

    def list_clusters(self, created_after=None, created_before=None,
                      cluster_states=None, marker=None):
        self._enforce_strict_ssl()

        # summaries of cluster state, to return
        cluster_summaries = []

        # add markers to clusters
        marked_clusters = [
            ((cluster.status.timeline.creationdatetime, cluster.id), cluster)
            for cluster in self.mock_emr_clusters.values()]
        marked_clusters.sort(reverse=True)

        # *marker* is just the marker for the last cluster we returned

        for cluster_marker, cluster in marked_clusters:
            if marker is not None and cluster_marker <= marker:
                continue

            # stop if we hit pagination limit
            if len(cluster_summaries) >= self.max_clusters_returned:
                break

            created = cluster.status.timeline.creationdatetime

            if created_after is not None and created < created_after:
                continue

            if created_before is not None and created > created_before:
                continue

            state = cluster.status.state

            if not (cluster_states is None or state in cluster_states):
                continue

            cluster_summaries.append(MockEmrObject(
                id=cluster.id,
                name=cluster.name,
                normalizedinstancehours=cluster.normalizedinstancehours,
                status=cluster.status))
        else:
            # we went through all clusters, no need to call again
            cluster_marker = None

        return MockEmrObject(clusters=cluster_summaries, marker=cluster_marker)

    def list_instances(self, cluster_id, instance_group_id=None,
                       instance_group_types=None, marker=None):
        """stripped-down simulation of list_instances() to support
        SSH tunneling; only includes state.status and the privateipaddress
        field.
        """
        self._enforce_strict_ssl()

        if instance_group_id is not None:
            raise NotImplementedError(
                'instance_group_id not simulated for ListInstances')

        if marker is not None:
            raise NotImplementedError(
                'marker not simulated for ListInstances')

        cluster = self._get_mock_cluster(cluster_id)

        instances = []

        for i, ig in enumerate(cluster._instancegroups):
            if instance_group_types and (
                    ig.instancegrouptype not in instance_group_types):
                continue

            for j in range(int(ig.requestedinstancecount)):
                instance = MockEmrObject()

                state = ig.status.state

                instance.status = MockEmrObject(state=state)

                if state not in ('PROVISIONING', 'AWAITING_FULLFILLMENT'):
                    # this is just an easy way to assign a unique IP
                    instance.privateipaddress = '172.172.%d.%d' % (
                        i + 1, j + 1)

                instances.append(instance)

        return MockEmrObject(instances=instances)

    def list_instance_groups(self, cluster_id, marker=None):
        self._enforce_strict_ssl()

        # TODO: not sure what order API returns instance groups in,
        # but doesn't matter for us, as our code treats them like
        # a dictionary. See #1316.

        if marker is not None:
            raise NotImplementedError(
                'marker not simulated for ListInstanceGroups')

        cluster = self._get_mock_cluster(cluster_id)

        return MockEmrObject(instancegroups=cluster._instancegroups)

    def list_steps(self, cluster_id, step_states=None, marker=None):
        self._enforce_strict_ssl()

        # make sure that we only call list_steps() when we've patched
        # around https://github.com/boto/boto/issues/3268
        if 'StartDateTime' not in boto.emr.emrobject.ClusterTimeline.Fields:
            raise Exception('called un-patched version of list_steps()!')

        steps = self._get_mock_cluster(cluster_id)._steps

        steps_listed = []

        # *marker* was the index of the last step we listed
        for index in reversed(range(marker or len(steps))):
            step = steps[index]
            if step_states is None or step.status.state in step_states:
                steps_listed.append(step)

            if len(steps_listed) >= self.max_steps_returned:
                break
        else:
            index = None  # listed all steps, no need to call again

        return MockEmrObject(steps=steps_listed, marker=index)

    def terminate_jobflow(self, jobflow_id):
        self._enforce_strict_ssl()

        cluster = self._get_mock_cluster(jobflow_id)

        # already terminated
        if cluster.status.state in (
                'TERMINATED', 'TERMINATED_WITH_ERRORS'):
            return

        # mark cluster as shutting down
        cluster.status.state = 'TERMINATING'
        cluster.status.statechangereason = MockEmrObject(
            code='USER_REQUEST',
            message='Terminated by user request',
        )

        for step in cluster._steps:
            if step.status.state == 'PENDING':
                step.status.state = 'CANCELLED'
            elif step.status.state == 'RUNNING':
                # pretty sure this is what INTERRUPTED is for
                step.status.state = 'INTERRUPTED'

    def _get_step_output_uri(self, step_args):
        """Figure out the output dir for a step by parsing step.args
        and looking for an -output argument."""
        # parse in reverse order, in case there are multiple -output args
        for i, arg in reversed(list(enumerate(step_args[:-1]))):
            if arg.value == '-output':
                return step_args[i + 1].value
        else:
            return None

    def simulate_progress(self, cluster_id, now=None):
        """Simulate progress on the given cluster. This is automatically
        run when we call :py:meth:`describe_step`, and, when the cluster is
        ``TERMINATING``, :py:meth:`describe_cluster`.

        :type cluster_id: str
        :param cluster_id: fake cluster ID
        :type now: py:class:`datetime.datetime`
        :param now: alternate time to use as the current time (should be UTC)
        """
        # TODO: this doesn't actually update steps to CANCELLED when
        # cluster is shut down
        if now is None:
            now = _boto3_now()

        cluster = self._get_mock_cluster(cluster_id)

        # allow clusters to get stuck
        if getattr(cluster, 'delay_progress_simulation', 0) > 0:
            cluster.delay_progress_simulation -= 1
            return

        # this code is pretty loose about updating statechangereason
        # (for the cluster, instance groups, and steps). Add this as needed.

        # if job is STARTING, move it along to BOOTSTRAPPING
        if cluster.status.state == 'STARTING':
            cluster.status.state = 'BOOTSTRAPPING'
            # instances are now provisioned
            for ig in cluster._instancegroups:
                ig.runninginstancecount = ig.requestedinstancecount,
                ig.status.state = 'BOOTSTRAPPING'

            return

        # if job is TERMINATING, move along to terminated
        if cluster.status.state == 'TERMINATING':
            code = getattr(getattr(cluster.status, 'statechangereason', None),
                           'code', None)

            if code and code.endswith('_FAILURE'):
                cluster.status.state = 'TERMINATED_WITH_ERRORS'
            else:
                cluster.status.state = 'TERMINATED'

            return

        # if job is done, nothing to do
        if cluster.status.state in ('TERMINATED', 'TERMINATED_WITH_ERRORS'):
            return

        # if job is BOOTSTRAPPING, move it along to RUNNING and continue
        if cluster.status.state == 'BOOTSTRAPPING':
            cluster.status.state = 'RUNNING'
            for ig in cluster._instancegroups:
                ig.status.state = 'RUNNING'

        # at this point, should be RUNNING or WAITING
        assert cluster.status.state in ('RUNNING', 'WAITING')

        # simulate self-termination
        if cluster_id in self.mock_emr_self_termination:
            cluster.status.state = 'TERMINATING'
            cluster.status.statechangereason = MockEmrObject(
                code='INSTANCE_FAILURE',
                message='The master node was terminated. ',  # sic
            )

            for step in cluster._steps:
                if step.status.state in ('PENDING', 'RUNNING'):
                    step.status.state = 'CANCELLED'  # not INTERRUPTED

            return

        # try to find the next step, and advance it

        for step_num, step in enumerate(cluster._steps):
            # skip steps that are already done
            if step.status.state in (
                    'COMPLETED', 'FAILED', 'CANCELLED', 'INTERRUPTED'):
                continue

            # found currently running step! handle it, then exit

            # start PENDING step
            if step.status.state == 'PENDING':
                step.status.state = 'RUNNING'
                step.status.timeline.startdatetime = to_iso8601(now)
                return

            assert step.status.state == 'RUNNING'

            # check if we're supposed to have an error
            if (cluster_id, step_num) in self.mock_emr_failures:
                step.status.state = 'FAILED'

                if step.actiononfailure in (
                        'TERMINATE_CLUSTER', 'TERMINATE_JOB_FLOW'):

                    cluster.status.state = 'TERMINATING'
                    cluster.status.statechangereason.code = 'STEP_FAILURE'
                    cluster.status.statechangereason.message = (
                        'Shut down as step failed')

                return

            # complete step
            step.status.state = 'COMPLETED'
            step.status.timeline.enddatetime = to_iso8601(now)

            # create fake output if we're supposed to write to S3
            output_uri = self._get_step_output_uri(step.config.args)
            if output_uri and is_s3_uri(output_uri):
                mock_output = self.mock_emr_output.get(
                    (cluster_id, step_num)) or [b'']

                bucket_name, key_name = parse_s3_uri(output_uri)

                # write output to S3
                for i, part in enumerate(mock_output):
                    add_mock_s3_data(self.mock_s3_fs, {
                        bucket_name: {key_name + 'part-%05d' % i: part}})
            elif (cluster_id, step_num) in self.mock_emr_output:
                raise AssertionError(
                    "can't use output for cluster ID %s, step %d "
                    "(it doesn't output to S3)" %
                    (cluster_id, step_num))

            # done!
            # if this is the last step, continue to autotermination code, below
            if step_num < len(cluster._steps) - 1:
                return

        # no pending steps. should we wait, or shut down?
        if cluster.autoterminate == 'true':
            cluster.status.state = 'TERMINATING'
            cluster.status.statechangereason.code = 'ALL_STEPS_COMPLETED'
            cluster.status.statechangereason.message = (
                'Steps Completed')
        else:
            # just wait
            cluster.status.state = 'WAITING'
            cluster.status.statechangereason = MockEmrObject()

        return


class MockObject(object):
    """Mock out boto.emr.EmrObject. This is just a generic object that you
    can set any attribute on."""

    def __init__(self, **kwargs):
        """Intialize with the given attributes, ignoring fields set to None."""
        for key, value in kwargs.items():
            if value is not None:
                setattr(self, key, value)

    def __setattr__(self, key, value):
        if isinstance(value, bytes):
            value = value.decode('utf_8')

        self.__dict__[key] = value

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        if len(self.__dict__) != len(other.__dict__):
            return False

        for k, v in self.__dict__.items():
            if not k in other.__dict__:
                return False
            else:
                if v != other.__dict__[k]:
                    return False

        return True

    # useful for hand-debugging tests
    def __repr__(self):
        return('%s.%s(%s)' % (
            self.__class__.__module__,
            self.__class__.__name__,
            ', '.join('%s=%r' % (k, v)
                      for k, v in sorted(self.__dict__.items()))))

# old name for this
MockEmrObject = MockObject

class MockPaginator(object):
    """Mock botocore paginators.

    Rather than mocking pagination, markers, etc. in every mock API call,
    we have our API calls return the full results, and make our paginators
    break them into pages.
    """
    def __init__(self, method, result_key, page_size):
        self.result_key = result_key
        self.method = method
        self.page_size = page_size

    def paginate(self, **kwargs):
        result = self.method(**kwargs)

        values = result[self.result_key]

        for page_start in range(0, len(values), self.page_size):
            page = values[page_start:page_start + self.page_size]
            yield combine_dicts(result, {self.result_key: page})


class MockIAMClient(object):

    DEFAULT_PATH = '/'

    DEFAULT_MAX_ITEMS = 100

    OPERATION_NAME_TO_RESULT_KEY = dict(
        list_attached_role_policies='AttachedPolicies',
        list_instance_profiles='InstanceProfiles',
        list_roles='Roles',
    )

    def __init__(self, region_name=None, api_version=None,
                 use_ssl=True, verify=None, endpoint_url=None,
                 aws_access_key_id=None, aws_secret_access_key=None,
                 aws_session_token=None, config=None,
                 mock_iam_instance_profiles=None, mock_iam_roles=None,
                 mock_iam_role_attached_policies=None):
        """Mock out connection to IAM.

        mock_iam_instance_profiles maps profile name to a dict containing:
            create_date -- ISO creation datetime
            path -- IAM path
            role_name -- name of single role for this instance profile, or None

        mock_iam_roles maps role name to a dict containing:
            assume_role_policy_document -- a JSON-then-URI-encoded policy doc
            create_date -- ISO creation datetime
            path -- IAM path

        mock_iam_role_attached_policies maps role to a list of ARNs for
        attached (managed) policies.

        We don't track which managed policies exist or what their contents are.
        We also don't support role IDs.
        """
        # if not passed dictionaries for these mock values, create our own
        self.mock_iam_instance_profiles = combine_values(
            {}, mock_iam_instance_profiles)
        self.mock_iam_roles = combine_values({}, mock_iam_roles)
        self.mock_iam_role_attached_policies = combine_values(
            {}, mock_iam_role_attached_policies)

        endpoint_url = endpoint_url or 'https://iam.amazonaws.com'
        region_name = region_name or 'aws-global'

        self.meta = MockObject(
            endpoint_url=endpoint_url,
            region_name=region_name)

    def get_paginator(self, operation_name):
        return MockPaginator(
            getattr(self, operation_name),
            self.OPERATION_NAME_TO_RESULT_KEY[operation_name],
            self.DEFAULT_MAX_ITEMS)

    # roles

    def create_role(self, AssumeRolePolicyDocument, RoleName):
        # Path not supported
        # mock RoleIds are all the same

        self._check_role_does_not_exist(RoleName, 'CreateRole')

        role = dict(
            Arn=('arn:aws:iam::012345678901:role/%s' % RoleName),
            AssumeRolePolicyDocument=json.loads(AssumeRolePolicyDocument),
            CreateDate=_boto3_now(),
            Path='/',
            RoleId='AROAMOCKMOCKMOCKMOCK',
            RoleName=RoleName,
        )
        self.mock_iam_roles[RoleName] = role

        return dict(Role=role)

    def list_roles(self):
        # PathPrefix not supported

        roles = list(data for name, data in
                     sorted(self.mock_iam_roles.items()))

        return dict(Roles=roles)

    def _check_role_does_not_exist(self, RoleName, OperationName):
        if RoleName in self.mock_iam_roles:
            raise ClientError(
                dict(Error=dict(
                    Code='EntityAlreadyExists',
                    Message=('Role with name %s already exists' % RoleName))),
                OperationName)

    def _check_role_exists(self, RoleName, OperationName):
        if RoleName not in self.mock_iam_roles:
            raise ClientError(
                dict(Error=dict(
                    Code='NoSuchEntity',
                    Message=('Role not found for %s' % RoleName))),
                OperationName)

    # attached role policies

    def attach_role_policy(self, PolicyArn, RoleName):
        self._check_role_exists(RoleName, 'AttachRolePolicy')

        arns = self.mock_iam_role_attached_policies.setdefault(RoleName, [])
        if PolicyArn not in arns:
            arns.append(PolicyArn)

        return {}

    def list_attached_role_policies(self, RoleName):
        self._check_role_exists(RoleName, 'ListAttachedRolePolicies')

        arns = self.mock_iam_role_attached_policies.get(RoleName, [])

        return dict(AttachedPolicies=[
            dict(PolicyArn=arn, PolicyName=arn.split('/')[-1])
            for arn in arns
        ])

    # instance profiles

    def create_instance_profile(self, InstanceProfileName):
        # Path not implemented
        # mock InstanceProfileIds are all the same

        self._check_instance_profile_does_not_exist(InstanceProfileName,
                                                    'CreateInstanceProfile')

        profile = dict(
            Arn=('arn:aws:iam::012345678901:instance-profile/%s' %
                 InstanceProfileName),
            CreateDate=_boto3_now(),
            InstanceProfileId='AIPAMOCKMOCKMOCKMOCK',
            InstanceProfileName=InstanceProfileName,
            Path='/',
            Roles=[],
        )
        self.mock_iam_instance_profiles[InstanceProfileName] = profile

        return dict(InstanceProfile=profile)

    def add_role_to_instance_profile(self, InstanceProfileName, RoleName):
        self._check_role_exists(RoleName, 'AddRoleToInstanceProfile')
        self._check_instance_profile_exists(
            InstanceProfileName, 'AddRoleToInstanceProfile')

        profile = self.mock_iam_instance_profiles[InstanceProfileName]
        if profile['Roles']:
            raise LimitExceededException(
                'An error occurred (LimitExceeded) when calling the'
                ' AddRoleToInstanceProfile operation: Cannot exceed quota for'
                ' InstanceSessionsPerInstanceProfile: 1')

        # just point straight at the mock role; never going to redefine it
        profile['Roles'] = [self.mock_iam_roles[RoleName]]

    def list_instance_profiles(self):
        # PathPrefix not implemented

        profiles = list(data for name, data in
                        sorted(self.mock_iam_instance_profiles.items()))

        return dict(InstanceProfiles=profiles)

    def _check_instance_profile_does_not_exist(
            self, InstanceProfileName, OperationName):

        if InstanceProfileName in self.mock_iam_instance_profiles:
            raise ClientError(
                dict(Error=dict(
                    Code='EntityAlreadyExists',
                    Message=('Instance Profile %s already exists' %
                             InstanceProfileName))),
                OperationName)

    def _check_instance_profile_exists(
            self, InstanceProfileName, OperationName):

        if InstanceProfileName not in self.mock_iam_instance_profiles:
            raise ClientError(
                dict(Error=dict(
                    Code='NoSuchEntity',
                    Message=('Instance Profile %s cannot be found' %
                             InstanceProfileName))),
                OperationName)


def _normalized_configurations(configurations):
    """The API will return an empty Properties list for configurations
    without properties set, and remove empty sub-configurations"""
    if not isinstance(configurations, list):
        raise ParamValidationError

    results = []

    for c in configurations:
        if not isinstance(c, dict):
            raise ParamValidationError
        c = dict(c)  # so we can modify it

        c.setdefault('properties', [])

        if 'configurations' in c:
            if c['configurations']:
                c['configurations'] = _normalized_configurations(
                    c['configurations'])
            else:
                del c['configurations']
