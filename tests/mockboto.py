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

This is by no means a complete mock of boto, just what we need for tests.
"""
import hashlib
import itertools
import os
import shutil
import tempfile
import time
from datetime import datetime
from io import BytesIO

try:
    import boto.emr.connection
    from boto.emr.instance_group import InstanceGroup
    from boto.emr.step import JarStep
    import boto.exception
    import boto.utils
    boto  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    boto = None

from mrjob.compat import map_version
from mrjob.compat import version_gte
from mrjob.conf import combine_values
from mrjob.emr import _EMR_LOG_DIR
from mrjob.emr import EMRJobRunner
from mrjob.parse import _RFC1123
from mrjob.parse import is_s3_uri
from mrjob.parse import parse_s3_uri

from tests.mockssh import create_mock_ssh_script
from tests.mockssh import mock_ssh_dir
from tests.mr_two_step_job import MRTwoStepJob
from tests.py2 import MagicMock
from tests.py2 import patch
from tests.sandbox import SandboxedTestCase

# list_clusters() only returns this many results at a time
DEFAULT_MAX_CLUSTERS_RETURNED = 50

# list_steps() only returns this many results at a time
DEFAULT_MAX_STEPS_RETURNED = 50

# Size of each chunk returned by the MockKey iterator
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

# hard-coded EmrConnection.DebuggingJar from boto 2.39.0. boto 2.40.0 more
# correctly uses a template with the correct region (see #1306), but
# mockboto's EMR stuff doesn't have any other reason to be region-aware.
DEBUGGING_JAR = (
    's3n://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar')

# likewise, this is EmrConnection.DebuggingArgs from boto 2.39.0
DEBUGGING_ARGS = (
    's3n://us-east-1.elasticmapreduce/libs/state-pusher/0.1/fetch')

# extra step to use when debugging_step=True is passed to run_jobflow()
DEBUGGING_STEP = JarStep(
    name='Setup Hadoop Debugging',
    action_on_failure='TERMINATE_CLUSTER',
    main_class=None,
    jar=DEBUGGING_JAR,
    step_args=DEBUGGING_ARGS)


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

    # if a test needs to create an EMR connection more than this many
    # times, there's probably a problem with simulating progress
    MAX_EMR_CONNECTIONS = 100

    def setUp(self):
        # patch boto
        self.mock_emr_failures = set()
        self.mock_emr_self_termination = set()
        self.mock_emr_clusters = {}
        self.mock_emr_output = {}
        self.mock_iam_instance_profiles = {}
        self.mock_iam_role_attached_policies = {}
        self.mock_iam_roles = {}
        self.mock_s3_fs = {}

        self.emr_conn_iterator = itertools.repeat(
            None, self.MAX_EMR_CONNECTIONS)

        self.start(patch.object(boto, 'connect_s3', self.connect_s3))
        self.start(patch.object(boto, 'connect_iam', self.connect_iam))
        self.start(patch.object(
            boto.emr.connection, 'EmrConnection', self.connect_emr))

        super(MockBotoTestCase, self).setUp()

        # patch slow things
        self.mrjob_zip_path = None

        def fake_create_mrjob_zip(runner, *args, **kwargs):
            if not self.mrjob_zip_path:
                self.mrjob_zip_path = self.makefile('fake_mrjob.zip')

            runner._mrjob_zip_path = self.mrjob_zip_path
            return self.mrjob_zip_path

        self.start(patch.object(
            EMRJobRunner, '_create_mrjob_zip',
            fake_create_mrjob_zip))

        self.start(patch.object(time, 'sleep'))

    def add_mock_s3_data(self, data, time_modified=None, location=None):
        """Update self.mock_s3_fs with a map from bucket name
        to key name to data."""
        add_mock_s3_data(self.mock_s3_fs, data, time_modified, location)

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

    # TODO: this should be replaced once we get rid of ssh_slave_hosts()
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

    def connect_s3(self, *args, **kwargs):
        kwargs['mock_s3_fs'] = self.mock_s3_fs
        return MockS3Connection(*args, **kwargs)

    def connect_emr(self, *args, **kwargs):
        try:
            next(self.emr_conn_iterator)
        except StopIteration:
            raise AssertionError(
                'Too many connections to mock EMR, may be stalled')

        kwargs['mock_s3_fs'] = self.mock_s3_fs
        kwargs['mock_emr_clusters'] = self.mock_emr_clusters
        kwargs['mock_emr_failures'] = self.mock_emr_failures
        kwargs['mock_emr_self_termination'] = self.mock_emr_self_termination
        kwargs['mock_emr_output'] = self.mock_emr_output
        return MockEmrConnection(*args, **kwargs)

    def connect_iam(self, *args, **kwargs):
        kwargs['mock_iam_instance_profiles'] = self.mock_iam_instance_profiles
        kwargs['mock_iam_roles'] = self.mock_iam_roles
        kwargs['mock_iam_role_attached_policies'] = (
            self.mock_iam_role_attached_policies)
        return MockIAMConnection(*args, **kwargs)


### S3 ###

def add_mock_s3_data(mock_s3_fs, data, time_modified=None, location=None):
    """Update mock_s3_fs (which is just a dictionary mapping bucket to
    key to contents) with a map from bucket name to key name to data and
    time last modified."""
    if time_modified is None:
        time_modified = datetime.utcnow()
    for bucket_name, key_name_to_bytes in data.items():
        bucket = mock_s3_fs.setdefault(bucket_name,
                                       {'keys': {}, 'location': ''})

        for key_name, key_data in key_name_to_bytes.items():
            if not isinstance(key_data, bytes):
                raise TypeError('mock s3 data must be bytes')
            bucket['keys'][key_name] = (key_data, time_modified)

        if location is not None:
            bucket['location'] = location


class MockS3Connection(object):
    """Mock out boto.s3.Connection
    """
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 is_secure=True, port=None, proxy=None, proxy_port=None,
                 proxy_user=None, proxy_pass=None,
                 host=None, debug=0, https_connection_factory=None,
                 calling_format=None, path='/', provider='aws',
                 bucket_class=None, mock_s3_fs=None, security_token=None):
        """Mock out a connection to S3. Most of these args are the same
        as for the real S3Connection, and are ignored.

        You can set up a mock filesystem to share with other objects
        by specifying mock_s3_fs, which is a map from bucket name to
        a dictionary with fields 'location' (bucket location constraint,
        a string) and 'keys' (a map from key to (bytes, time_modified).
        """
        # use mock_s3_fs even if it's {}
        self.mock_s3_fs = combine_values({}, mock_s3_fs)
        self.host = host or 's3.amazonaws.com'

    def _region(self):
        """Infer region from self.host. Return '' if on regionless
        endpoint."""
        return self.host.split('.')[0][3:]

    def get_bucket(self, bucket_name, validate=True, headers=None):
        if bucket_name in self.mock_s3_fs:
            # can't access buckets through wrong region's endpoint
            region = self._region()
            if region and self.mock_s3_fs[bucket_name]['location'] != region:
                raise boto.exception.S3ResponseError(301, 'Moved Permanently')

            return MockBucket(connection=self, name=bucket_name)
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')

    def get_all_buckets(self):
        return [self.get_bucket(name) for name in self.mock_s3_fs]

    def create_bucket(self, bucket_name, headers=None, location='',
                      policy=None):
        if bucket_name in self.mock_s3_fs:
            raise boto.exception.S3CreateError(409, 'Conflict')

        # for region endpoints, location constraint must match
        region = self._region()
        if region and region != location:
            raise boto.exception.S3CreateError(409, 'Bad Request')

        self.mock_s3_fs[bucket_name] = {'keys': {}, 'location': location}


class MockBucket(object):
    """Mock out boto.s3.Bucket
    """
    def __init__(self, connection=None, name=None, location=None):
        """You can optionally specify a 'data' argument, which will instantiate
        mock keys and mock data. data should be a map from key name to bytes
        and time last modified.
        """
        self.name = name
        self.connection = connection

    def mock_state(self):
        """Returns a dictionary from key to data representing the
        state of this bucket."""
        if self.name in self.connection.mock_s3_fs:
            return self.connection.mock_s3_fs[self.name]['keys']
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')

    def new_key(self, key_name):
        if key_name not in self.mock_state():
            self.mock_state()[key_name] = (
                b'', to_iso8601(datetime.utcnow()))
        return MockKey(bucket=self, name=key_name)

    def get_key(self, key_name):
        if key_name in self.mock_state():
            return MockKey(bucket=self, name=key_name, date_to_str=to_rfc1123)
        else:
            return None

    def get_location(self):
        return self.connection.mock_s3_fs[self.name]['location']

    def list(self, prefix=''):
        for key_name in sorted(self.mock_state()):
            if key_name.startswith(prefix):
                yield MockKey(bucket=self, name=key_name,
                              date_to_str=to_iso8601)

    def initiate_multipart_upload(self, key_name):
        key = self.new_key(key_name)
        return MockMultiPartUpload(key)


class MockKey(object):
    """Mock out boto.s3.Key"""

    def __init__(self, bucket=None, name=None, date_to_str=None):
        self.bucket = bucket
        self.name = name
        self.date_to_str = date_to_str or to_iso8601
        # position in data, for read() and next()
        self._pos = 0

    def read_mock_data(self):
        """Read the bytes for this key out of the fake boto state."""
        if self.name in self.bucket.mock_state():
            return self.bucket.mock_state()[self.name][0]
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')

    def mock_multipart_upload_was_cancelled(self):
        return isinstance(self.read_mock_data(), MultiPartUploadCancelled)

    def write_mock_data(self, data):
        # real boto automatically UTF-8 encodes unicode, but mrjob should
        # always pass bytes
        if not isinstance(data, bytes):
            #data = data.encode('utf_8')
            raise TypeError('mock s3 data must be bytes')

        if self.name in self.bucket.mock_state():
            self.bucket.mock_state()[self.name] = (data, datetime.utcnow())
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')

    def get_contents_to_filename(self, path, headers=None):
        with open(path, 'wb') as f:
            f.write(self.read_mock_data())

    def set_contents_from_filename(self, path):
        with open(path, 'rb') as f:
            self.write_mock_data(f.read())

    def get_contents_as_string(self):
        return self.read_mock_data()

    def set_contents_from_string(self, string):
        self.write_mock_data(string)

    def delete(self):
        if self.name in self.bucket.mock_state():
            del self.bucket.mock_state()[self.name]
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')

    def make_public(self):
        pass

    def read(self, size=None):
        data = self.read_mock_data()
        if size is None or size < 0:
            chunk = data[self._pos:]
        else:
            chunk = data[self._pos:self._pos + size]
        self._pos += len(chunk)
        return chunk

    # need this for Python 2
    def next(self):
        return self.__next__()

    def __next__(self):
        chunk = self.read(SIMULATED_BUFFER_SIZE)
        if chunk:
            return chunk
        else:
            raise StopIteration

    def __iter__(self):
        return self

    def _get_last_modified(self):
        if self.name in self.bucket.mock_state():
            return self.date_to_str(self.bucket.mock_state()[self.name][1])
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')

    # option to change last_modified time for testing purposes
    def _set_last_modified(self, time_modified):
        if self.name in self.bucket.mock_state():
            data = self.bucket.mock_state()[self.name][0]
            self.bucket.mock_state()[self.name] = (data, time_modified)
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')

    last_modified = property(_get_last_modified, _set_last_modified)

    def _get_etag(self):
        m = hashlib.md5()
        m.update(self.get_contents_as_string())
        return m.hexdigest()

    etag = property(_get_etag)

    @property
    def size(self):
        return len(self.get_contents_as_string())


class MultiPartUploadCancelled(bytes):
    """Thin wrapper for key data, to mark that multipart upload
    to this key was cancelled."""
    pass


class MockMultiPartUpload(object):

    def __init__(self, key):
        """Mock out boto.s3.MultiPartUpload

        Note that real MultiPartUpload objects don't actually know which key
        they're associated with. It's just simpler this way.
        """
        self.key = key
        self.parts = {}

    def upload_part_from_file(self, fp, part_num):
        part_num = int(part_num)  # boto leaves this to a format string

        # this check is actually in boto
        if part_num < 1:
            raise ValueError('Part numbers must be greater than zero')

        self.parts[part_num] = fp.read()

    def complete_upload(self):
        data = b''

        if self.parts:
            num_parts = max(self.parts)
            for part_num in range(1, num_parts + 1):
                # S3 might be more graceful about missing parts. But we
                # certainly don't want this to slip past testing
                data += self.parts[part_num]

        self.key.set_contents_from_string(data)

    def cancel_upload(self):
        self.parts = None  # should break any further calls

        # record that multipart upload was cancelled
        cancelled = MultiPartUploadCancelled(self.key.get_contents_as_string())
        self.key.set_contents_from_string(cancelled)


### EMR ###

def to_iso8601(when):
    """Convert a datetime to ISO8601 format.
    """
    return when.strftime(boto.utils.ISO8601_MS)


def to_rfc1123(when):
    """Convert a datetime to RFC1123 format.
    """
    # AWS sends us a time zone in all cases, but in Python it's more
    # annoying to figure out time zones, so just fake it.
    assert when.tzinfo is None
    return when.strftime(_RFC1123) + 'GMT'


class MockEmrConnection(object):
    """Mock out boto.emr.EmrConnection. This actually handles a small
    state machine that simulates EMR clusters."""

    # hook for simulating SSL cert errors. To use this, do:
    #
    # with patch.object(MockEmrConnection, 'STRICT_SSL', True):
    #     ...
    STRICT_SSL = False

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 is_secure=True, port=None, proxy=None, proxy_port=None,
                 proxy_user=None, proxy_pass=None, debug=0,
                 https_connection_factory=None, region=None,
                 security_token=None,
                 mock_s3_fs=None, mock_emr_clusters=None,
                 mock_emr_failures=None,
                 mock_emr_self_termination=None,
                 mock_emr_output=None,
                 max_clusters_returned=DEFAULT_MAX_CLUSTERS_RETURNED,
                 max_steps_returned=DEFAULT_MAX_STEPS_RETURNED):
        """Create a mock version of EmrConnection. Most of these args are
        the same as for the real EmrConnection, and are ignored.

        By default, jobs will run to conclusion, and if their output dir
        is on S3, create a single empty output file. You can manually
        decide that some jobs will fail, or give them different output
        by setting mock_emr_failures/mock_emr_output.

        Clusters are given IDs j-MOCKCLUSTER0, j-MOCKCLUSTER1, etc.
        Step numbers are 0-indexed.

        Extra args:
        :param mock_s3_fs: a mock S3 filesystem to point to. See
                           :py:meth:`MockS3Connection.__init__`
                           for format (usually you just want to use an empty
                           dictionary).
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

        if region is not None:
            self.host = region.endpoint
        else:
            self.host = 'elasticmapreduce.amazonaws.com'

    def _enforce_strict_ssl(self):
        if (self.STRICT_SSL and
                not self.host.endswith('elasticmapreduce.amazonaws.com')):
            from boto.https_connection import InvalidCertificateException
            raise InvalidCertificateException(
                self.host, None, 'hostname mismatch')

    # TODO: *now* is not a param to the real run_jobflow(), rename to _now
    def run_jobflow(self,
                    name, log_uri=None, ec2_keyname=None,
                    availability_zone=None,
                    master_instance_type='m1.small',
                    slave_instance_type='m1.small', num_instances=1,
                    action_on_failure='TERMINATE_CLUSTER', keep_alive=False,
                    enable_debugging=False,
                    hadoop_version=None,
                    steps=None,
                    bootstrap_actions=[],
                    instance_groups=None,
                    additional_info=None,
                    ami_version=None,
                    now=None,
                    api_params=None,
                    visible_to_all_users=None,
                    job_flow_role=None,
                    service_role=None,
                    _id=None):
        """Mock of run_jobflow().
        """
        self._enforce_strict_ssl()

        if now is None:
            now = datetime.utcnow()

        # convert api_params into MockEmrObject
        api_params_obj = _api_params_to_emr_object(api_params or {})

        # default fields that can be set from api_params
        if job_flow_role is None:
            job_flow_role = (api_params or {}).get('JobFlowRole')

        if job_flow_role is None:
            raise boto.exception.EmrResponseError(
                400, 'Bad Request', body=err_xml(
                    'InstanceProfile is required for creating cluster'))

        if service_role is None:
            service_role = (api_params or {}).get('ServiceRole')

        if service_role is None:
            raise boto.exception.EmrResponseError(
                400, 'Bad Request', body=err_xml(
                    'ServiceRole is required for creating cluster'))

        if visible_to_all_users is None:
            if api_params and 'VisibleToAllUsers' in api_params:
                if api_params['VisibleToAllUsers'] not in ('true', 'false'):
                    raise boto.exception.EmrResponseError(
                        400, 'Bad Request', err_xml(
                            'boolean must follow xsd1.1 definition',
                            code='MalformedInput'))
                visible_to_all_users = (
                    api_params['VisibleToAllUsers'] == 'true')

        release_label = None
        if api_params and api_params.get('ReleaseLabel'):
            release_label = api_params['ReleaseLabel']

        if release_label and ami_version:
            raise boto.exception.EmrResponseError(
                400, 'Bad Request', body=err_xml(
                    'Only one AMI version and release label may be specified.'
                    ' Provided AMI: %s, release label: %s.' % (
                        ami_version, release_label)))

        # API no longer allows you to explicitly specify 1.x versions
        if ami_version and ami_version.startswith('1.'):
            raise boto.exception.EmrResponseError(
                400, 'Bad Request', body=err_xml(
                    'Job flow role is not compatible with the supplied'
                    ' AMI version'))

        # figure out actual ami version (ami_version) and params for
        # 2.x and 3.x clusters (requested_ami_version,
        # running_ami_version)
        if release_label:
            ami_version = release_label.lstrip('emr-')
            requested_ami_version = None
            running_ami_version = None
        else:
            requested_ami_version = ami_version
            # translate "latest" and None
            ami_version = AMI_VERSION_ALIASES.get(ami_version, ami_version)
            running_ami_version = ami_version

        # determine hadoop version
        running_hadoop_version = map_version(
            ami_version, AMI_HADOOP_VERSION_UPDATES)

        # if hadoop_version is set, it should match AMI version
        # (this is probably no longer relevant to mrjob)
        if not (hadoop_version is None or
                hadoop_version == running_hadoop_version):
            raise boto.exception.EmrResponseError(
                400, 'Bad Request', body=err_xml(
                    'The requested AMI version does not support the requested'
                    ' Hadoop version'))

        # Applications
        application_objs = getattr(api_params_obj, 'applications', [])

        if version_gte(ami_version, '4'):
            application_names = set(a.name for a in application_objs)

            # if Applications is set but doesn't include Hadoop, the
            # cluster description won't either! (Even though Hadoop is
            # in fact installed.)
            if not application_names:
                application_names = set(['Hadoop'])

            applications = []
            for app_name in sorted(application_names):
                if app_name == 'Hadoop':
                    version = running_hadoop_version
                else:
                    version = DUMMY_APPLICATION_VERSION

                applications.append(
                    MockEmrObject(name=app_name, version=version))
        else:
            if application_objs:
                raise boto.exception.EmrResponseError(
                    400, 'Bad Request', body=err_xml(
                        'Cannot specify applications when AMI version is used.'
                        ' Specify supported products or new supported products'
                        ' instead.'))

            applications = [MockEmrObject(
                name='hadoop',  # lowercase on older AMIs
                version=running_hadoop_version
            )]

        # Configurations
        if hasattr(api_params_obj, 'configurations'):
            configurations = api_params_obj.configurations
            _normalize_configuration_objs(configurations)
        else:
            configurations = None

        if configurations and not version_gte(ami_version, '4'):
            raise boto.exception.EmrResponseError(
                400, 'Bad Request', body=err_xml(
                    'Cannot specify configurations when AMI version is used.'))

        # optional subnet (we don't do anything with this other than put it
        # in ec2instanceattributes)
        ec2_subnet_id = (api_params or {}).get('Instances.Ec2SubnetId')

        # create a MockEmrObject corresponding to the job flow. We only
        # need to fill in the fields that EMRJobRunner uses
        steps = steps or []

        cluster_id = _id or 'j-MOCKCLUSTER%d' % len(self.mock_emr_clusters)
        assert cluster_id not in self.mock_emr_clusters

        cluster = MockEmrObject(
            applications=applications,
            autoterminate=('false' if keep_alive else 'true'),
            configurations=configurations,
            ec2instanceattributes=MockEmrObject(
                ec2availabilityzone=availability_zone,
                ec2keyname=ec2_keyname,
                ec2subnetid=ec2_subnet_id,
                iaminstanceprofile=job_flow_role,
            ),
            id=cluster_id,
            loguri=log_uri,
            # TODO: set this later, once cluster is running
            masterpublicdnsname='mockmaster%d' % len(self.mock_emr_clusters),
            name=name,
            normalizedinstancehours='0',
            releaselabel=release_label,
            requestedamiversion=requested_ami_version,
            runningamiversion=running_ami_version,
            servicerole=service_role,
            status=MockEmrObject(
                state='STARTING',
                statechangereason=MockEmrObject(),
                timeline=MockEmrObject(
                    creationdatetime=to_iso8601(now)),
            ),
            tags=[],
            terminationprotected='false',
            visibletoallusers=('true' if visible_to_all_users else 'false')
        )

        # this other information we want to store about the cluster doesn't
        # actually get returned by DescribeCluster, so we keep it in
        # "hidden" fields.

        # need api_params for testing purposes
        cluster._api_params = api_params

        # bootstrap actions
        cluster._bootstrapactions = self._build_bootstrap_actions(
            bootstrap_actions)

        # instance groups
        if instance_groups:
            cluster._instancegroups = (
                self._build_instance_groups_from_list(instance_groups))
        else:
            cluster._instancegroups = (
                self._build_instance_groups_from_type_and_count(
                    master_instance_type, slave_instance_type, num_instances))

        # 3.x AMIs don't support m1.small
        if version_gte(ami_version, '3') and any(
                ig.instancetype == 'm1.small'
                for ig in cluster._instancegroups):
            raise boto.exception.EmrResponseError(
                400, 'Bad Request', body=err_xml(
                    'm1.small instance type is not supported with AMI'
                    ' version %s' % ami_version))

        # will handle steps arg in a moment
        cluster._steps = []

        self.mock_emr_clusters[cluster_id] = cluster

        # use add_jobflow_steps() to handle steps
        steps = list(steps or ())
        if enable_debugging:
            steps = [DEBUGGING_STEP] + steps

        self.add_jobflow_steps(cluster_id, steps, now=now)

        return cluster_id

    def _build_bootstrap_actions(self, bootstrap_actions):
        return [
            MockEmrObject(name=action.name,
                          scriptpath=action.path,
                          args=[MockEmrObject(value=str(v)) for v
                                in action.bootstrap_action_args])
            for action in bootstrap_actions
        ]

    def _build_instance_groups_from_list(self, instance_groups):
        mock_groups = []
        roles = set()

        for instance_group in instance_groups:
            # check num_instances
            if instance_group.num_instances < 1:
                raise boto.exception.EmrResponseError(
                    400, 'Bad Request', body=err_xml(
                        'An instance group must have at least one instance'))

            emr_group = MockEmrObject(
                id='ig-FAKE',
                instancegrouptype=instance_group.role,
                instancetype=instance_group.type,
                market=instance_group.market,
                name=instance_group.name,
                requestedinstancecount=str(instance_group.num_instances),
                runninginstancecount='0',
                status=MockEmrObject(state='PROVISIONING'),
            )

            if instance_group.market == 'SPOT':
                bid_price = instance_group.bidprice

                # simulate EMR's bid price validation
                try:
                    float(bid_price)
                except (TypeError, ValueError):
                    raise boto.exception.EmrResponseError(
                        400, 'Bad Request', body=err_xml(
                            'The bid price supplied for an instance group is'
                            ' invalid'))

                if '.' in bid_price and len(bid_price.split('.', 1)[1]) > 3:
                    raise boto.exception.EmrResponseError(
                        400, 'Bad Request', body=err_xml(
                            'No more than 3 digits are allowed after decimal'
                            ' place in bid price'))

                emr_group.bidprice = bid_price

            # check for duplicate role
            if instance_group.role in roles:
                role_desc = instance_group.role.lower()
                raise boto.exception.EmrResponseError(
                    400, 'Bad Request', body=err_xml(
                        'Multiple %s instance groups supplied, you'
                        ' must specify exactly one %s instance group' %
                        (role_desc, role_desc)))

            roles.add(instance_group.role)

            # check for multiple master instances
            if instance_group.role == 'MASTER':
                if instance_group.num_instances != 1:
                    raise boto.exception.EmrResponseError(
                        400, 'Bad Request', body=err_xml(
                            'A master instance group must specify a single'
                            ' instance'))

            # add mock instance group
            mock_groups.append(emr_group)

        # TASK roles require CORE roles (to host HDFS)
        if 'TASK' in roles and 'CORE' not in roles:
            raise boto.exception.EmrResponseError(
                400, 'Bad Request', body=err_xml(
                    'Clusters with task nodes must also define core'
                    ' nodes.'))

        # MASTER role is required
        if 'MASTER' not in roles:
            raise boto.exception.EmrResponseError(
                400, 'Bad Request', body=err_xml(
                    'Zero master instance groups supplied, you must'
                    ' specify exactly one master instance group'))

        # Done!
        return mock_groups

    def _build_instance_groups_from_type_and_count(
            self, master_instance_type, slave_instance_type, num_instances):

        # going to pass this to _build_instance_groups_from_list()
        instance_groups = []

        instance_groups.append(
            InstanceGroup(num_instances=1,
                          role='MASTER',
                          type=master_instance_type,
                          market='ON_DEMAND',
                          name='master'))

        if num_instances > 1:
            instance_groups.append(
                InstanceGroup(num_instances=(num_instances - 1),
                              role='CORE',
                              type=slave_instance_type,
                              market='ON_DEMAND',
                              name='core'))

        return self._build_instance_groups_from_list(instance_groups)

    def _get_mock_cluster(self, cluster_id):
        if not cluster_id in self.mock_emr_clusters:
            raise boto.exception.S3ResponseError(404, 'Not Found')

        return self.mock_emr_clusters[cluster_id]

    def add_jobflow_steps(self, jobflow_id, steps, now=None):
        self._enforce_strict_ssl()

        if now is None:
            now = datetime.utcnow()

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

        # termination protected clusters may not be terminated
        if getattr(cluster, '_TerminationProtected', False):
            raise boto.exception.EmrResponseError(
                400, 'Bad Request', body=err_xml(
                    'Could not shut down one or more job flows since they are'
                    ' termination protected.'))

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
            now = datetime.utcnow()

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


class MockEmrObject(object):
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


class MockIAMConnection(object):

    DEFAULT_PATH = '/'

    DEFAULT_MAX_ITEMS = 100

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 is_secure=True, port=None, proxy=None, proxy_port=None,
                 proxy_user=None, proxy_pass=None, host='iam.amazonaws.com',
                 debug=0, https_connection_factory=None, path='/',
                 security_token=None, validate_certs=True, profile_name=None,
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
        self.mock_iam_instance_profiles = combine_values(
            {}, mock_iam_instance_profiles)
        self.mock_iam_roles = combine_values({}, mock_iam_roles)
        self.mock_iam_role_attached_policies = combine_values(
            {}, mock_iam_role_attached_policies)

        self.host = host

    def get_response(self, action, params, path='/', parent=None,
                     verb='POST', list_marker='Set'):
        # this only supports actions for which there is no method
        # in boto's IAMConnection

        if action == 'AttachRolePolicy':
            return self._attach_role_policy(params['RoleName'],
                                            params['PolicyArn'])

        elif action == 'ListAttachedRolePolicies':
            if list_marker != 'AttachedPolicies':
                raise ValueError

            return self._list_attached_role_policies(params['RoleName'])

        else:
            raise NotImplementedError(
                'mockboto does not implement the %s API call' % action)

    # instance profiles

    def add_role_to_instance_profile(self, instance_profile_name, role_name):
        self._check_instance_profile_exists(instance_profile_name)

        profile_data = self.mock_iam_instance_profiles[instance_profile_name]

        if profile_data['role_name'] is not None:
            raise boto.exception.BotoServerError(
                409, 'Conflict', boto=err_xml(
                    ('Cannot exceed quota for'
                     ' InstanceSessionsPerInstanceProfile: 1'),
                    code='LimitExceeded'))

        self._check_role_exists(role_name)

        profile_data['role_name'] = role_name

        # response is empty
        return self._wrap_result('add_role_to_instance_profile')

    def create_instance_profile(self, instance_profile_name, path=None):
        self._check_path(path)
        self._check_instance_profile_does_not_exist(instance_profile_name)

        self.mock_iam_instance_profiles[instance_profile_name] = dict(
            create_date=to_iso8601(datetime.utcnow()),
            path=(path or self.DEFAULT_PATH),
            role_name=None,
        )

        return self._wrap_result(
            'create_instance_profile',
            {'instance_profile': self._describe_instance_profile(
                instance_profile_name)})

    def list_instance_profiles(self, path_prefix=None, marker=None,
                               max_items=None):

        self._check_path(path_prefix, field_name='pathPrefix')
        path_prefix = path_prefix or '/'

        profile_names = sorted(
            name for name, data in self.mock_iam_instance_profiles.items()
            if data['path'].startswith(path_prefix))

        profiles = [self._describe_instance_profile(profile_name)
                    for profile_name in profile_names]

        result = self._paginate(profiles, 'instance_profiles',
                                marker=marker, max_items=max_items)

        return self._wrap_result('list_instance_profiles', result)

    def _check_instance_profile_exists(self, instance_profile_name):
        if instance_profile_name not in self.mock_iam_instance_profiles:
            raise boto.exception.BotoServerError(
                404, 'Not Found', body=err_xml(
                    ('Instance Profile %s cannot be found.' %
                     instance_profile_name), code='NoSuchEntity'))

    def _check_instance_profile_does_not_exist(self, instance_profile_name):
        if instance_profile_name in self.mock_iam_instance_profiles:
            raise boto.exception.BotoServerError(
                409, 'Conflict', body=err_xml(
                    ('Instance Profile %s already exists.' %
                     instance_profile_name), code='EntityAlreadyExists'))

    def _describe_instance_profile(self, instance_profile_name):
        """Format the given instance profile for an API response."""
        profile_data = self.mock_iam_instance_profiles[instance_profile_name]

        if profile_data['role_name'] is None:
            roles = {}
        else:
            roles = {'member': self._describe_role(profile_data['role_name'])}

        return dict(
            create_date=profile_data['create_date'],
            instance_profile_name=instance_profile_name,
            path=profile_data['path'],
            roles=roles)

    # roles

    def create_role(self, role_name, assume_role_policy_document, path=None):
        # real boto has a default for assume_role_policy_document; not
        # supporting this for now
        self._check_path(path)
        self._check_role_does_not_exist(role_name)

        # there's no validation of assume_role_policy_document; not entirely
        # sure what the rules are

        self.mock_iam_roles[role_name] = dict(
            assume_role_policy_document=assume_role_policy_document,
            create_date=to_iso8601(datetime.utcnow()),
            path=(path or self.DEFAULT_PATH),
            policy_names=[],
        )

        result = dict(role=self._describe_role(role_name))

        return self._wrap_result('create_role', result)

    def list_roles(self, path_prefix=None, marker=None, max_items=None):
        self._check_path(path_prefix, field_name='pathPrefix')
        path_prefix = path_prefix or '/'

        # find all matching profiles
        role_names = sorted(
            name for name, data in self.mock_iam_roles.items()
            if data['path'].startswith(path_prefix))

        roles = [self._describe_role(role_name) for role_name in role_names]

        result = self._paginate(roles, 'roles',
                                marker=marker, max_items=max_items)

        return self._wrap_result('list_roles', result)

    def _check_role_exists(self, role_name):
        if role_name not in self.mock_iam_roles:
            raise boto.exception.BotoServerError(
                404, 'Not Found', body=err_xml(
                    ('The role with name %s cannot be found.' %
                     role_name), code='NoSuchEntity'))

    def _check_role_does_not_exist(self, role_name):
        if role_name in self.mock_iam_roles:
            raise boto.exception.BotoServerError(
                409, 'Conflict', body=err_xml(
                    ('Role with name %s already exists.' %
                     role_name), code='EntityAlreadyExists'))

    def _describe_role(self, role_name):
        """Format the given instance profile for an API response."""
        role_data = self.mock_iam_roles[role_name]

        # the IAM API doesn't include policy names when describing roles

        return dict(
            assume_role_policy_document=(
                role_data['assume_role_policy_document']),
            create_date=role_data['create_date'],
            role_name=role_name,
            path=role_data['path']
        )

    # attached (managed) role policies

    # boto does not yet have methods for these

    def _attach_role_policy(self, role_name, policy_arn):
        self._check_role_exists(role_name)

        arns = self.mock_iam_role_attached_policies.setdefault(role_name, [])
        if policy_arn not in arns:
            arns.append(policy_arn)

        return self._wrap_result('attach_role_policy')

    def _list_attached_role_policies(
            self, role_name, marker=None, max_items=None):

        self._check_role_exists(role_name)

        # in theory, pagination is supported, but in practice each role
        # can have a maximum of two policies attached
        if marker or max_items:
            raise NotImplementedError()

        arns = self.mock_iam_role_attached_policies.get(role_name, [])

        return self._wrap_result('list_attached_role_policies',
                                 {'attached_policies': [
                                     {'policy_arn': arn} for arn in arns]})

    # other utilities

    def _check_path(self, path=None, field_name='path'):
        if path is None or (path.startswith('/') and path.endswith('/')):
            return

        raise boto.exception.BotoServerError(
            400, 'Bad Request', body=err_xml(
                'The specified value for %s is invalid. It must begin and'
                ' end with / and contain only alphanumeric characters and/or'
                ' / characters.' % field_name))

    def _paginate(self, items, name, marker=None, max_items=None):
        """Given a list of items, return a dictionary mapping
        *name* to a slice of items, with additional keys
        'is_truncated' and, if 'is_truncated' is true, 'marker'.
        """
        max_items = max_items or self.DEFAULT_MAX_ITEMS

        start = 0
        if marker:
            start = int(marker)

        end = start + max_items

        result = {name: items[start:end]}

        result['is_truncated'] = end < len(items)
        if result['is_truncated']:
            result['marker'] = str(end)

        return result

    def _wrap_result(self, prefix, result=None):
        """Wrap result in two additional dictionaries (these result from boto's
        decoding of the XML response)."""
        if result is None:
            result_dict = {}
        else:
            result_dict = {prefix + '_result': result}

       # could add response_metadata to result_dict, but we don't use it

        return {prefix + '_response': result_dict}


def _api_params_to_emr_object(params):
    """Convert emr_api_params into a MockEmrObject."""
    result = MockEmrObject()

    # iteratively set value, creating MockEmrObjects as needed
    for key, value in params.items():
        # boto converts attrs to lowercase
        attrs = [a.lower() for a in key.split('.')]

        obj = result
        for attr in attrs[:-1]:
            if not hasattr(obj, attr):
                setattr(obj, attr, MockEmrObject())
            obj = getattr(obj, attr)

        setattr(obj, attrs[-1], str(value))

    # convert objects with "member" key to lists
    def _convert_lists(x):
        # base case
        if not isinstance(x, MockEmrObject):
            return x

        # recursively convert sub-objects
        if not hasattr(x, 'member'):
            for k, v in x.__dict__.items():
                setattr(x, k, _convert_lists(v))
            return x

        # special case: this object was meant to be a list
        result = []

        for k in sorted(x.member.__dict__, key=lambda k: int(k)):
            assert(len(result) == int(k) - 1)  # verify correct numbering
            result.append(_convert_lists(getattr(x.member, k)))

        return result

    return _convert_lists(result)


def _normalize_configuration_objs(configurations):
    """The API will return an empty Properties list for configurations
    without properties set, and remove empty sub-configurations"""
    for c in configurations:
        if not hasattr(c, 'properties'):
            c.properties = []

        if hasattr(c, 'configurations'):
            if not c.configurations:
                del c.configurations
            else:
                _normalize_configuration_objs(c.configurations)
