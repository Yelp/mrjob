# -*- coding: utf-8 -*-
# Copyright 2009-2015 Yelp and Contributors
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
import json
from datetime import datetime
from urllib import quote

try:
    from boto.emr.connection import EmrConnection
    from boto.emr.instance_group import InstanceGroup
    from boto.emr.step import JarStep
    import boto.exception
    import boto.utils
    boto  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    boto = None

from mrjob.conf import combine_values
from mrjob.parse import is_s3_uri
from mrjob.parse import parse_s3_uri
from mrjob.parse import RFC1123

# list_clusters() only returns this many results at a time
DEFAULT_MAX_CLUSTERS_RETURNED = 50

# Size of each chunk returned by the MockKey iterator
SIMULATED_BUFFER_SIZE = 256

# the AMI version the EMR API picks if you don't specify one
# (for old accounts; for new ones, it's 3.7.0)
DEFAULT_AMI_VERSION = '1.0.0'

# the AMI version given if you specify "latest"
LATEST_AMI_VERSION = '2.4.2'

# versions of hadoop for each AMI
AMI_VERSION_TO_HADOOP_VERSION = {
    '1.0': '0.20',
    '1.0.0': '0.20',
    '2.0': '0.20.205',
    '2.0.0': '0.20.205',
    '2.4.2': '0.20.205',
}

# extra step to use when debugging_step=True is passed to run_jobflow()
DEBUGGING_STEP = JarStep(
    name='Setup Hadoop Debugging',
    action_on_failure='TERMINATE_JOB_FLOW',
    main_class=None,
    jar=EmrConnection.DebuggingJar,
    step_args=EmrConnection.DebuggingArgs)

# Don't run EMR simluation longer than this
DEFAULT_MAX_SIMULATION_STEPS = 100


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


### S3 ###

def add_mock_s3_data(mock_s3_fs, data, time_modified=None):
    """Update mock_s3_fs with a map from bucket name to key name to data and
    time last modified."""
    if time_modified is None:
        time_modified = datetime.utcnow()
    for bucket_name, key_name_to_bytes in data.iteritems():
        mock_s3_fs.setdefault(bucket_name, {'keys': {}, 'location': ''})
        bucket = mock_s3_fs[bucket_name]

        for key_name, bytes in key_name_to_bytes.iteritems():
            bucket['keys'][key_name] = (bytes, time_modified)


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
        self.endpoint = host or 's3.amazonaws.com'

    def get_bucket(self, bucket_name, validate=True, headers=None):
        if bucket_name in self.mock_s3_fs:
            return MockBucket(connection=self, name=bucket_name)
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')

    def get_all_buckets(self):
        return [self.get_bucket(name) for name in self.mock_s3_fs]

    def create_bucket(self, bucket_name, headers=None, location='',
                      policy=None):
        if bucket_name in self.mock_s3_fs:
            raise boto.exception.S3CreateError(409, 'Conflict')
        else:
            self.mock_s3_fs[bucket_name] = {'keys': {}, 'location': ''}


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
                '',
                to_iso8601(datetime.utcnow())
            )
        return MockKey(bucket=self, name=key_name)

    def get_key(self, key_name):
        if key_name in self.mock_state():
            return MockKey(bucket=self, name=key_name, date_to_str=to_rfc1123)
        else:
            return None

    def get_location(self):
        return self.connection.mock_s3_fs[self.name]['location']

    def set_location(self, new_location):
        self.connection.mock_s3_fs[self.name]['location'] = new_location

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
        """You can optionally specify a 'data' argument, which will fill
        the key with mock data.
        """
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
        if self.name in self.bucket.mock_state():
            self.bucket.mock_state()[self.name] = (data, datetime.utcnow())
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')

    def get_contents_to_filename(self, path, headers=None):
        with open(path, 'w') as f:
            f.write(self.read_mock_data())

    def set_contents_from_filename(self, path):
        with open(path) as f:
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

    def next(self):
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


class MultiPartUploadCancelled(str):
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
        data = ''

        if self.parts:
            num_parts = max(self.parts)
            for part_num in xrange(1, num_parts + 1):
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
    return when.strftime(RFC1123) + 'GMT'


class MockEmrConnection(object):
    """Mock out boto.emr.EmrConnection. This actually handles a small
    state machine that simulates EMR job flows."""

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
                 mock_emr_failures=None, mock_emr_output=None,
                 max_clusters_returned=DEFAULT_MAX_CLUSTERS_RETURNED,
                 max_simulation_steps=None):
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
        :param mock_emr_output: a map from ``(cluster ID, step_num)`` to a
                                list of ``str``s representing file contents to
                                output when the job completes
        :type max_job_flows_returned: int
        :param max_job_flows_returned: the maximum number of job flows that
                                       :py:meth:`describe_jobflows` can return,
                                       to simulate a real limitation of EMR
        :type max_days_ago: int
        :param max_days_ago: the maximum amount of days that EMR will go back
                             in time

        :type max_simulation_steps: int
        :params max_simulation_steps: don't simulate progress in EMR more than
                                      this many times
        """
        self.mock_s3_fs = combine_values({}, mock_s3_fs)
        self.mock_emr_clusters = combine_values({}, mock_emr_clusters)
        self.mock_emr_failures = combine_values({}, mock_emr_failures)
        self.mock_emr_output = combine_values({}, mock_emr_output)
        self.max_clusters_returned = max_clusters_returned

        if max_simulation_steps is None:
            self.simulation_steps_left = DEFAULT_MAX_SIMULATION_STEPS
        else:
            self.simulation_steps_left = max_simulation_steps

        if region is not None:
            self.endpoint = region.endpoint
        else:
            self.endpoint = 'elasticmapreduce.amazonaws.com'

    def _enforce_strict_ssl(self):
        if (self.STRICT_SSL and
            not self.endpoint.endswith('elasticmapreduce.amazonaws.com')):
            from boto.https_connection import InvalidCertificateException
            raise InvalidCertificateException(
                self.endpoint, None, 'hostname mismatch')

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
                    service_role=None):
        """Mock of run_jobflow().
        """
        self._enforce_strict_ssl()

        if now is None:
            now = datetime.utcnow()

        # default fields that can be set from api_params
        if job_flow_role is None:
            job_flow_role = api_params.get('JobFlowRole')

        if service_role is None:
            service_role = api_params.get('ServiceRole')

        if visible_to_all_users is None:
            visible_to_all_users = (
                api_params.get('VisibleToAllUsers') == 'true')

        # Handle empty, "latest" AMI version
        if ami_version is None:
            running_ami_version = DEFAULT_AMI_VERSION
        elif ami_version == 'latest':
            running_ami_version = LATEST_AMI_VERSION
        else:
            running_ami_version = ami_version

        # determine Hadoop version
        if running_ami_version not in AMI_VERSION_TO_HADOOP_VERSION:
            raise boto.exception.EmrResponseError(400, 'Bad Request')

        hadoop_version = AMI_VERSION_TO_HADOOP_VERSION[running_ami_version]

        # create a MockEmrObject corresponding to the job flow. We only
        # need to fill in the fields that EMRJobRunner uses
        steps = steps or []

        cluster_id = u'j-MOCKCLUSTER%d' % len(self.mock_emr_clusters)
        assert cluster_id not in self.mock_emr_clusters

        cluster = MockEmrObject(
            applications=[MockEmrObject(
                name='hadoop',
                version=hadoop_version,
            )],
            autoterminate=(u'false' if keep_alive else u'true'),
            ec2instanceattributes=MockEmrObject(
                ec2availabilityzone=availability_zone,
                ec2keyname=ec2_keyname,
                iaminstanceprofile=job_flow_role,
            ),
            id=cluster_id,
            loguri=log_uri,
            # TODO: set this later, once cluster is running
            masterpublicdnsname=u'mockmaster',
            name=name,
            normalizedinstancehours=u'0',
            requestedamiversion=ami_version,
            runningamiversion=running_ami_version,
            servicerole=service_role,
            status=MockEmrObject(
                state=u'STARTING',
                statechangereason=MockEmrObject(),
                timeline=MockEmrObject(
                    creationdatetime=to_iso8601(now)),
            ),
            tags=[],
            terminationprotected=u'false',
            visibletoallusers=(u'true' if visible_to_all_users else u'false')
        )

        cluster._bootstrapactions = self._build_bootstrap_actions(
            bootstrap_actions)

        if instance_groups:
            cluster._instancegroups = (
                self._build_instance_groups_from_list(instance_groups))
        else:
            cluster._instancegroups = (
                self._build_instance_groups_from_type_and_count(
                    master_instance_type, slave_instance_type, num_instances))

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
                          args=[MockEmrObject(value=unicode(v)) for v \
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
                id=u'ig-FAKE',
                instancegrouptype=instance_group.role,
                instancetype=instance_group.type,
                market=instance_group.market,
                name=instance_group.name,
                requestedinstancecount=unicode(instance_group.num_instances),
                runninginstancecount=u'0',
                status=MockEmrObject(state=u'PROVISIONING'),
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

                if ('.' in bid_price and
                    len(bid_price.split('.', 1)[1]) > 3):
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

    def get_object(self, action, params, cls):
        """mrjob._mock_emr currently calls get_response() directly, to support
        old versions of boto. In real boto, the other methods call
        get_object(), but in mockboto, this method fans out to
        the other ones

        this can be removed in v0.5.0, when we use a newer version of
        boto (see #1081)
        """
        # common to most calls
        cluster_id = params.get('ClusterId')
        marker = params.get('Marker')

        if action == 'DescribeCluster':
            return self.describe_cluster(cluster_id)

        elif action == 'ListBootstrapActions':
            return self.list_bootstrap_actions(cluster_id, marker=marker)

        elif action == 'ListClusters':
            created_after = self._unpack_datetime(params.get('CreatedAfter'))
            created_before = self._unpack_datetime(params.get('CreatedBefore'))
            cluster_states = self._unpack_list_param('ClusterStates.member',
                                                     params)

            return self.list_clusters(created_after=created_after,
                                      created_before=created_before,
                                      cluster_states=cluster_states,
                                      marker=marker)

        elif action == 'ListInstanceGroups':
            return self.list_instance_groups(cluster_id, marker=marker)

        elif action == 'ListSteps':
            step_states = self._unpack_list_param('StepStateList.member',
                                                  params)

            return self.list_steps(cluster_id,
                                   marker=marker,
                                   step_states=step_states)

        else:
            raise NotImplementedError(
                'mockboto does not implement the %s API call' % action)

    def _unpack_datetime(self, iso_dt):
        """Undo conversion to ISO date. Remove in v0.5.0."""
        if iso_dt is None:
            return None

        return datetime.strptime(iso_dt, boto.utils.ISO8601)

    def _unpack_list_param(self, label, params):
        """Undo EmrConnection.build_list_params().

        Remove this in v0.5.0 (see #1081)
        """
        indexed_values = []  # tuples of (idx, value)

        for k, v in params.items():
            if k.startswith(label + '.'):
                idx = int(k[len(label) + 1:])

                indexed_values.append((idx, v))

        if indexed_values:
            return [v for idx, v in sorted(indexed_values)]
        else:
            return None

    def _get_mock_cluster(self, cluster_id):
        if not cluster_id in self.mock_emr_clusters:
            raise boto.exception.S3ResponseError(404, 'Not Found')

        return self.mock_emr_clusters[cluster_id]

    def describe_cluster(self, cluster_id):
        self._enforce_strict_ssl()

        self.simulate_progress(cluster_id)

        return self._get_mock_cluster(cluster_id)

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

        for cluster_id, cluster in sorted(self.mock_emr_clusters.items()):
            # skip ahead to marker
            if marker is not None and cluster_id < marker:
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
            cluster_id = None

        return MockEmrObject(clusters=cluster_summaries, marker=cluster_id)

    def list_instance_groups(self, cluster_id, marker=None):
        self._enforce_strict_ssl()

        if marker is not None:
            raise NotImplementedError(
                'marker not simulated for ListBootstrapActions')

        cluster = self._get_mock_cluster(cluster_id)

        return MockEmrObject(instancegroups=cluster._instancegroups)

    def list_steps(self, cluster_id, step_states=None, marker=None):
        self._enforce_strict_ssl()

        if marker is not None:
            raise NotImplementedError(
                'marker not simulated for ListBootstrapActions')

        cluster = self._get_mock_cluster(cluster_id)

        steps_listed = []
        for step in cluster._steps:
            if step_states is None or step.status.state in step_states:
                steps_listed.append(step)

        return MockEmrObject(steps=steps_listed)

    def add_jobflow_steps(self, jobflow_id, steps, now=None):
        self._enforce_strict_ssl()

        if now is None:
            now = datetime.utcnow()

        cluster = self._get_mock_cluster(jobflow_id)

        for step in steps:
            step_config = MockEmrObject(
                args=[MockEmrObject(value=arg) for arg in step.args()],
                jar=step.jar(),
                mainclass=step.main_class())
            # there's also a "properties" field, but boto doesn't handle it

            step_status = MockEmrObject(
                state=u'PENDING',
                timeline=MockEmrObject(
                    creationdatetime=to_iso8601(now)),
            )

            cluster._steps.append(MockEmrObject(
                actiononfailure=step.action_on_failure,
                config=step_config,
                id='s-FAKE',
                name=step.name,
                status=step_status,
            ))

    def terminate_jobflow(self, jobflow_id):
        self._enforce_strict_ssl()

        cluster = self._get_mock_cluster(jobflow_id)

        # already terminated
        if cluster.status.state in (
                u'TERMINATED', u'TERMINATED_WITH_ERRORS'):
            return

        # mark cluster as shutting down
        cluster.status.state = u'TERMINATING'
        cluster.status.statechangereason = MockEmrObject(
            code=u'USER_REQUEST',
            message=u'Terminated by user request',
        )

        for step in cluster._steps:
            if step.status.state == u'PENDING':
                step.status.state = u'CANCELLED'
            elif step.status.state == u'RUNNING':
                # pretty sure this is what INTERRUPTED is for
                step.status.state = u'INTERRUPTED'

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
        """Simulate progress on the given job flow. This is automatically
        run when we call describe_jobflow().

        :type jobflow_id: str
        :param jobflow_id: fake job flow ID
        :type now: py:class:`datetime.datetime`
        :param now: alternate time to use as the current time (should be UTC)
        """
        if now is None:
            now = datetime.utcnow()

        # don't allow simulating forever
        if self.simulation_steps_left <= 0:
            raise AssertionError(
                    'Simulated progress too many times; bailing out')

        self.simulation_steps_left -= 1

        cluster = self._get_mock_cluster(cluster_id)

        # this code is pretty loose about updating statechangereason
        # (for the cluster, instance groups, and steps). Add this as needed.

        # if job is STARTING, move it along to BOOTSTRAPPING
        if cluster.status.state == u'STARTING':
            cluster.status.state = u'BOOTSTRAPPING'
            # instances are now provisioned
            for ig in cluster._instancegroups:
                ig.runninginstancecount = ig.requestedinstancecount,
                ig.status.state = u'BOOTSTRAPPING'

            return

        # if job is BOOTSTRAPPING, move it along to RUNNING
        if cluster.status.state == u'BOOTSTRAPPING':
            cluster.status.state = u'RUNNING'
            for ig in cluster._instancegroups:
                ig.status.state = u'RUNNING'

            return

        # if job is TERMINATING, move along to terminated
        if cluster.status.state == u'TERMINATING':
            if cluster.status.statechangereason.code == 'STEP_FAILURE':
                cluster.status.state = u'TERMINATED_WITH_ERRORS'
            else:
                cluster.status.state = u'TERMINATED'

            return

        # if job is done, nothing to do
        if cluster.status.state in (u'TERMINATED', u'TERMINATED_WITH_ERRORS'):
            return

        # at this point, should be RUNNING or WAITING
        assert cluster.status.state in (u'RUNNING', u'WAITING')

        # try to find the next step, and advance it

        for step_num, step in enumerate(cluster._steps):
            # skip steps that are already done
            if step.status.state in (
                    u'COMPLETED', u'FAILED', u'CANCELLED', u'INTERRUPTED'):
                continue

            # allow steps to get stuck
            if getattr(step, 'mock_no_progress', None):
                return

            # found currently running step! handle it, then exit

            # start PENDING step
            if step.status.state == u'PENDING':
                step.status.state = u'RUNNING'
                step.status.timeline.startdatetime = to_iso8601(now)
                return

            assert step.status.state == u'RUNNING'

            # finish RUNNING step

            # failure
            if (cluster_id, step_num) in self.mock_emr_failures:
                step.status.state = u'FAILED'

            # found currently running step! going to handle it, then exit
            if step.status.state == u'PENDING':
                step.status.state = u'RUNNING'
                step.status.timeline.startdatetime = to_iso8601(now)
                return

            assert step.status.state == u'RUNNING'
            step.status.timeline.enddatetime = to_iso8601(now)

            # check if we're supposed to have an error
            if (cluster_id, step_num) in self.mock_emr_failures:
                step.status.state = u'FAILED'

                if step.actiononfailure in (
                    u'TERMINATE_CLUSTER', u'TERMINATE_JOB_FLOW'):

                    cluster.status.state = u'TERMINATING'
                    cluster.status.statechangereason.code = u'STEP_FAILURE'
                    cluster.status.statechangereason.message = (
                        u'Shut down as step failed')

                return

            # complete step
            step.status.state = 'COMPLETED'
            step.status.timeline.enddatetime = to_iso8601(now)

            # create fake output if we're supposed to write to S3
            output_uri = self._get_step_output_uri(step.config.args)
            if output_uri and is_s3_uri(output_uri):
                mock_output = self.mock_emr_output.get(
                    (cluster_id, step_num)) or ['']

                bucket_name, key_name = parse_s3_uri(output_uri)

                # write output to S3
                for i, bytes in enumerate(mock_output):
                    add_mock_s3_data(self.mock_s3_fs, {
                        bucket_name: {key_name + 'part-%05d' % i: bytes}})
            elif (cluster_id, step_num) in self.mock_emr_output:
                raise AssertionError(
                    "can't use output for job flow ID %s, step %d "
                    "(it doesn't output to S3)" %
                    (cluster_id, step_num))

            # done!
            return

        # no pending steps. should we wait, or shut down?
        if cluster.autoterminate == u'true':
            cluster.status.state = u'TERMINATING'
            cluster.status.statechangereason.code = u'ALL_STEPS_COMPLETED'
            cluster.status.statechangereason.message = (
                'Steps Completed')
        else:
            # just wait
            cluster.status.state = u'WAITING'
            cluster.status.statechangereason = MockEmrObject()

        return


class MockEmrObject(object):
    """Mock out boto.emr.EmrObject. This is just a generic object that you
    can set any attribute on."""

    def __init__(self, **kwargs):
        """Intialize with the given attributes, ignoring fields set to None."""
        for key, value in kwargs.iteritems():
            if value is not None:
                setattr(self, key, value)

    def __setattr__(self, key, value):
        self.__dict__[key] = value

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        my_items = self.__dict__.items()
        other_items = other.__dict__.items()

        if len(my_items) != len(other_items):
            return False

        for k, v in my_items:
            if not k in other_items:
                return False
            else:
                if v != other_items[k]:
                    return False

        return True

    # useful for hand-debugging tests
    def __repr__(self):
        return('%s.%s(%s)' % (
            self.__class__.__module__,
            self.__class__.__name__,
            ', '.join('%s=%r' % (k, v)
                      for k, v in sorted(self.__dict__.iteritems()))))


class MockIAMConnection(object):

    DEFAULT_PATH = '/'

    DEFAULT_MAX_ITEMS = 100

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 is_secure=True, port=None, proxy=None, proxy_port=None,
                 proxy_user=None, proxy_pass=None, host='iam.amazonaws.com',
                 debug=0, https_connection_factory=None, path='/',
                 security_token=None, validate_certs=True, profile_name=None,
                 mock_iam_instance_profiles=None, mock_iam_roles=None,
                 mock_iam_role_policies=None,
                 mock_iam_role_attached_policies=None):
        """Mock out connection to IAM.

        mock_iam_instance_profiles maps profile name to a dictionary containing:
            create_date -- ISO creation datetime
            path -- IAM path
            role_name -- name of single role for this instance profile, or None

        mock_iam_roles maps role name to a dictionary containing:
            assume_role_policy_document -- a JSON-then-URI-encoded policy doc
            create_date -- ISO creation datetime
            path -- IAM path

        mock_iam_role_policies maps policy name to a dictionary containing:
            policy_document -- JSON-then-URI-encoded policy doc
            role_name -- name of single role for this policy (always defined)

        mock_iam_role_attached_policies maps role to a list of ARNs for
        attached (managed) policies.

        We don't track which managed policies exist or what their contents are.
        We also don't support role IDs.
        """
        self.mock_iam_instance_profiles = combine_values(
            {}, mock_iam_instance_profiles)
        self.mock_iam_roles = combine_values({}, mock_iam_roles)
        self.mock_iam_role_policies = combine_values(
            {}, mock_iam_role_policies)
        self.mock_iam_role_attached_policies = combine_values(
            {}, mock_iam_role_attached_policies)

    def get_response(self, action, params, path='/', parent=None,
                     verb='POST', list_marker='Set'):
        # mrjob.iam currently only calls get_response(), to support old
        # versions of boto. In real boto, the other methods call
        # this one, but in mockboto, this method fans out to the other ones
        if action == 'AddRoleToInstanceProfile':
            return self.add_role_to_instance_profile(
                params['InstanceProfileName'],
                params['RoleName'])

        elif action == 'AttachRolePolicy':
            return self._attach_role_policy(params['RoleName'],
                                            params['PolicyArn'])

        elif action == 'CreateInstanceProfile':
            return self.create_instance_profile(
                params['InstanceProfileName'],
                path=params.get('Path'))

        elif action == 'CreateRole':
            return self.create_role(
                params['RoleName'],
                json.loads(params['AssumeRolePolicyDocument']),
                path=params.get('Path'))

        elif action == 'GetRolePolicy':
            return self.get_role_policy(
                params['RoleName'],
                params['PolicyName'])

        elif action == 'ListAttachedRolePolicies':
            if list_marker != 'AttachedPolicies':
                raise ValueError

            return self._list_attached_role_policies(params['RoleName'])

        elif action == 'ListInstanceProfiles':
            if list_marker != 'InstanceProfiles':
                raise ValueError

            return self.list_instance_profiles(
                path_prefix=params.get('PathPrefix'),
                marker=params.get('Marker'),
                max_items=params.get('MaxItems'))

        elif action == 'ListRolePolicies':
            if list_marker != 'PolicyNames':
                raise ValueError

            return self.list_role_policies(
                params['RoleName'],
                marker=params.get('Marker'),
                max_items=params.get('MaxItems'))

        elif action == 'ListRoles':
            if list_marker != 'Roles':
                raise ValueError

            return self.list_roles(
                path_prefix=params.get('PathPrefix'),
                marker=params.get('Marker'),
                max_items=params.get('MaxItems'))

        elif action == 'PutRolePolicy':
            # boto apparently doesn't make any attempt to
            # JSON-encode the role policy for you!
            return self.put_role_policy(
                params['RoleName'],
                params['PolicyName'],
                params['PolicyDocument'])


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
        # supporting this for now. It also allows assume_role_policy_document
        # to be a string, which we don't.

        self._check_path(path)
        self._check_role_does_not_exist(role_name)

        # there's no validation of assume_role_policy_document; not entirely
        # sure what the rules are

        self.mock_iam_roles[role_name] = dict(
            assume_role_policy_document=quote(json.dumps(
                assume_role_policy_document)),
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

    # (inline) role policies

    def get_role_policy(self, role_name, policy_name):
        self._check_role_exists(role_name)
        self._check_role_policy_exists(policy_name, role_name)

        result = self._describe_role_policy(policy_name)

        return self._wrap_result('get_role_policy', result)

    def list_role_policies(self, role_name, marker=None, max_items=None):
        policy_names = [
            name for name, data in sorted(self.mock_iam_role_policies.items())
            if data['role_name'] == role_name]

        result = self._paginate(policy_names, 'policy_names',
                                marker=marker, max_items=max_items)

        return self._wrap_result('list_role_policies', result)

    def put_role_policy(self, role_name, policy_name, policy_document):
        self._check_role_exists(role_name)

        # PutRolePolicy will happily overwrite existing role policies
        self.mock_iam_role_policies[policy_name] = dict(
            policy_document=quote(policy_document),
            role_name=role_name)

        return self._wrap_result('put_role_policy')

    def _check_role_policy_exists(self, policy_name, role_name):
        if (policy_name not in self.mock_iam_role_policies or
            self.mock_iam_role_policies[policy_name]['role_name'] != role_name):

            # the IAM API really does raise this error when the role policy
            # exists but has a different role name
            raise boto.exception.BotoServerError(
                404, 'Not Found', body=err_xml(
                    ('The role policy with name %s cannot be found.' %
                     role_name), code='NoSuchEntity'))

    def _describe_role_policy(self, policy_name):
        policy_data = self.mock_iam_role_policies[policy_name]

        return dict(
            policy_document=policy_data['policy_document'],
            policy_name=policy_name,
            role_name=policy_data['role_name'],
        )

    # attached (managed) role policies

    # boto does not yet have methods for these

    def _attach_role_policy(self, role_name, policy_arn):
        self._check_role_exists(role_name)

        arns = self.mock_iam_role_attached_policies.setdefault(role_name, [])
        if policy_arn not in arns:
            arns.append(policy_arn)

        return self._wrap_result('attach_role_policy')

    def _list_attached_role_policies(self, role_name):
        self._check_role_exists(role_name)

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
        *names* to a slice of items, with additional keys
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
