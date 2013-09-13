# -*- coding: utf-8 -*-
# Copyright 2009-2013 Yelp and Contributors
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

This is by no means a complete mock of boto; I just added the methods I needed
to make tests work.

If you need a more extensive set of mock boto objects, we recommend adding
some sort of sandboxing feature to boto, rather than extending these somewhat
ad-hoc mock objects.
"""
from __future__ import with_statement
from datetime import datetime
from datetime import timedelta
import hashlib

try:
    from boto.emr.connection import EmrConnection
    from boto.emr.step import JarStep
    import boto.exception
    import boto.utils
    boto  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    boto = None

from mrjob.conf import combine_values
from mrjob.emr import RFC1123
from mrjob.parse import is_s3_uri
from mrjob.parse import parse_s3_uri


DEFAULT_MAX_JOB_FLOWS_RETURNED = 500
DEFAULT_MAX_DAYS_AGO = 61

DEFAULT_JAR = '/stuff/hadoop-streaming.jar'

# Size of each chunk returned by the MockKey iterator
SIMULATED_BUFFER_SIZE = 256

# versions of hadoop available on each AMI version. The EMR API treats None
# and "latest" as separate logical AMIs, even though they're actually the
# same AMIs as 1.0 and whatever they most recently released.
AMI_VERSION_TO_HADOOP_VERSIONS = {
    None: ['0.18', '0.20'],
    '1.0': ['0.18', '0.20'],
    '2.0': ['0.20.205'],
    'latest': ['0.20.205'],
}


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
    """Update mock_s3_fs (which is just a dictionary mapping bucket to
    key to contents) with a map from bucket name to key name to data and
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
                 bucket_class=None, mock_s3_fs=None):
        """Mock out a connection to S3. Most of these args are the same
        as for the real S3Connection, and are ignored.

        You can set up a mock filesystem to share with other objects
        by specifying mock_s3_fs. The mock filesystem is just a map
        from bucket name to key name to bytes.
        """
        # use mock_s3_fs even if it's {}
        self.mock_s3_fs = combine_values({}, mock_s3_fs)
        self.endpoint = host or 's3.amazonaws.com'

    def get_bucket(self, bucket_name):
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
            self.mock_state()[key_name] = ('',
                    to_iso8601(datetime.utcnow()))
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


### EMR ###

def to_iso8601(when):
    """Convert a datetime to ISO8601 format.
    """
    return when.strftime(boto.utils.ISO8601)


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
                 mock_s3_fs=None, mock_emr_job_flows=None,
                 mock_emr_failures=None, mock_emr_output=None,
                 max_days_ago=DEFAULT_MAX_DAYS_AGO,
                 max_job_flows_returned=DEFAULT_MAX_JOB_FLOWS_RETURNED,
                 simulation_iterator=None):
        """Create a mock version of EmrConnection. Most of these args are
        the same as for the real EmrConnection, and are ignored.

        By default, jobs will run to conclusion, and if their output dir
        is on S3, create a single empty output file. You can manually
        decide that some jobs will fail, or give them different output
        by setting mock_emr_failures/mock_emr_output.

        Job flows are given IDs j-MOCKJOBFLOW0, j-MOCKJOBFLOW1, etc.
        Step numbers are 0-indexed.

        Extra args:
        :param mock_s3_fs: a mock S3 filesystem to point to (just a dictionary
                           mapping bucket name to key name to bytes)
        :param mock_emr_job_flows: a mock set of EMR job flows to point to
                                   (just a map from job flow ID to a
                                   :py:class:`MockEmrObject` representing a job
                                   flow)
        :param mock_emr_failures: a map from ``(job flow ID, step_num)`` to a
                                  failure message (or ``None`` for the default
                                  message)
        :param mock_emr_output: a map from ``(job flow ID, step_num)`` to a
                                list of ``str``s representing file contents to
                                output when the job completes
        :type max_job_flows_returned: int
        :param max_job_flows_returned: the maximum number of job flows that
                                       :py:meth:`describe_jobflows` can return,
                                       to simulate a real limitation of EMR
        :type max_days_ago: int
        :param max_days_ago: the maximum amount of days that EMR will go back
                             in time
        :param simulation_iterator: we call ``next()`` on this each time
                                    we simulate progress. If there is
                                    no next element, we bail out.
        """
        self.mock_s3_fs = combine_values({}, mock_s3_fs)
        self.mock_emr_job_flows = combine_values({}, mock_emr_job_flows)
        self.mock_emr_failures = combine_values({}, mock_emr_failures)
        self.mock_emr_output = combine_values({}, mock_emr_output)
        self.max_days_ago = max_days_ago
        self.max_job_flows_returned = max_job_flows_returned
        self.simulation_iterator = simulation_iterator
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
                    name, log_uri, ec2_keyname=None, availability_zone=None,
                    master_instance_type='m1.small',
                    slave_instance_type='m1.small', num_instances=1,
                    action_on_failure='TERMINATE_JOB_FLOW', keep_alive=False,
                    enable_debugging=False,
                    hadoop_version=None,
                    steps=None,
                    bootstrap_actions=[],
                    instance_groups=None,
                    additional_info=None,
                    ami_version=None,
                    now=None,
                    api_params=None):
        """Mock of run_jobflow().

        If you set log_uri to None, you can get a jobflow with no loguri
        attribute, which is useful for testing.
        """
        self._enforce_strict_ssl()

        if now is None:
            now = datetime.utcnow()

        # default and validate Hadoop and AMI versions

        # if nothing specified, use 0.20 for backwards compatibility
        if ami_version is None and hadoop_version is None:
            hadoop_version = '0.20'

        # check if AMI version is valid
        if ami_version not in AMI_VERSION_TO_HADOOP_VERSIONS:
            raise boto.exception.EmrResponseError(400, 'Bad Request')

        available_hadoop_versions = AMI_VERSION_TO_HADOOP_VERSIONS[ami_version]

        if hadoop_version is None:
            hadoop_version = available_hadoop_versions[0]
        elif hadoop_version not in available_hadoop_versions:
            raise boto.exception.EmrResponseError(400, 'Bad Request')

        # create a MockEmrObject corresponding to the job flow. We only
        # need to fill in the fields that EMRJobRunner uses
        steps = steps or []

        jobflow_id = 'j-MOCKJOBFLOW%d' % len(self.mock_emr_job_flows)
        assert jobflow_id not in self.mock_emr_job_flows

        def make_fake_action(real_action):
            return MockEmrObject(name=real_action.name,
                                 path=real_action.path,
                                 args=[MockEmrObject(value=str(v)) for v \
                                       in real_action.bootstrap_action_args])

        # create a MockEmrObject corresponding to the job flow. We only
        # need to fill in the fields that EMRJobRunnerUses
        if not instance_groups:
            mock_groups = [
                MockEmrObject(
                    instancerequestcount='1',
                    instancerole='MASTER',
                    instancerunningcount='0',
                    instancetype=master_instance_type,
                    market='ON_DEMAND',
                    name='master',
                ),
            ]
            if num_instances > 1:
                mock_groups.append(
                    MockEmrObject(
                        instancerequestcount=str(num_instances - 1),
                        instancerole='CORE',
                        instancerunningcount='0',
                        instancetype=slave_instance_type,
                        market='ON_DEMAND',
                        name='core',
                    ),
                )
            else:
                # don't display slave instance type if there are no slaves
                slave_instance_type = None
        else:
            slave_instance_type = None
            num_instances = 0

            mock_groups = []
            roles = set()

            for instance_group in instance_groups:
                if instance_group.num_instances < 1:
                    raise boto.exception.EmrResponseError(
                        400, 'Bad Request', body=err_xml(
                        'An instance group must have at least one instance'))

                emr_group = MockEmrObject(
                    instancerequestcount=str(instance_group.num_instances),
                    instancerole=instance_group.role,
                    instancerunningcount='0',
                    instancetype=instance_group.type,
                    market=instance_group.market,
                    name=instance_group.name,
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

                if instance_group.role in roles:
                    role_desc = instance_group.role.lower()
                    raise boto.exception.EmrResponseError(
                        400, 'Bad Request', body=err_xml(
                        'Multiple %s instance groups supplied, you'
                        ' must specify exactly one %s instance group' %
                        (role_desc, role_desc)))

                if instance_group.role == 'MASTER':
                    if instance_group.num_instances != 1:
                        raise boto.exception.EmrResponseError(
                            400, 'Bad Request', body=err_xml(
                            'A master instance group must specify a single'
                            ' instance'))

                    master_instance_type = instance_group.type

                elif instance_group.role == 'CORE':
                    slave_instance_type = instance_group.type
                mock_groups.append(emr_group)
                num_instances += instance_group.num_instances
                roles.add(instance_group.role)

                if 'TASK' in roles and 'CORE' not in roles:
                    raise boto.exception.EmrResponseError(
                        400, 'Bad Request', body=err_xml(
                        'Clusters with task nodes must also define core'
                        ' nodes.'))

                if 'MASTER' not in roles:
                    raise boto.exception.EmrResponseError(
                        400, 'Bad Request', body=err_xml(
                        'Zero master instance groups supplied, you must'
                        ' specify exactly one master instance group'))

        job_flow = MockEmrObject(
            availabilityzone=availability_zone,
            bootstrapactions=[make_fake_action(a) for a in bootstrap_actions],
            creationdatetime=to_iso8601(now),
            ec2keyname=ec2_keyname,
            hadoopversion=hadoop_version,
            instancecount=str(num_instances),
            instancegroups=mock_groups,
            jobflowid=jobflow_id,
            keepjobflowalivewhennosteps=('true' if keep_alive else 'false'),
            laststatechangereason='Provisioning Amazon EC2 capacity',
            masterinstancetype=master_instance_type,
            masterpublicdnsname='mockmaster',
            name=name,
            normalizedinstancehours='9999',  # just need this filled in for now
            state='STARTING',
            steps=[],
            visibletoallusers='false',  # can only be set with api_params
        )

        if slave_instance_type is not None:
            job_flow.slaveinstancetype = slave_instance_type

        # AMI version is only set when you specify it explicitly
        if ami_version is not None:
            job_flow.amiversion = ami_version

        # don't always set loguri, so we can test Issue #112
        if log_uri is not None:
            job_flow.loguri = log_uri

        # include raw api params in job flow object
        if api_params:
            for k, v in api_params.iteritems():
                setattr(job_flow, k.lower(), v)

        self.mock_emr_job_flows[jobflow_id] = job_flow

        if enable_debugging:
            debugging_step = JarStep(name='Setup Hadoop Debugging',
                                     action_on_failure='TERMINATE_JOB_FLOW',
                                     main_class=None,
                                     jar=EmrConnection.DebuggingJar,
                                     step_args=EmrConnection.DebuggingArgs)
            steps.insert(0, debugging_step)
        self.add_jobflow_steps(jobflow_id, steps)

        return jobflow_id

    def describe_jobflow(self, jobflow_id, now=None):
        self._enforce_strict_ssl()

        if not jobflow_id in self.mock_emr_job_flows:
            raise boto.exception.S3ResponseError(404, 'Not Found')

        self.simulate_progress(jobflow_id, now=now)

        return self.mock_emr_job_flows[jobflow_id]

    def describe_jobflows(self, states=None, jobflow_ids=None,
                          created_after=None, created_before=None):
        self._enforce_strict_ssl()

        now = datetime.utcnow()

        if created_before:
            min_created_before = now - timedelta(days=self.max_days_ago)

            if created_before < min_created_before:
                raise boto.exception.BotoServerError(
                    400, 'Bad Request', body=err_xml(
                    'Created-before field is before earliest allowed value'))

        jfs = sorted(self.mock_emr_job_flows.itervalues(),
                     key=lambda jf: jf.creationdatetime,
                     reverse=True)

        if states or jobflow_ids or created_after or created_before:
            if states:
                jfs = [jf for jf in jfs if jf.state in states]

            if jobflow_ids:
                jfs = [jf for jf in jfs if jf.jobflowid in jobflow_ids]

            if created_after:
                after_timestamp = to_iso8601(created_after)
                jfs = [jf for jf in jfs
                       if jf.creationdatetime > after_timestamp]

            if created_before:
                before_timestamp = to_iso8601(created_before)
                jfs = [jf for jf in jfs
                       if jf.creationdatetime < before_timestamp]
        else:
            # special case for no parameters, see:
            # http://docs.amazonwebservices.com/ElasticMapReduce/latest/API/API_DescribeJobFlows.html
            two_weeks_ago_timestamp = to_iso8601(
                now - timedelta(weeks=2))
            jfs = [jf for jf in jfs
                   if (jf.creationdatetime > two_weeks_ago_timestamp or
                       jf.state in ['RUNNING', 'WAITING',
                                    'SHUTTING_DOWN', 'STARTING'])]

        if self.max_job_flows_returned:
            jfs = jfs[:self.max_job_flows_returned]

        return jfs

    def add_jobflow_steps(self, jobflow_id, steps):
        self._enforce_strict_ssl()

        if not jobflow_id in self.mock_emr_job_flows:
            raise boto.exception.S3ResponseError(404, 'Not Found')

        job_flow = self.mock_emr_job_flows[jobflow_id]

        if getattr(job_flow, 'steps', None) is None:
            job_flow.steps = []

        for step in steps:
            step_object = MockEmrObject(
                state='PENDING',
                name=step.name,
                actiononfailure=step.action_on_failure,
                args=[MockEmrObject(value=arg) for arg in step.args()],
                jar=DEFAULT_JAR,
            )
            job_flow.state = 'PENDING'
            job_flow.steps.append(step_object)

    def terminate_jobflow(self, jobflow_id):
        self._enforce_strict_ssl()

        if not jobflow_id in self.mock_emr_job_flows:
            raise boto.exception.S3ResponseError(404, 'Not Found')

        job_flow = self.mock_emr_job_flows[jobflow_id]

        job_flow.state = 'SHUTTING_DOWN'
        job_flow.reason = 'Terminated by user request'

        steps = getattr(job_flow, 'steps', None) or []
        for step in steps:
            if step.state not in ('COMPLETED', 'FAILED'):
                step.state = 'CANCELLED'

    def _get_step_output_uri(self, step):
        """Figure out the output dir for a step by parsing step.args
        and looking for an -output argument."""
        # parse in reverse order, in case there are multiple -output args
        for i, arg in reversed(list(enumerate(step.args[:-1]))):
            if arg.value == '-output':
                return step.args[i + 1].value
        else:
            return None

    def simulate_progress(self, jobflow_id, now=None):
        """Simulate progress on the given job flow. This is automatically
        run when we call describe_jobflow().

        :type jobflow_id: str
        :param jobflow_id: fake job flow ID
        :type now: py:class:`datetime.datetime`
        :param now: alternate time to use as the current time (should be UTC)
        """
        if now is None:
            now = datetime.utcnow()

        if self.simulation_iterator:
            try:
                self.simulation_iterator.next()
            except StopIteration:
                raise AssertionError(
                    'Simulated progress too many times; bailing out')

        job_flow = self.mock_emr_job_flows[jobflow_id]

        # if job is STARTING, move it along to WAITING
        if job_flow.state == 'STARTING':
            job_flow.state = 'WAITING'
            job_flow.startdatetime = to_iso8601(now)
            # instances are now provisioned and running
            for ig in job_flow.instancegroups:
                ig.instancerunningcount = ig.instancerequestcount

        # if job is done, don't advance it
        if job_flow.state in ('COMPLETED', 'TERMINATED', 'FAILED'):
            return

        # if SHUTTING_DOWN, finish shutting down
        if job_flow.state == 'SHUTTING_DOWN':
            if job_flow.reason == 'Shut down as step failed':
                job_flow.state = 'FAILED'
            else:
                job_flow.state = 'TERMINATED'
            job_flow.enddatetime = to_iso8601(now)
            return

        # if a step is currently running, advance it
        steps = getattr(job_flow, 'steps', None) or []

        for step_num, step in enumerate(steps):
            # skip steps that are already done
            if step.state in ('COMPLETED', 'FAILED', 'CANCELLED'):
                continue
            if step.name in ('Setup Hadoop Debugging', ):
                step.state = 'COMPLETED'
                continue

            # allow steps to get stuck
            if getattr(step, 'mock_no_progress', None):
                return

            # found currently running step! going to handle it, then exit
            if step.state == 'PENDING':
                step.state = 'RUNNING'
                step.startdatetime = to_iso8601(now)
                return

            assert step.state == 'RUNNING'
            step.enddatetime = to_iso8601(now)

            # check if we're supposed to have an error
            if (jobflow_id, step_num) in self.mock_emr_failures:
                step.state = 'FAILED'
                reason = self.mock_emr_failures[(jobflow_id, step_num)]
                if reason:
                    job_flow.reason = reason
                if step.actiononfailure == 'TERMINATE_JOB_FLOW':
                    job_flow.state = 'SHUTTING_DOWN'
                    if not reason:
                        job_flow.reason = 'Shut down as step failed'
                return

            step.state = 'COMPLETED'

            # create fake output if we're supposed to write to S3
            output_uri = self._get_step_output_uri(step)
            if output_uri and is_s3_uri(output_uri):
                mock_output = self.mock_emr_output.get(
                    (jobflow_id, step_num)) or ['']

                bucket_name, key_name = parse_s3_uri(output_uri)

                # write output to S3
                for i, bytes in enumerate(mock_output):
                    add_mock_s3_data(self.mock_s3_fs, {
                        bucket_name: {key_name + 'part-%05d' % i: bytes}})
            elif (jobflow_id, step_num) in self.mock_emr_output:
                raise AssertionError(
                    "can't use output for job flow ID %s, step %d "
                    "(it doesn't output to S3)" %
                    (jobflow_id, step_num))

            # done!
            return

        # no pending steps. shut down job if appropriate
        if job_flow.keepjobflowalivewhennosteps == 'true':
            job_flow.state = 'WAITING'
            job_flow.reason = 'Waiting for steps to run'
        else:
            job_flow.state = 'COMPLETED'
            job_flow.reason = 'Steps Completed'


class MockEmrObject(object):
    """Mock out boto.emr.EmrObject. This is just a generic object that you
    can set any attribute on."""

    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
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
