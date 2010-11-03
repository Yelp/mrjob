# Copyright 2009-2010 Yelp
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
import time

try:
    import boto.exception
    import boto.utils
except ImportError:
    boto = None

from mrjob.conf import combine_values
from mrjob.emr import S3_URI_RE, parse_s3_uri

### S3 ###


def add_mock_s3_data(mock_s3_fs, data):
    """Update mock_s3_fs (which is just a dictionary mapping bucket to
    key to contents) with a map from bucket name to key name to data."""
    for bucket_name, key_name_to_bytes in data.iteritems():
        mock_s3_fs.setdefault(bucket_name, {})
        bucket = mock_s3_fs[bucket_name]
        
        for key_name, bytes in key_name_to_bytes.iteritems():
            bucket[key_name] = bytes

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
        self.mock_s3_fs = mock_s3_fs or {}

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
            self.mock_s3_fs[bucket_name] = {}

class MockBucket:
    """Mock out boto.s3.Bucket
    """
    def __init__(self, connection=None, name=None):
        """You can optionally specify a 'data' argument, which will instantiate
        mock keys and mock data. data should be a map from key name to bytes.
        """
        self.name = name
        self.connection = connection

    def mock_state(self):
        """Returns a dictionary from key to data representing the
        state of this bucket."""
        if self.name in self.connection.mock_s3_fs:
            return self.connection.mock_s3_fs[self.name]
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')

    def new_key(self, key_name):
        if key_name not in self.mock_state():
            self.mock_state()[key_name] = ''
        return MockKey(bucket=self, name=key_name)

    def get_key(self, key_name):
        if key_name in self.mock_state():
            return MockKey(bucket=self, name=key_name)
        else:
            return None

    def get_location(self):
        return 'us-west-1'

    def list(self, prefix=''):
        for key_name in sorted(self.mock_state()):
            if key_name.startswith(prefix):
                yield MockKey(bucket=self, name=key_name)

class MockKey(object):
    """Mock out boto.s3.Key"""

    def __init__(self, bucket=None, name=None):
        """You can optionally specify a 'data' argument, which will fill
        the key with mock data.
        """
        self.bucket = bucket
        self.name = name

    def read_mock_data(self):
        """Read the bytes for this key out of the fake boto state."""
        if self.name in self.bucket.mock_state():
            return self.bucket.mock_state()[self.name]
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')

    def write_mock_data(self, data):
        if self.name in self.bucket.mock_state():
            self.bucket.mock_state()[self.name] = data
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')

    def get_contents_to_filename(self, path, headers=None):
        with open(path, 'w') as f:
            f.write(self.read_mock_data())

    def set_contents_from_filename(self, path):
        with open(path) as f:
            self.write_mock_data(f.read())

    def delete(self):
        if self.name in self.bucket.mock_state():
            del self.bucket.mock_state()[self.name]
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')

    def make_public(self):
        pass

### EMR ###

class MockEmrConnection(object):
    """Mock out boto.emr.EmrConnection. This actually handles a small
    state machine that simulates EMR job flows."""

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 is_secure=True, port=None, proxy=None, proxy_port=None,
                 proxy_user=None, proxy_pass=None, debug=0,
                 https_connection_factory=None, region=None,
                 mock_s3_fs=None, mock_emr_job_flows=None,
                 mock_emr_failures=None, mock_emr_output=None,
                 max_simulation_steps=100):
        """Create a mock version of EmrConnection. Most of these args are
        the same as for the real EmrConnection, and are ignored.

        By default, jobs will run to conclusion, and if their output dir
        is on S3, create a single empty output file. You can manually
        decide that some jobs will fail, or give them different output
        by setting mock_emr_failures/mock_emr_output.

        Job flows are given IDs j-MOCKJOBFLOW0, j-MOCKJOBFLOW1, etc.
        Step numbers are 0-indexed.

        Extra args:
        mock_s3_fs -- a mock S3 filesystem to point to (just a dictionary
            mapping bucket name to key name to bytes)
        mock_emr_job_flows -- a mock set of EMR job flows to point to
            (just a map from job flow ID to a MockEmrObject representing
            a job flow)
        mock_emr_failures -- a map from (job flow ID, step_num) to a failure
            message (or None for the default message)
        mock_emr_output -- a map from (job flow ID, step_num) to a list of
            strs representing file contents to output when the job completes
        max_simulation_steps -- the maximum number of times we can simulate the
            the progress of EMR job flows (to protect against simulating
            forever)
        """
        self.mock_s3_fs = combine_values({}, mock_s3_fs)
        self.mock_emr_job_flows = combine_values({}, mock_emr_job_flows)
        self.mock_emr_failures = combine_values({}, mock_emr_failures)
        self.mock_emr_output = combine_values({}, mock_emr_output)
        self.simulation_steps_left = max_simulation_steps

    def run_jobflow(self,
                    name, log_uri, ec2_keyname=None, availability_zone=None,
                    master_instance_type='m1.small',
                    slave_instance_type='m1.small', num_instances=1,
                    action_on_failure='TERMINATE_JOB_FLOW', keep_alive=False,
                    enable_debugging=False,
                    hadoop_version='0.18',
                    steps=[],
                    bootstrap_actions=[]):
        init_args = locals().copy()
        del init_args['self']

        jobflow_id = 'j-MOCKJOBFLOW%d' % len(self.mock_emr_job_flows)
        assert jobflow_id not in self.mock_emr_job_flows

        # create a MockEmrObject corresponding to the job flow. We only
        # need to fill in the fields that EMRJobRunnerUses
        job_flow = MockEmrObject(
            state='STARTING',
            laststatechangereason='Provisioning Amazon EC2 capacity',
            loguri=log_uri,
            keepjobalivewhennosteps=keep_alive,
            steps = [],
        )
        self.mock_emr_job_flows[jobflow_id] = job_flow

        self.add_jobflow_steps(jobflow_id, steps)

        return jobflow_id

    def describe_jobflow(self, jobflow_id):
        if not jobflow_id in self.mock_emr_job_flows:
            raise boto.exception.S3ResponseError(404, 'Not Found')

        self.simulate_progress(jobflow_id)

        return self.mock_emr_job_flows[jobflow_id]

    def add_jobflow_steps(self, jobflow_id, steps):
        if not jobflow_id in self.mock_emr_job_flows:
            raise boto.exception.S3ResponseError(404, 'Not Found')

        job_flow = self.mock_emr_job_flows[jobflow_id]

        for step in steps:
            step_object = MockEmrObject(
                state='PENDING',
                name=step.name,
                actiononfailure=step.action_on_failure,
                args=step.args(),
            )
            
            job_flow.steps.append(step_object)

    def terminate_jobflow(self, jobflow_id):
        if not jobflow_id in self.mock_emr_job_flows:
            raise boto.exception.S3ResponseError(404, 'Not Found')

        job_flow = self.mock_emr_job_flows[jobflow_id]

        job_flow.state = 'SHUTTING_DOWN'
        job_flow.reason = 'Terminated by user request'

        for step in job_flow.steps:
            if step.state not in ('COMPLETED', 'FAILED'):
                step.state = 'CANCELLED'

    def _get_step_output_uri(self, step):
        """Figure out the output dir for a step by parsing step.args
        and looking for an -output argument."""
        # parse in reverse order, in case there are multiple -output args
        for i, arg in reversed(list(enumerate(step.args[:-1]))):
            if arg == '-output':
                return step.args[i+1]
        else:
            return None

    def simulate_progress(self, jobflow_id):
        """Simulate progress on the given job flow. This is automatically
        run when we call describe_jobflow()."""
        if self.simulation_steps_left <= 0:
            raise AssertionError(
                'Simulated progress too many times; bailing out')
        self.simulation_steps_left -= 1

        job_flow = self.mock_emr_job_flows[jobflow_id]

        # if job is done, don't advance it
        if job_flow.state in ('COMPLETED', 'TERMINATED', 'FAILED'):
            return

        # if SHUTTING_DOWN, finish shutting down
        if job_flow.state == 'SHUTTING_DOWN':
            if job_flow.reason == 'Shut down as step failed':
                job_flow.state = 'FAILED'
            else:
                job_flow.state = 'TERMINATED'
            return

        # if a step is currently running, advance it
        for step_num, step in enumerate(job_flow.steps):
            # skip steps that are already done
            if step.state in ('COMPLETED', 'FAILED', 'CANCELLED'):
                continue

            # found currently running step! going to handle it, then exit
            if step.state == 'PENDING':
                step.state = 'RUNNING'
                step.startdatetime = time.strftime(boto.utils.ISO8601,
                                                   time.gmtime())
                return

            assert step.state == 'RUNNING'
            step.enddatetime = time.strftime(boto.utils.ISO8601,
                                                   time.gmtime())
                
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
            if output_uri and S3_URI_RE.match(output_uri):
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
        if job_flow.keepjobalivewhennosteps:
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

