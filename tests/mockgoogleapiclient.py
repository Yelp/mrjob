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

import itertools
import os
import tempfile
import time
import mock
import httplib
import httplib2
from datetime import datetime
from io import BytesIO

try:
    from oauth2client.client import GoogleCredentials
    from googleapiclient import discovery
    from googleapiclient import errors as google_errors
    from googleapiclient import http as google_http
except ImportError:
    # don't require googleapiclient; MRJobs don't actually need it when running
    # inside hadoop streaming
    GoogleCredentials = None
    discovery = None
    google_errors = None
    google_http = None

from mrjob.conf import combine_values
from mrjob.dataproc import DataprocJobRunner
from mrjob.dataproc import _DATAPROC_API_REGION, _DATAPROC_IMAGE_TO_HADOOP_VERSION
from mrjob.fs.gcs import GCSFilesystem
from mrjob.fs.gcs import is_gcs_uri
from mrjob.fs.gcs import parse_gcs_uri

from tests.mr_two_step_job import MRTwoStepJob
from tests.py2 import patch
from tests.sandbox import SandboxedTestCase

# list_clusters() only returns this many results at a time
DEFAULT_MAX_CLUSTERS_RETURNED = 50

def mock_google_error(status):
    mock_resp = mock.Mock(spec=httplib2.Response)
    mock_resp.status = status
    return google_errors.HttpError(mock_resp, '')

# Addressable data structure specific
def _get_deep(data_structure, dot_path_or_list, default_value=None):
    """Attempts access nested data structures and not blow up on a gross key error
    {
        "hello": {
            "hi": 5
        }
    }
    """
    search_path = None

    param_type = type(dot_path_or_list)
    if param_type in (tuple, list):
        search_path = dot_path_or_list
    elif param_type == str:
        search_path = dot_path_or_list.split('.')

    assert len(search_path) > 0, "Missing valid search path"

    try:
        current_item = data_structure
        for search_key in search_path:
            current_item = current_item[search_key]
    except (KeyError, IndexError, TypeError):
        return default_value

    return current_item

def _set_deep(data_structure, dot_path_or_list, value_to_set):
    """Attempts access nested data structures and not blow up on a gross key error
    {
        "hello": {
            "hi": 5
        }
    }
    """
    assert hasattr(data_structure, '__setitem__')
    search_path = None

    param_type = type(dot_path_or_list)
    if param_type in (tuple, list):
        search_path = dot_path_or_list
    elif param_type == str:
        search_path = dot_path_or_list.split('.')

    assert len(search_path) > 0, "Missing valid search path"

    current_item = data_structure
    for search_key in search_path[:-1]:
        current_item.setdefault(search_key, dict())
        current_item = current_item[search_key]

    current_item[search_path[-1]] = value_to_set
    return data_structure



### Test Case ###

class MockGoogleAPITestCase(SandboxedTestCase):

    @classmethod
    def setUpClass(cls):
        # we don't care what's in this file, just want mrjob to stop creating
        # and deleting a complicated archive.
        cls.fake_mrjob_tgz_path = tempfile.mkstemp(
            prefix='fake_mrjob_', suffix='.tar.gz')[1]

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.fake_mrjob_tgz_path):
            os.remove(cls.fake_mrjob_tgz_path)

    def setUp(self):
        self.mock_dataproc_failures = {}
        self.mock_dataproc_clusters = {}
        self.mock_dataproc_output = {}

        self._dataproc_client = MockDataprocClient(self)
        self._gcs_client = MockGCSClient(self)

        self.start(patch.object(DataprocJobRunner, 'api_client', self._dataproc_client))

        self.start(patch.object(GCSFilesystem, 'api_client', self._gcs_client))
        self.start(patch.object(GCSFilesystem, '_download_io', self._gcs_client.download_io))
        self.start(patch.object(GCSFilesystem, '_upload_io', self._gcs_client.upload_io))

        super(MockGoogleAPITestCase, self).setUp()

        # patch slow things
        def fake_create_mrjob_tar_gz(mocked_self, *args, **kwargs):
            mocked_self._mrjob_tar_gz_path = self.fake_mrjob_tgz_path
            return self.fake_mrjob_tgz_path

        self.start(patch.object(
            DataprocJobRunner, '_create_mrjob_tar_gz',
            fake_create_mrjob_tar_gz))

        self.start(patch.object(time, 'sleep'))

    def put_gcs_data(self, data, time_modified=None, location=None):
        """Update self.mock_gcs_fs with a map from bucket name
            to key name to data."""
        self._gcs_client.put_data(data, time_modified=time_modified, location=location)


    def make_runner(self, *args):
        """create a dummy job, and call make_runner() on it.
        Use this in a with block:

        with self.make_runner() as runner:
            ...
        """
        stdin = BytesIO(b'foo\nbar\n')
        mr_job = MRTwoStepJob(['-r', 'dataproc'] + list(args))
        mr_job.sandbox(stdin=stdin)

        return mr_job.make_runner()


### GCS ###


class MockGCSClient(object):
    """Mock outMockGCSClient
    """

    def __init__(self, test_case):
        assert isinstance(test_case, MockGoogleAPITestCase)
        self._test_case = test_case

        self._cache_fs = dict()

        self._client_objects = MockGCSClientObjects(self)
        self._client_buckets = MockGCSClientBuckets(self)


    def objects(self):
        return self._client_objects

    def buckets(self):
        return self._client_buckets

    def put_data(self, data, time_modified=None, location=None):
        if time_modified is None:
            time_modified = datetime.utcnow()
        for bucket_name, key_name_to_bytes in data.items():
            bucket = self._cache_fs.setdefault(bucket_name,
                                           {'keys': {}, 'location': ''})

            for key_name, key_data in key_name_to_bytes.items():
                if not isinstance(key_data, bytes):
                    raise TypeError('mock gcs data must be bytes')
                bucket['keys'][key_name] = (key_data, time_modified)

            if location is not None:
                bucket['location'] = location

    def download_io(self, src_uri, io_obj):
        bucket_name, object_name = parse_gcs_uri(src_uri)

        key_details = _get_deep(self._cache_fs, [bucket_name, 'keys', object_name])

        if not key_details:
            raise Exception

        key_data, time_modified = key_details
        io_obj.write(key_data)
        return io_obj

    def upload_io(self, io_obj, dest_uri):
        bucket_name, object_name = parse_gcs_uri(dest_uri)

        bytes_array = io_obj.readall()
        output_data = {
            bucket_name: {
                object_name: bytes_array
            }
        }

        self.put_data(output_data)


class MockGCSClientObjects(object):
    def __init__(self, client):
        assert isinstance(client, MockGCSClient)
        self._client = client
        self._cache_fs = self._client._cache_fs

    def list(self, bucket=None, fields=None):
        pass

    def list_next(self, list_request, resp):
        pass

    def delete(self, bucket=None, object=None):
        pass

    def get_media(self, bucket=None, object=None):
        pass

    def insert(self, bucket=None, name=None, media_body=None):
        pass


class MockGCSClientBuckets(object):
    def __init__(self, client):
        assert isinstance(client, MockGCSClient)
        self._client = client
        self._cache_fs = self._client._cache_fs

    def list(self, bucket=None, fields=None):
        pass

    def list_next(self, list_request, resp):
        pass

    def get(self, bucket=None):
        pass

    def delete(self, bucket=None):
        pass

    def insert(self, **kwargs):
        pass
### EMR ###


class MockDataprocClient(object):
    """Mock out DataprocJobRunner.api_client. This actually handles a small
    state machine that simulates Dataproc clusters."""

    def __init__(self, test_case):
        assert isinstance(test_case, MockGoogleAPITestCase)
        self._test_case = test_case

        self._cache_clusters = {}
        self._cache_jobs = {}

        self._client_clusters = MockDataprocClientClusters(self)
        self._client_jobs = MockDataprocClientJobs(self)


    def clusters(self):
        return self._client_clusters

    def jobs(self):
        return self._client_jobs

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

        # if job is BOOTSTRAPPING, move it along to RUNNING
        if cluster.status.state == 'BOOTSTRAPPING':
            cluster.status.state = 'RUNNING'
            for ig in cluster._instancegroups:
                ig.status.state = 'RUNNING'

            return

        # if job is TERMINATING, move along to terminated
        if cluster.status.state == 'TERMINATING':
            code = getattr(getattr(cluster.status, 'statechangereason', None),
                'code', None)

            if code == 'STEP_FAILURE':
                cluster.status.state = 'TERMINATED_WITH_ERRORS'
            else:
                cluster.status.state = 'TERMINATED'

            return

        # if job is done, nothing to do
        if cluster.status.state in ('TERMINATED', 'TERMINATED_WITH_ERRORS'):
            return

        # at this point, should be RUNNING or WAITING
        assert cluster.status.state in ('RUNNING', 'WAITING')

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
            if (cluster_id, step_num) in self.mock_dataproc_failures:
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

            # create fake output if we're supposed to write to GCS
            output_uri = self._get_step_output_uri(step.config.args)
            if output_uri and is_gcs_uri(output_uri):
                mock_output = self.mock_dataproc_output.get(
                    (cluster_id, step_num)) or [b'']

                bucket_name, key_name = parse_gcs_uri(output_uri)

                # write output to GCS
                for i, part in enumerate(mock_output):
                    add_mock_gcs_data(self.mock_gcs_fs, {
                        bucket_name: {key_name + 'part-%05d' % i: part}})
            elif (cluster_id, step_num) in self.mock_dataproc_output:
                raise AssertionError(
                    "can't use output for cluster ID %s, step %d "
                    "(it doesn't output to GCS)" %
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

        return

class MockDataprocClientClusters(object):
    def __init__(self, client):
        assert isinstance(client, MockDataprocClient)
        self._client = client
        self._clusters = self._client._cache_clusters


    def create(self, projectId=None, region=None, body=None):
        """Mock of run_jobflow().
        """
        if now is None:
            now = datetime.utcnow()


        return cluster_id

    def get(self, projectId=None, region=None, clusterName=None):
        cluster = self._client.get_mock_cluster(cluster_id)

        if cluster.status.state == 'TERMINATING':
            # simulate progress, to support
            # _wait_for_logs_on_s3()
            self._client.simulate_progress(clusterName)

        return cluster

    def delete(self, projectId=None, region=None, clusterName=None):
        assert projectId is not None
        assert region == _DATAPROC_API_REGION

        cluster = self._client.get_mock_cluster(clusterName)

        # already terminated
        if cluster.status.state in (
                'TERMINATED', 'TERMINATED_WITH_ERRORS'):
            return

        # mark cluster as shutting down
        cluster.status.state = 'TERMINATING'
        cluster.status.statechangereason = MockDataprocObject(
            code='USER_REQUEST',
            message='Terminated by user request',
        )

        for step in cluster._steps:
            if step.status.state == 'PENDING':
                step.status.state = 'CANCELLED'
            elif step.status.state == 'RUNNING':
                # pretty sure this is what INTERRUPTED is for
                step.status.state = 'INTERRUPTED'

class MockDataprocClientJobs(object):
    def __init__(self, client):
        assert isinstance(client, MockDataprocClient)
        self._client = client
        self._jobs = self._client._cache_jobs

    def list(self, **kwargs):
        region = kwargs['region']
        assert region == _DATAPROC_API_REGION

        return None

    def list_next(self, list_request, resp):
        pass

    def get(self, projectId=None, region=None, jobId=None):
        assert region == _DATAPROC_API_REGION
        current_job = _get_deep(self._jobs, [projectId, jobId])

        if not current_job:
            raise mock_google_error(httplib.NOT_FOUND)

        return current_job

    def cancel(self, projectId=None, region=None, jobId=None):
        assert region == _DATAPROC_API_REGION
        current_job = self.get(projectId=projectId, region=region, jobId=jobId)

        # TODO - Implement CANCEL behavior

        return current_job

    def delete(self, projectId=None, region=None, jobId=None):
        assert region == _DATAPROC_API_REGION
        current_job = self.get(projectId=projectId, region=region, jobId=jobId)

        # TODO - Implement DELETE behavior

        return current_job



    def submit(self, projectId=None, region=None, body=None):
        assert region == _DATAPROC_API_REGION

        # TODO - Implement SUBMIT behavior

        return current_job