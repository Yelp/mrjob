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
import datetime
import collections
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
from mrjob.dataproc import DATAPROC_CLUSTER_STATES_ERROR, DATAPROC_CLUSTER_STATES_READY
from mrjob.dataproc import DATAPROC_JOB_STATES_ACTIVE, DATAPROC_JOB_STATES_INACTIVE
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

def _dict_deep_update(d, u):
    """ http://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth """
    for k, v in u.iteritems():
        if isinstance(v, collections.Mapping):
            r = _dict_deep_update(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d

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
        raise NotImplementedError

################################################################################
################################################################################
################################################################################

_DATAPROC_PROJECT = 'test-project-test'
_DATAPROC_CLUSTER = 'test-cluster-test'
_CLUSTER_REGION = _DATAPROC_API_REGION
_CLUSTER_ZONE = None
_CLUSTER_IMAGE_VERSION = '1.0'
_CLUSTER_STATE = ''
_CLUSTER_MACHINE_TYPE = 'n1-standard-1'
_CLUSTER_NUM_WORKERS = 2

def _datetime_to_dataproc_zulu(in_datetime=None):
    in_datetime = in_datetime or datetime.datetime.utcnow()
    return in_datetime.isoformat() + 'Z'

def _create_cluster_resp(project=None, zone=None, cluster=None, image_version=None, machine_type=None, machine_type_master=None, num_workers=None, now=None):
    project = project or _DATAPROC_PROJECT
    zone = zone or _CLUSTER_ZONE
    cluster = cluster or _DATAPROC_CLUSTER
    image_version = image_version or _CLUSTER_IMAGE_VERSION
    machine_type_master = machine_type_master or _CLUSTER_MACHINE_TYPE
    machine_type = machine_type or _CLUSTER_MACHINE_TYPE
    num_workers = num_workers or _CLUSTER_NUM_WORKERS

    gce_cluster_conf = {
      "zoneUri": "https://www.googleapis.com/compute/v1/projects/%(project)s/zones/%(zone)s" % locals(),
      "networkUri": "https://www.googleapis.com/compute/v1/projects/%(project)s/global/networks/default" % locals(),
      "serviceAccountScopes": [
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/bigtable.admin.table",
        "https://www.googleapis.com/auth/bigtable.data",
        "https://www.googleapis.com/auth/cloud.useraccounts.readonly",
        "https://www.googleapis.com/auth/devstorage.full_control",
        "https://www.googleapis.com/auth/devstorage.read_write",
        "https://www.googleapis.com/auth/logging.write"
      ]
    }

    master_conf = {
      "numInstances": 1,
      "instanceNames": [
        "%(cluster)s-m" % locals()
      ],
      "imageUri": "https://www.googleapis.com/compute/v1/projects/cloud-dataproc/global/images/dataproc-1-0-20160302-200123",
      "machineTypeUri": "https://www.googleapis.com/compute/v1/projects/%(project)s/zones/%(zone)s/machineTypes/%(machine_type_master)s" % locals(),
      "diskConfiguration": {
        "bootDiskSizeGb": 500
      }
    }

    worker_conf = {
      "numInstances": num_workers,
      "instanceNames": ['%s-w-%d' % (cluster, num) for num in xrange(num_workers)],
      "imageUri": "https://www.googleapis.com/compute/v1/projects/cloud-dataproc/global/images/dataproc-1-0-20160302-200123",
      "machineTypeUri": "https://www.googleapis.com/compute/v1/projects/%(project)s/zones/%(zone)s/machineTypes/%(machine_type)s" % locals(),
      "diskConfiguration": {
        "bootDiskSizeGb": 500
      }
    }

    software_conf = {
      "imageVersion": image_version,
      "properties": {
        "yarn:yarn.nodemanager.resource.memory-mb": "3072",
        "yarn:yarn.scheduler.minimum-allocation-mb": "256",
        "yarn:yarn.scheduler.maximum-allocation-mb": "3072",
        "mapred:mapreduce.map.memory.mb": "3072",
        "mapred:mapreduce.map.java.opts": "-Xmx2457m",
        "mapred:mapreduce.map.cpu.vcores": "1",
        "mapred:mapreduce.reduce.memory.mb": "3072",
        "mapred:mapreduce.reduce.java.opts": "-Xmx2457m",
        "mapred:mapreduce.reduce.cpu.vcores": "1",
        "mapred:yarn.app.mapreduce.am.resource.mb": "3072",
        "mapred:yarn.app.mapreduce.am.command-opts": "-Xmx2457m",
        "mapred:yarn.app.mapreduce.am.resource.cpu-vcores": "1",
        "distcp:mapreduce.map.memory.mb": "3072",
        "distcp:mapreduce.reduce.memory.mb": "3072",
        "distcp:mapreduce.map.java.opts": "-Xmx2457m",
        "distcp:mapreduce.reduce.java.opts": "-Xmx2457m",
        "spark:spark.executor.cores": "1",
        "spark:spark.executor.memory": "1152m",
        "spark:spark.yarn.executor.memoryOverhead": "384",
        "spark:spark.yarn.am.memory": "1152m",
        "spark:spark.yarn.am.memoryOverhead": "384",
        "spark:spark.driver.memory": "960m",
        "spark:spark.driver.maxResultSize": "480m"
      }
    }

    mock_response = {
      "projectId": project,
      "clusterName": cluster,
      "configuration": {
        "configurationBucket": "dataproc-801485be-0997-40e7-84a7-00926031747c-us",
        "gceClusterConfiguration": gce_cluster_conf,
        "masterConfiguration": master_conf,
        "workerConfiguration": worker_conf,
        "softwareConfiguration": software_conf
      },
      "status": {
        "state": "CREATING",
        "stateStartTime": _datetime_to_dataproc_zulu(now)
      },
      "clusterUuid": "adb4dc59-d109-4af9-badb-0d8e17e028e1"
    }
    return mock_response


class MockDataprocClientClusters(object):
    def __init__(self, client):
        assert isinstance(client, MockDataprocClient)
        self._client = client
        self._clusters = self._client._cache_clusters

    def create(self, projectId=None, region=None, body=None):
        """Mock of run_jobflow().
        """
        assert projectId is not None
        assert region == _DATAPROC_API_REGION

        body = body or dict()

        # Create an empty cluster
        cluster = _create_cluster_resp()

        # Then do a deep-update as to what was requested
        _dict_deep_update(cluster, body)

        _set_deep(self._clusters, [projectId, cluster['clusterName']], cluster)

        return cluster

    def get(self, projectId=None, region=None, clusterName=None):
        assert projectId is not None
        assert region == _DATAPROC_API_REGION

        cluster = _get_deep(self._clusters, [projectId, clusterName])
        if not cluster:
            raise mock_google_error(httplib.NOT_FOUND)

        return cluster

    def delete(self, projectId=None, region=None, clusterName=None):
        assert projectId is not None
        assert region == _DATAPROC_API_REGION

        return self.update_state(projectId, clusterName, 'DELETING')

    def get_state(self, projectId, clusterName):
        cluster = self.get(projectId=projectId, region=_DATAPROC_API_REGION, clusterName=clusterName)
        return cluster['status']['state']

    def update_state(self, projectId=None, clusterName=None, state=None, prev_state=None):
        assert state
        cluster = self.get(projectId=projectId, region=_DATAPROC_API_REGION, clusterName=clusterName)

        old_state = cluster['status']['state']
        assert old_state != state
        if prev_state:
            assert old_state == prev_state

        new_status = {
            "state": state,
            "stateStartTime": _datetime_to_dataproc_zulu()
        }

        old_status = cluster.pop('status')
        cluster['status'] = new_status

        cluster.setdefault('statusHistory', [])
        cluster['statusHistory'].append(old_status)

        return cluster
################################################################################
################################################################################
################################################################################
_JOB_STATE_MATCHER_ACTIVE = frozenset(['PENDING', 'RUNNING', 'CANCEL_PENDING'])
_JOB_STATE_MATCHER_NON_ACTIVE = frozenset(['CANCELLED', 'DONE', 'ERROR'])
_JOB_STATE_MATCHERS = {
    'ALL': _JOB_STATE_MATCHER_ACTIVE | _JOB_STATE_MATCHER_NON_ACTIVE,
    'ACTIVE': _JOB_STATE_MATCHER_ACTIVE,
    'NON_ACTIVE': _JOB_STATE_MATCHER_NON_ACTIVE
}

_SCRIPT_NAME = 'mr_test_mockgoogleapiclient'
_USER_NAME = 'testuser'
_INPUT_DIR = ''
_OUTPUT_DIR = ''

def _submit_hadoop_job_resp(project=None, cluster=None, script_name=None, input_dir=None, output_dir=None, now=None):
    project = project or _DATAPROC_PROJECT
    cluster = cluster or _DATAPROC_CLUSTER
    script_name = script_name or _SCRIPT_NAME
    now = now or datetime.datetime.utcnow()

    assert input_dir and output_dir

    job_elements = [script_name, _USER_NAME, now.strftime('%Y%m%d'), now.strftime('%H%M%S'), now.strftime('%f')]

    job_id = '-'.join(job_elements + ['-', 'Step', '1', 'of', '1'])
    dir_name = '.'.join(job_elements)

    mock_response = {
      "reference": {
        "projectId": project,
        "jobId": job_id
      },
      "placement": {
        "clusterName": cluster,
        "clusterUuid": "8b76d95e-ebdc-4b81-896d-b2c5009b3560"
      },
      "hadoopJob": {
        "mainJarFileUri": "file:///usr/lib/hadoop-mapreduce/hadoop-streaming.jar",
        "args": [
          "-files",
          "gs://boulder-input-data/tmp/mrjob-77657375140ce2a1/%(dir_name)s/files/%(script_name)s.py#%(script_name)s.py" % locals(),
          "-mapper",
          "python %(script_name)s.py --step-num=0 --mapper" % locals(),
          "-reducer",
          "python %(script_name)s.py --step-num=0 --reducer" % locals(),
          "-input",
          input_dir,
          "-output",
          output_dir
        ],
        "loggingConfiguration": {}
      },
      "status": {
        "state": "PENDING",
        "stateStartTime": _datetime_to_dataproc_zulu(now)
      },
      "driverControlFilesUri": "gs://dataproc-801485be-0997-40e7-84a7-00926031747c-us/google-cloud-dataproc-metainfo/8b76d95e-ebdc-4b81-896d-b2c5009b3560/jobs/%(job_id)s/" % locals(),
      "driverOutputResourceUri": "gs://dataproc-801485be-0997-40e7-84a7-00926031747c-us/google-cloud-dataproc-metainfo/8b76d95e-ebdc-4b81-896d-b2c5009b3560/jobs/%(job_id)s/driveroutput" % locals()
    }
    return mock_response

def _update_job_state(job_resp, state=None):
    assert state
    old_status = job_resp.pop('status')

    job_resp['status'] = {
        "state": state,
        "stateStartTime": _datetime_to_dataproc_zulu()
    }

    job_resp.setdefault('statusHistory', [])
    job_resp['statusHistory'].append(old_status)

    return job_resp

class MockDataprocClientJobs(object):
    def __init__(self, client):
        assert isinstance(client, MockDataprocClient)
        self._client = client
        self._jobs = self._client._cache_jobs

    def list(self, **kwargs):
        projectId = kwargs['projectId']
        region = kwargs['region']
        clusterName = kwargs.get('clusterName')
        jobStateMatcher = kwargs.get('jobStateMatcher') or 'ALL'

        assert projectId is not None
        assert region == _DATAPROC_API_REGION

        valid_job_states = _JOB_STATE_MATCHERS[jobStateMatcher]

        item_list = []

        job_map = _get_deep(self._jobs, [projectId], dict())
        for current_job_id, current_job in job_map.iteritems():
            job_cluster = current_job['placement']['clusterName']
            job_state = current_job['status']['state']

            # If we are searching for a specific clusterName and it doesn't match...
            if clusterName and job_cluster != clusterName:
                continue
            elif job_state not in valid_job_states:
                continue

            item_list.append(job_state)

        return dict(items=item_list, kwargs=kwargs)

    def list_next(self, list_request, resp):
        return list()

    def get(self, projectId=None, region=None, jobId=None):
        assert projectId is not None
        assert region == _DATAPROC_API_REGION

        current_job = _get_deep(self._jobs, [projectId, jobId])
        if not current_job:
            raise mock_google_error(httplib.NOT_FOUND)

        return current_job

    def cancel(self, projectId=None, region=None, jobId=None):
        assert projectId is not None
        assert region == _DATAPROC_API_REGION

        return self.update_state(projectId=projectId, jobId=jobId, state='CANCEL_PENDING')

    def delete(self, projectId=None, region=None, jobId=None):
        assert projectId is not None
        assert region == _DATAPROC_API_REGION

        return self.update_state(projectId=projectId, jobId=jobId, state='DELETING')

    def submit(self, projectId=None, region=None, body=None):
        assert projectId is not None
        assert region == _DATAPROC_API_REGION

        body = body or dict()

        # Create an empty cluster
        job = _submit_hadoop_job_resp()

        # Then do a deep-update as to what was requested
        _dict_deep_update(job, body)

        _set_deep(self._jobs, [projectId, job['reference']['jobId']], job)

        return job

    def get_state(self, projectId=None, jobId=None):
        job = self.get(projectId=projectId, region=_DATAPROC_API_REGION, jobId=jobId)
        return job['status']['state']

    def update_state(self, projectId=None, jobId=None, state=None, prev_state=None):
        assert state
        job = self.get(projectId=projectId, region=_DATAPROC_API_REGION, jobId=jobId)

        old_state = job['status']['state']
        assert old_state != state
        if prev_state:
            assert old_state == prev_state

        new_status = {
            "state": state,
            "stateStartTime": _datetime_to_dataproc_zulu()
        }

        old_status = job.pop('status')
        job['status'] = new_status

        job.setdefault('statusHistory', [])
        job['statusHistory'].append(old_status)

        return job