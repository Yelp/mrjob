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
import re
import collections
import copy
import time
import hashlib
import sys
from datetime import datetime
from httplib2 import Response
from io import BytesIO
from unittest import skipIf

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

from mrjob.dataproc import DataprocJobRunner
from mrjob.dataproc import _DATAPROC_API_REGION
from mrjob.fs.gcs import GCSFilesystem
from mrjob.fs.gcs import parse_gcs_uri
from mrjob.fs.gcs import _hex_to_base64
from mrjob.fs.gcs import _LS_FIELDS_TO_RETURN

from tests.mr_two_step_job import MRTwoStepJob
from tests.py2 import patch
from tests.py2 import mock
from tests.sandbox import SandboxedTestCase

# list_clusters() only returns this many results at a time
DEFAULT_MAX_CLUSTERS_RETURNED = 50
_TEST_PROJECT = 'test-mrjob:test-project'

_GCLOUD_CONFIG = {
    'compute.region': 'us-central1',
    'compute.zone': 'us-central1-b',
    'core.account': 'no@where.com',
    'core.disable_usage_reporting': 'False',
    'core.project': _TEST_PROJECT
}


def mock_api(fxn):
    def req_wrapper(*args, **kwargs):
        actual_resp = fxn(*args, **kwargs)

        mocked_req = mock.MagicMock(google_http.HttpRequest)
        mocked_req.execute.return_value = actual_resp

        return mocked_req

    return req_wrapper


def mock_google_error(status):
    mock_resp = mock.Mock(spec=Response)
    mock_resp.status = status
    return google_errors.HttpError(mock_resp, b'')


# Addressable data structure specific
def _get_deep(data_structure, dot_path_or_list, default_value=None):
    """Attempts access nested data structures and not blow up on a gross key
    error

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
    """Attempts access nested data structures and not blow up on a gross key
    error.

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
    """from http://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth """  # noqa
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            r = _dict_deep_update(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d


### Test Case ###

# disable these tests until we figure out a way to get the google API client
# to play well with PyPy 3 (which reports itself as Python 3.2, but has key
# Python 3.3 features)
@skipIf(
    hasattr(sys, 'pypy_version_info') and (3, 0) <= sys.version_info < (3, 3),
    "googleapiclient doesn't work with PyPy 3")
class MockGoogleAPITestCase(SandboxedTestCase):

    def setUp(self):
        self._dataproc_client = MockDataprocClient(self)
        self._gcs_client = MockGCSClient(self)
        self._gcs_fs = self._gcs_client._fs

        self.start(patch.object(
            DataprocJobRunner, 'api_client', self._dataproc_client))

        self.gcs_patch_api_client = patch.object(
            GCSFilesystem, 'api_client', self._gcs_client)
        self.gcs_patch_download_io = patch.object(
            GCSFilesystem, '_download_io', self._gcs_client.download_io)
        self.gcs_patch_upload_io = patch.object(
            GCSFilesystem, '_upload_io', self._gcs_client.upload_io)
        self.start(self.gcs_patch_api_client)
        self.start(self.gcs_patch_download_io)
        self.start(self.gcs_patch_upload_io)

        self.start(patch('mrjob.dataproc._read_gcloud_config',
                         lambda: _GCLOUD_CONFIG))

        super(MockGoogleAPITestCase, self).setUp()

        # patch slow things
        self.mrjob_zip_path = None

        def fake_create_mrjob_zip(runner, *args, **kwargs):
            if not self.mrjob_zip_path:
                self.mrjob_zip_path = self.makefile('fake_mrjob.zip')

            runner._mrjob_zip_path = self.mrjob_zip_path
            return self.mrjob_zip_path

        self.start(patch.object(
            DataprocJobRunner, '_create_mrjob_zip',
            fake_create_mrjob_zip))

        self.start(patch.object(time, 'sleep'))

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

    def put_gcs_multi(self, gcs_uri_to_data_map):
        """Convenience method"""
        self._gcs_client.put_gcs_multi(gcs_uri_to_data_map)

    def put_job_output_parts(self, dataproc_runner, raw_parts):
        assert type(raw_parts) is list

        base_uri = dataproc_runner.get_output_dir()
        gcs_multi_dict = dict()
        for part_num, part_data in enumerate(raw_parts):
            gcs_uri = base_uri + 'part-%05d' % part_num
            gcs_multi_dict[gcs_uri] = part_data

        self.put_gcs_multi(gcs_multi_dict)

    def get_cluster_from_runner(self, runner, cluster_id):
        cluster = runner.api_client.clusters().get(
            projectId=_TEST_PROJECT,
            region=_DATAPROC_API_REGION,
            clusterName=cluster_id,
        ).execute()
        return cluster

############################# BEGIN BEGIN BEGIN ###############################
########################### GCS Client - OVERALL ##############################
############################# BEGIN BEGIN BEGIN ###############################


class MockGCSClient(object):
    """Mock out GCSClient...

    TARGET API VERSION - Storage API v1

    Emulates GCS metadata and stores raw bytes
    Contains convenience functions for initializing items in GCS
    """

    def __init__(self, test_case):
        assert isinstance(test_case, MockGoogleAPITestCase)
        self._test_case = test_case
        self._fs = GCSFilesystem()

        self._cache_objects = dict()
        self._cache_buckets = dict()

        self._client_objects = MockGCSClientObjects(self)
        self._client_buckets = MockGCSClientBuckets(self)

    def objects(self):
        return self._client_objects

    def buckets(self):
        return self._client_buckets

    def put_gcs(self, gcs_uri, data):
        """Put data at gcs_uri, creating a bucket if necessary"""
        bucket, name = parse_gcs_uri(gcs_uri)

        try:
            self._fs.get_bucket(bucket)
        except google_errors.HttpError:
            self._fs.create_bucket(project=_TEST_PROJECT, name=bucket)

        bytes_io_obj = BytesIO(data)
        self.upload_io(bytes_io_obj, gcs_uri)

    def put_gcs_multi(self, gcs_uri_to_data_map):
        """Bulk put data at gcs_uris"""
        for gcs_uri, data in gcs_uri_to_data_map.items():
            self.put_gcs(gcs_uri, data)

    def download_io(self, src_uri, io_obj):
        """
        Clobber GCSFilesystem._download_io
        """
        bucket, name = parse_gcs_uri(src_uri)

        object_dict = _get_deep(self._cache_objects, [bucket, name])

        if not object_dict:
            raise Exception

        object_data = object_dict['_data']
        io_obj.write(object_data)
        return io_obj

    def upload_io(self, io_obj, dest_uri):
        """
        Clobber GCSFilesystem._upload_io
        """
        bucket, name = parse_gcs_uri(dest_uri)

        assert bucket in self._cache_buckets

        io_obj.seek(0)

        data = io_obj.read()

        # TODO - io_obj.close() ?  Not sure if callers of this function would
        # expect their io_objs to be closed

        object_resp = _insert_object_resp(bucket=bucket, name=name, data=data)

        _set_deep(self._cache_objects, [bucket, name], object_resp)

        return object_resp


class MockGCSClientObjects(object):
    def __init__(self, client):
        assert isinstance(client, MockGCSClient)
        self._client = client
        self._objects = self._client._cache_objects

    @mock_api
    def list(self, **kwargs):
        """Emulate objects().list - fields supported - bucket, prefix, fields
        """
        bucket = kwargs.get('bucket')
        prefix = kwargs.get('prefix') or ''
        fields = kwargs.get('fields') or _LS_FIELDS_TO_RETURN
        assert bucket is not None

        # Return only the fields that were requested
        field_match = re.findall('items\((.*?)\)', fields)[0]
        actual_fields = set(field_match.split(','))

        object_map = _get_deep(self._objects, [bucket], dict())

        item_list = []
        for object_name, current_object in object_map.items():
            # Filter out on prefix match
            if not object_name.startswith(prefix):
                continue

            # Copy output fields for the requestor
            output_item = dict()
            for current_field in actual_fields:
                output_item[current_field] = current_object[current_field]

            item_list.append(output_item)

        return dict(items=item_list, kwargs=kwargs)

    def list_next(self, list_request, resp):
        """list always returns all results in a single shot"""
        return None

    @mock_api
    def delete(self, bucket=None, object=None):
        bucket_dict = self._objects[bucket]
        del bucket_dict[object]

    @mock_api
    def get_media(self, bucket=None, object=None):
        raise NotImplementedError('See MockGCSClient.download_io')

    @mock_api
    def insert(self, bucket=None, name=None, media_body=None):
        raise NotImplementedError('See MockGCSClient.upload_io')


class MockGCSClientBuckets(object):
    def __init__(self, client):
        assert isinstance(client, MockGCSClient)
        self._client = client
        self._buckets = self._client._cache_buckets

    @mock_api
    def list(self, **kwargs):
        """Emulate buckets().list - fields supported - project, prefix"""
        project = kwargs.get('project')
        prefix = kwargs.get('prefix') or ''

        item_list = []
        for bucket_name, current_bucket in self._buckets.items():
            if not bucket_name.startswith(prefix):
                continue

            if project and project != current_bucket['_projectName']:
                continue

            item_list.append(current_bucket)

        return dict(items=item_list, kwargs=kwargs)

    @mock_api
    def get(self, bucket=None):
        try:
            return self._buckets[bucket]
        except KeyError:
            raise mock_google_error(404)

    @mock_api
    def delete(self, bucket=None):
        del self._buckets[bucket]

    @mock_api
    def insert(self, project=None, body=None):
        assert project is not None
        body = body or dict()

        bucket_name = body['name']
        assert bucket_name not in self._buckets

        # Create an empty cluster
        bucket = _make_bucket_resp(project=project)

        # Then do a deep-update as to what was requested
        bucket = _dict_deep_update(bucket, body)

        self._buckets[bucket_name] = bucket

        return bucket


def _insert_object_resp(bucket=None, name=None, data=None):
    """Fake GCS object metadata"""
    assert type(data) is bytes

    hasher = hashlib.md5()
    hasher.update(data)
    md5_hex_hash = hasher.hexdigest()

    return {
        u'bucket': bucket,
        u'name': name,
        u'md5Hash': _hex_to_base64(md5_hex_hash),
        u'timeCreated': _datetime_to_gcptime(),
        u'size': str(len(data)),
        u'_data': data
    }


def _make_bucket_resp(project=None, now=None):
    """Fake GCS bucket metadata"""
    now_time = _datetime_to_gcptime(now)

    return {
        u'etag': u'CAE=',
        u'kind': u'storage#bucket',
        u'location': u'US',
        u'metageneration': u'1',
        u'owner': {u'entity': u'project-owners-1234567890'},
        u'projectNumber': u'1234567890',
        u'storageClass': u'STANDARD',
        u'timeCreated': now_time,
        u'updated': now_time,
        u'_projectName': project
    }

#############################  END   END   END  ###############################
########################### GCS Client - OVERALL ##############################
#############################  END   END   END  ###############################


############################# BEGIN BEGIN BEGIN ###############################
######################### Dataproc Client - OVERALL ###########################
############################# BEGIN BEGIN BEGIN ###############################
class MockDataprocClient(object):
    """Mock out DataprocJobRunner.api_client...

    TARGET API VERSION - Dataproc API v1

    Emulates Dataproc cluster / job metadata
    Convenience functions for cluster/job state and updating
    """

    def __init__(self, test_case):
        assert isinstance(test_case, MockGoogleAPITestCase)
        self._test_case = test_case

        self._cache_clusters = {}
        self._cache_jobs = {}

        self._client_clusters = MockDataprocClientClusters(self)
        self._client_jobs = MockDataprocClientJobs(self)

        # By default - we always resolve our infinite loops by default to
        # state RUNNING / DONE
        self.cluster_get_advances_states = collections.deque(['RUNNING'])
        self.job_get_advances_states = collections.deque(
            ['SETUP_DONE', 'RUNNING', 'DONE'])

    def clusters(self):
        return self._client_clusters

    def jobs(self):
        return self._client_jobs

    def cluster_create(self, project=None, cluster=None):
        cluster_body = _create_cluster_resp(project=project, cluster=cluster)
        cluster_resp = self._client_clusters.create(
            projectId=cluster_body['projectId'],
            region=_DATAPROC_API_REGION,
            body=cluster_body).execute()
        return cluster_resp

    def get_state(self, cluster_or_job):
        return cluster_or_job['status']['state']

    def update_state(self, cluster_or_job, state=None, prev_state=None):
        old_state = cluster_or_job['status']['state']
        if prev_state:
            assert old_state == prev_state

        if old_state == state:
            return cluster_or_job

        new_status = {
            "state": state,
            "stateStartTime": _datetime_to_gcptime()
        }

        old_status = cluster_or_job.pop('status')
        cluster_or_job['status'] = new_status

        cluster_or_job.setdefault('statusHistory', [])
        cluster_or_job['statusHistory'].append(old_status)

        return cluster_or_job
#############################  END   END   END  ###############################
######################### Dataproc Client - OVERALL ###########################
#############################  END   END   END  ###############################


############################# BEGIN BEGIN BEGIN ###############################
######################### Dataproc Client - Clusters ##########################
############################# BEGIN BEGIN BEGIN ###############################

_DATAPROC_CLUSTER = 'test-cluster-test'
_CLUSTER_REGION = _DATAPROC_API_REGION
_CLUSTER_ZONE = None
_CLUSTER_IMAGE_VERSION = '1.0'
_CLUSTER_STATE = ''
_CLUSTER_MACHINE_TYPE = 'n1-standard-1'
_CLUSTER_NUM_CORE_INSTANCESS = 2


def _datetime_to_gcptime(in_datetime=None):
    in_datetime = in_datetime or datetime.utcnow()
    return in_datetime.isoformat() + 'Z'


def _create_cluster_resp(
        project=None, zone=None, cluster=None, image_version=None,
        machine_type=None, machine_type_master=None, num_core_instancess=None,
        now=None):
    """Fake Dataproc Cluster metadata"""
    project = project or _TEST_PROJECT
    zone = zone or _CLUSTER_ZONE
    cluster = cluster or _DATAPROC_CLUSTER
    image_version = image_version or _CLUSTER_IMAGE_VERSION
    machine_type_master = machine_type_master or _CLUSTER_MACHINE_TYPE
    machine_type = machine_type or _CLUSTER_MACHINE_TYPE
    num_core_instancess = num_core_instancess or _CLUSTER_NUM_CORE_INSTANCESS

    gce_cluster_conf = {
        "zoneUri": (
            "https://www.googleapis.com/compute/v1/projects/%(project)s/"
            "zones/%(zone)s" % locals()),
        "networkUri": (
            "https://www.googleapis.com/compute/v1/projects/%(project)s/"
            "global/networks/default" % locals()),
        "serviceAccountScopes": [
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/bigtable.admin.table",
            "https://www.googleapis.com/auth/bigtable.data",
            "https://www.googleapis.com/auth/cloud.useraccounts.readonly",
            "https://www.googleapis.com/auth/devstorage.full_control",
            "https://www.googleapis.com/auth/devstorage.read_write",
            "https://www.googleapis.com/auth/logging.write"
        ],
    }

    master_conf = {
        "numInstances": 1,
        "instanceNames": [
            "%(cluster)s-m" % locals()
        ],
        "imageUri": (
            "https://www.googleapis.com/compute/v1/projects/cloud-dataproc/"
            "global/images/dataproc-1-0-20160302-200123"),
        "machineTypeUri": (
            "https://www.googleapis.com/compute/v1/projects/%(project)s/"
            "zones/%(zone)s/machineTypes/%(machine_type_master)s" % locals()),
        "diskConfig": {
            "bootDiskSizeGb": 500
        },
    }

    worker_conf = {
        "numInstances": num_core_instancess,
        "instanceNames": [
            '%s-w-%d' % (cluster, num) for num in range(num_core_instancess)],
        "imageUri": (
            "https://www.googleapis.com/compute/v1/projects/cloud-dataproc/"
            "global/images/dataproc-1-0-20160302-200123"),
        "machineTypeUri": (
            "https://www.googleapis.com/compute/v1/projects/%(project)s/"
            "zones/%(zone)s/machineTypes/%(machine_type)s" % locals()),
        "diskConfig": {
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
        "config": {
            "configBucket": "dataproc-801485be-0997-40e7-84a7-00926031747c-us",
            "gceClusterConfig": gce_cluster_conf,
            "masterConfig": master_conf,
            "workerConfig": worker_conf,
            "softwareConfig": software_conf
        },
        "status": {
            "state": "CREATING",
            "stateStartTime": _datetime_to_gcptime(now)
        },
        "clusterUuid": "adb4dc59-d109-4af9-badb-0d8e17e028e1"
    }
    return mock_response


class MockDataprocClientClusters(object):
    def __init__(self, client):
        assert isinstance(client, MockDataprocClient)
        self._client = client
        self._clusters = self._client._cache_clusters

    @mock_api
    def create(self, projectId=None, region=None, body=None):
        assert projectId is not None
        assert region == _DATAPROC_API_REGION

        body = body or dict()

        cluster_name = body['clusterName']

        existing_cluster = _get_deep(self._clusters, [projectId, cluster_name])
        assert not existing_cluster

        # Create an empty cluster
        cluster = _create_cluster_resp()

        # Then do a deep-update as to what was requested
        cluster = _dict_deep_update(cluster, body)

        # Create a local copy of advances states
        cluster['_get_advances_states'] = copy.copy(
            self._client.cluster_get_advances_states)

        _set_deep(self._clusters, [projectId, cluster_name], cluster)

        return cluster

    @mock_api
    def get(self, projectId=None, region=None, clusterName=None):
        assert projectId is not None
        assert region == _DATAPROC_API_REGION

        cluster = _get_deep(self._clusters, [projectId, clusterName])
        if not cluster:
            raise mock_google_error(404)

        # NOTE - TESTING ONLY - Side effect is to advance the state
        advances_states = cluster['_get_advances_states']
        if advances_states:
            next_state = advances_states.popleft()
            self._client.update_state(cluster, state=next_state)

        return cluster

    @mock_api
    def delete(self, projectId=None, region=None, clusterName=None):
        cluster = self.get(
            projectId=projectId,
            region=region,
            clusterName=clusterName,
        ).execute()

        return self._client.update_state(cluster, state='DELETING')

#############################  END   END   END  ###############################
######################### Dataproc Client - Clusters ##########################
#############################  END   END   END  ###############################


############################# BEGIN BEGIN BEGIN ###############################
########################### Dataproc Client - Jobs ############################
############################# BEGIN BEGIN BEGIN ###############################

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


def _submit_hadoop_job_resp(
        project=None, cluster=None, script_name=None, now=None):
    """Fake Dataproc Job metadata"""
    project = project or _TEST_PROJECT
    cluster = cluster or _DATAPROC_CLUSTER
    script_name = script_name or _SCRIPT_NAME
    now = now or datetime.utcnow()

    job_elements = [
        script_name, _USER_NAME, now.strftime('%Y%m%d'),
        now.strftime('%H%M%S'), now.strftime('%f')]

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
            "mainJarFileUri": (
                "file:///usr/lib/hadoop-mapreduce/hadoop-streaming.jar"),
            "args": [],
            "loggingConfig": {}
        },
        "status": {
            "state": "PENDING",
            "stateStartTime": _datetime_to_gcptime(now)
        },
        "driverControlFilesUri": (
            "gs://dataproc-801485be-0997-40e7-84a7-00926031747c-us/"
            "google-cloud-dataproc-metainfo/"
            "8b76d95e-ebdc-4b81-896d-b2c5009b3560/jobs/%(job_id)s/" % locals()
        ),
        "driverOutputResourceUri": (
            "gs://dataproc-801485be-0997-40e7-84a7-00926031747c-us/"
            "google-cloud-dataproc-metainfo/"
            "8b76d95e-ebdc-4b81-896d-b2c5009b3560/jobs/%(job_id)s/"
            "driveroutput" % locals()
        ),
    }
    return mock_response


class MockDataprocClientJobs(object):
    def __init__(self, client):
        assert isinstance(client, MockDataprocClient)
        self._client = client
        self._jobs = self._client._cache_jobs

    @mock_api
    def list(self, **kwargs):
        """Emulate jobs().list -
            fields supported - projectId, region, clusterName, jobStateMatcher
        """
        project_id = kwargs['projectId']
        region = kwargs['region']
        cluster_name = kwargs.get('clusterName')
        job_state_matcher = kwargs.get('jobStateMatcher') or 'ALL'

        assert project_id is not None
        assert region == _DATAPROC_API_REGION

        valid_job_states = _JOB_STATE_MATCHERS[job_state_matcher]

        item_list = []

        job_map = _get_deep(self._jobs, [project_id], dict())

        # Sort all jobs by latest status update time
        jobs_sorted_by_time = sorted(
            job_map.values(), key=lambda j: j['status']['stateStartTime'])
        for current_job in jobs_sorted_by_time:
            job_cluster = current_job['placement']['clusterName']
            job_state = current_job['status']['state']

            # Filter out non-matching clusters and job-states
            if cluster_name and job_cluster != cluster_name:
                continue
            elif job_state not in valid_job_states:
                continue

            item_list.append(current_job)

        return dict(items=item_list, kwargs=kwargs)

    def list_next(self, list_request, resp):
        return None

    @mock_api
    def get(self, projectId=None, region=None, jobId=None):
        assert projectId is not None
        assert region == _DATAPROC_API_REGION

        current_job = _get_deep(self._jobs, [projectId, jobId])
        if not current_job:
            raise mock_google_error(404)

        # NOTE - TESTING ONLY - Side effect is to advance the state
        advances_states = current_job['_get_advances_states']
        if advances_states:
            next_state = advances_states.popleft()
            self._client.update_state(current_job, state=next_state)

        return current_job

    @mock_api
    def cancel(self, projectId=None, region=None, jobId=None):
        assert projectId is not None
        assert region == _DATAPROC_API_REGION

        job = self.get(
            projectId=projectId, region=_DATAPROC_API_REGION, jobId=jobId)
        return self._client.update_state(job, state='CANCEL_PENDING')

    @mock_api
    def delete(self, projectId=None, region=None, jobId=None):
        assert projectId is not None
        assert region == _DATAPROC_API_REGION

        job = self.get(
            projectId=projectId, region=_DATAPROC_API_REGION, jobId=jobId)
        return self._client.update_state(job, state='DELETING')

    @mock_api
    def submit(self, projectId=None, region=None, body=None):
        assert projectId is not None
        assert region == _DATAPROC_API_REGION

        body = body or dict()

        # Create an empty job
        job = _submit_hadoop_job_resp()

        body_job = body.get('job') or dict()

        # Then do a deep-update as to what was requested
        _dict_deep_update(job, body_job)

        # Create a local copy of advances states
        job['_get_advances_states'] = copy.copy(
            self._client.job_get_advances_states)

        _set_deep(self._jobs, [projectId, job['reference']['jobId']], job)

        return job

#############################  END   END   END  ###############################
########################### Dataproc Client - Jobs ############################
#############################  END   END   END  ###############################
