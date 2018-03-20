# Copyright 2018 Google Inc.
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
"""Limited mock of google-cloud-sdk for tests
"""
from io import BytesIO

from google.oauth2.credentials import Credentials

from mrjob.fs.gcs import parse_gcs_uri

from .dataproc import MockGoogleDataprocClusterClient
from .dataproc import MockGoogleDataprocJobClient
from .storage import MockGoogleStorageClient
from tests.mr_two_step_job import MRTwoStepJob
from tests.py2 import patch
from tests.sandbox import SandboxedTestCase

_TEST_PROJECT = 'test-mrjob:test-project'


class MockGoogleTestCase(SandboxedTestCase):

    def setUp(self):
        super(MockGoogleTestCase, self).setUp()

        # maps (project_id, region, cluster_name) to a
        # google.cloud.dataproc_v1.types.Cluster
        self.mock_clusters = {}

        # maps (project_id, region, job_name) to a
        # google.cloud.dataproc_v1.types.Job
        self.mock_jobs = {}

        # set this to False to make jobs ERROR
        self.mock_jobs_succeed = True

        # mock credentials, returned by mock google.auth.default()
        self.mock_credentials = Credentials('mock_token')

        # mock project ID, returned by mock google.auth.default()
        self.mock_project_id = 'mock-project-12345'

        # Maps bucket name to a dictionary with the key
        # *blobs*. *blobs* maps object name to
        # a dictionary with the key *data*, which is
        # a bytestring.
        self.mock_gcs_fs = {}

        self.start(patch('google.auth.default', self.auth_default))

        self.start(patch('google.cloud.dataproc_v1.ClusterControllerClient',
                         self.cluster_client))

        self.start(patch('google.cloud.dataproc_v1.JobControllerClient',
                         self.job_client))

        self.start(patch('google.cloud.storage.client.Client',
                         self.storage_client))

        self.start(patch('time.sleep'))

    def auth_default(self):
        return (self.mock_credentials, self.mock_project_id)

    def cluster_client(self, channel=None, credentials=None):
        return MockGoogleDataprocClusterClient(
            mock_clusters=self.mock_clusters,
            mock_gcs_fs=self.mock_gcs_fs,
            mock_jobs=self.mock_jobs,
        )

    def job_client(self, channel=None, credentials=None):
        return MockGoogleDataprocJobClient(
            mock_clusters=self.mock_clusters,
            mock_gcs_fs=self.mock_gcs_fs,
            mock_jobs=self.mock_jobs,
            mock_jobs_succeed=self.mock_jobs_succeed,
        )

    def storage_client(self, project=None, credentials=None):
        return MockGoogleStorageClient(mock_gcs_fs=self.mock_gcs_fs)

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
        client = self.storage_client()

        for uri, data in gcs_uri_to_data_map.items():
            bucket_name, blob_name = parse_gcs_uri(uri)

            bucket = client.bucket(bucket_name)
            if not bucket.exists():
                bucket.create()

            blob = bucket.blob(blob_name)
            blob.upload_from_string(data)

    def put_job_output_parts(self, dataproc_runner, raw_parts):
        """Generate fake output on GCS for the given Dataproc runner."""
        assert type(raw_parts) is list

        base_uri = dataproc_runner.get_output_dir()
        gcs_multi_dict = dict()
        for part_num, part_data in enumerate(raw_parts):
            gcs_uri = base_uri + 'part-%05d' % part_num
            gcs_multi_dict[gcs_uri] = part_data

        self.put_gcs_multi(gcs_multi_dict)
