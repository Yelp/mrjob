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
"""Limited mock of google.cloud.storage."""
from google.api_core.exceptions import NotFound


class MockGoogleStorageClient(object):
    """Mock out google.cloud.storage.client.Client

    :param mock_s3_fs: Maps bucket name to a dictionary with the key
                       *blobs*. *blobs* maps object name to
                       a dictionary with the key *data*, which is
                       a bytestring.
    """
    def __init__(self, mock_gcs_fs=None):
        self.mock_gcs_fs = mock_gcs_fs or {}

    def get_bucket(self, bucket_name):
        if bucket_name not in self.mock_gcs_fs:
            raise NotFound(
                'GET https://www.googleapis.com/storage/v1/b/%s'
                '?projection=noAcl: Not Found' % bucket_name)

        return MockGoogleStorageBucket(self, bucket_name)


class MockGoogleStorageBucket(object):
    """Mock out google.cloud.storage.client.Bucket"""
    def __init__(self, client, name):
        self.client = client
        self.name = name

    def blob(self, blob_name, chunk_size=None):
        # always returns something whether it exists or not
        return MockGoogleStorageBlob(blob_name, self, chunk_size=chunk_size)


class MockGoogleStorageBlob(object):
    """Mock out google.cloud.storage.blob.Blob"""
    def __init__(self, name, bucket, chunk_size=None):
        self.name = name
        self.bucket = bucket
        self.chunk_size = chunk_size

    def download_to_file(self, file_obj):
        fs = self.bucket.client.mock_gcs_fs
        if (self.bucket.name not in fs or
                self.name not in fs[self.bucket.name]['blobs']):
            raise NotFound('GET https://www.googleapis.com/download/storage'
                           '/v1/b/%s/o/%s?alt=media: Not Found' %
                           self.bucket.name, self.name)

        file_obj.write(fs[self.bucket.name]['blobs'][self.name]['data'])

    def upload_from_filename(self, filename):
        with open(filename, 'rb') as f:
            data = f.read()

        self.upload_from_string(data)

    def upload_from_string(self, data):
        fs = self.bucket.client.mock_gcs_fs
        if self.bucket.name not in fs:
            raise NotFound('POST https://www.googleapis.com/upload/storage'
                           '/v1/b/%s/o?uploadType=multipart: Not Found' %
                           self.bucket.name)

        fs_objs = fs[self.bucket.name]['blobs']
        fs_obj = fs_objs.setdefault(self.name, dict(data=b''))
        fs_obj['data'] = data
