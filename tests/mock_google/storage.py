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
import md5
from base64 import b64encode
from copy import deepcopy

from google.api_core.exceptions import Conflict
from google.api_core.exceptions import NotFound


class MockGoogleStorageClient(object):
    """Mock out google.cloud.storage.client.Client

    :param mock_s3_fs: Maps bucket name to a dictionary with the key
                       *blobs*. *blobs* maps object name to
                       a dictionary with the key *data*, which is
                       a bytestring.
    """
    def __init__(self, mock_gcs_fs):
        self.mock_gcs_fs = mock_gcs_fs

    def bucket(self, bucket_name):
        return MockGoogleStorageBucket(self, bucket_name)

    def get_bucket(self, bucket_name):
        if bucket_name not in self.mock_gcs_fs:
            raise NotFound(
                'GET https://www.googleapis.com/storage/v1/b/%s'
                '?projection=noAcl: Not Found' % bucket_name)

        return self.bucket(bucket_name)

    def list_buckets(self, prefix=None):
        for bucket_name in sorted(self.mock_gcs_fs):
            if prefix and not bucket_name.startswith(prefix):
                continue

            yield self.get_bucket(bucket_name)


class MockGoogleStorageBucket(object):
    """Mock out google.cloud.storage.client.Bucket"""
    def __init__(self, client, name):
        self.client = client
        self.name = name

        # for setting location manually
        self._changes = set()
        self._properties = {}

    def blob(self, blob_name, chunk_size=None):
        # always returns something whether it exists or not
        return MockGoogleStorageBlob(blob_name, self, chunk_size=chunk_size)

    def create(self):
        if self.exists():
            raise Conflict(
                'POST https://www.googleapis.com/storage/v1'
                '/b?project=%s: Sorry, that name is not available.'
                ' Please try a different one.')

        if 'location' in self._changes and 'location' in self._properties:
            location = self._properties['location'].upper()
        else:
            location = 'US'

        self.client.mock_gcs_fs[self.name] = dict(
            blobs={}, lifecycle_rules=[], location=location)

    def exists(self):
        return self.name in self.client.mock_gcs_fs

    def get_blob(self, blob_name):
        fs = self.client.mock_gcs_fs

        if self.name in fs and blob_name in fs[self.name]['blobs']:
            blob = self.blob(blob_name)
            blob._set_md5_hash()
            return blob

    @property
    def lifecycle_rules(self):
        fs = self.client.mock_gcs_fs

        if self.name in fs:
            return deepcopy(fs[self.name]['lifecycle_rules'])
        else:
            # google-cloud-sdk silently ignores missing buckets
            return []

    @lifecycle_rules.setter
    def lifecycle_rules(self, rules):
        fs = self.client.mock_gcs_fs

        if self.name in fs:
            fs[self.name]['lifecycle_rules'] = deepcopy(rules)
        # google-cloud-sdk silently ignores buckets that don't exist

    def list_blobs(self, prefix=None):
        fs = self.client.mock_gcs_fs

        if self.name not in fs:
            raise NotFound('GET https://www.googleapis.com/storage/v1/b'
                           '/%s/o?projection=noAcl: Not Found' % self.name)

        for blob_name in sorted(fs[self.name]['blobs']):
            if prefix and not blob_name.startswith(prefix):
                continue

            yield self.blob(blob_name)

    @property
    def location(self):
        fs = self.client.mock_gcs_fs

        if self.name in fs:
            return fs[self.name]['location']
        else:
            # google-cloud-sdk silently ignores missing buckets
            return []


class MockGoogleStorageBlob(object):
    """Mock out google.cloud.storage.blob.Blob"""
    def __init__(self, name, bucket, chunk_size=None):
        self.name = name
        self.bucket = bucket
        self.chunk_size = chunk_size

        # this is only set when we call self.get_blob() or upload new data
        self.md5_hash = None

    def delete(self):
        fs = self.bucket.client.mock_gcs_fs

        if (self.bucket.name not in fs or
                self.name not in fs[self.bucket.name]['blobs']):
            raise NotFound('DELETE https://www.googleapis.com/storage/v1/b'
                           '/%s/o/%s: Not Found' %
                           (self.bucket.name, self.name))

        del fs[self.bucket.name]['blobs'][self.name]

    def download_as_string(self):
        fs = self.bucket.client.mock_gcs_fs

        try:
            return fs[self.bucket.name]['blobs'][self.name]['data']
        except KeyError:
            raise NotFound('GET https://www.googleapis.com/download/storage'
                           '/v1/b/%s/o/%s?alt=media: Not Found' %
                           (self.bucket.name, self.name))

    def download_to_file(self, file_obj):
        data = self.download_as_string()
        file_obj.write(data)

    def _set_md5_hash(self):
        # call this when we upload data, or when we _get_blob
        try:
            self.md5_hash = b64encode(
                md5.new(self.download_as_string()).digest())
        except NotFound:
            pass

    @property
    def size(self):
        try:
            return len(self.download_as_string())
        except NotFound:
            return None

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

        self._set_md5_hash()
