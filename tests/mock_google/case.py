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
from mrjob.fs.gcs import parse_gcs_uri

from tests.py2 import patch
from tests.sandbox import SandboxedTestCase


from .storage import MockGoogleStorageClient


class MockGoogleTestCase(SandboxedTestCase):

    def setUp(self):
        # Maps bucket name to a dictionary with the key
        # *blobs*. *blobs* maps object name to
        # a dictionary with the key *data*, which is
        # a bytestring.
        self.mock_gcs_fs = {}

        self.start(patch('mrjob.fs.gcs.StorageClient',
                         MockGoogleStorageClient))

    def put_gcs_multi(self, gcs_uri_to_data_map):
        for uri, data in gcs_uri_to_data_map.items():
            bucket_name, path = parse_gcs_uri(uri)

            mock_bucket = self.mock_gcs_fs.setdefault(
                bucket_name, dict(objects={}))

            mock_bucket[path] = dict(data=data)
