# Copyright 2009-2016 Yelp and Contributors
# Copyright 2017 Yelp
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

# see case.py for definition of mock_clusters and mock_gcs_fs


class MockGoogleDataprocClusterClient(object):

    """Mock out google.cloud.dataproc_v1.ClusterControllerClient"""
    def __init__(self, mock_clusters, mock_gcs_fs):
        self.mock_clusters = mock_clusters
        self.mock_gcs_fs = mock_gcs_fs


class MockGoogleDataprocJobClient(object):

    """Mock out google.cloud.dataproc_v1.JobControllerClient"""
    def __init__(self, mock_clusters, mock_gcs_fs):
        self.mock_clusters = mock_clusters
        self.mock_gcs_fs = mock_gcs_fs
