# Copyright 2009-2012 Yelp
# Copyright 2015-2016 Yelp
# Copyright 2017 Yelp
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
"""Test the cluster termination tool"""
from mrjob.tools.emr.terminate_cluster import main as terminate_main
from mrjob.tools.emr.terminate_cluster import _make_arg_parser

from tests.tools.emr import ToolTestCase


class TerminateToolTestCase(ToolTestCase):

    def test_make_arg_parser(self):
        _make_arg_parser()

    def test_terminate_cluster(self):
        cluster_id = self.make_cluster(pool_clusters=True)
        self.monkey_patch_argv('--quiet', '--no-conf', 'j-MOCKCLUSTER0')

        terminate_main()

        self.simulate_emr_progress(cluster_id)

        cluster = self.mock_emr_clusters[cluster_id]
        self.assertEqual(cluster['Status']['State'], 'TERMINATED')
