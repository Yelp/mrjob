# -*- coding: utf-8 -*-
# Copyright 2009-2019 Yelp and Contributors
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
import datetime
import functools
import multiprocessing
import os
import signal
import tempfile

import boto3

from mrjob.cache import ClusterCache
from tests.mock_boto3 import MockBoto3TestCase


class TimeoutError(Exception):
    pass


def _handle_timeout(signum, frame):
    raise TimeoutError("timeout error")


# module-level so we can pickle it
def _describe_cluster(client, tmp_file, cluster_id, num):
    cache = ClusterCache(client, tmp_file, 5)
    return cache.describe_cluster(cluster_id)


class CacheTestCase(MockBoto3TestCase):

    def _create_cluster(self):
        return self.client.run_job_flow(
            Instances=dict(
                InstanceCount=1,
                KeepJobFlowAliveWhenNoSteps=True,
                MasterInstanceType='m1.medium',
            ),
            JobFlowRole='fake-instance-profile',
            Name='Development Cluster',
            ReleaseLabel='emr-5.0.0',
            ServiceRole='fake-service-role',
        )['JobFlowId']

    def setUp(self):
        super(CacheTestCase, self).setUp()

        self.client = boto3.client('emr')
        self.tmp_file = os.path.join(tempfile.mkdtemp(), 'cache')
        self.cluster_id = self._create_cluster()

    def tearDown(self):
        self.client.describe_cluster.__func__.calls = 0
        open(self.tmp_file, 'w+').close()

    def test_cache_setup(self):
        """Tests the cache setup method"""
        ClusterCache.setup(self.tmp_file)
        self.assertTrue(os.path.isfile(self.tmp_file))
        self.assertTrue(os.path.isfile(self.tmp_file + '.age_marker'))

    def test_basics(self):
        """Tests the basic cluster operations."""
        ClusterCache.setup(self.tmp_file)
        cache = ClusterCache(self.client, self.tmp_file, 5)

        # describe new cluster and ensure we call describe
        result_1 = cache.describe_cluster(self.cluster_id)
        self.assertEqual(self.client.describe_cluster.calls, 1)

        # describe the same cluster and ensure we hit the cache
        result_2 = cache.describe_cluster(self.cluster_id)
        self.assertEqual(self.client.describe_cluster.calls, 1)

        # ensure results are equal (other than the created time, since we
        # stringify datetimes in the cache file)
        self.assertEqual(self.cluster_id, result_1['Id'])
        del result_1['Status']['Timeline']['CreationDateTime']
        del result_2['Status']['Timeline']['CreationDateTime']
        self.assertEqual(result_1, result_2)

    def test_basics_two(self):
        """Tests the basic cluster operations with two clusters."""
        ClusterCache.setup(self.tmp_file)
        cache = ClusterCache(self.client, self.tmp_file, 5)

        new_cluster_id = self._create_cluster()

        # describe both clusters
        result_1 = cache.describe_cluster(self.cluster_id)
        self.assertEqual(self.client.describe_cluster.calls, 1)
        result_2 = cache.describe_cluster(new_cluster_id)
        self.assertEqual(self.client.describe_cluster.calls, 2)
        result_3 = cache.describe_cluster(new_cluster_id)
        self.assertEqual(self.client.describe_cluster.calls, 2)

        # ensure results are equal
        self.assertNotEqual(result_1['Id'], result_2['Id'])
        del result_2['Status']['Timeline']['CreationDateTime']
        del result_3['Status']['Timeline']['CreationDateTime']
        self.assertEqual(result_2, result_3)

    def test_expiry(self):
        """Tests that the cache is correctly cleaned when required."""
        ClusterCache.setup(self.tmp_file)
        cache = ClusterCache(self.client, self.tmp_file, 3)

        # describe new cluster and ensure we call describe
        cache.describe_cluster(self.cluster_id)
        self.assertEqual(self.client.describe_cluster.calls, 1)

        stinfo = os.stat(self.tmp_file)
        secs_in_day = datetime.timedelta(days=1).total_seconds()

        # move age-marker back one day
        os.utime(self.tmp_file + '.age_marker',
                 (stinfo.st_atime, stinfo.st_mtime - 1*secs_in_day))

        # there should be no calls to descibe
        cache.describe_cluster(self.cluster_id)
        self.assertEqual(self.client.describe_cluster.calls, 1)

        # move age-marker back 5 days
        os.utime(self.tmp_file + '.age_marker',
                 (stinfo.st_atime, stinfo.st_mtime - 5*secs_in_day))

        # the cache should truncate and thus we call describe
        cache.describe_cluster(self.cluster_id)
        self.assertEqual(self.client.describe_cluster.calls, 2)

    def test_locking(self):
        """Tests the basic locking functionality around the cache."""
        ClusterCache.setup(self.tmp_file)
        cache1 = ClusterCache(self.client, self.tmp_file, 3)
        cache2 = ClusterCache(self.client, self.tmp_file, 3)

        # lock the first cache, set a timeout of 1 second, and then try to
        # lock the second cache; check that this timed out
        with cache1.cache_mutex('r+'):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.setitimer(signal.ITIMER_REAL, 1)
            with self.assertRaises(TimeoutError):
                with cache2.cache_mutex('r+'):
                    self.assertTrue(False)

    def test_interleaving(self):
        """Tests there are no issues accessing the cache in parallel."""
        ClusterCache.setup(self.tmp_file)

        worker = functools.partial(_describe_cluster, self.client,
                                   self.tmp_file, self.cluster_id)
        pool = multiprocessing.Pool(10)
        results = pool.map(worker, range(10))

        for result in results:
            self.assertEqual(self.cluster_id, result['Id'])
