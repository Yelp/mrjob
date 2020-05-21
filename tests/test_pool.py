# Copyright 2009-2012 Yelp and Contributors
# Copyright 2013 Lyft
# Copyright 2015-2018 Yelp
# Copyright 2019 Yelp
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
import time

from mrjob.emr import EMRJobRunner
from mrjob.pool import _attempt_to_lock_cluster
from mrjob.pool import _attempt_to_unlock_cluster
from mrjob.pool import _get_cluster_state_and_lock
from mrjob.pool import _make_cluster_lock
from mrjob.pool import _parse_cluster_lock
from mrjob.pool import _pool_hash
from mrjob.pool import _pool_name
from mrjob.pool import _POOL_LOCK_KEY

from tests.mock_boto3 import MockBoto3TestCase
from tests.py2 import patch
from tests.sandbox import BasicTestCase


class PoolHashAndNameTestCase(BasicTestCase):

    def test_empty(self):
        self.assertEqual(_pool_hash({}), None)

    def test_pooled_cluster(self):
        cluster = dict(Tags=[
            dict(Key='__mrjob_pool_hash',
                 Value='0123456789abcdef0123456789abcdef'),
        ])

        self.assertEqual(_pool_hash(cluster),
                         '0123456789abcdef0123456789abcdef')

    def test_pooled_cluster_with_other_tags(self):
        cluster = dict(Tags=[
            dict(Key='__mrjob_pool_hash',
                 Value='0123456789abcdef0123456789abcdef'),
            dict(Key='__mrjob_pool_name',
                 Value='reflecting'),
            dict(Key='price', Value='$9.99'),
        ])

        self.assertEqual(_pool_hash(cluster),
                         '0123456789abcdef0123456789abcdef')


class PoolNameTestCase(BasicTestCase):

    def test_empty(self):
        self.assertEqual(_pool_name({}), None)

    def test_pooled_cluster(self):
        cluster = dict(Tags=[
            dict(Key='__mrjob_pool_name',
                 Value='reflecting'),
        ])

        self.assertEqual(_pool_name(cluster), 'reflecting')

    def test_pooled_cluster_with_other_tags(self):
        cluster = dict(Tags=[
            dict(Key='__mrjob_pool_hash',
                 Value='0123456789abcdef0123456789abcdef'),
            dict(Key='__mrjob_pool_name',
                 Value='reflecting'),
            dict(Key='price', Value='$9.99'),
        ])

        self.assertEqual(_pool_name(cluster), 'reflecting')


class ParseClusterLockTestCase(BasicTestCase):

    def test_empty(self):
        self.assertRaises(ValueError, _parse_cluster_lock, '')

    def test_None(self):
        # this happens when the lock tag is not set
        self.assertRaises(TypeError, _parse_cluster_lock, None)

    def test_basic(self):
        self.assertEqual(
            _parse_cluster_lock(
                'mr_wc.dmarin.20200419.185348.359278 1587405489.550173'),
            ('mr_wc.dmarin.20200419.185348.359278', 1587405489.550173)
        )

    def test_round_trip(self):
        self.assertEqual(
            _parse_cluster_lock(_make_cluster_lock(
                'mr_wc.dmarin.20200419.185348.359278', 1587405489.550173)),
            ('mr_wc.dmarin.20200419.185348.359278', 1587405489.550173)
        )

    def test_too_few_fields(self):
        self.assertRaises(
            ValueError,
            _parse_cluster_lock, 'mr_wc.dmarin.20200419.185348.359278')

    def test_too_many_fields(self):
        self.assertRaises(
            ValueError,
            _parse_cluster_lock,
            'mr_wc.dmarin.20200419.185348.359278 1587405489.550173 yay')

    def test_bad_timestamp(self):
        # shouldn't raise TypeError
        self.assertRaises(
            ValueError,
            _parse_cluster_lock,
            'mr_wc.dmarin.20200419.185348.359278 in-a-minute'
        )


class AttemptToLockClusterTestCase(MockBoto3TestCase):

    def setUp(self):
        super(AttemptToLockClusterTestCase, self).setUp()

        self.emr_client = self.client('emr')
        self.cluster_id = EMRJobRunner().make_persistent_cluster()
        # get into WAITING state
        self.simulate_emr_progress(self.cluster_id)
        self.simulate_emr_progress(self.cluster_id)

        self.log = self.start(patch('mrjob.pool.log'))
        self.mock_sleep = self.start(patch('time.sleep'))

        self.time = time.time  # save for safekeeping
        self.mock_time = self.start(patch('time.time', side_effect=time.time))

        self.our_key = 'mr_wc.dmarin.20200419.185348.359278'
        self.their_key = 'mr_wc.them.20200419.185348.999999'

    def test_cluster_with_no_lock(self):
        acquired = _attempt_to_lock_cluster(
            self.emr_client, self.cluster_id, self.our_key)

        self.assertTrue(acquired)
        self.mock_sleep.assert_called_once_with(10.0)

    def test_running_cluster(self):
        self.mock_emr_clusters[self.cluster_id]['Status']['State'] = 'RUNNING'

        acquired = _attempt_to_lock_cluster(
            self.emr_client, self.cluster_id, self.our_key)

        self.assertFalse(acquired)
        self.assertFalse(self.mock_sleep.called)

    def test_terminating_cluster(self):
        self.emr_client.terminate_job_flows(JobFlowIds=[self.cluster_id])

        acquired = _attempt_to_lock_cluster(
            self.emr_client, self.cluster_id, self.our_key)

        self.assertFalse(acquired)
        self.assertFalse(self.mock_sleep.called)

    def test_cluster_with_current_lock(self):
        they_acquired = _attempt_to_lock_cluster(
            self.emr_client, self.cluster_id, self.their_key)
        self.assertTrue(they_acquired)

        we_acquired = _attempt_to_lock_cluster(
            self.emr_client, self.cluster_id, self.our_key)
        self.assertFalse(we_acquired)

        self.mock_sleep.assert_called_with(10.0)

    def test_cluster_with_expired_lock(self):
        they_acquired = _attempt_to_lock_cluster(
            self.emr_client, self.cluster_id, self.their_key)
        self.assertTrue(they_acquired)

        # 60 seconds later...
        self.mock_time.side_effect = lambda: self.time() + 60.0
        we_acquired = _attempt_to_lock_cluster(
            self.emr_client, self.cluster_id, self.our_key)
        self.assertTrue(we_acquired)

    def test_cluster_with_bad_lock(self):
        # spaces in job key result in invalid lock
        _attempt_to_lock_cluster(
            self.emr_client, self.cluster_id, 'blah blah blah')

        acquired = _attempt_to_lock_cluster(
            self.emr_client, self.cluster_id, self.our_key)

        self.assertTrue(acquired)

    def test_too_slow_to_read_tags(self):
        start = self.time()
        self.mock_time.side_effect = [start, start + 6.0]

        acquired = _attempt_to_lock_cluster(
            self.emr_client, self.cluster_id, self.our_key)
        self.assertFalse(acquired)

        self.assertFalse(self.mock_sleep.called)

    def test_too_slow_to_check_own_lock(self):
        start = self.time()
        self.mock_time.side_effect = [start, start + 3.0, start + 21.0]

        acquired = _attempt_to_lock_cluster(
            self.emr_client, self.cluster_id, self.our_key)
        self.assertFalse(acquired)

        self.assertTrue(self.mock_sleep.called)

    def test_lock_overwritten_with_valid_key(self):

        def _overwrite_lock(_):
            self.emr_client.add_tags(
                ResourceId=self.cluster_id,
                Tags=[dict(
                    Key=_POOL_LOCK_KEY,
                    Value=_make_cluster_lock(self.their_key, self.time())
                )],
            )

        self.mock_sleep.side_effect = _overwrite_lock

        acquired = _attempt_to_lock_cluster(
            self.emr_client, self.cluster_id, self.our_key)
        self.assertFalse(acquired)

    def test_lock_overwritten_with_invalid_key(self):

        def _overwrite_lock(_):
            self.emr_client.add_tags(
                ResourceId=self.cluster_id,
                Tags=[dict(
                    Key=_POOL_LOCK_KEY,
                    Value='garbage'
                )],
            )

        self.mock_sleep.side_effect = _overwrite_lock

        acquired = _attempt_to_lock_cluster(
            self.emr_client, self.cluster_id, self.our_key)
        self.assertFalse(acquired)

    def test_lock_deleted(self):

        def _overwrite_lock(_):
            self.emr_client.remove_tags(
                ResourceId=self.cluster_id,
                TagKeys=[_POOL_LOCK_KEY],
            )

        self.mock_sleep.side_effect = _overwrite_lock

        acquired = _attempt_to_lock_cluster(
            self.emr_client, self.cluster_id, self.our_key)
        self.assertFalse(acquired)

    def test_cluster_terminated_after_locking(self):
        def _terminate_cluster(_):
            self.emr_client.terminate_job_flows(JobFlowIds=[self.cluster_id])

        self.mock_sleep.side_effect = _terminate_cluster

        acquired = _attempt_to_lock_cluster(
            self.emr_client, self.cluster_id, self.our_key)
        self.assertFalse(acquired)


class AttemptToUnlockClusterTestCase(MockBoto3TestCase):

    def setUp(self):
        super(AttemptToUnlockClusterTestCase, self).setUp()

        self.emr_client = self.client('emr')

        self.cluster_id = EMRJobRunner().make_persistent_cluster()
        # get into WAITING state
        self.simulate_emr_progress(self.cluster_id)
        self.simulate_emr_progress(self.cluster_id)

        self.log = self.start(patch('mrjob.pool.log'))

        self.our_key = 'mr_wc.dmarin.20200419.185348.359278'

    def _get_cluster_lock(self):
        return _get_cluster_state_and_lock(
            self.emr_client, self.cluster_id)[1]

    def test_remove_existing_tags(self):
        _attempt_to_lock_cluster(
            self.emr_client, self.cluster_id, self.our_key)

        self.assertIsNotNone(self._get_cluster_lock())

        unlocked = _attempt_to_unlock_cluster(
            self.emr_client, self.cluster_id)

        self.assertTrue(unlocked)
        self.assertIsNone(self._get_cluster_lock())

    def test_okay_if_no_tag(self):
        unlocked = _attempt_to_unlock_cluster(
            self.emr_client, self.cluster_id)

        self.assertTrue(unlocked)
        self.assertIsNone(self._get_cluster_lock())

    def test_okay_if_cluster_terminated(self):
        _attempt_to_lock_cluster(
            self.emr_client, self.cluster_id, self.our_key)

        self.assertIsNotNone(self._get_cluster_lock())

        self.emr_client.terminate_job_flows(JobFlowIds=[self.cluster_id])

        cluster = self.emr_client.describe_cluster(
            ClusterId=self.cluster_id)['Cluster']

        self.assertEqual(cluster['Status']['State'], 'TERMINATED')

        unlocked = _attempt_to_unlock_cluster(
            self.emr_client, self.cluster_id)

        # failed, but didn't crash
        self.assertFalse(unlocked)
        self.assertIsNotNone(self._get_cluster_lock())
