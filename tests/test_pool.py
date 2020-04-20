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
from mrjob.pool import _pool_hash_and_name
from mrjob.pool import _make_cluster_lock
from mrjob.pool import _parse_cluster_lock

from tests.sandbox import BasicTestCase


class PoolHashAndNameTestCase(BasicTestCase):

    def test_empty(self):
        self.assertEqual(_pool_hash_and_name({}), (None, None))

    def test_pooled_cluster(self):
        cluster = dict(Tags=[
            dict(Key='__mrjob_pool_hash',
                 Value='0123456789abcdef0123456789abcdef'),
            dict(Key='__mrjob_pool_name',
                 Value='reflecting'),
        ])

        self.assertEqual(_pool_hash_and_name(cluster),
                         ('0123456789abcdef0123456789abcdef', 'reflecting'))

    def test_pooled_cluster_with_other_tags(self):
        cluster = dict(Tags=[
            dict(Key='__mrjob_pool_hash',
                 Value='0123456789abcdef0123456789abcdef'),
            dict(Key='__mrjob_pool_name',
                 Value='reflecting'),
            dict(Key='price', Value='$9.99'),
        ])

        self.assertEqual(_pool_hash_and_name(cluster),
                         ('0123456789abcdef0123456789abcdef', 'reflecting'))


class ParseClusterLockTestCase(BasicTestCase):

    def test_empty(self):
        self.assertRaises(ValueError, _parse_cluster_lock, '')

    def test_basic(self):
        self.assertEqual(
            _parse_cluster_lock(
                'mr_wc.davidmarin.20200419.185348.359278 1587405489.550173'),
            ('mr_wc.davidmarin.20200419.185348.359278', 1587405489.550173)
        )

    def test_round_trip(self):
        self.assertEqual(
            _parse_cluster_lock(_make_cluster_lock(
                'mr_wc.davidmarin.20200419.185348.359278', 1587405489.550173)),
            ('mr_wc.davidmarin.20200419.185348.359278', 1587405489.550173)
        )

    def test_too_few_fields(self):
        self.assertRaises(
            ValueError,
            _parse_cluster_lock, 'mr_wc.davidmarin.20200419.185348.359278')

    def test_too_many_fields(self):
        self.assertRaises(
            ValueError,
            _parse_cluster_lock,
            'mr_wc.davidmarin.20200419.185348.359278 1587405489.550173 yay')

    def test_bad_timestamp(self):
        # shouldn't raise TypeError
        self.assertRaises(
            ValueError,
            _parse_cluster_lock,
            'mr_wc.davidmarin.20200419.185348.359278 in-a-minute'
        )
