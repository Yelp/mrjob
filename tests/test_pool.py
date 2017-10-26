# Copyright 2009-2012 Yelp and Contributors
# Copyright 2013 Lyft
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
from unittest import TestCase

from mrjob.pool import _legacy_pool_hash_and_name
from mrjob.pool import _pool_hash_and_name


class TestPoolHashAndName(TestCase):

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


class TestLegacyPoolHashAndName(TestCase):

    def test_empty(self):
        actions = []

        self.assertEqual(_legacy_pool_hash_and_name(actions), (None, None))

    def test_pooled_cluster(self):
        actions = [
            dict(
                Args=['pool-0123456789abcdef0123456789abcdef', 'reflecting'],
                Name='master',
            ),
        ]

        self.assertEqual(_legacy_pool_hash_and_name(actions),
                         ('0123456789abcdef0123456789abcdef', 'reflecting'))

    def test_pooled_cluster_with_other_bootstrap_actions(self):
        actions = [
            dict(Args=[], Name='action 0'),
            dict(Args=[], Name='action 1'),
            dict(
                Args=['pool-0123456789abcdef0123456789abcdef', 'reflecting'],
                Name='master',
            ),
        ]

        self.assertEqual(_legacy_pool_hash_and_name(actions),
                         ('0123456789abcdef0123456789abcdef', 'reflecting'))

    def test_pooled_cluster_with_max_mins_idle(self):
        # max-mins-idle script is added AFTER the master bootstrap script,
        # which was a problem when we just look at the last action
        actions = [
            dict(
                Args=['pool-0123456789abcdef0123456789abcdef', 'reflecting'],
                Name='master',
            ),
            dict(
                Args=['900', '300'],
                Name='idle timeout',
            ),
        ]

        self.assertEqual(_legacy_pool_hash_and_name(actions),
                         ('0123456789abcdef0123456789abcdef', 'reflecting'))

    def test_first_arg_doesnt_start_with_pool(self):
        actions = [
            dict(
                Args=['cowsay', 'mrjob'],
                Name='master',
            ),
        ]

        self.assertEqual(_legacy_pool_hash_and_name(actions), (None, None))

    def test_too_many_args(self):
        actions = [
            dict(
                Args=['cowsay', '-b', 'mrjob'],
                Name='master',
            ),
        ]

        self.assertEqual(_legacy_pool_hash_and_name(actions), (None, None))

    def test_too_few_args(self):
        actions = [
            dict(
                Args=['pool-0123456789abcdef0123456789abcdef'],
                Name='master',
            ),
        ]

        self.assertEqual(_legacy_pool_hash_and_name(actions), (None, None))

    def test_bootstrap_action_isnt_named_master(self):
        actions = [
            dict(
                Args=['pool-0123456789abcdef0123456789abcdef', 'reflecting'],
                Name='apprentice',
            ),
        ]

        self.assertEqual(_legacy_pool_hash_and_name(actions), (None, None))
