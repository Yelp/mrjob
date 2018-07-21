# Copyright 2009-2012 Yelp and Contributors
# Copyright 2013 Lyft
# Copyright 2015-2016 Yelp
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
from datetime import datetime
from datetime import timedelta
from unittest import TestCase

from mrjob.pool import _est_time_to_hour
from mrjob.pool import _pool_hash_and_name

from tests.mockboto import MockEmrObject
from tests.mockboto import to_iso8601


class EstTimeToEndOfHourTestCase(TestCase):

    def test_empty(self):
        cs = MockEmrObject()
        self.assertEqual(_est_time_to_hour(cs), timedelta(hours=1))

    def test_not_yet_started(self):
        cs = MockEmrObject(
            status=MockEmrObject(
                timeline=MockEmrObject(
                    creationdatetime=to_iso8601(datetime(2010, 6, 6, 4)))))

        self.assertEqual(
            _est_time_to_hour(cs, now=datetime(2010, 6, 6, 4, 35)),
            timedelta(minutes=25))

        self.assertEqual(
            _est_time_to_hour(cs, now=datetime(2010, 6, 6, 5, 20)),
            timedelta(minutes=40))

        self.assertEqual(
            _est_time_to_hour(cs, now=datetime(2010, 6, 6, 4)),
            timedelta(minutes=60))

    def test_started(self):
        cs = MockEmrObject(
            status=MockEmrObject(
                timeline=MockEmrObject(
                    creationdatetime=to_iso8601(datetime(2010, 6, 6, 4, 26)),
                    readydatetime=to_iso8601(datetime(2010, 6, 6, 4, 30)))))

        self.assertEqual(
            _est_time_to_hour(cs, now=datetime(2010, 6, 6, 4, 35)),
            timedelta(minutes=51))

        self.assertEqual(
            _est_time_to_hour(cs, now=datetime(2010, 6, 6, 5, 20)),
            timedelta(minutes=6))

        self.assertEqual(
            _est_time_to_hour(cs, now=datetime(2010, 6, 6, 6, 26)),
            timedelta(minutes=60))

    def test_now_is_automatically_set(self):
        cs = MockEmrObject(
            status=MockEmrObject(
                timeline=MockEmrObject(
                    creationdatetime=to_iso8601(datetime.utcnow()))))

        t = _est_time_to_hour(cs)

        self.assertLessEqual(t, timedelta(minutes=60))
        self.assertGreater(t, timedelta(minutes=59))

    def test_clock_skew(self):
        # make sure something reasonable happens if now is before
        # the start time
        cs = MockEmrObject(
            status=MockEmrObject(
                timeline=MockEmrObject(
                    creationdatetime=to_iso8601(datetime(2010, 6, 6, 4, 26)))))

        self.assertEqual(
            _est_time_to_hour(cs, now=datetime(2010, 6, 6, 4, 25, 59)),
            timedelta(seconds=1))


class TestPoolHashAndName(TestCase):

    def test_empty(self):
        actions = []

        self.assertEqual(_pool_hash_and_name(actions), (None, None))

    def test_pooled_cluster(self):
        actions = [
            MockEmrObject(args=[
                MockEmrObject(
                    value='pool-0123456789abcdef0123456789abcdef'),
                MockEmrObject(value='reflecting'),
            ], name='master'),
        ]

        self.assertEqual(_pool_hash_and_name(actions),
                         ('0123456789abcdef0123456789abcdef', 'reflecting'))

    def test_pooled_cluster_with_other_bootstrap_actions(self):
        actions = [
            MockEmrObject(args=[], name='action 0'),
            MockEmrObject(args=[], name='action 1'),
            MockEmrObject(args=[
                MockEmrObject(
                    value='pool-0123456789abcdef0123456789abcdef'),
                MockEmrObject(value='reflecting'),
            ], name='master'),
        ]

        self.assertEqual(_pool_hash_and_name(actions),
                         ('0123456789abcdef0123456789abcdef', 'reflecting'))

    def test_pooled_cluster_with_max_hours_idle(self):
        # max hours idle is added AFTER the master bootstrap script,
        # which was a problem when we just look at the last action
        actions = [
            MockEmrObject(args=[
                MockEmrObject(
                    value='pool-0123456789abcdef0123456789abcdef'),
                MockEmrObject(value='reflecting'),
            ], name='master'),
            MockEmrObject(args=[
                MockEmrObject(value='900'),
                MockEmrObject(value='300'),
            ], name='idle timeout'),
        ]

        self.assertEqual(_pool_hash_and_name(actions),
                         ('0123456789abcdef0123456789abcdef', 'reflecting'))

    def test_first_arg_doesnt_start_with_pool(self):
        actions = [
            MockEmrObject(args=[
                MockEmrObject(value='cowsay'),
                MockEmrObject(value='mrjob'),
            ], name='master'),
        ]

        self.assertEqual(_pool_hash_and_name(actions), (None, None))

    def test_too_many_args(self):
        actions = [
            MockEmrObject(args=[
                MockEmrObject(value='cowsay'),
                MockEmrObject(value='-b'),
                MockEmrObject(value='mrjob'),
            ], name='master'),
        ]

        self.assertEqual(_pool_hash_and_name(actions), (None, None))

    def test_too_few_args(self):
        actions = [
            MockEmrObject(args=[
                MockEmrObject(
                    value='pool-0123456789abcdef0123456789abcdef'),
            ], name='master'),
        ]

        self.assertEqual(_pool_hash_and_name(actions), (None, None))

    def test_bootstrap_action_isnt_named_master(self):
        actions = [
            MockEmrObject(args=[
                MockEmrObject(
                    value='pool-0123456789abcdef0123456789abcdef'),
                MockEmrObject(value='reflecting'),
            ], name='apprentice'),
        ]

        self.assertEqual(_pool_hash_and_name(actions), (None, None))
