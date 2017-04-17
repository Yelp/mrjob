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

from dateutil.tz import tzutc

from mrjob.aws import _boto3_now
from mrjob.pool import _est_time_to_hour
from mrjob.pool import _pool_hash_and_name

from tests.py2 import TestCase


class EstTimeToEndOfHourTestCase(TestCase):

    def test_empty(self):
        cs = dict()
        self.assertEqual(_est_time_to_hour(cs), timedelta(hours=1))

    def test_not_yet_started(self):
        cs = dict(
            Status=dict(
                Timeline=dict(
                    CreationDateTime=datetime(
                        2010, 6, 6, 4, tzinfo=tzutc()))))

        self.assertEqual(
            _est_time_to_hour(cs, now=datetime(
                2010, 6, 6, 4, 35, tzinfo=tzutc())),
            timedelta(minutes=25))

        self.assertEqual(
            _est_time_to_hour(cs, now=datetime(
                2010, 6, 6, 5, 20, tzinfo=tzutc())),
            timedelta(minutes=40))

        self.assertEqual(
            _est_time_to_hour(cs, now=datetime(
                2010, 6, 6, 4, tzinfo=tzutc())),
            timedelta(minutes=60))

    def test_started(self):
        cs = dict(
            Status=dict(
                Timeline=dict(
                    CreationDateTime=datetime(
                        2010, 6, 6, 4, 26, tzinfo=tzutc())),
                    ReadyDateTime=datetime(
                        2010, 6, 6, 4, 30, tzinfo=tzutc())))

        self.assertEqual(
            _est_time_to_hour(cs, now=datetime(
                2010, 6, 6, 4, 35, tzinfo=tzutc())),
            timedelta(minutes=51))

        self.assertEqual(
            _est_time_to_hour(cs, now=datetime(
                2010, 6, 6, 5, 20, tzinfo=tzutc())),
            timedelta(minutes=6))

        self.assertEqual(
            _est_time_to_hour(cs, now=datetime(
                2010, 6, 6, 6, 26, tzinfo=tzutc())),
            timedelta(minutes=60))

    def test_now_is_automatically_set(self):
        cs = dict(
            Status=dict(
                Timeline=dict(
                    CreationDateTime=_boto3_now())))

        t = _est_time_to_hour(cs)

        self.assertLessEqual(t, timedelta(minutes=60))
        self.assertGreater(t, timedelta(minutes=59))

    def test_clock_skew(self):
        # make sure something reasonable happens if now is before
        # the start time
        cs = dict(
            Status=dict(
                Timeline=dict(
                    CreationDateTime=datetime(
                        2010, 6, 6, 4, 26, tzinfo=tzutc()))))

        self.assertEqual(
            _est_time_to_hour(cs, now=datetime(
                2010, 6, 6, 4, 25, 59, tzinfo=tzutc())),
            timedelta(seconds=1))


class TestPoolHashAndName(TestCase):

    def test_empty(self):
        actions = []

        self.assertEqual(_pool_hash_and_name(actions), (None, None))

    def test_pooled_cluster(self):
        actions = [
            dict(
                Args=['pool-0123456789abcdef0123456789abcdef', 'reflecting'],
                Name='master',
            ),
        ]

        self.assertEqual(_pool_hash_and_name(actions),
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

        self.assertEqual(_pool_hash_and_name(actions),
                         ('0123456789abcdef0123456789abcdef', 'reflecting'))

    def test_pooled_cluster_with_max_hours_idle(self):
        # max hours idle is added AFTER the master bootstrap script,
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

        self.assertEqual(_pool_hash_and_name(actions),
                         ('0123456789abcdef0123456789abcdef', 'reflecting'))

    def test_first_arg_doesnt_start_with_pool(self):
        actions = [
            dict(
                Args=['cowsay', 'mrjob'],
                Name='master',
            ),
        ]

        self.assertEqual(_pool_hash_and_name(actions), (None, None))

    def test_too_many_args(self):
        actions = [
            dict(
                Args=['cowsay', '-b', 'mrjob'],
                Name='master',
            ),
        ]

        self.assertEqual(_pool_hash_and_name(actions), (None, None))

    def test_too_few_args(self):
        actions = [
            dict(
                Args=['pool-0123456789abcdef0123456789abcdef'],
                Name='master',
            ),
        ]

        self.assertEqual(_pool_hash_and_name(actions), (None, None))

    def test_bootstrap_action_isnt_named_master(self):
        actions = [
            dict(
                Args=['pool-0123456789abcdef0123456789abcdef', 'reflecting'],
                Name='apprentice',
            ),
        ]

        self.assertEqual(_pool_hash_and_name(actions), (None, None))
