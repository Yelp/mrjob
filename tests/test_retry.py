# Copyright 2013 David Marin
# Copyright 2015 Yelp
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

from mrjob.retry import RetryGoRound
from mrjob.retry import RetryWrapper

from tests.py2 import Mock


class RetryGoRoundTestCase(TestCase):

    def test_empty(self):
        self.assertRaises(
            ValueError, RetryGoRound, [], lambda ex: isinstance(ex, IOError))

    def test_success(self):
        a1 = Mock()
        # need __name__ so wraps() will work
        a1.f = Mock(__name__='f', return_value=1)
        a2 = Mock()
        a2.f = Mock(__name__='f', return_value=2)

        a = RetryGoRound([a1, a2], lambda ex: isinstance(ex, IOError))

        self.assertEqual(a.f(), 1)
        self.assertEqual(a1.f.call_count, 1)
        # never needed to try a2.f()
        self.assertEqual(a2.f.call_count, 0)

    def test_one_failure(self):
        a1 = Mock()
        a1.f = Mock(__name__='f', side_effect=IOError)
        a1.x = 100
        a2 = Mock()
        a2.f = Mock(__name__='f', return_value=2)
        a2.x = 200

        a = RetryGoRound([a1, a2], lambda ex: isinstance(ex, IOError))

        self.assertEqual(a.x, 100)
        self.assertEqual(a.f(), 2)
        # a2 was the last alternative that worked, so now we get x from it
        self.assertEqual(a.x, 200)
        # this time we should skip calling a1.f() entirely
        self.assertEqual(a.f(), 2)

        self.assertEqual(a1.f.call_count, 1)
        self.assertEqual(a2.f.call_count, 2)

    def test_all_fail(self):
        a1 = Mock()
        a1.f = Mock(__name__='f', side_effect=IOError)
        a1.x = 100
        a2 = Mock()
        a2.f = Mock(__name__='f', side_effect=IOError)
        a2.x = 200

        a = RetryGoRound([a1, a2], lambda ex: isinstance(ex, IOError))

        self.assertEqual(a.x, 100)
        # ran out of alternatives
        self.assertRaises(IOError, a.f)
        # nothing worked, so we're still pointing at a1
        self.assertEqual(a.x, 100)
        # yep, still broken
        self.assertRaises(IOError, a.f)

        self.assertEqual(a1.f.call_count, 2)
        self.assertEqual(a2.f.call_count, 2)

    def test_unrecoverable_error(self):
        a1 = Mock()
        a1.f = Mock(__name__='f', side_effect=ValueError)
        a2 = Mock()
        a2.f = Mock(__name__='f', return_value=2)

        a = RetryGoRound([a1, a2], lambda ex: isinstance(ex, IOError))

        self.assertRaises(ValueError, a.f)
        self.assertRaises(ValueError, a.f)

        self.assertEqual(a1.f.call_count, 2)
        self.assertEqual(a2.f.call_count, 0)

    def test_can_wrap_around(self):
        a1 = Mock()
        a1.f = Mock(__name__='f', side_effect=[IOError, 1])
        a2 = Mock()
        a2.f = Mock(__name__='f', side_effect=[2, IOError])

        a = RetryGoRound([a1, a2], lambda ex: isinstance(ex, IOError))

        self.assertEqual(a.f(), 2)
        self.assertEqual(a.f(), 1)

        self.assertEqual(a1.f.call_count, 2)
        self.assertEqual(a2.f.call_count, 2)

    def test_wrapping(self):
        a1 = Mock()
        a1.f = Mock(__name__='f', side_effect=IOError)
        a2 = Mock()
        a2.f = Mock(__name__='f', return_value=2)

        a = RetryGoRound([a1, a2], lambda ex: isinstance(ex, IOError))

        self.assertEqual(a.f('foo', bar='baz'), 2)
        a1.f.assert_called_once_with('foo', bar='baz')
        a2.f.assert_called_once_with('foo', bar='baz')
        self.assertEqual(a.f.__name__, 'f')


class RetryWrapperTestCase(TestCase):
    def test_success(self):
        a1 = Mock()
        a1.f = Mock(__name__='f', side_effect=None)
        a = RetryWrapper(
            a1,
            retry_if=lambda x: True,
            backoff=0.0001,
            max_tries=2
        )

        a.f()
        a1.f.assert_called_once_with()

    def test_failure(self):
        a1 = Mock()
        a1.f = Mock(__name__='f', side_effect=[IOError, 1])
        a = RetryWrapper(
            a1,
            retry_if=lambda x: True,
            backoff=0.0001,
            max_tries=2
        )

        self.assertEqual(a.f(), 1)
        self.assertEqual(a1.f.call_count, 2)

    def test_failure_raises_if_all_tries_fail(self):
        a1 = Mock()
        a1.f = Mock(__name__='f', side_effect=[IOError, IOError])
        a = RetryWrapper(
            a1,
            retry_if=lambda x: True,
            backoff=0.0001,
            max_tries=2
        )
        with self.assertRaises(IOError):
            a.f()
        self.assertEqual(a1.f.call_count, 2)

    def test_try_till_success(self):
        a1 = Mock()
        a1.f = Mock(__name__='f', side_effect=[IOError, IOError, None])
        a = RetryWrapper(
            a1,
            retry_if=lambda x: True,
            backoff=0.0001,
            max_tries=0
        )
        a.f()
        self.assertEqual(a1.f.call_count, 3)
