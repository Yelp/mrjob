# Copyright 2013 David Marin
# Copyright 2015 Yelp
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

from mrjob.retry import RetryWrapper

from tests.py2 import Mock


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
