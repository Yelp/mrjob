# -*- coding: utf-8 -*-

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
from mrjob.py2 import to_text

from tests.py2 import TestCase


class ToTextTestCase(TestCase):

    def test_None(self):
        self.assertRaises(TypeError, to_text, None)

    def test_ascii_bytes(self):
        self.assertEqual(to_text(b'foo'), 'foo')

    def test_utf_8_bytes(self):
        self.assertEqual(to_text(b'caf\xc3\xa9'), 'café')

    def test_latin_1_bytes(self):
        self.assertEqual(to_text(b'caf\xe9'), 'caf\xe9')

    def test_ascii_unicode(self):
        self.assertEqual(to_text(u'foo'), u'foo')

    def test_non_ascii_unicode(self):
        self.assertEqual(to_text(u'café'), u'café')
