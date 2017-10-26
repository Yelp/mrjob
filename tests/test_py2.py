# -*- coding: utf-8 -*-

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

from mrjob.py2 import to_unicode


class ToUnicodeTestCase(TestCase):

    def test_None(self):
        self.assertRaises(TypeError, to_unicode, None)

    def test_ascii_bytes(self):
        self.assertEqual(to_unicode(b'foo'), u'foo')

    def test_utf_8_bytes(self):
        self.assertEqual(to_unicode(b'caf\xc3\xa9'), u'café')

    def test_latin_1_bytes(self):
        self.assertEqual(to_unicode(b'caf\xe9'), u'caf\xe9')

    def test_ascii_unicode(self):
        self.assertEqual(to_unicode(u'foo'), u'foo')

    def test_non_ascii_unicode(self):
        self.assertEqual(to_unicode(u'café'), u'café')
