# Copyright 2012 Yelp
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
from __future__ import with_statement

try:
    import unittest2 as unittest
    unittest  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import unittest

from mrjob.cmd import name_uniquely


class NameUniqueTestCase(unittest.TestCase):

    def test_no_empty_names(self):
        self.assertEqual(name_uniquely(''), '_')

    def test_empty_proposed_name_same_as_none(self):
        self.assertEqual(name_uniquely('foo.py', proposed_name=None), 'foo.py')
        self.assertEqual(name_uniquely('foo.py', proposed_name=''), 'foo.py')

    def test_use_basename_by_default(self):
        self.assertEqual(name_uniquely('foo/bar.py'), 'bar.py')
        self.assertEqual(name_uniquely('foo/bar/'), '_')

    def test_dont_use_names_taken(self):
        self.assertEqual(name_uniquely('foo.py'), 'foo.py')
        self.assertEqual(
            name_uniquely('foo.py', names_taken=['foo.py']),
            'foo-1.py')
        self.assertEqual(
            name_uniquely('foo.py', names_taken=['foo.py', 'foo-1.py']),
            'foo-2.py')

    def test_dont_used_proposed_names_taken(self):
        self.assertEqual(
            name_uniquely('foo.py', proposed_name='bar.py'),
            'bar.py')
        self.assertEqual(
            name_uniquely('foo.py', names_taken=['bar.py'],
                          proposed_name='bar.py'),
            'bar-1.py')
        self.assertEqual(
            name_uniquely('foo.py', names_taken=['bar.py', 'bar-1.py'],
                          proposed_name='bar.py'),
            'bar-2.py')

        # doesn't matter if path is taken
        self.assertEqual(
            name_uniquely(
                'foo.py', names_taken=['foo.py'], proposed_name='bar.py'),
            'bar.py')

    def test_auto_names_preserve_full_extension(self):
        self.assertEqual(
            name_uniquely(
                'foo.tar.gz', names_taken=['foo.tar.gz']),
            'foo-1.tar.gz')

    def test_auto_names_with_no_extension(self):
        self.assertEqual(
            name_uniquely(
                'foo', names_taken=['foo']),
            'foo-1')
        self.assertEqual(
            name_uniquely(
                '', names_taken=['_']),
            '_-1')

    def test_initial_dot_isnt_extension(self):
        self.assertEqual(
            name_uniquely(
                '.emacs', names_taken=['.emacs']),
            '.emacs-1')  # not '-1.emacs'

        self.assertEqual(
            name_uniquely(
                '.mrjob.conf', names_taken=['.mrjob.conf']),
            '.mrjob-1.conf')  # not '-1.mrjob.conf'
