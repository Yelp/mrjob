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
from mrjob.cmd import UploadDirManager
from mrjob.cmd import WorkingDirManager


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


class UploadDirManagerTestCase(unittest.TestCase):

    def test_empty(self):
        sd = UploadDirManager('hdfs:///')
        self.assertEqual(sd.path_to_uri(), {})

    def test_simple(self):
        sd = UploadDirManager('hdfs:///')
        sd.add('foo/bar.py')
        self.assertEqual(sd.path_to_uri(), {'foo/bar.py': 'hdfs:///bar.py'})

    def test_name_collision(self):
        sd = UploadDirManager('hdfs:///')
        sd.add('foo/bar.py')
        sd.add('bar.py')
        self.assertEqual(sd.path_to_uri(),
                         {'foo/bar.py': 'hdfs:///bar.py',
                          'bar.py': 'hdfs:///bar-1.py'})

    def test_add_is_idempotent(self):
        sd = UploadDirManager('hdfs:///')
        sd.add('foo/bar.py')
        self.assertEqual(sd.path_to_uri(), {'foo/bar.py': 'hdfs:///bar.py'})
        sd.add('foo/bar.py')
        self.assertEqual(sd.path_to_uri(), {'foo/bar.py': 'hdfs:///bar.py'})

    def test_uri(self):
        sd = UploadDirManager('hdfs:///')
        sd.add('foo/bar.py')
        self.assertEqual(sd.uri('foo/bar.py'), 'hdfs:///bar.py')

    def test_unknown_uri(self):
        sd = UploadDirManager('hdfs:///')
        sd.add('foo/bar.py')
        self.assertEqual(sd.path_to_uri(), {'foo/bar.py': 'hdfs:///bar.py'})
        self.assertEqual(sd.uri('hdfs://host/path/to/bar.py'),
                         'hdfs://host/path/to/bar.py')
        # checking unknown URIs doesn't add them
        self.assertEqual(sd.path_to_uri(), {'foo/bar.py': 'hdfs:///bar.py'})

    def uri_adds_trailing_slash(self):
        sd = UploadDirManager('s3://bucket/dir')
        sd.add('foo/bar.py')
        self.assertEqual(sd.uri('foo/bar.py'), 's3://bucket/dir/bar.py')
        self.assertEqual(sd.path_to_uri(),
                         {'foo/bar.py': 's3://bucket/dir/bar.py'})


class WorkingDirManagerTestCase(unittest.TestCase):

    def test_empty(self):
        wd = WorkingDirManager()
        self.assertEqual(wd.name_to_path('archive'), {})
        self.assertEqual(wd.name_to_path('file'), {})

    def test_simple(self):
        wd = WorkingDirManager()
        wd.add('archive', 's3://bucket/path/to/baz.tar.gz')
        wd.add('file', 'foo/bar.py')
        self.assertEqual(wd.name_to_path('file'),
                         {'bar.py': 'foo/bar.py'})
        self.assertEqual(wd.name_to_path('archive'),
                         {'baz.tar.gz': 's3://bucket/path/to/baz.tar.gz'})

    def test_explicit_name_collision(self):
        wd = WorkingDirManager()
        wd.add('file', 'foo.py', name='qux.py')
        self.assertRaises(ValueError, wd.add, 'file', 'bar.py', name='qux.py')

    def test_okay_to_give_same_path_same_name(self):
        wd = WorkingDirManager()
        wd.add('file', 'foo/bar.py', name='qux.py')
        wd.add('file', 'foo/bar.py', name='qux.py')
        self.assertEqual(wd.name_to_path('file'),
                         {'qux.py': 'foo/bar.py'})

    def test_auto_names_are_different_from_assigned_names(self):
        wd = WorkingDirManager()
        wd.add('file', 'foo/bar.py', name='qux.py')
        wd.add('file', 'foo/bar.py')  # use default name bar.py
        self.assertEqual(wd.name_to_path('file'),
                         {'qux.py': 'foo/bar.py',
                          'bar.py': 'foo/bar.py'})

    def test_cant_give_same_path_different_types(self):
        wd = WorkingDirManager()
        wd.add('archive', 'foo/bar.py', name='qux.py')
        self.assertRaises(ValueError,
                          wd.add, 'file', 'foo/bar.py', name='qux.py')

    def test_lazy_naming(self):
        wd = WorkingDirManager()
        wd.add('file', 'qux.py')  # qux.py by default
        wd.add('file', 'bar.py', name='qux.py')
        self.assertEqual(wd.name_to_path('file'),
                         {'qux.py': 'bar.py', 'qux-1.py': 'qux.py'})

    def test_eager_naming(self):
        wd = WorkingDirManager()
        wd.add('file', 'qux.py')  # qux.py by default
        self.assertEqual(wd.name('file', 'qux.py'), 'qux.py')
        # whoops, picked that name too soon!
        self.assertRaises(ValueError, wd.add, 'file', 'bar.py', name='qux.py')

    def test_bad_path_type(self):
        wd = WorkingDirManager()
        self.assertRaises(TypeError, wd.add, 'dir', 'foo.py')
        self.assertRaises(TypeError, wd.name_to_path, 'dir')
        self.assertRaises(TypeError, wd.name, 'dir', 'foo.py')

    def test_cant_name_unknown_paths(self):
        wd = WorkingDirManager()
        self.assertRaises(ValueError, wd.name, 'file', 'bar.py')
        self.assertRaises(ValueError, wd.name, 'file', 'bar.py', name='qux.py')

    def test_cant_auto_name_unless_added_as_auto(self):
        wd = WorkingDirManager()
        wd.add('file', 'bar.py', name='qux.py')
        self.assertEqual(wd.name('file', 'bar.py', 'qux.py'), 'qux.py')
        self.assertRaises(ValueError,
                          wd.name, 'file', 'bar.py')
