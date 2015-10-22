# Copyright 2009-2015 Yelp and Contributors
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
import os.path
import posixpath

from mrjob.fs.base import Filesystem

from tests.py2 import TestCase
from tests.py2 import patch
from tests.quiet import no_handlers_for_logger
from tests.sandbox import SandboxedTestCase


class JoinTestCase(SandboxedTestCase):

    def setUp(self):
        super(JoinTestCase, self).setUp()

        # os.path.join() and posixpath.join() do the same thing in
        # UNIX and OS X, so track which one we called
        self.start(patch('os.path.join', wraps=os.path.join))
        self.start(patch('posixpath.join', wraps=posixpath.join))

        self.fs = Filesystem()

    def test_local_paths(self):
        self.assertEqual(self.fs.join('foo', 'bar'),
                         'foo%sbar' % os.path.sep)
        self.assertEqual(self.fs.join('foo', '%sbar' % os.path.sep),
                         '%sbar' % os.path.sep)

        self.assertTrue(os.path.join.called)

    def test_path_onto_uri(self):
        self.assertEqual(self.fs.join('hdfs://host', 'path'),
                         'hdfs://host/path')

        self.assertTrue(posixpath.join.called)

    def test_uri_onto_anything(self):
        self.assertEqual(self.fs.join('hdfs://host', 'hdfs://host2/path'),
                         'hdfs://host2/path')
        self.assertEqual(self.fs.join('/', 'hdfs://host2/path'),
                         'hdfs://host2/path')

        self.assertFalse(os.path.join.called)
        self.assertFalse(posixpath.join.called)


class DeprecatedAliasesTestCase(TestCase):

    def test_path_exists(self):
        fs = Filesystem()

        with patch.object(fs, 'exists'):
            with no_handlers_for_logger('mrjob.fs.base'):
                fs.path_exists('foo')

            fs.exists.assert_called_once_with('foo')

    def test_path_join(self):
        fs = Filesystem()

        with patch.object(fs, 'join'):
            with no_handlers_for_logger('mrjob.fs.base'):
                fs.path_join('foo', 'bar')

            fs.join.assert_called_once_with('foo', 'bar')
