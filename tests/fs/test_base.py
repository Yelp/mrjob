# Copyright 2009-2015 Yelp and Contributors
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
import os.path

from mrjob.fs.base import Filesystem

from tests.py2 import Mock
from tests.sandbox import SandboxedTestCase


class CatTestCase(SandboxedTestCase):

    def test_multiple_files(self):
        fs = Filesystem()

        fs.ls = Mock(return_value=['path1', 'path2', 'path3'])
        fs._cat_file = Mock(return_value=[b'chunk1\n', b'chunk2'])

        chunks = list(fs.cat('whatever'))

        self.assertEqual(
            chunks,
            [b'chunk1\n', b'chunk2', b'',
             b'chunk1\n', b'chunk2', b'',
             b'chunk1\n', b'chunk2'])


class JoinTestCase(SandboxedTestCase):

    def setUp(self):
        super(JoinTestCase, self).setUp()

        self.fs = Filesystem()

    def test_local_paths(self):
        self.assertEqual(self.fs.join('foo', 'bar'),
                         'foo%sbar' % os.path.sep)
        self.assertEqual(self.fs.join('foo', '%sbar' % os.path.sep),
                         '%sbar' % os.path.sep)
        self.assertEqual(self.fs.join('foo', 'bar', 'baz'),
                         'foo%sbar%sbaz' % (os.path.sep, os.path.sep))

    def test_path_onto_uri(self):
        self.assertEqual(self.fs.join('hdfs://host', 'path'),
                         'hdfs://host/path')

    def test_uri_onto_anything(self):
        self.assertEqual(self.fs.join('hdfs://host', 'hdfs://host2/path'),
                         'hdfs://host2/path')
        self.assertEqual(self.fs.join('/', 'hdfs://host2/path'),
                         'hdfs://host2/path')
        self.assertEqual(self.fs.join('/', 'hdfs://host2/path', 'subdir'),
                         'hdfs://host2/path/subdir')
