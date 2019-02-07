# Copyright 2019 Yelp
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
from mrjob.fs.base import Filesystem
from mrjob.fs.composite import CompositeFilesystem
from mrjob.parse import is_s3_uri
from mrjob.parse import is_uri

from tests.py2 import Mock
from tests.py2 import patch
from tests.sandbox import BasicTestCase


class CompositeFilesystemTestCase(BasicTestCase):

    def setUp(self):
        super(CompositeFilesystemTestCase, self).setUp()

        self.log = self.start(patch('mrjob.fs.composite.log'))

        self.hadoop_fs = Mock(spec=Filesystem)
        self.hadoop_fs.get_hadoop_version = Mock()
        self.hadoop_fs.can_handle_path.side_effect = is_uri

        self.local_fs = Mock(spec=Filesystem)
        self.local_fs.can_handle_path.side_effect = lambda p: not is_uri(p)

        self.s3_fs = Mock(spec=Filesystem)
        self.s3_fs.create_bucket = Mock()
        self.s3_fs.can_handle_path.side_effect = is_s3_uri

    def test_empty_fs(self):
        fs = CompositeFilesystem()

        self.assertFalse(fs.can_handle_path('s3://walrus/fish'))
        self.assertFalse(fs.can_handle_path('/'))

        self.assertRaises(IOError, fs.ls, '/')

    def test_pick_fs(self):
        fs = CompositeFilesystem()

        fs.add_fs('s3', self.s3_fs)
        fs.add_fs('hadoop', self.hadoop_fs)

        self.assertEqual(fs.ls('s3://walrus/fish'),
                         self.s3_fs.ls.return_value)
        # hadoop fs could have handled it, but s3_fs got it first
        self.assertTrue(self.hadoop_fs.can_handle_path('s3://walrus/fish'))
        self.assertFalse(self.hadoop_fs.ls.called)

        self.assertEqual(fs.ls('hdfs:///user/hadoop/'),
                         self.hadoop_fs.ls.return_value)

        # don't move on to the next FS on an error (unlike old
        # CompositeFilesystem implementation)
        self.s3_fs.ls.side_effect = IOError

        self.assertRaises(IOError, fs.ls, 's3://walrus/fish')

    def test_forward_join(self):
        # join() is a special case since it takes multiple arguments
        fs = CompositeFilesystem()

        fs.add_fs('s3', self.s3_fs)

        self.assertEqual(fs.join('s3://walrus/fish', 'salmon'),
                         self.s3_fs.join.return_value)
        self.s3_fs.join.assert_called_once_with(
            's3://walrus/fish', 'salmon')

    def test_forward_put(self):
        # put() is a special case since the path that matters comes second
        fs = CompositeFilesystem()

        fs.add_fs('s3', self.s3_fs)

        fs.put('/path/to/file', 's3://walrus/file')
        self.s3_fs.put.assert_called_once_with(
            '/path/to/file', 's3://walrus/file', None)

    def test_forward_put_with_part_size(self):
        fs = CompositeFilesystem()

        fs.add_fs('s3', self.s3_fs)

        fs.put('/path/to/file', 's3://walrus/file', part_size_mb=99999)
        self.s3_fs.put.assert_called_once_with(
            '/path/to/file', 's3://walrus/file', 99999)

    def test_forward_fs_extensions(self):
        fs = CompositeFilesystem()

        fs.add_fs('s3', self.s3_fs)
        fs.add_fs('hadoop', self.hadoop_fs)

        self.assertEqual(fs.create_bucket, self.s3_fs.create_bucket)
        self.assertEqual(fs.get_hadoop_version,
                         self.hadoop_fs.get_hadoop_version)

        self.assertRaises(AttributeError, lambda: fs.client)

    def test_disable_fs(self):
        class NoCredentialsError(Exception):
            pass

        fs = CompositeFilesystem()

        # tentatively use S3 filesystem, if set up
        fs.add_fs('s3', self.s3_fs,
                  disable_if=lambda ex: isinstance(ex, NoCredentialsError))
        fs.add_fs('hadoop', self.hadoop_fs)

        self.s3_fs.ls.side_effect = NoCredentialsError

        # calling ls() on S3 fs disables it, so we move on to hadoop fs
        self.assertEqual(fs.ls('s3://walrus/'),
                         self.hadoop_fs.ls.return_value)
        self.assertTrue(self.s3_fs.ls.called)

        self.assertIn('s3', fs._disabled)

        # now that s3 fs is disabled, we won't even try to call it
        self.assertEqual(fs.cat('s3://walrus/fish'),
                         self.hadoop_fs.cat.return_value)
        self.assertFalse(self.s3_fs.cat.called)
