# Copyright 2009-2013 Yelp and Contributors
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
import bz2
import os

try:
    import boto
    boto  # pyflakes
except ImportError:
    boto = None

from mrjob.fs.s3 import S3Filesystem

from tests.compress import gzip_compress
from tests.mockboto import MockS3Connection
from tests.mockboto import add_mock_s3_data
from tests.sandbox import SandboxedTestCase


class S3FSTestCase(SandboxedTestCase):

    def setUp(self):
        self.sandbox_boto()
        self.addCleanup(self.unsandbox_boto)
        self.fs = S3Filesystem('key_id', 'secret', 'nowhere')

    def sandbox_boto(self):
        self.mock_s3_fs = {}

        def mock_boto_connect_s3(*args, **kwargs):
            kwargs['mock_s3_fs'] = self.mock_s3_fs
            return MockS3Connection(*args, **kwargs)

        self._real_boto_connect_s3 = boto.connect_s3
        boto.connect_s3 = mock_boto_connect_s3

        # copy the old environment just to be polite
        self._old_environ = os.environ.copy()

    def unsandbox_boto(self):
        boto.connect_s3 = self._real_boto_connect_s3

    def add_mock_s3_data(self, bucket, path, contents, time_modified=None):
        """Update self.mock_s3_fs with a map from bucket name
        to key name to data."""
        add_mock_s3_data(self.mock_s3_fs,
                         {bucket: {path: contents}},
                         time_modified)
        return 's3://%s/%s' % (bucket, path)

    def test_cat_uncompressed(self):
        remote_path = self.add_mock_s3_data(
            'walrus', 'data/foo', 'foo\nfoo\n')

        self.assertEqual(list(self.fs._cat_file(remote_path)),
                         ['foo\n', 'foo\n'])

    def test_cat_bz2(self):
        remote_path = self.add_mock_s3_data(
            'walrus', 'data/foo.bz2', bz2.compress('foo\n' * 1000))

        self.assertEqual(list(self.fs._cat_file(remote_path)),
                         ['foo\n'] * 1000)

    def test_cat_gz(self):
        remote_path = self.add_mock_s3_data(
            'walrus', 'data/foo.gz', gzip_compress('foo\n' * 10000))

        self.assertEqual(list(self.fs._cat_file(remote_path)),
                         ['foo\n'] * 10000)

    def test_ls_basic(self):
        remote_path = self.add_mock_s3_data('walrus', 'data/foo', 'foo\nfoo\n')

        self.assertEqual(list(self.fs.ls(remote_path)), [remote_path])
        self.assertEqual(list(self.fs.ls('s3://walrus/')), [remote_path])

    def test_ls_recurse(self):
        paths = [
            self.add_mock_s3_data('walrus', 'data/bar', 'bar\nbar\n'),
            self.add_mock_s3_data('walrus', 'data/bar/baz', 'baz\nbaz\n'),
            self.add_mock_s3_data('walrus', 'data/foo', 'foo\nfoo\n'),
        ]

        self.assertEqual(list(self.fs.ls('s3://walrus/')), paths)
        self.assertEqual(list(self.fs.ls('s3://walrus/*')), paths)

    def test_ls_glob(self):
        paths = [
            self.add_mock_s3_data('walrus', 'data/bar', 'bar\nbar\n'),
            self.add_mock_s3_data('walrus', 'data/bar/baz', 'baz\nbaz\n'),
            self.add_mock_s3_data('walrus', 'data/foo', 'foo\nfoo\n'),
        ]

        self.assertEqual(list(self.fs.ls('s3://walrus/*/baz')), [paths[1]])

    def test_ls_s3n(self):
        paths = [
            self.add_mock_s3_data('walrus', 'data/bar', 'abc123'),
            self.add_mock_s3_data('walrus', 'data/baz', '123abc')
        ]

        self.assertEqual(list(self.fs.ls('s3n://walrus/data/*')),
                         [p.replace('s3://', 's3n://') for p in paths])

    def test_du(self):
        paths = [
            self.add_mock_s3_data('walrus', 'data/foo', 'abcd'),
            self.add_mock_s3_data('walrus', 'data/bar/baz', 'defg'),
        ]
        self.assertEqual(self.fs.du('s3://walrus/'), 8)
        self.assertEqual(self.fs.du(paths[0]), 4)
        self.assertEqual(self.fs.du(paths[1]), 4)

    def test_path_exists_no(self):
        path = os.path.join('s3://walrus/data/foo')
        self.assertEqual(self.fs.path_exists(path), False)

    def test_path_exists_yes(self):
        path = self.add_mock_s3_data('walrus', 'data/foo', 'abcd')
        self.assertEqual(self.fs.path_exists(path), True)

    def test_rm(self):
        path = self.add_mock_s3_data('walrus', 'data/foo', 'abcd')
        self.assertEqual(self.fs.path_exists(path), True)

        self.fs.rm(path)
        self.assertEqual(self.fs.path_exists(path), False)
