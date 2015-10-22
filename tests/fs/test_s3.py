# Copyright 2009-2013 Yelp and Contributors
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
import bz2

try:
    import boto
    boto  # pyflakes
except ImportError:
    boto = None

from mrjob.fs.s3 import S3Filesystem

from tests.compress import gzip_compress
from tests.mockboto import MockBotoTestCase


class S3FSTestCase(MockBotoTestCase):

    def setUp(self):
        super(S3FSTestCase, self).setUp()
        self.fs = S3Filesystem()

    def test_cat_uncompressed(self):
        self.add_mock_s3_data(
            {'walrus': {'data/foo': b'foo\nfoo\n'}})

        self.assertEqual(list(self.fs._cat_file('s3://walrus/data/foo')),
                         [b'foo\n', b'foo\n'])

    def test_cat_bz2(self):
        self.add_mock_s3_data(
            {'walrus': {'data/foo.bz2': bz2.compress(b'foo\n' * 1000)}})

        self.assertEqual(list(self.fs._cat_file('s3://walrus/data/foo.bz2')),
                         [b'foo\n'] * 1000)

    def test_cat_gz(self):
        self.add_mock_s3_data(
            {'walrus': {'data/foo.gz': gzip_compress(b'foo\n' * 10000)}})

        self.assertEqual(list(self.fs._cat_file('s3://walrus/data/foo.gz')),
                         [b'foo\n'] * 10000)

    def test_ls_basic(self):
        self.add_mock_s3_data(
            {'walrus': {'data/foo': b'foo\nfoo\n'}})

        self.assertEqual(list(self.fs.ls('s3://walrus/data/foo')),
                         ['s3://walrus/data/foo'])
        self.assertEqual(list(self.fs.ls('s3://walrus/')),
                         ['s3://walrus/data/foo'])

    def test_ls_recurse(self):
        self.add_mock_s3_data(
            {'walrus': {'data/bar': b'bar\nbar\n',
                        'data/bar/baz': b'baz\nbaz\n',
                        'data/foo': b'foo\nfoo\n'}})

        paths = [
            's3://walrus/data/bar',
            's3://walrus/data/bar/baz',
            's3://walrus/data/foo',
        ]

        self.assertEqual(list(self.fs.ls('s3://walrus/')), paths)
        self.assertEqual(list(self.fs.ls('s3://walrus/*')), paths)

    def test_ls_glob(self):
        self.add_mock_s3_data(
            {'walrus': {'data/bar': b'bar\nbar\n',
                        'data/bar/baz': b'baz\nbaz\n',
                        'data/foo': b'foo\nfoo\n'}})

        self.assertEqual(list(self.fs.ls('s3://walrus/*/baz')),
                         ['s3://walrus/data/bar/baz'])

    def test_ls_s3n(self):
        self.add_mock_s3_data(
            {'walrus': {'data/bar': b'abc123',
                        'data/baz': b'123abc'}})

        self.assertEqual(list(self.fs.ls('s3n://walrus/data/*')),
                         ['s3n://walrus/data/bar',
                          's3n://walrus/data/baz'])

    def test_du(self):
        self.add_mock_s3_data({
            'walrus': {'data/foo': b'abcde',
                       'data/bar/baz': b'fgh'}})

        self.assertEqual(self.fs.du('s3://walrus/'), 8)
        self.assertEqual(self.fs.du('s3://walrus/data/foo'), 5)
        self.assertEqual(self.fs.du('s3://walrus/data/bar/baz'), 3)

    def test_exists(self):
        self.add_mock_s3_data({
            'walrus': {'data/foo': b'abcd'}})
        self.assertEqual(self.fs.exists('s3://walrus/data/foo'), True)
        self.assertEqual(self.fs.exists('s3://walrus/data/bar'), False)

    def test_rm(self):
        self.add_mock_s3_data({
            'walrus': {'data/foo': b'abcd'}})

        self.assertEqual(self.fs.exists('s3://walrus/data/foo'), True)
        self.fs.rm('s3://walrus/data/foo')
        self.assertEqual(self.fs.exists('s3://walrus/data/foo'), False)


class S3FSRegionTestCase(MockBotoTestCase):

    def test_default_endpoint(self):
        fs = S3Filesystem()

        s3_conn = fs.make_s3_conn()
        self.assertEqual(s3_conn.host, 's3.amazonaws.com')

    def test_force_s3_endpoint(self):
        fs = S3Filesystem(s3_endpoint='s3-us-west-1.amazonaws.com')

        s3_conn = fs.make_s3_conn()
        self.assertEqual(s3_conn.host, 's3-us-west-1.amazonaws.com')

    def test_endpoint_for_bucket_in_us_west_2(self):
        self.add_mock_s3_data({'walrus': {}}, location='us-west-2')

        fs = S3Filesystem()

        bucket = fs.get_bucket('walrus')
        self.assertEqual(bucket.connection.host,
                         's3-us-west-2.amazonaws.com')

    def test_endpoint_for_bucket_in_us_east_1(self):
        # location constraint for us-east-1 is '', not 'us-east-1'
        self.add_mock_s3_data({'walrus': {}}, location='')

        fs = S3Filesystem()

        bucket = fs.get_bucket('walrus')
        self.assertEqual(bucket.connection.host, 's3.amazonaws.com')

    def test_buckets_from_forced_s3_endpoint(self):
        self.add_mock_s3_data({'walrus-east': {}}, location='us-east-2')
        self.add_mock_s3_data({'walrus-west': {}}, location='us-west-2')

        fs = S3Filesystem(s3_endpoint='s3-us-east-2.amazonaws.com')

        bucket_east = fs.get_bucket('walrus-east')
        self.assertEqual(bucket_east.connection.host,
                         's3-us-east-2.amazonaws.com')

        # can't access this bucket from wrong endpoint!
        self.assertRaises(boto.exception.S3ResponseError,
                          fs.get_bucket, 'walrus-west')


class TestS3Ls(MockBotoTestCase):

    def test_s3_ls(self):
        self.add_mock_s3_data(
            {'walrus': {'one': b'', 'two': b'', 'three': b''}})

        fs = S3Filesystem()

        self.assertEqual(set(fs._s3_ls('s3://walrus/')),
                         set(['s3://walrus/one',
                              's3://walrus/two',
                              's3://walrus/three',
                              ]))

        self.assertEqual(set(fs._s3_ls('s3://walrus/t')),
                         set(['s3://walrus/two',
                              's3://walrus/three',
                              ]))

        self.assertEqual(set(fs._s3_ls('s3://walrus/t/')),
                         set([]))

        # if we ask for a nonexistent bucket, we should get some sort
        # of exception (in practice, buckets with random names will
        # probably be owned by other people, and we'll get some sort
        # of permissions error)
        self.assertRaises(Exception, set, fs._s3_ls('s3://lolcat/'))
