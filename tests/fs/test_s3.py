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
from tests.py2 import patch


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

    def test_ls_key(self):
        self.add_mock_s3_data(
            {'walrus': {'data/foo': b''}})

        self.assertEqual(list(self.fs.ls('s3://walrus/data/foo')),
                         ['s3://walrus/data/foo'])

    def test_ls_recursively(self):
        self.add_mock_s3_data(
            {'walrus': {'data/bar': b'',
                        'data/bar/baz': b'',
                        'data/foo': b'',
                        'qux': b''}})

        uris = [
            's3://walrus/data/bar',
            's3://walrus/data/bar/baz',
            's3://walrus/data/foo',
            's3://walrus/qux',
        ]

        self.assertEqual(list(self.fs.ls('s3://walrus/')), uris)
        self.assertEqual(list(self.fs.ls('s3://walrus/*')), uris)

        self.assertEqual(list(self.fs.ls('s3://walrus/data')), uris[:-1])
        self.assertEqual(list(self.fs.ls('s3://walrus/data/')), uris[:-1])
        self.assertEqual(list(self.fs.ls('s3://walrus/data/*')), uris[:-1])

    def test_ls_globs(self):
        self.add_mock_s3_data(
            {'w': {'a': b'',
                   'a/b': b'',
                   'ab': b'',
                   'b': b''}})

        self.assertEqual(list(self.fs.ls('s3://w/')),
                         ['s3://w/a', 's3://w/a/b', 's3://w/ab', 's3://w/b'])
        self.assertEqual(list(self.fs.ls('s3://w/*')),
                         ['s3://w/a', 's3://w/a/b', 's3://w/ab', 's3://w/b'])
        self.assertEqual(list(self.fs.ls('s3://w/*/')),
                         ['s3://w/a/b'])
        self.assertEqual(list(self.fs.ls('s3://w/*/*')),
                         ['s3://w/a/b'])
        self.assertEqual(list(self.fs.ls('s3://w/a?')),
                         ['s3://w/ab'])
        # * can match /
        self.assertEqual(list(self.fs.ls('s3://w/a*')),
                         ['s3://w/a', 's3://w/a/b', 's3://w/ab'])
        self.assertEqual(list(self.fs.ls('s3://w/*b')),
                         ['s3://w/a/b', 's3://w/ab', 's3://w/b'])

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
            'walrus': {'foo': b''}})

        self.assertEqual(self.fs.exists('s3://walrus/foo'), True)
        self.fs.rm('s3://walrus/foo')
        self.assertEqual(self.fs.exists('s3://walrus/foo'), False)

    def test_rm_dir(self):
        self.add_mock_s3_data({
            'walrus': {'data/foo': b'',
                       'data/bar/baz': b''}})

        self.assertEqual(self.fs.exists('s3://walrus/data/foo'), True)
        self.assertEqual(self.fs.exists('s3://walrus/data/bar/baz'), True)
        self.fs.rm('s3://walrus/data')
        self.assertEqual(self.fs.exists('s3://walrus/data/foo'), False)
        self.assertEqual(self.fs.exists('s3://walrus/data/bar/baz'), False)


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

    def test_get_location_is_forbidden(self):
        self.add_mock_s3_data({'walrus': {}}, location='us-west-2')

        fs = S3Filesystem()

        with patch(
                'tests.mockboto.MockBucket.get_location',
                side_effect=boto.exception.S3ResponseError(403, 'Forbidden')):

            bucket = fs.get_bucket('walrus')

        self.assertEqual(bucket.connection.host, 's3.amazonaws.com')

    def test_get_location_other_error(self):
        self.add_mock_s3_data({'walrus': {}}, location='us-west-2')

        fs = S3Filesystem()

        with patch(
                'tests.mockboto.MockBucket.get_location',
                side_effect=boto.exception.S3ResponseError(404, 'Not Found')):

            self.assertRaises(boto.exception.S3ResponseError,
                              fs.get_bucket, 'walrus')

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

        with patch('tests.mockboto.MockBucket.get_location') as mock_get_loc:
            self.assertEqual(bucket_east.connection.host,
                             's3-us-east-2.amazonaws.com')
            # no reason to check bucket location if endpoint is forced
            self.assertFalse(mock_get_loc.called)

        # can't access this bucket from wrong endpoint!
        self.assertRaises(boto.exception.S3ResponseError,
                          fs.get_bucket, 'walrus-west')
