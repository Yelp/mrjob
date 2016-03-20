# Copyright 2016 Google Inc.
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

from mrjob.fs.gcs import GCSFilesystem

from tests.compress import gzip_compress
from tests.mockboto import MockBotoTestCase
from tests.py2 import patch


class GCSFSTestCase(MockBotoTestCase):

    def setUp(self):
        super(GCSFSTestCase, self).setUp()
        self.fs = GCSFilesystem()

    def test_cat_uncompressed(self):
        self.add_mock_gcs_data(
            {'walrus': {'data/foo': b'foo\nfoo\n'}})

        self.assertEqual(list(self.fs._cat_file('gs://walrus/data/foo')),
                         [b'foo\n', b'foo\n'])

    def test_cat_bz2(self):
        self.add_mock_gcs_data(
            {'walrus': {'data/foo.bz2': bz2.compress(b'foo\n' * 1000)}})

        self.assertEqual(list(self.fs._cat_file('gs://walrus/data/foo.bz2')),
                         [b'foo\n'] * 1000)

    def test_cat_gz(self):
        self.add_mock_gcs_data(
            {'walrus': {'data/foo.gz': gzip_compress(b'foo\n' * 10000)}})

        self.assertEqual(list(self.fs._cat_file('gs://walrus/data/foo.gz')),
                         [b'foo\n'] * 10000)

    def test_ls_key(self):
        self.add_mock_gcs_data(
            {'walrus': {'data/foo': b''}})

        self.assertEqual(list(self.fs.ls('gs://walrus/data/foo')),
                         ['gs://walrus/data/foo'])

    def test_ls_recursively(self):
        self.add_mock_gcs_data(
            {'walrus': {'data/bar': b'',
                        'data/bar/baz': b'',
                        'data/foo': b'',
                        'qux': b''}})

        uris = [
            'gs://walrus/data/bar',
            'gs://walrus/data/bar/baz',
            'gs://walrus/data/foo',
            'gs://walrus/qux',
        ]

        self.assertEqual(list(self.fs.ls('gs://walrus/')), uris)
        self.assertEqual(list(self.fs.ls('gs://walrus/*')), uris)

        self.assertEqual(list(self.fs.ls('gs://walrus/data')), uris[:-1])
        self.assertEqual(list(self.fs.ls('gs://walrus/data/')), uris[:-1])
        self.assertEqual(list(self.fs.ls('gs://walrus/data/*')), uris[:-1])

    def test_ls_globs(self):
        self.add_mock_gcs_data(
            {'w': {'a': b'',
                   'a/b': b'',
                   'ab': b'',
                   'b': b''}})

        self.assertEqual(list(self.fs.ls('gs://w/')),
                         ['gs://w/a', 'gs://w/a/b', 'gs://w/ab', 'gs://w/b'])
        self.assertEqual(list(self.fs.ls('gs://w/*')),
                         ['gs://w/a', 'gs://w/a/b', 'gs://w/ab', 'gs://w/b'])
        self.assertEqual(list(self.fs.ls('gs://w/*/')),
                         ['gs://w/a/b'])
        self.assertEqual(list(self.fs.ls('gs://w/*/*')),
                         ['gs://w/a/b'])
        self.assertEqual(list(self.fs.ls('gs://w/a?')),
                         ['gs://w/ab'])
        # * can match /
        self.assertEqual(list(self.fs.ls('gs://w/a*')),
                         ['gs://w/a', 'gs://w/a/b', 'gs://w/ab'])
        self.assertEqual(list(self.fs.ls('gs://w/*b')),
                         ['gs://w/a/b', 'gs://w/ab', 'gs://w/b'])

    def test_ls_GCSn(self):
        self.add_mock_gcs_data(
            {'walrus': {'data/bar': b'abc123',
                        'data/baz': b'123abc'}})

        self.assertEqual(list(self.fs.ls('GCSn://walrus/data/*')),
                         ['GCSn://walrus/data/bar',
                          'GCSn://walrus/data/baz'])

    def test_du(self):
        self.add_mock_gcs_data({
            'walrus': {'data/foo': b'abcde',
                       'data/bar/baz': b'fgh'}})

        self.assertEqual(self.fs.du('gs://walrus/'), 8)
        self.assertEqual(self.fs.du('gs://walrus/data/foo'), 5)
        self.assertEqual(self.fs.du('gs://walrus/data/bar/baz'), 3)

    def test_exists(self):
        self.add_mock_gcs_data({
            'walrus': {'data/foo': b'abcd'}})
        self.assertEqual(self.fs.exists('gs://walrus/data/foo'), True)
        self.assertEqual(self.fs.exists('gs://walrus/data/bar'), False)

    def test_rm(self):
        self.add_mock_gcs_data({
            'walrus': {'foo': b''}})

        self.assertEqual(self.fs.exists('gs://walrus/foo'), True)
        self.fs.rm('gs://walrus/foo')
        self.assertEqual(self.fs.exists('gs://walrus/foo'), False)

    def test_rm_dir(self):
        self.add_mock_gcs_data({
            'walrus': {'data/foo': b'',
                       'data/bar/baz': b''}})

        self.assertEqual(self.fs.exists('gs://walrus/data/foo'), True)
        self.assertEqual(self.fs.exists('gs://walrus/data/bar/baz'), True)
        self.fs.rm('gs://walrus/data')
        self.assertEqual(self.fs.exists('gs://walrus/data/foo'), False)
        self.assertEqual(self.fs.exists('gs://walrus/data/bar/baz'), False)


class GCSFSRegionTestCase(MockBotoTestCase):

    def test_default_endpoint(self):
        fs = GCSFilesystem()

        GCS_conn = fs.make_GCS_conn()
        self.assertEqual(GCS_conn.host, 'GCS.amazonaws.com')

    def test_force_GCS_endpoint(self):
        fs = GCSFilesystem(GCS_endpoint='GCS-us-west-1.amazonaws.com')

        GCS_conn = fs.make_GCS_conn()
        self.assertEqual(GCS_conn.host, 'GCS-us-west-1.amazonaws.com')

    def test_endpoint_for_bucket_in_us_west_2(self):
        self.add_mock_gcs_data({'walrus': {}}, location='us-west-2')

        fs = GCSFilesystem()

        bucket = fs.get_bucket('walrus')
        self.assertEqual(bucket.connection.host,
                         'GCS-us-west-2.amazonaws.com')

    def test_get_location_is_forbidden(self):
        self.add_mock_gcs_data({'walrus': {}}, location='us-west-2')

        fs = GCSFilesystem()

        with patch(
                'tests.mockboto.MockBucket.get_location',
                side_effect=boto.exception.GCSResponseError(403, 'Forbidden')):

            bucket = fs.get_bucket('walrus')

        self.assertEqual(bucket.connection.host, 'GCS.amazonaws.com')

    def test_get_location_other_error(self):
        self.add_mock_gcs_data({'walrus': {}}, location='us-west-2')

        fs = GCSFilesystem()

        with patch(
                'tests.mockboto.MockBucket.get_location',
                side_effect=boto.exception.GCSResponseError(404, 'Not Found')):

            self.assertRaises(boto.exception.GCSResponseError,
                              fs.get_bucket, 'walrus')


    def test_endpoint_for_bucket_in_us_east_1(self):
        # location constraint for us-east-1 is '', not 'us-east-1'
        self.add_mock_gcs_data({'walrus': {}}, location='')

        fs = GCSFilesystem()

        bucket = fs.get_bucket('walrus')
        self.assertEqual(bucket.connection.host, 'GCS.amazonaws.com')

    def test_buckets_from_forced_GCS_endpoint(self):
        self.add_mock_gcs_data({'walrus-east': {}}, location='us-east-2')
        self.add_mock_gcs_data({'walrus-west': {}}, location='us-west-2')

        fs = GCSFilesystem(GCS_endpoint='GCS-us-east-2.amazonaws.com')

        bucket_east = fs.get_bucket('walrus-east')

        with patch('tests.mockboto.MockBucket.get_location') as mock_get_loc:
            self.assertEqual(bucket_east.connection.host,
                             'GCS-us-east-2.amazonaws.com')
            # no reason to check bucket location if endpoint is forced
            self.assertFalse(mock_get_loc.called)

        # can't access this bucket from wrong endpoint!
        self.assertRaises(boto.exception.GCSResponseError,
                          fs.get_bucket, 'walrus-west')
