# Copyright 2009-2013 Yelp and Contributors
# Copyright 2015 Yelp
# Copyright 2017 Yelp and Contributors
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

from botocore.exceptions import ClientError

from mrjob.fs.s3 import S3Filesystem

from tests.compress import gzip_compress
from tests.mock_boto3 import MockBoto3TestCase
from tests.py2 import patch


class CatTestCase(MockBoto3TestCase):

    def setUp(self):
        super(CatTestCase, self).setUp()
        self.fs = S3Filesystem()

    def test_cat_uncompressed(self):
        self.add_mock_s3_data(
            {'walrus': {'data/foo': b'foo\nfoo\n'}})

        self.assertEqual(
            b''.join(self.fs._cat_file('s3://walrus/data/foo')),
            b'foo\nfoo\n')

    def test_cat_bz2(self):
        self.add_mock_s3_data(
            {'walrus': {'data/foo.bz2': bz2.compress(b'foo\n' * 1000)}})

        self.assertEqual(
            b''.join(self.fs._cat_file('s3://walrus/data/foo.bz2')),
            b'foo\n' * 1000)

    def test_cat_gz(self):
        self.add_mock_s3_data(
            {'walrus': {'data/foo.gz': gzip_compress(b'foo\n' * 10000)}})

        self.assertEqual(
            b''.join(self.fs._cat_file('s3://walrus/data/foo.gz')),
            b'foo\n' * 10000)

    def test_chunks_file(self):
        self.add_mock_s3_data(
            {'walrus': {'data/foo': b'foo\n' * 1000}})

        self.assertGreater(
            len(list(self.fs._cat_file('s3://walrus/data/foo'))),
            1)


class S3FSTestCase(MockBoto3TestCase):

    def setUp(self):
        super(S3FSTestCase, self).setUp()
        self.fs = S3Filesystem()

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

    def test_ls_s3a(self):
        self.add_mock_s3_data(
            {'walrus': {'data/bar': b'abc123',
                        'data/baz': b'123abc'}})

        self.assertEqual(list(self.fs.ls('s3a://walrus/data/*')),
                         ['s3a://walrus/data/bar',
                          's3a://walrus/data/baz'])

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

    def test_md5sum(self):
        self.add_mock_s3_data({
            'walrus': {'data/foo': b'abcd'}})

        self.assertEqual(self.fs.md5sum('s3://walrus/data/foo'),
                         'e2fc714c4727ee9395f324cd2e7f331f')

    def test_touchz(self):
        self.add_mock_s3_data({'walrus': {}})

        self.assertEqual(list(self.fs.ls('s3://walrus/')), [])

        self.fs.touchz('s3://walrus/empty')

        self.assertEqual(list(self.fs.ls('s3://walrus/')),
                         ['s3://walrus/empty'])

    def test_okay_to_touchz_empty_file(self):
        self.add_mock_s3_data({'walrus': {'empty': b''}})

        self.assertEqual(list(self.fs.ls('s3://walrus/')),
                         ['s3://walrus/empty'])

        self.fs.touchz('s3://walrus/empty')

        self.assertEqual(list(self.fs.ls('s3://walrus/')),
                         ['s3://walrus/empty'])

    def test_cant_touchz_file_with_contents(self):
        self.add_mock_s3_data({'walrus': {'full': b'contents'}})

        self.assertRaises(OSError,
                          self.fs.touchz, 's3://walrus/full')

    def test_mkdir_does_nothing(self):
        self.add_mock_s3_data({'walrus': {}})

        self.assertEqual(list(self.fs.ls('s3://walrus/')), [])

        self.fs.mkdir('s3://walrus/data')

        self.assertEqual(list(self.fs.ls('s3://walrus/')), [])

    # S3-specific utilities

    def test_get_all_bucket_names(self):
        self.add_mock_s3_data({'walrus': {}, 'kitteh': {}})

        self.assertEqual(list(self.fs.get_all_bucket_names()),
                         ['kitteh', 'walrus'])


class S3FSRegionTestCase(MockBoto3TestCase):

    def test_default_endpoint(self):
        fs = S3Filesystem()

        client = fs.make_s3_client()
        self.assertEqual(client.meta.endpoint_url,
                         'https://s3.amazonaws.com')

        resource = fs.make_s3_resource()
        self.assertEqual(resource.meta.client.meta.endpoint_url,
                         'https://s3.amazonaws.com')

    def test_force_s3_endpoint_host(self):
        fs = S3Filesystem(s3_endpoint='myproxy')

        client = fs.make_s3_client()
        self.assertEqual(client.meta.endpoint_url,
                         'https://myproxy')

        resource = fs.make_s3_resource()
        self.assertEqual(resource.meta.client.meta.endpoint_url,
                         'https://myproxy')

    def test_force_s3_endpoint_url(self):
        fs = S3Filesystem(s3_endpoint='https://myproxy:8080')

        client = fs.make_s3_client()
        self.assertEqual(client.meta.endpoint_url,
                         'https://myproxy:8080')

        resource = fs.make_s3_resource()
        self.assertEqual(resource.meta.client.meta.endpoint_url,
                         'https://myproxy:8080')

    def test_force_s3_endpoint_region(self):
        # this is the actual mrjob default region
        fs = S3Filesystem(s3_region='us-west-2')

        client = fs.make_s3_client()
        self.assertEqual(client.meta.endpoint_url,
                         'https://s3-us-west-2.amazonaws.com')
        self.assertEqual(client.meta.region_name,
                         'us-west-2')

        resource = fs.make_s3_resource()
        self.assertEqual(resource.meta.client.meta.endpoint_url,
                         'https://s3-us-west-2.amazonaws.com')
        self.assertEqual(resource.meta.client.meta.region_name,
                         'us-west-2')

    def test_endpoint_for_bucket_in_us_west_1(self):
        self.add_mock_s3_data({'walrus': {}}, location='us-west-1')

        fs = S3Filesystem()

        bucket = fs.get_bucket('walrus')
        self.assertEqual(bucket.meta.client.meta.region_name,
                         'us-west-1')

    def test_get_location_is_forbidden(self):
        self.add_mock_s3_data({'walrus': {}}, location='us-west-2')

        fs = S3Filesystem()

        access_denied_error = ClientError(
            dict(
                Error=dict(
                    Code='AccessDenied',
                    Message='Access Denied',
                ),
                ResponseMetadata=dict(
                    HTTPStatusCode=403
                ),
            ),
            'GetBucketLocation')

        with patch(
                'tests.mock_boto3.s3.MockS3Client.get_bucket_location',
                side_effect=access_denied_error):

            bucket = fs.get_bucket('walrus')

        self.assertEqual(bucket.meta.client.meta.endpoint_url,
                         'https://s3.amazonaws.com')
        self.assertEqual(bucket.meta.client.meta.region_name, 'us-east-1')

    def test_bucket_does_not_exist(self):
        fs = S3Filesystem()

        self.assertRaises(ClientError, fs.get_bucket, 'walrus')

    def test_endpoint_for_bucket_in_us_east_1(self):
        # location constraint for us-east-1 is '', not 'us-east-1'
        self.add_mock_s3_data({'walrus': {}}, location='')

        fs = S3Filesystem()

        bucket = fs.get_bucket('walrus')
        self.assertEqual(bucket.meta.client.meta.endpoint_url,
                         'https://s3.amazonaws.com')

    def test_buckets_from_forced_s3_endpoint(self):
        self.add_mock_s3_data({'walrus-east': {}}, location='us-east-2')

        fs = S3Filesystem(s3_endpoint='s3-us-west-2.amazonaws.com')

        bucket_east = fs.get_bucket('walrus-east')

        with patch('tests.mock_boto3.s3.MockS3Client.get_bucket_location'
                   ) as mock_gbl:
            # won't actually be able to access this bucket from this endpoint,
            # but boto3 doesn't check that on bucket creation
            self.assertEqual(bucket_east.meta.client.meta.endpoint_url,
                             'https://s3-us-west-2.amazonaws.com')
            # no reason to check bucket location if endpoint is forced
            self.assertFalse(mock_gbl.called)

    def test_create_bucket_with_no_region(self):
        fs = S3Filesystem()

        fs.create_bucket('walrus')

        s3_client = fs.make_s3_client()
        self.assertEqual(
            s3_client.get_bucket_location('walrus')['LocationConstraint'],
            None)

        bucket = fs.get_bucket('walrus')

        self.assertEqual(bucket.meta.client.meta.endpoint_url,
                         'https://s3.amazonaws.com')
        self.assertEqual(bucket.meta.client.meta.region_name,
                         'us-east-1')

    def test_create_bucket_in_us_east_1(self):
        fs = S3Filesystem()

        fs.create_bucket('walrus', region='us-east-1')

        s3_client = fs.make_s3_client()
        self.assertEqual(
            s3_client.get_bucket_location('walrus')['LocationConstraint'],
            None)

        bucket = fs.get_bucket('walrus')

        self.assertEqual(bucket.meta.client.meta.endpoint_url,
                         'https://s3.amazonaws.com')
        self.assertEqual(bucket.meta.client.meta.region_name,
                         'us-east-1')

    def test_create_bucket_in_us_west_2(self):
        fs = S3Filesystem()

        fs.create_bucket('walrus', region='us-west-2')

        s3_client = fs.make_s3_client()
        self.assertEqual(
            s3_client.get_bucket_location('walrus')['LocationConstraint'],
            'us-west-2')

        bucket = fs.get_bucket('walrus')

        self.assertEqual(bucket.meta.client.meta.endpoint_url,
                         'https://s3-us-west-2.amazonaws.com')
        self.assertEqual(bucket.meta.client.meta.region_name,
                         'us-west-2')
