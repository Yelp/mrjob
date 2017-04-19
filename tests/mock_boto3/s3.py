# Copyright 2009-2017 Yelp and Contributors
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
"""Mock boto3 S3 support."""
import hashlib
from datetime import timedelta

from botocore.exceptions import ClientError
from boto3.exceptions import S3UploadFailedError

from mrjob.aws import _DEFAULT_AWS_REGION
from mrjob.aws import _boto3_now

from .util import MockClientMeta


class MockS3Client(object):
    """Mock out boto3 S3 client

    :param mock_s3_fs: Maps bucket name to a dictionary with the keys *keys*
                       and *location*. *keys* maps key name to tuples of
                       ``(data, time_modified)``. *data* is bytes, and
                        *time_modified* is a UTC
                        :py:class:`~datetime.datetime`. *location* is an
                        optional location constraint for the bucket
                        (a region name).
    """
    def __init__(self,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 aws_session_token=None,
                 endpoint_url=None,
                 region_name=None,
                 mock_s3_fs=None):

        self.mock_s3_fs = mock_s3_fs

        region_name = region_name or _DEFAULT_AWS_REGION
        if not endpoint_url:
            if region_name == _DEFAULT_AWS_REGION:
                endpoint_url = 'https://s3.amazonaws.com'
            else:
                endpoint_url = 'https://s3-%s.amazonaws.com' % region_name

        self.meta = MockClientMeta(
            endpoint_url=endpoint_url,
            region_name=region_name)

    def _check_bucket_exists(self, bucket_name, operation_name):
        if bucket_name not in self.mock_s3_fs:
            raise _no_such_bucket_error(bucket_name, operation_name)

    def create_bucket(self, Bucket, CreateBucketConfiguration=None):
        # boto3 doesn't seem to mind if you try to create a bucket that exists
        if Bucket not in self.mock_s3_fs:
            location = (CreateBucketConfiguration or {}).get(
                'LocationConstraint', '')
            self.mock_s3_fs[Bucket] = dict(keys={}, location=location)

        # "Location" here actually refers to the bucket name
        return dict(Location=('/' + Bucket))

    def get_bucket_location(self, Bucket):
        self._check_bucket_exists(Bucket, 'GetBucketLocation')

        location_constraint = self.mock_s3_fs[Bucket].get('location') or None

        return dict(LocationConstraint=location_constraint)


def add_mock_s3_data(mock_s3_fs, data, age=None, location=None):
    """Update *mock_s3_fs* with a map from bucket name to key name to data.

    :param age: a timedelta
    :param location string: the bucket's location constraint (a region name)
    """
    age = age or timedelta(0)
    time_modified = _boto3_now() - age

    for bucket_name, key_name_to_bytes in data.items():
        bucket = mock_s3_fs.setdefault(bucket_name,
                                       {'keys': {}, 'location': ''})

        for key_name, key_data in key_name_to_bytes.items():
            if not isinstance(key_data, bytes):
                raise TypeError('mock s3 data must be bytes')
            bucket['keys'][key_name] = (key_data, time_modified)

        if location is not None:
            bucket['location'] = location


class MockS3Resource(object):
    """Mock out boto3 S3 resource"""
    def __init__(self,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 aws_session_token=None,
                 endpoint_url=None,
                 region_name=None,
                 mock_s3_fs=None):

        self.mock_s3_fs = mock_s3_fs

        self.meta = MockClientMeta(
            client=MockS3Client(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                endpoint_url=endpoint_url,
                region_name=region_name,
                mock_s3_fs=mock_s3_fs
            )
        )

        self.buckets = MockS3Buckets(_client=self.meta.client)

    def Bucket(self, name):
        # boto3's Bucket() doesn't care if the bucket exists
        return MockS3Bucket(self.meta.client, name)


class MockS3Buckets(object):
    """The *buckets* field of a :py:class:`MockS3Resource`"""
    def __init__(self, _client):
        self._client = _client

    def all(self):
        # technically, this only lists buckets we own, but our mock fs
        # doesn't simulate buckets owned by others
        for bucket_name in sorted(self._client.mock_s3_fs):
            yield MockS3Bucket(self._client, bucket_name)


class MockS3Bucket(object):
    """Mock out boto3 bucket
    """
    def __init__(self, client, name):
        """Create a mock bucket with the given name and client
        """
        self.name = name
        self.meta = MockClientMeta(client=client)

        self.objects = MockS3Objects(self)

    def Object(self, key):
        return MockS3Object(self.meta.client, self.name, key)

    def _check_bucket_exists(self, operation_name):
        if self.name not in self.meta.client.mock_s3_fs:
            raise _no_such_bucket_error(self.name, operation_name)


class MockS3Objects(object):
    """The *objects* field of a :py:class:`MockS3Bucket`"""
    def __init__(self, _bucket):
        self._bucket = _bucket

    def all(self):
        return self.filter()

    def filter(self, Prefix=None):
        self._bucket._check_bucket_exists('ListObjects')

        # there are several other keyword arguments that we don't support
        mock_s3_fs = self._bucket.meta.client.mock_s3_fs

        for key in sorted(mock_s3_fs[self._bucket.name]['keys']):
            if Prefix and not key.startswith(Prefix):
                continue

            key = self._bucket.Object(key)
            # emulate ObjectSummary by pre-filling size, e_tag, etc.
            key.get()
            yield key


class MockS3Object(object):
    """Mock out s3.Object"""

    def __init__(self, client, bucket_name, key):
        self.bucket_name = bucket_name
        self.key = key

        self.meta = MockClientMeta(client=client)

    def delete(self):
        mock_keys = self._mock_bucket_keys('DeleteObject')

        # okay if key doesn't exist
        if self.key in mock_keys:
            del mock_keys[self.key]

        return {}

    def get(self):
        key_data, mtime = self._get_key_data_and_mtime()

        # fill in known attributes
        m = hashlib.md5()
        m.update(key_data)

        self.e_tag = '"%s"' % m.hexdigest()
        self.last_modified = mtime
        self.size = len(key_data)

        return dict(
            Body=MockStreamingBody(key_data),
            ContentLength=self.size,
            ETag=self.e_tag,
            LastModified=self.last_modified,
        )

    def put(self, Body):
        if not isinstance(Body, bytes):
            raise NotImplementedError('mock put() only support bytes')

        mock_keys = self._mock_bucket_keys('PutObject')

        if isinstance(Body, bytes):
            data = Body
        elif hasattr(Body, 'read'):
            data = Body.read()

        if not isinstance(data, bytes):
            raise TypeError('Body or Body.read() must be bytes')

        mock_keys[self.key] = (data, _boto3_now())

    def upload_file(self, path, Config=None):
        if self.bucket_name not in self.meta.client.mock_s3_fs:
            # upload_file() is a higher-order operation, has fancy errors
            raise S3UploadFailedError(
                'Failed to upload %s to %s/%s: %s' % (
                    path, self.bucket_name, self.key,
                    str(_no_such_bucket_error('PutObject'))))

        mock_keys = self._mock_bucket_keys('PutObject')
        with open(path, 'rb') as f:
            mock_keys[self.key] = (f.read(), _boto3_now())

    def __getattr__(self, key):
        if key in ('e_tag', 'last_modified', 'size'):
            try:
                self.get()
            except ClientError:
                pass

        if hasattr(self, key):
            return getattr(self, key)
        else:
            raise AttributeError(
                "'s3.Object' object has no attribute '%s'" % key)

    def _mock_bucket_keys(self, operation_name):
        self._check_bucket_exists(operation_name)

        return self.meta.client.mock_s3_fs[self.bucket_name]['keys']

    def _check_bucket_exists(self, operation_name):
        if self.bucket_name not in self.meta.client.mock_s3_fs:
            raise _no_such_bucket_error(self.bucket_name, operation_name)

    def _get_key_data_and_mtime(self):
        """Return (key_data, time_modified)."""
        mock_keys = self._mock_bucket_keys('GetBucket')

        if self.key not in mock_keys:
            raise _no_such_key_error(self.key, 'GetObject')

        return mock_keys[self.key]


class MockStreamingBody(object):
    """Mock of boto3's not-really-a-fileobj for reading from S3"""

    def __init__(self, data):
        if not isinstance(data, bytes):
            raise TypeError

        self._data = data
        self._offset = 0

    def read(self, amt=None):
        start = self._offset

        if amt is None:
            end = len(self._data)
        else:
            end = start + amt

        self._offset = end
        return self._data[start:end]


# Errors

def _no_such_bucket_error(bucket_name, operation_name):
    return ClientError(
        dict(
            Error=dict(
                Bucket=bucket_name,
                Code='NoSuchBucket',
                Message='The specified bucket does not exist',
            ),
            ResponseMetadata=dict(
                HTTPStatusCode=404
            ),
        ),
        operation_name)


def _no_such_key_error(key_name, operation_name):
    return ClientError(
        dict(
            Error=dict(
                Code='NoSuchKey',
                Key=key_name,
                Message='The specified key does not exist',
            ),
            ResponseMetadata=dict(
                HTTPStatusCode=404,
            ),
        ),
        operation_name,
    )
