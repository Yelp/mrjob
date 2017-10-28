# Copyright 2009-2016 Yelp and Contributors
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
"""S3 Filesystem.

Also the place for common code used to establish and wrap AWS connections."""
import fnmatch
import logging
import socket

try:
    import botocore.client
    botocore  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    botocore = None

try:
    import boto3
    boto3  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    boto3 = None

from mrjob.aws import _S3_REGION_WITH_NO_LOCATION_CONSTRAINT
from mrjob.cat import decompress
from mrjob.fs.base import Filesystem
from mrjob.parse import is_uri
from mrjob.parse import is_s3_uri
from mrjob.parse import parse_s3_uri
from mrjob.parse import urlparse
from mrjob.retry import RetryWrapper
from mrjob.runner import GLOB_RE


log = logging.getLogger(__name__)

_CHUNK_SIZE = 8192

# if EMR throttles us, how long to wait (in seconds) before trying again?
_EMR_BACKOFF = 20
_EMR_BACKOFF_MULTIPLIER = 1.5
_EMR_MAX_TRIES = 20  # this takes about a day before we run out of tries


def _client_error_code(ex):
    """Get the error code for the given ClientError"""
    return ex.response.get('Error', {}).get('Code', '')


def _client_error_status(ex):
    """Get the HTTP status for the given ClientError"""
    resp = ex.response
    # sometimes status code is in ResponseMetadata, not Error
    return (resp.get('Error', {}).get('HTTPStatusCode') or
            resp.get('ResponseMetadata', {}).get('HTTPStatusCode'))


def _endpoint_url(host_or_uri):
    """If *host_or_uri* is non-empty and isn't a URI, prepend ``'https://'``.

    Otherwise, pass through as-is.
    """
    if not host_or_uri:
        return host_or_uri
    elif is_uri(host_or_uri):
        return host_or_uri
    else:
        return 'https://' + host_or_uri


def _get_bucket_region(client, bucket_name):
    """Look up the given bucket's location constraint and translate
    it to a region name."""
    resp = client.get_bucket_location(Bucket=bucket_name)
    return resp['LocationConstraint'] or _S3_REGION_WITH_NO_LOCATION_CONSTRAINT


def _is_retriable_client_error(ex):
    """Is the exception from a boto3 client retriable?"""
    if isinstance(ex, botocore.exceptions.ClientError):
        code = _client_error_code(ex)
        # "Throttl" catches "Throttled" and "Throttling"
        if any(c in code for c in ('Throttl', 'RequestExpired', 'Timeout')):
            return True
        # spurious 505s thought to be part of an AWS load balancer issue
        return _client_error_status(ex) == 505
    elif isinstance(ex, socket.error):
        return ex.args in ((104, 'Connection reset by peer'),
                           (110, 'Connection timed out'))
    else:
        return False


def _wrap_aws_client(raw_client):
    """Wrap a given boto3 Client object so that it can retry when
    throttled."""
    return RetryWrapper(raw_client,
                        retry_if=_is_retriable_client_error,
                        backoff=_EMR_BACKOFF,
                        multiplier=_EMR_BACKOFF_MULTIPLIER,
                        max_tries=_EMR_MAX_TRIES)


class S3Filesystem(Filesystem):
    """Filesystem for Amazon S3 URIs. Typically you will get one of these via
    ``EMRJobRunner().fs``, composed with
    :py:class:`~mrjob.fs.ssh.SSHFilesystem` and
    :py:class:`~mrjob.fs.local.LocalFilesystem`.
    """

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 aws_session_token=None, s3_endpoint=None, s3_region=None):
        """
        :param aws_access_key_id: Your AWS access key ID
        :param aws_secret_access_key: Your AWS secret access key
        :param aws_session_token: session token for use with temporary
                                   AWS credentials
        :param s3_endpoint: If set, always use this endpoint
        :param s3_region: Region name corresponding to s3_endpoint. Only used
                          if *s3_endpoint* is set
        """
        super(S3Filesystem, self).__init__()
        self._s3_endpoint_url = _endpoint_url(s3_endpoint)
        self._s3_region = s3_region
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_session_token = aws_session_token

    def can_handle_path(self, path):
        return is_s3_uri(path)

    def du(self, path_glob):
        """Get the size of all files matching path_glob."""
        return sum(key.size for uri, key in self._ls(path_glob))

    def ls(self, path_glob):
        """Recursively yield the URIs of S3 keys matching the given glob.

        *path_glob* can include ``?`` to match single characters or
        ``*`` to match 0 or more characters. Both ``?`` and ``*`` can match
        ``/``.

        .. versionchanged:: 0.5.0

            You no longer need a trailing slash to list "directories" on S3;
            both ``ls('s3://b/dir')`` and `ls('s3://b/dir/')` will list
            all keys starting with ``dir/``.
        """
        for uri, key in self._ls(path_glob):
            yield uri

    def _ls(self, path_glob):
        """Helper method for :py:meth:`ls`; yields tuples of
        ``(uri, key)`` where *key* is the corresponding boto3 s3.ObjectSummary.
        """
        # clean up the  base uri to ensure we have pass boto3 an s3:// URI
        # (not s3n://)
        scheme = urlparse(path_glob).scheme

        # support globs
        glob_match = GLOB_RE.match(path_glob)

        # we're going to search for all keys starting with base_uri
        if glob_match:
            # cut it off at first wildcard
            base_uri = glob_match.group(1)
        else:
            base_uri = path_glob

        bucket_name, base_name = parse_s3_uri(base_uri)

        # allow subdirectories of the path/glob
        if path_glob and not path_glob.endswith('/'):
            dir_glob = path_glob + '/*'
        else:
            dir_glob = path_glob + '*'

        try:
            bucket = self.get_bucket(bucket_name)
        except botocore.exceptions.ClientError as ex:
            if _client_error_status(ex) == 404:  # treat nonexistent as empty
                return
            raise

        for key in bucket.objects.filter(Prefix=base_name):
            uri = "%s://%s/%s" % (scheme, bucket_name, key.key)

            # enforce globbing
            if not (fnmatch.fnmatchcase(uri, path_glob) or
                    fnmatch.fnmatchcase(uri, dir_glob)):
                continue

            yield uri, key

    def md5sum(self, path):
        k = self._get_s3_key(path)
        return k.e_tag.strip('"')

    def _cat_file(self, filename):
        # stream lines from the s3 key
        s3_key = self._get_s3_key(filename)
        body = s3_key.get()['Body']

        return decompress(body, filename)

    def mkdir(self, dest):
        """Make a directory. This does nothing on S3 because there are
        no directories.
        """
        pass

    def exists(self, path_glob):
        """Does the given path exist?

        If dest is a directory (ends with a "/"), we check if there are
        any files starting with that path.
        """
        # just fall back on _ls(); it's smart
        return any(self._ls(path_glob))

    def rm(self, path_glob):
        """Remove all files matching the given glob."""
        for uri, key in self._ls(path_glob):
            log.debug('deleting ' + uri)
            key.delete()

    def touchz(self, dest):
        """Make an empty file in the given location. Raises an error if
        a non-empty file already exists in that location."""
        key = self._get_s3_key(dest)

        data = None
        try:
            data = key.get()
        except botocore.exceptions.ClientError as ex:
            # okay if key doesn't exist
            if _client_error_status(ex) != 404:
                raise

        if data and data['ContentLength'] != 0:
            raise OSError('Non-empty file %r already exists!' % (dest,))

        key.put(Body=b'')

    # Utilities for interacting with S3 using S3 URIs.

    # Try to use the more general filesystem interface unless you really
    # need to do something S3-specific (e.g. setting file permissions)

    # sadly resources aren't as smart as we'd like; they provide a Bucket
    # abstraction, but don't automatically connect to buckets on the
    # correct region

    def make_s3_resource(self, region_name=None):
        """Create a :py:mod:`boto3` S3 resource, with its client
        wrapped in a :py:class:`mrjob.retry.RetryWrapper`

        :param region: region to use to choose S3 endpoint

        It's best to use :py:meth:`get_bucket` because it chooses the
        appropriate S3 endpoint automatically. If you are trying to get
        bucket metadata, use :py:meth:`make_s3_client`.

        .. versionadded:: 0.6.0
        """
        # give a non-cryptic error message if boto3 isn't installed
        if boto3 is None:
            raise ImportError('You must install boto3 to connect to S3')

        kwargs = self._client_kwargs(region_name)

        log.debug('creating S3 resource (%s)' % (
            kwargs['endpoint_url'] or kwargs['region_name'] or 'default'))

        s3_resource = boto3.resource('s3', **kwargs)
        s3_resource.meta.client = _wrap_aws_client(s3_resource.meta.client)

        return s3_resource

    def make_s3_client(self, region_name=None):
        """Create a :py:mod:`boto3` S3 client,
        wrapped in a :py:class:`mrjob.retry.RetryWrapper`

        :param region: region to use to choose S3 endpoint.

        .. versionadded:: 0.6.0
        """
        # give a non-cryptic error message if boto3 isn't installed
        if boto3 is None:
            raise ImportError('You must install boto3 to connect to S3')

        kwargs = self._client_kwargs(region_name or self._s3_region)

        log.debug('creating S3 client (%s)' % (
            kwargs['endpoint_url'] or kwargs['region_name'] or 'default'))

        return _wrap_aws_client(boto3.client('s3', **kwargs))

    def _client_kwargs(self, region_name):
        """Keyword args for creating resources or clients."""

        return dict(
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            aws_session_token=self._aws_session_token,
            endpoint_url=self._s3_endpoint_url,
            region_name=(region_name or self._s3_region),
        )

    def get_bucket(self, bucket_name):
        """Get the (:py:mod:`boto3`) bucket, connecting through the
        appropriate endpoint."""
        client = self.make_s3_client()

        try:
            region_name = _get_bucket_region(client, bucket_name)
        except botocore.exceptions.ClientError as ex:
            # it's possible to have access to a bucket but not access
            # to its location metadata. This happens on the 'elasticmapreduce'
            # bucket, for example (see #1170)
            if _client_error_status(ex) != 403:
                raise
            log.warning('Could not infer endpoint for bucket %s; '
                        'assuming defaults', bucket_name)
            region_name = None

        resource = self.make_s3_resource(region_name)
        return resource.Bucket(bucket_name)

    def _get_s3_key(self, uri):
        """Get the boto3 s3.Object matching the given S3 uri, or
        return None if that key doesn't exist.

        uri is an S3 URI: ``s3://foo/bar``
        """
        bucket_name, key_name = parse_s3_uri(uri)
        return self.get_bucket(bucket_name).Object(key_name)

    def get_all_bucket_names(self):
        """Get a stream of the names of all buckets owned by this user
        on S3.

        .. versionadded:: 0.6.0
        """
        # we don't actually want to return these Bucket objects to
        # the user because their client might connect to the wrong region
        # endpoint
        r = self.make_s3_resource()
        for b in r.buckets.all():
            yield b.name

    def create_bucket(self, bucket_name, region=None):
        """Create a bucket on S3 with a location constraint
        matching the given region.

        .. versionchanged:: 0.6.0

           The *region* argument used to be called *location*.
        """
        client = self.make_s3_client()

        conf = {}
        if region and region != _S3_REGION_WITH_NO_LOCATION_CONSTRAINT:
            conf['LocationConstraint'] = region

        client.create_bucket(Bucket=bucket_name,
                             CreateBucketConfiguration=conf)
