# Copyright 2009-2016 Yelp and Contributors
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
import fnmatch
import logging
import socket

try:
    import boto
    boto  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    # don't require boto; MRJobs don't actually need it when running
    # inside hadoop streaming
    boto = None

from mrjob.aws import s3_endpoint_for_region
from mrjob.fs.base import Filesystem
from mrjob.parse import is_s3_uri
from mrjob.parse import parse_s3_uri
from mrjob.parse import urlparse
from mrjob.retry import RetryWrapper
from mrjob.runner import GLOB_RE
from mrjob.util import read_file


log = logging.getLogger(__name__)

# if EMR throttles us, how long to wait (in seconds) before trying again?
_EMR_BACKOFF = 20
_EMR_BACKOFF_MULTIPLIER = 1.5
_EMR_MAX_TRIES = 20  # this takes about a day before we run out of tries


def s3_key_to_uri(s3_key):
    """Convert a boto Key object into an ``s3://`` URI"""
    return 's3://%s/%s' % (s3_key.bucket.name, s3_key.name)


def wrap_aws_conn(raw_conn):
    """Wrap a given boto Connection object so that it can retry when
    throttled."""
    def retry_if(ex):
        """Retry if we get a server error indicating throttling. Also
        handle spurious 505s that are thought to be part of a load
        balancer issue inside AWS."""
        return ((isinstance(ex, boto.exception.BotoServerError) and
                 ('Throttling' in ex.body or
                  'RequestExpired' in ex.body or
                  ex.status == 505)) or
                (isinstance(ex, socket.error) and
                 ex.args in ((104, 'Connection reset by peer'),
                             (110, 'Connection timed out'))))

    return RetryWrapper(raw_conn,
                        retry_if=retry_if,
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
                 aws_security_token=None, s3_endpoint=None):
        """
        :param aws_access_key_id: Your AWS access key ID
        :param aws_secret_access_key: Your AWS secret access key
        :param aws_security_token: security token for use with temporary
                                   AWS credentials
        :param s3_endpoint: If set, always use this endpoint
        """
        super(S3Filesystem, self).__init__()
        self._s3_endpoint = s3_endpoint
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_security_token = aws_security_token

    def can_handle_path(self, path):
        return is_s3_uri(path)

    def du(self, path_glob):
        """Get the size of all files matching path_glob."""
        return sum(self.get_s3_key(uri).size for uri in self.ls(path_glob))

    def ls(self, path_glob):
        """Recursively list files on S3.

        *path_glob* can include ``?`` to match single characters or
        ``*`` to match 0 or more characters. Both ``?`` and ``*`` can match
        ``/``.

        .. versionchanged:: 0.5.0

            You no longer need a trailing slash to list "directories" on S3;
            both ``ls('s3://b/dir')`` and `ls('s3://b/dir/')` will list
            all keys starting with ``dir/``.
        """

        # clean up the  base uri to ensure we have an equal uri to boto (s3://)
        # just in case we get passed s3n://
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

        bucket = self.get_bucket(bucket_name)
        for key in bucket.list(base_name):
            uri = "%s://%s/%s" % (scheme, bucket_name, key.name)

            # enforce globbing
            if not (fnmatch.fnmatchcase(uri, path_glob) or
                    fnmatch.fnmatchcase(uri, dir_glob)):
                continue

            yield uri

    def md5sum(self, path):
        k = self.get_s3_key(path)
        return k.etag.strip('"')

    def _cat_file(self, filename):
        # stream lines from the s3 key
        s3_key = self.get_s3_key(filename)
        # yields_lines=False: warn read_file that s3_key yields chunks of bytes
        return read_file(
            s3_key_to_uri(s3_key), fileobj=s3_key, yields_lines=False)

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
        # just fall back on ls(); it's smart
        try:
            paths = self.ls(path_glob)
        except boto.exception.S3ResponseError:
            paths = []
        return any(paths)

    def rm(self, path_glob):
        """Remove all files matching the given glob."""
        for uri in self.ls(path_glob):
            key = self.get_s3_key(uri)
            if key:
                log.debug('deleting ' + uri)
                key.delete()

    def touchz(self, dest):
        """Make an empty file in the given location. Raises an error if
        a non-empty file already exists in that location."""
        key = self.get_s3_key(dest)
        if key and key.size != 0:
            raise OSError('Non-empty file %r already exists!' % (dest,))

        self.make_s3_key(dest).set_contents_from_string('')

    # Utilities for interacting with S3 using S3 URIs.

    # Try to use the more general filesystem interface unless you really
    # need to do something S3-specific (e.g. setting file permissions)

    def make_s3_conn(self, region=''):
        """Create a connection to S3.

        :param region: region to use to choose S3 endpoint.

        If you are doing anything with buckets other than creating them
        or fetching basic metadata (name and location), it's best to use
        :py:meth:`get_bucket` because it chooses the appropriate S3 endpoint
        automatically.

        :return: a :py:class:`boto.s3.connection.S3Connection`, wrapped in a
                 :py:class:`mrjob.retry.RetryWrapper`
        """
        # give a non-cryptic error message if boto isn't installed
        if boto is None:
            raise ImportError('You must install boto to connect to S3')

        # self._s3_endpoint overrides region
        host = self._s3_endpoint or s3_endpoint_for_region(region)

        log.debug('creating S3 connection (to %s)' % host)

        raw_s3_conn = boto.connect_s3(
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            host=host,
            security_token=self._aws_security_token)
        return wrap_aws_conn(raw_s3_conn)

    def get_bucket(self, bucket_name):
        """Get the bucket, connecting through the appropriate endpoint."""
        s3_conn = self.make_s3_conn()

        bucket = s3_conn.get_bucket(bucket_name)
        if self._s3_endpoint:
            return bucket

        try:
            location = bucket.get_location()
        except boto.exception.S3ResponseError as e:
            # it's possible to have access to a bucket but not access
            # to its location metadata. This happens on the 'elasticmapreduce'
            # bucket, for example (see #1170)
            if e.status == 403:
                log.warning('Could not infer endpoint for bucket %s; '
                            'assuming %s', bucket_name, s3_conn.host)
                return bucket

            raise

        if (s3_endpoint_for_region(location) != s3_conn.host):
            s3_conn = self.make_s3_conn(location)
            bucket = s3_conn.get_bucket(bucket_name)

        return bucket

    def get_s3_key(self, uri):
        """Get the boto Key object matching the given S3 uri, or
        return None if that key doesn't exist.

        uri is an S3 URI: ``s3://foo/bar``
        """
        bucket_name, key_name = parse_s3_uri(uri)

        try:
            bucket = self.get_bucket(bucket_name)
        except boto.exception.S3ResponseError as e:
            if e.status != 404:
                raise e
            key = None
        else:
            key = bucket.get_key(key_name)

        return key

    def make_s3_key(self, uri):
        """Create the given S3 key, and return the corresponding
        boto Key object.

        uri is an S3 URI: ``s3://foo/bar``
        """
        bucket_name, key_name = parse_s3_uri(uri)

        return self.get_bucket(bucket_name).new_key(key_name)

    def get_s3_keys(self, uri):
        """Get a stream of boto Key objects for each key inside
        the given dir on S3.

        uri is an S3 URI: ``s3://foo/bar``
        """
        bucket_name, key_prefix = parse_s3_uri(uri)
        bucket = self.get_bucket(bucket_name)
        for key in bucket.list(key_prefix):
            yield key

    def get_all_buckets(self):
        """Get a stream of all buckets owned by this user on S3."""
        return self.make_s3_conn().get_all_buckets()

    def create_bucket(self, bucket_name, location=''):
        """Create a bucket on S3, optionally setting location constraint."""
        return self.make_s3_conn().create_bucket(
            bucket_name, location=location)
