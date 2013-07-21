# Copyright 2009-2012 Yelp and Contributors
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
import posixpath
import socket

try:
    import boto
    boto  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    # don't require boto; MRJobs don't actually need it when running
    # inside hadoop streaming
    boto = None

from mrjob.fs.base import Filesystem
from mrjob.parse import is_s3_uri
from mrjob.parse import parse_s3_uri
from urlparse import urlparse
from mrjob.retry import RetryWrapper
from mrjob.runner import GLOB_RE
from mrjob.util import buffer_iterator_to_line_iterator
from mrjob.util import read_file


log = logging.getLogger('mrjob.fs.s3')

# if EMR throttles us, how long to wait (in seconds) before trying again?
EMR_BACKOFF = 20
EMR_BACKOFF_MULTIPLIER = 1.5
EMR_MAX_TRIES = 20  # this takes about a day before we run out of tries


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
                        backoff=EMR_BACKOFF,
                        multiplier=EMR_BACKOFF_MULTIPLIER,
                        max_tries=EMR_MAX_TRIES)


class S3Filesystem(Filesystem):
    """Filesystem for Amazon S3 URIs. Typically you will get one of these via
    ``EMRJobRunner().fs``, composed with
    :py:class:`~mrjob.fs.ssh.SSHFilesystem` and
    :py:class:`~mrjob.fs.local.LocalFilesystem`.
    """

    def __init__(self, aws_access_key_id, aws_secret_access_key, s3_endpoint):
        """
        :param aws_access_key_id: Your AWS access key ID
        :param aws_secret_access_key: Your AWS secret access key
        :param s3_endpoint: S3 endpoint to access, e.g. ``us-west-1``
        """
        super(S3Filesystem, self).__init__()
        self._s3_endpoint = s3_endpoint
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key

    def can_handle_path(self, path):
        return is_s3_uri(path)

    def du(self, path_glob):
        """Get the size of all files matching path_glob."""
        return sum(self.get_s3_key(uri).size for uri in self.ls(path_glob))

    def ls(self, path_glob):
        """Recursively list files on S3.

        This doesn't list "directories" unless there's actually a
        corresponding key ending with a '/' (which is weird and confusing;
        don't make S3 keys ending in '/')

        To list a directory, path_glob must end with a trailing
        slash (foo and foo/ are different on S3)
        """

        # clean up the  base uri to ensure we have an equal uri to boto (s3://)
        # just incase we get passed s3n://
        scheme = urlparse(path_glob).scheme

        # support globs
        glob_match = GLOB_RE.match(path_glob)

        # if it's a "file" (doesn't end with /), just check if it exists
        if not glob_match and not path_glob.endswith('/'):
            uri = path_glob
            if self.get_s3_key(uri):
                yield uri
            return

        # we're going to search for all keys starting with base_uri
        if glob_match:
            # cut it off at first wildcard
            base_uri = glob_match.group(1)
        else:
            base_uri = path_glob

        for uri in self._s3_ls(base_uri):
            uri = "%s://%s/%s" % ((scheme,) + parse_s3_uri(uri))

            # enforce globbing
            if glob_match and not fnmatch.fnmatchcase(uri, path_glob):
                continue

            yield uri

    def _s3_ls(self, uri):
        """Helper for ls(); doesn't bother with globbing or directories"""
        s3_conn = self.make_s3_conn()
        bucket_name, key_name = parse_s3_uri(uri)

        bucket = s3_conn.get_bucket(bucket_name)
        for key in bucket.list(key_name):
            yield s3_key_to_uri(key)

    def md5sum(self, path, s3_conn=None):
        k = self.get_s3_key(path, s3_conn=s3_conn)
        return k.etag.strip('"')

    def _cat_file(self, filename):
        # stream lines from the s3 key
        s3_key = self.get_s3_key(filename)
        buffer_iterator = read_file(s3_key_to_uri(s3_key), fileobj=s3_key)
        return buffer_iterator_to_line_iterator(buffer_iterator)

    def mkdir(self, dest):
        """Make a directory. This does nothing on S3 because there are
        no directories.
        """
        pass

    def path_exists(self, path_glob):
        """Does the given path exist?

        If dest is a directory (ends with a "/"), we check if there are
        any files starting with that path.
        """
        # just fall back on ls(); it's smart
        try:
            paths = self.ls(path_glob)
        except boto.exception.S3ResponseError, e:
            paths = []
        return any(paths)

    def path_join(self, dirname, filename):
        return posixpath.join(dirname, filename)

    def rm(self, path_glob):
        """Remove all files matching the given glob."""
        s3_conn = self.make_s3_conn()
        for uri in self.ls(path_glob):
            key = self.get_s3_key(uri, s3_conn)
            if key:
                log.debug('deleting ' + uri)
                key.delete()

            # special case: when deleting a directory, also clean up
            # the _$folder$ files that EMR creates.
            if uri.endswith('/'):
                folder_uri = uri[:-1] + '_$folder$'
                folder_key = self.get_s3_key(folder_uri, s3_conn)
                if folder_key:
                    log.debug('deleting ' + folder_uri)
                    folder_key.delete()

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

    def make_s3_conn(self):
        """Create a connection to S3.

        :return: a :py:class:`boto.s3.connection.S3Connection`, wrapped in a
                 :py:class:`mrjob.retry.RetryWrapper`
        """
        # give a non-cryptic error message if boto isn't installed
        if boto is None:
            raise ImportError('You must install boto to connect to S3')

        log.debug('creating S3 connection (to %s)' % self._s3_endpoint)

        raw_s3_conn = boto.connect_s3(
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            host=self._s3_endpoint)
        return wrap_aws_conn(raw_s3_conn)

    def get_s3_key(self, uri, s3_conn=None):
        """Get the boto Key object matching the given S3 uri, or
        return None if that key doesn't exist.

        uri is an S3 URI: ``s3://foo/bar``

        You may optionally pass in an existing s3 connection through
        ``s3_conn``.
        """
        if not s3_conn:
            s3_conn = self.make_s3_conn()
        bucket_name, key_name = parse_s3_uri(uri)

        try:
            bucket = s3_conn.get_bucket(bucket_name)
        except boto.exception.S3ResponseError, e:
            if e.status != 404:
                raise e
            key = None
        else:
            key = bucket.get_key(key_name)

        return key

    def make_s3_key(self, uri, s3_conn=None):
        """Create the given S3 key, and return the corresponding
        boto Key object.

        uri is an S3 URI: ``s3://foo/bar``

        You may optionally pass in an existing S3 connection through
        ``s3_conn``.
        """
        if not s3_conn:
            s3_conn = self.make_s3_conn()
        bucket_name, key_name = parse_s3_uri(uri)

        return s3_conn.get_bucket(bucket_name).new_key(key_name)

    def get_s3_keys(self, uri, s3_conn=None):
        """Get a stream of boto Key objects for each key inside
        the given dir on S3.

        uri is an S3 URI: ``s3://foo/bar``

        You may optionally pass in an existing S3 connection through s3_conn
        """
        if not s3_conn:
            s3_conn = self.make_s3_conn()

        bucket_name, key_prefix = parse_s3_uri(uri)
        bucket = s3_conn.get_bucket(bucket_name)
        for key in bucket.list(key_prefix):
            yield key

    def get_s3_folder_keys(self, uri, s3_conn=None):
        """.. deprecated:: 0.4.0

        Background: EMR used to fake directories on S3 by creating special
        ``*_$folder$`` keys in S3. That is no longer true, so this method is
        deprecated.

        For example if your job outputs ``s3://walrus/tmp/output/part-00000``,
        EMR will also create these keys:

        - ``s3://walrus/tmp_$folder$``
        - ``s3://walrus/tmp/output_$folder$``

        If you want to grant another Amazon user access to your files so they
        can use them in S3, you must grant read access on the actual keys,
        plus any ``*_$folder$`` keys that "contain" your keys; otherwise
        EMR will error out with a permissions error.

        This gets all the ``*_$folder$`` keys associated with the given URI,
        as boto Key objects.

        This does not support globbing.

        You may optionally pass in an existing S3 connection through
        ``s3_conn``.
        """
        if not s3_conn:
            s3_conn = self.make_s3_conn()

        bucket_name, key_name = parse_s3_uri(uri)
        bucket = s3_conn.get_bucket(bucket_name)

        dirs = key_name.split('/')
        for i in range(len(dirs)):
            folder_name = '/'.join(dirs[:i]) + '_$folder$'
            key = bucket.get_key(folder_name)
            if key:
                yield key
