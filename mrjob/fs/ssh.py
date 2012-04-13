import fnmatch
import logging
import os
import posixpath

try:
    from cStringIO import StringIO
    StringIO  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    from StringIO import StringIO

from mrjob.ssh import ssh_cat
from mrjob.ssh import ssh_copy_key
from mrjob.ssh import ssh_ls
from mrjob.ssh import SSHException
from mrjob.ssh import SSH_PREFIX
from mrjob.ssh import SSH_URI_RE
from mrjob.util import buffer_iterator_to_line_iterator
from mrjob.util import read_file


log = logging.getLogger('mrjob.fs.ssh')


class S3Filesystem(LocalFilesystem):

    def __init__(self, opts, aws_region, ssh_key_name):
        super(S3Filesystem, self).__init__()
        self._opts = opts
        self._aws_region = aws_region
        self._ssh_key_name = ssh_key_name
        self._ssh_key_copied = False

        # this will be populated by EMRJobRunner when possible
        self.address_of_master = None

    def _enable_slave_ssh_access(self):
        if not self._ssh_key_copied:
            self._ssh_key_copied = True
            ssh_copy_key(
                self._opts['ssh_bin'],
                self.address_of_master,
                self._opts['ec2_key_pair_file'],
                self._ssh_key_name)

    def du(self, path_glob):
        """Get the size of all files matching path_glob."""
        if not is_s3_uri(path_glob):
            return super(S3Filesystem, self).du(path_glob)

        return sum(self.get_s3_key(uri).size for uri in self.ls(path_glob))

    def ls(self, path_glob):
        """Recursively list files locally or on S3.

        This doesn't list "directories" unless there's actually a
        corresponding key ending with a '/' (which is weird and confusing;
        don't make S3 keys ending in '/')

        To list a directory, path_glob must end with a trailing
        slash (foo and foo/ are different on S3)
        """
        if SSH_URI_RE.match(path_glob):
            for item in self._ssh_ls(path_glob):
                yield item
            return

        if not is_s3_uri(path_glob):
            for path in super(S3Filesystem, self).ls(path_glob):
                yield path
            return

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
            # enforce globbing
            if glob_match and not fnmatch.fnmatchcase(uri, path_glob):
                continue

            yield uri

    def _ssh_ls(self, uri):
        """Helper for ls(); obeys globbing"""
        m = SSH_URI_RE.match(uri)
        try:
            addr = m.group('hostname') or self.address_of_master
            if addr is None:
                raise SSHException('Address of master is not set')
            if '!' in addr:
                self._enable_slave_ssh_access()
            output = ssh_ls(
                self._opts['ssh_bin'],
                addr,
                self._opts['ec2_key_pair_file'],
                m.group('filesystem_path'),
                self._ssh_key_name,
            )
            for line in output:
                # skip directories, we only want to return downloadable files
                if line and not line.endswith('/'):
                    yield SSH_PREFIX + addr + line
        except SSHException, e:
            raise IOError(e)

    def _s3_ls(self, uri):
        """Helper for ls(); doesn't bother with globbing or directories"""
        s3_conn = self.make_s3_conn()
        bucket_name, key_name = parse_s3_uri(uri)

        bucket = s3_conn.get_bucket(bucket_name)
        for key in bucket.list(key_name):
            yield s3_key_to_uri(key)

    def md5sum(self, path, s3_conn=None):
        if is_s3_uri(path):
            k = self.get_s3_key(path, s3_conn=s3_conn)
            return k.etag.strip('"')
        else:
            return super(S3Filesystem, self).md5sum(path)

    def _cat_file(self, filename):
        ssh_match = SSH_URI_RE.match(filename)
        if is_s3_uri(filename):
            # stream lines from the s3 key
            s3_key = self.get_s3_key(filename)
            buffer_iterator = read_file(s3_key_to_uri(s3_key), fileobj=s3_key)
            return buffer_iterator_to_line_iterator(buffer_iterator)
        elif ssh_match:
            try:
                addr = ssh_match.group('hostname') or self._address_of_master()
                if '!' in addr:
                    self._enable_slave_ssh_access()
                output = ssh_cat(
                    self._opts['ssh_bin'],
                    addr,
                    self._opts['ec2_key_pair_file'],
                    ssh_match.group('filesystem_path'),
                    self._ssh_key_name,
                )
                return read_file(filename, fileobj=StringIO(output))
            except SSHException, e:
                raise IOError(e)
        else:
            # read from local filesystem
            return super(S3Filesystem, self)._cat_file(filename)

    def mkdir(self, dest):
        """Make a directory. This does nothing on S3 because there are
        no directories.
        """
        if not is_s3_uri(dest):
            super(S3Filesystem, self).mkdir(dest)

    def path_exists(self, path_glob):
        """Does the given path exist?

        If dest is a directory (ends with a "/"), we check if there are
        any files starting with that path.
        """
        if not is_s3_uri(path_glob):
            return super(S3Filesystem, self).path_exists(path_glob)

        # just fall back on ls(); it's smart
        return any(self.ls(path_glob))

    def path_join(self, dirname, filename):
        if is_s3_uri(dirname):
            return posixpath.join(dirname, filename)
        else:
            return os.path.join(dirname, filename)

    def rm(self, path_glob):
        """Remove all files matching the given glob."""
        if not is_s3_uri(path_glob):
            return super(S3Filesystem, self).rm(path_glob)

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
        if not is_s3_uri(dest):
            super(S3Filesystem, self).touchz(dest)

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

        s3_endpoint = self._get_s3_endpoint()
        log.debug('creating S3 connection (to %s)' % s3_endpoint)

        raw_s3_conn = boto.connect_s3(
            aws_access_key_id=self._opts['aws_access_key_id'],
            aws_secret_access_key=self._opts['aws_secret_access_key'],
            host=s3_endpoint)
        return wrap_aws_conn(raw_s3_conn)

    def _get_s3_endpoint(self):
        if self._opts['s3_endpoint']:
            return self._opts['s3_endpoint']
        else:
            # look it up in our table
            try:
                return REGION_TO_S3_ENDPOINT[self._aws_region]
            except KeyError:
                raise Exception(
                    "Don't know the S3 endpoint for %s;"
                    " try setting s3_endpoint explicitly" % self._aws_region)

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

        return s3_conn.get_bucket(bucket_name).get_key(key_name)

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
        """Background: S3 is even less of a filesystem than HDFS in that it
        doesn't have directories. EMR fakes directories by creating special
        ``*_$folder$`` keys in S3.

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
