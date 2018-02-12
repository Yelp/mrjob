# -*- coding: utf-8 -*-
# Copyright 2016 Google Inc.
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
import fnmatch
import logging
from base64 import b64decode
from binascii import hexlify
from tempfile import TemporaryFile

from mrjob.cat import decompress
from mrjob.fs.base import Filesystem
from mrjob.parse import urlparse
from mrjob.py2 import PY2
from mrjob.runner import GLOB_RE

# TODO: loading credentials
try:
    from google.api_core.exceptions import NotFound
    from google.cloud.storage.client import Client as StorageClient
except ImportError:
    NotFound = None
    StorageClient = None


log = logging.getLogger(__name__)


def _path_glob_to_parsed_gcs_uri(path_glob):
    # support globs
    glob_match = GLOB_RE.match(path_glob)

    # we're going to search for all keys starting with base_uri
    if glob_match:
        # cut it off at first wildcard
        base_uri = glob_match.group(1)
    else:
        base_uri = path_glob

    bucket_name, base_name = parse_gcs_uri(base_uri)
    return bucket_name, base_name


class GCSFilesystem(Filesystem):
    """Filesystem for Google Cloud Storage (GCS) URIs. Typically you will get
    one of these via
    ``DataprocJobRunner().fs``, composed with
    :py:class:`~mrjob.fs.ssh.SSHFilesystem` and
    :py:class:`~mrjob.fs.local.LocalFilesystem`.
    """
    def __init__(self, local_tmp_dir=None):
        self._client = None
        self._local_tmp_dir = local_tmp_dir

    @property
    def client(self):
        # TODO: push credentials from Dataproc Runner
        if not self._client:
            self._client = StorageClient()

        return self._client

    @property
    def api_client(self):
        raise NotImplementedError(
            '"api_client" was disabled in v0.6.2. use "client" instead')

    def can_handle_path(self, path):
        return is_gcs_uri(path)

    def du(self, path_glob):
        """Get the size of all files matching path_glob."""
        return sum(blob.size for uri, blob in self._ls(path_glob))

    def ls(self, path_glob):
        for uri, blob in self._ls(path_glob):
            yield uri

    def _ls(self, path_glob):
        """Helper method for :py:meth:`ls`; yields tuples of
        ``(uri, blob)`` where *blob* is the corresponding
        :py:class:`google.cloud.storage.blob.Blob`.
        """
        # support globs
        glob_match = GLOB_RE.match(path_glob)

        # we're going to search for all keys starting with base_uri
        if glob_match:
            # cut it off at first wildcard
            base_uri = glob_match.group(1)
        else:
            base_uri = path_glob

        bucket_name, base_name = parse_gcs_uri(base_uri)

        # allow subdirectories of the path/glob
        if path_glob and not path_glob.endswith('/'):
            dir_glob = path_glob + '/*'
        else:
            dir_glob = path_glob + '*'

        try:
            bucket = self.get_bucket(bucket_name)
        except NotFound:
            return  # treat nonexistent buckets as empty

        for blob in bucket.list_blobs(prefix=base_name):
            uri = "gs://%s/%s" % (bucket_name, blob.name)

            # don't return directory "blobs"
            if uri.endswith('/'):
                continue

            # enforce globbing
            if not (fnmatch.fnmatchcase(uri, path_glob) or
                    fnmatch.fnmatchcase(uri, dir_glob)):
                continue

            yield uri, blob

    def md5sum(self, path):
        blob = self._get_blob(path)
        if not blob:
            raise IOError('Object %r does not exist' % (path,))
        return hexlify(b64decode(blob.md5_hash))

    def _cat_file(self, gcs_uri):
        blob = self._blob(gcs_uri)
        if not blob:
            return  # don't cat nonexistent files

        with TemporaryFile(dir=self._local_tmp_dir) as temp:
            blob.download_to_file(temp)

            # now read from that file
            temp.seek(0)

            for chunk in decompress(temp, gcs_uri):
                yield chunk

    def mkdir(self, dest):
        """Make a directory. This does nothing on GCS because there are
        no directories.
        """
        pass

    def exists(self, path_glob):
        """Does the given path exist?

        If dest is a directory (ends with a "/"), we check if there are
        any files starting with that path.
        """
        # TODO - mtai @ davidmarin - catch specific Exceptions, not sure what
        # types of exceptions this can throw
        try:
            paths = self.ls(path_glob)
        except:
            paths = []
        return any(paths)

    # TODO: use google-cloud-sdk
    def rm(self, path_glob):
        """Remove all files matching the given glob."""
        for uri, blob in self._ls(path_glob):
            blob.delete()

    def touchz(self, dest_uri):
        # check if already exists
        old_blob = self._get_blob(dest_uri)
        if old_blob:
            raise IOError('Non-empty file %r already exists!' % (dest_uri,))

        self._blob(dest_uri).upload_from_string(b'')

    def put(self, src_path, dest_uri):
        """Uploads a local file to a specific destination."""
        old_blob = self._get_blob(dest_uri)
        if old_blob:
            raise IOError('File already exists: %s' % dest_uri)

        self._blob(dest_uri).upload_from_filename(src_path)

    def get_all_bucket_names(self, prefix=None):
        """Yield the names of all buckets associated with this client.

        :param prefix: optional prefix to search under (e.g. ``'mrjob-'``)

        .. versionadded:: 0.6.2
        """
        for b in self.client.list_buckets(prefix=prefix):
            yield b.name

    # NOTE: disabled this
    def list_buckets(self, project, prefix=None):
        """List buckets on GCS."""
        raise NotImplementedError(
            'list_buckets() was disabled in v0.6.2. Use'
            'get_all_bucket_names() and get_bucket()')

    # NOTE: returns a google-cloud-sdk bucket
    def get_bucket(self, bucket_name):
        """Return a :py:class:`google.cloud.storage.bucket.Bucket`
        Raises an exception if the bucket does not exist."""
        return self.client.get_bucket(bucket_name)

    # NOTE: function signature has changed
    def create_bucket(self, name,
                      location=None, object_ttl_days=None):
        """Create a bucket on GCS, optionally setting location constraint.
        and time-to-live."""
        bucket = self.client.bucket(name)

        # as of google-cloud 0.32.0, there isn't a direct way to set location
        if location:
            bucket._changes.add('location')
            bucket._properties['location'] = location

        bucket.create()

        bucket.lifecycle_rules = [
            dict(
                action=dict(type='Delete'),
                condition=dict(age=object_ttl_days)
            )
        ]

    # NOTE: disabled this
    def delete_bucket(self, bucket):
        raise NotImplementedError(
            'delete_bucket() was disabled in v0.6.2. Use'
            'fs.bucket(name).delete()')

    def _get_blob(self, uri):
        bucket_name, blob_name = parse_gcs_uri(uri)
        bucket = self.client.get_bucket(bucket_name)
        return bucket.get_blob(blob_name)

    def _blob(self, uri):
        bucket_name, blob_name = parse_gcs_uri(uri)
        bucket = self.client.get_bucket(bucket_name)
        return bucket.blob(blob_name)


# The equivalent S3 methods are in parse.py but it's cleaner to keep them
# in the filesystem module; let's do that going forward

def is_gcs_uri(uri):
    """Return True if *uri* can be parsed into an S3 URI, False otherwise.
    """
    try:
        parse_gcs_uri(uri)
        return True
    except ValueError:
        return False


def parse_gcs_uri(uri):
    """Parse a GCS URI into (bucket, key)

    >>> parse_gcs_uri("gs://walrus/tmp/")
    ('walrus', 'tmp/')

    If ``uri`` is not a GCS URI, raise a ValueError
    """
    components = urlparse(uri)
    if components.scheme != "gs" or '/' not in components.path:
        raise ValueError('Invalid GCS URI: %s' % uri)

    return components.netloc, components.path[1:]
