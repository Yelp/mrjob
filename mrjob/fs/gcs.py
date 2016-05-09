# -*- coding: utf-8 -*-
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
import fnmatch
import logging
import mimetypes

from mrjob.fs.base import Filesystem
from mrjob.parse import urlparse
from mrjob.runner import GLOB_RE
from mrjob.util import read_file

try:
    from oauth2client.client import GoogleCredentials
    from googleapiclient import discovery
    from googleapiclient import errors as google_errors
    from googleapiclient import http as google_http
except ImportError:
    # don't require googleapiclient; MRJobs don't actually need it when running
    # inside hadoop streaming
    GoogleCredentials = None
    discovery = None
    google_errors = None
    google_http = None

import io
import os
import tempfile
import base64
import binascii

log = logging.getLogger(__name__)

_GCS_API_ENDPOINT = 'storage'
_GCS_API_VERSION = 'v1'

_BINARY_MIMETYPE = 'application/octet-stream'
_LS_FIELDS_TO_RETURN = 'nextPageToken,items(name,size,timeCreated,md5Hash)'


def _base64_to_hex(base64_encoded):
    base64_decoded = base64.decodestring(base64_encoded)
    return binascii.hexlify(base64_decoded)


def _hex_to_base64(hex_encoded):
    hex_decoded = binascii.unhexlify(hex_encoded)
    return base64.encodestring(hex_decoded)


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
    def __init__(self):
        self._api_client = None

    @property
    def api_client(self):
        if not self._api_client:
            credentials = GoogleCredentials.get_application_default()
            self._api_client = discovery.build(
                _GCS_API_ENDPOINT, _GCS_API_VERSION, credentials=credentials)

        return self._api_client

    def can_handle_path(self, path):
        return is_gcs_uri(path)

    def du(self, path_glob):
        """Get the size of all files matching path_glob."""
        return sum(item['size'] for item in self._ls_detailed(path_glob))

    def ls(self, path_glob):
        for item in self._ls_detailed(path_glob):
            yield item['_uri']

    def _ls_detailed(self, path_glob):
        """Recursively list files on GCS and includes some metadata about them:
        - object name
        - size
        - md5 hash
        - _uri

        *path_glob* can include ``?`` to match single characters or
        ``*`` to match 0 or more characters. Both ``?`` and ``*`` can match
        ``/``.
        """

        scheme = urlparse(path_glob).scheme

        bucket_name, base_name = _path_glob_to_parsed_gcs_uri(path_glob)

        # allow subdirectories of the path/glob
        if path_glob and not path_glob.endswith('/'):
            dir_glob = path_glob + '/*'
        else:
            dir_glob = path_glob + '*'

        list_request = self.api_client.objects().list(
            bucket=bucket_name, prefix=base_name, fields=_LS_FIELDS_TO_RETURN)

        uri_prefix = '%s://%s' % (scheme, bucket_name)
        while list_request:
            try:
                resp = list_request.execute()
            except google_errors.HttpError as e:
                if e.resp.status == 404:
                    return

                raise

            resp_items = resp.get('items') or []
            for item in resp_items:
                # We generate the item URI by adding the "gs://" prefix
                uri = "%s/%s" % (uri_prefix, item['name'])

                # enforce globbing
                if not (fnmatch.fnmatchcase(uri, path_glob) or
                        fnmatch.fnmatchcase(uri, dir_glob)):
                    continue

                # filter out folders
                if uri.endswith('/'):
                    continue

                item['_uri'] = uri
                item['bucket'] = bucket_name
                item['size'] = int(item['size'])
                yield item

            list_request = self.api_client.objects().list_next(
                list_request, resp)

    def md5sum(self, path):
        object_list = list(self._ls_detailed(path))
        if len(object_list) != 1:
            raise Exception(
                "path for md5 sum doesn't resolve to single object" + path)

        item = object_list[0]
        return _base64_to_hex(item['md5Hash'])

    def _cat_file(self, gcs_uri):
        tmp_fd, tmp_path = tempfile.mkstemp()

        with os.fdopen(tmp_fd, 'w+b') as tmp_fileobj:
            self._download_io(gcs_uri, tmp_fileobj)

            tmp_fileobj.seek(0)

            line_gen = read_file(
                gcs_uri, fileobj=tmp_fileobj, yields_lines=False)
            for current_line in line_gen:
                yield current_line

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

    def rm(self, path_glob):
        """Remove all files matching the given glob."""
        bucket_name, base_name = _path_glob_to_parsed_gcs_uri(path_glob)

        for item in self._ls_detailed(path_glob):
            req = self.api_client.objects().delete(
                bucket=bucket_name, object=item['name'])
            log.debug("deleting " + item['_uri'])
            req.execute()

    def touchz(self, dest_uri):
        with io.BytesIO() as io_obj:
            return self._upload_io(io_obj, dest_uri)

    def put(self, src_path, dest_uri):
        """Uploads a local file to a specific destination."""
        with io.FileIO(src_path) as io_obj:
            return self._upload_io(io_obj, dest_uri)

    def _download_io(self, src_uri, io_obj):
        bucket_name, object_name = parse_gcs_uri(src_uri)

        # Chunked file download
        req = self.api_client.objects().get_media(
            bucket=bucket_name, object=object_name)
        downloader = google_http.MediaIoBaseDownload(io_obj, req)

        done = False
        while not done:
            try:
                status, done = downloader.next_chunk()
            except google_errors.HttpError as e:
                # Error code 416 (request range not satisfiable)
                # implies we're trying to download a file of size 0
                if e.resp.status == 416:
                    break

                raise

            if status:
                log.debug("Download %d%%." % int(status.progress() * 100))

        log.debug("Download Complete for %s", src_uri)
        return io_obj

    def _upload_io(self, io_obj, dest_uri, metadata=False):
        bucket, name = parse_gcs_uri(dest_uri)
        if self.exists(dest_uri):
            raise Exception("File already exists: " + dest_uri)

        mimetype, _ = mimetypes.guess_type(dest_uri)
        mimetype = mimetype or _BINARY_MIMETYPE

        # Chunked file upload
        media = google_http.MediaIoBaseUpload(io_obj, mimetype, resumable=True)
        upload_req = self.api_client.objects().insert(
            bucket=bucket, name=name, media_body=media)

        upload_resp = None
        while upload_resp is None:
            status, upload_resp = upload_req.next_chunk()
            if status:
                log.debug("Uploaded %d%%." % int(status.progress() * 100))

        log.debug('Upload Complete! %s', dest_uri)

        if metadata:
            return self.api_client.objects().get(
                bucket=bucket, object=name).execute()

    def list_buckets(self, project, prefix=None):
        """List buckets on GCS."""
        list_kwargs = dict(project=project)
        if prefix:
            list_kwargs['prefix'] = prefix

        req = self.api_client.buckets().list(**list_kwargs)
        resp = req.execute()

        buckets_to_return = resp.get('items') or []
        return buckets_to_return

    def get_bucket(self, bucket):
        req = self.api_client.buckets().get(bucket=bucket)
        return req.execute()

    def create_bucket(self, project, name,
                      location=None, object_ttl_days=None):
        """Create a bucket on GCS, optionally setting location constraint."""
        # https://cloud.google.com/storage/docs/lifecycle
        body = dict(name=name)

        if location:
            body['location'] = location

        # Lifecycle management
        if object_ttl_days is not None:
            lifecycle_rule = dict(
                action=dict(type='Delete'),
                condition=dict(age=object_ttl_days)
            )
            body['lifecycle'] = dict(rule=[lifecycle_rule])

        req = self.api_client.buckets().insert(project=project, body=body)
        return req.execute()

    def delete_bucket(self, bucket):
        req = self.api_client.buckets().delete(bucket=bucket)
        return req.execute()


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
