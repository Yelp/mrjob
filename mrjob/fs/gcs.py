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

# from mrjob.aws import s3_endpoint_for_region
from mrjob.fs.base import Filesystem
from mrjob.parse import urlparse
from mrjob.runner import GLOB_RE
from mrjob.util import read_file

from apiclient import discovery
from oauth2client.client import GoogleCredentials
from apiclient import http
import io
import os
import tempfile
import base64
import binascii

log = logging.getLogger(__name__)

_BINARY_MIMETYPE = 'application/octet-stream'
_LS_FIELDS_TO_RETURN = 'nextPageToken,items(id,size,name,md5Hash)'


def _base64_to_hex(base64_encoded):
    base64_decoded = base64.decodestring(base64_encoded)
    return binascii.hexlify(base64_decoded)


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
    """Filesystem for Google Cloud Storage (GCS) URIs. Typically you will get one of these via
    ``DataprocJobRunner().fs``, composed with
    :py:class:`~mrjob.fs.ssh.SSHFilesystem` and
    :py:class:`~mrjob.fs.local.LocalFilesystem`.
    """

    def __init__(self):
        credentials = GoogleCredentials.get_application_default()
        self._service = discovery.build('storage', 'v1', credentials=credentials)

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
        - uri
        - size
        - bucket name
        - object name
        - md5 hash
        - etag

        *path_glob* can include ``?`` to match single characters or
        ``*`` to match 0 or more characters. Both ``?`` and ``*`` can match
        ``/``.
        """

        scheme = urlparse(path_glob).scheme

        bucket_name, _ = _path_glob_to_parsed_gcs_uri(path_glob)

        # allow subdirectories of the path/glob
        if path_glob and not path_glob.endswith('/'):
            dir_glob = path_glob + '/*'
        else:
            dir_glob = path_glob + '*'

        list_request = self._service.objects().list(bucket=bucket_name, fields=_LS_FIELDS_TO_RETURN)

        while list_request:
            resp = list_request.execute()
            for item in resp['items']:
                # We generate the item URI by adding the "gs://" prefix and snipping the "/generation"
                # number which is at the end of the id that we retrieved.
                file_path, _, generation = item['id'].rpartition('/')
                uri = "%s://%s" % (scheme, file_path)

                # enforce globbing
                if not (fnmatch.fnmatchcase(uri, path_glob) or fnmatch.fnmatchcase(uri, dir_glob)):
                    continue

                item['_uri'] = uri
                item['size'] = int(item['size'])
                yield item

            list_request = self._service.objects().list_next(list_request, resp)

    def md5sum(self, path):
        object_list = list(self._ls_detailed(path))
        if len(object_list) != 1:
            raise Exception("path for md5 sum doesn't resolve to single object" + path)

        item = object_list[0]
        return _base64_to_hex(item['md5Hash'])

    def _cat_file(self, gcs_uri):
        tmp_fd, tmp_path = tempfile.mkstemp()
        tmp_fileobj = os.fdopen(tmp_fd, 'w+b')

        self._download_io(tmp_fileobj, gcs_uri)

        tmp_fileobj.seek(0)

        return read_file(gcs_uri, fileobj=tmp_fileobj, yields_lines=False)

    def mkdir(self, dest):
        """Make a directory. This does nothing on GCS because there are
        no directories.
        """
        raise NotImplementedError

    def exists(self, path_glob):
        """Does the given path exist?

        If dest is a directory (ends with a "/"), we check if there are
        any files starting with that path.
        """
        # just fall back on ls(); it's smart
        try:
            paths = self.ls(path_glob)
        except:
            paths = []
        return any(paths)

    def rm(self, path_glob):
        """Remove all files matching the given glob."""
        bucket_name, base_name = _path_glob_to_parsed_gcs_uri(path_glob)

        for item in self._ls_detailed(path_glob):
            req = self._service.objects().delete(bucket=bucket_name, object=item['name'])
            log.debug("deleting " + item['_uri'])
            req.execute()

    def touchz(self, dest_uri):
        io_obj = io.BytesIO()
        return self._upload_io(io_obj, dest_uri)

    def upload(self, src_path, dest_uri):
        """Uploads a local file to a specific destination."""
        io_obj = io.FileIO(src_path)
        return self._upload_io(io_obj, dest_uri)

    def _download_io(self, io_obj, src_uri):
        bucket_name, object_name = parse_gcs_uri(src_uri)

        # Chunked file download
        req = self._service.objects().get_media(bucket=bucket_name, object=object_name)
        downloader = http.MediaIoBaseDownload(io_obj, req)

        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                log.debug("Download %d%%." % int(status.progress() * 100))

        log.debug("Download Complete for %s", src_uri)
        return io_obj

    def _upload_io(self, io_obj, dest_uri):
        bucket, name = parse_gcs_uri(dest_uri)

        if self.exists(dest_uri):
            raise Exception("File already exists: " + dest_uri)

        # Chunked file upload
        media = http.MediaIoBaseUpload(io_obj, _BINARY_MIMETYPE, resumable=True)
        upload_req = self._service.objects().insert(bucket=bucket, name=name, media_body=media)

        upload_resp = None
        while upload_resp is None:
          status, upload_resp = upload_req.next_chunk()
          if status:
            log.debug("Uploaded %d%%." % int(status.progress() * 100))

        log.debug('Upload Complete! %s', dest_uri)

    def buckets_list(self, project, prefix=None):
        """List buckets on GCS."""
        list_kwargs = dict(project=project)
        if prefix:
            list_kwargsp['prefix'] = prefix

        req = self._service.buckets().list(**list_kwargs)
        resp = req.execute()

        return resp['items']

    def bucket_get(self, project, bucket):
        req = self._service.buckets().get(project=project, name=bucket)
        return req.execute()

    def bucket_create(self, project, bucket, location='', object_ttl_days=None):
        """Create a bucket on S3, optionally setting location constraint."""
        # https://cloud.google.com/storage/docs/lifecycle
        insert_kwargs = dict(project=project, name=bucket)

        if location:
            insert_kwargs['location'] = location

        if object_ttl_days is not None:
            lifecycle_rule = dict(
                action=dict(type='Delete'),
                condition=dict(age=object_ttl_days)
            )
            insert_kwargs['lifecycle'] = dict(rule=[lifecycle_rule])

        req = self._service.buckets().insert(**insert_kwargs)
        return req.execute()

def is_gcs_uri(uri):
    """Return True if *uri* can be parsed into an S3 URI, False otherwise.
    The equivalent s3 method is in parse.py but it seemed cleaner to keep the GCS one in gcs.py
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
    The equivalent S3 method is in parse.py but it seemed cleaner to keep the GCS one in gcs.py
    """
    components = urlparse(uri)
    if (components.scheme not in ("gs") or
        '/' not in components.path):  # noqa

        raise ValueError('Invalid GCS URI: %s' % uri)

    return components.netloc, components.path[1:]

if __name__ == "__main__":
    try:
        from oauth2client.client import GoogleCredentials
        from googleapiclient import discovery
    except ImportError:
        # don't require boto; MRJobs don't actually need it when running
        #  inside hadoop streaming
        GoogleCredentials = None
        discovery = None

    fs = GCSFilesystem()
    test_path = "gs://boulder-input-data/mrjob-boulder-kir/part-00000"
    ls_gen = list(fs._ls_detailed(test_path))
    import pprint
    pprint.pprint(ls_gen)

    for line in fs.cat(test_path):
        print line.rstrip('\n')
    # fs.upload("/Users/vbp/code/boulder/CHANGES.txt", "gs://boulder-input-data/vbptest/changes")
