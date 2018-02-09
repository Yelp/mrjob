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
import base64
import binascii
import fnmatch
import io
import logging
from tempfile import TemporaryFile

from mrjob.cat import decompress
from mrjob.fs.base import Filesystem
from mrjob.parse import urlparse
from mrjob.py2 import PY2
from mrjob.runner import GLOB_RE

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

# TODO: loading credentials
try:
    from google.cloud.storage.client import Client as StorageClient
except ImportError:
    StorageClient = None


log = logging.getLogger(__name__)

_GCS_API_ENDPOINT = 'storage'
_GCS_API_VERSION = 'v1'

_BINARY_MIMETYPE = 'application/octet-stream'
_LS_FIELDS_TO_RETURN = 'nextPageToken,items(name,size,timeCreated,md5Hash)'

if PY2:
    base64_decode = base64.decodestring
    base64_encode = base64.encodestring
else:
    base64_decode = base64.decodebytes
    base64_encode = base64.encodebytes


def _base64_to_hex(base64_encoded):
    base64_decoded = base64_decode(base64_encoded)
    return binascii.hexlify(base64_decoded)


def _hex_to_base64(hex_encoded):
    hex_decoded = binascii.unhexlify(hex_encoded)
    return base64_encode(hex_decoded)


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
        self._api_client = None  # TODO: remove this when done porting
        self._client = None
        self._local_tmp_dir = local_tmp_dir

    @property
    def client(self):
        if not self._client:
            self._client = StorageClient()

        return self._client

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

    # TODO: need to use google cloud sdk
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

    # TODO: use google-cloud-sdk
    def md5sum(self, path):
        object_list = list(self._ls_detailed(path))
        if len(object_list) != 1:
            raise Exception(
                "path for md5 sum doesn't resolve to single object" + path)

        item = object_list[0]
        return _base64_to_hex(item['md5Hash'])

    def _cat_file(self, gcs_uri):
        bucket_name, blob_name = parse_gcs_uri(gcs_uri)
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)

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
        bucket_name, base_name = _path_glob_to_parsed_gcs_uri(path_glob)

        for item in self._ls_detailed(path_glob):
            req = self.api_client.objects().delete(
                bucket=bucket_name, object=item['name'])
            log.debug("deleting " + item['_uri'])
            req.execute()

    # TODO: use google-cloud-sdk
    # TODO: raise an error if file already exists!
    def touchz(self, dest_uri):
        with io.BytesIO() as io_obj:
            return self._upload_io(io_obj, dest_uri)

    def put(self, src_path, dest_uri):
        """Uploads a local file to a specific destination."""
        bucket_name, blob_name = parse_gcs_uri(dest_uri)
        if self.exists(dest_uri):
            raise IOError('File already exists: %s' % dest_uri)

        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)

        blob.upload_from_filename(src_path)

    # TODO: this returns a JSON; need to make a backwards-compatible version
    #
    # should implement get_all_bucket_names()
    def list_buckets(self, project, prefix=None):
        """List buckets on GCS."""
        list_kwargs = dict(project=project)
        if prefix:
            list_kwargs['prefix'] = prefix

        req = self.api_client.buckets().list(**list_kwargs)
        resp = req.execute()

        buckets_to_return = resp.get('items') or []
        return buckets_to_return

    def get_bucket(self, bucket_name):
        """Return a :py:class:`google.cloud.storage.bucket.Bucket`
        Raises an exception if the bucket does not exist."""
        return self.client.get_bucket(bucket_name)

    # TODO: implement with google-cloud-sdk
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

    # TODO: implement with google-cloud-sdk, deprecate
    # Why is this even here?
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
