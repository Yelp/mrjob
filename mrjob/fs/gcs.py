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


from apiclient import discovery
from oauth2client.client import GoogleCredentials
from apiclient.http import MediaIoBaseDownload
from apiclient import http
import io
import json
import base64
import binascii

log = logging.getLogger(__name__)


class GCSFilesystem(Filesystem):
    """Filesystem for Google Cloud Storage (GCS) URIs. Typically you will get one of these via
    ``DataprocJobRunner().fs``, composed with
    :py:class:`~mrjob.fs.ssh.SSHFilesystem` and
    :py:class:`~mrjob.fs.local.LocalFilesystem`.
    """

    def __init__(self):
        credentials = GoogleCredentials.get_application_default()
        self.service = discovery.build('storage', 'v1', credentials=credentials)

    def can_handle_path(self, path):
        return is_gcs_uri(path)

    def du(self, path_glob):
        """Get the size of all files matching path_glob."""
        return sum(item[1] for item in self.lsWithInfo(path_glob))

    def ls(self, path_glob):
        for item in self.lsWithInfo(path_glob):
            yield item[0]

    def lsWithInfo(self, path_glob):
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


        fields_to_return = "nextPageToken,items(id,size,bucket,name,md5Hash,etag)"
        req = self.service.objects().list(bucket=bucket_name, fields=fields_to_return)
        while req:
            resp = req.execute()
            resp_json = json.loads(json.dumps(resp))
            for item in resp_json["items"]:
                # We generate the item URI by adding the "gs://" prefix and snipping the "/generation"
                # number which is at the end of the id that we retrieved.
                uri = "%s://%s" % (scheme, item["id"].rpartition("/")[0])
                # enforce globbing
                if not (fnmatch.fnmatchcase(uri, path_glob) or fnmatch.fnmatchcase(uri, dir_glob)):
                    continue
                yield uri, long(item["size"]), item["bucket"], item["name"], item["md5Hash"], item["etag"]
            req = self.service.objects().list_next(req, resp)

    def md5sum(self, path):
        object_list = list(self.lsWithInfo(path))
        print object_list
        if len(object_list) == 1:
            return binascii.hexlify(base64.decodestring(object_list[0][4]))
        else:
            raise Exception("path for md5 sum doesn't resolve to single object" + path)

    def _cat_file(self, filename):
        bucket_name, object_name = parse_gcs_uri(filename)
        req = self.service.objects().get_media(bucket=bucket_name, object=object_name)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, req, chunksize=1024*1024)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                log.info("Download %d%%." % int(status.progress() * 100))
            log.info("Download Complete for " + filename)
        """ TODO: So, at this point we have the file content as you can see it using fh.getvalue().
        But rather than re-implementing the decompression logic (to deal with compressed files) I'd rather
        use the same read_file() method (in util.py) that the S3 and local versions are using.
        But I can't figure out how to build the right file object to pass it to read_file().
        Help appreciated.
        """

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
        for item in self.lsWithInfo(path_glob):
            req = self.service.objects().delete(bucket=item[2], object=item[3])
            log.debug("deleting " + item[0])
            req.execute()

    def touchz(self, dest):
        """ TODO: too much common code between touchz and updload. Refactor.
        Make an empty file in the given location. Raises an error if
        a file already exists in that location."""
        if self.exists(dest):
            raise Exception("File already exists: " + dest)
        media = http.MediaIoBaseUpload(io.BytesIO(""), "text/plain")
        req = self.service.objects().insert( bucket=parse_gcs_uri(dest)[0], name=parse_gcs_uri(dest)[1], media_body=media)
        req.execute()

    def upload(self, local_path, destination_uri):
        """Uploads a local file to a specific destination."""
        if self.exists(destination_uri):
            raise Exception("File already exists: " + destination_uri)
        media = http.MediaIoBaseUpload(io.FileIO(local_path), "text/plain")
        req = self.service.objects().insert( bucket=parse_gcs_uri(destination_uri)[0], name=parse_gcs_uri(destination_uri)[1], media_body=media)
        req.execute()

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
    testPath = "gs://boulder-input-data/big.txt"
    ls_gen = fs.ls(testPath)
    print list(ls_gen)
    print fs.md5sum(testPath)
    #fs.upload("/Users/vbp/code/boulder/CHANGES.txt", "gs://boulder-input-data/vbptest/changes")


