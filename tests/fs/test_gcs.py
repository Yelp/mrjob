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
import bz2
import io
import sys
from unittest import skipIf

from tests.py2 import patch
from tests.py2 import mock

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

from mrjob.fs.gcs import GCSFilesystem

from tests.compress import gzip_compress
from tests.mockgoogleapiclient import MockGoogleAPITestCase
from tests.sandbox import PatcherTestCase


class GCSFSTestCase(MockGoogleAPITestCase):

    def setUp(self):
        super(GCSFSTestCase, self).setUp()
        self.fs = GCSFilesystem()

    def test_cat_uncompressed(self):
        self.put_gcs_multi({
            'gs://walrus/data/foo': b'foo\nfoo\n'
        })

        self.assertEqual(list(self.fs._cat_file('gs://walrus/data/foo')),
                         [b'foo\n', b'foo\n'])

    def test_cat_bz2(self):
        self.put_gcs_multi({
            'gs://walrus/data/foo.bz2': bz2.compress(b'foo\n' * 1000)
        })

        self.assertEqual(list(self.fs._cat_file('gs://walrus/data/foo.bz2')),
                         [b'foo\n'] * 1000)

    def test_cat_gz(self):
        self.put_gcs_multi({
            'gs://walrus/data/foo.gz': gzip_compress(b'foo\n' * 10000)
        })

        self.assertEqual(list(self.fs._cat_file('gs://walrus/data/foo.gz')),
                         [b'foo\n'] * 10000)

    def test_ls_key(self):
        self.put_gcs_multi({
            'gs://walrus/data/foo': b''
        })

        self.assertEqual(list(self.fs.ls('gs://walrus/data/foo')),
                         ['gs://walrus/data/foo'])

    def test_ls_recursively(self):
        self.put_gcs_multi({
            'gs://walrus/data/bar': b'',
            'gs://walrus/data/bar/baz': b'',
            'gs://walrus/data/foo': b'',
            'gs://walrus/qux': b'',
        })

        uris = [
            'gs://walrus/data/bar',
            'gs://walrus/data/bar/baz',
            'gs://walrus/data/foo',
            'gs://walrus/qux',
        ]

        self.assertEqual(set(self.fs.ls('gs://walrus/')), set(uris))
        self.assertEqual(set(self.fs.ls('gs://walrus/*')), set(uris))

        self.assertEqual(set(self.fs.ls('gs://walrus/data')), set(uris[:-1]))
        self.assertEqual(set(self.fs.ls('gs://walrus/data/')), set(uris[:-1]))
        self.assertEqual(set(self.fs.ls('gs://walrus/data/*')), set(uris[:-1]))

    def test_ls_globs(self):
        self.put_gcs_multi({
            'gs://w/a': b'',
            'gs://w/a/b': b'',
            'gs://w/ab': b'',
            'gs://w/b': b'',
        })

        self.assertEqual(set(self.fs.ls('gs://w/')),
                         set(['gs://w/a', 'gs://w/a/b',
                              'gs://w/ab', 'gs://w/b']))
        self.assertEqual(set(self.fs.ls('gs://w/*')),
                         set(['gs://w/a', 'gs://w/a/b',
                              'gs://w/ab', 'gs://w/b']))
        self.assertEqual(list(self.fs.ls('gs://w/*/')),
                         ['gs://w/a/b'])
        self.assertEqual(list(self.fs.ls('gs://w/*/*')),
                         ['gs://w/a/b'])
        self.assertEqual(list(self.fs.ls('gs://w/a?')),
                         ['gs://w/ab'])
        # * can match /
        self.assertEqual(set(self.fs.ls('gs://w/a*')),
                         set(['gs://w/a', 'gs://w/a/b', 'gs://w/ab']))
        self.assertEqual(set(self.fs.ls('gs://w/*b')),
                         set(['gs://w/a/b', 'gs://w/ab', 'gs://w/b']))

    def test_du(self):
        self.put_gcs_multi({
            'gs://walrus/data/foo': b'abcde',
            'gs://walrus/data/bar/baz': b'fgh'
        })

        self.assertEqual(self.fs.du('gs://walrus/'), 8)
        self.assertEqual(self.fs.du('gs://walrus/data/foo'), 5)
        self.assertEqual(self.fs.du('gs://walrus/data/bar/baz'), 3)

    def test_exists(self):
        self.put_gcs_multi({
            'gs://walrus/data/foo': b'abcd'
        })
        self.assertEqual(self.fs.exists('gs://walrus/data/foo'), True)
        self.assertEqual(self.fs.exists('gs://walrus/data/bar'), False)

    def test_rm(self):
        self.put_gcs_multi({
            'gs://walrus/foo': b''
        })

        self.assertEqual(self.fs.exists('gs://walrus/foo'), True)
        self.fs.rm('gs://walrus/foo')
        self.assertEqual(self.fs.exists('gs://walrus/foo'), False)

    def test_rm_dir(self):
        self.put_gcs_multi({
            'gs://walrus/data/foo': b'',
            'gs://walrus/data/bar/baz': b'',
        })

        self.assertEqual(self.fs.exists('gs://walrus/data/foo'), True)
        self.assertEqual(self.fs.exists('gs://walrus/data/bar/baz'), True)
        self.fs.rm('gs://walrus/data')
        self.assertEqual(self.fs.exists('gs://walrus/data/foo'), False)
        self.assertEqual(self.fs.exists('gs://walrus/data/bar/baz'), False)


def _http_exception(status_code):
    mock_resp = mock.Mock()
    mock_resp.status = status_code

    return google_errors.HttpError(mock_resp, b'')


@skipIf(
    hasattr(sys, 'pypy_version_info') and (3, 0) <= sys.version_info < (3, 3),
    "googleapiclient doesn't work with PyPy 3")
class GCSFSHTTPErrorTestCase(PatcherTestCase):

    def setUp(self):
        self.fs = GCSFilesystem()
        self.gcs_path = 'gs://walrus/data'

        self.list_req_mock = mock.MagicMock()

        objects_ret = mock.MagicMock()
        objects_ret.list.return_value = self.list_req_mock
        objects_ret.get_media.return_value = google_http.HttpRequest(
            None, None, self.gcs_path)

        api_client = mock.MagicMock()
        api_client.objects.return_value = objects_ret

        self.fs._api_client = api_client
        self.next_chunk_patch = patch.object(
            google_http.MediaIoBaseDownload, 'next_chunk')

    def test_list_missing(self):
        self.list_req_mock.execute.side_effect = _http_exception(404)

        list(self.fs._ls_detailed(self.gcs_path))

    def test_list_actual_error(self):
        self.list_req_mock.execute.side_effect = _http_exception(500)

        with self.assertRaises(google_http.HttpError):
            list(self.fs._ls_detailed(self.gcs_path))

    def test_download_io_empty_file(self):
        io_obj = io.BytesIO()

        with self.next_chunk_patch as media_io_next_chunk:
            media_io_next_chunk.side_effect = _http_exception(416)

            self.fs._download_io(self.gcs_path, io_obj)
            self.assertEqual(len(io_obj.getvalue()), 0)

    def test_download_io_actual_error(self):
        io_obj = io.BytesIO()

        with self.next_chunk_patch as media_io_next_chunk:
            media_io_next_chunk.side_effect = _http_exception(500)

            with self.assertRaises(google_http.HttpError):
                self.fs._download_io(self.gcs_path, io_obj)
