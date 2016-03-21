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


class GCSFSTestCase(MockGoogleAPITestCase):

    def setUp(self):
        super(GCSFSTestCase, self).setUp()
        self.fs = GCSFilesystem()

    def test_cat_uncompressed(self):
        self.put_gcs_data(
            {'walrus': {'data/foo': b'foo\nfoo\n'}})

        self.assertEqual(list(self.fs._cat_file('gs://walrus/data/foo')),
                         [b'foo\n', b'foo\n'])

    def test_cat_bz2(self):
        self.put_gcs_data(
            {'walrus': {'data/foo.bz2': bz2.compress(b'foo\n' * 1000)}})

        self.assertEqual(list(self.fs._cat_file('gs://walrus/data/foo.bz2')),
                         [b'foo\n'] * 1000)

    def test_cat_gz(self):
        self.put_gcs_data(
            {'walrus': {'data/foo.gz': gzip_compress(b'foo\n' * 10000)}})

        self.assertEqual(list(self.fs._cat_file('gs://walrus/data/foo.gz')),
                         [b'foo\n'] * 10000)

    def test_ls_key(self):
        self.put_gcs_data(
            {'walrus': {'data/foo': b''}})

        self.assertEqual(list(self.fs.ls('gs://walrus/data/foo')),
                         ['gs://walrus/data/foo'])

    def test_ls_recursively(self):
        self.put_gcs_data(
            {'walrus': {'data/bar': b'',
                        'data/bar/baz': b'',
                        'data/foo': b'',
                        'qux': b''}})

        uris = [
            'gs://walrus/data/bar',
            'gs://walrus/data/bar/baz',
            'gs://walrus/data/foo',
            'gs://walrus/qux',
        ]

        self.assertEqual(list(self.fs.ls('gs://walrus/')), uris)
        self.assertEqual(list(self.fs.ls('gs://walrus/*')), uris)

        self.assertEqual(list(self.fs.ls('gs://walrus/data')), uris[:-1])
        self.assertEqual(list(self.fs.ls('gs://walrus/data/')), uris[:-1])
        self.assertEqual(list(self.fs.ls('gs://walrus/data/*')), uris[:-1])

    def test_ls_globs(self):
        self.put_gcs_data(
            {'w': {'a': b'',
                   'a/b': b'',
                   'ab': b'',
                   'b': b''}})

        self.assertEqual(list(self.fs.ls('gs://w/')),
                         ['gs://w/a', 'gs://w/a/b', 'gs://w/ab', 'gs://w/b'])
        self.assertEqual(list(self.fs.ls('gs://w/*')),
                         ['gs://w/a', 'gs://w/a/b', 'gs://w/ab', 'gs://w/b'])
        self.assertEqual(list(self.fs.ls('gs://w/*/')),
                         ['gs://w/a/b'])
        self.assertEqual(list(self.fs.ls('gs://w/*/*')),
                         ['gs://w/a/b'])
        self.assertEqual(list(self.fs.ls('gs://w/a?')),
                         ['gs://w/ab'])
        # * can match /
        self.assertEqual(list(self.fs.ls('gs://w/a*')),
                         ['gs://w/a', 'gs://w/a/b', 'gs://w/ab'])
        self.assertEqual(list(self.fs.ls('gs://w/*b')),
                         ['gs://w/a/b', 'gs://w/ab', 'gs://w/b'])


    def test_du(self):
        self.put_gcs_data({
            'walrus': {'data/foo': b'abcde',
                       'data/bar/baz': b'fgh'}})

        self.assertEqual(self.fs.du('gs://walrus/'), 8)
        self.assertEqual(self.fs.du('gs://walrus/data/foo'), 5)
        self.assertEqual(self.fs.du('gs://walrus/data/bar/baz'), 3)

    def test_exists(self):
        self.put_gcs_data({
            'walrus': {'data/foo': b'abcd'}})
        self.assertEqual(self.fs.exists('gs://walrus/data/foo'), True)
        self.assertEqual(self.fs.exists('gs://walrus/data/bar'), False)

    def test_rm(self):
        self.put_gcs_data({
            'walrus': {'foo': b''}})

        self.assertEqual(self.fs.exists('gs://walrus/foo'), True)
        self.fs.rm('gs://walrus/foo')
        self.assertEqual(self.fs.exists('gs://walrus/foo'), False)

    def test_rm_dir(self):
        self.put_gcs_data({
            'walrus': {'data/foo': b'',
                       'data/bar/baz': b''}})

        self.assertEqual(self.fs.exists('gs://walrus/data/foo'), True)
        self.assertEqual(self.fs.exists('gs://walrus/data/bar/baz'), True)
        self.fs.rm('gs://walrus/data')
        self.assertEqual(self.fs.exists('gs://walrus/data/foo'), False)
        self.assertEqual(self.fs.exists('gs://walrus/data/bar/baz'), False)

