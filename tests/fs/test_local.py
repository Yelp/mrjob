# Copyright 2009-2012 Yelp
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
from __future__ import with_statement

import bz2
import gzip
import os

from mrjob.fs.local import LocalFilesystem

from tests.sandbox import SandboxedTestCase


class LocalFSTestCase(SandboxedTestCase):

    def setUp(self):
        super(LocalFSTestCase, self).setUp()
        self.fs = LocalFilesystem()

    def test_can_handle_local_paths(self):
        self.assertEqual(self.fs.can_handle_path('/dem/bitties'), True)
        # relative paths
        self.assertEqual(self.fs.can_handle_path('garden'), True)

    def test_cant_handle_uris(self):
        self.assertEqual(self.fs.can_handle_path('http://yelp.com/'), False)

    def test_du(self):
        data_path_1 = self.makefile('data1', 'abcd')
        data_path_2 = self.makefile('more/data2', 'defg')

        self.assertEqual(self.fs.du(self.tmp_dir), 8)
        self.assertEqual(self.fs.du(data_path_1), 4)
        self.assertEqual(self.fs.du(data_path_2), 4)

    def test_ls_empty(self):
        self.assertEqual(list(self.fs.ls(self.tmp_dir)), [])

    def test_ls_basic(self):
        self.makefile('f', 'contents')
        self.assertEqual(list(self.fs.ls(self.tmp_dir)), self.abs_paths('f'))

    def test_ls_basic_2(self):
        self.makefile('f', 'contents')
        self.makefile('f2', 'contents')
        self.assertEqual(list(self.fs.ls(self.tmp_dir)),
                         self.abs_paths('f', 'f2'))

    def test_ls_recurse(self):
        self.makefile('f', 'contents')
        self.makefile('d/f2', 'contents')
        self.assertEqual(list(self.fs.ls(self.tmp_dir)),
                         self.abs_paths('f', 'd/f2'))

    def test_cat_uncompressed(self):
        path = self.makefile('f', 'bar\nfoo\n')
        self.assertEqual(list(self.fs._cat_file(path)), ['bar\n', 'foo\n'])

    def test_cat_gz(self):
        input_gz_path = os.path.join(self.tmp_dir, 'input.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'w')
        input_gz.write('foo\nbar\n')
        input_gz.close()

        self.assertEqual(list(self.fs._cat_file(input_gz_path)),
                         ['foo\n', 'bar\n'])

    def test_cat_bz2(self):
        input_bz2_path = os.path.join(self.tmp_dir, 'input.bz2')
        input_bz2 = bz2.BZ2File(input_bz2_path, 'w')
        input_bz2.write('bar\nbar\nfoo\n')
        input_bz2.close()

        self.assertEqual(list(self.fs._cat_file(input_bz2_path)),
                         ['bar\n', 'bar\n', 'foo\n'])

    def test_mkdir(self):
        path = os.path.join(self.tmp_dir, 'dir')
        self.fs.mkdir(path)
        self.assertEqual(os.path.isdir(path), True)

    def test_path_exists_no(self):
        path = os.path.join(self.tmp_dir, 'f')
        self.assertEqual(self.fs.path_exists(path), False)

    def test_path_exists_yes(self):
        path = self.makefile('f', 'contents')
        self.assertEqual(self.fs.path_exists(path), True)

    def test_rm_file(self):
        path = self.makefile('f', 'contents')
        self.assertEqual(self.fs.path_exists(path), True)

        self.fs.rm(path)
        self.assertEqual(self.fs.path_exists(path), False)

    def test_rm_dir(self):
        path = self.makedirs('foobar')
        self.assertEqual(self.fs.path_exists(path), True)

        self.fs.rm(path)
        self.assertEqual(self.fs.path_exists(path), False)

    def test_touchz(self):
        path = os.path.join(self.tmp_dir, 'f')
        self.fs.touchz(path)
        self.fs.touchz(path)
        with open(path, 'w') as f:
            f.write('not empty anymore')
        self.assertRaises(OSError, self.fs.touchz, path)

    def test_md5sum(self):
        path = self.makefile('f', 'abcd')
        self.assertEqual(self.fs.md5sum(path),
                         'e2fc714c4727ee9395f324cd2e7f331f')
