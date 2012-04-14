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

import os
from shutil import rmtree
from tempfile import mkdtemp

try:
    from unittest2 import TestCase
except ImportError:
    from unittest2 import TestCase

from mrjob.fs.local import LocalFilesystem


class LocalFSTestCase(TestCase):

    def setUp(self):
        self.root = mkdtemp()
        self.addCleanup(rmtree, self.root)
        self.fs = LocalFilesystem()

    def makedirs(self, path):
        os.makedirs(os.path.join(self.root, path))

    def makefile(self, path, contents):
        self.makedirs(os.path.split(path[0]))
        with open(os.path.join(self.root, path), 'w') as f:
            f.write(contents)

    def test_can_handle_path_match(self):
        self.assertEqual(self.fs.can_handle_path('/dem/bitties'), True)

    def test_can_handle_path_nomatch(self):
        self.assertEqual(self.fs.can_handle_path('http://yelp.com/'), False)

    def test_ls_empty(self):
        self.assertEqual(list(self.fs.ls(self.root)), [])
