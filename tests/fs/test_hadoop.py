# Copyright 2009-2013 Yelp and Contributors
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
import os

from mrjob.fs.hadoop import HadoopFilesystem
from mrjob.fs import hadoop as fs_hadoop

from tests.compress import gzip_compress
from tests.fs import MockSubprocessTestCase
from tests.mockhadoop import main as mock_hadoop_main


class HadoopFSTestCase(MockSubprocessTestCase):

    def setUp(self):
        super(HadoopFSTestCase, self).setUp()
        # wrap HadoopFilesystem so it gets cat()
        self.fs = HadoopFilesystem(['hadoop'])
        self.set_up_mock_hadoop()
        self.mock_popen(fs_hadoop, mock_hadoop_main, self.env)

    def set_up_mock_hadoop(self):
        # setup fake hadoop home
        self.env = {}
        self.env['HADOOP_HOME'] = self.makedirs('mock_hadoop_home')

        self.makefile(
            os.path.join(
                'mock_hadoop_home',
                'contrib',
                'streaming',
                'hadoop-0.X.Y-streaming.jar'),
            'i are java bytecode',
        )

        self.env['MOCK_HDFS_ROOT'] = self.makedirs('mock_hdfs_root')
        self.env['MOCK_HADOOP_OUTPUT'] = self.makedirs('mock_hadoop_output')
        self.env['USER'] = 'mrjob_tests'
        # don't set MOCK_HADOOP_LOG, we get command history other ways

    def make_mock_file(self, name, contents='contents'):
        return self.makefile(os.path.join('mock_hdfs_root', name), contents)

    def test_ls_empty(self):
        self.assertEqual(list(self.fs.ls('hdfs:///')), [])

    def test_ls_basic(self):
        self.make_mock_file('f')
        self.assertEqual(list(self.fs.ls('hdfs:///')), ['hdfs:///f'])

    def test_ls_basic_2(self):
        self.make_mock_file('f')
        self.make_mock_file('f2')
        self.assertItemsEqual(list(self.fs.ls('hdfs:///')), ['hdfs:///f',
                                                             'hdfs:///f2'])

    def test_ls_recurse(self):
        self.make_mock_file('f')
        self.make_mock_file('d/f2')
        self.assertItemsEqual(list(self.fs.ls('hdfs:///')),
                              ['hdfs:///f', 'hdfs:///d/f2'])

    def test_ls_s3n(self):
        # hadoop fs -lsr doesn't have user and group info when reading from s3
        self.make_mock_file('f', 'foo')
        self.make_mock_file('f3 win', 'foo' * 10)
        self.assertItemsEqual(list(self.fs.ls('s3n://bucket/')),
                              ['s3n://bucket/f', 's3n://bucket/f3 win'])

    def test_single_space(self):
        self.make_mock_file('foo bar')
        self.assertItemsEqual(list(self.fs.ls('hdfs:///')),
                              ['hdfs:///foo bar'])

    def test_double_space(self):
        self.make_mock_file('foo  bar')
        self.assertItemsEqual(list(self.fs.ls('hdfs:///')),
                              ['hdfs:///foo  bar'])

    def test_cat_uncompressed(self):
        self.make_mock_file('data/foo', 'foo\nfoo\n')

        remote_path = self.fs.path_join('hdfs:///data', 'foo')

        self.assertEqual(list(self.fs._cat_file(remote_path)),
                         ['foo\n', 'foo\n'])

    def test_cat_bz2(self):
        self.make_mock_file('data/foo.bz2', bz2.compress('foo\n' * 1000))

        remote_path = self.fs.path_join('hdfs:///data', 'foo.bz2')

        self.assertEqual(list(self.fs._cat_file(remote_path)),
                         ['foo\n'] * 1000)

    def test_cat_gz(self):
        self.make_mock_file('data/foo.gz', gzip_compress('foo\n' * 10000))

        remote_path = self.fs.path_join('hdfs:///data', 'foo.gz')

        self.assertEqual(list(self.fs._cat_file(remote_path)),
                         ['foo\n'] * 10000)

    def test_du(self):
        self.make_mock_file('data1', 'abcd')
        self.make_mock_file('more/data2', 'defg')
        self.make_mock_file('more/data3', 'hijk')

        self.assertEqual(self.fs.du('hdfs:///'), 12)
        self.assertEqual(self.fs.du('hdfs:///data1'), 4)
        self.assertEqual(self.fs.du('hdfs:///more'), 8)
        self.assertEqual(self.fs.du('hdfs:///more/*'), 8)
        self.assertEqual(self.fs.du('hdfs:///more/data2'), 4)
        self.assertEqual(self.fs.du('hdfs:///more/data3'), 4)

    def test_mkdir(self):
        self.fs.mkdir('hdfs:///d')
        local_path = os.path.join(self.tmp_dir, 'mock_hdfs_root', 'd')
        self.assertEqual(os.path.isdir(local_path), True)

    def test_path_exists_no(self):
        path = 'hdfs:///f'
        self.assertEqual(self.fs.path_exists(path), False)

    def test_path_exists_yes(self):
        self.make_mock_file('f')
        path = 'hdfs:///f'
        self.assertEqual(self.fs.path_exists(path), True)

    def test_rm(self):
        local_path = self.make_mock_file('f')
        self.assertEqual(os.path.exists(local_path), True)
        self.fs.rm('hdfs:///f')
        self.assertEqual(os.path.exists(local_path), False)

    def test_touchz(self):
        # mockhadoop doesn't implement this.
        pass


class NewerHadoopFSTestCase(HadoopFSTestCase):

    def set_up_mock_hadoop(self):
        super(NewerHadoopFSTestCase, self).set_up_mock_hadoop()

        self.env['MOCK_HADOOP_LS_RETURNS_FULL_URIS'] = '1'
