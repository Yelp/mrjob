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
from os.path import join

from mrjob.fs.hadoop import HadoopFilesystem
from mrjob.fs import hadoop as fs_hadoop
from mrjob.util import which

from tests.compress import gzip_compress
from tests.fs import MockSubprocessTestCase
from tests.mockhadoop import main as mock_hadoop_main
from tests.py2 import patch
from tests.quiet import no_handlers_for_logger
from tests.sandbox import SandboxedTestCase


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
        # don't set MOCK_HADOOP_LOG, we get command history other ways]

        self.env['MOCK_HADOOP_VERSION'] = '2.7.1'

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
        self.assertEqual(sorted(self.fs.ls('hdfs:///')),
                         ['hdfs:///f', 'hdfs:///f2'])

    def test_ls_recurse(self):
        self.make_mock_file('f')
        self.make_mock_file('d/f2')
        self.assertEqual(sorted(self.fs.ls('hdfs:///')),
                         ['hdfs:///d/f2', 'hdfs:///f'])

    def test_ls_s3n(self):
        # hadoop fs -lsr doesn't have user and group info when reading from s3
        self.make_mock_file('f', 'foo')
        self.make_mock_file('f3 win', 'foo' * 10)
        self.assertEqual(sorted(self.fs.ls('s3n://bucket/')),
                         ['s3n://bucket/f', 's3n://bucket/f3 win'])

    def test_single_space(self):
        self.make_mock_file('foo bar')
        self.assertEqual(sorted(self.fs.ls('hdfs:///')),
                         ['hdfs:///foo bar'])

    def test_double_space(self):
        self.make_mock_file('foo  bar')
        self.assertEqual(sorted(self.fs.ls('hdfs:///')),
                         ['hdfs:///foo  bar'])

    def test_cat_uncompressed(self):
        self.make_mock_file('data/foo', 'foo\nfoo\n')

        remote_path = self.fs.join('hdfs:///data', 'foo')

        self.assertEqual(list(self.fs._cat_file(remote_path)),
                         [b'foo\n', b'foo\n'])

    def test_cat_bz2(self):
        self.make_mock_file('data/foo.bz2', bz2.compress(b'foo\n' * 1000))

        remote_path = self.fs.join('hdfs:///data', 'foo.bz2')

        self.assertEqual(list(self.fs._cat_file(remote_path)),
                         [b'foo\n'] * 1000)

    def test_cat_gz(self):
        self.make_mock_file('data/foo.gz', gzip_compress(b'foo\n' * 10000))

        remote_path = self.fs.join('hdfs:///data', 'foo.gz')

        self.assertEqual(list(self.fs._cat_file(remote_path)),
                         [b'foo\n'] * 10000)

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
        self.fs.mkdir('hdfs:///d/ave')
        local_path = os.path.join(self.tmp_dir, 'mock_hdfs_root', 'd', 'ave')
        self.assertEqual(os.path.isdir(local_path), True)

    def test_exists_no(self):
        path = 'hdfs:///f'
        self.assertEqual(self.fs.exists(path), False)

    def test_exists_yes(self):
        self.make_mock_file('f')
        path = 'hdfs:///f'
        self.assertEqual(self.fs.exists(path), True)

    def test_rm(self):
        local_path = self.make_mock_file('f')
        self.assertEqual(os.path.exists(local_path), True)
        self.fs.rm('hdfs:///f')
        self.assertEqual(os.path.exists(local_path), False)

    def test_rm_recursive(self):
        local_path = self.make_mock_file('foo/bar')
        self.assertEqual(os.path.exists(local_path), True)
        self.fs.rm('hdfs:///foo')  # remove containing directory
        self.assertEqual(os.path.exists(local_path), False)

    def test_rm_nonexistent(self):
        self.fs.rm('hdfs:///baz')

    def test_touchz(self):
        # mockhadoop doesn't implement this.
        pass


class Hadoop1FSTestCase(HadoopFSTestCase):
    def set_up_mock_hadoop(self):
        super(Hadoop1FSTestCase, self).set_up_mock_hadoop()

        self.env['MOCK_HADOOP_VERSION'] = '1.0.0'


class FindHadoopBinTestCase(SandboxedTestCase):

    def setUp(self):
        super(FindHadoopBinTestCase, self).setUp()

        # track calls to which()
        self.which = self.start(patch('mrjob.fs.hadoop.which', wraps=which))

        # keep which() from searching in /bin, etc.
        os.environ['PATH'] = self.tmp_dir

    def test_do_nothing_on_init(self):
        fs = HadoopFilesystem()

        self.assertFalse(self.which.called)

    def test_fallback(self):
        fs = HadoopFilesystem()

        self.assertFalse(self.which.called)

        with no_handlers_for_logger('mrjob.fs.hadoop'):
            self.assertEqual(fs.get_hadoop_bin(), ['hadoop'])

        self.which.assert_called_once_with('hadoop', path=None)

    def test_predefined_hadoop_bin(self):
        fs = HadoopFilesystem(hadoop_bin=['hadoop', '-v'])

        self.assertEqual(fs.get_hadoop_bin(), ['hadoop', '-v'])

        self.assertFalse(self.which.called)

    def _test_environment_variable(self, name, expect_failure=False):
        fs = HadoopFilesystem()

        fake_hadoop_bin = self.makefile(join(name.lower(), 'bin', 'hadoop'),
                                        executable=True)

        os.environ[name] = join(self.tmp_dir, name.lower())

        with no_handlers_for_logger('mrjob.fs.hadoop'):
            hadoop_bin = fs.get_hadoop_bin()

        if expect_failure:
            self.assertEqual(hadoop_bin, ['hadoop'])
        else:
            self.assertEqual(hadoop_bin, [fake_hadoop_bin])

    def test_hadoop_prefix(self):
        self._test_environment_variable('HADOOP_PREFIX')

    def test_hadoop_home(self):
        self._test_environment_variable('HADOOP_HOME')

    def test_hadoop_install(self):
        self._test_environment_variable('HADOOP_INSTALL')

    def test_hadoop_install_hadoop_subdir(self):
        fs = HadoopFilesystem()

        fake_hadoop_bin = self.makefile(
            join('install', 'hadoop', 'bin', 'hadoop'),
            executable=True)

        os.environ['HADOOP_INSTALL'] = join(self.tmp_dir, 'install')

        with no_handlers_for_logger('mrjob.fs.hadoop'):
            self.assertEqual(fs.get_hadoop_bin(), [fake_hadoop_bin])

    def test_path(self):
        fs = HadoopFilesystem()

        # PATH is set to self.tmp_dir
        fake_hadoop_bin = self.makefile('hadoop', executable=True)

        with no_handlers_for_logger('mrjob.fs.hadoop'):
            self.assertEqual(fs.get_hadoop_bin(), [fake_hadoop_bin])

    def test_hadoop_mapred_home(self):
        self._test_environment_variable('HADOOP_MAPRED_HOME')

    def test_hadoop_whatever_home(self):
        self._test_environment_variable('HADOOP_WHATEVER_HOME')

    def test_other_environment_variable(self):
        self._test_environment_variable('YARN_HADOOP_MRJOB_DIR',
                                        expect_failure=True)

    # TODO: test hadoop_home

    # TODO: test precedence
