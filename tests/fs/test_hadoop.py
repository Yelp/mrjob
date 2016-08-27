# Copyright 2009-2013 Yelp and Contributors
# Copyright 2014 Shusen Liu
# Copyright 2015-2016 Yelp
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
from tests.mockhadoop import get_mock_hdfs_root
from tests.mockhadoop import main as mock_hadoop_main
from tests.py2 import MagicMock
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

        self.env['MOCK_HADOOP_TMP'] = self.makedirs('mock_hadoop')
        self.env['MOCK_HADOOP_VERSION'] = '2.7.1'

        self.env['USER'] = 'mrjob_tests'

    def make_mock_file(self, name, contents='contents'):
        return self.makefile(
            os.path.join(get_mock_hdfs_root(self.env), name), contents)

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

    def test_du_non_existent(self):
        self.assertEqual(self.fs.du('hdfs:///does-not-exist'), 0)

    def test_mkdir(self):
        self.fs.mkdir('hdfs:///d/ave')
        local_path = os.path.join(get_mock_hdfs_root(self.env), 'd', 'ave')
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

        # create basic HadoopFilesystem (okay to overwrite)
        self.fs = HadoopFilesystem()

    def _add_hadoop_bin_for_envvar(self, envvar, *dirnames):
        """Add a fake "Hadoop" binary to its own subdirectory of
        ``self.tmp_dir``, and set *os.environ[envvar]* to point at it. You can
        use *dirnames* to put the binary in a subdirectory of
        *os.environ[envvar]* (e.g. ``'bin'``).

        return the path to the fake Hadoop binary.
        """
        os.environ[envvar] = join(self.tmp_dir, envvar.lower())

        hadoop_bin_path = join(join(os.environ[envvar], *dirnames), 'hadoop')

        self.makefile(hadoop_bin_path, executable=True)

        return hadoop_bin_path

    # tests without environment variables

    def test_do_nothing_on_init(self):
        self.assertFalse(self.which.called)

    def test_fallback(self):
        self.assertFalse(self.which.called)

        with no_handlers_for_logger('mrjob.fs.hadoop'):
            self.assertEqual(self.fs.get_hadoop_bin(), ['hadoop'])

        self.which.assert_called_once_with('hadoop', path=None)

    def test_predefined_hadoop_bin(self):
        self.fs = HadoopFilesystem(hadoop_bin=['hadoop', '-v'])

        self.assertEqual(self.fs.get_hadoop_bin(), ['hadoop', '-v'])

        self.assertFalse(self.which.called)

    def test_deprecated_hadoop_home_option(self):
        hadoop_home = join(self.tmp_dir, 'hadoop_home_option')
        hadoop_bin = self.makefile(join(hadoop_home, 'bin', 'hadoop'),
                                   executable=True)

        # deprecation warning is in HadoopJobRunner
        self.fs = HadoopFilesystem(hadoop_home=hadoop_home)

        with no_handlers_for_logger('mrjob.fs.hadoop'):
            self.assertEqual(self.fs.get_hadoop_bin(), [hadoop_bin])

    # environment variable tests

    def _test_environment_variable(self, envvar, *dirnames):
        """Check if we can find the hadoop binary from *envvar*"""
        # okay to add after HadoopFilesystem() created; it hasn't looked yet
        hadoop_bin = self._add_hadoop_bin_for_envvar(envvar, *dirnames)

        with no_handlers_for_logger('mrjob.fs.hadoop'):
            self.assertEqual(self.fs.get_hadoop_bin(), [hadoop_bin])

    def test_hadoop_prefix(self):
        self._test_environment_variable('HADOOP_PREFIX', 'bin')

    def test_hadoop_home_envvar(self):
        self._test_environment_variable('HADOOP_HOME', 'bin')

    def test_hadoop_install(self):
        self._test_environment_variable('HADOOP_INSTALL', 'bin')

    def test_hadoop_install_hadoop_subdir(self):
        self._test_environment_variable('HADOOP_INSTALL', 'hadoop', 'bin')

    def test_path(self):
        self._test_environment_variable('PATH')

    def test_two_part_path(self):
        hadoop_path1 = join(self.tmp_dir, 'path1')
        hadoop_path1_bin = self.makefile(join(hadoop_path1, 'hadoop'),
                                         executable=True)
        hadoop_path2 = join(self.tmp_dir, 'path2')
        hadoop_path2_bin = self.makefile(join(hadoop_path2, 'hadoop'),
                                         executable=True)

        os.environ['PATH'] = ':'.join([hadoop_path1, hadoop_path2])

        with no_handlers_for_logger('mrjob.fs.hadoop'):
            self.assertEqual(self.fs.get_hadoop_bin(), [hadoop_path1_bin])
            self.assertNotEqual(self.fs.get_hadoop_bin(), [hadoop_path2_bin])

    def test_hadoop_mapred_home(self):
        self._test_environment_variable('HADOOP_MAPRED_HOME', 'bin')

    def test_hadoop_anything_home(self):
        self._test_environment_variable('HADOOP_ANYTHING_HOME', 'bin')

    def test_other_environment_variable(self):
        self._add_hadoop_bin_for_envvar('HADOOP_YARN_MRJOB_DIR', 'bin')

        with no_handlers_for_logger('mrjob.fs.hadoop'):
            self.assertEqual(self.fs.get_hadoop_bin(), ['hadoop'])

    # precedence tests

    def test_deprecated_hadoop_home_option_beats_hadoop_prefix(self):
        self._add_hadoop_bin_for_envvar('HADOOP_PREFIX', 'bin')
        self.test_deprecated_hadoop_home_option()

    def test_hadoop_prefix_beats_hadoop_home_envvar(self):
        self._add_hadoop_bin_for_envvar('HADOOP_HOME', 'bin')
        self.test_hadoop_prefix()

    def test_hadoop_home_envvar_beats_hadoop_install(self):
        self._add_hadoop_bin_for_envvar('HADOOP_INSTALL', 'bin')
        self.test_hadoop_home_envvar()

    def test_hadoop_install_beats_hadoop_install_subdir(self):
        self._add_hadoop_bin_for_envvar('HADOOP_INSTALL', 'hadoop', 'bin')
        # verify that this test and test_hadoop_install() use same value
        # for $HADOOP_INSTALL
        hadoop_install = os.environ['HADOOP_INSTALL']

        self.test_hadoop_install()

        self.assertEqual(hadoop_install, os.environ['HADOOP_INSTALL'])

    def test_hadoop_install_hadoop_subdir_beats_path(self):
        self._add_hadoop_bin_for_envvar('PATH')
        self.test_hadoop_install_hadoop_subdir()

    def test_path_beats_hadoop_mapred_home(self):
        self._add_hadoop_bin_for_envvar('HADOOP_MAPRED_HOME', 'bin')
        self.test_path()

    def test_hadoop_anything_home_is_alphabetical(self):
        # $HADOOP_ANYTHING_HOME comes before $HADOOP_MAPRED_HOME
        self._add_hadoop_bin_for_envvar('HADOOP_MAPRED_HOME', 'bin')
        self.test_hadoop_anything_home()


class CatFileCleanupTestCase(SandboxedTestCase):
    # regression test for #1396

    def test_logging_stderr_in_cleanup(self):

        def mock_Popen(*args, **kwargs):
            mock_proc = MagicMock()

            mock_proc.stdout = MagicMock()
            mock_proc.stdout.__iter__.return_value = [
                b'line1\n', b'line2\n']

            mock_proc.stderr = MagicMock()
            mock_proc.stderr.__iter__.return_value = [
                b'Emergency, everybody to get from street\n']

            mock_proc.wait.return_value = 0

            return mock_proc

        self.start(patch('mrjob.fs.hadoop.Popen', mock_Popen))

        mock_log = self.start(patch('mrjob.fs.hadoop.log'))

        fs = HadoopFilesystem()

        data = b''.join(fs._cat_file('/some/path'))
        self.assertEqual(data, b'line1\nline2\n')

        mock_log.error.assert_called_once_with(
            'STDERR: Emergency, everybody to get from street')
