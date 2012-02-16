# Copyright 2009-2011 Yelp
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

"""Test the hadoop job runner."""

from __future__ import with_statement

from StringIO import StringIO
import bz2
import getpass
import gzip
import os
import shlex
import shutil
from subprocess import check_call
import tempfile

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from tests.mockhadoop import create_mock_hadoop_script
from tests.mockhadoop import add_mock_hadoop_output
from tests.mr_two_step_job import MRTwoStepJob
from tests.quiet import logger_disabled

from mrjob.hadoop import HadoopJobRunner
from mrjob.hadoop import find_hadoop_streaming_jar


class TestFindHadoopStreamingJar(unittest.TestCase):

    def setUp(self):
        self.setup_tmp_dir()

    def tearDown(self):
        self.rm_tmp_dir()

    def setup_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_find_hadoop_streaming_jar(self):
        # shouldn't find anything if nothing's there
        self.assertEqual(find_hadoop_streaming_jar(self.tmp_dir), None)

        jar_dir = os.path.join(self.tmp_dir, 'a', 'b', 'c')
        os.makedirs(jar_dir)
        empty_dir = os.path.join(self.tmp_dir, 'empty')
        os.makedirs(empty_dir)

        # not just any jar will do
        mason_jar_path = os.path.join(jar_dir, 'mason.jar')
        open(mason_jar_path, 'w').close()
        self.assertEqual(find_hadoop_streaming_jar(self.tmp_dir), None)

        # should match streaming jar
        streaming_jar_path = os.path.join(
            jar_dir, 'hadoop-0.20.2-streaming.jar')
        open(streaming_jar_path, 'w').close()
        self.assertEqual(find_hadoop_streaming_jar(self.tmp_dir),
                         streaming_jar_path)

        # shouldn't find anything if we look in the wrong dir
        self.assertEqual(find_hadoop_streaming_jar(empty_dir), None)


class MockHadoopTestCase(unittest.TestCase):

    def setUp(self):
        self.setup_hadoop_home_and_environment_vars()

    def tearDown(self):
        self.delete_hadoop_home_and_restore_environment_vars()

    def setup_hadoop_home_and_environment_vars(self):
        self._old_environ = os.environ.copy()

        # setup fake hadoop home
        hadoop_home = tempfile.mkdtemp(prefix='mock_hadoop_home.')
        os.environ['HADOOP_HOME'] = hadoop_home

        # make fake hadoop binary
        os.mkdir(os.path.join(hadoop_home, 'bin'))
        self.hadoop_bin = os.path.join(hadoop_home, 'bin', 'hadoop')
        create_mock_hadoop_script(self.hadoop_bin)

        # make fake streaming jar
        os.makedirs(os.path.join(hadoop_home, 'contrib', 'streaming'))
        streaming_jar_path = os.path.join(
            hadoop_home, 'contrib', 'streaming', 'hadoop-0.X.Y-streaming.jar')
        open(streaming_jar_path, 'w').close()

        # set up fake HDFS
        mock_hdfs_root = tempfile.mkdtemp(prefix='mock_hdfs.')
        os.environ['MOCK_HDFS_ROOT'] = mock_hdfs_root

        # make fake output dir
        mock_output_dir = tempfile.mkdtemp(prefix='mock_hadoop_output.')
        os.environ['MOCK_HADOOP_OUTPUT'] = mock_output_dir

        # set up cmd log
        _, mock_log_path = tempfile.mkstemp(prefix='mockhadoop.log')
        os.environ['MOCK_HADOOP_LOG'] = mock_log_path

    def delete_hadoop_home_and_restore_environment_vars(self):
        mock_hdfs_root = os.environ['MOCK_HDFS_ROOT']
        mock_output_dir = os.environ['MOCK_HADOOP_OUTPUT']
        mock_log_path = os.environ['MOCK_HADOOP_LOG']

        os.environ.clear()
        os.environ.update(self._old_environ)

        shutil.rmtree(mock_hdfs_root)
        shutil.rmtree(mock_output_dir)
        os.unlink(mock_log_path)


class HadoopJobRunnerEndToEndTestCase(MockHadoopTestCase):

    def setUp(self):
        super(HadoopJobRunnerEndToEndTestCase, self).setUp()
        self.make_tmp_dir()

    def tearDown(self):
        self.rm_tmp_dir()
        super(HadoopJobRunnerEndToEndTestCase, self).tearDown()

    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def _test_end_to_end(self, args=()):
        # read from STDIN, a local file, and a remote file
        stdin = StringIO('foo\nbar\n')

        local_input_path = os.path.join(self.tmp_dir, 'input')
        with open(local_input_path, 'w') as local_input_file:
            local_input_file.write('bar\nqux\n')

        input_to_upload = os.path.join(self.tmp_dir, 'remote_input')
        with open(input_to_upload, 'w') as input_to_upload_file:
            input_to_upload_file.write('foo\n')
        remote_input_path = 'hdfs:///data/foo'
        check_call([self.hadoop_bin,
                    'fs', '-put', input_to_upload, remote_input_path])

        # doesn't matter what the intermediate output is; just has to exist.
        add_mock_hadoop_output([''])
        add_mock_hadoop_output(['1\t"qux"\n2\t"bar"\n', '2\t"foo"\n5\tnull\n'])

        mr_job = MRTwoStepJob(['-r', 'hadoop', '-v',
                               '--no-conf', '--hadoop-arg', '-libjar',
                               '--hadoop-arg', 'containsJars.jar'] + list(args)
                              + ['-', local_input_path, remote_input_path]
                              + ['--hadoop-input-format', 'FooFormat']
                              + ['--hadoop-output-format', 'BarFormat']
                              + ['--jobconf', 'x=y'])
        mr_job.sandbox(stdin=stdin)

        local_tmp_dir = None
        results = []

        # don't care that --hadoop-*-format is deprecated
        with logger_disabled('mrjob.job'):
            runner = mr_job.make_runner()

        with runner as runner:  # i.e. call cleanup when we're done
            assert isinstance(runner, HadoopJobRunner)
            runner.run()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

            local_tmp_dir = runner._get_local_tmp_dir()
            # make sure cleanup hasn't happened yet
            assert os.path.exists(local_tmp_dir)
            assert any(runner.ls(runner.get_output_dir()))

            # make sure we're writing to the correct path in HDFS
            hdfs_root = os.environ['MOCK_HDFS_ROOT']
            self.assertEqual(sorted(os.listdir(hdfs_root)), ['data', 'user'])
            home_dir = os.path.join(hdfs_root, 'user', getpass.getuser())
            self.assertEqual(os.listdir(home_dir), ['tmp'])
            self.assertEqual(os.listdir(os.path.join(home_dir, 'tmp')),
                             ['mrjob'])
            self.assertEqual(runner._opts['hadoop_extra_args'],
                             ['-libjar', 'containsJars.jar'])

            # make sure mrjob.tar.gz is uploaded and in PYTHONPATH
            assert runner._mrjob_tar_gz_path
            mrjob_tar_gz_file_dicts = [
                file_dict for file_dict in runner._files
                if file_dict['path'] == runner._mrjob_tar_gz_path]
            self.assertEqual(len(mrjob_tar_gz_file_dicts), 1)

            mrjob_tar_gz_file_dict = mrjob_tar_gz_file_dicts[0]
            assert mrjob_tar_gz_file_dict['name']

            pythonpath = runner._get_cmdenv()['PYTHONPATH']
            self.assertIn(mrjob_tar_gz_file_dict['name'],
                          pythonpath.split(':'))

        self.assertEqual(sorted(results),
                         [(1, 'qux'), (2, 'bar'), (2, 'foo'), (5, None)])

        # make sure we called hadoop the way we expected
        with open(os.environ['MOCK_HADOOP_LOG']) as mock_log:
            hadoop_cmd_args = [shlex.split(line) for line in mock_log]

        jar_cmd_args = [args for args in hadoop_cmd_args
                        if args[:1] == ['jar']]
        self.assertEqual(len(jar_cmd_args), 2)
        step_0_args, step_1_args = jar_cmd_args

        # check input/output format
        self.assertIn('-inputformat', step_0_args)
        self.assertNotIn('-outputformat', step_0_args)
        self.assertNotIn('-inputformat', step_1_args)
        self.assertIn('-outputformat', step_1_args)

        # make sure -libjar extra arg comes before -mapper
        for args in (step_0_args, step_1_args):
            self.assertIn('-libjar', args)
            self.assertIn('-mapper', args)
            self.assertLess(args.index('-libjar'), args.index('-mapper'))

        # make sure -jobconf made it through
        self.assertIn('-D', step_0_args)

        # make sure cleanup happens
        assert not os.path.exists(local_tmp_dir)
        assert not any(runner.ls(runner.get_output_dir()))

    def test_end_to_end(self):
        self._test_end_to_end()

    def test_end_to_end_with_explicit_hadoop_bin(self):
        self._test_end_to_end(['--hadoop-bin', self.hadoop_bin])


class TestCat(MockHadoopTestCase):

    def setUp(self):
        super(TestCat, self).setUp()
        self.make_tmp_dir()

    def tearDown(self):
        self.rm_tmp_dir()
        super(TestCat, self).tearDown()

    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_cat_uncompressed(self):
        local_input_path = os.path.join(self.tmp_dir, 'input')
        with open(local_input_path, 'w') as input_file:
            input_file.write('bar\nfoo\n')

        input_to_upload = os.path.join(self.tmp_dir, 'remote_input')
        with open(input_to_upload, 'w') as input_to_upload_file:
            input_to_upload_file.write('foo\nfoo\n')
        remote_input_path = 'hdfs:///data/foo'
        check_call([self.hadoop_bin,
                    'fs', '-put', input_to_upload, remote_input_path])

        with HadoopJobRunner(cleanup=['NONE'], conf_path=False) as runner:
            local_output = []
            for line in runner.cat(local_input_path):
                local_output.append(line)

            remote_output = []
            for line in runner.cat(remote_input_path):
                remote_output.append(line)

        self.assertEqual(local_output, ['bar\n', 'foo\n'])
        self.assertEqual(remote_output, ['foo\n', 'foo\n'])

    def test_cat_compressed(self):
        input_gz_path = os.path.join(self.tmp_dir, 'input.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'w')
        input_gz.write('foo\nbar\n')
        input_gz.close()

        with HadoopJobRunner(cleanup=['NONE'], conf_path=False) as runner:
            output = []
            for line in runner.cat(input_gz_path):
                output.append(line)

        self.assertEqual(output, ['foo\n', 'bar\n'])

        input_bz2_path = os.path.join(self.tmp_dir, 'input.bz2')
        input_bz2 = bz2.BZ2File(input_bz2_path, 'w')
        input_bz2.write('bar\nbar\nfoo\n')
        input_bz2.close()

        with HadoopJobRunner(cleanup=['NONE'], conf_path=False) as runner:
            output = []
            for line in runner.cat(input_bz2_path):
                output.append(line)

        self.assertEqual(output, ['bar\n', 'bar\n', 'foo\n'])


class TestURIs(MockHadoopTestCase):

    def test_uris(self):
        runner = HadoopJobRunner(conf_path=False)
        list(runner.ls('hdfs://tmp/waffles'))
        list(runner.ls('leggo://my/eggo'))
        list(runner.ls('/tmp'))

        with open(os.environ['MOCK_HADOOP_LOG']) as mock_log:
            hadoop_cmd_args = [shlex.split(line) for line in mock_log]

        self.assertEqual(hadoop_cmd_args, [
            ['fs', '-lsr', 'hdfs://tmp/waffles'],
            ['fs', '-lsr', 'leggo://my/eggo'],
        ])
