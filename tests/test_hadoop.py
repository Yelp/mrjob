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

"""Test the hadoop job runner."""

from __future__ import with_statement

from StringIO import StringIO
import getpass
import os
import shlex
from subprocess import check_call

from mock import patch

from mrjob.hadoop import HadoopJobRunner
from mrjob.hadoop import find_hadoop_streaming_jar

from tests.mockhadoop import create_mock_hadoop_script
from tests.mockhadoop import add_mock_hadoop_output
from tests.mr_two_step_hadoop_format_job import MRTwoStepJob
from tests.sandbox import SandboxedTestCase


class TestHadoopHomeRegression(SandboxedTestCase):

    def test_hadoop_home_regression(self):
        mason_jar_path = os.path.join(
            self.tmp_dir, 'hadoop-0.20.20-streaming.jar')
        open(mason_jar_path, 'w').close()

        # kill $HADOOP_HOME if it exists
        try:
            del os.environ['HADOOP_HOME']
        except KeyError:
            pass

        HadoopJobRunner(hadoop_home=self.tmp_dir, conf_path=False)


class TestFindHadoopStreamingJar(SandboxedTestCase):

    def test_find_hadoop_streaming_jar(self):
        # not just any jar will do
        with patch.object(os, 'walk', return_value=[
            ('/some_dir', None, 'mason.jar')]):
            self.assertEqual(find_hadoop_streaming_jar('/some_dir'), None)

        # should match streaming jar
        with patch.object(os, 'walk', return_value=[
            ('/some_dir', None, 'hadoop-0.20.2-streaming.jar')]):
            self.assertEqual(find_hadoop_streaming_jar('/some_dir'), None)

        # shouldn't find anything in an empty dir
        with patch.object(os, 'walk', return_value=[]):
            self.assertEqual(find_hadoop_streaming_jar('/some_dir'), None)


class MockHadoopTestCase(SandboxedTestCase):

    def setUp(self):
        super(MockHadoopTestCase, self).setUp()
        # setup fake hadoop home
        hadoop_home = self.makedirs('mock_hadoop_home')
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
        mock_hdfs_root = self.makedirs('mock_hdfs_root')
        os.environ['MOCK_HDFS_ROOT'] = mock_hdfs_root

        # make fake output dir
        mock_output_dir = self.makedirs('mock_hadoop_output')
        os.environ['MOCK_HADOOP_OUTPUT'] = mock_output_dir

        # set up cmd log
        mock_log_path = self.makefile('mock_hadoop_logs', '')
        os.environ['MOCK_HADOOP_LOG'] = mock_log_path


class HadoopJobRunnerEndToEndTestCase(MockHadoopTestCase):

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
                              + ['--jobconf', 'x=y'])
        mr_job.sandbox(stdin=stdin)

        local_tmp_dir = None
        results = []

        with mr_job.make_runner() as runner:
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


class TestURIs(MockHadoopTestCase):

    def test_uris(self):
        runner = HadoopJobRunner(conf_paths=[])
        list(runner.ls('hdfs://tmp/waffles'))
        list(runner.ls('leggo://my/eggo'))
        list(runner.ls('/tmp'))

        with open(os.environ['MOCK_HADOOP_LOG']) as mock_log:
            hadoop_cmd_args = [shlex.split(line) for line in mock_log]

        self.assertEqual(hadoop_cmd_args, [
            ['fs', '-lsr', 'hdfs://tmp/waffles'],
            ['fs', '-lsr', 'leggo://my/eggo'],
        ])
