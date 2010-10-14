# Copyright 2009-2010 Yelp
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
import os
import shutil
from subprocess import check_call
import sys
import tempfile
from testify import TestCase, assert_equal, setup, teardown

from tests.mockhadoop import create_mock_hadoop_script, add_mock_hadoop_output
from tests.mr_two_step_job import MRTwoStepJob
from mrjob.hadoop import *

class TestFindHadoopStreamingJar(TestCase):

    @setup
    def setup_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()
        
    @teardown
    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_find_hadoop_streaming_jar(self):
        # shouldn't find anything if nothing's there
        assert_equal(find_hadoop_streaming_jar(self.tmp_dir), None)

        jar_dir = os.path.join(self.tmp_dir, 'a', 'b', 'c')
        os.makedirs(jar_dir)
        empty_dir = os.path.join(self.tmp_dir, 'empty')
        os.makedirs(empty_dir)

        # not just any jar will do
        mason_jar_path = os.path.join(jar_dir, 'mason.jar')
        open(mason_jar_path, 'w').close()
        assert_equal(find_hadoop_streaming_jar(self.tmp_dir), None)

        # should match streaming jar
        streaming_jar_path = os.path.join(
            jar_dir, 'hadoop-0.20.2-streaming.jar')
        open(streaming_jar_path, 'w').close()
        assert_equal(find_hadoop_streaming_jar(self.tmp_dir),
                     streaming_jar_path)

        # shouldn't find anything if we look in the wrong dir
        assert_equal(find_hadoop_streaming_jar(empty_dir), None)
        
class MockHadoopTestCase(TestCase):

    @setup
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
                
    @teardown
    def delete_hadoop_home_and_restore_environment_vars(self):
        mock_hdfs_root = os.environ['MOCK_HDFS_ROOT']
        mock_output_dir = os.environ['MOCK_HADOOP_OUTPUT']

        os.environ.clear()
        os.environ.update(self._old_environ)

        shutil.rmtree(mock_hdfs_root)
        shutil.rmtree(mock_output_dir)

class HadoopJobRunnerEndToEndTestCase(MockHadoopTestCase):

    @setup
    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    @teardown
    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_end_to_end(self):
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
                               '--no-conf',
                               '-', local_input_path, remote_input_path])
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
            assert_equal(sorted(os.listdir(hdfs_root)), ['data', 'user'])
            home_dir = os.path.join(hdfs_root, 'user', os.environ['USER'])
            assert_equal(os.listdir(home_dir), ['tmp'])
            assert_equal(os.listdir(os.path.join(home_dir, 'tmp')), ['mrjob'])

        assert_equal(sorted(results),
                     [(1, 'qux'), (2, 'bar'), (2, 'foo'), (5, None)])

        # make sure cleanup happens
        assert not os.path.exists(local_tmp_dir)
        assert not any(runner.ls(runner.get_output_dir()))

