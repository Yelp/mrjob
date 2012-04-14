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
from StringIO import StringIO
import subprocess
from shutil import rmtree
from tempfile import mkdtemp

from mrjob.fs.hadoop import HadoopFilesystem
from mrjob.fs.local import LocalFilesystem
from mrjob.fs.multi import MultiFilesystem
from mrjob.fs import hadoop as fs_hadoop

from tests.fs import TempdirTestCase
from tests.mockhadoop import main as mock_hadoop_main


class HadoopFSTestCase(TempdirTestCase):

    def setUp(self):
        super(HadoopFSTestCase, self).setUp()
        self.fs = MultiFilesystem(HadoopFilesystem(['hadoop']),
                                  LocalFilesystem())
        self.mock_popen()

    def mock_popen(self):
        self.command_log = []
        self.io_log = []

        self.set_up_mock_hadoop()
        PopenClass = self.make_popen_class()

        original_popen = fs_hadoop.Popen
        fs_hadoop.Popen = PopenClass

        self.addCleanup(setattr, fs_hadoop, 'Popen', original_popen)

    def set_up_mock_hadoop(self):
        # setup fake hadoop home
        self.hadoop_env = {}
        self.hadoop_env['HADOOP_HOME'] = self.makedirs('mock_hadoop_home')

        self.makefile(
            os.path.join(
                'mock_hadoop_home',
                'contrib',
                'streaming',
                'hadoop-0.X.Y-streaming.jar'),
            'i are java bytecode',
        )

        self.hadoop_env['MOCK_HDFS_ROOT'] = self.makedirs('mock_hdfs_root')
        self.hadoop_env['MOCK_HADOOP_OUTPUT'] = self.makedirs(
                                                    'mock_hadoop_output')
        self.hadoop_env['USER'] = 'mrjob_tests'
        # don't set MOCK_HADOOP_LOG, we get command history other ways

    def make_popen_class(outer):

        class MockPopen(object):

            def __init__(self, args, stdin=None, stdout=None, stderr=None):
                self.args = args

                # discard incoming stdin/stdout/stderr objects
                self.stdin = StringIO()
                self.stdout = StringIO()
                self.stderr = StringIO()

                # pre-emptively run the "process"
                self.returncode = mock_hadoop_main(
                    self.stdout, self.stderr, self.args, outer.hadoop_env)

                # log what happened
                outer.command_log.append(self.args)
                outer.io_log.append((stdout, stderr))

                # store the result
                self.stdout_result, self.stderr_result = (
                    self.stdout.getvalue(), self.stderr.getvalue())

                # expose the results as readable file objects
                self.stdout = StringIO(self.stdout_result)
                self.stderr = StringIO(self.stderr_result)

            def communicate(self):
                return self.stdout_result, self.stderr_result

            def wait(self):
                return self.returncode

        return MockPopen

    def test_cat_uncompressed(self):
        # mockhadoop doesn't support compressed files, so we won't test for it.
        # this is only a sanity check anyway.
        path = self.makefile('f', 'bar\nfoo\n')
        self.assertEqual(list(self.fs.cat(path)), ['bar\n', 'foo\n'])

        self.makefile(os.path.join('mock_hdfs_root', 'data', 'foo'), 'foo\nfoo\n')
        remote_path = self.fs.path_join('hdfs:///data', 'foo')

        self.assertEqual(list(self.fs.cat(remote_path)), ['foo\n', 'foo\n'])

    def test_du(self):
        root = self.hadoop_env['MOCK_HDFS_ROOT']
        self.makefile(os.path.join('mock_hdfs_root', 'data1'), 'abcd')
        remote_data_1 = 'hdfs:///data1'

        remote_dir = self.makedirs('mock_hdfs_root/more')

        self.makefile(os.path.join('mock_hdfs_root', 'more', 'data2'), 'defg')
        remote_data_2 = 'hdfs:///more/data2'

        self.makefile(os.path.join('mock_hdfs_root', 'more', 'data3'), 'hijk')
        remote_data_3 = 'hdfs:///more/data3'

        self.assertEqual(self.fs.du(root), 12)
        self.assertEqual(self.fs.du(remote_dir), 8)
        self.assertEqual(self.fs.du(remote_dir + '/*'), 8)
        self.assertEqual(self.fs.du(remote_data_1), 4)
        self.assertEqual(self.fs.du(remote_data_2), 4)
