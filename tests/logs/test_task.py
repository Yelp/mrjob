# -*- encoding: utf-8 -*-
# Copyright 2015 Yelp
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
from tests.py2 import TestCase

from mrjob.logs.task import _parse_task_syslog
from mrjob.logs.task import _parse_task_stderr



class ParseTaskSyslogTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(_parse_task_syslog([]), {})

    def test_split(self):
        lines = [
            '2015-12-21 14:06:17,707 INFO [main]'
            ' org.apache.hadoop.mapred.MapTask: Processing split:'
            ' hdfs://e4270474c8ee:9000/user/root/tmp/mrjob'
            '/mr_boom.root.20151221.190511.059097/files/bootstrap.sh:0+335\n',
        ]

        self.assertEqual(
            _parse_task_syslog(lines),
            dict(
                split=dict(
                    path=('hdfs://e4270474c8ee:9000/user/root/tmp/mrjob'
                          '/mr_boom.root.20151221.190511.059097/files'
                          '/bootstrap.sh'),
                    start_line=0,
                    num_lines=335)))

    def test_opening_file(self):
        lines = [
            '2010-07-27 17:54:54,344 INFO'
            ' org.apache.hadoop.fs.s3native.NativeS3FileSystem (main):'
            " Opening 's3://yourbucket/logs/2010/07/23/log2-00077.gz'"
            ' for reading\n'
        ]

        self.assertEqual(
            _parse_task_syslog(lines),
            dict(
                split=dict(
                    path='s3://yourbucket/logs/2010/07/23/log2-00077.gz')))

    def test_yarn_error(self):
        lines = [
            '2015-12-21 14:06:18,538 WARN [main]'
            ' org.apache.hadoop.mapred.YarnChild: Exception running child'
            ' : java.lang.RuntimeException: PipeMapRed.waitOutputThreads():'
            ' subprocess failed with code 1\n',
            '        at org.apache.hadoop.streaming.PipeMapRed'
            '.waitOutputThreads(PipeMapRed.java:322)\n',
            '        at org.apache.hadoop.streaming.PipeMapRed'
            '.mapRedFinished(PipeMapRed.java:535)\n',
        ]

        self.assertEqual(
            _parse_task_syslog(lines),
            dict(
                hadoop_error=dict(
                    message=(
                        'Exception running child : java.lang.RuntimeException:'
                        ' PipeMapRed.waitOutputThreads():'
                        ' subprocess failed with code 1\n'
                        '        at org.apache.hadoop.streaming.PipeMapRed'
                        '.waitOutputThreads(PipeMapRed.java:322)\n'
                        '        at org.apache.hadoop.streaming.PipeMapRed'
                        '.mapRedFinished(PipeMapRed.java:535)'),
                    num_lines=3,
                    start_line=0,
                )
            ))

    def test_pre_yarn_error(self):
        lines = [
            '2015-12-30 19:21:39,980 WARN'
            ' org.apache.hadoop.mapred.Child (main): Error running child\n',
            'java.lang.RuntimeException: PipeMapRed.waitOutputThreads():'
            ' subprocess failed with code 1\n',
            '        at org.apache.hadoop.streaming.PipeMapRed'
            '.waitOutputThreads(PipeMapRed.java:372)\n',
        ]

        self.assertEqual(
            _parse_task_syslog(lines),
            dict(
                hadoop_error=dict(
                    message=(
                        'Error running child\n'
                        'java.lang.RuntimeException:'
                        ' PipeMapRed.waitOutputThreads():'
                        ' subprocess failed with code 1\n'
                        '        at org.apache.hadoop.streaming.PipeMapRed'
                        '.waitOutputThreads(PipeMapRed.java:372)'),
                    num_lines=3,
                    start_line=0,
                )))


class ParseTaskStderrTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(_parse_task_stderr([]), None)

    def test_python_exception(self):
        lines = [
            '+ python mr_boom.py --step-num=0 --mapper\n',
            'Traceback (most recent call last):\n',
            '  File "mr_boom.py", line 10, in <module>\n',
            '    MRBoom.run()\n',
            'Exception: BOOM\n',
        ]

        self.assertEqual(
            _parse_task_stderr(lines),
            dict(
                message=''.join(lines).rstrip('\n'),
                start_line=0,
                num_lines=5,
            ))

    def test_setup_command_error(self):
        lines = [
            '+ __mrjob_PWD=/mnt/var/lib/hadoop/tmp/nm-local-dir/usercache'
            '/hadoop/appcache/application_1453488173054_0002'
            '/container_1453488173054_0002_01_000005\n',
            '+ exec\n',
            "+ python3 -c 'import fcntl; fcntl.flock(9, fcntl.LOCK_EX)\n",
            '+ export PYTHONPATH=/mnt/var/lib/hadoop/tmp/nm-local-dir'
            '/usercache/hadoop/appcache/application_1453488173054_0002'
            '/container_1453488173054_0002_01_000005/mrjob.tar.gz:\n',
            '+ PYTHONPATH=/mnt/var/lib/hadoop/tmp/nm-local-dir/usercache'
            '/hadoop/appcache/application_1453488173054_0002'
            '/container_1453488173054_0002_01_000005/mrjob.tar.gz:\n',
            '+ rm /\n',
            'rm: cannot remove ‘/’: Is a directory\n',
        ]

        self.assertEqual(
            _parse_task_stderr(lines),
            dict(
                message='+ rm /\nrm: cannot remove ‘/’: Is a directory',
                start_line=5,
                num_lines=2,
            ))

    def test_strip_carriage_return(self):
        lines = [
            '+ rm /\r\n',
            'rm: cannot remove ‘/’: Is a directory\r\n',
        ]

        self.assertEqual(
            _parse_task_stderr(lines),
            dict(
                message='+ rm /\nrm: cannot remove ‘/’: Is a directory',
                start_line=0,
                num_lines=2,
            ))

    def test_silent_bad_actor(self):
        lines = [
            '+ false\n',
        ]

        self.assertEqual(
            _parse_task_stderr(lines),
            dict(
                message='+ false',
                start_line=0,
                num_lines=1,
            ))

    def test_error_without_leading_plus(self):
        lines = [
            'ERROR: something is terribly, terribly wrong\n',
        ]

        self.assertEqual(
            _parse_task_stderr(lines), None)
