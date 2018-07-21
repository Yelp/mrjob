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
from unittest import TestCase

from mrjob.logs.log4j import _parse_hadoop_log4j_records
from mrjob.py2 import StringIO


class ParseHadoopLog4JRecordsCase(TestCase):

    def test_empty(self):
        self.assertEqual(list(_parse_hadoop_log4j_records([])), [])

    def test_log_lines(self):
        lines = StringIO('15/12/11 13:26:07 INFO client.RMProxy:'
                         ' Connecting to ResourceManager at /0.0.0.0:8032\n'
                         '15/12/11 13:26:08 ERROR streaming.StreamJob:'
                         ' Error Launching job :'
                         ' Output directory already exists\n')
        self.assertEqual(
            list(_parse_hadoop_log4j_records(lines)), [
                dict(
                    caller_location='',
                    level='INFO',
                    logger='client.RMProxy',
                    message='Connecting to ResourceManager at /0.0.0.0:8032',
                    num_lines=1,
                    start_line=0,
                    thread='',
                    timestamp='15/12/11 13:26:07',
                ),
                dict(
                    caller_location='',
                    level='ERROR',
                    logger='streaming.StreamJob',
                    message=('Error Launching job :'
                             ' Output directory already exists'),
                    num_lines=1,
                    start_line=1,
                    thread='',
                    timestamp='15/12/11 13:26:08',
                ),
            ])

    def test_trailing_carriage_return(self):
        lines = StringIO('15/12/11 13:26:07 INFO client.RMProxy:'
                         ' Connecting to ResourceManager at /0.0.0.0:8032\r\n')
        self.assertEqual(
            list(_parse_hadoop_log4j_records(lines)), [
                dict(
                    caller_location='',
                    level='INFO',
                    logger='client.RMProxy',
                    message='Connecting to ResourceManager at /0.0.0.0:8032',
                    num_lines=1,
                    start_line=0,
                    thread='',
                    timestamp='15/12/11 13:26:07',
                )
            ])

    def test_thread(self):
        lines = StringIO(
            '2015-08-22 00:46:18,411 INFO amazon.emr.metrics.MetricsSaver'
            ' (main): Thread 1 created MetricsLockFreeSaver 1\n')

        self.assertEqual(
            list(_parse_hadoop_log4j_records(lines)), [
                dict(
                    caller_location='',
                    level='INFO',
                    logger='amazon.emr.metrics.MetricsSaver',
                    message='Thread 1 created MetricsLockFreeSaver 1',
                    num_lines=1,
                    start_line=0,
                    thread='main',
                    timestamp='2015-08-22 00:46:18,411',
                )
            ])

    def test_caller_location(self):
        # tests fix for #1406
        lines = StringIO(
            '  2016-08-19 13:30:03,816 INFO  [main] impl.YarnClientImpl'
            ' (YarnClientImpl.java:submitApplication(251)) - Submitted'
            ' application application_1468316211405_1354')

        self.assertEqual(
            list(_parse_hadoop_log4j_records(lines)), [
                dict(
                    caller_location=(
                        'YarnClientImpl.java:submitApplication(251)'),
                    level='INFO',
                    logger='impl.YarnClientImpl',
                    message=('Submitted application'
                             ' application_1468316211405_1354'),
                    num_lines=1,
                    start_line=0,
                    thread='main',
                    timestamp='2016-08-19 13:30:03,816',
                )
            ])

    def test_multiline_message(self):
        lines = StringIO(
            '2015-08-22 00:47:35,323 INFO org.apache.hadoop.mapreduce.Job'
            ' (main): Counters: 54\r\n'
            '        File System Counters\r\n'
            '                FILE: Number of bytes read=83\r\n')

        self.assertEqual(
            list(_parse_hadoop_log4j_records(lines)), [
                dict(
                    caller_location='',
                    level='INFO',
                    logger='org.apache.hadoop.mapreduce.Job',
                    # strip \r's, no trailing \n
                    message=('Counters: 54\n'
                             '        File System Counters\n'
                             '                FILE: Number of bytes read=83'),
                    num_lines=3,
                    start_line=0,
                    thread='main',
                    timestamp='2015-08-22 00:47:35,323',
                )
            ])

    def test_non_log_lines(self):
        lines = StringIO('foo\n'
                         'bar\n'
                         '15/12/11 13:26:08 ERROR streaming.StreamJob:'
                         ' Error Launching job :'
                         ' Output directory already exists\n'
                         'Streaming Command Failed!')

        self.assertEqual(
            list(_parse_hadoop_log4j_records(lines)), [
                dict(
                    caller_location='',
                    level='',
                    logger='',
                    message='foo',
                    num_lines=1,
                    start_line=0,
                    thread='',
                    timestamp='',
                ),
                dict(
                    caller_location='',
                    level='',
                    logger='',
                    message='bar',
                    num_lines=1,
                    start_line=1,
                    thread='',
                    timestamp='',
                ),
                dict(
                    caller_location='',
                    level='ERROR',
                    logger='streaming.StreamJob',
                    # no way to know that Streaming Command Failed! wasn't part
                    # of a multi-line message
                    message=('Error Launching job :'
                             ' Output directory already exists\n'
                             'Streaming Command Failed!'),
                    num_lines=2,
                    start_line=2,
                    thread='',
                    timestamp='15/12/11 13:26:08',
                ),
            ])
