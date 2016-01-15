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
from mrjob.logs.log4j import _parse_hadoop_log4j_records
from mrjob.py2 import StringIO
from mrjob.util import log_to_stream

from tests.py2 import TestCase
from tests.quiet import no_handlers_for_logger


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
                    timestamp='15/12/11 13:26:07',
                    level='INFO',
                    logger='client.RMProxy',
                    thread=None,
                    message='Connecting to ResourceManager at /0.0.0.0:8032'),
                dict(
                    timestamp='15/12/11 13:26:08',
                    level='ERROR',
                    logger='streaming.StreamJob',
                    thread=None,
                    message=('Error Launching job :'
                             ' Output directory already exists'))
            ])

    def test_trailing_carriage_return(self):
        lines = StringIO('15/12/11 13:26:07 INFO client.RMProxy:'
                         ' Connecting to ResourceManager at /0.0.0.0:8032\r\n')
        self.assertEqual(
            list(_parse_hadoop_log4j_records(lines)), [
                dict(
                    timestamp='15/12/11 13:26:07',
                    level='INFO',
                    logger='client.RMProxy',
                    thread=None,
                    message='Connecting to ResourceManager at /0.0.0.0:8032')
            ])

    def test_thread(self):
        lines = StringIO(
            '2015-08-22 00:46:18,411 INFO amazon.emr.metrics.MetricsSaver'
            ' (main): Thread 1 created MetricsLockFreeSaver 1\n')

        self.assertEqual(
            list(_parse_hadoop_log4j_records(lines)), [
                dict(
                    timestamp='2015-08-22 00:46:18,411',
                    level='INFO',
                    logger='amazon.emr.metrics.MetricsSaver',
                    thread='main',
                    message='Thread 1 created MetricsLockFreeSaver 1')
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
                    timestamp='2015-08-22 00:47:35,323',
                    level='INFO',
                    logger='org.apache.hadoop.mapreduce.Job',
                    thread='main',
                    # strip \r's, no trailing \n
                    message=('Counters: 54\n'
                             '        File System Counters\n'
                             '                FILE: Number of bytes read=83'))
            ])

    def test_non_log_lines(self):
        lines = StringIO('foo\n'
                         'bar\n'
                         '15/12/11 13:26:08 ERROR streaming.StreamJob:'
                         ' Error Launching job :'
                         ' Output directory already exists\n'
                         'Streaming Command Failed!')

        with no_handlers_for_logger('mrjob.logs.log4j'):
            stderr = StringIO()
            log_to_stream('mrjob.logs.log4j', stderr)

            self.assertEqual(
            list(_parse_hadoop_log4j_records(lines)), [
                # ignore leading non-log lines
                dict(
                    timestamp='15/12/11 13:26:08',
                    level='ERROR',
                    logger='streaming.StreamJob',
                    thread=None,
                    # no way to know that Streaming Command Failed! wasn't part
                    # of a multi-line message
                    message=('Error Launching job :'
                             ' Output directory already exists\n'
                             'Streaming Command Failed!'))
            ])

            # should be one warning for each leading non-log line
            log_lines = stderr.getvalue().splitlines()
            self.assertEqual(len(log_lines), 2)
