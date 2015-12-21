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
from mrjob.logs.parse import _parse_hadoop_log_lines
from mrjob.logs.parse import _parse_hadoop_streaming_log
from mrjob.logs.parse import _parse_indented_counters
from mrjob.logs.parse import _parse_yarn_task_syslog
from mrjob.py2 import StringIO
from mrjob.util import log_to_stream

from tests.py2 import TestCase
from tests.quiet import no_handlers_for_logger


class ParseHadoopLogLinesTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(list(_parse_hadoop_log_lines([])), [])

    def test_log_lines(self):
        lines = StringIO('15/12/11 13:26:07 INFO client.RMProxy:'
                         ' Connecting to ResourceManager at /0.0.0.0:8032\n'
                         '15/12/11 13:26:08 ERROR streaming.StreamJob:'
                         ' Error Launching job :'
                         ' Output directory already exists\n')
        self.assertEqual(
            list(_parse_hadoop_log_lines(lines)), [
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
            list(_parse_hadoop_log_lines(lines)), [
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
            list(_parse_hadoop_log_lines(lines)), [
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
            list(_parse_hadoop_log_lines(lines)), [
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

        with no_handlers_for_logger('mrjob.logs.parse'):
            stderr = StringIO()
            log_to_stream('mrjob.logs.parse', stderr)

            self.assertEqual(
            list(_parse_hadoop_log_lines(lines)), [
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


class ParseHadoopStreamingLogTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(
            _parse_hadoop_streaming_log([]),
            dict(application_id=None,
                 counters=None,
                 job_id=None,
                 output_dir=None))

    def test_yarn_output(self):
        # abbreviated version of real output from Hadoop 2.7.0.
        # Including things that might be interesting to parse later on
        lines = StringIO(
            '15/12/11 13:32:44 INFO client.RMProxy:'
            ' Connecting to ResourceManager at /0.0.0.0:8032\n'
            '15/12/11 13:32:45 INFO mapreduce.JobSubmitter:'
            ' Submitting tokens for job: job_1449857544442_0002\n'
            '15/12/11 13:32:45 INFO impl.YarnClientImpl:'
            ' Submitted application application_1449857544442_0002\n'
            '15/12/11 13:32:45 INFO mapreduce.Job:'
            ' The url to track the job:'
            ' http://0a7802e19139:8088/proxy/application_1449857544442_0002/\n'
            '15/12/11 13:33:11 INFO mapreduce.Job:  map 100% reduce 100%\n'
            '15/12/11 13:33:11 INFO mapreduce.Job:'
            ' Job job_1449857544442_0002 completed successfully\n'
            '15/12/11 13:33:11 INFO mapreduce.Job: Counters: 49\n'
            '        File System Counters\n'
            '                FILE: Number of bytes read=86\n'
            '15/12/11 13:33:11 INFO streaming.StreamJob:'
            ' Output directory:'
            ' hdfs:///user/root/tmp/mrjob/mr_wc.root.20151211.181326.984074'
            '/output\n')

        self.assertEqual(
            _parse_hadoop_streaming_log(lines),
            dict(application_id='application_1449857544442_0002',
                 counters={
                     'File System Counters': {
                         'FILE: Number of bytes read': 86,
                     }
                 },
                 job_id='job_1449857544442_0002',
                 output_dir=('hdfs:///user/root/tmp/mrjob'
                             '/mr_wc.root.20151211.181326.984074/output')))

    def test_pre_yarn_output(self):
        # actual output from Hadoop 1.0.3 on EMR AMI 2.4.9
        # Including things that might be interesting to parse later on
        lines = StringIO(
            '15/12/11 23:08:37 INFO streaming.StreamJob:'
            ' getLocalDirs(): [/mnt/var/lib/hadoop/mapred]\n'
            '15/12/11 23:08:37 INFO streaming.StreamJob:'
            ' Running job: job_201512112247_0003\n'
            '15/12/11 23:08:37 INFO streaming.StreamJob:'
            ' Tracking URL:'
            ' http://ip-172-31-27-129.us-west-2.compute.internal:9100'
            '/jobdetails.jsp?jobid=job_201512112247_0003\n'
            '15/12/11 23:09:16 INFO streaming.StreamJob:'
            '  map 100%  reduce 100%\n'
            '15/12/11 23:09:22 INFO streaming.StreamJob:'
            ' Output: hdfs:///user/hadoop/tmp/mrjob'
            '/mr_wc.hadoop.20151211.230352.433691/output\n')

        self.assertEqual(
            _parse_hadoop_streaming_log(lines),
            dict(application_id=None,
                 counters=None,
                 job_id='job_201512112247_0003',
                 output_dir=('hdfs:///user/hadoop/tmp/mrjob'
                             '/mr_wc.hadoop.20151211.230352.433691/output')))


class ParseIndentedCountersTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(_parse_indented_counters([]), {})

    def test_without_header(self):
        lines = [
            '  File System Counters',
            '    FILE: Number of bytes read=86',
            '    FILE: Number of bytes written=359982',
            '  Job Counters',
            '    Launched map tasks=2',
        ]

        self.assertEqual(_parse_indented_counters(lines), {
            'File System Counters': {
                'FILE: Number of bytes read': 86,
                'FILE: Number of bytes written': 359982,
            },
            'Job Counters': {
                'Launched map tasks': 2,
            },
        })

    def test_with_header(self):
        lines = [
            'Counters: 1',
            '  File System Counters',
            '    FILE: Number of bytes read=86',
        ]

        with no_handlers_for_logger('mrjob.logs.parse'):
            stderr = StringIO()
            log_to_stream('mrjob.logs.parse', stderr)

            self.assertEqual(_parse_indented_counters(lines), {
                'File System Counters': {
                    'FILE: Number of bytes read': 86,
                },
            })

            # header shouldn't freak it out
            self.assertEqual(stderr.getvalue(), '')

    def test_indentation_is_required(self):
        lines = [
            'File System Counters',
            '   FILE: Number of bytes read=8',
        ]

        with no_handlers_for_logger('mrjob.logs.parse'):
            stderr = StringIO()
            log_to_stream('mrjob.logs.parse', stderr)

            # counter line is interpreted as group
            self.assertEqual(_parse_indented_counters(lines), {})

            # should complain
            self.assertNotEqual(stderr.getvalue(), '')

    def test_no_empty_groups(self):
        lines = [
            '  File System Counters',
            '  Job Counters',
            '    Launched map tasks=2',
        ]

        self.assertEqual(_parse_indented_counters(lines), {
            'Job Counters': {
                'Launched map tasks': 2,
            },
        })


class ParseYARNTaskSyslogTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(_parse_yarn_task_syslog([]),
                         dict(error=None, split=None))
