# -*- encoding: utf-8 -*-
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

from mrjob.logs.history import _interpret_history_log
from mrjob.logs.history import _match_history_log_path
from mrjob.logs.history import _parse_pre_yarn_history_log
from mrjob.logs.history import _parse_pre_yarn_history_records
from mrjob.logs.history import _parse_pre_yarn_counters
from mrjob.logs.history import _parse_yarn_history_log

from tests.sandbox import PatcherTestCase
from tests.py2 import Mock
from tests.py2 import patch


# path matching
class MatchHistoryLogTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(_match_history_log_path(''), None)

    def test_pre_yarn(self):
        history_path = (
            '/logs/history/done/version-1/host_1451590133273_/2015/12/31'
            '/000000/job_201512311928_0001_1451590317008_hadoop'
            '_streamjob8025762403845318969.jar')

        self.assertEqual(
            _match_history_log_path(history_path),
            dict(job_id='job_201512311928_0001', yarn=False))

        conf_path = (
            '/logs/history/done/version-1/host_1451590133273_/2015/12/31'
            '/000000/job_201512311928_0001_conf.xml')

        self.assertEqual(
            _match_history_log_path(conf_path),
            None)

    def test_pre_yarn_filter_by_job_id(self):
        history_path = (
            '/logs/history/done/version-1/host_1451590133273_/2015/12/31'
            '/000000/job_201512311928_0001_1451590317008_hadoop'
            '_streamjob8025762403845318969.jar')

        self.assertEqual(
            _match_history_log_path(
                history_path, job_id='job_201512311928_0001'),
            dict(job_id='job_201512311928_0001', yarn=False))

        self.assertEqual(
            _match_history_log_path(
                history_path, job_id='job_201512311928_0002'),
            None)

    def test_yarn(self):
        history_path = (
            'hdfs:///tmp/hadoop-yarn/staging/history/done/2015/12/31/000000/'
            'job_1451592123989_0001-1451592605470-hadoop-QuasiMonteCarlo'
            '-1451592786882-10-1-SUCCEEDED-default-1451592631082.jhist')

        self.assertEqual(
            _match_history_log_path(history_path),
            dict(job_id='job_1451592123989_0001', yarn=True))

        conf_path = (
            'hdfs:///tmp/hadoop-yarn/staging/history/done/2015/12/31/000000/'
            'job_1451592123989_0001_conf.xml')

        self.assertEqual(
            _match_history_log_path(conf_path),
            None)

    def test_yarn_filter_by_job_id(self):
        history_path = (
            'hdfs:///tmp/hadoop-yarn/staging/history/done/2015/12/31/000000/'
            'job_1451592123989_0001-1451592605470-hadoop-QuasiMonteCarlo'
            '-1451592786882-10-1-SUCCEEDED-default-1451592631082.jhist')

        self.assertEqual(
            _match_history_log_path(
                history_path, job_id='job_1451592123989_0001'),
            dict(job_id='job_1451592123989_0001', yarn=True))

        self.assertEqual(
            _match_history_log_path(
                history_path, job_id='job_1451592123989_0002'),
            None)


class InterpretHistoryLogTestCase(PatcherTestCase):

    def setUp(self):
        super(InterpretHistoryLogTestCase, self).setUp()

        self.mock_fs = Mock()

        # don't include errors in return value, as they get patched
        mock_return_value = dict(
            counters={'foo': {'bar': 42}},
            errors=[])

        self.mock_parse_yarn_history_log = self.start(
            patch('mrjob.logs.history._parse_yarn_history_log',
                  return_value=mock_return_value))

        self.mock_parse_pre_yarn_history_log = self.start(
            patch('mrjob.logs.history._parse_pre_yarn_history_log',
                  return_value=mock_return_value))

        self.mock_cat_log = self.start(patch('mrjob.logs.history._cat_log'))

    def interpret_history_log(self, matches):
        """Wrap _interpret_history_log(), since fs doesn't matter."""
        return _interpret_history_log(self.mock_fs, matches)

    def test_empty(self):
        self.assertEqual(self.interpret_history_log([]), {})

    def test_pre_yarn(self):
        self.assertEqual(
            self.interpret_history_log(
                [dict(path='/path/to/pre-yarn-history.jar', yarn=False)]),
            self.mock_parse_pre_yarn_history_log.return_value)

        self.mock_cat_log.called_once_with(
            self.mock_fs, '/path/to/pre-yarn-history.jar')

        self.assertEqual(self.mock_parse_pre_yarn_history_log.call_count, 1)

    def test_yarn(self):
        self.assertEqual(
            self.interpret_history_log(
                [dict(path='/path/to/yarn-history.jhist', yarn=True)]),
            self.mock_parse_yarn_history_log.return_value)

        self.mock_cat_log.called_once_with(
            self.mock_fs, '/path/to/yarn-history.jhist')

        self.assertEqual(self.mock_parse_yarn_history_log.call_count, 1)

    # code works the same way for YARN and pre-YARN, so testing on YARN
    # from here on out

    def test_ignore_multiple_matches(self):
        self.assertEqual(
            self.interpret_history_log(
                [dict(path='/path/to/yarn-history-1.jhist', yarn=True),
                 dict(path='/path/to/yarn-history-2.jhist', yarn=True)]),
            self.mock_parse_yarn_history_log.return_value)

        self.mock_cat_log.called_once_with(
            self.mock_fs, '/path/to/yarn-history-1.jhist')

        self.assertEqual(self.mock_parse_yarn_history_log.call_count, 1)

    def test_patch_errors(self):
        self.mock_parse_yarn_history_log.return_value = dict(
            errors=[
                dict(attempt_id='attempt_1449525218032_0005_m_000000_0'),
                dict(hadoop_error=dict()),
            ])

        self.assertEqual(
            self.interpret_history_log(
                [dict(path='/path/to/yarn-history.jhist', yarn=True)]),
            dict(errors=[
                dict(
                    attempt_id=(
                        'attempt_1449525218032_0005_m_000000_0'),
                    task_id='task_1449525218032_0005_m_000000'),
                dict(
                    hadoop_error=dict(
                        path='/path/to/yarn-history.jhist')),
            ]))


# log parsing
class ParseYARNHistoryLogTestCase(TestCase):

    JOB_COUNTER_LINES = [
        '{"type":"JOB_FINISHED","event":{'
        '"org.apache.hadoop.mapreduce.jobhistory.JobFinished":{'
        '"jobid":"job_1452815622929_0001","totalCounters":{'
        '"name":"TOTAL_COUNTERS","groups":[{'
        '"name":"org.apache.hadoop.mapreduce.FileSystemCounter",'
        '"displayName":"File System Counters","counts":[{'
        '"name":"FILE_BYTES_READ","displayName":'
        '"FILE: Number of bytes read","value":0},{"name":'
        '"HDFS_BYTES_READ","displayName":"HDFS: Number of bytes read",'
        '"value":588}]}]}}}}\n',
    ]

    TASK_COUNTER_LINES = [
        '{"type":"TASK_FINISHED","event":{'
        '"org.apache.hadoop.mapreduce.jobhistory.TaskFinished":{'
        '"taskid":"task_1452815622929_0001_m_000001","taskType":"MAP",'
        '"status":"SUCCEEDED","counters":{"name":"COUNTERS","groups":[{'
        '"name":"org.apache.hadoop.mapreduce.FileSystemCounter",'
        '"displayName":"File System Counters","counts":[{"name":'
        '"FILE_BYTES_READ","displayName":"FILE: Number of bytes read",'
        '"value":0},{"name":"FILE_BYTES_WRITTEN","displayName":'
        '"FILE: Number of bytes written","value":102090}]}]}}}}\n',
        '{"type":"TASK_FINISHED","event":{'
        '"org.apache.hadoop.mapreduce.jobhistory.TaskFinished":{'
        '"taskid":"task_1452815622929_0001_m_000002","taskType":"MAP",'
        '"status":"SUCCEEDED","counters":{"name":"COUNTERS","groups":[{'
        '"name":"org.apache.hadoop.mapreduce.FileSystemCounter",'
        '"displayName":"File System Counters","counts":[{"name":'
        '"FILE_BYTES_WRITTEN","displayName":'
        '"FILE: Number of bytes written","value":1}]}]}}}}\n',
    ]

    def test_empty(self):
        self.assertEqual(_parse_yarn_history_log([]), {})

    def handle_non_json(self):
        lines = [
            'Avro-Json\n',
            '\n',
            'BLARG\n',
            '{not JSON\n',
        ]

        self.assertEqual(_parse_yarn_history_log(lines), {})

    def test_job_counters(self):
        self.assertEqual(
            _parse_yarn_history_log(self.JOB_COUNTER_LINES),
            dict(
                counters={
                    'File System Counters': {
                        'FILE: Number of bytes read': 0,
                        'HDFS: Number of bytes read': 588,
                    }
                },
            ))

    def test_task_counters(self):
        self.assertEqual(
            _parse_yarn_history_log(self.TASK_COUNTER_LINES),
            dict(
                counters={
                    'File System Counters': {
                        'FILE: Number of bytes read': 0,
                        'FILE: Number of bytes written': 102091,
                    }
                }
            ))

    def test_job_counters_beat_task_counters(self):
        self.assertEqual(
            _parse_yarn_history_log(self.JOB_COUNTER_LINES +
                                    self.TASK_COUNTER_LINES),
            dict(
                counters={
                    'File System Counters': {
                        'FILE: Number of bytes read': 0,
                        'HDFS: Number of bytes read': 588,
                    }
                },
            ))

    def test_errors(self):
        lines = [
            '{"type":"MAP_ATTEMPT_FAILED","event":{'
            '"org.apache.hadoop.mapreduce.jobhistory'
            '.TaskAttemptUnsuccessfulCompletion":{"taskid":'
            '"task_1449525218032_0005_m_000000","taskType":"MAP",'
            '"attemptId":"attempt_1449525218032_0005_m_000000_0",'
            '"status":"FAILED","error":'
            '"Error: java.lang.RuntimeException: PipeMapRed'
            '.waitOutputThreads(): subprocess failed with code 1\\n'
            '\\tat org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads('
            'PipeMapRed.java:322)\\n"}}}\n',
            '{"type":"MAP_ATTEMPT_FAILED","event":{'
            '"org.apache.hadoop.mapreduce.jobhistory'
            '.TaskAttemptUnsuccessfulCompletion":{"error":'
            '"Error: java.lang.RuntimeException: PipeMapRed'
            '.waitOutputThreads(): subprocess failed with code 1\\n"}}}\n',
        ]

        self.assertEqual(
            _parse_yarn_history_log(lines),
            dict(
                errors=[
                    dict(
                        hadoop_error=dict(
                            message=(
                                'Error: java.lang.RuntimeException: PipeMapRed'
                                '.waitOutputThreads(): subprocess failed with'
                                ' code 1\n\tat org.apache.hadoop.streaming'
                                '.PipeMapRed.waitOutputThreads('
                                'PipeMapRed.java:322)\n'),
                            start_line=0,
                            num_lines=1,
                        ),
                        task_id='task_1449525218032_0005_m_000000',
                        attempt_id='attempt_1449525218032_0005_m_000000_0',
                    ),
                    dict(
                        hadoop_error=dict(
                            message=(
                                'Error: java.lang.RuntimeException: PipeMapRed'
                                '.waitOutputThreads(): subprocess failed with'
                                ' code 1\n'),
                            start_line=1,
                            num_lines=1,
                        )
                    ),
                ]))


class ParsePreYARNHistoryLogTestCase(TestCase):
    JOB_COUNTER_LINES = [
        'Job JOBID="job_201106092314_0003" FINISH_TIME="1307662284564"'
        ' JOB_STATUS="SUCCESS" FINISHED_MAPS="2" FINISHED_REDUCES="1"'
        ' FAILED_MAPS="0" FAILED_REDUCES="0" COUNTERS="'
        '{(org\.apache\.hadoop\.mapred\.JobInProgress$Counter)'
        '(Job Counters )'
        '[(TOTAL_LAUNCHED_REDUCES)(Launched reduce tasks)(1)]}" .\n',
    ]

    TASK_COUNTER_LINES = [
        'Task TASKID="task_201601081945_0005_m_000005" TASK_TYPE="SETUP"'
        ' TASK_STATUS="SUCCESS" FINISH_TIME="1452283612363"'
        ' COUNTERS="{(FileSystemCounters)(FileSystemCounters)'
        '[(FILE_BYTES_WRITTEN)(FILE_BYTES_WRITTEN)(27785)]}" .\n',
        'Task TASKID="task_201601081945_0005_m_000000" TASK_TYPE="MAP"'
        ' TASK_STATUS="SUCCESS" FINISH_TIME="1452283651437"'
        ' COUNTERS="{'
        '(org\.apache\.hadoop\.mapred\.FileOutputFormat$Counter)'
        '(File Output Format Counters )'
        '[(BYTES_WRITTEN)(Bytes Written)(0)]}'
        '{(FileSystemCounters)(FileSystemCounters)'
        '[(FILE_BYTES_WRITTEN)(FILE_BYTES_WRITTEN)(27785)]'
        '[(HDFS_BYTES_READ)(HDFS_BYTES_READ)(248)]}" .\n',
    ]

    def test_empty(self):
        self.assertEqual(_parse_pre_yarn_history_log([]), {})

    def test_job_counters(self):
        self.assertEqual(
            _parse_pre_yarn_history_log(self.JOB_COUNTER_LINES),
            dict(counters={'Job Counters ': {'Launched reduce tasks': 1}}))

    def test_task_counters(self):
        self.assertEqual(
            _parse_pre_yarn_history_log(self.TASK_COUNTER_LINES),
            dict(
                counters={
                    'FileSystemCounters': {
                        'FILE_BYTES_WRITTEN': 55570,
                        'HDFS_BYTES_READ': 248,
                    },
                    'File Output Format Counters ': {
                        'Bytes Written': 0,
                    },
                }
            )
        )

    def test_job_counters_beat_task_counters(self):
        self.assertEqual(
            _parse_pre_yarn_history_log(self.JOB_COUNTER_LINES +
                                        self.TASK_COUNTER_LINES),
            dict(counters={'Job Counters ': {'Launched reduce tasks': 1}}))

    def test_errors(self):
        lines = [
            'MapAttempt TASK_TYPE="MAP"'
            ' TASKID="task_201601081945_0005_m_000001"'
            ' TASK_ATTEMPT_ID='
            '"attempt_201601081945_0005_m_00000_2"'
            ' TASK_STATUS="FAILED"'
            ' ERROR="java\.lang\.RuntimeException:'
            ' PipeMapRed\.waitOutputThreads():'
            ' subprocess failed with code 1\n',
            '        at org\\.apache\\.hadoop\\.streaming\\.PipeMapRed'
            '\\.waitOutputThreads(PipeMapRed\\.java:372)\n',
            '        at org\\.apache\\.hadoop\\.streaming\\.PipeMapRed'
            '\\.mapRedFinished(PipeMapRed\\.java:586)\n',
            '" .\n',
        ]

        self.assertEqual(
            _parse_pre_yarn_history_log(lines),
            dict(
                errors=[
                    dict(
                        hadoop_error=dict(
                            message=(
                                'java.lang.RuntimeException: PipeMapRed'
                                '.waitOutputThreads():'
                                ' subprocess failed with code 1\n'
                                '        at org.apache.hadoop.streaming'
                                '.PipeMapRed.waitOutputThreads'
                                '(PipeMapRed.java:372)\n'
                                '        at org.apache.hadoop.streaming'
                                '.PipeMapRed.mapRedFinished'
                                '(PipeMapRed.java:586)\n'),
                            num_lines=4,
                            start_line=0,
                        ),
                        attempt_id='attempt_201601081945_0005_m_00000_2',
                    ),
                ]))

    def test_ignore_killed_task_with_empty_error(self):
        # regression test for #1288
        lines = [
            'MapAttempt TASK_TYPE="MAP"'
            ' TASKID="task_201603252302_0001_m_000003"'
            ' TASK_ATTEMPT_ID="attempt_201603252302_0001_m_000003_3"'
            ' TASK_STATUS="KILLED" FINISH_TIME="1458947137998"'
            ' HOSTNAME="172\.31\.18\.180" ERROR="" .\n',
        ]

        self.assertEqual(_parse_pre_yarn_history_log(lines), {})

    def test_ignore_killed_task(self):
        # not sure if this actually happens, but just to be safe
        lines = [
            'MapAttempt TASK_TYPE="MAP"'
            ' TASKID="task_201601081945_0005_m_000001"'
            ' TASK_ATTEMPT_ID='
            '"attempt_201601081945_0005_m_00000_2"'
            ' TASK_STATUS="KILLED"'
            ' ERROR="java\.lang\.RuntimeException:'
            ' PipeMapRed\.waitOutputThreads():'
            ' subprocess failed with code 1\n',
            '        at org\\.apache\\.hadoop\\.streaming\\.PipeMapRed'
            '\\.waitOutputThreads(PipeMapRed\\.java:372)\n',
            '        at org\\.apache\\.hadoop\\.streaming\\.PipeMapRed'
            '\\.mapRedFinished(PipeMapRed\\.java:586)\n',
            '" .\n',
        ]

        self.assertEqual(_parse_pre_yarn_history_log(lines), {})

    def test_ignore_blank_error(self):
        lines = [
            'MapAttempt TASK_TYPE="MAP"'
            ' TASKID="task_201601081945_0005_m_000001"'
            ' TASK_ATTEMPT_ID='
            '"attempt_201601081945_0005_m_00000_2"'
            ' TASK_STATUS="FAILED"'
            ' ERROR="" .\n',
        ]

        self.assertEqual(_parse_pre_yarn_history_log(lines), {})


# edge cases in pre-YARN history record parsing
class ParsePreYARNHistoryRecordsTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(list(_parse_pre_yarn_history_records([])), [])

    def test_basic(self):
        lines = [
            'Meta VERSION="1" .\n',
            'Job JOBID="job_201601081945_0005" JOB_PRIORITY="NORMAL" .\n',
        ]
        self.assertEqual(
            list(_parse_pre_yarn_history_records(lines)),
            [
                dict(
                    fields=dict(
                        VERSION='1'
                    ),
                    start_line=0,
                    num_lines=1,
                    type='Meta',
                ),
                dict(
                    fields=dict(
                        JOBID='job_201601081945_0005',
                        JOB_PRIORITY='NORMAL'
                    ),
                    num_lines=1,
                    start_line=1,
                    type='Job',
                )
            ])

    def test_unescape(self):
        lines = [
            'Task TASKID="task_201512311928_0001_m_000003" TASK_TYPE="MAP"'
            ' START_TIME="1451590341378"'
            ' SPLITS="/default-rack/172\\.31\\.22\\.226" .\n',
        ]

        self.assertEqual(
            list(_parse_pre_yarn_history_records(lines)),
            [
                dict(
                    fields=dict(
                        TASKID='task_201512311928_0001_m_000003',
                        TASK_TYPE='MAP',
                        START_TIME='1451590341378',
                        SPLITS='/default-rack/172.31.22.226',
                    ),
                    num_lines=1,
                    start_line=0,
                    type='Task',
                ),
            ])

    def test_multiline(self):
        lines = [
            'MapAttempt TASK_TYPE="MAP"'
            ' TASKID="task_201601081945_0005_m_000001"'
            ' TASK_STATUS="FAILED"'
            ' ERROR="java\.lang\.RuntimeException:'
            ' PipeMapRed\.waitOutputThreads():'
            ' subprocess failed with code 1\n',
            '        at org\\.apache\\.hadoop\\.streaming\\.PipeMapRed'
            '\\.waitOutputThreads(PipeMapRed\\.java:372)\n',
            '        at org\\.apache\\.hadoop\\.streaming\\.PipeMapRed'
            '\\.mapRedFinished(PipeMapRed\\.java:586)\n',
            '" .\n',
        ]

        self.assertEqual(
            list(_parse_pre_yarn_history_records(lines)),
            [
                dict(
                    fields=dict(
                        ERROR=(
                            'java.lang.RuntimeException: PipeMapRed'
                            '.waitOutputThreads():'
                            ' subprocess failed with code 1\n'
                            '        at org.apache.hadoop.streaming.PipeMapRed'
                            '.waitOutputThreads(PipeMapRed.java:372)\n'
                            '        at org.apache.hadoop.streaming.PipeMapRed'
                            '.mapRedFinished(PipeMapRed.java:586)\n'),
                        TASK_TYPE='MAP',
                        TASKID='task_201601081945_0005_m_000001',
                        TASK_STATUS='FAILED',
                    ),
                    num_lines=4,
                    start_line=0,
                    type='MapAttempt',
                ),
            ])

    def test_bad_records(self):
        # should just silently ignore bad records and yield good ones
        lines = [
            '\n',
            'Foo BAZ .\n',
            'Job JOBID="job_201601081945_0005" JOB_PRIORITY="NORMAL" .\n',
            'Job JOBID="\n',
        ]

        self.assertEqual(
            list(_parse_pre_yarn_history_records(lines)),
            [
                dict(
                    fields=dict(
                        JOBID='job_201601081945_0005',
                        JOB_PRIORITY='NORMAL'
                    ),
                    num_lines=1,
                    start_line=2,
                    type='Job',
                )
            ])


# edge cases in pre-YARN counter parsing
class ParsePreYARNCountersTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(_parse_pre_yarn_counters(''), {})

    def test_basic(self):
        counter_str = (
            '{(org.apache.hadoop.mapred.JobInProgress$Counter)'
            '(Job Counters )'
            '[(TOTAL_LAUNCHED_REDUCES)(Launched reduce tasks)(1)]'
            '[(TOTAL_LAUNCHED_MAPS)(Launched map tasks)(2)]}'
            '{(FileSystemCounters)(FileSystemCounters)'
            '[(FILE_BYTES_READ)(FILE_BYTES_READ)(10547174)]}')

        self.assertEqual(
            _parse_pre_yarn_counters(counter_str), {
                'Job Counters ': {
                    'Launched reduce tasks': 1,
                    'Launched map tasks': 2,
                },
                'FileSystemCounters': {
                    'FILE_BYTES_READ': 10547174,
                },
            })

    def test_escape_sequences(self):
        counter_str = (
            r'{(\)\(\)\(\)\})(\)\(\)\(\)\})'
            r'[(\\)(\\)(1)]'
            r'[(\[\])(\[\])(2)]'
            r'[(\{\})(\{\})(3)]'
            r'[(\(\))(\(\))(4)]}')

        self.assertEqual(
            _parse_pre_yarn_counters(counter_str), {
                ')()()}': {
                    '\\': 1,
                    '[]': 2,
                    '{}': 3,
                    '()': 4,
                },
            })
