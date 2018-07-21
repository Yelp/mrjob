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
import errno
from unittest import TestCase

from mrjob.logs.step import _interpret_emr_step_syslog
from mrjob.logs.step import _interpret_emr_step_stderr
from mrjob.logs.step import _interpret_hadoop_jar_command_stderr
from mrjob.logs.step import _ls_emr_step_syslogs
from mrjob.logs.step import _ls_emr_step_stderr_logs
from mrjob.logs.step import _match_emr_step_syslog_path
from mrjob.logs.step import _match_emr_step_stderr_path
from mrjob.logs.step import _parse_indented_counters
from mrjob.logs.step import _parse_step_syslog
from mrjob.py2 import StringIO
from mrjob.util import log_to_stream

from tests.py2 import Mock
from tests.py2 import patch
from tests.quiet import no_handlers_for_logger
from tests.sandbox import PatcherTestCase


# abbreviated version of real output from Hadoop 2.7.0.
# Including things that might be interesting to parse later on
YARN_STEP_LOG_LINES = [
    '15/12/11 13:32:44 INFO client.RMProxy:'
    ' Connecting to ResourceManager at /0.0.0.0:8032\n',
    '15/12/11 13:32:45 INFO mapreduce.JobSubmitter:'
    ' Submitting tokens for job: job_1449857544442_0002\n',
    '15/12/11 13:32:45 INFO impl.YarnClientImpl:'
    ' Submitted application application_1449857544442_0002\n',
    '15/12/11 13:32:45 INFO mapreduce.Job:'
    ' The url to track the job:'
    ' http://0a7802e19139:8088/proxy/application_1449857544442_0002/\n',
    '15/12/11 13:33:11 INFO mapreduce.Job:'
    ' Running job: job_1449857544442_0002\n',
    '15/12/11 13:33:11 INFO mapreduce.Job:  map 100% reduce 100%\n',
    '15/12/11 13:33:11 INFO mapreduce.Job:'
    ' Job job_1449857544442_0002 completed successfully\n',
    '15/12/11 13:33:11 INFO mapreduce.Job: Counters: 49\n',
    '        File System Counters\n',
    '                FILE: Number of bytes read=86\n',
    '15/12/11 13:33:11 INFO streaming.StreamJob:'
    ' Output directory:'
    ' hdfs:///user/root/tmp/mrjob/mr_wc.root.20151211.181326.984074'
    '/output\n',
]

PARSED_YARN_STEP_LOG_LINES = dict(
    application_id='application_1449857544442_0002',
    counters={
        'File System Counters': {
            'FILE: Number of bytes read': 86,
        }
    },
    job_id='job_1449857544442_0002',
    output_dir=('hdfs:///user/root/tmp/mrjob'
                '/mr_wc.root.20151211.181326.984074/output'))


# abbreviated version of real output from Hadoop 1.0.3 on EMR AMI 2.4.9
# Including things that might be interesting to parse later on
PRE_YARN_STEP_LOG_LINES = [
    '15/12/11 23:08:37 INFO streaming.StreamJob:'
    ' getLocalDirs(): [/mnt/var/lib/hadoop/mapred]\n',
    '15/12/11 23:08:37 INFO streaming.StreamJob:'
    ' Running job: job_201512112247_0003\n',
    '15/12/11 23:08:37 INFO streaming.StreamJob:'
    ' Tracking URL:'
    ' http://ip-172-31-27-129.us-west-2.compute.internal:9100'
    '/jobdetails.jsp?jobid=job_201512112247_0003\n',
    '15/12/11 23:09:16 INFO streaming.StreamJob:'
    '  map 100%  reduce 100%\n',
    '15/12/11 23:09:22 INFO streaming.StreamJob:'
    ' Output: hdfs:///user/hadoop/tmp/mrjob'
    '/mr_wc.hadoop.20151211.230352.433691/output\n',
]

PARSED_PRE_YARN_STEP_LOG_LINES = dict(
    job_id='job_201512112247_0003',
    output_dir=('hdfs:///user/hadoop/tmp/mrjob'
                '/mr_wc.hadoop.20151211.230352.433691/output'),
)


class ParseStepSyslogTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(_parse_step_syslog([]), {})

    def test_yarn(self):
        self.assertEqual(
            _parse_step_syslog(YARN_STEP_LOG_LINES),
            PARSED_YARN_STEP_LOG_LINES)

    def test_pre_yarn(self):
        self.assertEqual(
            _parse_step_syslog(PRE_YARN_STEP_LOG_LINES),
            PARSED_PRE_YARN_STEP_LOG_LINES)


class InterpretHadoopJarCommandStderrTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(_interpret_hadoop_jar_command_stderr([]), {})

    def test_yarn(self):
        self.assertEqual(
            _interpret_hadoop_jar_command_stderr(YARN_STEP_LOG_LINES),
            PARSED_YARN_STEP_LOG_LINES)

    def test_pre_yarn(self):
        self.assertEqual(
            _interpret_hadoop_jar_command_stderr(PRE_YARN_STEP_LOG_LINES),
            PARSED_PRE_YARN_STEP_LOG_LINES)

    def test_infer_job_id_from_application_id(self):
        lines = [
            '15/12/11 13:32:45 INFO impl.YarnClientImpl:'
            ' Submitted application application_1449857544442_0002\n',
        ]

        self.assertEqual(
            _interpret_hadoop_jar_command_stderr(lines),
            dict(application_id='application_1449857544442_0002',
                 job_id='job_1449857544442_0002'))

    def test_yarn_error(self):
        lines = [
            '16/01/22 19:14:16 INFO mapreduce.Job: Task Id :'
            ' attempt_1453488173054_0001_m_000000_0, Status : FAILED\n',
            'Error: java.lang.RuntimeException: PipeMapRed'
            '.waitOutputThreads(): subprocess failed with code 1\n',
            '\tat org.apache.hadoop.streaming.PipeMapRed'
            '.waitOutputThreads(PipeMapRed.java:330)\n',
            '\tat org.apache.hadoop.streaming.PipeMapRed.mapRedFinished'
            '(PipeMapRed.java:543)\n',
            '\n',
        ]

        self.assertEqual(
            _interpret_hadoop_jar_command_stderr(lines),
            dict(
                errors=[
                    dict(
                        attempt_id='attempt_1453488173054_0001_m_000000_0',
                        hadoop_error=dict(
                            message=(
                                'Error: java.lang.RuntimeException: PipeMapRed'
                                '.waitOutputThreads(): subprocess failed with'
                                ' code 1\n\tat org.apache.hadoop.streaming'
                                '.PipeMapRed.waitOutputThreads(PipeMapRed.java'
                                ':330)\n\tat org.apache.hadoop.streaming'
                                '.PipeMapRed.mapRedFinished(PipeMapRed.java'
                                ':543)'
                            ),
                            num_lines=5,
                            start_line=0,
                        ),
                        # task ID is implied by attempt ID
                        task_id='task_1453488173054_0001_m_000000',
                    )
                ]
            ))

    def test_yarn_error_without_exception(self):
        # when there's no exception, just use the whole line as the message
        lines = [
            '16/01/22 19:14:16 INFO mapreduce.Job: Task Id :'
            ' attempt_1453488173054_0001_m_000000_0, Status : FAILED\n',
        ]

        self.assertEqual(
            _interpret_hadoop_jar_command_stderr(lines),
            dict(
                errors=[
                    dict(
                        attempt_id='attempt_1453488173054_0001_m_000000_0',
                        hadoop_error=dict(
                            message=(
                                'Task Id :'
                                ' attempt_1453488173054_0001_m_000000_0,'
                                ' Status : FAILED'
                            ),
                            num_lines=1,
                            start_line=0,
                        ),
                        # task ID is implied by attempt ID
                        task_id='task_1453488173054_0001_m_000000',
                    )
                ]
            ))

    def test_lines_can_be_bytes(self):
        self.assertEqual(
            _interpret_hadoop_jar_command_stderr([
                b'15/12/11 13:33:11 INFO mapreduce.Job:'
                b' Running job: job_1449857544442_0002\n']),
            dict(job_id='job_1449857544442_0002'))

    def test_record_callback(self):
        records = []

        def record_callback(record):
            records.append(record)

        lines = [
            'packageJobJar: [/mnt/var/lib/hadoop/tmp/hadoop'
            '-unjar7873615084086492115/] []'
            ' /tmp/streamjob737002412080260811.jar tmpDir=null\n',
            '15/12/11 13:33:11 INFO mapreduce.Job:'
            ' Running job: job_1449857544442_0002\n',
            'Streaming Command Failed!\n',
        ]

        self.assertEqual(
            _interpret_hadoop_jar_command_stderr(
                lines, record_callback=record_callback),
            dict(job_id='job_1449857544442_0002'))

        self.assertEqual(
            records,
            [
                dict(
                    caller_location='',
                    level='',
                    logger='',
                    message=('packageJobJar: [/mnt/var/lib/hadoop/tmp/hadoop'
                             '-unjar7873615084086492115/] []'
                             ' /tmp/streamjob737002412080260811.jar'
                             ' tmpDir=null'),
                    num_lines=1,
                    start_line=0,
                    thread='',
                    timestamp='',
                ),
                dict(
                    caller_location='',
                    level='INFO',
                    logger='mapreduce.Job',
                    message='Running job: job_1449857544442_0002',
                    num_lines=1,
                    start_line=1,
                    thread='',
                    timestamp='15/12/11 13:33:11',
                ),
                dict(
                    caller_location='',
                    level='',
                    logger='',
                    message='Streaming Command Failed!',
                    num_lines=1,
                    start_line=2,
                    thread='',
                    timestamp='',
                ),
            ])

    def test_treat_eio_as_eof(self):
        def yield_lines():
            yield ('15/12/11 13:33:11 INFO mapreduce.Job:'
                   ' Running job: job_1449857544442_0002\n')
            e = IOError()
            e.errno = errno.EIO
            raise e

        self.assertEqual(
            _interpret_hadoop_jar_command_stderr(yield_lines()),
            dict(job_id='job_1449857544442_0002'))

    def test_raise_other_io_errors(self):
        def yield_lines():
            yield ('15/12/11 13:33:11 INFO mapreduce.Job:'
                   ' Running job: job_1449857544442_0002\n')
            raise IOError

        self.assertRaises(
            IOError,
            _interpret_hadoop_jar_command_stderr, yield_lines())


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

        with no_handlers_for_logger('mrjob.logs.step'):
            stderr = StringIO()
            log_to_stream('mrjob.logs.step', stderr)

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

        with no_handlers_for_logger('mrjob.logs.step'):
            stderr = StringIO()
            log_to_stream('mrjob.logs.step', stderr)

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


class MatchEMRStepSyslogPathTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(_match_emr_step_syslog_path(''), None)

    def test_ssh(self):
        log_path = (
            'ssh://masterssh://master/mnt/var/log/hadoop/steps/'
            's-2BQ5U0ZHTR16N/syslog')

        self.assertEqual(
            _match_emr_step_syslog_path(log_path),
            dict(step_id='s-2BQ5U0ZHTR16N', timestamp=None))

    def test_s3(self):
        log_path = (
            's3://mrjob-394dc542f5df5612/tmp/logs/j-1GIXXKEE3MJ2H/steps'
            '/s-2BQ5U0ZHTR16N/syslog.gz')

        self.assertEqual(
            _match_emr_step_syslog_path(log_path),
            dict(step_id='s-2BQ5U0ZHTR16N', timestamp=None))

    def test_s3_log_rotation(self):
        log_path = (
            's3://mrjob-394dc542f5df5612/tmp/logs/j-1GIXXKEE3MJ2H/steps'
            '/s-2BQ5U0ZHTR16N/syslog.2016-02-26-23.gz')

        self.assertEqual(
            _match_emr_step_syslog_path(log_path),
            dict(step_id='s-2BQ5U0ZHTR16N', timestamp='2016-02-26-23'))

    def test_match_syslog_only(self):
        syslog_path = (
            'ssh://master/mnt/var/log/hadoop/steps/s-2BQ5U0ZHTR16N/syslog')

        self.assertEqual(
            _match_emr_step_stderr_path(syslog_path), None)


# this is currently almost identical to MatchEMRStepSyslogPathTestCase,
# but that could change (for example, previous versions of the code
# ignored rotated stderr logs)
class MatchEMRStepStderrPathTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(_match_emr_step_stderr_path(''), None)

    def test_ssh(self):
        log_path = (
            'ssh://masterssh://master/mnt/var/log/hadoop/steps/'
            's-2BQ5U0ZHTR16N/stderr')

        self.assertEqual(
            _match_emr_step_stderr_path(log_path),
            dict(step_id='s-2BQ5U0ZHTR16N', timestamp=None))

    def test_s3(self):
        log_path = (
            's3://mrjob-394dc542f5df5612/tmp/logs/j-1GIXXKEE3MJ2H/steps'
            '/s-2BQ5U0ZHTR16N/stderr.gz')

        self.assertEqual(
            _match_emr_step_stderr_path(log_path),
            dict(step_id='s-2BQ5U0ZHTR16N', timestamp=None))

    def test_s3_log_rotation(self):
        log_path = (
            's3://mrjob-394dc542f5df5612/tmp/logs/j-1GIXXKEE3MJ2H/steps'
            '/s-2BQ5U0ZHTR16N/stderr.2016-02-26-23.gz')

        self.assertEqual(
            _match_emr_step_stderr_path(log_path),
            dict(step_id='s-2BQ5U0ZHTR16N', timestamp='2016-02-26-23'))

    def test_match_stderr_only(self):
        syslog_path = (
            'ssh://master/mnt/var/log/hadoop/steps/s-2BQ5U0ZHTR16N/syslog')

        self.assertEqual(
            _match_emr_step_stderr_path(syslog_path), None)


class InterpretEMRStepSyslogTestCase(PatcherTestCase):

    def setUp(self):
        super(InterpretEMRStepSyslogTestCase, self).setUp()

        # instead of mocking out contents of files, just mock out
        # what _parse_step_log() should return, and have
        # _cat_log() just pass through the path
        self.mock_paths = []
        self.path_to_mock_result = {}

        self.mock_paths_catted = []

        def mock_cat_log(fs, path):
            if path in self.mock_paths:
                self.mock_paths_catted.append(path)
            return path

        # (the real _parse_step_log() expects lines, not paths)
        def mock_parse_step_syslog(path_from_mock_cat_log):
            return self.path_to_mock_result.get(path_from_mock_cat_log, {})

        # need to mock ls so that _ls_task_syslogs() can work
        def mock_exists(path):
            return path in self.mock_paths

        def mock_ls(log_dir):
            return self.mock_paths

        self.mock_fs = Mock()
        self.mock_fs.ls = Mock(side_effect=mock_ls)

        self.mock_cat_log = self.start(
            patch('mrjob.logs.step._cat_log', side_effect=mock_cat_log))

        self.start(patch('mrjob.logs.step._parse_step_syslog',
                         side_effect=mock_parse_step_syslog))

    def mock_path_matches(self):
        mock_log_dir_stream = [['']]  # needed to make _ls_logs() work
        return _ls_emr_step_syslogs(self.mock_fs, mock_log_dir_stream)

    def interpret_emr_step_syslog(self, **kwargs):
        return _interpret_emr_step_syslog(
            self.mock_fs, self.mock_path_matches(), **kwargs)

    def test_empty(self):
        self.assertEqual(self.interpret_emr_step_syslog(), {})

    def test_single_log(self):
        log_path = 'ssh://master/mnt/var/log/hadoop/steps/s-STEPID/syslog'

        self.mock_paths = [log_path]

        self.path_to_mock_result = {
            log_path: dict(output_dir='hdfs:///output')
        }

        self.assertEqual(self.interpret_emr_step_syslog(),
                         dict(output_dir='hdfs:///output'))

    def test_implied_job_id(self):
        log_path = 'ssh://master/mnt/var/log/hadoop/steps/s-STEPID/syslog'

        self.mock_paths = [log_path]

        self.path_to_mock_result = {
            log_path: dict(application_id='application_1')
        }

        self.assertEqual(self.interpret_emr_step_syslog(),
                         dict(application_id='application_1',
                              job_id='job_1'))

    def test_error(self):
        # test patching in path and implied ID
        log_path = 'ssh://master/mnt/var/log/hadoop/steps/s-STEPID/syslog'

        self.mock_paths = [log_path]

        self.path_to_mock_result = {
            log_path: dict(errors=[dict(
                attempt_id='attempt_201512232143_0008_m_000001_3',
                hadoop_error=dict(message='BOOM'),
            )]),
        }

        self.assertEqual(
            self.interpret_emr_step_syslog(),
            dict(errors=[dict(
                attempt_id='attempt_201512232143_0008_m_000001_3',
                hadoop_error=dict(
                    message='BOOM',
                    path=log_path,
                ),
                task_id='task_201512232143_0008_m_000001')]))

    def test_multiple_logs(self):
        prev_path = 's3://bucket/logs/steps/s-STEPID/syslog.2015-05-06-09.gz'
        current_path = 's3://bucket/logs/steps/s-STEPID/syslog.gz'

        # current log would come first in alphabetical sort
        self.mock_paths = [prev_path, current_path]

        self.path_to_mock_result = {
            prev_path: dict(
                errors=[
                    dict(
                        hadoop_error=dict(
                            message='first error',
                        ),
                    ),
                ],
                job_id='job_1',
            ),
            current_path: dict(
                counters=dict(foo=dict(bar=1)),
                errors=[
                    dict(
                        hadoop_error=dict(
                            message='second error',
                        ),
                    ),
                ],
            ),
        }

        # both errors should appear, in the correct order
        self.assertEqual(
            self.interpret_emr_step_syslog(),
            dict(
                counters=dict(foo=dict(bar=1)),
                errors=[
                    dict(
                        hadoop_error=dict(
                            message='first error',
                            path=prev_path,
                        ),
                    ),
                    dict(
                        hadoop_error=dict(
                            message='second error',
                            path=current_path,
                        ),
                    ),
                ],
                job_id='job_1',
            ),
        )

        self.assertEqual(self.mock_paths_catted, [prev_path, current_path])

    maxDiff = None


class InterpretEMRStepStderrTestCase(PatcherTestCase):

    def setUp(self):
        super(InterpretEMRStepStderrTestCase, self).setUp()

        # instead of mocking out contents of files, just mock out
        # what _parse_step_syslog() should return, and have
        # _cat_log() just pass through the path
        self.mock_paths = []
        self.path_to_mock_result = {}

        self.mock_paths_catted = []

        def mock_cat_log(fs, path):
            if path in self.mock_paths:
                self.mock_paths_catted.append(path)
            return path

        def mock_parse_task_stderr(path_from_mock_cat_log):
            return self.path_to_mock_result.get(path_from_mock_cat_log)

        # need to mock ls so that _ls_task_syslogs() can work
        def mock_exists(path):
            return path in self.mock_paths

        def mock_ls(log_dir):
            return self.mock_paths

        self.mock_fs = Mock()
        self.mock_fs.ls = Mock(side_effect=mock_ls)

        self.mock_cat_log = self.start(
            patch('mrjob.logs.step._cat_log', side_effect=mock_cat_log))

        self.start(patch('mrjob.logs.step._parse_task_stderr',
                         side_effect=mock_parse_task_stderr))

    def mock_path_matches(self):
        mock_log_dir_stream = [['']]  # needed to make _ls_logs() work
        return _ls_emr_step_stderr_logs(self.mock_fs, mock_log_dir_stream)

    def interpret_emr_step_stderr(self, **kwargs):
        return _interpret_emr_step_stderr(
            self.mock_fs, self.mock_path_matches(), **kwargs)

    def test_empty(self):
        self.assertEqual(self.interpret_emr_step_stderr(), {})

    def test_single_log(self):
        log_path = 'ssh://master/mnt/var/log/hadoop/steps/s-STEPID/stderr'

        self.mock_paths = [log_path]

        self.path_to_mock_result = {
            log_path: dict(message='unfortunate error')
        }

        self.assertEqual(
            self.interpret_emr_step_stderr(),
            dict(errors=[dict(task_error=dict(
                message='unfortunate error', path=log_path))]))

    def test_multiple_logs(self):
        prev_path = 's3://bucket/logs/steps/s-STEPID/stderr.2015-05-06-09.gz'
        current_path = 's3://bucket/logs/steps/s-STEPID/stderr.gz'

        self.mock_paths = [prev_path, current_path]

        self.path_to_mock_result = {
            prev_path: dict(
                message='warning',
            ),
            current_path: dict(
                message='actual error',
            ),
        }

        self.assertEqual(
            self.interpret_emr_step_stderr(),
            dict(
                errors=[
                    dict(
                        task_error=dict(
                            message='actual error',
                            path=current_path,
                        ),
                    ),
                ]
            ),
        )

        # no need to look at previous chunk of stderr (basically tailing here)
        self.assertEqual(self.mock_paths_catted, [current_path])

    def test_fall_back_to_rotated_logs(self):
        prev_path = 's3://bucket/logs/steps/s-STEPID/stderr.2015-05-06-09.gz'
        current_path = 's3://bucket/logs/steps/s-STEPID/stderr.gz'

        self.mock_paths = [prev_path, current_path]

        self.path_to_mock_result = {
            prev_path: dict(
                message='warning',
            ),
        }

        self.assertEqual(
            self.interpret_emr_step_stderr(),
            dict(
                errors=[
                    dict(
                        task_error=dict(
                            message='warning',
                            path=prev_path,
                        ),
                    ),
                ]
            ),
        )

        self.assertEqual(self.mock_paths_catted, [current_path, prev_path])

    maxDiff = None
