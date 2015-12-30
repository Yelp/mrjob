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
from mrjob.logs.interpret import _format_cause_of_failure

from tests.py2 import TestCase


class FormatCauseOfFailureTestCase(TestCase):

    # data from actual failures
    TASK_LOG_PREFIX = (
        '/usr/local/hadoop/logs/userlogs/application_1450723037222_0007'
        '/container_1450723037222_0007_01_000010/')
    SYSLOG_PATH = TASK_LOG_PREFIX + 'syslog'
    STDERR_PATH = TASK_LOG_PREFIX + 'stderr'

    JAVA_EXCEPTION = (
        'java.lang.RuntimeException: PipeMapRed.waitOutputThreads():'
        ' subprocess failed with code 1')

    JAVA_STACK_TRACE = [
        '\tat org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads'
        '(PipeMapRed.java:322)']

    INPUT_URI = (
        'hdfs://e4270474c8ee:9000/user/root/tmp/mrjob'
        '/mr_boom.root.20151221.192347.898037/files/bootstrap.sh')

    PYTHON_EXCEPTION = 'Exception: BOOM'

    PYTHON_TRACEBACK = [
        'Traceback (most recent call last):\n',
        '  File "mr_boom.py", line 10, in <module>\n',
        '    MRBoom.run()\n',
    ]

    def test_empty(self):
        # should fall back
        self.assertEqual(
            _format_cause_of_failure(None),
            ['Probable cause of failure: None'])

    def test_task_log_error(self):
        cause = dict(
            type='task',
            syslog=dict(
                path=self.SYSLOG_PATH,
                split=dict(
                    start_line=0,
                    num_lines=335,
                    path=self.INPUT_URI,
                ),
                error=dict(
                    stack_trace=self.JAVA_STACK_TRACE,
                    exception=self.JAVA_EXCEPTION,
                ),
            ),
            stderr=dict(
                path=self.STDERR_PATH,
                error=dict(
                    exception=self.PYTHON_EXCEPTION,
                    traceback=self.PYTHON_TRACEBACK,
                ),
            ),
        )

        self.assertEqual(
            _format_cause_of_failure(cause),
            ['Probable cause of failure (from ' + self.SYSLOG_PATH + '):',
             '',
             self.JAVA_EXCEPTION] +
            self.JAVA_STACK_TRACE +
            ['',
             'caused by Python exception (from ' + self.STDERR_PATH + '):',
             ''] +
            self.PYTHON_TRACEBACK +
            [self.PYTHON_EXCEPTION,
             '',
             'while reading input from lines 1-336 of ' + self.INPUT_URI])

    def test_task_log_error_no_line_nums(self):
        cause = dict(
            type='task',
            syslog=dict(
                path=self.SYSLOG_PATH,
                split=dict(
                    start_line=None,
                    num_lines=None,
                    path=self.INPUT_URI,
                ),
                error=dict(
                    stack_trace=self.JAVA_STACK_TRACE,
                    exception=self.JAVA_EXCEPTION,
                ),
            ),
            stderr=dict(
                path=self.STDERR_PATH,
                error=dict(
                    exception=self.PYTHON_EXCEPTION,
                    traceback=self.PYTHON_TRACEBACK,
                ),
            ),
        )

        self.assertEqual(
            _format_cause_of_failure(cause),
            ['Probable cause of failure (from ' + self.SYSLOG_PATH + '):',
             '',
             self.JAVA_EXCEPTION] +
            self.JAVA_STACK_TRACE +
            ['',
             'caused by Python exception (from ' + self.STDERR_PATH + '):',
             ''] +
            self.PYTHON_TRACEBACK +
            [self.PYTHON_EXCEPTION,
             '',
             'while reading input from ' + self.INPUT_URI])

    def test_task_log_error_no_traceback(self):
        cause = dict(
            type='task',
            syslog=dict(
                path=self.SYSLOG_PATH,
                split=dict(
                    start_line=0,
                    num_lines=335,
                    path=self.INPUT_URI,
                ),
                error=dict(
                    stack_trace=self.JAVA_STACK_TRACE,
                    exception=self.JAVA_EXCEPTION,
                ),
            ),
            stderr=dict(
                path=self.STDERR_PATH,
                error=None,
            ),
        )

        self.assertEqual(
            _format_cause_of_failure(cause),
            ['Probable cause of failure (from ' + self.SYSLOG_PATH + '):',
             '',
             self.JAVA_EXCEPTION] +
            self.JAVA_STACK_TRACE +
            ['',
             'while reading input from lines 1-336 of ' + self.INPUT_URI,
             '',
             '(see ' + self.STDERR_PATH + ' for task stderr)'])
