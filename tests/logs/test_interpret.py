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

    def test_empty(self):
        # should fall back
        self.assertEqual(
            _format_cause_of_failure(None),
            ['Probable cause of failure: None'])

    def test_task_log_error_no_traceback(self):
        task_log_prefix = (
            '/usr/local/hadoop/logs/userlogs/application_1450723037222_0007'
            '/container_1450723037222_0007_01_000010/')

        syslog_path = task_log_prefix + 'syslog'
        stderr_path = task_log_prefix + 'stderr'

        java_exception = (
            'java.lang.RuntimeException: PipeMapRed.waitOutputThreads():'
            ' subprocess failed with code 1')

        java_stack_trace = [
            '\tat org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads'
            '(PipeMapRed.java:322)']

        input_uri = (
            'hdfs://e4270474c8ee:9000/user/root/tmp/mrjob'
            '/mr_boom.root.20151221.192347.898037/files/bootstrap.sh')

        cause = dict(
            type='task',
            syslog=dict(
                path=syslog_path,
                split=dict(
                    start_line=0,
                    num_lines=335,
                    path=input_uri,
                ),
                error=dict(
                    stack_trace=java_stack_trace,
                    exception=java_exception,
                ),
            ),
            stderr=dict(
                path=stderr_path,
                error=None,
            ),
        )

        self.assertEqual(
            _format_cause_of_failure(cause),
            ['Probable cause of failure (from ' + syslog_path + '):',
             java_exception] +
            java_stack_trace +
            ['(see ' + stderr_path + ' for task stderr)',
             'while reading input from lines 1-336 of ' + input_uri])
