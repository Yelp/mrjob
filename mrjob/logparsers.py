# Copyright 2011 Matthew Tai
# Copyright 2011 Yelp
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
"""Parsing classes to find errors in Hadoop logs"""
from __future__ import with_statement

import re

from mrjob.parse import find_hadoop_java_stack_trace, find_interesting_hadoop_streaming_error, find_python_traceback, find_timeout_error


# Constants used to tell :py:func:`~mrjob.emr.EMRJobRunner.ssh_list_logs` and :py:func:`~mrjob.emr.EMRJobRunner.s3_list_logs`  what logs to find and return
TASK_ATTEMPT_LOGS = 'TASK_ATTEMPT_LOGS'
STEP_LOGS = 'STEP_LOGS'
JOB_LOGS = 'JOB_LOGS'
NODE_LOGS = 'NODE_LOGS'


def processing_order():
    """Define a mapping and order for the log parsers.

    Returns tuples of ``(LOG_TYPE, sort_function, [parser])``, where
    *sort_function* takes a list of log URIs and returns a list of tuples
    ``(sort_key, info, log_file_uri)``.

    :return: [(LOG_TYPE, sort_function, [LogParser])]
    """
    return [
        # give priority to task-attempts/ logs as they contain more useful
        # error messages. this may take a while.
        (TASK_ATTEMPT_LOGS, make_task_attempt_log_sort_key,
         [
             PythonTracebackLogParser(),
             HadoopJavaStackTraceLogParser()
         ]),
        (STEP_LOGS, make_step_log_sort_key,
         [
             HadoopStreamingErrorLogParser()
         ]),
        (JOB_LOGS, make_job_log_sort_key,
         [
             TimeoutErrorLogParser()
         ]),
    ]


### SORT KEY FUNCTIONS ###


# prefer stderr to syslog (Python exceptions are more
# helpful than Java ones)
def make_task_attempt_log_sort_key(info):
    return (info['step_num'], info['node_type'],
            info['attempt_num'],
            info['stream'] == 'stderr',
            info['node_num'])


def make_step_log_sort_key(info):
    return info['step_num']


def make_job_log_sort_key(info):
    return (info['timestamp'], info['step_num'])


### LOG PARSERS ###


class LogParser(object):
    """Methods for parsing information from Hadoop logs"""

    # Log type is sometimes too general, so allow parsers to limit their logs
    # further by name
    LOG_NAME_RE = re.compile(r'.*')

    def parse(self, lines):
        """Parse one kind of error from *lines*. Return list of lines or None.

        :type lines: iterable of str
        :param lines: lines to scan for information
        :return: [str] or None
        """
        raise NotImplementedError


class PythonTracebackLogParser(LogParser):

    LOG_NAME_RE = re.compile(r'.*stderr$')

    def parse(self, lines):
        return find_python_traceback(lines)


class HadoopJavaStackTraceLogParser(LogParser):

    def parse(self, lines):
        return find_hadoop_java_stack_trace(lines)


class HadoopStreamingErrorLogParser(LogParser):

    def parse(self, lines):
        msg = find_interesting_hadoop_streaming_error(lines)
        if msg:
            return [msg + '\n']
        else:
            return None


class TimeoutErrorLogParser(LogParser):

    def parse(self, lines):
        n = find_timeout_error(lines)
        if n is not None:
            return ['Timeout after %d seconds\n' % n]
        else:
            return None
