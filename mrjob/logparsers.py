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

# regex for matching task-attempts log URIs
TASK_ATTEMPTS_LOG_URI_RE = re.compile(r'^.*/attempt_(?P<timestamp>\d+)_(?P<step_num>\d+)_(?P<node_type>m|r)_(?P<node_num>\d+)_(?P<attempt_num>\d+)/(?P<stream>stderr|syslog)$')

# regex for matching step log URIs
STEP_LOG_URI_RE = re.compile(r'^.*/(?P<step_num>\d+)/syslog$')

# regex for matching job log URIs
JOB_LOG_URI_RE = re.compile(r'^.*?/.+?_(?P<mystery_string_1>\d+)_job_(?P<timestamp>\d+)_(?P<step_num>\d+)_hadoop_streamjob(?P<mystery_string_2>\d+).jar$')

# regex for matching slave log URIs
NODE_LOG_URI_RE = re.compile(r'^.*?/hadoop-hadoop-(jobtracker|namenode).*.out$')


def _make_sorting_func(regexp, sort_key_func):

    def sorting_func(log_file_uris):
        """Sort *log_file_uris* matching *regexp* according to *sort_key_func*

        :return: [(sort_key, info, log_file_uri)]
        """
        relevant_logs = [] # list of (sort key, info, URI)
        for log_file_uri in log_file_uris:
            match = regexp.match(log_file_uri)
            if not match:
                continue

            info = match.groupdict()

            sort_key = sort_key_func(info)

            relevant_logs.append((sort_key, info, log_file_uri))

        relevant_logs.sort(reverse=True)
        return relevant_logs

    return sorting_func


def processing_order():
    """Define a mapping and order for the log parsers.

    Returns tuples of ``(LOG_TYPE, sort_function, [parser])``, where
    *sort_function* takes a list of log URIs and returns a list of tuples
    ``(sort_key, info, log_file_uri)``.

    :return: [(LOG_TYPE, sort_function, [LogParser])]
    """
    task_attempt_sort = _make_sorting_func(TASK_ATTEMPTS_LOG_URI_RE,
                                           make_task_attempt_log_sort_key)
    step_sort = _make_sorting_func(STEP_LOG_URI_RE,
                                   make_step_log_sort_key)
    job_sort = _make_sorting_func(JOB_LOG_URI_RE,
                                  make_job_log_sort_key)
    return [
        # give priority to task-attempts/ logs as they contain more useful
        # error messages. this may take a while.
        (TASK_ATTEMPT_LOGS, task_attempt_sort,
         [
             PythonTracebackLogParser(),
             HadoopJavaStackTraceLogParser()
         ]),
        (STEP_LOGS, step_sort,
         [
             HadoopStreamingErrorLogParser()
         ]),
        (JOB_LOGS, job_sort,
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
