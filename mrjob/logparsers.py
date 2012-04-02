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

import logging
import posixpath
import re

from mrjob.parse import find_hadoop_java_stack_trace
from mrjob.parse import find_input_uri_for_mapper
from mrjob.parse import find_interesting_hadoop_streaming_error
from mrjob.parse import find_python_traceback
from mrjob.parse import find_timeout_error
from mrjob.parse import parse_hadoop_counters_from_line


log = logging.getLogger('mrjob.logparser')


# Constants used to distinguish between different kinds of logs
TASK_ATTEMPT_LOGS = 'TASK_ATTEMPT_LOGS'
STEP_LOGS = 'STEP_LOGS'
JOB_LOGS = 'JOB_LOGS'
NODE_LOGS = 'NODE_LOGS'

# regex for matching task-attempts log URIs
TASK_ATTEMPTS_LOG_URI_RE = re.compile(
    r'^.*/attempt_'                 #attempt_
    r'(?P<timestamp>\d+)_'          #201203222119_
    r'(?P<step_num>\d+)_'           #0001_
    r'(?P<node_type>\w)_'           #m_
    r'(?P<node_num>\d+)_'           #000000_
    r'(?P<attempt_num>\d+)/'        #3/
    r'(?P<stream>stderr|syslog)$')  #stderr

# regex for matching step log URIs
STEP_LOG_URI_RE = re.compile(
    r'^.*/(?P<step_num>\d+)/(?P<stream>syslog|stderr)$')

# regex for matching job log URIs. There is some variety in how these are
# formatted, so this expression is pretty general.
EMR_JOB_LOG_URI_RE = re.compile(
    r'^.*?'     # sometimes there is a number at the beginning, and the
                # containing directory can be almost anything.
    r'job_(?P<timestamp>\d+)_(?P<step_num>\d+)' # oh look, meaningful data!
    r'(_\d+)?'  # sometimes there is a number here.
    r'_hadoop_streamjob(\d+).jar$')
HADOOP_JOB_LOG_URI_RE = re.compile(
    r'^.*?/job_(?P<timestamp>\d+)_(?P<step_num>\d+)_(?P<mystery_string_1>\d+)'
    r'_(?P<user>.*?)_streamjob(?P<mystery_string_2>\d+).jar$')

# regex for matching slave log URIs
NODE_LOG_URI_RE = re.compile(
    r'^.*?/hadoop-hadoop-(jobtracker|namenode).*.out$')


def scan_for_counters_in_files(log_file_uris, runner, hadoop_version):
    """Scan *log_file_uris* for counters, using *runner* for file system access
    """
    counters = {}
    relevant_logs = []  # list of (sort key, URI)

    for log_file_uri in log_file_uris:
        match = EMR_JOB_LOG_URI_RE.match(log_file_uri)
        if match is None:
            match = HADOOP_JOB_LOG_URI_RE.match(log_file_uri)

        if not match:
            continue

        relevant_logs.append((match.group('step_num'), log_file_uri))

    relevant_logs.sort()

    for _, log_file_uri in relevant_logs:
        log_lines = runner.cat(log_file_uri)
        if not log_lines:
            continue

        for line in log_lines:
            new_counters, step_num = parse_hadoop_counters_from_line(
                                        line, hadoop_version)
            if new_counters:
                counters[step_num] = new_counters
    return counters


def scan_logs_in_order(task_attempt_logs, step_logs, job_logs, runner):
    """Use mapping and order from :py:func:`processing_order` to find errors in
    logs.

    Returns::

        None (nothing found) or a dictionary containing:
        lines -- lines in the log file containing the error message
        log_file_uri -- the log file containing the error message
        input_uri -- if the error happened in a mapper in the first
            step, the URI of the input file that caused the error
            (otherwise None)
    """
    log_type_to_uri_list = {
        TASK_ATTEMPT_LOGS: task_attempt_logs,
        STEP_LOGS: step_logs,
        # job logs may be scanned twice, so save the ls generator output
        JOB_LOGS: list(job_logs),
    }
    for log_type, sort_func, parsers in processing_order():
        relevant_logs = sort_func(log_type_to_uri_list[log_type])

        # unfortunately need to special case task attempts since later
        # attempts may have succeeded and we don't want those (issue #31)
        tasks_seen = set()
        for sort_key, info, log_file_uri in relevant_logs:
            log.debug('Parsing %s' % log_file_uri)

            if log_type == TASK_ATTEMPT_LOGS:
                task_info = (info['step_num'], info['node_type'],
                             info['node_num'], info['stream'])
                if task_info in tasks_seen:
                    continue
                tasks_seen.add(task_info)

            val = _apply_parsers_to_log(parsers, log_file_uri, runner)
            if val:
                if info.get('node_type', None) == 'm':
                    val['input_uri'] = _scan_for_input_uri(log_file_uri,
                                                           runner)
                return val

    return None


def _apply_parsers_to_log(parsers, log_file_uri, runner):
    """Have each :py:class:`LogParser` in *parsers* try to find an error
    in the contents of *log_file_uri*
    """
    for parser in parsers:
        if parser.LOG_NAME_RE.match(log_file_uri):
            log_lines = runner.cat(log_file_uri)
            if not log_lines:
                continue

            lines = parser.parse(log_lines)
            if lines is not None:
                return {
                    'lines': lines,
                    'log_file_uri': log_file_uri,
                    'input_uri': None,
                }
    return None


def _scan_for_input_uri(log_file_uri, runner):
    """Scan the syslog file corresponding to log_file_uri for
    information about the input file.

    Helper function for :py:func:`scan_task_attempt_logs()`
    """
    syslog_uri = posixpath.join(
        posixpath.dirname(log_file_uri), 'syslog')

    syslog_lines = runner.cat(syslog_uri)
    if syslog_lines:
        log.debug('scanning %s for input URI' % syslog_uri)
        return find_input_uri_for_mapper(syslog_lines)
    else:
        return None


def _make_sorting_func(regexp, sort_key_func):

    def sorting_func(log_file_uris):
        """Sort *log_file_uris* matching *regexp* according to *sort_key_func*

        :return: [(sort_key, info, log_file_uri)]
        """
        relevant_logs = []  # list of (sort key, info, URI)
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
    emr_job_sort = _make_sorting_func(EMR_JOB_LOG_URI_RE,
                                  make_job_log_sort_key)
    hadoop_job_sort = _make_sorting_func(HADOOP_JOB_LOG_URI_RE,
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
        (JOB_LOGS, emr_job_sort,
         [
             TimeoutErrorLogParser()
         ]),
        (JOB_LOGS, hadoop_job_sort,
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
    return (info['step_num'], info['stream'] == 'stderr')


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
