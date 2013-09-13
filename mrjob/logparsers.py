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


log = logging.getLogger(__name__)


# Constants used to distinguish between different kinds of logs
TASK_ATTEMPT_LOGS = 'TASK_ATTEMPT_LOGS'
STEP_LOGS = 'STEP_LOGS'
JOB_LOGS = 'JOB_LOGS'
NODE_LOGS = 'NODE_LOGS'

# regex for matching task-attempts log URIs
TASK_ATTEMPTS_LOG_URI_RE = re.compile(
    r'^.*/attempt_'                 # attempt_
    r'(?P<timestamp>\d+)_'          # 201203222119_
    r'(?P<step_num>\d+)_'           # 0001_
    r'(?P<node_type>\w)_'           # m_
    r'(?P<node_num>\d+)_'           # 000000_
    r'(?P<attempt_num>\d+)/'        # 3/
    r'(?P<stream>stderr|syslog)$')  # stderr

# regex for matching step log URIs
STEP_LOG_URI_RE = re.compile(
    r'^.*/(?P<step_num>\d+)/(?P<stream>syslog|stderr)$')

# regex for matching job log URIs. There is some variety in how these are
# formatted, so this expression is pretty general.
EMR_JOB_LOG_URI_RE = re.compile(
    r'^.*?'     # sometimes there is a number at the beginning, and the
                # containing directory can be almost anything.
    r'job_(?P<timestamp>\d+)_(?P<step_num>\d+)'  # oh look, meaningful data!
    r'(_\d+)?'  # sometimes there is a number here.
    r'_hadoop_streamjob(\d+).jar$')
HADOOP_JOB_LOG_URI_RE = re.compile(
    r'^.*?/job_(?P<timestamp>\d+)_(?P<step_num>\d+)_(?P<mystery_string_1>\d+)'
    r'_(?P<user>.*?)_streamjob(?P<mystery_string_2>\d+).jar$')

# regex for matching slave log URIs
NODE_LOG_URI_RE = re.compile(
    r'^.*?/hadoop-hadoop-(jobtracker|namenode).*.out$')


def _filter_sort(logs, exprs, sort_key_func):
    """Return *logs* that match any compiled regex in *exprs*. *sort_key_func*
    should be a function that takes the groupdict() of the regex match result
    and returns a sort key. Each log is in a duple (info, log_path) where info
    is the groupdict() of the regex match object.
    """
    relevant = []
    for path in logs:
        for e in exprs:
            m = e.match(path)
            m = e.match(path)
            if m:
                relevant.append((m.groupdict(), path))
                break

    def sort_key_wrapper(items):
        # item[0] is the match groupdict()
        return sort_key_func(items[0])

    return sorted(relevant, reverse=True, key=sort_key_wrapper)


### Helpers to sort different kinds of logs


def _sorted_task_attempts(logs):
    return _filter_sort(
        logs,
        [TASK_ATTEMPTS_LOG_URI_RE],
        lambda info: (
            info['step_num'], info['node_type'],
            info['attempt_num'],
            info['stream'] == 'stderr',
            info['node_num']))


def _sorted_steps(logs):
    return _filter_sort(
        logs,
        [STEP_LOG_URI_RE],
        lambda info: (info['step_num'], info['stream'] == 'stderr'))


def _sorted_jobs(logs):
    return _filter_sort(
        logs,
        [EMR_JOB_LOG_URI_RE, HADOOP_JOB_LOG_URI_RE],
        lambda info: (info['timestamp'], info['step_num']))


### Helpers for log parsing logic


def _parsed_error(fs, path, parse_func):
    """If log lines at *path* (as downloaded by *fs*, which in 0.3.5 is a
    runner object but in 0.4+ will be a filesystem object) are matched by
    *parse_func*, return the relevant lines. Otherwise, return None.
    """
    lines = fs.cat(path)
    if not lines:
        return None
    return parse_func(lines)


def _parse_simple_logs(fs, logs, parse_func):
    """Return the relevant lines of the first error in the files at *logs*, or
    None if none found.
    """
    for _, path in logs:
        lines = _parsed_error(fs, path, parse_func)
        if lines:
            return {
                'lines': lines,
                'log_file_uri': path,
                'input_uri': None,
            }


def _parse_task_attempts(fs, logs):
    """Like :py:func:`_parse_simple_logs()`, but with lots of special cases for
    task attempt logs
    """
    tasks_seen = set()
    for info, path in logs:
        task_info = (info['step_num'], info['node_type'],
                     info['node_num'], info['stream'])
        if task_info in tasks_seen:
            continue
        tasks_seen.add(task_info)

        # Python tracebacks should win in a single file, but Java tracebacks
        # should win for later attempts
        if path.endswith('stderr'):
            lines = (_parsed_error(fs, path, find_python_traceback) or
                     _parsed_error(fs, path, find_hadoop_java_stack_trace))
        else:
            lines = _parsed_error(fs, path, find_hadoop_java_stack_trace)

        if lines:
            if info.get('node_type', None) == 'm':
                input_uri = _scan_for_input_uri(path, fs)
            else:
                input_uri = None
            return {
                'lines': lines,
                'log_file_uri': path,
                'input_uri': input_uri,
            }


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


def best_error_from_logs(fs, task_attempts, steps, jobs):
    task_attempts = _sorted_task_attempts(task_attempts)
    steps = _sorted_steps(steps)
    jobs = _sorted_jobs(jobs)

    val = _parse_task_attempts(fs, task_attempts)
    if val:
        return val

    val = _parse_simple_logs(fs, steps, _hadoop_streaming_error_wrapper)
    if val:
        return val

    return _parse_simple_logs(fs, jobs, _timeout_error_wrapper)


def _hadoop_streaming_error_wrapper(lines):
    msg = find_interesting_hadoop_streaming_error(lines)
    return [msg + '\n'] if msg else None


def _timeout_error_wrapper(lines):
    n = find_timeout_error(lines)
    return ['Task timeout after %d seconds\n' % n] if n else None


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
            new_counters, step_num = (
                parse_hadoop_counters_from_line(line, hadoop_version))
            if new_counters:
                counters[step_num] = new_counters
    return counters
