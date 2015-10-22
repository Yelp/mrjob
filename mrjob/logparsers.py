# Copyright 2011-2012 Yelp
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
import logging
import posixpath

from mrjob.logs.ls import _JOB_LOG_PATH_RE
from mrjob.logs.ls import _TASK_LOG_PATH_RE
from mrjob.parse import find_hadoop_java_stack_trace
from mrjob.parse import find_input_uri_for_mapper
from mrjob.parse import find_interesting_hadoop_streaming_error
from mrjob.parse import find_python_traceback
from mrjob.parse import find_timeout_error
from mrjob.parse import parse_hadoop_counters_from_line

log = logging.getLogger(__name__)


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


def _parse_simple_logs(fs, log_paths, parse_func):
    """Return the relevant lines of the first error in the files at *logs*, or
    None if none found.
    """
    for path in log_paths:
        lines = _parsed_error(fs, path, parse_func)
        if lines:
            return {
                'lines': lines,
                'log_file_uri': path,
                'input_uri': None,
            }


def _parse_task_attempts(fs, log_paths):
    """Like :py:func:`_parse_simple_logs()`, but with lots of special cases for
    task attempt logs
    """
    tasks_seen = set()
    for path in log_paths:
        # skip subsequent logs for same task
        m = _TASK_LOG_PATH_RE.match(path)
        if not m:
            continue

        m_groups = m.groupdict()
        task_key = tuple(m_groups.get(k) for k in
                         ['step_num', 'task_type', 'task_num', 'stream'])
        if task_key in tasks_seen:
            continue

        tasks_seen.add(task_key)

        # Python tracebacks should win in a single file, but Java tracebacks
        # should win for later attempts
        if path.endswith('stderr'):
            lines = (_parsed_error(fs, path, find_python_traceback) or
                     _parsed_error(fs, path, find_hadoop_java_stack_trace))
        else:
            lines = _parsed_error(fs, path, find_hadoop_java_stack_trace)

        if lines:
            input_uri = _scan_for_input_uri(path, fs)

            return {
                'lines': lines,
                'log_file_uri': path,
                'input_uri': input_uri,
            }


def _scan_for_input_uri(log_file_uri, fs):
    """Scan the syslog file corresponding to log_file_uri for
    information about the input file.

    Helper function for :py:func:`scan_task_attempt_logs()`
    """
    # TODO: verify that this works on 3.x AMIs
    syslog_uri = posixpath.join(
        posixpath.dirname(log_file_uri), 'syslog')
    if log_file_uri.endswith('.gz'):
        syslog_uri += '.gz'

    syslog_lines = fs.cat(syslog_uri)
    if syslog_lines:
        log.debug('scanning %s for input URI' % syslog_uri)
        return find_input_uri_for_mapper(syslog_lines)
    else:
        return None


def best_error_from_logs(fs, task_attempts, steps, jobs):
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


def scan_for_counters_in_files(log_file_uris, fs, hadoop_version):
    """Scan *log_file_uris* for counters, using *fs* for file system access
    """
    counters = {}
    relevant_logs = []  # list of (sort key, URI)

    for log_file_uri in log_file_uris:
        m = _JOB_LOG_PATH_RE.match(log_file_uri)
        if not m:
            continue

        relevant_logs.append((int(m.group('step_num')), log_file_uri))

    relevant_logs.sort()

    for _, log_file_uri in relevant_logs:
        log_lines = fs.cat(log_file_uri)
        if not log_lines:
            continue

        for line in log_lines:
            new_counters, step_num = (
                parse_hadoop_counters_from_line(line, hadoop_version))
            if new_counters:
                counters[step_num] = new_counters
    return counters
