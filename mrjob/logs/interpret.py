# -*- coding: utf-8 -*-
# Copyright 2015 Yelp and Contributors
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
"""Scan logs for cause of failure and counters."""
from logging import getLogger

from mrjob.compat import uses_yarn
from mrjob.logs.ls import _ls_pre_yarn_task_syslogs
from mrjob.logs.ls import _ls_yarn_task_syslogs
from mrjob.logs.ls import _stderr_for_syslog
from mrjob.logs.parse import _parse_task_syslog
from mrjob.logs.parse import _parse_python_task_stderr
from mrjob.py2 import to_string


log = getLogger(__name__)



def _cat_log(fs, path):
    """fs.cat() the given log, converting lines to strings, and logging
    errors."""
    try:
        for line in fs.cat(path):
            yield to_string(line)
    except IOError as e:
        log.warning("couldn't cat() %s: %r" % (path, e))


def _find_error_in_pre_yarn_task_logs(fs, log_dirs_stream, job_id=None):
    # TODO: merge this with _find_error_in_yarn_task_logs()
    raise NotImplementedError


def _find_error_in_task_logs(fs, log_dirs_stream, hadoop_version,
                             application_id=None, job_id=None):
    """Given a filesystem and a stream of lists of log dirs to search in,
    find the last error and return details about it. *hadoop_version*
    is required, as task logs have very different paths in YARN.

    In YARN, you must set *application_id*, and pre-YARN, you must set
    *job_id*, or we'll bail out and return None.

    Returns a dictionary with the following keys ("optional" means
    that something may be None):

    syslog: dict with keys:
       path: path of syslog we found error in
       error: error details; dict with keys:
           exception: Java exception (as string)
           stack_trace: array of lines with Java stack trace
       split: optional input split we were reading; dict with keys:
           path: path of input file
           start_line: first line of split (0-indexed)
           num_lines: number of lines in split
    stderr: optional dict with keys:
       path: path of stderr corresponding to syslog
       error: optional error details; dict with keys:
           exception: string  (Python exception)
           traceback: array of lines with Python stack trace
    type: always set to 'task'
    """
    syslog_paths = []

    yarn = uses_yarn(hadoop_version)

    if ((yarn and application_id is None) or (not yarn and job_id is None)):
        return None

    # we assume that each set of log paths contains the same copies
    # of syslogs, so stop once we find any non-empty set of log dirs
    for log_dirs in log_dirs_stream:
        if yarn:
            syslog_paths = _ls_yarn_task_syslogs(fs, log_dirs,
                                                 application_id=application_id)
        else:
            syslog_paths = _ls_pre_yarn_task_syslogs(fs, log_dirs,
                                                     job_id=job_id)

        if syslog_paths:
            break

    for syslog_path in syslog_paths:
        log.debug('Looking for error in %s' % syslog_path)
        syslog_info = _parse_task_syslog(_cat_log(fs, syslog_path))

        if not syslog_info['error']:
            continue

        # found error! see if we can explain it

        # TODO: don't bother if error wasn't due to child process
        stderr_path = _stderr_for_syslog(syslog_path)

        stderr_info = _parse_python_task_stderr(_cat_log(fs, stderr_path))

        # output error info
        syslog_info['path'] = syslog_path
        stderr_info['path'] = stderr_path

        return dict(type='task', syslog=syslog_info, stderr=stderr_info)

    return None



def _format_cause_of_failure(cause):
    """Format error found by this module as lines, so we can easily
    log it and put it into an exception."""
    try:
        if cause['type'] == 'task':
            return _format_error_from_task_logs(cause)
    except:
        pass

    # if it's an unknown error type or there's something wrong with
    # the format function, just print a repr
    return ['Probable cause of failure: %r' % (cause,)]


def _format_error_from_task_logs(cause):
    """Helper for _format_cause_of_failure()"""
    lines = []

    lines.append(
        'Probable cause of failure (from %s):' % cause['syslog']['path'])
    lines.append('')
    lines.append(cause['syslog']['error']['exception'])
    lines.extend(cause['syslog']['error']['stack_trace'])

    if cause['stderr'] and cause['stderr']['error']:
        lines.append('')
        lines.append('caused by Python exception (from %s):' %
                     cause['stderr']['path'])
        lines.append('')
        lines.extend(cause['stderr']['error']['traceback'])
        lines.append(cause['stderr']['error']['exception'])

    if cause['syslog']['split']:
        split = cause['syslog']['split']

        lines.append('')

        line_nums_desc = ''
        if not (split['start_line'] is None or split['num_lines'] is None):
            first_line = split['start_line'] + 1
            last_line = first_line + split['num_lines']
            line_nums_desc = 'lines %d-%d of ' % (first_line, last_line)

        lines.append('while reading input from %s%s' %
                     (line_nums_desc, split['path']))

    # if we didn't mention stderr above, mention it now
    if cause['stderr'] and not cause['stderr']['error']:
        lines.append('')
        lines.append('(see %s for task stderr)' % cause['stderr']['path'])

    return lines
