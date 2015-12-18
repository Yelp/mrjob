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

from mrjob.py2 import to_string
from mrjob.logs.ls import _ls_yarn_task_syslogs
from mrjob.logs.ls import _stderr_for_syslog
from mrjob.logs.parse import _parse_yarn_task_syslog
from mrjob.logs.parse import _parse_python_task_stderr


log = getLogger(__name__)



def _cat_log(fs, uri):
    """fs.cat() the given log, converting lines to strings, and logging
    errors."""
    try:
        for line in fs.cat(uri):
            yield to_string(line)
    except IOError as e:
        log.warning("couldn't cat() %s: %r" % (uri, e))


def _find_error_in_yarn_task_logs(fs, log_dirs_stream, application_id=None):
    """Given a filesystem and a stream of lists of log dirs to search in,
    find the last error and return details about it.

    Returns a dictionary with the following keys ("optional" means
    that something may be None):

    syslog: dict with keys:
       uri: URI of syslog we found error in
       error: error details; dict with keys:
           exception: Java exception (as string)
           stack_trace: array of lines with Java stack trace
       split: optional input split we were reading; dict with keys:
           uri: URI of input file
           start_line: first line of split (0-indexed)
           num_lines: number of lines in split
    stderr: optional dict with keys:
       uri: URI of stderr corresponding to syslog
       error: optional error details; dict with keys:
           exception: string  (Python exception)
           traceback: array of lines with Python stack trace
    """
    syslog_uris = []

    # we assume that each set of log URIs contains the same copies
    # of syslogs, so stop once we find any non-empty set of log dirs
    for log_dirs in log_dirs_stream:
        syslog_uris = _ls_yarn_task_syslogs(fs, log_dirs,
                                            application_id=application_id)
        if syslog_uris:
            break

    for syslog_uri in syslog_uris:
        syslog_info = _parse_yarn_task_syslog(_cat_log(fs, syslog_uri))

        if not syslog_info['error']:
            continue

        # found error! see if we can explain it

        # TODO: don't bother if error wasn't due to child process
        stderr_uri = _stderr_for_syslog(syslog_uri)

        stderr_info = _parse_python_task_stderr(_cat_log(fs, stderr_uri))

        # output error info
        syslog_info['uri'] = syslog_uri
        stderr_info['uri'] = stderr_uri

        return dict(syslog=syslog_info, stderr=stderr_info)

    return None
