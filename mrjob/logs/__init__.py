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
"""Utilities for parsing and interpreting logs.

There is one module for each kind of logs:

history: high-level job history info (found in <log dir>/history/)
step: stderr of `hadoop jar` command (so named because on EMR it appears in
    <log dir>/steps/)
task: stderr and syslog of individual tasks (found in <log dir>/userlogs/)


Each of these should have methods like this:



_find_*_logs(fs, log_dir_stream, ...): Find paths of all logs of the given
     type.

     log_dir_stream is a list of lists of log dirs. We assume that you might
     have multiple ways to fetch the same logs (e.g. from S3, or by SSHing to
     nodes), so once we find a list of log dirs that works, we stop searching.

     This yields dictionaries with the following format:

     path: path/URI of log
     application_id: (YARN application ID)
     container_id: (YARN container ID)
     job_id: (ID of job)
     task_attempt_id: (ID of task attempt)
     task_id: (ID of task)


_ls_*_logs(ls, log_dir, ...): Implementation of _find_*_logs().

    Use mrjob.logs.wrap _find_logs() to use this.


_interpret_*_logs(fs, matches, ...):

    Once we know where our logs are, search them for counters and/or errors.

    In most cases, we want to stop when we find the first error.

    counters: group -> counter -> amount
    errors: [
        hadoop_error:  (for errors internal to Hadoop)
            error: string representation of Java stack trace
            path: URI of log file containing error
            start_line: first line of <path> with error (0-indexed)
            num_lines: # of lines containing error
        task_error:   (for errors caused by one task)
            error: stderr explaining error (e.g. Python traceback)
            command: command that was run to cause this error
            path: (see above)
            start_line: (see above)
            num_lines: (see above)
        application_id: (YARN application ID)
        attempt_id: (ID of task attempt)
        container_id: (YARN container ID)
        job_id: (ID of job)
        task_id: (ID of task)
    ]

    This assumes there might be several sets of logs dirs to look in (e.g.
    the log dir on S3, directories on master and slave nodes, etc.).

_parse_*_log(lines):

    Pull important information from a log file. This generally follows the same
    format as _interpret_<type>_logs(), above.

    Log lines are always strings (see mrjob.logs.wrap._cat_log()).


_parse_*_records(lines):

    Helper method which parses low-level records out of a given log type.


There is one module for each kind of entity we want to deal with:

    counters: manipulating and printing counters
    errors: picking the best error, error reporting
    ids: handles parsing IDs and sorting IDs by recency

Finally:

    log4j: handles log4j record parsing (used by step and task syslog)
    wrap: module for listing and catting logs in an error-free
        way (since log parsing shouldn't kill a job).
"""
