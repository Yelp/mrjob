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
"""Parse the log4j syslog format used by Hadoop."""
import re
from logging import getLogger

# log line format output by hadoop jar command
_HADOOP_LOG4J_LINE_RE = re.compile(
    r'^(?P<timestamp>.*?)'
    r'\s+(?P<level>[A-Z]+)'
    r'\s+(?P<logger>\S+)'
    r'(\s+\((?P<thread>.*?)\))?'
    r': (?P<message>.*?)$')

# log line format output to Hadoop syslog
_HADOOP_LOG4J_LINE_ALTERNATE_RE = re.compile(
    r'^(?P<timestamp>.*?)'
    r'\s+(?P<level>[A-Z]+)'
    r'(\s+\[(?P<thread>.*?)\])'
    r'\s+(?P<logger>\S+)'
    r': (?P<message>.*?)$')

log = getLogger(__name__)


# TODO: add line numbers
def _parse_hadoop_log4j_records(lines, pre_filter=None):
    """Parse lines from a hadoop log into log4j records.

    Yield dictionaries with the following keys:
    level -- e.g. 'INFO'
    logger -- e.g. 'amazon.emr.metrics.MetricsSaver'
    message -- the actual message. If this is a multi-line message (e.g.
        for counters), the lines will be joined by '\n'
    thread -- e.g. 'main'. May be None
    timestamp -- unparsed timestamp, e.g. '15/12/07 20:49:28',
        '2015-08-22 00:46:18,411'

    Trailing \r and \n will be stripped from lines.

    If set, *pre_filter* will be applied to stripped lines. If it
    returns a true-ish value, that value will be yielded.
    """
    last_record = None

    for line in lines:
        line = line.rstrip('\r\n')

        # had to patch this in here to get _parse_hadoop_jar_command_stderr()'s
        # record_callback to fire on the correct line. The problem is that
        # we don't emit records until we see the next line (to handle
        # multiline records)
        if pre_filter:
            fake_record = pre_filter(line)
            if fake_record:
                if last_record:
                    yield last_record
                yield fake_record
                last_record = None
                continue

        m = (_HADOOP_LOG4J_LINE_RE.match(line) or
             _HADOOP_LOG4J_LINE_ALTERNATE_RE.match(line))

        if m:
            if last_record:
                yield last_record
            last_record = m.groupdict()
        else:
            # add on to previous record
            if last_record:
                last_record['message'] += '\n' + line
            else:
                log.warning('unexpected log line: %s' % line)

    if last_record:
        yield last_record
