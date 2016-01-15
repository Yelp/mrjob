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


def _parse_hadoop_log4j_records(lines):
    """Parse lines from a hadoop log into log4j records.

    Yield dictionaries with the following keys:
    timestamp -- unparsed timestamp, e.g. '15/12/07 20:49:28',
        '2015-08-22 00:46:18,411'
    level -- e.g. 'INFO'
    logger -- e.g. 'amazon.emr.metrics.MetricsSaver'
    thread -- e.g. 'main'. May be None
    message -- the actual message. If this is a multi-line message (e.g.
        for counters), the lines will be joined by '\n'

    Trailing \r and \n will be stripped from lines.
    """
    last_record = None

    for line in lines:
        line = line.rstrip('\r\n')

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
