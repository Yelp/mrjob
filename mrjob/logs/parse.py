# Copyright 2015 Yelp
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
import re
from logging import getLogger


_HADOOP_LOG_LINE_RE = re.compile(
    r'^(?P<timestamp>.*?)'
    r'\s+(?P<level>[A-Z]+)'
    r'\s+(?P<logger>\S+)'
    r'(\s+\((?P<thread>.*?)\))?'
    r': (?P<message>.*?)$')

_APPLICATION_ID_RE = re.compile(r'\bapplication_\d+_\d{4}\b')

_JOB_ID_RE = re.compile(r'\bjob_\d+_\d{4}\b$')

_OUTPUT_DIR_LINE_RE = re.compile(
    r'^Output( directory)?:'
    r'\s+(?P<output_dir>\S+://\S+)')

# marks start of counters
_INDENTED_COUNTERS_START_RE = re.compile('^Counters: ')

# header for a group of counters
_INDENTED_COUNTER_GROUP_RE = re.compile('^(?P<indent>\s+)(?P<group>.*)$')

# line for a counter
_INDENTED_COUNTER_RE = re.compile(
    '^(?P<indent>\s+)(?P<counter>.*)=(?P<amount>\d+)\s*$')


# TODO: move to mrjob.hadoop
_NON_HADOOP_LOG_LINE_RE = re.compile(
    r'^(packageJobJar: |Streaming Command Failed!)')


log = getLogger(__name__)


def _parse_hadoop_log_lines(lines):
    """Parse lines from a hadoop log into log4j (I think?) records.

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

        m = _HADOOP_LOG_LINE_RE.match(line)

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


def _parse_hadoop_streaming_log(lines, record_callback=None):
    """Parse lines from Hadoop's log. This is printed to stderr by the
    Hadoop streaming jar, and to the step's syslog on EMR.

    Returns a dictionary with the following keys:
    application_id: a string like 'application_1449857544442_0002'. Only
        set on YARN
    counters: a map from counter group -> counter -> amount, or None if
        no counters found (only YARN prints counters)
    job_id: a string like 'job_201512112247_0003'. Should always be set
    output_dir: a URI like 'hdfs:///user/hadoop/tmp/my-output-dir'. Should
        always be set on success.

    Optionally, call a callback function (*record_callback*) once for each
    record we parse. Anything we need to know about while the job is running
    (e.g. location of the job tracker, map/reduce %), is better handled by a
    callback.

    This doesn't properly handle the 'Streaming Command Failed!'
    printed by the Hadoop binary (you'll have to pre-filter that).
    """
    application_id = None
    counters = None
    job_id = None
    output_dir = None

    for record in _parse_hadoop_log_lines(lines):
        if record_callback:
            record_callback(record)

        message = record['message']

        if _INDENTED_COUNTERS_START_RE.match(message):
            counters = _parse_indented_counters(message.splitlines())
            continue

        m = _OUTPUT_DIR_LINE_RE.match(message)
        if m:
            output_dir = m.group('output_dir')
            continue

        # grab these wherever we see them
        m = _APPLICATION_ID_RE.search(message)
        if m:
            application_id = m.group()

        m = _JOB_ID_RE.search(message)
        if m:
            job_id = m.group()

    return dict(
        application_id=application_id,
        counters=counters,
        job_id=job_id,
        output_dir=output_dir)


def _parse_indented_counters(lines):
    """Parse counters in the indented format output/logged by the
    Hadoop binary.

    Takes input as lines (should not include log record stuff) and
    returns a map from counter group to counter to amount.

    You usually don't need to call this directly; use
    _parse_hadoop_streaming_log() instead.
    """
    counters = {}  # map group -> counter -> amount
    group = None
    group_indent = None

    for line in lines:
        if not (group is None or group_indent is None):
            m = _INDENTED_COUNTER_RE.match(line)
            if m and len(m.group('indent')) > group_indent:

                counter = m.group('counter')
                amount = int(m.group('amount'))

                counters.setdefault(group, {})
                counters[group][counter] = amount

                continue

        m = _INDENTED_COUNTER_GROUP_RE.match(line)
        if m:
            group = m.group('group')
            group_indent = len(m.group('indent'))

        elif not _INDENTED_COUNTERS_START_RE.match(line):
            log.warning('unexpected counter line: %s' % line)

    return counters
