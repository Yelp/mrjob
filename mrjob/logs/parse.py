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

from .log4j import _parse_hadoop_log4j_records


_APPLICATION_ID_RE = re.compile(r'\bapplication_\d+_\d{4}\b')

_JOB_ID_RE = re.compile(r'\bjob_\d+_\d{4}\b$')

_OUTPUT_DIR_LINE_RE = re.compile(
    r'^Output( directory)?:'
    r'\s+(?P<output_dir>\S+://\S+)')

# marks start of counters message
_INDENTED_COUNTERS_START_RE = re.compile(r'^Counters: ')

# header for a group of counters
_INDENTED_COUNTER_GROUP_RE = re.compile(r'^(?P<indent>\s+)(?P<group>.*)$')

# line for a counter
_INDENTED_COUNTER_RE = re.compile(
    r'^(?P<indent>\s+)(?P<counter>.*)=(?P<amount>\d+)\s*$')

# message telling us about a (input) split. Looks like this:
#
# Processing split: hdfs://ddf64167693a:9000/path/to/bootstrap.sh:0+335
_YARN_INPUT_SPLIT_RE = re.compile(
    r'^Processing split:\s+(?P<path>.*)'
    r':(?P<start_line>\d+)\+(?P<num_lines>\d+)$')

# this seems to only happen for S3. Not sure if this happens in YARN
_OPENING_FOR_READING_RE = re.compile(
    r"^Opening '(?P<path>.*?)' for reading$")


# start of message telling us about a Java stacktrace on YARN
_YARN_JAVA_EXCEPTION_HEADER_RE = re.compile(
    r'^Exception running child\s?:\s+(?P<exception>.*)$')

# start of message telling us about a Java stacktrace pre-YARN
# (the exception is on the next line)
_PRE_YARN_JAVA_ERROR_HEADER_RE = re.compile(r'^Error running child$')

# start of line telling us about Python exception
_PYTHON_EXCEPTION_HEADER_RE = re.compile(
    r'^Traceback \(most recent call last\):$')

# once we see the exception header, every line starting with whitespace
# is part of the traceback
_PYTHON_TRACEBACK_LINE_RE = re.compile(r'^\s+')

# escape sequence in pre-YARN history file. Characters inside COUNTERS
# fields are double escaped
_PRE_YARN_HISTORY_ESCAPE_RE = re.compile(r'\\(.)')

# capture key-value pairs like JOBNAME="streamjob8025762403845318969\.jar"
_PRE_YARN_HISTORY_KEY_PAIR = re.compile(
    r'(?P<key>\w+)="(?P<escaped_value>(\\.|[^"\\])*)"', re.MULTILINE)

# an entire line in a pre-YARN history file
_PRE_YARN_HISTORY_RECORD = re.compile(
    r'^(?P<type>\w+)'
    r'(?P<key_pairs>( ' + _PRE_YARN_HISTORY_KEY_PAIR.pattern + ')*)'
    r' \.$', re.MULTILINE)

# capture one group of counters
# this looks like: {(group_id)(group_name)[counter][counter]...}
_PRE_YARN_COUNTER_GROUP_RE = re.compile(
    r'{\('
    r'(?P<group_id>(\\.|[^)}\\])*)'
    r'\)\('
    r'(?P<group_name>(\\.|[^)}\\])*)'
    r'\)'
    r'(?P<counter_list_str>\[(\\.|[^}\\])*\])'
    r'}')

# parse a single counter from a counter group (counter_list_str above)
# this looks like: [(counter_id)(counter_name)(amount)]
_PRE_YARN_COUNTER_RE = re.compile(
    r'\[\('
    r'(?P<counter_id>(\\.|[^)\\])*)'
    r'\)\('
    r'(?P<counter_name>(\\.|[^)\\])*)'
    r'\)\('
    r'(?P<amount>\d+)'
    r'\)\]')


log = getLogger(__name__)


def _parse_task_syslog(lines):
    """Parse out last Java stacktrace (if any) and last split (if any)
    from syslog file.

    Returns a dictionary with the keys 'error' and 'split':

    error: optional (may be None) dictionary with keys:
        exception: string
        stack_trace: [lines]
    split: optional (may be None) dictionary with the keys:
       path: URI of input file
       start_line: optional first line of split (0-indexed)
       num_lines: optional number of lines in split
    """
    # TODO: just make error a string
    result = dict(error=None, split=None)

    for record in _parse_hadoop_log4j_records(lines):
        message = record['message']

        m = _OPENING_FOR_READING_RE.match(message)
        if m:
            result['split'] = dict(
                path = m.group('path'),
                start_line=None,
                num_lines=None)
            continue

        # doesn't really hurt to try this on non-YARN logs
        m = _YARN_INPUT_SPLIT_RE.match(message)
        if m:
            result['split'] = dict(
                path=m.group('path'),
                start_line=int(m.group('start_line')),
                num_lines=int(m.group('num_lines')))
            continue

        message_lines = message.splitlines()
        if not message_lines:
            continue

        # TODO: could also generalize this by looking for exception lines
        # ("    at ...(...:###)") rather than a Hadoop-version-specific header
        m = _YARN_JAVA_EXCEPTION_HEADER_RE.match(message_lines[0])
        if m:
            result['error'] = dict(
                exception=m.group('exception'),
                stack_trace=message_lines[1:])
            continue

        if (_PRE_YARN_JAVA_ERROR_HEADER_RE.match(message_lines[0])
            and len(message_lines) > 1):
            result['error'] = dict(
                exception=message_lines[1],
                stack_trace=message_lines[2:])
            continue


    return result


def _parse_python_task_stderr(lines):
    """Parse out the python stacktrace and exception, if any.

    Returns a dictionary with the optional (may be None) key 'error':

    error: dictionary with keys:
        exception: string
        traceback: [lines]
    """
    # stderr can also contain the java Exception (or a warning that no
    # appenders can be found for the logger), but there's not much point
    # in parsing this as we already get it from syslog

    # TODO: handle errors from the setup script (see #1203)

    # TODO: just make error a string, try to find command as well
    result = dict(error=None)

    traceback = None

    for line in lines:
        line = line.rstrip('\r\n')

        if traceback is None:
            if _PYTHON_EXCEPTION_HEADER_RE.match(line):
                traceback = [line]
            # otherwise this line is uninteresting
        else:
            # parsing traceback
            if _PYTHON_TRACEBACK_LINE_RE.match(line):
                traceback.append(line)
            else:
                result['error'] = dict(
                    exception=line,
                    traceback=traceback)
                traceback = None  # done parsing traceback

    return result
