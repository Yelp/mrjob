# Copyright 2009-2011 Yelp
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
"""Utilities for parsing errors, counters, and status messages."""
import logging
from optparse import OptionValueError
import re

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

# match the filename of a hadoop streaming jar
HADOOP_STREAMING_JAR_RE = re.compile(r'^hadoop.*streaming.*\.jar$')

# match an mrjob job name (these are used to name EMR job flows)
JOB_NAME_RE = re.compile(r'^(.*)\.(.*)\.(\d+)\.(\d+)\.(\d+)$')


log = logging.getLogger('mrjob.parse')


_HADOOP_0_20_ESCAPED_CHARS_RE = re.compile(r'\\([.(){}[\]"\\])')

def counter_unescape(escaped_string):
    """Fix names of counters and groups emitted by Hadoop 0.20+ logs, which
    use escape sequences for more characters than most decoders know about
    (e.g. ``().``).

    :param escaped_string: string from a counter log line
    :type escaped_string: str
    """
    escaped_string = escaped_string.decode('string_escape')
    escaped_string = _HADOOP_0_20_ESCAPED_CHARS_RE.sub(r'\1', escaped_string)
    return escaped_string


def find_python_traceback(lines):
    """Scan a log file or other iterable for a Python traceback,
    and return it as a list of lines.

    In logs from EMR, we find python tracebacks in ``task-attempts/*/stderr``
    """
    for line in lines:
        if line.startswith('Traceback (most recent call last):'):
            tb_lines = []
            for line in lines:
                tb_lines.append(line)
                if not line.startswith(' '):
                    break
            return tb_lines
    else:
        return None


def find_hadoop_java_stack_trace(lines):
    """Scan a log file or other iterable for a java stack trace from Hadoop,
    and return it as a list of lines.

    In logs from EMR, we find java stack traces in ``task-attempts/*/syslog``

    Sample stack trace::

        2010-07-27 18:25:48,397 WARN org.apache.hadoop.mapred.TaskTracker (main): Error running child
        java.lang.OutOfMemoryError: Java heap space
                at org.apache.hadoop.mapred.IFile$Reader.readNextBlock(IFile.java:270)
                at org.apache.hadoop.mapred.IFile$Reader.next(IFile.java:332)
                at org.apache.hadoop.mapred.Merger$Segment.next(Merger.java:147)
                at org.apache.hadoop.mapred.Merger$MergeQueue.adjustPriorityQueue(Merger.java:238)
                at org.apache.hadoop.mapred.Merger$MergeQueue.next(Merger.java:255)
                at org.apache.hadoop.mapred.Merger.writeFile(Merger.java:86)
                at org.apache.hadoop.mapred.Merger$MergeQueue.merge(Merger.java:377)
                at org.apache.hadoop.mapred.Merger.merge(Merger.java:58)
                at org.apache.hadoop.mapred.ReduceTask.run(ReduceTask.java:277)
                at org.apache.hadoop.mapred.TaskTracker$Child.main(TaskTracker.java:2216)

    (We omit the "Error running child" line from the results)
    """
    for line in lines:
        if line.rstrip('\n').endswith("Error running child"):
            st_lines = []
            for line in lines:
                st_lines.append(line)
                for line in lines:
                    if not line.startswith('        at '):
                        break
                    st_lines.append(line)
                return st_lines
    else:
        return None

_OPENING_FOR_READING_RE = re.compile("^.*: Opening '(.*)' for reading$")

def find_input_uri_for_mapper(lines):
    """Scan a log file or other iterable for the path of an input file
    for the first mapper on Hadoop. Just returns the path, or None if
    no match.

    In logs from EMR, we find python tracebacks in ``task-attempts/*/syslog``

    Matching log lines look like::

        2010-07-27 17:54:54,344 INFO org.apache.hadoop.fs.s3native.NativeS3FileSystem (main): Opening 's3://yourbucket/logs/2010/07/23/log2-00077.gz' for reading
    """
    for line in lines:
        match = _OPENING_FOR_READING_RE.match(line)
        if match:
            return match.group(1)
    else:
        return None


_HADOOP_STREAMING_ERROR_RE = re.compile(r'^.*ERROR org\.apache\.hadoop\.streaming\.StreamJob \(main\): (.*)$')

def find_interesting_hadoop_streaming_error(lines):
    """Scan a log file or other iterable for a hadoop streaming error
    other than "Job not Successful!". Return the error as a string, or None
    if nothing found.

    In logs from EMR, we find java stack traces in ``steps/*/syslog``

    Example line::

        2010-07-27 19:53:35,451 ERROR org.apache.hadoop.streaming.StreamJob (main): Error launching job , Output path already exists : Output directory s3://yourbucket/logs/2010/07/23/ already exists and is not empty
    """
    for line in lines:
        match = _HADOOP_STREAMING_ERROR_RE.match(line)
        if match:
            msg = match.group(1)
            if msg != 'Job not Successful!':
                return msg
    else:
        return None


_TIMEOUT_ERROR_RE = re.compile(r'Task.*?TASK_STATUS="FAILED".*?ERROR=".*?failed to report status for (\d+) seconds. Killing!"')

def find_timeout_error(lines):
    """Scan a log file or other iterable for a timeout error from Hadoop.
    Return the number of seconds the job ran for before timing out, or None if
    nothing found.

    In logs from EMR, we find timeouterrors in ``jobs/*.jar``

    Example line::

        Task TASKID="task_201010202309_0001_m_000153" TASK_TYPE="MAP" TASK_STATUS="FAILED" FINISH_TIME="1287618918658" ERROR="Task attempt_201010202309_0001_m_000153_3 failed to report status for 602 seconds. Killing!"
    """
    result = None
    for line in lines:
        match = _TIMEOUT_ERROR_RE.match(line)
        if match:
            result = match.group(1)
    return int(result)


# recognize hadoop streaming output
_COUNTER_RE = re.compile(r'reporter:counter:([^,]*),([^,]*),(-?\d+)$')
_STATUS_RE = re.compile(r'reporter:status:(.*)$')

def parse_mr_job_stderr(stderr, counters=None):
    """Parse counters and status messages out of MRJob output.

    :param data: a filehandle, a list of lines, or a str containing data
    :type counters: Counters so far, to update; a map from group to counter name to count.

    Returns a dictionary with the keys *counters*, *statuses*, *other*:

    - *counters*: counters so far; same format as above
    - *statuses*: a list of status messages encountered
    - *other*: lines that aren't either counters or status messages
    """
    # For the corresponding code in Hadoop Streaming, see ``incrCounter()`` in
    # http://svn.apache.org/viewvc/hadoop/mapreduce/trunk/src/contrib/streaming/src/java/org/apache/hadoop/streaming/PipeMapRed.java?view=markup
    if isinstance(stderr, str):
        stderr = StringIO(stderr)

    if counters is None:
        counters = {}
    statuses = []
    other = []

    for line in stderr:
        m = _COUNTER_RE.match(line)
        if m:
            group, counter, amount_str = m.groups()
            counters.setdefault(group, {})
            counters[group].setdefault(counter, 0)
            counters[group][counter] += int(amount_str)
            continue

        m = _STATUS_RE.match(line)
        if m:
            statuses.append(m.group(1))
            continue

        other.append(line)

    return {'counters': counters, 'statuses': statuses, 'other': other}


# Match a job output line containing counter data.
# The line is of the form 
# "Job KEY="value" KEY2="value2" ... COUNTERS="<counter_string>"
# We just want to pull out the counter string, which varies between 
# Hadoop versions.
_KV_EXPR = r'\s+\w+=".*?"'  # this matches KEY="VALUE"
_COUNTER_LINE_EXPR = r'Job(%s)*\s+COUNTERS="%s"' % (_KV_EXPR,
                                                    r'(?P<counters>.*?)')
_COUNTER_LINE_RE = re.compile(_COUNTER_LINE_EXPR)

# 0.18-specific
# see _parse_counters_0_18 for format
# A counter looks like this: groupname.countername:countervalue
_COUNTER_EXPR_0_18 = r'(?P<group>[^,]+?)[.](?P<name>.+?):(?P<value>\d+)'
_COUNTER_RE_0_18 = re.compile(_COUNTER_EXPR_0_18)

# aggregate 'is this 0.18?' expression
# these are comma-separated counter expressions (see _COUNTER_EXPR_0_18)
_ONE_0_18_COUNTER = r'[^,]+?[.].+?:\d+'
_0_18_EXPR = r'(%s)(,%s)*' % (_ONE_0_18_COUNTER, _ONE_0_18_COUNTER)
_COUNTER_FORMAT_IS_0_18 = re.compile(_0_18_EXPR)

# 0.20-specific

# capture one group including sub-counters
# these look like: {(gid)(gname)[...][...][...]...}
_COUNTER_LIST_EXPR = r'(?P<counter_list_str>\[.*?\])'
_GROUP_RE_0_20 = re.compile(r'{\(%s\)\(%s\)%s}' % (r'(?P<group_id>.*?)',
                                                   r'(?P<group_name>.*?)',
                                                   _COUNTER_LIST_EXPR))

# capture a single counter from a group
# this is what the ... is in _COUNTER_LIST_EXPR (incl. the brackets).
# it looks like: [(cid)(cname)(value)]
_COUNTER_VALUE_EXPR = r'(?P<counter_value>\d+)'
_COUNTER_0_20_EXPR = r'\[\(%s\)\(%s\)\(%s\)\]' % (r'(?P<counter_id>.*?)',
                                                  r'(?P<counter_name>.*?)',
                                                  _COUNTER_VALUE_EXPR)
_COUNTER_RE_0_20 = re.compile(_COUNTER_0_20_EXPR)

# aggregate 'is this 0.20?' expression
# combines most of the rules from the capturing expressions above
# should match strings like: {(gid)(gname)[(cid)(cname)(cvalue)][...]...}
_0_20_EXPR = r'{\(%s\)\(%s\)(%s)*' % (r'.*?',
                                      r'.*?',
                                      _COUNTER_0_20_EXPR)
_COUNTER_FORMAT_IS_0_20 = re.compile(_0_20_EXPR)

def _parse_counters_0_18(counter_string):
    # 0.18 counters look like this:
    # GroupName.CounterName:Value,GroupName.Crackers:3,AnotherGroup.Nerf:243,... 
    matches = _COUNTER_RE_0_18.findall(counter_string)
    for group, name, amount_str in matches:
        yield group, name, int(amount_str)


def _parse_counters_0_20(group_string):
    # 0.20 counters look like this:
    # {(groupid)(groupname)[(counterid)(countername)(countervalue)][...]...} 
    for group_id, group_name, counter_str in _GROUP_RE_0_20.findall(group_string):
        matches = _COUNTER_RE_0_20.findall(counter_str)
        for counter_id, counter_name, counter_value in matches:
            try:
                group_name = counter_unescape(group_name)
            except ValueError:
                log.warn("Could not decode group name %s" % group_name)

            try:
                counter_name = counter_unescape(counter_name)
            except ValueError:
                log.warn("Could not decode counter name %s" % counter_name)

            yield group_name, counter_name, int(counter_value)


def parse_hadoop_counters_from_line(line):
    """Parse Hadoop counter values from a log line.

    The counter log line format changed significantly between Hadoop 0.18 and
    0.20, so this function switches between parsers for them.

    :param line: log line containing counter data
    :type line: str
    :param hadoop_version: Version of Hadoop that produced the log files because they are formatted differently.
    :type hadoop_version: str
    """
    m = _COUNTER_LINE_RE.match(line)
    if not m:
        return None

    parser_switch = (
        (_COUNTER_FORMAT_IS_0_18, _parse_counters_0_18),
        (_COUNTER_FORMAT_IS_0_20, _parse_counters_0_20),
    )

    counter_substring = m.group('counters')

    correct_func = None
    for regex, func in parser_switch:
        if regex.match(counter_substring):
            correct_func = func
            break

    if correct_func is None:
        log.warn('Cannot parse Hadoop counter line: %s' % line)
        return None

    counters = {}
    for group, counter, value in correct_func(counter_substring):
        counters.setdefault(group, {})
        counters[group].setdefault(counter, 0)
        counters[group][counter] += int(value)
    return counters


def parse_port_range_list(range_list_str):
    all_ranges = []
    for range_str in range_list_str.split(','):
        if ':' in range_str:
            a, b = [int(x) for x in range_str.split(':')]
            all_ranges.extend(range(a, b+1))
        else:
            all_ranges.append(int(range_str))
    return all_ranges


def check_kv_pair(option, opt, value):
    items = value.split('=', 1)
    if len(items) == 2:
        return items
    else:
        raise OptionValueError(
            "option %s: value is not of the form KEY=VALUE: %r" % (opt, value))


def check_range_list(option, opt, value):
    try:
        ports = parse_port_range_list(value)
        return ports
    except ValueError, e:
        raise OptionValueError('option %s: invalid port range list "%s": \n%s' % (opt, value, e.args[0]))

