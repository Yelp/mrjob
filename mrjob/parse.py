# Copyright 2009-2012 Yelp
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
from functools import wraps
import logging
import re
from urlparse import ParseResult
from urlparse import urlparse as urlparse_buggy

try:
    from cStringIO import StringIO
    StringIO  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    from StringIO import StringIO

from mrjob.compat import uses_020_counters


# match the filename of a hadoop streaming jar
HADOOP_STREAMING_JAR_RE = re.compile(r'^hadoop.*streaming.*\.jar$')

# match an mrjob job name (these are used to name EMR job flows)
JOB_NAME_RE = re.compile(r'^(.*)\.(.*)\.(\d+)\.(\d+)\.(\d+)$')

# match an mrjob step name (these are used to name steps in EMR)
STEP_NAME_RE = re.compile(
    r'^(.*)\.(.*)\.(\d+)\.(\d+)\.(\d+): Step (\d+) of (\d+)$')

log = logging.getLogger(__name__)


### URI PARSING ###


# Used to parse the real netloc out of a malformed path from Python 2.5
# urlparse()
NETLOC_RE = re.compile(r'//(.*?)((/.*?)?)$')


def is_uri(uri):
    """Return True if *uri* is any sort of URI."""
    return bool(urlparse(uri).scheme)


def is_s3_uri(uri):
    """Return True if *uri* can be parsed into an S3 URI, False otherwise."""
    try:
        parse_s3_uri(uri)
        return True
    except ValueError:
        return False


def parse_s3_uri(uri):
    """Parse an S3 URI into (bucket, key)

    >>> parse_s3_uri('s3://walrus/tmp/')
    ('walrus', 'tmp/')

    If ``uri`` is not an S3 URI, raise a ValueError
    """
    components = urlparse(uri)
    if (components.scheme not in ('s3', 's3n')
            or '/' not in components.path):
        raise ValueError('Invalid S3 URI: %s' % uri)

    return components.netloc, components.path[1:]


@wraps(urlparse_buggy)
def urlparse(urlstring, scheme='', allow_fragments=True, *args, **kwargs):
    """A wrapper for :py:func:`urlparse.urlparse` with the following
    differences:

    * Handles buckets in S3 URIs correctly. (:py:func:`~urlparse.urlparse`
      does this correctly sometime after 2.6.1; this is just a patch for older
      Python versions.)
    * Splits the fragment correctly in all URIs, not just Web-related ones.
      This behavior was fixed in the Python 2.7.4 standard library but we have
      to back-port it for previous versions.
    """
    # we're probably going to mess with at least one of these values and
    # re-pack the whole thing before we return it.
    # NB: urlparse_buggy()'s second argument changes names from
    # 'default_scheme' to 'scheme' in Python 2.6, so urlparse_buggy() should
    # be called with positional arguments.
    (scheme, netloc, path, params, query, fragment) = (
        urlparse_buggy(urlstring, scheme, allow_fragments, *args, **kwargs))
    if netloc == '' and path.startswith('//'):
        m = NETLOC_RE.match(path)
        netloc = m.group(1)
        path = m.group(1)
    if allow_fragments and '#' in path and not fragment:
        path, fragment = path.split('#', 1)
    return ParseResult(scheme, netloc, path, params, query, fragment)


### OPTION PARSING ###

def parse_port_range_list(range_list_str):
    """Parse a port range list of the form (start[:end])(,(start[:end]))*"""
    all_ranges = []
    for range_str in range_list_str.split(','):
        if ':' in range_str:
            a, b = [int(x) for x in range_str.split(':')]
            all_ranges.extend(xrange(a, b + 1))
        else:
            all_ranges.append(int(range_str))
    return all_ranges


def parse_key_value_list(kv_string_list, error_fmt, error_func):
    """Parse a list of strings like ``KEY=VALUE`` into a dictionary.

    :param kv_string_list: Parse a list of strings like ``KEY=VALUE`` into a
                           dictionary.
    :type kv_string_list: [str]
    :param error_fmt: Format string accepting one ``%s`` argument which is the
                      malformed (i.e. not ``KEY=VALUE``) string
    :type error_fmt: str
    :param error_func: Function to call when a malformed string is encountered.
    :type error_func: function(str)
    """
    ret = {}
    for value in kv_string_list:
        try:
            k, v = value.split('=', 1)
            ret[k] = v
        except ValueError:
            error_func(error_fmt % (value,))
    return ret


### LOG PARSING ###


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
    # Lines to pass back representing entire error found
    all_tb_lines = []

    # This is used to store a working list of lines in a single traceback
    tb_lines = []

    # This is used to store a working list of non-traceback lines between the
    # current traceback and the previous one
    non_tb_lines = []

    # Track whether or not we are in a traceback rather than consuming the
    # iterator
    in_traceback = False

    for line in lines:
        if in_traceback:
            tb_lines.append(line)

            # If no indentation, this is the last line of the traceback
            if line.lstrip() == line:
                in_traceback = False

                if line.startswith('subprocess.CalledProcessError'):
                    # CalledProcessError may mean that the subprocess printed
                    # errors to stderr which we can show the user
                    all_tb_lines += non_tb_lines

                all_tb_lines += tb_lines

                # Reset all working lists
                tb_lines = []
                non_tb_lines = []
        else:
            if line.startswith('Traceback (most recent call last):'):
                tb_lines.append(line)
                in_traceback = True
            else:
                non_tb_lines.append(line)
    if all_tb_lines:
        return all_tb_lines
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
        if line.rstrip('\r\n').endswith("Error running child"):
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
    val = None
    for line in lines:
        match = _OPENING_FOR_READING_RE.match(line)
        if match:
            val = match.group(1)
    return val


_HADOOP_STREAMING_ERROR_RE = re.compile(
    r'^.*ERROR org\.apache\.hadoop\.streaming\.StreamJob \(main\): (.*)$')
_HADOOP_STREAMING_ERROR_RE_2 = re.compile(r'^(.*does not exist.*)$')


def find_interesting_hadoop_streaming_error(lines):
    """Scan a log file or other iterable for a hadoop streaming error
    other than "Job not Successful!". Return the error as a string, or None
    if nothing found.

    In logs from EMR, we find java stack traces in ``steps/*/syslog``

    Example line::

        2010-07-27 19:53:35,451 ERROR org.apache.hadoop.streaming.StreamJob (main): Error launching job , Output path already exists : Output directory s3://yourbucket/logs/2010/07/23/ already exists and is not empty
    """
    for line in lines:
        match = (
            _HADOOP_STREAMING_ERROR_RE.match(line) or
            _HADOOP_STREAMING_ERROR_RE_2.match(line))
        if match:
            msg = match.group(1)
            if msg != 'Job not Successful!':
                return msg
    return None


_MULTILINE_JOB_LOG_ERROR_RE = re.compile(
    r'^\w+Attempt.*?TASK_STATUS="FAILED".*?ERROR="(?P<first_line>[^"]*)$')


def find_job_log_multiline_error(lines):
    """Scan a log file for an arbitrary multi-line error. Return it as a list
    of lines, or None of nothing was found.

    Here is an example error::

        MapAttempt TASK_TYPE="MAP" TASKID="task_201106280040_0001_m_000218" TASK_ATTEMPT_ID="attempt_201106280040_0001_m_000218_5" TASK_STATUS="FAILED" FINISH_TIME="1309246900665" HOSTNAME="/default-rack/ip-10-166-239-133.us-west-1.compute.internal" ERROR="Error initializing attempt_201106280040_0001_m_000218_5:
        java.io.IOException: Cannot run program "bash": java.io.IOException: error=12, Cannot allocate memory
            at java.lang.ProcessBuilder.start(ProcessBuilder.java:460)
            at org.apache.hadoop.util.Shell.runCommand(Shell.java:149)
            at org.apache.hadoop.util.Shell.run(Shell.java:134)
            at org.apache.hadoop.fs.DF.getAvailable(DF.java:73)
            at org.apache.hadoop.fs.LocalDirAllocator$AllocatorPerContext.getLocalPathForWrite(LocalDirAllocator.java:296)
            at org.apache.hadoop.fs.LocalDirAllocator.getLocalPathForWrite(LocalDirAllocator.java:124)
            at org.apache.hadoop.mapred.TaskTracker.localizeJob(TaskTracker.java:648)
            at org.apache.hadoop.mapred.TaskTracker.startNewTask(TaskTracker.java:1320)
            at org.apache.hadoop.mapred.TaskTracker.offerService(TaskTracker.java:956)
            at org.apache.hadoop.mapred.TaskTracker.run(TaskTracker.java:1357)
            at org.apache.hadoop.mapred.TaskTracker.main(TaskTracker.java:2361)
        Caused by: java.io.IOException: java.io.IOException: error=12, Cannot allocate memory
            at java.lang.UNIXProcess.<init>(UNIXProcess.java:148)
            at java.lang.ProcessImpl.start(ProcessImpl.java:65)
            at java.lang.ProcessBuilder.start(ProcessBuilder.java:453)
            ... 10 more
        "

    The first line returned will only include the text after ``ERROR="``, and
    discard the final line with just ``"``.

    These errors are parsed from jobs/\*.jar.
    """
    for line in lines:
        m = _MULTILINE_JOB_LOG_ERROR_RE.match(line)
        if m:
            st_lines = []
            if m.group('first_line'):
                st_lines.append(m.group('first_line'))
            for line in lines:
                st_lines.append(line)
                for line in lines:
                    if line.strip() == '"':
                        break
                    st_lines.append(line)
                return st_lines
    return None


_TIMEOUT_ERROR_RE = re.compile(
    r'.*?TASK_STATUS="FAILED".*?ERROR=".*?failed to report status for (\d+)'
    r' seconds.*?"')


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
    if result is None:
        return None
    else:
        return int(result)


# recognize hadoop streaming output
_COUNTER_RE = re.compile(r'^reporter:counter:([^,]*),([^,]*),(-?\d+)$')
_STATUS_RE = re.compile(r'^reporter:status:(.*)$')


def parse_mr_job_stderr(stderr, counters=None):
    """Parse counters and status messages out of MRJob output.

    :param data: a filehandle, a list of lines, or a str containing data
    :type counters: Counters so far, to update; a map from group to counter
                    name to count.

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
        m = _COUNTER_RE.match(line.rstrip('\r\n'))
        if m:
            group, counter, amount_str = m.groups()
            counters.setdefault(group, {})
            counters[group].setdefault(counter, 0)
            counters[group][counter] += int(amount_str)
            continue

        m = _STATUS_RE.match(line.rstrip('\r\n'))
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
_COUNTER_LINE_EXPR = r'^.*?JOBID=".*?_%s".*?\bCOUNTERS="%s".*?$' % \
    ('(?P<step_num>\d+)', r'(?P<counters>.*?)')
_COUNTER_LINE_RE = re.compile(_COUNTER_LINE_EXPR)

# 0.18-specific
# see _parse_counters_0_18 for format
# A counter looks like this: groupname.countername:countervalue
_COUNTER_EXPR_0_18 = r'(,|^)(?P<group>[^,]+?)[.](?P<name>[^,]+):(?P<value>\d+)'
_COUNTER_RE_0_18 = re.compile(_COUNTER_EXPR_0_18)

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
_COUNTER_0_20_EXPR = r'\[\(%s\)\(%s\)\(%s\)\]' % (r'(?P<counter_id>.*?)',
                                                  r'(?P<counter_name>.*?)',
                                                  r'(?P<counter_value>\d+)')
_COUNTER_RE_0_20 = re.compile(_COUNTER_0_20_EXPR)


def _parse_counters_0_18(counter_string):
    # 0.18 counters look like this:
    # GroupName.CounterName:Value,Group1.Crackers:3,Group2.Nerf:243,...
    groups = _COUNTER_RE_0_18.finditer(counter_string)
    if groups is None:
        log.warn('Cannot parse Hadoop counter string: %s' % counter_string)

    for m in groups:
        yield m.group('group'), m.group('name'), int(m.group('value'))


def _parse_counters_0_20(counter_string):
    # 0.20 counters look like this:
    # {(groupid)(groupname)[(counterid)(countername)(countervalue)][...]...}
    groups = _GROUP_RE_0_20.findall(counter_string)
    if not groups:
        log.warn('Cannot parse Hadoop counter string: %s' % counter_string)

    for group_id, group_name, counter_str in groups:
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


def parse_hadoop_counters_from_line(line, hadoop_version=None):
    """Parse Hadoop counter values from a log line.

    The counter log line format changed significantly between Hadoop 0.18 and
    0.20, so this function switches between parsers for them.

    :param line: log line containing counter data
    :type line: str

    :return: (counter_dict, step_num) or (None, None)
    """
    m = _COUNTER_LINE_RE.match(line)
    if not m:
        return None, None

    if hadoop_version is None:
        # try both if hadoop_version not specified
        counters_1, step_num_1 = parse_hadoop_counters_from_line(line, '0.20')
        if counters_1:
            return (counters_1, step_num_1)
        else:
            return parse_hadoop_counters_from_line(line, '0.18')

    if uses_020_counters(hadoop_version):
        parse_func = _parse_counters_0_20
    else:
        parse_func = _parse_counters_0_18

    counter_substring = m.group('counters')

    counters = {}
    for group, counter, value in parse_func(counter_substring):
        counters.setdefault(group, {})
        counters[group].setdefault(counter, 0)
        counters[group][counter] += int(value)
    return counters, int(m.group('step_num'))
