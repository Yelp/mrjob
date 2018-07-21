# Copyright 2009-2012 Yelp
# Copyright 2013 Steve Johnson and David Marin
# Copyright 2014 Yelp and Contributors
# Copyright 2015-2016 Yelp
# Copyright 2017 Yelp
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
import calendar
import logging
import re
import time
from datetime import datetime
from functools import wraps
from io import BytesIO

from mrjob.py2 import ParseResult
from mrjob.py2 import to_string
from mrjob.py2 import urlparse as urlparse_buggy

try:
    import boto.utils
except ImportError:
    # don't require boto; MRJobs don't actually need it when running
    # inside hadoop streaming
    boto = None

log = logging.getLogger(__name__)


### URI PARSING ###

# Used to check if the candidate uri is actually a local windows path.
_WINPATH_RE = re.compile(r"^[aA-zZ]:\\")


def is_windows_path(uri):
    """Return True if *uri* is a windows path.

    .. deprecated:: 0.5.7
    """
    log.warning(
        'is_windows_path() is deprecated and will be removed in v0.6.0')

    if _WINPATH_RE.match(uri):
        return True
    else:
        return False


def is_uri(uri):
    """Return True if *uri* is a URI and contains ``://``
    (we only care about URIs that can describe files)

    .. versionchanged:: 0.5.7

       used to recognize anything containing a colon as a URI
       unless it was a Windows path (``C:\...``).
    """
    return '://' in uri and bool(urlparse(uri).scheme)


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
    if (components.scheme not in ('s3', 's3n', 's3a') or
        '/' not in components.path):  # noqa

        raise ValueError('Invalid S3 URI: %s' % uri)

    return components.netloc, components.path[1:]


@wraps(urlparse_buggy)
def urlparse(urlstring, scheme='', allow_fragments=True, *args, **kwargs):
    """A wrapper for :py:func:`urlparse.urlparse` that splits the fragment
    correctly in all URIs, not just Web-related ones.
    This behavior was fixed in the Python 2.7.4 standard library but we have
    to back-port it for previous versions.
    """
    (scheme, netloc, path, params, query, fragment) = (
        urlparse_buggy(urlstring, scheme, allow_fragments, *args, **kwargs))

    if allow_fragments and '#' in path and not fragment:
        path, fragment = path.split('#', 1)

    return ParseResult(scheme, netloc, path, params, query, fragment)


### OPTION PARSING ###

def parse_port_range_list(range_list_str):
    """Parse a port range list of the form ``(start[:end])(,(start[:end]))*``

    .. deprecated:: 0.5.7
    """
    log.warning('parse_key_value_list() is deprecated and will be removed'
                ' in v0.6.0')
    return _parse_port_range_list(range_list_str)


# planning to move this into mrjob.options
def _parse_port_range_list(range_list_str):
    all_ranges = []
    for range_str in range_list_str.split(','):
        if ':' in range_str:
            a, b = [int(x) for x in range_str.split(':')]
            all_ranges.extend(range(a, b + 1))
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

    .. deprecated:: 0.5.7
    """
    log.warning('parse_key_value_list() is deprecated and will be removed'
                ' in v0.6.0')

    ret = {}
    for value in kv_string_list:
        try:
            k, v = value.split('=', 1)
            ret[k] = v
        except ValueError:
            error_func(error_fmt % (value,))
    return ret


### parsing job output/stderr ###

_COUNTER_RE = re.compile(br'^reporter:counter:([^,]*),([^,]*),(-?\d+)$')
_STATUS_RE = re.compile(br'^reporter:status:(.*)$')


def parse_mr_job_stderr(stderr, counters=None):
    """Parse counters and status messages out of MRJob output.

    :param stderr: a filehandle, a list of lines (bytes), or bytes
    :param counters: Counters so far, to update; a map from group (string to
                     counter name (string) to count.

    Returns a dictionary with the keys *counters*, *statuses*, *other*:

    - *counters*: counters so far; same format as above
    - *statuses*: a list of status messages encountered
    - *other*: lines (strings) that aren't either counters or status messages
    """
    # For the corresponding code in Hadoop Streaming, see ``incrCounter()`` in
    # http://svn.apache.org/viewvc/hadoop/mapreduce/trunk/src/contrib/streaming/src/java/org/apache/hadoop/streaming/PipeMapRed.java?view=markup  # noqa
    if isinstance(stderr, bytes):
        stderr = BytesIO(stderr)

    if counters is None:
        counters = {}
    statuses = []
    other = []

    for line in stderr:
        m = _COUNTER_RE.match(line.rstrip(b'\r\n'))
        if m:
            group, counter, amount_str = m.groups()

            # don't leave these as bytes on Python 3
            group = to_string(group)
            counter = to_string(counter)

            counters.setdefault(group, {})
            counters[group].setdefault(counter, 0)
            counters[group][counter] += int(amount_str)
            continue

        m = _STATUS_RE.match(line.rstrip(b'\r\n'))
        if m:
            # don't leave as bytes on Python 3
            statuses.append(to_string(m.group(1)))
            continue

        other.append(to_string(line))

    return {'counters': counters, 'statuses': statuses, 'other': other}


def _find_python_traceback(lines):
    """Scan subprocess stderr for Python traceback."""
    # Essentially, we detect the start of the traceback, and continue
    # until we find a non-indented line, with some special rules for exceptions
    # from subprocesses.

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
        # don't return bytes in Python 3
        line = to_string(line)

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


### job tracker/resource manager ###

_JOB_TRACKER_HTML_RE = re.compile(br'\b(\d{1,3}\.\d{2})%')
_RESOURCE_MANAGER_JS_RE = re.compile(
    br'\s*\[.*application_[_\d]+.*"RUNNING"'
    br'.*width:(?P<percent>\d{1,3}.\d)%.*\]'
)


def _parse_progress_from_job_tracker(html_bytes):
    """Pull (map_percent, reduce_percent) from running job from job tracker
    HTML as floats, or return (None, None).

    This assumes at most one running job (designed for EMR).
    """
    # snip out the Running Jobs section (ignore the header)
    start = html_bytes.rfind(b'Running Jobs')
    if start == -1:
        return None, None
    end = html_bytes.find(b'Jobs', start + len(b'Running Jobs'))
    if end == -1:
        end = None

    html_bytes = html_bytes[start:end]

    # search it for percents
    matches = _JOB_TRACKER_HTML_RE.findall(html_bytes)
    if len(matches) >= 2:
        return float(matches[0]), float(matches[1])
    else:
        return None, None


def _parse_progress_from_resource_manager(html_bytes):
    """Pull progress_precent for running job from job tracker HTML, as a
    float, or return None.

    This assumes at most one running job (designed for EMR).
    """
    # this is for EMR and assumes only one running job
    for line in html_bytes.splitlines():
        m = _RESOURCE_MANAGER_JS_RE.match(line)
        if m:
            return float(m.group('percent'))

    return None


### AWS Date-time parsing ###

# sometimes AWS gives us seconds as a decimal, which we can't parse
# with boto.utils.ISO8601
_SUBSECOND_RE = re.compile('\.[0-9]+')


# Thu, 29 Mar 2012 04:55:44 GMT
_RFC1123 = '%a, %d %b %Y %H:%M:%S %Z'


# TODO: test this, now that it uses UTC time
def iso8601_to_timestamp(iso8601_time):
    iso8601_time = _SUBSECOND_RE.sub('', iso8601_time)
    try:
        return calendar.timegm(time.strptime(iso8601_time, boto.utils.ISO8601))
    except ValueError:
        return calendar.timegm(time.strptime(iso8601_time, _RFC1123))


def iso8601_to_datetime(iso8601_time):
    iso8601_time = _SUBSECOND_RE.sub('', iso8601_time)
    try:
        return datetime.strptime(iso8601_time, boto.utils.ISO8601)
    except ValueError:
        return datetime.strptime(iso8601_time, _RFC1123)
