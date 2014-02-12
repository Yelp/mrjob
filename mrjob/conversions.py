# Copyright 2012 Yelp and Contributors
# Copyright 2013 Lyft
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
"""Utilities for handling ISO8601 date/time formats.
"""

# AWS actually gives dates in two formats, and we only recently started using
# API calls that return the second. So the date parsing function is called
# iso8601_to_*, but it also parses RFC1123.
# Until boto starts seamlessly parsing these, we check for them ourselves.

from datetime import datetime
import re
import time

try:
    import boto.utils
except ImportError:
    # don't require boto; MRJobs don't actually need it when running
    # inside hadoop streaming
    boto = None

# sometimes AWS gives us seconds as a decimal, which we can't parse
# with boto.utils.ISO8601
SUBSECOND_RE = re.compile('\.[0-9]+')


# Thu, 29 Mar 2012 04:55:44 GMT
RFC1123 = '%a, %d %b %Y %H:%M:%S %Z'


def iso8601_to_timestamp(iso8601_time):
    iso8601_time = SUBSECOND_RE.sub('', iso8601_time)
    try:
        return time.mktime(time.strptime(iso8601_time, boto.utils.ISO8601))
    except ValueError:
        return time.mktime(time.strptime(iso8601_time, RFC1123))


def iso8601_to_datetime(iso8601_time):
    iso8601_time = SUBSECOND_RE.sub('', iso8601_time)
    try:
        return datetime.strptime(iso8601_time, boto.utils.ISO8601)
    except ValueError:
        return datetime.strptime(iso8601_time, RFC1123)
