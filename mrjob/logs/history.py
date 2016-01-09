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
"""Code for parsing the history file, which contains counters and error
messages for each task."""
import re

from .wrap import _ls_logs


# what job history (e.g. counters) look like on either YARN or pre-YARN.
# YARN uses - instead of _ to separate fields. This should work for
# non-streaming jars as well.
_JOB_HISTORY_RE = re.compile(
    r'^(?P<prefix>.*?/)'
    r'(?P<job_id>job_\d+_\d{4})'
    r'[_-]\d+[_-]hadoop[_-]\S*$')


def _ls_history_logs(fs, log_dir_stream, job_id=None):
    """Yield paths/URIs of job history files, optionally filtering by *job_id*.

    *log_dir_stream* is a sequence of lists of log dirs. For each list, we'll
    look in all directories, and if we find any logs, we'll stop. (The
    assumption is that subsequent lists of log dirs would have copies
    of the same logs, just in a different location.
    """
    return _ls_logs(fs, log_dir_stream, _match_job_history_log,
                    job_id=job_id)


def _match_history_log(path, job_id=None):
    """Yield paths/uris of all job history files in the given directories,
    optionally filtering by *job_id*.
    """
    m = _JOB_HISTORY_RE.match(path)
    if not m:
        return None

    if not (job_id is None or m.group('job_id') == job_id):
        return None

    return dict(job_id=m.group('job_id'))
