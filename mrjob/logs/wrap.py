# -*- coding: utf-8 -*-
# Copyright 2015-2016 Yelp
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
"""Utilities for ls()ing and cat()ing logs without raising exceptions."""
from logging import getLogger

from mrjob.py2 import to_string

from .ids import _sort_by_recency

log = getLogger(__name__)


def _cat_log(fs, path):
    """fs.cat() the given log, converting lines to strings, and logging
    errors."""
    try:
        if not fs.exists(path):
            return
        for line in fs.cat(path):
            yield to_string(line)
    except (IOError, OSError) as e:
        log.warning("couldn't cat() %s: %r" % (path, e))


def _ls_logs(fs, log_dir_stream, matcher, **kwargs):
    """Return a list matches against log files. Used to implement
    ``_ls_*_logs()`` functions.

    This yields dictionaries with ``path`` set to matching log path, and
    other information (e.g. corresponding job_id) returned by *matcher*

    *fs* is a :py:class:`mrjob.fs.Filesystem`

    *log_dir_stream* is a sequence of lists of log dirs. The idea is that
    there may be copies of the same logs in multiple places (e.g.
    on S3 and by SSHing into nodes) and we want to list them all without
    finding duplicate copies. This function will go through the lists of
    log dirs in turn, stopping if it finds any matches from a list.

    *matcher* is a function that takes (log_path, **kwargs)
    and returns either None (no match) or a dictionary with information
    about the path (e.g. the corresponding job_id). It's okay to return
    an empty dict.
    """
    # wrapper for fs.ls() that turns IOErrors into warnings
    def _fs_ls(path):
        try:
            if fs.exists(log_dir):
                for path in fs.ls(log_dir):
                    yield path
        except (IOError, OSError) as e:
            log.warning("couldn't ls() %s: %r" % (log_dir, e))

    for log_dirs in log_dir_stream:
        if isinstance(log_dirs, str):
            raise TypeError

        matches = []

        for log_dir in log_dirs:
            for path in _fs_ls(log_dir):
                m = matcher(path, **kwargs)
                if m is not None:
                    m['path'] = path
                    matches.append(m)

        if matches:
            return _sort_by_recency(matches)

    return []


def _logs_exist(fs, path):
    """Do ``fs.exists(path)``, and return ``None`` if it raises ``IOError``"""
    try:
        return fs.exists(path)
    except IOError:
        return None
