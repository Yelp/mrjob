# Copyright 2009-2011 Yelp and Contributors
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
from __future__ import with_statement

import glob
import os
import posixpath
import re

# Constants used to tell :py:func:`list_logs` what logs to find and return
TASK_ATTEMPT_LOGS = 'TASK_ATTEMPT_LOGS'
STEP_LOGS = 'STEP_LOGS'
JOB_LOGS = 'JOB_LOGS'


# use to detect globs and break into the part before and after the glob
GLOB_RE = re.compile(r'^(.*?)([\[\*\?].*)$')


# regex for matching task-attempts log URIs
# general enough to match any base directory
TASK_ATTEMPTS_LOG_URI_RE = re.compile(r'^.*/attempt_(?P<timestamp>\d+)_(?P<step_num>\d+)_(?P<node_type>m|r)_(?P<node_num>\d+)_(?P<attempt_num>\d+)/(?P<stream>stderr|syslog)$')

# regex for matching step log URIs
# general enough to match any base directory
STEP_LOG_URI_RE = re.compile(r'^.*/(?P<step_num>\d+)/syslog$')

# regex for matching job log URIs
# general enough to match any base directory
JOB_LOG_URI_RE = re.compile(r'^.*?/.+?_(?P<mystery_string_1>\d+)_job_(?P<timestamp>\d+)_(?P<step_num>\d+)_hadoop_streamjob(?P<mystery_string_2>\d+).jar$')


class LogFetchException(Exception):
    pass


class LogFetcher(object):
    """LogFetcher subclasses provide information about how to get different
    kinds of logs from their respective log sources. There is also a
    convenience function, ``list_logs()``, to get the locations of commonly
    used log types.
    """

    def __init__(self, root_path='/'):
        super(LogFetcher, self).__init__()
        self.root_path = root_path

    def path_map(self):
        """Map log types (``TASK_ATTEMPT_LOGS``, ``STEP_LOGS``, and
        ``JOB_LOGS``) to their locations relative to ``root_path``
        """
        raise NotImplementedError

    def list_logs(self, ls_func=None, log_types=None):
        """Find and return lists of log paths corresponding to the kinds
        specified in ``log_types``.

        :param ls: function that returns a list of strings representing paths
        :type log_types: list
        :param log_types: list containing some combination of the constants ``TASK_ATTEMPT_LOGS``, ``STEP_LOGS``, and ``JOB_LOGS``. Defaults to ``[TASK_ATTEMPT_LOGS, STEP_LOGS, JOB_LOGS]``.

        :return: list of matching log files in the order specified by ``log_types``
        """
        log_types = log_types or [TASK_ATTEMPT_LOGS, STEP_LOGS, JOB_LOGS]

        path_map = self.path_map()
        re_map = {
            TASK_ATTEMPT_LOGS: TASK_ATTEMPTS_LOG_URI_RE,
            STEP_LOGS: STEP_LOG_URI_RE,
            JOB_LOGS: JOB_LOG_URI_RE,
        }

        if len(log_types) > 1:
            paths = ls_func(self.root_path)
        else:
            path = posixpath.join(self.root_path, path_map[log_types[0]])
            paths = ls_func(path)

        output = []
        for log_type in log_types:
            output.append([])

        for item in paths:
            for i, log_type in enumerate(log_types):
                if re_map[log_type].match(item):
                    output[i].append(item)
        if len(output) > 1:
            return output
        else:
            return output[0]


class S3LogFetcher(LogFetcher):

    def path_map(self):
        return {
            TASK_ATTEMPT_LOGS: 'task-attempts',
            STEP_LOGS: 'steps',
            JOB_LOGS: 'jobs',
        }


class SSHLogFetcher(LogFetcher):

    def __init__(self):
        super(SSHLogFetcher, self).__init__('/mnt/var/log/hadoop')

    def path_map(self):
        return {
            TASK_ATTEMPT_LOGS: 'userlogs',
            STEP_LOGS: 'steps',
            JOB_LOGS: 'history',
        }
