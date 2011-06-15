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
import re

# Constants used to tell :py:func:`list_logs` what logs to find and return
TASK_ATTEMPT_LOGS = 'TASK_ATTEMPT_LOGS'
STEP_LOGS = 'STEP_LOGS'
JOB_LOGS = 'JOB_LOGS'


# use to detect globs and break into the part before and after the glob
GLOB_RE = re.compile(r'^(.*?)([\[\*\?].*)$')


class LogFetchException(Exception):
    pass


class LogFetcher(object):
    """Abstract base class for log fetchers.

    Log fetchers are responsible for listing and downloading logs from a
    specific storage medium such as S3, SSH, or the local file system.

    Subclasses: :py:class:`~mrjob.logfetch.s3.S3LogFetcher`,
    :py:class:`~mrjob.logfetch.ssh.SSHLogFetcher`
    """

    def __init__(self, local_temp_dir='/tmp'):
        super(LogFetcher, self).__init__()
        self.local_temp_dir = local_temp_dir
        self.root_path = '/'

    ### RETRIEVING/RECOGNIZING KINDS OF LOG FILES ###

    def task_attempts_log_uri_re(self):
        """Returns a regular expression object that matches task attempt log
        URIs
        """
        raise NotImplementedError

    def task_attempts_log_path(self):
        """Returns the relative path of task attempt log files"""
        raise NotImplementedError

    def step_log_uri_re(self):
        """Returns a regular expression object that matches step log URIs"""
        raise NotImplementedError

    def step_log_path(self):
        """Returns the relative path of step log files"""
        raise NotImplementedError

    def job_log_uri_re(self):
        """Returns a regular expression object that matches job log URIs"""
        raise NotImplementedError

    def job_log_path(self):
        """Returns the relative path of job log files"""
        raise NotImplementedError

    ### BASIC ACTIONS RELATED TO DOWNLOADING LOGS ###

    def list_logs(self, log_types=None):
        """Find and return lists of log paths corresponding to the kinds
        specified in ``log_types``.

        :type log_types: list
        :param log_types: list containing some combination of the constants ``TASK_ATTEMPT_LOGS``, ``STEP_LOGS``, and ``JOB_LOGS``. Defaults to ``[TASK_ATTEMPT_LOGS, STEP_LOGS, JOB_LOGS]``.

        :return: list of matching log files in the order specified by ``log_types``
        """
        log_types = log_types or [TASK_ATTEMPT_LOGS, STEP_LOGS, JOB_LOGS]
        re_map = {
            TASK_ATTEMPT_LOGS: self.task_attempts_log_uri_re(),
            STEP_LOGS: self.step_log_uri_re(),
            JOB_LOGS: self.job_log_uri_re(),
        }
        path_map = {
            TASK_ATTEMPT_LOGS: self.task_attempts_log_path(),
            STEP_LOGS: self.step_log_path(),
            JOB_LOGS: self.job_log_path(),
        }
        if len(log_types) > 1:
            paths = self.ls()
        else:
            paths = self.ls(path_map[log_types[0]])

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

    def ls(self, path):
        """Recursively list all files in the given path.

        We try not to return directories for compatibility with S3 (which
        has no concept of them)

        Corresponds roughly to: ``hadoop fs -lsr path_glob``
        """
        for path in glob.glob(path_glob):
            if os.path.isdir(path):
                for dirname, _, filenames in os.walk(path):
                    for filename in filenames:
                        yield os.path.join(dirname, filename)
            else:
                yield path

    def get(self, path, dest=None):
        """Download the file at ``path`` and return its local path. Specify
        ``dest`` to download the file to that path.
        """
        return path
