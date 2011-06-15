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

import fnmatch
import logging
import os
import posixpath
import re

from mrjob.logfetch import GLOB_RE, LogFetcher, LogFetchException


S3_URI_RE = re.compile(r'^s3://([A-Za-z0-9-\.]+)/(.*)$')

ABSOLUTE_RE = re.compile(r'^(/|\w+://).*')


# regex for matching task-attempts log URIs
TASK_ATTEMPTS_LOG_URI_RE = re.compile(r'^.*/task-attempts/attempt_(?P<timestamp>\d+)_(?P<step_num>\d+)_(?P<node_type>m|r)_(?P<node_num>\d+)_(?P<attempt_num>\d+)/(?P<stream>stderr|syslog)$')

# regex for matching step log URIs
STEP_LOG_URI_RE = re.compile(r'^.*/steps/(?P<step_num>\d+)/syslog$')

# regex for matching job log URIs
JOB_LOG_URI_RE = re.compile(r'^.*?/jobs/.+?_(?P<mystery_string_1>\d+)_job_(?P<timestamp>\d+)_(?P<step_num>\d+)_hadoop_streamjob(?P<mystery_string_2>\d+).jar$')


log = logging.getLogger('mrjob.fetch_s3')


def parse_s3_uri(uri):
    """Parse an S3 URI into (bucket, key)

    >>> parse_s3_uri('s3://walrus/tmp/')
    ('walrus', 'tmp/')

    If ``uri`` is not an S3 URI, raise a ValueError
    """
    match = S3_URI_RE.match(uri)
    if match:
        return match.groups()
    else:
        raise ValueError('Invalid S3 URI: %s' % uri)


def s3_key_to_uri(s3_key):
    """Convert a boto Key object into an ``s3://`` URI"""
    return 's3://%s/%s' % (s3_key.bucket.name, s3_key.name)


class S3LogFetcher(LogFetcher):

    def __init__(self, s3_conn, root_path, local_temp_dir='/tmp'):
        super(S3LogFetcher, self).__init__(local_temp_dir=local_temp_dir)
        self.s3_conn = s3_conn
        self.root_path = root_path
        self._uri_of_downloaded_log_file = None

    ### RETRIEVING/RECOGNIZING KINDS OF LOG FILES ###

    def task_attempts_log_uri_re(self):
        return TASK_ATTEMPTS_LOG_URI_RE

    def step_log_uri_re(self):
        return STEP_LOG_URI_RE

    def job_log_uri_re(self):
        return JOB_LOG_URI_RE

    def task_attempts_log_path(self):
        return 'task-attempts'

    def step_log_path(self):
        return 'steps'

    def job_log_path(self):
        return 'jobs'

    ### BASIC ACTIONS RELATED TO DOWNLOADING LOGS ###

    def ls(self, path='*'):
        """Recursively list files locally or on S3.

        This doesn't list "directories" unless there's actually a
        corresponding key ending with a '/' (which is weird and confusing;
        don't make S3 keys ending in '/')

        To list a directory, path_glob must end with a trailing
        slash (foo and foo/ are different on S3)
        """
        if self.root_path and not ABSOLUTE_RE.match(path):
            # Convert the relative path to an absolute one
            path = posixpath.join(self.root_path, path)

        # Fall back on local behavior if it's a local path
        if not S3_URI_RE.match(path):
            for path in super(S3LogFetcher, self).ls(path):
                yield path

        # support globs
        glob_match = GLOB_RE.match(path)

        # if it's a "file" (doesn't end with /), just check if it exists
        if not glob_match and not path.endswith('/'):
            uri = path
            if self.get_s3_key(uri):
                yield uri
            return

        # we're going to search for all keys starting with base_uri
        if glob_match:
            # cut it off at first wildcard
            base_uri = glob_match.group(1)
        else:
            base_uri = path

        for uri in self._s3_ls(base_uri):
            # enforce globbing
            if glob_match and not fnmatch.fnmatch(uri, path):
                continue

            yield uri

    def _s3_ls(self, uri):
        """Helper for ls(); doesn't bother with globbing or directories"""
        bucket_name, key_name = parse_s3_uri(uri)

        bucket = self.s3_conn.get_bucket(bucket_name)
        for key in bucket.list(key_name):
            yield s3_key_to_uri(key)

    def get(self, path, dest=None):
        save_dest = (dest is None)
        dest = dest or os.path.join(self.local_temp_dir, 'log')

        if not save_dest or self._uri_of_downloaded_log_file != path:
            s3_log_file = self.get_s3_key(path)
            if not s3_log_file:
                raise LogFetchException('Could not fetch file %s from S3' % path)

            log.debug('downloading %s -> %s' % (path, dest))
            s3_log_file.get_contents_to_filename(dest)
            if save_dest:
                self._uri_of_downloaded_log_file = s3_log_file

        return dest

    def get_s3_key(self, uri):
        """Get the boto Key object matching the given S3 uri, or
        return None if that key doesn't exist.

        uri is an S3 URI: ``s3://foo/bar``

        You may optionally pass in an existing s3 connection through ``s3_conn``
        """
        bucket_name, key_name = parse_s3_uri(uri)

        return self.s3_conn.get_bucket(bucket_name).get_key(key_name)

