# -*- coding: utf-8 -*-
# Copyright 2009-2019 Yelp and Contributors
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
from contextlib import contextmanager
from datetime import datetime
import fcntl
import json
import logging
import os


log = logging.getLogger(__name__)


class ClusterCache(object):
    """Read-through file cache of cluster info to reduce EMR API calls.

    We still hit the EMR API in
        - :py:meth:`~mrjob.yarnemr.YarnEMRJobRunner._wait_for_cluster`
          (waiting for a cluster to spin up)
        - :py:meth:`~mrjob.emr.EMRJobRunner._compare_cluster_setup
          (listing instance groups/fleets in cluster)`
        - :py:meth:`~mrjob.emr.EMRJobRunner._get_cluster_info
          (creates in-memory "cache" of cluster info)`

    The latter two can be cached as well, but let's keep these changes
    minimal at this time.
    """
    def __init__(self, emr_client, cache_filepath, cache_file_ttl):
        self._emr_client = emr_client
        self._cache_filepath = cache_filepath
        self._cache_file_ttl = cache_file_ttl

    @staticmethod
    def setup(cache_filepath):
        if not os.path.isfile(cache_filepath):
            open(cache_filepath, 'a').close()
            open(cache_filepath + '.age_marker', 'a').close()

    @contextmanager
    def cache_mutex(self, mode):
        try:
            self._fd = open(self._cache_filepath, mode=mode)
            fcntl.flock(self._fd, fcntl.LOCK_EX)
            yield self._fd
        finally:
            # We must always ensure we flush before we unlock and close, lest
            # we unlock and another process begins reading and writing before
            # this process has had a change to write its buffer.
            self._fd.flush()
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            self._fd.close()
            self._fd = None

    def _is_empty(self):
        return os.stat(self._cache_filepath).st_size == 0

    def _handle_cache_expiry(self):
        age_marker_file = self._cache_filepath + '.age_marker'
        mtime = os.stat(age_marker_file).st_mtime
        days_old = (datetime.now() - datetime.utcfromtimestamp(mtime)).days
        if days_old > self._cache_file_ttl:
            log.info('Cluster cache expired, truncating cache')
            open(age_marker_file, 'a').close()  # update mtime
            open(self._cache_filepath, 'w+').close()  # truncate cache

    def describe_cluster(self, cluster_id):
        # If there is no cluster cache file then fallback to a normal describe
        if self._cache_filepath is None:
            cluster = self._emr_client.describe_cluster(ClusterId=cluster_id)
            return cluster['Cluster']

        with self.cache_mutex('r+') as fd:
            self._handle_cache_expiry()

            # Get contents and check if the cluster id is present
            if self._is_empty():
                content = {}
            else:
                content = json.load(fd)
            cluster = content.get(cluster_id, None)
            if cluster:
                log.debug('Cluser cache hit: found cluster {}'
                          .format(cluster_id))
                return cluster

            log.debug('Cluser cache miss: not entry for cluster {}'
                      .format(cluster_id))

            # If there is no cluster with this id then get the info from EMR
            cluster = self._emr_client.describe_cluster(ClusterId=cluster_id)
            content[cluster_id] = cluster['Cluster']

            # We must seek back to the beginning of the file before writing.
            # There is no reason to truncate as the content will never shorten.
            fd.seek(0)
            json.dump(content, fd, default=str)

            return cluster['Cluster']
