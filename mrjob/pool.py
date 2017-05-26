# Copyright 2012 Yelp and Contributors
# Copyright 2013 Lyft
# Copyright 2014 Brett Gibson
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
"""Utilities related to cluster pooling. This code used to be in mrjob.emr.
"""
from datetime import timedelta
from logging import getLogger

from mrjob.aws import _boto3_now

log = getLogger(__name__)


### current versions of these functions, using "cluster" API calls ###

# these are "hidden" because there's no need to access them directly

def _est_time_to_hour(cluster_summary, now=None):
    """How long before job reaches the end of the next full hour since it
    began. This is important for billing purposes.

    If it happens to be exactly a whole number of hours, we return
    one hour, not zero.
    """
    if now is None:
        now = _boto3_now()

    timeline = cluster_summary.get('Status', {}).get('Timeline', {})

    creationdatetime = timeline.get('CreationDateTime')

    if not creationdatetime:
        # do something reasonable if creationdatetime isn't set
        return timedelta(minutes=60)

    run_time = now - creationdatetime
    return timedelta(seconds=((-run_time).seconds % 3600.0 or 3600.0))


def _pool_tags(hash, name):
    """Return a dict with "hidden" tags to add to the given cluster."""
    return dict(__mrjob_pool_hash=hash, __mrjob_pool_name=name)


def _extract_tags(cluster):
    """Pull the tags from a cluster, as a dict."""
    return {t['Key']: t['Value'] for t in cluster.get('Tags') or []}


def _pool_hash_and_name(cluster):
    """Return the hash and pool name for the given cluster, or
    ``(None, None)`` if it isn't pooled."""
    tags = _extract_tags(cluster)
    return tags.get('__mrjob_pool_hash'), tags.get('__mrjob_pool_name')


def _legacy_pool_hash_and_name(bootstrap_actions):
    """Get pool hash and name from a pre-v0.6.0 job."""
    for ba in bootstrap_actions:
        if ba['Name'] == 'master':
            args = ba['Args']
            if len(args) == 2 and args[0].startswith('pool-'):
                return args[0][5:], args[1]

    return None, None
