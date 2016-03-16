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
from datetime import datetime
from datetime import timedelta
from logging import getLogger

from mrjob.parse import iso8601_to_datetime

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
        now = datetime.utcnow()

    timeline = getattr(
        getattr(cluster_summary, 'status', None), 'timeline', None)

    creationdatetime = getattr(timeline, 'creationdatetime', None)

    if creationdatetime:
        start = iso8601_to_datetime(creationdatetime)
    else:
        # do something reasonable if creationdatetime isn't set
        return timedelta(minutes=60)

    run_time = now - start
    return timedelta(seconds=((-run_time).seconds % 3600.0 or 3600.0))


def _pool_hash_and_name(bootstrap_actions):
    """Return the hash and pool name for the given cluster, or
    ``(None, None)`` if it isn't pooled."""
    for bootstrap_action in bootstrap_actions:
        if bootstrap_action.name == 'master':
            args = [arg.value for arg in bootstrap_action.args]
            if len(args) == 2 and args[0].startswith('pool-'):
                return args[0][5:], args[1]

    return (None, None)
