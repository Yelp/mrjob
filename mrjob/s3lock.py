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
"""Facilities for "locking" EMR job flows via S3. This can be used to avoid
race conditions where two jobs see that a job flow is available and add
themselves to it, resulting in one job flow being scheduled behidn the other
and probably making the developer very unhappy."""
from __future__ import with_statement

import os
import time

from mrjob.parse import parse_s3_uri


def make_lock_uri(s3_tmp_uri, emr_job_flow_id, step_num):
    """Generate the URI to lock the job flow ``emr_job_flow_id``"""
    return s3_tmp_uri + 'locks/' + emr_job_flow_id + '/' + str(step_num)


def _acquire_step_1(s3_conn, lock_uri, job_name):
    bucket_name, key_prefix = parse_s3_uri(lock_uri)
    bucket = s3_conn.get_bucket(bucket_name)
    key = bucket.get_key(key_prefix)
    if key is None:
        key = bucket.new_key(key_prefix)
        key.set_contents_from_string(job_name)
        return key
    else:
        return None


def _acquire_step_2(key, job_name):
    key_value = key.get_contents_as_string()
    return (key_value == job_name)


def attempt_to_acquire(s3_conn, lock_uri, sync_wait_time, job_name):
    """Returns True if this session successfully took ownership of the lock
    specified by ``lock_uri``.
    """
    key = _acquire_step_1(s3_conn, lock_uri, job_name)
    if key is not None:
        time.sleep(sync_wait_time)
        success = _acquire_step_2(key, job_name)
        if success:
            return True

    return False
