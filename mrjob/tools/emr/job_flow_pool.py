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

import bisect
import boto.utils
import functools
import math
import time

from mrjob import botoemr
from mrjob.emr import EMRJobRunner
from mrjob.tools.emr.audit_usage import to_timestamp
from mrjob.tools.emr.terminate_idle_job_flows import is_job_flow_done, is_job_flow_running


ANY_INSTANCE_TYPE = '*.*'


def job_flows_matching_instance_types(
    master_instance_type=ANY_INSTANCE_TYPE,
    slave_instance_type=ANY_INSTANCE_TYPE):

    emr_conn = EMRJobRunner().make_emr_conn()
    all_job_flows = emr_conn.describe_jobflows()

    def matches(job_flow):
        if is_job_flow_done(job_flow):
            return False
        if is_job_flow_running(job_flow):
            return False

        if job_flow.masterinstancetype != master_instance_type \
           and master_instance_type != ANY_INSTANCE_TYPE:
            return False

        jf_slave_instance_type = getattr(job_flow, 'slaveinstancetype', None)
        if jf_slave_instance_type != slave_instance_type \
           and slave_instance_type != ANY_INSTANCE_TYPE:
            return False

        return True

    available_job_flows = [jf for jf in all_job_flows if matches(jf)]
    job_flows_with_times = [(est_time_to_hour(jf), jf) for jf in available_job_flows]
    return sorted(job_flows_with_times, key=lambda (t, jf): t)


def est_time_to_hour(job_flow):
    if not hasattr(job_flow, 'startdatetime'):
        print 'no start'
        return 0.0
    else:
        now = time.time()

        # find out how long the job flow has been running
        jf_start = to_timestamp(job_flow.startdatetime)
        if hasattr(job_flow, 'enddatetime'):
            jf_end = to_timestamp(job_flow.enddatetime)
        else:
            jf_end = now

        minutes = (jf_end - jf_start) / 60.0
        hours = minutes / 60.0
        return math.ceil(hours)*60 - minutes


def to_timestamp(iso8601_time):
    if iso8601_time is None: return None
    return time.mktime(time.strptime(iso8601_time, boto.utils.ISO8601))


def find_optimal(min_time, job_flows_with_times):
    # from http://docs.python.org/library/bisect.html
    # derived from find_gt() to find leftmost value greater than x
    # but returns job_flows_With_times[0] if none found
    i = bisect.bisect_right(job_flows_with_times, (min_time, None))
    if i != len(job_flows_with_times):
        return job_flows_with_times[i]
    return job_flows_with_times[0]


def pprint_job_flow(jf):
    instance_count = int(jf.instancecount)

    nosep_segments = [
        '%s: %d instance' % (jf.jobflowid, instance_count),
    ]
    if instance_count > 1:
        nosep_segments.append('s')

    comma_segments = [
        'master=%s' % jf.masterinstancetype,
    ]

    if instance_count > 1:
        comma_segments.append('slaves=%s' % jf.slaveinstancetype)

    comma_segments.append('%0.0f minutes to the hour' % est_time_to_hour(jf))

    nosep_segments += [
        ' (',
        ', '.join(comma_segments),
        ')',
    ]

    print jf.name
    print ''.join(nosep_segments)
    print


if __name__ == '__main__':
    sorted_tagged_job_flows = job_flows_matching_instance_types()
    for time_to_hour, jf in sorted_tagged_job_flows:
        pprint_job_flow(jf)

    estimate = 15
    time_to_hour, jf = find_optimal(estimate, sorted_tagged_job_flows)
    print 'You should use this one (%d minutes of padding):' % (time_to_hour-estimate)
    pprint_job_flow(jf)
