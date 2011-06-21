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
from optparse import OptionParser, OptionGroup
import time

from mrjob import botoemr
from mrjob.emr import EMRJobRunner
from mrjob.job import MRJob
from mrjob.tools.emr.audit_usage import to_timestamp
from mrjob.util import scrape_options_into_new_groups


def job_flows_matching_runner(runner):
    """Get job flows with specific properties

    :return: list of (job_minutes_float, :py:class:`botoemr.emrobject.JobFlow`)
    """

    emr_conn = runner.make_emr_conn()
    all_job_flows = emr_conn.describe_jobflows()
    jf_args = runner.job_flow_args(persistent=True)

    def matches(job_flow):
        if job_flow.state != 'WAITING':
            return False

        # boto gives us a ustring for this. why???
        job_flow.instancecount = int(job_flow.instancecount)
        keep_alive = job_flow.keepjobflowalivewhennosteps  
        job_flow.keepjobflowalivewhennosteps = bool(keep_alive == 'true')

        args_to_check = [
            ('ec2_master_instance_type', job_flow.masterinstancetype),
            ('num_ec2_instances', job_flow.instancecount),
            ('hadoop_version', job_flow.hadoopversion),
            ('keep_alive', job_flow.keepjobflowalivewhennosteps),
        ]

        if job_flow.instancecount > 1:
            args_to_check.append(
                ('ec2_slave_instance_type', job_flow.slaveinstancetype))

        for arg_name, required_value in args_to_check:
            if jf_args.has_key(arg_name) \
               and jf_args[arg_name] != required_value:
                print job_flow.name, 'fails', arg_name, '(%r != %r)' % (required_value, jf_args[arg_name])
                return False

        return True

    available_job_flows = [jf for jf in all_job_flows if matches(jf)]
    job_flows_with_times = [(est_time_to_hour(jf), jf) for jf in available_job_flows]
    return sorted(job_flows_with_times, key=lambda (t, jf): t)


def est_time_to_hour(job_flow):
    """If available, get the difference between hours billed and hours used.
    This metric is used to determine which job flow to use if more than one
    is available.
    """

    if not hasattr(job_flow, 'startdatetime'):
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
    """Given a list of job flows tagged with the amount of time they would
    waste if killed now, find the one that would waste the least time if
    it ran a job lasting ``min_time`` minutes.

    :param min_time: Upper bound on the number of minutes the prospective job will take
    :type min_time: int
    :param job_flows_with_times: Jobs matching criteria for running the prospective job
    :type job_flows_with_times: list of (job_minutes_float, :py:class:`botoemr.emrobject.JobFlow`)
    :return: (job_minutes_float, :py:class:`botoemr.emrobject.JobFlow`)
    """
    # from http://docs.python.org/library/bisect.html
    # derived from find_gt() to find leftmost value greater than x
    # but returns job_flows_With_times[0] if none found
    i = bisect.bisect_right(job_flows_with_times, (min_time, None))
    if i != len(job_flows_with_times):
        return job_flows_with_times[i]
    return job_flows_with_times[0]


def pprint_job_flow(jf):
    """Print a job flow to stdout in this form:
    job.flow.name
    j-JOB_FLOW_ID: 2 instances (master=m1.small, slaves=m1.small, 20 minutes to the hour)
    """
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


def find_or_create_job_flow(runner, estimate=15):
    """Find or create a job flow matching the given runner"""

    sorted_tagged_job_flows = job_flows_matching_runner(runner)
    if sorted_tagged_job_flows:
        time_to_hour, jf = find_optimal(estimate, sorted_tagged_job_flows)
        return jf
    else:
        return None


if __name__ == '__main__':
    usage = '%prog [options]'
    description = 'Identify an exising job flow to run jobs in or create a new one if none are available. WARNING: do not run this without mrjob.tools.emr.terminate.idle_job_flows in your crontab; job flows left idle can quickly become expensive!'
    option_parser = OptionParser(usage=usage, description=description)

    def make_option_group(halp):
        g = OptionGroup(option_parser, halp)
        option_parser.add_option_group(g)
        return g

    ec2_opt_group = make_option_group('EC2 instance configuration')
    hadoop_opt_group = make_option_group('Hadoop configuration')
    job_opt_group = make_option_group('Job flow configuration')

    assignments = {
        option_parser: ('conf_path', 'quiet', 'verbose'),
        ec2_opt_group: ('ec2_instance_type', 'ec2_master_instance_type',
                        'ec2_slave_instance_type', 'num_ec2_instances',
                        'aws_availability_zone', 
                        'ec2_key_pair', 'ec2_key_pair_file',
                        'emr_endpoint',),
        hadoop_opt_group: ('hadoop_version', 'label', 'owner'),
        job_opt_group: ('bootstrap_mrjob', 'bootstrap_cmds',
                        'bootstrap_files', 'bootstrap_python_packages',),
    }

    # Scrape options from MRJob and index them by dest
    mr_job = MRJob()
    scrape_options_into_new_groups(mr_job.all_option_groups(), assignments)
    options, args = option_parser.parse_args()

    runner = EMRJobRunner(**options.__dict__)
    sorted_tagged_job_flows = job_flows_matching_runner(runner)
    for time_to_hour, jf in sorted_tagged_job_flows:
        pprint_job_flow(jf)

    if sorted_tagged_job_flows:
        estimate = 15
        time_to_hour, jf = find_optimal(estimate, sorted_tagged_job_flows)
        print 'You should use this one (%d minutes of padding):' % (time_to_hour-estimate)
        pprint_job_flow(jf)
    else:
        print 'No idle job flows match criteria'
