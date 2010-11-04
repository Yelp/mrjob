# Copyright 2009-2010 Yelp
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
"""Audit EMR usage over the past 2 weeks, sorted by job flow name and user.
"""
from __future__ import with_statement

import boto.utils
from collections import defaultdict
import datetime
import math
import logging
from optparse import OptionParser
import re
import time

from mrjob.emr import EMRJobRunner, describe_all_job_flows
from mrjob.util import log_to_stream

log = logging.getLogger('mrjob.tools.emr.audit_emr_usage')

JOB_FLOW_NAME_RE = re.compile(r'^(.*)\.(.*)\.(\d+)\.(\d+)\.(\d+)$')

def main():
    # parser command-line args
    option_parser = make_option_parser()
    options, args = option_parser.parse_args()

    if args:
        option_parser.error('takes no arguments')

    # set up logging
    if not options.quiet:
        log_to_stream(name='mrjob', debug=options.verbose)
    # suppress No handlers could be found for logger "boto" message
    log_to_stream(name='boto', level=logging.CRITICAL)

    print_report(options)

def make_option_parser():
    usage = '%prog [options]'
    description = 'Print a report on emr usage over the past 2 weeks.'
    option_parser = OptionParser(usage=usage, description=description)
    option_parser.add_option(
            '-v', '--verbose', dest='verbose', default=False,
            action='store_true',
            help='print more messages to stderr')
    option_parser.add_option(
            '-q', '--quiet', dest='quiet', default=False,
            action='store_true',
            help='just print job flow ID to stdout')
    option_parser.add_option(
            '-c', '--conf-path', dest='conf_path', default=None,
            help='Path to alternate mrjob.conf file to read from')
    option_parser.add_option(
            '--no-conf', dest='conf_path', action='store_false',
            help="Don't load mrjob.conf even if it's available")

    return option_parser

def print_report(options):

    emr_conn = EMRJobRunner(conf_path=options.conf_path).make_emr_conn()
    
    log.info(
        'getting info about all job flows (this goes back about 2 months)')
    now = datetime.datetime.utcnow()
    job_flows = describe_all_job_flows(emr_conn)

    job_flow_infos = []
    for jf in job_flows:
        job_flow_info = {}

        job_flow_info['id'] = jf.jobflowid

        job_flow_info['name'] = jf.name

        job_flow_info['created'] = to_datetime(jf.creationdatetime)

        # this looks to be an integer, but let's protect against
        # future changes
        job_flow_info['hours'] = float(jf.normalizedinstancehours)

        # estimate hours billed but not used
        job_flow_info['hours_bbnu'] = (
            job_flow_info['hours'] *
            estimate_proportion_billed_but_not_used(jf))

        # split out mr job name and user
        # jobs flows created by MRJob have names like:
        # mr_word_freq_count.dave.20101103.121249.638552
        match = JOB_FLOW_NAME_RE.match(jf.name)
        if match:
            job_flow_info['mr_job_name'] = match.group(1)
            job_flow_info['user'] = match.group(2)
        else:
            # not run by mrjob
            job_flow_info['mr_job_name'] = None
            job_flow_info['user'] = None

        job_flow_infos.append(job_flow_info)

    if not job_flow_infos:
        print 'No job flows created in the past two months!'
        return

    earliest = min(info['created'] for info in job_flow_infos)
    latest = max(info['created'] for info in job_flow_infos)

    print 'Total # of Job Flows: %d' % len(job_flow_infos)
    print

    print '* All times are in UTC.'
    print

    # Techincally, I should be using ISO8601 time format here, but it's
    # ugly and hard to read.
    ISOISH_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    print 'Min create time: %s' % (
        earliest.strftime(ISOISH_TIME_FORMAT))
    print 'Max create time: %s' % (
        latest.strftime(ISOISH_TIME_FORMAT))
    print '   Current time: %s' % (
        now.strftime(ISOISH_TIME_FORMAT))
    print

    print '* All usage is measured in Normalized Instance Hours, which are'
    print '  roughly equivalent to running an m1.small instance for an hour.'
    print

    # total compute-unit hours used
    total_hours = sum(info['hours'] for info in job_flow_infos)
    print 'Total Usage: %d' % total_hours
    print

    print '* Time billed but not used is estimated, and may not match'
    print "  Amazon's billing system exactly."
    print

    total_hours_bbnu = sum(info['hours_bbnu'] for info in job_flow_infos)
    print 'Total time billed but not used: %.2f' % total_hours_bbnu
    print
    
    def fmt(mr_job_name_or_user):
        if mr_job_name_or_user:
            return mr_job_name_or_user
        else:
            return '(not started by mrjob)'

    print '* Job flows are considered to belong to the user and job that'
    print '  started them (even if other jobs use the job flow).'
    print

    # Top jobs
    print 'Top jobs, by total usage:'
    mr_job_name_to_hours = defaultdict(float)
    for info in job_flow_infos:
        mr_job_name_to_hours[info['mr_job_name']] += info['hours']
    for mr_job_name, hours in sorted(mr_job_name_to_hours.iteritems(),
                                     key=lambda (n, h): (-h, n)):
        print '  %6d %s' % (hours, fmt(mr_job_name))
    print

    print 'Top jobs, by time billed but not used:'
    mr_job_name_to_hours_bbnu = defaultdict(float)
    for info in job_flow_infos:
        mr_job_name_to_hours_bbnu[info['mr_job_name']] += info['hours_bbnu']
    for mr_job_name, hours_bbnu in sorted(mr_job_name_to_hours_bbnu.iteritems(),
                                     key=lambda (n, h): (-h, n)):
        print '  %9.2f %s' % (hours_bbnu, fmt(mr_job_name))
    print
    
    # Top users
    print 'Top users, by total usage:'
    user_to_hours = defaultdict(float)
    for info in job_flow_infos:
        user_to_hours[info['user']] += info['hours']
    for user, hours in sorted(user_to_hours.iteritems(),
                              key=lambda (n, h): (-h, n)):
        print '  %6d %s' % (hours, fmt(user))
    print

    print 'Top users, by time billed but not used:'
    user_to_hours_bbnu = defaultdict(float)
    for info in job_flow_infos:
        user_to_hours_bbnu[info['user']] += info['hours_bbnu']
    for user, hours_bbnu in sorted(user_to_hours_bbnu.iteritems(),
                              key=lambda (n, h): (-h, n)):
        print '  %9.2f %s' % (hours_bbnu, fmt(user))
    print

    # Top job flows
    print 'All job flows:'
    top_job_flows = sorted(job_flow_infos,
                           key=lambda i: (-i['hours'], i['name']))
    for info in top_job_flows:
        print '  %6d %9.2f %-15s %s' % (info['hours'], info['hours_bbnu'],
                                        info['id'], info['name'])
    print

def estimate_proportion_billed_but_not_used(job_flow):
    """Estimate what proportion of time that a job flow was billed for
    was not used.

    We look at the job's start and end time, and consider the job to
    have been billed up to the next full hour (unless the job is currently
    running).

    We then calculate what proportion of that time was actually used
    by steps.

    This is a little bit of a fudge (the billing clock can actually start
    at different times for different instances), but it's good enough.
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

        hours = (jf_end - jf_start) / 60.0 / 60.0
        # if job is still running, don't bill for full hour
        # (we might actually make use of that time still)
        if hasattr(job_flow, 'enddatetime'):
            hours_billed = math.ceil(hours)
        else:
            hours_billed = hours

        if hours_billed <= 0:
            return 0.0

        # find out how much time has been used by steps
        secs_used = 0
        for step in job_flow.steps:
            if hasattr(step, 'startdatetime'):
                step_start = to_timestamp(step.startdatetime)
                if hasattr(step, 'enddatetime'):
                    step_end = to_timestamp(step.enddatetime)
                else:
                    step_end = jf_end

                secs_used += step_end - step_start

        hours_used = secs_used / 60.0 / 60.0

        return (hours_billed - hours_used) / hours_billed

def to_timestamp(iso8601_time):
    return time.mktime(time.strptime(iso8601_time, boto.utils.ISO8601))

def to_datetime(iso8601_time):
    return datetime.datetime.strptime(iso8601_time, boto.utils.ISO8601)

if __name__ == '__main__':
    main()

    
