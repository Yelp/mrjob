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

Usage::

    python -m mrjob.tools.emr.audit_usage > report

Options::

  -h, --help            show this help message and exit
  -v, --verbose         print more messages to stderr
  -q, --quiet           Don't log status messages; just print the report.
  -c CONF_PATH, --conf-path=CONF_PATH
                        Path to alternate mrjob.conf file to read from
  --no-conf             Don't load mrjob.conf even if it's available
  --max-days-ago=MAX_DAYS_AGO
                        Max number of days ago to look at jobs. By default, we
                        go back as far as EMR supports (currently about 2
                        months)
"""
from __future__ import with_statement

import boto.utils
from collections import defaultdict
import datetime
import math
import logging
from optparse import OptionParser
import sys
import time

from mrjob.emr import EMRJobRunner
from mrjob.emr import describe_all_job_flows
from mrjob.parse import JOB_NAME_RE
from mrjob.util import log_to_stream

log = logging.getLogger('mrjob.tools.emr.audit_usage')


def main(args):
    # parser command-line args
    option_parser = make_option_parser()
    options, args = option_parser.parse_args(args)

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
    description = 'Print a giant report on EMR usage.'
    option_parser = OptionParser(usage=usage, description=description)
    option_parser.add_option(
        '-v', '--verbose', dest='verbose', default=False, action='store_true',
        help='print more messages to stderr')
    option_parser.add_option(
        '-q', '--quiet', dest='quiet', default=False, action='store_true',
        help="Don't log status messages; just print the report.")
    option_parser.add_option(
        '-c', '--conf-path', dest='conf_path', default=None,
        help='Path to alternate mrjob.conf file to read from')
    option_parser.add_option(
        '--no-conf', dest='conf_path', action='store_false',
        help="Don't load mrjob.conf even if it's available")
    option_parser.add_option(
        '--max-days-ago', dest='max_days_ago', type='float', default=None,
        help=('Max number of days ago to look at jobs. By default, we go back'
              ' as far as EMR supports (currently about 2 months)'))
    return option_parser


def print_report(options):

    emr_conn = EMRJobRunner(conf_path=options.conf_path).make_emr_conn()

    log.info('getting job flow history...')
    # microseconds just make our report messy
    now = datetime.datetime.utcnow().replace(microsecond=0)

    # if --max-days-ago is set, only look at recent jobs
    created_after = None
    if options.max_days_ago is not None:
        created_after = now - datetime.timedelta(days=options.max_days_ago)

    job_flows = describe_all_job_flows(emr_conn, created_after=created_after)

    job_flow_infos = []
    for jf in job_flows:
        job_flow_info = {}

        job_flow_info['id'] = jf.jobflowid

        job_flow_info['name'] = jf.name

        job_flow_info['created'] = to_datetime(jf.creationdatetime)

        start_time = to_datetime(getattr(jf, 'startdatetime', None))
        if start_time:
            end_time = to_datetime(getattr(jf, 'enddatetime', None)) or now
            job_flow_info['ran'] = end_time - start_time
        else:
            job_flow_info['ran'] = datetime.timedelta(0)

        job_flow_info['state'] = jf.state

        job_flow_info['num_steps'] = len(jf.steps or [])

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
        match = JOB_NAME_RE.match(jf.name)
        if match:
            job_flow_info['job_name'] = match.group(1)
            job_flow_info['user'] = match.group(2)
        else:
            # not run by mrjob
            job_flow_info['job_name'] = None
            job_flow_info['user'] = None

        job_flow_infos.append(job_flow_info)

    if not job_flow_infos:
        print 'No job flows created in the past two months!'
        return

    earliest = min(info['created'] for info in job_flow_infos)
    latest = max(info['created'] for info in job_flow_infos)

    print 'Total  # of Job Flows: %d' % len(job_flow_infos)
    print

    print '* All times are in UTC.'
    print

    print 'Min create time: %s' % earliest
    print 'Max create time: %s' % latest
    print '   Current time: %s' % now
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
    print 'Total time billed but not used (waste): %.2f' % total_hours_bbnu
    print

    date_to_hours = defaultdict(float)
    date_to_hours_bbnu = defaultdict(float)
    for info in job_flow_infos:
        date_created = info['created'].date()
        date_to_hours[date_created] += info['hours']
        date_to_hours_bbnu[date_created] += info['hours_bbnu']
    print 'Daily statistics:'
    print
    print ' date        usage     waste'
    d = latest.date()
    while d >= earliest.date():
        print ' %10s %6d %9.2f' % (d, date_to_hours[d], date_to_hours_bbnu[d])
        d -= datetime.timedelta(days=1)
    print

    def fmt(job_name_or_user):
        if job_name_or_user:
            return job_name_or_user
        else:
            return '(not started by mrjob)'

    print '* Job flows are considered to belong to the user and job that'
    print '  started them (even if other jobs use the job flow).'
    print

    # Top jobs
    print 'Top jobs, by total usage:'
    job_name_to_hours = defaultdict(float)
    for info in job_flow_infos:
        job_name_to_hours[info['job_name']] += info['hours']
    for job_name, hours in sorted(job_name_to_hours.iteritems(),
                                     key=lambda (n, h): (-h, n)):
        print '  %6d %s' % (hours, fmt(job_name))
    print

    print 'Top jobs, by time billed but not used:'
    job_name_to_hours_bbnu = defaultdict(float)
    for info in job_flow_infos:
        job_name_to_hours_bbnu[info['job_name']] += info['hours_bbnu']
    for job_name, hours_bbnu in sorted(job_name_to_hours_bbnu.iteritems(),
                                     key=lambda (n, h): (-h, n)):
        print '  %9.2f %s' % (hours_bbnu, fmt(job_name))
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
    print 'All job flows, by total usage:'
    top_job_flows = sorted(job_flow_infos,
                           key=lambda i: (-i['hours'], i['name']))
    for info in top_job_flows:
        print '  %6d %-15s %s' % (info['hours'], info['id'], info['name'])
    print

    print 'All job flows, by time billed but not used:'
    top_job_flows_bbnu = sorted(job_flow_infos,
                           key=lambda i: (-i['hours_bbnu'], i['name']))
    for info in top_job_flows_bbnu:
        print '  %9.2f %-15s %s' % (
            info['hours_bbnu'], info['id'], info['name'])
    print

    print 'Details for all job flows:'
    print
    print (' id              state         created             steps'
           '        time ran  usage     waste   user   name')

    all_job_flows = sorted(job_flow_infos, key=lambda i: i['created'],
                           reverse=True)
    for info in all_job_flows:
        print ' %-15s %-13s %19s %3d %17s %6d %9.2f %8s %s' % (
            info['id'], info['state'], info['created'], info['num_steps'],
            info['ran'], info['hours'], info['hours_bbnu'],
            (info['user'] or ''), fmt(info['job_name']))


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
    if iso8601_time is None:
        return None

    return time.mktime(time.strptime(iso8601_time, boto.utils.ISO8601))


def to_datetime(iso8601_time):
    if iso8601_time is None:
        return None

    return datetime.datetime.strptime(iso8601_time, boto.utils.ISO8601)


if __name__ == '__main__':
    main(sys.argv[1:])
