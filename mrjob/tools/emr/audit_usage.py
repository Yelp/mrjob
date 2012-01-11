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
from datetime import datetime
from datetime import timedelta
import math
import logging
from optparse import OptionParser
import sys

from mrjob.emr import EMRJobRunner
from mrjob.emr import describe_all_job_flows
from mrjob.job import MRJob
from mrjob.parse import JOB_NAME_RE
from mrjob.parse import STEP_NAME_RE

log = logging.getLogger('mrjob.tools.emr.audit_usage')


def main(args):
    # parser command-line args
    option_parser = make_option_parser()
    options, args = option_parser.parse_args(args)

    if args:
        option_parser.error('takes no arguments')

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)

    now = get_now()

    log.info('getting job flow history...')
    job_flows = get_job_flows(options.conf_path, options.max_days_ago, now=now)

    log.info('compiling job flow stats...')
    stats = job_flows_to_stats(job_flows, now=now)

    print_report(stats, now=now)


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


def job_flows_to_stats(job_flows, now=None):
    s = {}  # stats for all job flows

    s['flows'] = [job_flow_to_full_summary(job_flow, now=now)
                  for job_flow in job_flows]

    # total usage
    for nih_type in ('nih_billed', 'nih_used', 'nih_bbnu'):
        s[nih_type] = float(sum(jf[nih_type] for jf in s['flows']))

    # stats by date
    for nih_type in ('nih_billed', 'nih_used', 'nih_bbnu'):
        date_to_nih = {}
        for jf in s['flows']:
            for u in jf['usage']:
                for d, nih in u['date_to_%s' % nih_type].iteritems():
                    date_to_nih.setdefault(d, 0.0)
                    date_to_nih[d] += nih
        s['date_to_%s' % nih_type] = date_to_nih

    # break down by usage/waste
    s['bootstrap_nih_used'] = float(sum(
        jf['usage'][0]['nih_used'] for jf in s['flows'] if jf['usage']))
    s['job_nih_used'] = float(sum(
        sum(u['nih_used'] for u in jf['usage'][1:])
        for jf in s['flows']))
    s['end_nih_bbnu'] = float(sum(
        jf['usage'][-1]['nih_bbnu'] for jf in s['flows'] if jf['usage']))
    s['other_nih_bbnu'] = float(sum(
        sum(u['nih_bbnu'] for u in jf['usage'][:-1])
        for jf in s['flows']))

    # break down by label ("job name") and owner ("user")
    for key in ('label', 'owner'):
        for nih_type in ('nih_used', 'nih_billed', 'nih_bbnu'):
            key_to_nih = {}
            for jf in s['flows']:
                for u in jf['usage']:
                    key_to_nih.setdefault(u[key], 0.0)
                    key_to_nih[u[key]] += u[nih_type]
            s['%s_to_%s' % (key, nih_type)] = key_to_nih

    # break down by job step. separate out un-pooled jobs
    for nih_type in ('nih_used', 'nih_billed', 'nih_bbnu'):
        job_step_to_nih = {}
        job_step_to_nih_no_pool = {}
        for jf in s['flows']:
            for u in jf['usage'][1:]:
                job_step = (u['label'], u['step_num'])
                job_step_to_nih.setdefault(job_step, 0.0)
                job_step_to_nih[job_step] += u[nih_type]
                if not jf['pool']:
                    job_step_to_nih_no_pool.setdefault(job_step, 0.0)
                    job_step_to_nih_no_pool[job_step] += u[nih_type]

            s['job_step_to_%s' % nih_type] = job_step_to_nih
            s['job_step_to_%s_no_pool' % nih_type] = job_step_to_nih_no_pool

    # break down by pool
    for nih_type in ('nih_used', 'nih_billed', 'nih_bbnu'):
        pool_to_nih = {}
        for jf in s['flows']:
            pool_to_nih.setdefault(jf['pool'], 0.0)
            pool_to_nih[jf['pool']] += jf[nih_type]

        s['pool_to_%s' % nih_type] = pool_to_nih

    return s


def job_flow_to_full_summary(job_flow, now=None):
    """Convert a job flow to a full summary for use in creating a report,
    including billing/usage information."""

    jf = job_flow_to_basic_summary(job_flow, now=now)

    jf['usage'] = job_flow_to_usage_data(job_flow, basic_summary=jf, now=now)

    # add up billing info
    if jf['end']:
        # avoid rounding errors if the job is done
        jf['nih_billed'] = jf['nih']
    else:
        jf['nih_billed'] = float(sum(u['nih_billed'] for u in jf['usage']))

    for nih_type in ('nih_used', 'nih_bbnu'):
        jf[nih_type] = float(sum(u[nih_type] for u in jf['usage']))

    return jf


def job_flow_to_basic_summary(job_flow, now=None):
    """Extract fields such as creation time, owner, etc. from the job flow.

    We need to extract this information before we can compute billing
    information for the full summary.
    """
    if now is None:
        now = get_now()

    jf = {}  # summary to fill in

    jf['id'] = getattr(job_flow, 'jobflowid', None)
    jf['name'] = getattr(job_flow, 'name', None)

    jf['created'] = to_datetime(getattr(job_flow, 'creationdatetime', None))
    jf['start'] = to_datetime(getattr(job_flow, 'startdatetime', None))
    jf['ready'] = to_datetime(getattr(job_flow, 'readydatetime', None))
    jf['end'] = to_datetime(getattr(job_flow, 'enddatetime', None))

    jf['ran'] = (jf['end'] or now) - (jf['start'] or now)

    jf['state'] = getattr(job_flow, 'state', None)

    jf['num_steps'] = len(getattr(job_flow, 'steps', None) or ())

    jf['pool'] = None
    bootstrap_actions = getattr(job_flow, 'bootstrapactions', None)
    if bootstrap_actions:
        args = [arg.value for arg in bootstrap_actions[-1].args]
        if len(args) == 2 and args[0].startswith('pool-'):
            jf['pool'] = args[1]

    m = JOB_NAME_RE.match(getattr(job_flow, 'name', ''))
    if m:
        jf['label'], jf['owner'] = m.group(1), m.group(2)
    else:
        jf['label'], jf['owner'] = None, None

    jf['nih'] = float(getattr(job_flow, 'normalizedinstancehours', '0'))

    return jf


def job_flow_to_usage_data(job_flow, basic_summary=None, now=None):
    """Divide job flow into a list of dictionaries containing usage
    information, one for bootstrapping, and one for each step.

    If the job flow hasn't started yet, return []
    """
    jf = basic_summary or job_flow_to_basic_summary(job_flow)

    if now is None:
        now = get_now()

    if not jf['start']:
        return []

    # Figure out billing rate per second for the job, given that
    # normalizedinstancehours is how much we're charged up until
    # the next full hour.
    full_hours = math.ceil(to_secs(jf['ran']) / 60.0 / 60.0)
    nih_per_sec = jf['nih'] / (full_hours * 3600.0)

    # Don't actually count a step as billed for the full hour until
    # the job flow finishes. This means that our total "nih_billed"
    # will be less than normalizedinstancehours in the job flow, but it
    # also keeps stats stable for steps that have already finished.
    if jf['end']:
        jf_end_billing = jf['start'] + timedelta(hours=full_hours)
    else:
        jf_end_billing = now

    intervals = []

    # add a fake step for the job that started the job flow, and credit
    # it for time spent bootstrapping.
    intervals.append({
        'label': jf['label'],
        'owner': jf['owner'],
        'start': jf['start'],
        'end': jf['ready'] or now,
        'step_num': None,
    })

    for step in (getattr(job_flow, 'steps', None) or ()):
        # we've reached the last step that's actually run
        if not hasattr(step, 'startdatetime'):
            break

        step_start = to_datetime(step.startdatetime)

        step_end = to_datetime(getattr(step, 'enddatetime', None))
        if step_end is None:
            # step started running and was cancelled. credit it for 0 usage
            if jf['end']:
                step_end = step_start
            # step is still running
            else:
                step_end = now

        m = STEP_NAME_RE.match(getattr(step, 'name', ''))
        if m:
            step_label = m.group(1)
            step_owner = m.group(2)
            step_num = int(m.group(6))
        else:
            step_label, step_owner, step_num = None, None, None

        intervals.append({
            'label': step_label,
            'owner': step_owner,
            'start': step_start,
            'end': step_end,
            'step_num': step_num,
        })

    # fill in end_billing
    for i in xrange(len(intervals) - 1):
        intervals[i]['end_billing'] = intervals[i + 1]['start']

    intervals[-1]['end_billing'] = jf_end_billing

    # fill normalized usage information
    for interval in intervals:

        interval['nih_used'] = (
            nih_per_sec *
            to_secs(interval['end'] - interval['start']))

        interval['date_to_nih_used'] = dict(
            (d, nih_per_sec * secs)
            for d, secs
            in subdivide_interval_by_date(interval['start'],
                                          interval['end']).iteritems())

        interval['nih_billed'] = (
            nih_per_sec *
            to_secs(interval['end_billing'] - interval['start']))

        interval['date_to_nih_billed'] = dict(
            (d, nih_per_sec * secs)
            for d, secs
            in subdivide_interval_by_date(interval['start'],
                                          interval['end_billing']).iteritems())
        # time billed but not used
        interval['nih_bbnu'] = interval['nih_billed'] - interval['nih_used']

        interval['date_to_nih_bbnu'] = {}
        for d, nih_billed in interval['date_to_nih_billed'].iteritems():
            nih_bbnu = nih_billed - interval['date_to_nih_used'].get(d, 0.0)
            if nih_bbnu:
                interval['date_to_nih_bbnu'][d] = nih_bbnu

    return intervals


def subdivide_interval_by_date(start, end):
    if start.date() == end.date():
        date_to_secs = {start.date(): to_secs(end - start)}
    else:
        date_to_secs = {}

        date_to_secs[start.date()] = to_secs(
            datetime(start.year, start.month, start.day) + timedelta(days=1) -
            start)

        date_to_secs[end.date()] = to_secs(
            end - datetime(end.year, end.month, end.day))

        # fill in dates in the middle
        cur_date = start.date() + timedelta(days=1)
        while cur_date < end.date():
            date_to_secs[cur_date] = to_secs(timedelta(days=1))
            cur_date += timedelta(days=1)

    # remove zeros
    date_to_secs = dict(
        (d, secs) for d, secs in date_to_secs.iteritems() if secs)

    return date_to_secs


def get_now():
    """get the current time, without microseconds, which just make our
    report messy."""
    return datetime.utcnow().replace(microsecond=0)


def get_job_flows(conf_path, max_days_ago=None, now=None):
    if now is None:
        now = get_now()

    emr_conn = EMRJobRunner(conf_path=conf_path).make_emr_conn()

    # if --max-days-ago is set, only look at recent jobs
    created_after = None
    if max_days_ago is not None:
        created_after = now - timedelta(days=max_days_ago)

    return describe_all_job_flows(emr_conn, created_after=created_after)


def print_report(stats, now=None):
    if now is None:
        now = get_now()

    s = stats

    if not s['flows']:
        print 'No job flows created in the past two months!'
        return

    print 'Total  # of Job Flows: %d' % len(s['flows'])
    print

    print '* All times are in UTC.'
    print

    print 'Min create time: %s' % min(jf['created'] for jf in s['flows'])
    print 'Max create time: %s' % max(jf['created'] for jf in s['flows'])
    print '   Current time: %s' % now
    print

    print '* All usage is measured in Normalized Instance Hours, which are'
    print '  roughly equivalent to running an m1.small instance for an hour.'
    print "  Billing is estimated, and may not match Amazon's system exactly."
    print

    # total compute-unit hours used
    print 'Total billed:  %9.2f' % s['nih_billed']
    print '  Total used:  %9.2f' % s['nih_used']
    print '    bootstrap: %9.2f' % s['bootstrap_nih_used']
    print '    jobs:      %9.2f' % s['job_nih_used']
    print '  Total waste: %9.2f' % s['nih_bbnu']
    print '    at end:    %9.2f' % s['end_nih_bbnu']
    print '    other:     %9.2f' % s['other_nih_bbnu']
    print

    if s['date_to_nih_billed']:
        print 'Daily statistics:'
        print
        print ' date        billed    used     waste'
        d = max(s['date_to_nih_billed'])
        while d >= min(s['date_to_nih_billed']):
            print ' %10s %9.2f %9.2f %9.2f' % (
                d,
                s['date_to_nih_billed'][d],
                s['date_to_nih_used'].get(d, 0.0),
                s['date_to_nih_bbnu'].get(d, 0.0))
            d -= timedelta(days=1)
        print

    print '* Job flows are considered to belong to the user and job that'
    print '  started them or last ran on them.'
    print

    # Top jobs
    print 'Top jobs, by total time used:'
    for label, nih_used in sorted(s['label_to_nih_used'].iteritems(),
                                    key=lambda (lb, nih): (-nih, lb)):
        print '  %9.2f %s' % (nih_used, label)
    print

    print 'Top jobs, by time billed but not used:'
    for label, nih_bbnu in sorted(s['label_to_nih_bbnu'].iteritems(),
                                  key=lambda (lb, nih): (-nih, lb)):
        print '  %9.2f %s' % (nih_bbnu, label)
    print

    # Top users
    print 'Top users, by total time used:'
    for owner, nih_used in sorted(s['owner_to_nih_used'].iteritems(),
                                    key=lambda (o, nih): (-nih, o)):
        print '  %9.2f %s' % (nih_used, owner)
    print

    print 'Top users, by time billed but not used:'
    for owner, nih_bbnu in sorted(s['owner_to_nih_bbnu'].iteritems(),
                                  key=lambda (o, nih): (-nih, o)):
        print '  %9.2f %s' % (nih_bbnu, owner)
    print

    # Top job steps
    print 'Top job steps, by total time used (step number first):'
    for (label, step_num), nih_used in sorted(
        s['job_step_to_nih_used'].iteritems(), key=lambda (k, nih): (-nih, k)):
        if label:
            print '  %9.2f %3d %s' % (nih_used, step_num, label)
        else:
            print '  %9.2f     (non-mrjob step)' % (nih_used,)
    print

    print 'Top job steps, by total time billed but not used (un-pooled only):'
    for (label, step_num), nih_bbnu in sorted(
        s['job_step_to_nih_bbnu_no_pool'].iteritems(),
        key=lambda (k, nih): (-nih, k)):

        if label:
            print '  %9.2f %3d %s' % (nih_bbnu, step_num, label)
        else:
            print '  %9.2f     (non-mrjob step)' % (nih_bbnu,)
    print

    # Top pools
    print 'All pools, by total time billed:'
    for pool, nih_billed in sorted(s['pool_to_nih_billed'].iteritems(),
                                   key=lambda (p, nih): (-nih, p)):
        print '  %9.2f %s' % (nih_billed, pool or '(not pooled)')
    print

    print 'All pools, by total time billed but not used:'
    for pool, nih_bbnu in sorted(s['pool_to_nih_bbnu'].iteritems(),
                                 key=lambda (p, nih): (-nih, p)):
        print '  %9.2f %s' % (nih_bbnu, pool or '(not pooled)')
    print

    # Top job flows
    print 'All job flows, by total time billed:'
    top_job_flows = sorted(s['flows'],
                           key=lambda jf: (-jf['nih_billed'], jf['name']))
    for jf in top_job_flows:
        print '  %9.2f %-15s %s' % (
            jf['nih_billed'], jf['id'], jf['name'])
    print

    print 'All job flows, by time billed but not used:'
    top_job_flows_bbnu = sorted(s['flows'],
                                key=lambda jf: (-jf['nih_bbnu'], jf['name']))
    for jf in top_job_flows_bbnu:
        print '  %9.2f %-15s %s' % (
            jf['nih_bbnu'], jf['id'], jf['name'])
    print

    # Details
    print 'Details for all job flows:'
    print
    print (' id              state         created             steps'
           '        time ran     billed    waste   user   name')

    all_job_flows = sorted(s['flows'], key=lambda jf: jf['created'],
                           reverse=True)
    for jf in all_job_flows:
        print ' %-15s %-13s %19s %3d %17s %9.2f %9.2f %8s %s' % (
            jf['id'], jf['state'], jf['created'], jf['num_steps'],
            jf['ran'], jf['nih_used'], jf['nih_bbnu'],
            (jf['owner'] or ''), (jf['label'] or ('not started by mrjob')))


def to_secs(delta):
    return (delta.days * 86400.0 +
            delta.seconds +
            delta.microseconds / 1000000.0)


def to_datetime(iso8601_time):
    if iso8601_time is None:
        return None

    return datetime.strptime(iso8601_time, boto.utils.ISO8601)


if __name__ == '__main__':
    main(sys.argv[1:])
