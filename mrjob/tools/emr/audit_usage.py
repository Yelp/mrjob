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

    mrjob audit-emr-usage > report
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

from mrjob.emr import EMRJobRunner
from mrjob.emr import describe_all_job_flows
from mrjob.job import MRJob
from mrjob.options import add_basic_opts
from mrjob.parse import JOB_NAME_RE
from mrjob.parse import STEP_NAME_RE
from mrjob.util import strip_microseconds

log = logging.getLogger(__name__)


def main(args):
    # parser command-line args
    option_parser = make_option_parser()
    options, args = option_parser.parse_args(args)

    if args:
        option_parser.error('takes no arguments')

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)

    now = datetime.utcnow()

    log.info('getting job flow history...')
    job_flows = get_job_flows(
        options.conf_paths, options.max_days_ago, now=now)

    log.info('compiling job flow stats...')
    stats = job_flows_to_stats(job_flows, now=now)

    print_report(stats, now=now)


def make_option_parser():
    usage = '%prog [options]'
    description = 'Print a giant report on EMR usage.'

    option_parser = OptionParser(usage=usage, description=description)

    option_parser.add_option(
        '--max-days-ago', dest='max_days_ago', type='float', default=None,
        help=('Max number of days ago to look at jobs. By default, we go back'
              ' as far as EMR supports (currently about 2 months)'))

    add_basic_opts(option_parser)

    return option_parser


def job_flows_to_stats(job_flows, now=None):
    """Aggregate statistics for several job flows into a dictionary.

    :param job_flows: a list of :py:class:`boto.emr.EmrObject`
    :param now: the current UTC time, as a :py:class:`datetime.datetime`.
                Defaults to the current time.

    Returns a dictionary with many keys, including:

    * *flows*: A list of dictionaries; the result of running
      :py:func:`job_flow_to_full_summary` on each job flow.

    total usage:

    * *nih_billed*: total normalized instances hours billed, for all job flows
    * *nih_used*: total normalized instance hours actually used for
      bootstrapping and running jobs.
    * *nih_bbnu*: total usage billed but not used (`nih_billed - nih_used`)

    further breakdown of total usage:

    * *bootstrap_nih_used*: total usage for bootstrapping
    * *end_nih_bbnu*: unused time at the end of job flows
    * *job_nih_used*: total usage for jobs (`nih_used - bootstrap_nih_used`)
    * *other_nih_bbnu*: other unused time (`nih_bbnu - end_nih_bbnu`)

    grouping by various keys:

    (There is a *_used*, *_billed*, and *_bbnu* version of all stats below)

    * *date_to_nih_\**: map from a :py:class:`datetime.date` to number
      of normalized instance hours on that date
    * *hour_to_nih_\**: map from a :py:class:`datetime.datetime` to number
      of normalized instance hours during the hour starting at that time
    * *label_to_nih_\**: map from jobs' labels (usually the module name of
      the job) to normalized instance hours, with ``None`` for
      non-:py:mod:`mrjob` jobs. This includes usage data for bootstrapping.
    * *job_step_to_nih_\**: map from jobs' labels and step number to
      normalized instance hours, using ``(None, None)`` for non-:py:mod:`mrjob`
      jobs. This does not include bootstrapping.
    * *job_step_to_nih_\*_no_pool*: Same as *job_step_to_nih_\**, but only
      including non-pooled job flows.
    * *owner_to_nih_\**: map from jobs' owners (usually the user who ran them)
      to normalized instance hours, with ``None`` for non-:py:mod:`mrjob` jobs.
      This includes usage data for bootstrapping.
    * *pool_to_nih_\**: Map from pool name to normalized instance hours,
      with ``None`` for non-pooled jobs and non-:py:mod:`mrjob` jobs.
    """
    s = {}  # stats for all job flows

    s['flows'] = [job_flow_to_full_summary(job_flow, now=now)
                  for job_flow in job_flows]

    # total usage
    for nih_type in ('nih_billed', 'nih_used', 'nih_bbnu'):
        s[nih_type] = float(sum(jf[nih_type] for jf in s['flows']))

    # break down by usage/waste
    s['bootstrap_nih_used'] = float(sum(
        jf['usage'][0]['nih_used'] for jf in s['flows'] if jf['usage']))
    s['job_nih_used'] = s['nih_used'] - s['bootstrap_nih_used']
    s['end_nih_bbnu'] = float(sum(
        jf['usage'][-1]['nih_bbnu'] for jf in s['flows'] if jf['usage']))
    s['other_nih_bbnu'] = s['nih_bbnu'] - s['end_nih_bbnu']

    # stats by date/hour
    for interval_type in ('date', 'hour'):
        for nih_type in ('nih_billed', 'nih_used', 'nih_bbnu'):
            key = '%s_to_%s' % (interval_type, nih_type)
            start_to_nih = {}
            for jf in s['flows']:
                for u in jf['usage']:
                    for start, nih in u[key].iteritems():
                        start_to_nih.setdefault(start, 0.0)
                        start_to_nih[start] += nih
            s[key] = start_to_nih

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
    including billing/usage information.

    :param job_flow: a :py:class:`boto.emr.EmrObject`
    :param now: the current UTC time, as a :py:class:`datetime.datetime`.
                Defaults to the current time.

    Returns a dictionary with the keys from
    :py:func:`job_flow_to_basic_summary` plus:

    * *nih_billed*: total normalized instances hours billed for this job flow
    * *nih_used*: total normalized instance hours actually used for
      bootstrapping and running jobs.
    * *nih_bbnu*: total usage billed but not used (`nih_billed - nih_used`)
    * *usage*: job-specific usage information, returned by
      :py:func:`job_flow_to_usage_data`.
    """

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
    """Extract fields such as creation time, owner, etc. from the job flow,
    so we can safely reference them without using :py:func:`getattr`.

    :param job_flow: a :py:class:`boto.emr.EmrObject`
    :param now: the current UTC time, as a :py:class:`datetime.datetime`.
                Defaults to the current time.

    Returns a dictionary with the following keys. These will be ``None`` if the
    corresponding field in the job flow is unavailable.

    * *created*: UTC `datetime.datetime` that the job flow was created,
      or ``None``
    * *end*: UTC `datetime.datetime` that the job flow finished, or ``None``
    * *id*: job flow ID, or ``None`` (this should never happen)
    * *label*: The label for the job flow (usually the module name of the
      :py:class:`~mrjob.job.MRJob` script that started it), or
      ``None`` for non-:py:mod:`mrjob` job flows.
    * *name*: job flow name, or ``None`` (this should never happen)
    * *nih*: number of normalized instance hours used by the job flow.
    * *num_steps*: Number of steps in the job flow.
    * *owner*: The owner for the job flow (usually the user that started it),
      or ``None`` for non-:py:mod:`mrjob` job flows.
    * *pool*: pool name (e.g. ``'default'``) if the job flow is pooled,
      otherwise ``None``.
    * *ran*: How long the job flow ran, or has been running, as a
      :py:class:`datetime.timedelta`. This will be ``timedelta(0)`` if
      the job flow hasn't started.
    * *ready*: UTC `datetime.datetime` that the job flow finished
      bootstrapping, or ``None``
    * *start*: UTC `datetime.datetime` that the job flow became available, or
      ``None``
    * *state*: The job flow's state as a string (e.g. ``'RUNNING'``)
    """
    if now is None:
        now = datetime.utcnow()

    jf = {}  # summary to fill in

    jf['id'] = getattr(job_flow, 'jobflowid', None)
    jf['name'] = getattr(job_flow, 'name', None)

    jf['created'] = to_datetime(getattr(job_flow, 'creationdatetime', None))
    jf['start'] = to_datetime(getattr(job_flow, 'startdatetime', None))
    jf['ready'] = to_datetime(getattr(job_flow, 'readydatetime', None))
    jf['end'] = to_datetime(getattr(job_flow, 'enddatetime', None))

    if jf['start']:
        jf['ran'] = (jf['end'] or now) - jf['start']
    else:
        jf['ran'] = timedelta(0)

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
    """Break billing/usage information for a job flow down by job.

    :param job_flow: a :py:class:`boto.emr.EmrObject`
    :param basic_summary: a basic summary of the job flow, returned by
                          :py:func:`job_flow_to_basic_summary`. If this
                          is ``None``, we'll call
                          :py:func:`job_flow_to_basic_summary` ourselves.
    :param now: the current UTC time, as a :py:class:`datetime.datetime`.
                Defaults to the current time.

    Returns a list of dictionaries containing usage information, one for
    bootstrapping, and one for each step that ran or is currently running. If
    the job flow hasn't started yet, return ``[]``.

    Usage dictionaries have the following keys:

    * *end*: when the job finished running, or *now* if it's still running.
    * *end_billing*: the effective end of the job for billing purposes, either
      when the next job starts, the current time if the job
      is still running, or the end of the next full hour
      in the job flow.
    * *nih_billed*: normalized instances hours billed for this job or
      bootstrapping step
    * *nih_used*: normalized instance hours actually used for running
      the job or bootstrapping
    * *nih_bbnu*: usage billed but not used (`nih_billed - nih_used`)
    * *date_to_nih_\**: map from a :py:class:`datetime.date` to number
      of normalized instance hours billed/used/billed but not used on that date
    * *hour_to_nih_\**: map from a :py:class:`datetime.datetime` to number
      of normalized instance hours billed/used/billed but not used during
      the hour starting at that time
    * *label*: job's label (usually the module name of the job), or for the
      bootstrapping step, the label of the job flow
    * *owner*: job's owner (usually the user that started it), or for the
      bootstrapping step, the owner of the job flow
    * *start*: when the job or bootstrapping step started, as a
      :py:class:`datetime.datetime`
    """
    jf = basic_summary or job_flow_to_basic_summary(job_flow)

    if now is None:
        now = datetime.utcnow()

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

        interval['hour_to_nih_used'] = dict(
            (d, nih_per_sec * secs)
            for d, secs
            in subdivide_interval_by_hour(interval['start'],
                                          interval['end']).iteritems())

        interval['nih_billed'] = (
            nih_per_sec *
            to_secs(interval['end_billing'] - interval['start']))

        interval['date_to_nih_billed'] = dict(
            (d, nih_per_sec * secs)
            for d, secs
            in subdivide_interval_by_date(interval['start'],
                                          interval['end_billing']).iteritems())

        interval['hour_to_nih_billed'] = dict(
            (d, nih_per_sec * secs)
            for d, secs
            in subdivide_interval_by_hour(interval['start'],
                                          interval['end_billing']).iteritems())

        # time billed but not used
        interval['nih_bbnu'] = interval['nih_billed'] - interval['nih_used']

        interval['date_to_nih_bbnu'] = {}
        for d, nih_billed in interval['date_to_nih_billed'].iteritems():
            nih_bbnu = nih_billed - interval['date_to_nih_used'].get(d, 0.0)
            if nih_bbnu:
                interval['date_to_nih_bbnu'][d] = nih_bbnu

        interval['hour_to_nih_bbnu'] = {}
        for d, nih_billed in interval['hour_to_nih_billed'].iteritems():
            nih_bbnu = nih_billed - interval['hour_to_nih_used'].get(d, 0.0)
            if nih_bbnu:
                interval['hour_to_nih_bbnu'][d] = nih_bbnu

    return intervals


def subdivide_interval_by_date(start, end):
    """Convert a time interval to a map from :py:class:`datetime.date` to
    the number of seconds within the interval on that date.

    *start* and *end* are :py:class:`datetime.datetime` objects.
    """
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


def subdivide_interval_by_hour(start, end):
    """Convert a time interval to a map from hours (represented as
    :py:class:`datetime.datetime` for the start of the hour) to the number of
    seconds during that hour that are within the interval

    *start* and *end* are :py:class:`datetime.datetime` objects.
    """
    start_hour = start.replace(minute=0, second=0, microsecond=0)
    end_hour = end.replace(minute=0, second=0, microsecond=0)

    if start_hour == end_hour:
        hour_to_secs = {start_hour: to_secs(end - start)}
    else:
        hour_to_secs = {}

        hour_to_secs[start_hour] = to_secs(
            start_hour + timedelta(hours=1) - start)

        hour_to_secs[end_hour] = to_secs(end - end_hour)

        # fill in dates in the middle
        cur_hour = start_hour + timedelta(hours=1)
        while cur_hour < end_hour:
            hour_to_secs[cur_hour] = to_secs(timedelta(hours=1))
            cur_hour += timedelta(hours=1)

    # remove zeros
    hour_to_secs = dict(
        (h, secs) for h, secs in hour_to_secs.iteritems() if secs)

    return hour_to_secs


def get_job_flows(conf_paths, max_days_ago=None, now=None):
    """Get relevant job flow information from EMR.

    :param str conf_path: Alternate path to read :py:mod:`mrjob.conf` from, or
                          ``False`` to ignore all config files.
    :param float max_days_ago: If set, don't fetch job flows created longer
                               than this many days ago.
    :param now: the current UTC time, as a :py:class:`datetime.datetime`.
                Defaults to the current time.
    """
    if now is None:
        now = datetime.utcnow()

    emr_conn = EMRJobRunner(conf_paths=conf_paths).make_emr_conn()

    # if --max-days-ago is set, only look at recent jobs
    created_after = None
    if max_days_ago is not None:
        created_after = now - timedelta(days=max_days_ago)

    return describe_all_job_flows(emr_conn, created_after=created_after)


def print_report(stats, now=None):
    """Print final report.

    :param stats: a dictionary returned by :py:func:`job_flows_to_stats`
    :param now: the current UTC time, as a :py:class:`datetime.datetime`.
                Defaults to the current time.
    """
    if now is None:
        now = datetime.utcnow()

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
    print '   Current time: %s' % now.replace(microsecond=0)
    print

    print '* All usage is measured in Normalized Instance Hours, which are'
    print '  roughly equivalent to running an m1.small instance for an hour.'
    print "  Billing is estimated, and may not match Amazon's system exactly."
    print

    # total compute-unit hours used
    def with_pct(usage):
        return (usage, percent(usage, s['nih_billed']))

    print 'Total billed:  %9.2f  %5.1f%%' % with_pct(s['nih_billed'])
    print '  Total used:  %9.2f  %5.1f%%' % with_pct(s['nih_used'])
    print '    bootstrap: %9.2f  %5.1f%%' % with_pct(s['bootstrap_nih_used'])
    print '    jobs:      %9.2f  %5.1f%%' % with_pct(s['job_nih_used'])
    print '  Total waste: %9.2f  %5.1f%%' % with_pct(s['nih_bbnu'])
    print '    at end:    %9.2f  %5.1f%%' % with_pct(s['end_nih_bbnu'])
    print '    other:     %9.2f  %5.1f%%' % with_pct(s['other_nih_bbnu'])
    print

    if s['date_to_nih_billed']:
        print 'Daily statistics:'
        print
        print ' date          billed      used     waste   % waste'
        d = max(s['date_to_nih_billed'])
        while d >= min(s['date_to_nih_billed']):
            print ' %10s %9.2f %9.2f %9.2f     %5.1f' % (
                d,
                s['date_to_nih_billed'].get(d, 0.0),
                s['date_to_nih_used'].get(d, 0.0),
                s['date_to_nih_bbnu'].get(d, 0.0),
                percent(s['date_to_nih_bbnu'].get(d, 0.0),
                        s['date_to_nih_billed'].get(d, 0.0)))
            d -= timedelta(days=1)
        print

    if s['hour_to_nih_billed']:
        print 'Hourly statistics:'
        print
        print ' hour              billed      used     waste   % waste'
        h = max(s['hour_to_nih_billed'])
        while h >= min(s['hour_to_nih_billed']):
            print ' %13s  %9.2f %9.2f %9.2f     %5.1f' % (
                h.strftime('%Y-%m-%d %H'),
                s['hour_to_nih_billed'].get(h, 0.0),
                s['hour_to_nih_used'].get(h, 0.0),
                s['hour_to_nih_bbnu'].get(h, 0.0),
                percent(s['hour_to_nih_bbnu'].get(h, 0.0),
                        s['hour_to_nih_billed'].get(h, 0.0)))
            h -= timedelta(hours=1)
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
            s['job_step_to_nih_used'].iteritems(),
            key=lambda (k, nih): (-nih, k)):

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
            strip_microseconds(jf['ran']), jf['nih_used'], jf['nih_bbnu'],
            (jf['owner'] or ''), (jf['label'] or ('not started by mrjob')))


def to_secs(delta):
    """Convert a :py:class:`datetime.timedelta` to a number of seconds.

    (This is basically a backport of
    :py:meth:`datetime.timedelta.total_seconds`.)
    """
    return (delta.days * 86400.0 +
            delta.seconds +
            delta.microseconds / 1000000.0)


def to_datetime(iso8601_time):
    """Convert a ISO8601-formatted datetime (from :py:mod:`boto`) to
    a :py:class:`datetime.datetime`."""
    if iso8601_time is None:
        return None

    return datetime.strptime(iso8601_time, boto.utils.ISO8601)


def percent(x, total, default=0.0):
    """Return what percentage *x* is of *total*, or *default* if
    *total* is zero."""
    if total:
        return 100.0 * x / total
    else:
        return default


if __name__ == '__main__':
    main(None)
