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
"""Count EMR list active clusters and instances now.

Usage::

    mrjob count-emr-list-active > report
    python -m mrjob.tools.emr.count_emr_list_active > report

Options::

  -h, --help            show this help message and exit
  -v, --verbose         print more messages to stderr
  -q, --quiet           Don't log status messages; just print the report.
  -c CONF_PATH, --conf-path=CONF_PATH
                        Path to alternate mrjob.conf file to read from
  --no-conf             Don't load mrjob.conf even if it's available

"""
from __future__ import with_statement

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
from mrjob.parse import iso8601_to_datetime
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

    print options

    log.info('list all active emr jobflows')
    job_flows = get_active_job_flows(options.conf_paths, now=now)

    print job_flows
    log.info('compiling active emr jobflows stats...')

    import subprocess
    subprocess.call(['emr --list --state STARTING'], shell=True)

    for jf in job_flows:
        print job_flow_to_basic_summary(jf, now=now)
#    stats = job_flows_to_stats(job_flows, now=now)
#    print stats
#    print_report(stats, now=now)


def make_option_parser():
    usage = '%prog [options]'
    description = 'Count EMR active jobflows now, identical to \'emr --list --active\'.'

    option_parser = OptionParser(usage=usage, description=description)

#    option_parser.add_option(
#        '--max-days-ago', dest='max_days_ago', type='float', default=None,
#        help=('Max number of days ago to look at jobs. By default, we go back'
#              ' as far as EMR supports (currently about 2 months)'))

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


    """
    s = {}  # stats for all job flows
    s['flows'] = [job_flow_to_basic_summary(job_flow, now=now)
                  for job_flow in job_flows]
    return s

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



def get_active_job_flows(conf_paths, now=None):
    """Get current active job flow information from EMR (STARTING, BOOTSTRAPPING, WAITING).
    :param str conf_path: Alternate path to read :py:mod:`mrjob.conf` from, or
                          ``False`` to ignore all config files.
    :param now: the current UTC time, as a :py:class:`datetime.datetime`.
                Defaults to the current time.
    """
    if now is None:
        now = datetime.utcnow()

    emr_conn = EMRJobRunner(conf_paths=conf_paths).make_emr_conn()

#    states = ['STARTING', 'BOOTSTRAPPING', 'WAITING']
    states = ['STARTING']

    return describe_all_job_flows(emr_conn, states=states)


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


def to_datetime(iso8601_time):
    """Convert a ISO8601-formatted datetime (from :py:mod:`boto`) to
        a :py:class:`datetime.datetime`."""
    if iso8601_time is None:
        return None
    return iso8601_to_datetime(iso8601_time)


if __name__ == '__main__':
    main(None)
