# Copyright 2012 Yelp
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
"""Report jobs running for more than a certain number of hours (by default,
24.0). This can help catch buggy jobs and Hadoop/EMR operational issues.

Suggested usage: run this as a daily cron job with the ``-q`` option::

    0 0 * * * mrjob report-long-jobs
    0 0 * * * python -m mrjob.tools.emr.report_long_jobs -q

Options::

  -h, --help            show this help message and exit
  -v, --verbose         print more messages to stderr
  -q, --quiet           Don't log status messages; just print the report.
  -c CONF_PATH, --conf-path=CONF_PATH
                        Path to alternate mrjob.conf file to read from
  --no-conf             Don't load mrjob.conf even if it's available
  --min-hours=MIN_HOURS
                        Minimum number of hours a job can run before we report
                        it. Default: 24.0
"""
from __future__ import with_statement

from datetime import datetime
from datetime import timedelta
import logging
from optparse import OptionParser
import sys

import boto.utils

from mrjob.emr import EMRJobRunner
from mrjob.emr import describe_all_job_flows
from mrjob.job import MRJob
from mrjob.options import add_basic_opts
from mrjob.util import strip_microseconds

# default minimum number of hours a job can run before we report it.
DEFAULT_MIN_HOURS = 24.0

log = logging.getLogger(__name__)


def main(args, now=None):
    if now is None:
        now = datetime.utcnow()

    option_parser = make_option_parser()
    options, args = option_parser.parse_args(args)

    if args:
        option_parser.error('takes no arguments')

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)

    log.info('getting information about running jobs')
    emr_conn = EMRJobRunner(conf_paths=options.conf_paths).make_emr_conn()
    job_flows = describe_all_job_flows(
        emr_conn, states=['BOOTSTRAPPING', 'RUNNING'])

    min_time = timedelta(hours=options.min_hours)

    job_info = find_long_running_jobs(job_flows, min_time, now=now)

    print_report(job_info)


def find_long_running_jobs(job_flows, min_time, now=None):
    """Identify jobs that have been running or pending for a long time.

    :param job_flows: a list of :py:class:`boto.emr.emrobject.JobFlow`
                      objects to inspect.
    :param min_time: a :py:class:`datetime.timedelta`: report jobs running or
                     pending longer than this
    :param now: the current UTC time, as a :py:class:`datetime.datetime`.
                Defaults to the current time.

    For each job that is running or pending longer than *min_time*, yields
    a dictionary with the following keys:

    * *job_flow_id*: the job flow's unique ID (e.g. ``j-SOMEJOBFLOW``)
    * *name*: name of the step, or the job flow when bootstrapping
    * *step_state*: state of the step, either ``'RUNNING'`` or ``'PENDING'``
    * *time*: amount of time step was running or pending, as a
              :py:class:`datetime.timedelta`
    """
    if now is None:
        now = datetime.utcnow()

    for jf in job_flows:

        # special case for jobs that are taking a long time to bootstrap
        if jf.state == 'BOOTSTRAPPING':
            start_timestamp = jf.startdatetime
            start = datetime.strptime(start_timestamp, boto.utils.ISO8601)

            time_running = now - start

            if time_running >= min_time:
                # we tell bootstrapping info by step_state being empty,
                # and only use job_flow_id and time in the report
                yield({'job_flow_id': jf.jobflowid,
                       'name': jf.name,
                       'step_state': '',
                       'time': time_running})

        # the default case: running job flows
        if jf.state != 'RUNNING':
            continue

        running_steps = [step for step in jf.steps if step.state == 'RUNNING']
        pending_steps = [step for step in jf.steps if step.state == 'PENDING']

        if running_steps:
            # should be only one, but if not, we should know
            for step in running_steps:

                start_timestamp = step.startdatetime
                start = datetime.strptime(start_timestamp, boto.utils.ISO8601)

                time_running = now - start

                if time_running >= min_time:
                    yield({'job_flow_id': jf.jobflowid,
                           'name': step.name,
                           'step_state': step.state,
                           'time': time_running})

        # sometimes EMR says it's "RUNNING" but doesn't actually run steps!
        elif pending_steps:
            step = pending_steps[0]

            # PENDING job should have run starting when the job flow
            # became ready, or the previous step completed
            start_timestamp = jf.readydatetime
            for step in jf.steps:
                if step.state == 'COMPLETED':
                    start_timestamp = step.enddatetime

            start = datetime.strptime(start_timestamp, boto.utils.ISO8601)
            time_pending = now - start

            if time_pending >= min_time:
                yield({'job_flow_id': jf.jobflowid,
                       'name': step.name,
                       'step_state': step.state,
                       'time': time_pending})


def print_report(job_info):
    """Takes in a dictionary of info about a long-running job (see
    :py:func:`find_long_running_jobs`), and prints information about it
    on a single (long) line.
    """
    for ji in job_info:
        # BOOTSTRAPPING case
        if not ji['step_state']:
            print '%-15s BOOTSTRAPPING for %17s (%s)' % (
                ji['job_flow_id'], format_timedelta(ji['time']),
                ji['name'])
        else:
            print '%-15s       %7s for %17s (%s)' % (
                ji['job_flow_id'],
                ji['step_state'], format_timedelta(ji['time']),
                ji['name'])


def format_timedelta(time):
    """Format a timedelta for use in a columnar format. This just
    tweaks stuff like ``'3 days, 9:00:00'`` to line up with
    ``'3 days, 10:00:00'``
    """
    result = str(strip_microseconds(time))

    parts = result.split()
    if len(parts) == 3 and len(parts[-1]) == 7:
        return '%s %s  %s' % tuple(parts)
    else:
        return result


def make_option_parser():
    usage = '%prog [options]'
    description = ('Report jobs running for more than a certain number of'
                   ' hours (by default, %.1f). This can help catch buggy jobs'
                   ' and Hadoop/EMR operational issues.' % DEFAULT_MIN_HOURS)

    option_parser = OptionParser(usage=usage, description=description)

    option_parser.add_option(
        '--min-hours', dest='min_hours', type='float',
        default=DEFAULT_MIN_HOURS,
        help=('Minimum number of hours a job can run before we report it.'
              ' Default: %default'))

    add_basic_opts(option_parser)

    return option_parser


if __name__ == '__main__':
    main(sys.argv[1:])
