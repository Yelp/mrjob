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
24).

Suggested usage: run this as a cron job with the ``-q`` option::

    python -m mrjob.tools.emr.report_long_jobs -q

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
from mrjob.util import strip_microseconds

# default minimum number of hours a job can run before we report it.
DEFAULT_MIN_HOURS = 24.0

log = logging.getLogger('mrjob.tools.emr.report_long_jobs')


def main(args):
    option_parser = make_option_parser()
    options, args = option_parser.parse_args(args)

    if args:
        option_parser.error('takes no arguments')

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)

    log.info('getting information about running jobs')
    emr_conn = EMRJobRunner(conf_path=options.conf_path).make_emr_conn()
    job_flows = describe_all_job_flows(emr_conn, states=['RUNNING'])

    min_time = timedelta(hours=options.min_hours)

    job_info = find_long_running_jobs(job_flows, min_time)

    print_report(job_info)


def find_long_running_jobs(job_flows, min_time, now=None):
    if now is None:
        now = datetime.utcnow()

    for jf in job_flows:
        assert jf.state == 'RUNNING'

        total_steps = len(jf.steps)
        
        num_and_running_steps = [(step_num, step)
                                 for (step_num, step) in enumerate(jf.steps)
                                 if step.state == 'RUNNING']
        num_and_pending_steps = [(step_num, step)
                                 for (step_num, step) in enumerate(jf.steps)
                                 if step.state == 'PENDING']

        if num_and_running_steps:
            # should be only one, but if not, we should know
            for step_num, step in num_and_running_steps:
                
                start_timestamp = step.startdatetime
                start = datetime.strptime(start_timestamp, boto.utils.ISO8601)

                time_running = now - start

                if time_running >= min_time:
                    yield(jf.jobflowid, step_num, total_steps,
                          step.name, step.state, time_running)

        # sometimes EMR says it's "RUNNING" but doesn't actually run steps!
        elif num_and_pending_steps:
            step_num, step = num_and_pending_steps[0]

            # PENDING job should have run starting when the job flow
            # became ready, or the previous step completed
            start_timestamp = jf.readydatetime
            for step in jf.steps:
                if step.state == 'COMPLETED':
                    start_timestamp = step.enddatetime

            start = datetime.strptime(start_timestamp, boto.utils.ISO8601)
            time_pending = now - start

            if time_pending >= min_time:
                    yield(jf.jobflowid, step_num, total_steps,
                          step.name, step.state, time_pending)


def print_report(job_info):

    for ji in job_info:
        (job_flow_id, step_num, total_steps, step_name, step_state, time) = ji
        print '%-15s step %3d of %3d: %7s for %17s (%s)' % (
            job_flow_id, step_num + 1, total_steps,
            step_state, format_timedelta(time), step_name)


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
                   ' hours (by default, %d).' % DEFAULT_MIN_HOURS)
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
        '--min-hours', dest='min_hours', type='float',
        default=DEFAULT_MIN_HOURS,
        help=('Minimum number of hours a job can run before we report it.'
              ' Default: %default'))

    return option_parser


if __name__ == '__main__':
    main(sys.argv[1:])
