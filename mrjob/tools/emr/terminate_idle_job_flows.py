# Copyright 2009-2012 Yelp
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
# limitations under the License
"""Terminate idle EMR job flows that meet the criteria passed in on the command
line (or, by default, job flows that have been idle for one hour).

Suggested usage: run this as a cron job with the -q option::

    */30 * * * * python -m mrjob.tools.emr.terminate_idle_job_flows -q

Options::

  -h, --help            show this help message and exit
  -v, --verbose         Print more messages
  -q, --quiet           Don't print anything to stderr; just print IDs of
                        terminated job flows and idle time information to
                        stdout
  -c CONF_PATH, --conf-path=CONF_PATH
                        Path to alternate mrjob.conf file to read from
  --no-conf             Don't load mrjob.conf even if it's available
  --max-hours-idle=MAX_HOURS_IDLE
                        Max number of hours a job can run before being
                        terminated
  --mins-to-end-of-hour=MINS_TO_END_OF_HOUR
                        Terminate job flows that are within this many minutes
                        of the end of a full hour since the job started
                        running (since job flows are billed by the full hour)
  --unpooled-only       Only terminate un-pooled job flows
  --pooled-only         Only terminate pooled job flows
  --pool-name=POOL_NAME
                        Only terminate job flows in the given named pool.
  --dry-run             Don't actually kill idle jobs; just log that we would
"""
from datetime import datetime
from datetime import timedelta
import logging
from optparse import OptionParser
import re

try:
    import boto.utils
except ImportError:
    boto = None

from mrjob.emr import EMRJobRunner
from mrjob.emr import describe_all_job_flows
from mrjob.job import MRJob
from mrjob.pool import est_time_to_hour
from mrjob.pool import pool_hash_and_name
from mrjob.util import strip_microseconds

log = logging.getLogger('mrjob.tools.emr.terminate_idle_job_flows')

DEFAULT_MAX_HOURS_IDLE = 1

DEBUG_JAR_RE = re.compile(
    r's3n://.*\.elasticmapreduce/libs/state-pusher/[^/]+/fetch')


def main():
    option_parser = make_option_parser()
    options, args = option_parser.parse_args()

    if args:
        option_parser.error('takes no arguments')

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)

    inspect_and_maybe_terminate_job_flows(
        conf_path=options.conf_path,
        dry_run=options.dry_run,
        max_hours_idle=options.max_hours_idle,
        mins_to_end_of_hour=options.mins_to_end_of_hour,
        unpooled_only=options.unpooled_only,
        now=datetime.utcnow(),
        pool_name=options.pool_name,
        pooled_only=options.pooled_only,
    )


def inspect_and_maybe_terminate_job_flows(
    conf_path=None,
    dry_run=False,
    max_hours_idle=None,
    mins_to_end_of_hour=None,
    now=None,
    pool_name=None,
    pooled_only=False,
    unpooled_only=False,
):

    if now is None:
        now = datetime.utcnow()

    # old default behavior
    if max_hours_idle is None and mins_to_end_of_hour is None:
        max_hours_idle = DEFAULT_MAX_HOURS_IDLE

    emr_conn = EMRJobRunner(conf_path=conf_path).make_emr_conn()

    log.info(
        'getting info about all job flows (this goes back about 2 months)')
    # We don't filter by job flow state because we want this to work even
    # if Amazon adds another kind of idle state.
    job_flows = describe_all_job_flows(emr_conn)

    num_running = 0
    num_idle = 0
    num_done = 0
    num_non_streaming = 0
    # a list of tuples of job flow id, name, idle time (as a timedelta)
    to_terminate = []

    for jf in job_flows:

        # check if job flow is done
        if is_job_flow_done(jf):
            num_done += 1

        # we can't really tell if non-streaming jobs are idle or not, so
        # let them be (see Issue #60)
        elif is_job_flow_non_streaming(jf):
            num_non_streaming += 1

        elif is_job_flow_running(jf):
            num_running += 1

        else:
            num_idle += 1
            time_idle = time_job_flow_idle(jf, now=now)
            time_to_end_of_hour = est_time_to_hour(jf, now=now)
            _, pool = pool_hash_and_name(jf)

            log.debug(
                'Job flow %-15s idle for %s, %s to end of hour, %s (%s)' %
                      (jf.jobflowid,
                       strip_microseconds(time_idle),
                       strip_microseconds(time_to_end_of_hour),
                       ('unpooled' if pool is None else 'in %s pool' % pool),
                       jf.name))

            # filter out job flows that don't meet our criteria
            if (max_hours_idle is not None and
                time_idle <= timedelta(hours=max_hours_idle)):
                continue

            if (mins_to_end_of_hour is not None and
                time_to_end_of_hour >=
                    timedelta(minutes=mins_to_end_of_hour)):
                continue

            if (pooled_only and pool is None):
                continue

            if (unpooled_only and pool is not None):
                continue

            if (pool_name is not None and pool != pool_name):
                continue

            to_terminate.append(
                (jf.jobflowid, jf.name, time_idle, time_to_end_of_hour))

    log.info(
        'Job flow statuses: %d running, %d idle, %d active non-streaming,'
        ' %d done' % (num_running, num_idle, num_non_streaming, num_done))

    terminate_and_notify(emr_conn, to_terminate, dry_run=dry_run)


def is_job_flow_done(job_flow):
    """Return True if the given job flow is done running."""
    return hasattr(job_flow, 'enddatetime')


def is_job_flow_non_streaming(job_flow):
    """Return True if the give job flow has steps, but none of them are
    Hadoop streaming steps (for example, if the job flow is running Hive).
    """
    steps = getattr(job_flow, 'steps', None)

    if not steps:
        return False

    for step in steps:
        args = [a.value for a in step.args]
        for arg in args:
            # This is hadoop streaming
            if arg == '-mapper':
                return False
            # This is a debug jar associated with hadoop streaming
            if DEBUG_JAR_RE.match(arg):
                return False

    # job has at least one step, and none are streaming steps
    return True


def is_job_flow_running(job_flow):
    """Return ``True`` if the given job has any steps which are currently
    running."""
    steps = getattr(job_flow, 'steps', None) or []
    active_steps = [step for step in steps if step.state != 'CANCELLED']

    if not active_steps:
        return False

    return not getattr(active_steps[-1], 'enddatetime', None)


def time_job_flow_idle(job_flow, now):
    """How long has the given job flow been idle?

    :param job_flow: job flow to inspect
    :type now: :py:class:`datetime.datetime`
    :param now: the current time, in UTC

    :rtype: :py:class:`datetime.timedelta`
    """
    if is_job_flow_done(job_flow):
        return timedelta(0)

    steps = getattr(job_flow, 'steps', None) or []
    active_steps = [step for step in steps if step.state != 'CANCELLED']

    if active_steps:
        if not getattr(active_steps[-1], 'enddatetime', None):
            return timedelta(0)
        else:
            return now - datetime.strptime(
                active_steps[-1].enddatetime, boto.utils.ISO8601)
    elif getattr(job_flow, 'startdatetime', None):
        return now - datetime.strptime(job_flow.startdatetime,
                                       boto.utils.ISO8601)
    else:
        return now - datetime.strptime(job_flow.creationdatetime,
                                       boto.utils.ISO8601)


def terminate_and_notify(emr_conn, to_terminate, dry_run=False):
    if not to_terminate:
        return

    for job_flow_id, name, time_idle, time_to_end_of_hour in to_terminate:
        if not dry_run:
            emr_conn.terminate_jobflow(job_flow_id)
        print ('Terminated job flow %s (%s); was idle for %s,'
               ' %s to end of hour' %
               (job_flow_id, name, strip_microseconds(time_idle),
                strip_microseconds(time_to_end_of_hour)))


def make_option_parser():
    usage = '%prog [options]'
    description = ('Terminate idle EMR job flows that meet the criteria'
                   ' passed in on the command line (or, by default,'
                   ' job flows that have been idle for one hour).')
    option_parser = OptionParser(usage=usage, description=description)
    option_parser.add_option(
        '-v', '--verbose', dest='verbose', default=False,
        action='store_true',
        help='Print more messages')
    option_parser.add_option(
        '-q', '--quiet', dest='quiet', default=False,
        action='store_true',
        help=("Don't print anything to stderr; just print IDs of terminated"
              " job flows and idle time information to stdout"))
    option_parser.add_option(
        '-c', '--conf-path', dest='conf_path', default=None,
        help='Path to alternate mrjob.conf file to read from')
    option_parser.add_option(
        '--no-conf', dest='conf_path', action='store_false',
        help="Don't load mrjob.conf even if it's available")
    option_parser.add_option(
        '--max-hours-idle', dest='max_hours_idle',
        default=None, type='float',
        help='Max number of hours a job can run before being terminated')
    option_parser.add_option(
        '--mins-to-end-of-hour', dest='mins_to_end_of_hour',
        default=None, type='float',
        help=('Terminate job flows that are within this many minutes of'
              ' the end of a full hour since the job started running'
              ' (since job flows are billed by the full hour)'))
    option_parser.add_option(
        '--unpooled-only', dest='unpooled_only', action='store_true',
        default=False,
        help='Only terminate un-pooled job flows')
    option_parser.add_option(
        '--pooled-only', dest='pooled_only', action='store_true',
        default=False,
        help='Only terminate pooled job flows')
    option_parser.add_option(
        '--pool-name', dest='pool_name', default=None,
        help='Only terminate job flows in the given named pool.')
    option_parser.add_option(
        '--dry-run', dest='dry_run', default=False,
        action='store_true',
        help="Don't actually kill idle jobs; just log that we would")

    return option_parser


if __name__ == '__main__':
    main()
