# -*- coding: utf-8 -*-
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

Suggested usage: run this as a cron job with the ``-q`` option::

    */30 * * * * mrjob terminate-idle-job-flows -q
    */30 * * * * python -m mrjob.tools.emr.terminate_idle_job_flows -q

Options::

  -h, --help            show this help message and exit
  -v, --verbose         Print more messages
  -q, --quiet           Don't print anything to stderr; just print IDs of
                        terminated job flows and idle time information to
                        stdout. Use twice to print absolutely nothing.
  -c CONF_PATH, --conf-path=CONF_PATH
                        Path to alternate mrjob.conf file to read from
  --no-conf             Don't load mrjob.conf even if it's available
  --max-hours-idle=MAX_HOURS_IDLE
                        Max number of hours a job flow can go without
                        bootstrapping, running a step, or having a new step
                        created. This will fire even if there are pending
                        steps which EMR has failed to start. Make sure you set
                        this higher than the amount of time your jobs can take
                        to start instances and bootstrap.
  --mins-to-end-of-hour=MINS_TO_END_OF_HOUR
                        Terminate job flows that are within this many minutes
                        of the end of a full hour since the job started
                        running AND have no pending steps.
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
    boto  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    boto = None

from mrjob.emr import attempt_to_acquire_lock
from mrjob.emr import EMRJobRunner
from mrjob.emr import describe_all_job_flows
from mrjob.job import MRJob
from mrjob.options import add_basic_opts
from mrjob.pool import est_time_to_hour
from mrjob.pool import pool_hash_and_name
from mrjob.util import strip_microseconds

log = logging.getLogger(__name__)

DEFAULT_MAX_HOURS_IDLE = 1
DEFAULT_MAX_MINUTES_LOCKED = 1

DEBUG_JAR_RE = re.compile(
    r's3n://.*\.elasticmapreduce/libs/state-pusher/[^/]+/fetch')


def main(cl_args=None):
    option_parser = make_option_parser()
    options, args = option_parser.parse_args(cl_args)

    if args:
        option_parser.error('takes no arguments')

    MRJob.set_up_logging(quiet=options.quiet,
                         verbose=options.verbose)

    inspect_and_maybe_terminate_job_flows(
        conf_paths=options.conf_paths,
        dry_run=options.dry_run,
        max_hours_idle=options.max_hours_idle,
        mins_to_end_of_hour=options.mins_to_end_of_hour,
        unpooled_only=options.unpooled_only,
        now=datetime.utcnow(),
        pool_name=options.pool_name,
        pooled_only=options.pooled_only,
        max_mins_locked=options.max_mins_locked,
        quiet=(options.quiet > 1),
    )


def inspect_and_maybe_terminate_job_flows(
    conf_paths=None,
    dry_run=False,
    max_hours_idle=None,
    mins_to_end_of_hour=None,
    now=None,
    pool_name=None,
    pooled_only=False,
    unpooled_only=False,
    max_mins_locked=None,
    quiet=False,
    **kwargs
):

    if now is None:
        now = datetime.utcnow()

    # old default behavior
    if max_hours_idle is None and mins_to_end_of_hour is None:
        max_hours_idle = DEFAULT_MAX_HOURS_IDLE

    runner = EMRJobRunner(conf_paths=conf_paths, **kwargs)
    emr_conn = runner.make_emr_conn()

    log.info(
        'getting info about all job flows (this goes back about 2 months)')
    # We don't filter by job flow state because we want this to work even
    # if Amazon adds another kind of idle state.
    job_flows = describe_all_job_flows(emr_conn)

    num_bootstrapping = 0
    num_done = 0
    num_idle = 0
    num_non_streaming = 0
    num_pending = 0
    num_running = 0

    # a list of tuples of job flow id, name, idle time (as a timedelta)
    to_terminate = []

    for jf in job_flows:

        # check if job flow is done
        if is_job_flow_done(jf):
            num_done += 1

        # check if job flow is bootstrapping
        elif is_job_flow_bootstrapping(jf):
            num_bootstrapping += 1

        # we can't really tell if non-streaming jobs are idle or not, so
        # let them be (see Issue #60)
        elif not is_job_flow_streaming(jf):
            num_non_streaming += 1

        elif is_job_flow_running(jf):
            num_running += 1

        else:
            time_idle = now - time_last_active(jf)
            time_to_end_of_hour = est_time_to_hour(jf, now=now)
            _, pool = pool_hash_and_name(jf)
            pending = job_flow_has_pending_steps(jf)

            if pending:
                num_pending += 1
            else:
                num_idle += 1

            log.debug(
                'Job flow %s %s for %s, %s to end of hour, %s (%s)' %
                (jf.jobflowid,
                 'pending' if pending else 'idle',
                 strip_microseconds(time_idle),
                 strip_microseconds(time_to_end_of_hour),
                 ('unpooled' if pool is None else 'in %s pool' % pool),
                 jf.name))

            # filter out job flows that don't meet our criteria
            if (max_hours_idle is not None and
                    time_idle <= timedelta(hours=max_hours_idle)):

                continue

            # mins_to_end_of_hour doesn't apply to jobs with pending steps
            if (mins_to_end_of_hour is not None and
                (pending or
                 time_to_end_of_hour >= timedelta(
                    minutes=mins_to_end_of_hour))):
                continue

            if (pooled_only and pool is None):
                continue

            if (unpooled_only and pool is not None):
                continue

            if (pool_name is not None and pool != pool_name):
                continue

            to_terminate.append((jf, pending, time_idle, time_to_end_of_hour))

    log.info(
        'Job flow statuses: %d bootstrapping, %d running, %d pending, %d idle,'
        ' %d active non-streaming, %d done' % (
            num_running, num_bootstrapping, num_pending, num_idle,
            num_non_streaming, num_done))

    terminate_and_notify(runner, to_terminate, dry_run=dry_run,
                         max_mins_locked=max_mins_locked, quiet=quiet)


def is_job_flow_done(job_flow):
    """Return True if the given job flow is done running."""
    return hasattr(job_flow, 'enddatetime')


def is_job_flow_streaming(job_flow):
    """Return ``False`` if the give job flow has steps, but none of them are
    Hadoop streaming steps (for example, if the job flow is running Hive).
    """
    steps = getattr(job_flow, 'steps', None)

    if not steps:
        return True

    for step in steps:
        args = [a.value for a in step.args]
        for arg in args:
            # This is hadoop streaming
            if arg == '-mapper':
                return True
            # This is a debug jar associated with hadoop streaming
            if DEBUG_JAR_RE.match(arg):
                return True

    # job has at least one step, and none are streaming steps
    return False


def is_job_flow_running(job_flow):
    """Return ``True`` if *job_flow* has any steps which are currently
    running."""
    steps = getattr(job_flow, 'steps', None) or []
    return any(is_step_running(step) for step in steps)


def is_job_flow_bootstrapping(job_flow):
    """Return ``True`` if *job_flow* is currently bootstrapping."""
    return bool(getattr(job_flow, 'startdatetime', None) and
                not getattr(job_flow, 'readydatetime', None) and
                not getattr(job_flow, 'enddatetime', None))


def is_step_running(step):
    """Return true if the given job flow step is currently running."""
    return bool(getattr(step, 'state', None) != 'CANCELLED' and
                getattr(step, 'startdatetime', None) and
                not getattr(step, 'enddatetime', None))


def time_last_active(job_flow):
    """When did something last happen with the given job flow?

    Things we look at:

    * ``job_flow.creationdatetime`` (always set)
    * ``job_flow.startdatetime``
    * ``job_flow.readydatetime`` (i.e. when bootstrapping finished)
    * ``step.creationdatetime`` for any step
    * ``step.startdatetime`` for any step
    * ``step.enddatetime`` for any step

    This is not really meant to be run on job flows which are currently
    running, or done.
    """
    timestamps = []

    for key in 'creationdatetime', 'startdatetime', 'readydatetime':
        value = getattr(job_flow, key, None)
        if value:
            timestamps.append(value)

    steps = getattr(job_flow, 'steps', None) or []
    for step in steps:
        for key in 'creationdatetime', 'startdatetime', 'enddatetime':
            value = getattr(step, key, None)
            if value:
                timestamps.append(value)

    # for ISO8601 timestamps, alpha order == chronological order
    last_timestamp = max(timestamps)

    return datetime.strptime(last_timestamp, boto.utils.ISO8601)


def job_flow_has_pending_steps(job_flow):
    """Return ``True`` if *job_flow* has any steps in the ``PENDING``
    state."""
    steps = getattr(job_flow, 'steps', None) or []

    return any(getattr(step, 'state', None) == 'PENDING'
               for step in steps)


def terminate_and_notify(runner, to_terminate, dry_run=False,
                         max_mins_locked=None, quiet=False):
    if not to_terminate:
        return

    for jf, pending, time_idle, time_to_end_of_hour in to_terminate:
        fmt = ('Terminated job flow %s (%s); was %s for %s, %s to end of hour')
        msg = fmt % (
            jf.jobflowid, jf.name,
            'pending' if pending else 'idle',
            strip_microseconds(time_idle),
            strip_microseconds(time_to_end_of_hour))

        did_terminate = False
        if not dry_run:
            status = attempt_to_acquire_lock(
                runner.make_s3_conn(),
                runner._lock_uri(jf),
                runner._opts['s3_sync_wait_time'],
                '%s (%s)' % (msg,
                             runner._make_unique_job_name(label='terminate')),
                mins_to_expiration=max_mins_locked,
            )
            if status:
                runner.make_emr_conn().terminate_jobflow(jf.jobflowid)
                did_terminate = True
            elif not quiet:
                log.info('%s was locked between getting job flow info and'
                         ' trying to terminate it; skipping' % jf.jobflowid)

        if did_terminate and not quiet:
            print msg


def make_option_parser():
    usage = '%prog [options]'
    description = ('Terminate idle EMR job flows that meet the criteria'
                   ' passed in on the command line (or, by default,'
                   ' job flows that have been idle for one hour).')

    option_parser = OptionParser(usage=usage, description=description)

    option_parser.add_option(
        '--max-hours-idle', dest='max_hours_idle',
        default=None, type='float',
        help=('Max number of hours a job flow can go without bootstrapping,'
              ' running a step, or having a new step created. This will fire'
              ' even if there are pending steps which EMR has failed to'
              ' start. Make sure you set this higher than the amount of time'
              ' your jobs can take to start instances and bootstrap.'))
    option_parser.add_option(
        '--max-mins-locked', dest='max_mins_locked',
        default=DEFAULT_MAX_MINUTES_LOCKED, type='float',
        help='Max number of minutes a job flow can be locked while idle.')
    option_parser.add_option(
        '--mins-to-end-of-hour', dest='mins_to_end_of_hour',
        default=None, type='float',
        help=('Terminate job flows that are within this many minutes of'
              ' the end of a full hour since the job started running'
              ' AND have no pending steps.'))
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

    option_parser.add_option(
        '-t', '--test', dest='test', default=False,
        action='store_true',
        help="Don't actually delete any files; just log that we would")

    add_basic_opts(option_parser)

    return option_parser


if __name__ == '__main__':
    main()
