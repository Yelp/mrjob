# -*- coding: utf-8 -*-
# Copyright 2009-2012 Yelp
# Copyright 2013 Yelp and Contributors
# Copyright 2014 Brett Gibson
# Copyright 2015-2017 Yelp
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
"""Terminate idle EMR clusters that meet the criteria passed in on the command
line (or, by default, clusters that have been idle for one hour).

Suggested usage: run this as a cron job with the ``-q`` option::

    */30 * * * * mrjob terminate-idle-clusters -q

Options::

  -h, --help            show this help message and exit
  -c CONF_PATHS, --conf-path=CONF_PATHS
                        Path to alternate mrjob.conf file to read from
  --no-conf             Don't load mrjob.conf even if it's available
  --dry-run             Don't actually kill idle jobs; just log that we would
  --emr-endpoint=EMR_ENDPOINT
                        Force mrjob to connect to EMR on this endpoint (e.g.
                        us-west-1.elasticmapreduce.amazonaws.com). Default is
                        to infer this from region.
  --max-hours-idle=MAX_HOURS_IDLE
                        Max number of hours a cluster can go without
                        bootstrapping, running a step, or having a new step
                        created. This will fire even if there are pending
                        steps which EMR has failed to start. Make sure you set
                        this higher than the amount of time your jobs can take
                        to start instances and bootstrap.
  --max-mins-locked=MAX_MINS_LOCKED
                        Max number of minutes a cluster can be locked while
                        idle.
  --mins-to-end-of-hour=MINS_TO_END_OF_HOUR
                        Terminate clusters that are within this many minutes
                        of the end of a full hour since the job started
                        running AND have no pending steps.
  --pool-name=POOL_NAME
                        Only terminate clusters in the given named pool.
  --pooled-only         Only terminate pooled clusters
  -q, --quiet           Don't print anything to stderr
  --region=REGION       GCE/AWS region to run Dataproc/EMR jobs in.
  --aws-region=REGION   Deprecated alias for --region
  --s3-endpoint=S3_ENDPOINT
                        Force mrjob to connect to S3 on this endpoint (e.g. s3
                        -us-west-1.amazonaws.com). You usually shouldn't set
                        this; by default mrjob will choose the correct
                        endpoint for each S3 bucket based on its location.
  --unpooled-only       Only terminate un-pooled clusters
  -v, --verbose         print more messages to stderr
"""
from __future__ import print_function

from datetime import datetime
from datetime import timedelta
import logging
from optparse import OptionParser

from boto.exception import EmrResponseError

from mrjob.emr import _attempt_to_acquire_lock
from mrjob.emr import EMRJobRunner
from mrjob.emr import _list_all_steps
from mrjob.emr import _yield_all_bootstrap_actions
from mrjob.emr import _yield_all_clusters
from mrjob.job import MRJob
from mrjob.options import _add_basic_options
from mrjob.options import _add_runner_options
from mrjob.options import _alphabetize_options
from mrjob.options import _pick_runner_opts
from mrjob.parse import iso8601_to_datetime
from mrjob.pool import _est_time_to_hour
from mrjob.pool import _pool_hash_and_name
from mrjob.util import strip_microseconds

log = logging.getLogger(__name__)

_DEFAULT_MAX_HOURS_IDLE = 1
_DEFAULT_MAX_MINUTES_LOCKED = 1


def main(cl_args=None):
    option_parser = _make_option_parser()
    options, args = option_parser.parse_args(cl_args)

    if args:
        option_parser.error('takes no arguments')

    MRJob.set_up_logging(quiet=options.quiet,
                         verbose=options.verbose)

    _maybe_terminate_clusters(
        dry_run=options.dry_run,
        max_hours_idle=options.max_hours_idle,
        mins_to_end_of_hour=options.mins_to_end_of_hour,
        unpooled_only=options.unpooled_only,
        now=datetime.utcnow(),
        pool_name=options.pool_name,
        pooled_only=options.pooled_only,
        max_mins_locked=options.max_mins_locked,
        quiet=options.quiet,
        **_runner_kwargs(options)
    )


def _runner_kwargs(options):
    kwargs = options.__dict__.copy()
    for unused_arg in ('quiet', 'verbose', 'max_hours_idle',
                       'max_mins_locked',
                       'mins_to_end_of_hour', 'unpooled_only',
                       'pooled_only', 'pool_name', 'dry_run'):
        del kwargs[unused_arg]

    return kwargs


def _maybe_terminate_clusters(dry_run=False,
                              max_hours_idle=None,
                              mins_to_end_of_hour=None,
                              now=None,
                              pool_name=None,
                              pooled_only=False,
                              unpooled_only=False,
                              max_mins_locked=None,
                              quiet=False,
                              **kwargs):
    if now is None:
        now = datetime.utcnow()

    # old default behavior
    if max_hours_idle is None and mins_to_end_of_hour is None:
        max_hours_idle = _DEFAULT_MAX_HOURS_IDLE

    runner = EMRJobRunner(**kwargs)
    emr_conn = runner.make_emr_conn()

    num_starting = 0
    num_bootstrapping = 0
    num_done = 0
    num_idle = 0
    num_pending = 0
    num_running = 0

    # We don't filter by cluster state because we want this to work even
    # if Amazon adds another kind of idle state.
    for cluster_summary in _yield_all_clusters(emr_conn):
        cluster_id = cluster_summary.id

        # check if cluster is done
        if _is_cluster_done(cluster_summary):
            num_done += 1
            continue

        # check if cluster is starting
        if _is_cluster_starting(cluster_summary):
            num_starting += 1
            continue

        # check if cluster is bootstrapping
        if _is_cluster_bootstrapping(cluster_summary):
            num_bootstrapping += 1
            continue

        # need steps to learn more about cluster
        steps = _list_all_steps(emr_conn, cluster_id)

        if any(_is_step_running(step) for step in steps):
            num_running += 1
            continue

        # cluster is idle
        time_idle = now - _time_last_active(cluster_summary, steps)
        time_to_end_of_hour = _est_time_to_hour(cluster_summary, now=now)
        is_pending = _cluster_has_pending_steps(steps)

        bootstrap_actions = list(_yield_all_bootstrap_actions(
            emr_conn, cluster_id))
        _, pool = _pool_hash_and_name(bootstrap_actions)

        if is_pending:
            num_pending += 1
        else:
            num_idle += 1

        log.debug(
            'cluster %s %s for %s, %s to end of hour, %s (%s)' %
            (cluster_id,
             'pending' if is_pending else 'idle',
             strip_microseconds(time_idle),
             strip_microseconds(time_to_end_of_hour),
             ('unpooled' if pool is None else 'in %s pool' % pool),
             cluster_summary.name))

        # filter out clusters that don't meet our criteria
        if (max_hours_idle is not None and
                time_idle <= timedelta(hours=max_hours_idle)):
            continue

        # mins_to_end_of_hour doesn't apply to jobs with pending steps
        if (mins_to_end_of_hour is not None and
            (is_pending or
             time_to_end_of_hour >= timedelta(
                minutes=mins_to_end_of_hour))):
            continue

        if (pooled_only and pool is None):
            continue

        if (unpooled_only and pool is not None):
            continue

        if (pool_name is not None and pool != pool_name):
            continue

        # terminate idle cluster
        _terminate_and_notify(
            runner=runner,
            cluster_id=cluster_id,
            cluster_name=cluster_summary.name,
            num_steps=len(steps),
            is_pending=is_pending,
            time_idle=time_idle,
            time_to_end_of_hour=time_to_end_of_hour,
            dry_run=dry_run,
            max_mins_locked=max_mins_locked,
            quiet=quiet)

    log.info(
        'Cluster statuses: %d starting, %d bootstrapping, %d running,'
        ' %d pending, %d idle, %d done' % (
            num_starting, num_bootstrapping, num_running,
            num_pending, num_idle, num_done))


def _is_cluster_done(cluster):
    """Return True if the given cluster is done running."""
    return (cluster.status.state == 'TERMINATING' or
            hasattr(cluster.status.timeline, 'enddatetime'))


def _is_cluster_starting(cluster_summary):
    return cluster_summary.status.state == 'STARTING'


def _is_cluster_bootstrapping(cluster_summary):
    """Return ``True`` if *cluster_summary* is currently bootstrapping."""
    return (cluster_summary.status.state != 'STARTING' and
            not hasattr(cluster_summary.status.timeline, 'readydatetime'))


def _is_cluster_running(steps):
    return any(_is_step_running(step) for step in steps)


def _is_step_running(step):
    """Return true if the given step is currently running."""
    return (getattr(step.status, 'state', None) not in
            ('CANCELLED', 'INTERRUPTED') and
            hasattr(step.status.timeline, 'startdatetime') and
            not hasattr(step.status.timeline, 'enddatetime'))


def _cluster_has_pending_steps(steps):
    """Does *cluster* have any steps in the ``PENDING`` state?"""
    return any(step.status.state == 'PENDING' for step in steps)


def _time_last_active(cluster_summary, steps):
    """When did something last happen with the given cluster?

    Things we look at:

    * ``cluster.creationdatetime`` (always set)
    * ``cluster.readydatetime`` (i.e. when bootstrapping finished)
    * ``step.creationdatetime`` for any step
    * ``step.startdatetime`` for any step
    * ``step.enddatetime`` for any step

    This is not really meant to be run on clusters which are currently
    running, or done.
    """
    timestamps = []

    for key in 'creationdatetime', 'readydatetime':
        value = getattr(cluster_summary.status.timeline, key, None)
        if value:
            timestamps.append(value)

    for step in steps:
        for key in 'creationdatetime', 'startdatetime', 'enddatetime':
            value = getattr(step.status.timeline, key, None)
            if value:
                timestamps.append(value)

    # for ISO8601 timestamps, alpha order == chronological order
    last_timestamp = max(timestamps)

    return iso8601_to_datetime(last_timestamp)


def _terminate_and_notify(runner, cluster_id, cluster_name, num_steps,
                          is_pending, time_idle, time_to_end_of_hour,
                          dry_run=False, max_mins_locked=None, quiet=False):

    fmt = ('Terminated cluster %s (%s); was %s for %s, %s to end of hour')
    msg = fmt % (
        cluster_id, cluster_name,
        'pending' if is_pending else 'idle',
        strip_microseconds(time_idle),
        strip_microseconds(time_to_end_of_hour))

    did_terminate = False
    if dry_run:
        did_terminate = True
    else:
        status = _attempt_to_acquire_lock(
            runner.fs,
            runner._lock_uri(cluster_id, num_steps),
            runner._opts['cloud_fs_sync_secs'],
            '%s (%s)' % (msg,
                         runner._make_unique_job_key(label='terminate')),
            mins_to_expiration=max_mins_locked,
        )
        if status:
            try:
                runner.make_emr_conn().terminate_jobflow(cluster_id)
                did_terminate = True
            except EmrResponseError as e:
                if (e.code == 'ValidationError' and
                        'termination protected' in e.body):
                    if not quiet:
                        log.info('%s was termination protected, skipping' % (
                                 cluster_id,))
                else:
                    raise
        elif not quiet:
            log.info('%s was locked between getting cluster info and'
                     ' trying to terminate it; skipping' % cluster_id)

    if did_terminate and not quiet:
        print(msg)


def _make_option_parser():
    usage = '%prog [options]'
    description = ('Terminate idle EMR clusters that meet the criteria'
                   ' passed in on the command line (or, by default,'
                   ' clusters that have been idle for one hour).')

    option_parser = OptionParser(usage=usage, description=description)

    option_parser.add_option(
        '--max-hours-idle', dest='max_hours_idle',
        default=None, type='float',
        help=('Max number of hours a cluster can go without bootstrapping,'
              ' running a step, or having a new step created. This will fire'
              ' even if there are pending steps which EMR has failed to'
              ' start. Make sure you set this higher than the amount of time'
              ' your jobs can take to start instances and bootstrap.'))
    option_parser.add_option(
        '--max-mins-locked', dest='max_mins_locked',
        default=_DEFAULT_MAX_MINUTES_LOCKED, type='float',
        help='Max number of minutes a cluster can be locked while idle.')
    option_parser.add_option(
        '--mins-to-end-of-hour', dest='mins_to_end_of_hour',
        default=None, type='float',
        help=('Terminate clusters that are within this many minutes of'
              ' the end of a full hour since the job started running'
              ' AND have no pending steps.'))
    option_parser.add_option(
        '--unpooled-only', dest='unpooled_only', action='store_true',
        default=False,
        help='Only terminate un-pooled clusters')
    option_parser.add_option(
        '--pooled-only', dest='pooled_only', action='store_true',
        default=False,
        help='Only terminate pooled clusters')
    option_parser.add_option(
        '--pool-name', dest='pool_name', default=None,
        help='Only terminate clusters in the given named pool.')
    option_parser.add_option(
        '--dry-run', dest='dry_run', default=False,
        action='store_true',
        help="Don't actually kill idle jobs; just log that we would")

    _add_basic_options(option_parser)
    _add_runner_options(
        option_parser,
        _pick_runner_opts('emr', 'connect'))

    _alphabetize_options(option_parser)
    return option_parser


if __name__ == '__main__':
    main()
