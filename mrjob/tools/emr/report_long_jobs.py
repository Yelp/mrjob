# Copyright 2012 Yelp
# Copyright 2013 David Marin and Steve Johnson
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
# limitations under the License.
"""Report jobs running for more than a certain number of hours (by default,
24.0). This can help catch buggy jobs and Hadoop/EMR operational issues.

Suggested usage: run this as a daily cron job with the ``-q`` option::

    0 0 * * * mrjob report-long-jobs

Options::

  -h, --help            show this help message and exit
  -c CONF_PATHS, --conf-path=CONF_PATHS
                        Path to alternate mrjob.conf file to read from
  --no-conf             Don't load mrjob.conf even if it's available
  --emr-endpoint=EMR_ENDPOINT
                        Force mrjob to connect to EMR on this endpoint (e.g.
                        us-west-1.elasticmapreduce.amazonaws.com). Default is
                        to infer this from region.
  --min-hours=MIN_HOURS
                        Minimum number of hours a job can run before we report
                        it. Default: 24.0
  -q, --quiet           Don't print anything to stderr
  --region=REGION       GCE/AWS region to run Dataproc/EMR jobs in.
  --aws-region=REGION   Deprecated alias for --region
  --s3-endpoint=S3_ENDPOINT
                        Force mrjob to connect to S3 on this endpoint (e.g. s3
                        -us-west-1.amazonaws.com). You usually shouldn't set
                        this; by default mrjob will choose the correct
                        endpoint for each S3 bucket based on its location.
  -x, --exclude=TAG_KEY,TAG_VALUE
                        Exclude clusters that have the specified tag key/value
                        pair
  -v, --verbose         print more messages to stderr
"""
from __future__ import print_function

from datetime import datetime
from datetime import timedelta
import logging
from optparse import OptionParser

from mrjob.emr import EMRJobRunner
from mrjob.emr import _list_all_steps
from mrjob.emr import _yield_all_clusters
from mrjob.job import MRJob
from mrjob.options import _add_basic_options
from mrjob.options import _add_runner_options
from mrjob.options import _alphabetize_options
from mrjob.options import _pick_runner_opts
from mrjob.parse import iso8601_to_datetime
from mrjob.util import strip_microseconds

# default minimum number of hours a job can run before we report it.
DEFAULT_MIN_HOURS = 24.0

log = logging.getLogger(__name__)


def main(args=None):
    now = datetime.utcnow()

    option_parser = _make_option_parser()
    options, args = option_parser.parse_args(args)

    if args:
        option_parser.error('takes no arguments')

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)

    log.info('getting information about running jobs')
    emr_conn = EMRJobRunner(**_runner_kwargs(options)).make_emr_conn()
    cluster_summaries = _yield_all_clusters(
        emr_conn, cluster_states=['STARTING', 'BOOTSTRAPPING', 'RUNNING'])

    min_time = timedelta(hours=options.min_hours)

    if not options.exclude:
        filtered_cluster_summaries = cluster_summaries
    else:
        filtered_cluster_summaries = _filter_clusters(
            cluster_summaries, emr_conn, options.exclude)

    job_info = _find_long_running_jobs(
        emr_conn, filtered_cluster_summaries, min_time, now=now)

    _print_report(job_info)


def _runner_kwargs(options):
    """Given the command line options, return the arguments to
    :py:class:`EMRJobRunner`
    """
    kwargs = options.__dict__.copy()
    for unused_arg in ('quiet', 'verbose', 'min_hours', 'exclude'):
        del kwargs[unused_arg]

    return kwargs


def _filter_clusters(cluster_summaries, emr_conn, exclude_strings):
    """Filter out clusters that have tags matching any specified in
    exclude_strings.
    :param cluster_summaries: a list of :py:mod:`boto3` cluster summary data
                              structures
    :param exclude_strings: A list of strings of the form TAG_KEY,TAG_VALUE
    """
    exclude_as_dicts = []
    for exclude_string in exclude_strings:
        exclude_key, exclude_value = exclude_string.split(',')
        exclude_as_dicts.append({'Key': exclude_key, 'Value': exclude_value})

    for cs in cluster_summaries:
        cluster_id = cs.id
        # import pdb; pdb.set_trace()
        cluster_tags = emr_conn.describe_cluster(cluster_id).tags
        for cluster_tag in cluster_tags:
            if ({'Key': cluster_tag.key, 'Value': cluster_tag.value}
                    in exclude_as_dicts):
                break
        else:
            yield cs


def _find_long_running_jobs(emr_conn, cluster_summaries, min_time, now=None):
    """Identify jobs that have been running or pending for a long time.

    :param clusters: a list of :py:class:`boto.emr.emrobject.Cluster`
                      objects to inspect.
    :param min_time: a :py:class:`datetime.timedelta`: report jobs running or
                     pending longer than this
    :param now: the current UTC time, as a :py:class:`datetime.datetime`.
                Defaults to the current time.

    For each job that is running or pending longer than *min_time*, yields
    a dictionary with the following keys:

    * *cluster_id*: the cluster's unique ID (e.g. ``j-SOMECLUSTER``)
    * *name*: name of the step, or the cluster when bootstrapping
    * *state*: state of the step (``'RUNNING'`` or ``'PENDING'``) or, if there
               is no step, the cluster (``'STARTING'`` or ``'BOOTSTRAPPING'``)
    * *time*: amount of time step was running or pending, as a
              :py:class:`datetime.timedelta`
    """
    if now is None:
        now = datetime.utcnow()

    for cs in cluster_summaries:

        # special case for jobs that are taking a long time to bootstrap
        if cs.status.state in ('STARTING', 'BOOTSTRAPPING'):
            # there isn't a way to tell when the cluster stopped being
            # provisioned and started bootstrapping, so just measure
            # from cluster creation time
            created_timestamp = cs.status.timeline.creationdatetime
            created = iso8601_to_datetime(created_timestamp)

            time_running = now - created

            if time_running >= min_time:
                yield({'cluster_id': cs.id,
                       'name': cs.name,
                       'state': cs.status.state,
                       'time': time_running})

        # the default case: running clusters
        if cs.status.state != 'RUNNING':
            continue

        steps = _list_all_steps(emr_conn, cs.id)

        running_steps = [
            step for step in steps if step.status.state == 'RUNNING']
        pending_steps = [
            step for step in steps if step.status.state == 'PENDING']

        if running_steps:
            # should be only one, but if not, we should know about it
            for step in running_steps:

                start_timestamp = step.status.timeline.startdatetime
                start = iso8601_to_datetime(start_timestamp)

                time_running = now - start

                if time_running >= min_time:
                    yield({'cluster_id': cs.id,
                           'name': step.name,
                           'state': step.status.state,
                           'time': time_running})

        # sometimes EMR says it's "RUNNING" but doesn't actually run steps!
        elif pending_steps:
            step = pending_steps[0]

            # PENDING job should have run starting when the cluster
            # became ready, or the previous step completed
            start_timestamp = cs.status.timeline.readydatetime
            for step in steps:
                if step.status.state == 'COMPLETED':
                    start_timestamp = step.status.timeline.enddatetime

            start = iso8601_to_datetime(start_timestamp)
            time_pending = now - start

            if time_pending >= min_time:
                yield({'cluster_id': cs.id,
                       'name': step.name,
                       'state': step.status.state,
                       'time': time_pending})


def _print_report(job_info):
    """Takes in a dictionary of info about a long-running job (see
    :py:func:`_find_long_running_jobs`), and prints information about it
    on a single (long) line.
    """
    for ji in job_info:
        print('%-15s %13s for %17s (%s)' % (
            ji['cluster_id'],
            ji['state'], _format_timedelta(ji['time']),
            ji['name']))


def _format_timedelta(time):
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


def _make_option_parser():
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

    option_parser.add_option(
        '-x', '--exclude', action='append',
        help=('Exclude clusters that match the specified tags.'
              ' Specifed in the form TAG_KEY,TAG_VALUE.')
    )

    _add_basic_options(option_parser)
    _add_runner_options(
        option_parser,
        _pick_runner_opts('emr', 'connect')
    )

    _alphabetize_options(option_parser)

    return option_parser


if __name__ == '__main__':
    main()
