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
# limitations under the License
"""Find EMR job flows that have been idle for a long time (by default, one
hour) and terminate them.

Suggested usage: run this as a cron job with the -q option::

    */30 * * * * python -m mrjob.tools.emr.terminate_idle_emr_job_flows -q

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
from mrjob.util import log_to_stream

log = logging.getLogger('mrjob.tools.emr.terminate_idle_job_flows')

DEFAULT_MAX_HOURS_IDLE = 1

DEBUG_JAR_RE = re.compile(
    r's3n://.*\.elasticmapreduce/libs/state-pusher/[^/]+/fetch')


def main():
    option_parser = make_option_parser()
    options, args = option_parser.parse_args()

    if args:
        option_parser.error('takes no arguments')

    # set up logging
    if not options.quiet:
        log_to_stream(name='mrjob', debug=options.verbose)
    # suppress No handlers could be found for logger "boto" message
    log_to_stream(name='boto', level=logging.CRITICAL)

    inspect_and_maybe_terminate_job_flows(
        conf_path=options.conf_path,
        max_hours_idle=options.max_hours_idle,
        now=datetime.utcnow(),
        dry_run=options.dry_run)


def inspect_and_maybe_terminate_job_flows(
    conf_path, max_hours_idle, now, dry_run):

    emr_conn = EMRJobRunner(conf_path=conf_path).make_emr_conn()

    log.info(
        'getting info about all job flows (this goes back about 2 months)')
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

            # don't care about fractions of a second
            time_idle = timedelta(time_idle.days, time_idle.seconds)

            log.debug('Job flow %s (%s) idle for %s' %
                      (jf.jobflowid, jf.name, time_idle))
            if time_idle > timedelta(hours=max_hours_idle):
                to_terminate.append(
                    (jf.jobflowid, jf.name, time_idle))

    log.info(
        'Job flow statuses: %d running, %d idle, %d active non-streaming,'
        ' %d done' % (num_running, num_idle, num_non_streaming, num_done))

    terminate_and_notify(emr_conn, to_terminate, dry_run=dry_run)


def is_job_flow_done(job_flow):
    """Return True if the given job flow is done running."""
    return hasattr(job_flow, 'enddatetime')


def is_job_flow_non_streaming(job_flow):
    """Return True if the give job flow has steps, but not of them are
    Hadoop streaming steps (for example, if the job flow is running Hive).
    """
    if not job_flow.steps:
        return False

    for step in job_flow.steps:
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
    active_steps = [step for step in job_flow.steps
                    if step.state != 'CANCELLED']

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

    active_steps = [step for step in job_flow.steps
                    if step.state != 'CANCELLED']

    if active_steps:
        if not getattr(active_steps[-1], 'enddatetime', None):
            return timedelta(0)
        else:
            return now - datetime.strptime(
                active_steps[-1].enddatetime, boto.utils.ISO8601)
    else:
        return now - datetime.strptime(job_flow.creationdatetime,
                                       boto.utils.ISO8601)


def terminate_and_notify(emr_conn, to_terminate, dry_run=False):
    if not to_terminate:
        return

    for job_flow_id, name, time_idle in to_terminate:
        if not dry_run:
            emr_conn.terminate_jobflow(job_flow_id)
        print 'Terminated job flow %s (%s); was idle for %s' % (
            (job_flow_id, name, time_idle))


def make_option_parser():
    usage = '%prog [options]'
    description = ('Terminate all EMR job flows that have been idle for a long'
                   ' time (by default, one hour).')
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
        default=DEFAULT_MAX_HOURS_IDLE, type='float',
        help='Max number of hours a job can run before being terminated')
    option_parser.add_option(
        '--dry-run', dest='dry_run', default=False,
        action='store_true',
        help="Don't actually kill idle jobs; just log that we would")

    return option_parser


if __name__ == '__main__':
    main()
