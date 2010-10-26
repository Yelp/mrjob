"""Find EMR job flows that have been idle for a long time (by default, one
hour) and terminate them.

Suggested usage: run this as a cron job with the -q option::

    */30 * * * * python -m mrjob.tools.emr.terminate_idle_emr_job_flows -q
"""
from boto.utils import ISO8601
from datetime import datetime, timedelta
import logging
from optparse import OptionParser

from mrjob.emr import EMRJobRunner
from mrjob.util import log_to_stream

log = logging.getLogger('mrjob.tools.emr.terminate_idle_job_flows')

DEFAULT_MAX_HOURS_IDLE = 1

def main():
    option_parser = make_option_parser()
    options, args = option_parser.parse_args()
    
    if args:
        option_parser.error('takes no arguments')

    # set up logging
    if not options.quiet:
        log_to_stream(name='mrjob', debug=options.verbose)

    emr_conn = EMRJobRunner().make_emr_conn()

    log.info(
        'getting info about all job flows (this goes back about 2 weeks)')
    job_flows = emr_conn.describe_jobflows()
        
    now = datetime.utcnow()

    num_running = 0
    num_idle = 0
    num_done = 0
    # a list of tuples of job flow id, name, idle time (as a timedelta)
    to_terminate = []

    for jf in job_flows:
        # check if job flow is done
        if hasattr(jf, 'enddatetime'):
            num_done += 1
        # check if job flow is currently running
        elif jf.steps and not hasattr(jf.steps[-1], 'enddatetime'):
            num_running += 1
        # job flow is idle. how long?
        else:
            num_idle += 1
            if jf.steps:
                idle_since = datetime.strptime(
                    jf.steps[-1].enddatetime, ISO8601)
            else:
                idle_since = datetime.strptime(
                    jf.creationdatetime, ISO8601)
            idle_time = now - idle_since

            # don't care about fractions of a second
            idle_time = timedelta(idle_time.days, idle_time.seconds)

            log.debug('Job flow %s (%s) idle for %s' %
                           (jf.jobflowid, jf.name, idle_time))
            if idle_time > timedelta(hours=options.max_hours_idle):
                to_terminate.append(
                    (jf.jobflowid, jf.name, idle_time))

    log.info('Job flow statuses: %d running, %d idle, %d done' %
                  (num_running, num_idle, num_done))

    terminate_and_notify(emr_conn, to_terminate, options)

def terminate_and_notify(emr_conn, to_terminate, options):
    if not to_terminate:
        return

    for job_flow_id, name, idle_time in to_terminate:
        if not options.dry_run:
            emr_conn.terminate_jobflow(job_flow_id)
        print('Terminated job flow %s (%s); was idle for %s' % (
            (job_flow_id, name, idle_time)))

def make_option_parser():
    usage = '%prog [options]'
    description = 'Terminate all EMR job flows that have been idle for a long time (by default, one hour).'
    option_parser = OptionParser(usage=usage, description=description)
    option_parser.add_option(
        '-v', '--verbose', dest='verbose', default=False,
        action='store_true',
        help='Print more messages')
    option_parser.add_option(
        '-q', '--quiet', dest='quiet', default=False,
        action='store_true',
        help="Don't print anything to stderr; just print IDs of terminated job flows and idle time information to stdout")
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

