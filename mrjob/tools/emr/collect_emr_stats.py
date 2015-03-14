# Copyright 2009-2010 Yelp
# Copyright 2014 Yelp
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
""" Collect EMR stats from active jobflows.
    Active jobflows are those in states of:
        BOOTSTRAPPING, RUNNING, STARTING, and WAITING.
    Collected stats include total number of active jobflows and total
    number of Amazon EC2 instances used to execute these jobflows.
    The instance counts are not separated by instance type.

Usage::

    mrjob collect-emr-stats > report
    python -m mrjob.tools.emr.collect_emr_stats > report

Options::

  -h, --help            Show this help message and exit
  -v, --verbose         Print more messages to stderr
  -q, --quiet           Don't log status messages; just print the report.
  -c CONF_PATH, --conf-path=CONF_PATH
                        Path to alternate mrjob.conf file to read from
  -p, --pretty-print    Pretty print the collected stats.
  --no-conf             Don't load mrjob.conf even if it's available
"""

from datetime import datetime
from logging import getLogger
from optparse import OptionParser
from time import mktime

try:
    import simplejson as json
except ImportError:
    import json

from mrjob.emr import EMRJobRunner
from mrjob.emr import describe_all_job_flows
from mrjob.job import MRJob
from mrjob.options import add_basic_opts

log = getLogger(__name__)


def main(args):
    # parser command-line args
    usage = '%prog [options]'
    description = "Collect EMR stats from active jobflows. "
    description += "Active jobflows are those in states of: "
    description += "BOOTSTRAPPING, RUNNING, STARTING, and WAITING. "
    description += "Collected stats include total number of active jobflows"
    description += "and total number of Amazon EC2 instances used to execute"
    description += "these jobflows. The instance counts are not separated by"
    description += "instance type."
    option_parser = OptionParser(usage=usage, description=description)
    option_parser.add_option(
        "-p", "--pretty-print",
        action="store_true", dest="pretty_print", default=False,
        help=('Pretty print the collected stats'))
    add_basic_opts(option_parser)

    options, args = option_parser.parse_args(args)
    if args:
        option_parser.error('takes no arguments')

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)
    log.info('collecting EMR active jobflows...')
    job_flows = collect_active_job_flows(options.conf_paths)
    log.info('compiling stats from collected jobflows...')
    stats = job_flows_to_stats(job_flows)

    if options.pretty_print:
        pretty_print(stats)
    else:
        print json.dumps(stats)


def pretty_print(stats):
    """Pretty print stats report.

    :param stats: A dictionary returned by :py:func:`job_flows_to_stats`
    """
    s = stats
    print '                Timestamp: %s' % s['timestamp']
    print 'Number of active jobflows: %s' % s['num_jobflows']
    print 'Number of instance counts: %s' % s['total_instance_count']
    print '* The active jobflows are those in states of BOOTSTRAPPING,'
    print '  STARTING, RUNNING, and WAITING.'


def collect_active_job_flows(conf_paths):
    """Collect active job flow information from EMR.

    :param str conf_path: Alternate path to read :py:mod:`mrjob.conf` from,
                          or ``False`` to ignore all config files

    Return a list of job flows
    """
    emr_conn = EMRJobRunner(conf_paths=conf_paths).make_emr_conn()
    active_states = ['STARTING', 'BOOTSTRAPPING', 'WAITING', 'RUNNING']

    return describe_all_job_flows(emr_conn, states=active_states)


def job_flows_to_stats(job_flows):
    """ Compute total number of jobflows and instance count given a list of
        jobflows.

    :param job_flows: A list of :py:class:`boto.emr.EmrObject`

    Returns a dictionary with many keys, including:

    * *timestamp*: The time when stats are collected (current UTC time in
                   POSIX timestamp, float format).
    * *num_jobflows*: Total number of jobflows.
    * *total_instance_count*: Total number of instance counts from jobflows.

    """
    job_flow_ids = [jf.jobflowid for jf in job_flows]
    instance_counts = [int(jf.instancecount) for jf in job_flows]
    total_instance_count = sum(instance_counts)

    now = datetime.utcnow()

    stats = {}
    stats['timestamp'] = mktime(now.timetuple())  # convert to POSIX timestamp
    stats['num_jobflows'] = len(job_flow_ids)
    stats['total_instance_count'] = total_instance_count

    return stats


if __name__ == '__main__':
    main(None)
