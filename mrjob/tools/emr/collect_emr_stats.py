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
"""Collect EMR stats from active jobflows.

Usage::

    mrjob collect-emr-stats > report
    python -m mrjob.tools.emr.collect_emr_status > report

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
from optparse import OptionParser

from mrjob.emr import EMRJobRunner
from mrjob.emr import describe_all_job_flows
from mrjob.job import MRJob
from mrjob.options import add_basic_opts

import logging

log = logging.getLogger(__name__)

def main(args):
    # parser command-line args
    option_parser = make_option_parser()
    options, args = option_parser.parse_args(args)

    if args:
        option_parser.error('takes no arguments')

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)

    now = datetime.utcnow()

    log.info('collecting emr jobflow information...')

    job_flows = collect_active_job_flows(options.conf_paths)

    log.info('compiling stats from emr jobflows...')

    stats = job_flows_to_stats(job_flows, now=now)

    print stats


def make_option_parser():
    usage = '%prog [options]'
    description = 'Collect EMR stats from active jobflows.'
    option_parser = OptionParser(usage=usage, description=description)
#    option_parser.add_option(
#        '--max-days-ago', dest='max_days_ago', type='float', default=None,
#        help=('Max number of days ago to look at jobs. By default, we go back'
#              ' as far as EMR supports (currently about 2 months)'))
    add_basic_opts(option_parser)
    return option_parser

def collect_active_job_flows(conf_paths):
    """Collect active job flow information from EMR.

    :param str conf_path: Alternate path to read :py:mod:`mrjob.conf` from, or ``False`` to ignore all config files
    """
    emr_conn = EMRJobRunner(conf_paths=conf_paths).make_emr_conn()

    active_states = ['STARTING', 'BOOTSTRAPPING', 'WAITING', 'RUNNING']  
    return describe_all_job_flows(emr_conn, states=active_states)


def job_flows_to_stats(job_flows, now=None):

    job_flow_ids = [getattr(jf, 'jobflowid', None) for jf in job_flows]
    instance_counts = [int(getattr(jf, 'instancecount', None)) for jf in job_flows]
    total_instance_count = sum(instance_counts)

    if now is None:
        now = datetime.utcnow()

    stats = {}
    stats['timestamp'] = now
    stats['total_num_jobflow'] = len(job_flow_ids)
    stats['total_instance_count'] = total_instance_count

    return stats


if __name__ == '__main__':
    main(None)
