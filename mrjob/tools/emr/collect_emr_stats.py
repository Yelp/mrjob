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
    usage = '%prog [options]'
    description = 'Collect EMR stats from active jobflows.'
    option_parser = OptionParser(usage=usage, description=description)
    add_basic_opts(option_parser)
    options, args = option_parser.parse_args(args)
    if args:
        option_parser.error('takes no arguments')

    now = datetime.utcnow()

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)
    log.info('collecting emr jobflow information...')

    job_flows = collect_active_job_flows(options.conf_paths)

    log.info('compiling stats from collected jobflows...')

    stats = job_flows_to_stats(job_flows, now=now)

    print stats


def collect_active_job_flows(conf_paths):
    """Collect active job flow information from EMR.

    :param str conf_path: Alternate path to read :py:mod:`mrjob.conf` from, or ``False`` to ignore all config files
    """
    emr_conn = EMRJobRunner(conf_paths=conf_paths).make_emr_conn()
    active_states = ['STARTING', 'BOOTSTRAPPING', 'WAITING', 'RUNNING']

    return describe_all_job_flows(emr_conn, states=active_states)


def job_flows_to_stats(job_flows, now=None):
    """ Compute total number of jobflows and instance count given a list of jobflows
    
    :param job_flows: A list of :py:class:`boto.emr.EmrObject`
    :param now: the current UTC time, as a :py:class:`datetime.datetime`. Default to current time.

    Returns a dictionary with many keys, including:

    * *timestamp*: The time these jobflows are collected (current UTC time)
    * *num_jobflows*: total number of jobflows
    * *total_instance_count*: total number of instance count from jobflows

    """
    job_flow_ids = [getattr(jf, 'jobflowid', None) for jf in job_flows]
    instance_counts = [int(getattr(jf, 'instancecount', None)) for jf in job_flows]
    total_instance_count = sum(instance_counts)

    if now is None:
        now = datetime.utcnow()

    stats = {}
    stats['timestamp'] = now.isoformat()
    stats['num_jobflows'] = len(job_flow_ids)
    stats['total_instance_count'] = total_instance_count

    return stats


if __name__ == '__main__':
    main(None)

