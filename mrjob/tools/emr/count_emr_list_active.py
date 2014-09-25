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
"""Count the numbers of EMR active clusters and instances from current jobflows.

Usage::

    mrjob count-emr-list-active > report
    python -m mrjob.tools.emr.count_emr_list_active > report

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
from datetime import timedelta
from optparse import OptionParser

from mrjob.emr import EMRJobRunner
from mrjob.emr import describe_all_job_flows
from mrjob.job import MRJob
#from mrjob.options import add_basic_opts
#from mrjob.parse import JOB_NAME_RE
#from mrjob.parse import STEP_NAME_RE
#from mrjob.parse import iso8601_to_datetime
#from mrjob.util import strip_microseconds
#from mrjob.tools.emr.audit_usage import job_flow_to_basic_summary

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

    log.info('listing all active emr jobflows...')

    active_states = ['STARTING', 'BOOTSTRAPPING', 'WAITING', 'RUNNING']
#    active_states = ['WAITING']
    emr_conn = EMRJobRunner(conf_paths=options.conf_paths).make_emr_conn()
    job_flows = describe_all_job_flows(emr_conn, states=active_states)

    import subprocess
#    subprocess.call(['emr --list --state active \| grep \'j-\''], shell=True)
    subprocess.call(['./count.sh'], shell=True)

    log.info('compiling active emr jobflows stats...')

    job_flow_ids = [getattr(jf, 'jobflowid', None) for jf in job_flows]
    instance_counts = [int(getattr(jf, 'instancecount', None)) for jf in job_flows]
   
#    print job_flow_ids 
#    print instance_counts
    total_instance_count = sum(instance_counts)

    result =  'timestamp:' + repr(now) + 'emr_instances:' + repr(total_instance_count)

    print result


def make_option_parser():
    usage = '%prog [options]'
    description = 'Count EMR active jobflows now, identical to \'emr --list --active\'.'
    option_parser = OptionParser(usage=usage, description=description)
#    option_parser.add_option(
#        '--max-days-ago', dest='max_days_ago', type='float', default=None,
#        help=('Max number of days ago to look at jobs. By default, we go back'
#              ' as far as EMR supports (currently about 2 months)'))
    add_basic_opts(option_parser)
    return option_parser

if __name__ == '__main__':
    main(None)
