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
from __future__ import with_statement

import functools
from optparse import OptionParser, OptionValueError
import sys

from mrjob.logfetch import LogFetchException
from mrjob.logfetch.s3 import S3LogFetcher
from mrjob.logfetch.ssh import SSHLogFetcher
from mrjob.emr import EMRJobRunner


def main():
    usage = 'usage: %prog [options] JOB_FLOW_ID'
    description = 'Retrieve log files for EMR jobs.'
    option_parser = OptionParser(usage=usage,description=description)

    option_parser.add_option('-l', '--list',
                             action="callback", callback=list_relevant,
                             help='List log files MRJob finds relevant')

    option_parser.add_option('-a', '--list-all',
                             action="callback", callback=list_all,
                             help='List log files MRJob finds relevant')

    options, args = option_parser.parse_args()


def ssh_fetcher(jobflow_id):
    dummy_runner = EMRJobRunner()
    emr_conn = dummy_runner.make_emr_conn()
    ec2_key_pair_file = dummy_runner._opts['ec2_key_pair_file']
    return SSHLogFetcher(emr_conn, jobflow_id,
                         ec2_key_pair_file=ec2_key_pair_file)


def s3_fetcher(jobflow_id):
    dummy_runner = EMRJobRunner()
    emr_conn = dummy_runner.make_emr_conn()

    jobflow = emr_conn.describe_jobflow(jobflow_id)
    log_uri = getattr(jobflow, 'loguri', '')
    tweaked_log_uri = log_uri.replace('s3n://', 's3://')
    root_path = '%s%s/' % (tweaked_log_uri, jobflow_id)

    s3_conn = dummy_runner.make_s3_conn()
    return S3LogFetcher(s3_conn, root_path)


def with_fetcher(func):
    @functools.wraps(func)
    def wrap(option, opt_str, value, parser, *args, **kwargs):
        try:
            fetcher = ssh_fetcher(parser.rargs[-1])
            func(fetcher)
        except LogFetchException:
            fetcher = s3_fetcher(parser.rargs[-1])
            func(fetcher)
    return wrap


def prettyprint_paths(paths):
    for path in paths:
        print path
    print


@with_fetcher
def list_relevant(fetcher):
    task_attempts, steps, jobs = fetcher.list_logs()
    print 'Task attempts:'
    prettyprint_paths(task_attempts)
    print 'Steps:'
    prettyprint_paths(steps)
    print 'Jobs:'
    prettyprint_paths(jobs)


@with_fetcher
def list_all(fetcher):
    prettyprint_paths(fetcher.ls())


if __name__ == '__main__':
    main()
