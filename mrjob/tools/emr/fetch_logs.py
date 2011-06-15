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
import os
import sys

from mrjob.emr import EMRJobRunner
from mrjob.job import MRJob
from mrjob.logfetch import LogFetchException
from mrjob.logfetch.s3 import S3LogFetcher
from mrjob.logfetch.ssh import SSHLogFetcher
from mrjob.util import scrape_options_into_new_groups


def main():
    usage = 'usage: %prog [options] JOB_FLOW_ID'
    description = 'Retrieve log files for EMR jobs.'
    option_parser = OptionParser(usage=usage,description=description)

    option_parser.add_option('-l', '--list', dest='list_relevant',
                             action="store_true", default=False,
                             help='List log files MRJob finds relevant')

    option_parser.add_option('-L', '--list-all', dest='list_all',
                             action="store_true", default=False,
                             help='List all log files')

    option_parser.add_option('-d', '--download', dest='download_relevant',
                             action="store_true", default=False,
                             help='Download log files MRJob finds relevant')

    option_parser.add_option('-D', '--download-all', dest='download_all',
                             action="store_true", default=False,
                             help='Download all log files to JOB_FLOW_ID/')
    
    assignments = {
        option_parser: ('conf_path', 'quiet', 'verbose',
                        'ec2_key_pair_file')
    }

    mr_job = MRJob()
    job_option_groups = (mr_job.option_parser, mr_job.mux_opt_group,
                     mr_job.proto_opt_group, mr_job.runner_opt_group,
                     mr_job.hadoop_emr_opt_group, mr_job.emr_opt_group)
    scrape_options_into_new_groups(job_option_groups, assignments)

    options, args = option_parser.parse_args()

    if options.list_relevant:
        list_relevant(args[0], **options.__dict__)

    if options.list_all:
        list_all(args[0], **options.__dict__)

    if options.download_relevant:
        download_relevant(args[0], **options.__dict__)

    if options.download_all:
        download_all(args[0], **options.__dict__)


def ssh_fetcher(jobflow_id, **runner_kwargs):
    dummy_runner = EMRJobRunner(**runner_kwargs)
    emr_conn = dummy_runner.make_emr_conn()
    ec2_key_pair_file = dummy_runner._opts['ec2_key_pair_file']
    return SSHLogFetcher(emr_conn, jobflow_id,
                         ec2_key_pair_file=ec2_key_pair_file)


def s3_fetcher(jobflow_id, **runner_kwargs):
    dummy_runner = EMRJobRunner(**runner_kwargs)
    emr_conn = dummy_runner.make_emr_conn()

    jobflow = emr_conn.describe_jobflow(jobflow_id)
    log_uri = getattr(jobflow, 'loguri', '')
    tweaked_log_uri = log_uri.replace('s3n://', 's3://')
    root_path = '%s%s/' % (tweaked_log_uri, jobflow_id)

    s3_conn = dummy_runner.make_s3_conn()
    return S3LogFetcher(s3_conn, root_path)


def with_fetcher(func):
    def wrap(jobflow_id, **runner_kwargs):
        try:
            fetcher = ssh_fetcher(jobflow_id, **runner_kwargs)
            func(fetcher, jobflow_id)
        except LogFetchException, e:
            print e
            fetcher = s3_fetcher(jobflow_id, **runner_kwargs)
            func(fetcher, jobflow_id)
    return wrap


def prettyprint_paths(paths):
    for path in paths:
        print path
    print


@with_fetcher
def list_relevant(fetcher, jobflow_id):
    task_attempts, steps, jobs = fetcher.list_logs()
    print 'Task attempts:'
    prettyprint_paths(task_attempts)
    print 'Steps:'
    prettyprint_paths(steps)
    print 'Jobs:'
    prettyprint_paths(jobs)


@with_fetcher
def list_all(fetcher, jobflow_id):
    prettyprint_paths(fetcher.ls())


@with_fetcher
def download_relevant(fetcher, jobflow_id):
    if not os.path.exists(jobflow_id):
        os.makedirs(jobflow_id)
    task_attempts, steps, jobs = fetcher.list_logs()
    folder_and_items = [
        ('task_attempts', task_attempts),
        ('steps', steps),
        ('jobs', jobs),
    ]
    for base_path, items in folder_and_items:
        base_path = os.path.join(jobflow_id, base_path)
        if not os.path.exists(base_path):
            os.makedirs(base_path)
        for item in items:

            head, tail = os.path.split(item)
            fetcher.get(item, os.path.join(base_path, tail))
            print os.path.join(base_path, tail)


@with_fetcher
def download_all(fetcher, jobflow_id):
    if not os.path.exists(jobflow_id):
        os.makedirs(jobflow_id)
    for item in fetcher.ls():
        local_path = os.path.join(jobflow_id, item[len(fetcher.root_path):])
        head, tail = os.path.split(local_path)
        if not os.path.exists(head):
            os.makedirs(head)
        fetcher.get(item, local_path)
        print local_path


if __name__ == '__main__':
    main()
