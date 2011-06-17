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

from mrjob.emr import EMRJobRunner, LogFetchException
from mrjob.job import MRJob
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

    option_parser.add_option('-a', '--cat', dest='cat_relevant',
                             action="store_true", default=False,
                             help='Cat log files MRJob finds relevant')

    option_parser.add_option('-A', '--cat-all', dest='cat_all',
                             action="store_true", default=False,
                             help='Cat all log files to JOB_FLOW_ID/')

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

    if options.cat_relevant:
        cat_relevant(args[0], **options.__dict__)

    if options.cat_all:
        cat_all(args[0], **options.__dict__)


def with_runner(func):
    def wrap(jobflow_id, **runner_kwargs):
        runner = EMRJobRunner(emr_job_flow_id=jobflow_id,
                              **runner_kwargs)
        func(runner)
    return wrap


def prettyprint_paths(paths):
    for path in paths:
        print path
    print


def _prettyprint_relevant(task_attempts, steps, jobs):
    print 'Task attempts:'
    prettyprint_paths(task_attempts)
    print 'Steps:'
    prettyprint_paths(steps)
    print 'Jobs:'
    prettyprint_paths(jobs)


@with_runner
def list_relevant(runner):
    try:
        _prettyprint_relevant(*runner.ssh_list_logs())
    except LogFetchException, e:
        print e
        _prettyprint_relevant(*runner.s3_list_logs())


@with_runner
def list_all(runner):
    try:
        prettyprint_paths(runner.ssh_list_all())
    except LogFetchException, e:
        print e
        prettyprint_paths(runner.s3_list_all())


def cat_from_list(runner, path_list):
    for path in path_list:
        print '===', path, '==='
        print runner.cat(path)


def cat_from_relevant(runner, task_attempts, steps, jobs):
    print 'Task attempts:'
    cat_from_list(runner, task_attempts)
    print 'Steps:'
    cat_from_list(runner, steps)
    print 'Jobs:'
    cat_from_list(runner, jobs)


@with_runner
def cat_relevant(runner):
    try:
        cat_from_relevant(runner, *runner.ssh_list_logs())
    except LogFetchException, e:
        print e
        cat_from_relevant(runner, *runner.s3_list_logs())


@with_runner
def cat_all(runner):
    try:
        cat_from_list(runner, runner.ssh_list_all())
    except LogFetchException, e:
        print e
        cat_from_list(runner, runner.s3_list_all())

if __name__ == '__main__':
    main()
