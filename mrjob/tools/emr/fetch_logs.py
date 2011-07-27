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
from mrjob.util import scrape_options_into_new_groups, log_to_stream


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

    option_parser.add_option('-s', '--step-num', dest='step_num',
                             action='store', type='int', default=None,
                             help='Limit results to a single step. To be used with --list and --cat.')
    option_parser.add_option('--counters', dest='get_counters',
                             action='store_true', default=False,
                             help='Show counters from the job flow')

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

    if not options.quiet:
        log_to_stream(name='mrjob', debug=options.verbose)

    if options.step_num:
        step_nums = [options.step_num]
    else:
        step_nums = None

    runner_kwargs = options.__dict__.copy()
    for unused_arg in ('quiet', 'verbose', 'list_relevant', 'list_all',
                       'cat_relevant', 'cat_all', 'get_counters', 'step_num'):
        del runner_kwargs[unused_arg]

    with EMRJobRunner(emr_job_flow_id=args[0], **runner_kwargs) as runner:
        if options.list_relevant:
            list_relevant(runner, step_nums)

        if options.list_all:
            list_all(runner)

        if options.cat_relevant:
            cat_relevant(runner, step_nums)

        if options.cat_all:
            cat_all(runner)

        if options.get_counters:
            runner._set_s3_job_log_uri(runner._describe_jobflow())
            runner._fetch_counters(range(100), skip_s3_wait=True)
            runner.print_counters()


def prettyprint_paths(paths):
    for path in paths:
        print path
    print


def _prettyprint_relevant(task_attempts, steps, jobs, nodes):
    print 'Task attempts:'
    prettyprint_paths(task_attempts)
    print 'Steps:'
    prettyprint_paths(steps)
    print 'Jobs:'
    prettyprint_paths(jobs)
    print 'Nodes:'
    prettyprint_paths(nodes)


def list_relevant(runner, step_nums):
    try:
        _prettyprint_relevant(*runner.ssh_list_logs(step_nums=step_nums))
    except LogFetchException, e:
        print 'SSH error:', e
        _prettyprint_relevant(*runner.s3_list_logs(step_nums=step_nums))


def list_all(runner):
    try:
        prettyprint_paths(runner.ssh_list_all())
    except LogFetchException, e:
        print 'SSH error:', e
        prettyprint_paths(runner.s3_list_all())


def cat_from_list(runner, path_list):
    for path in path_list:
        print '===', path, '==='
        for line in runner.cat(path):
            print line
        print


def _cat_from_relevant(runner, task_attempts, steps, jobs, nodes):
    print 'Task attempts:'
    cat_from_list(runner, task_attempts)
    print 'Steps:'
    cat_from_list(runner, steps)
    print 'Jobs:'
    cat_from_list(runner, jobs)
    print 'Slaves:'
    cat_from_list(runner, nodes)


def cat_relevant(runner, step_nums):
    try:
        _cat_from_relevant(runner, *runner.ssh_list_logs(step_nums=step_nums))
    except LogFetchException, e:
        print 'SSH error:', e
        _cat_from_relevant(runner, *runner.s3_list_logs(step_nums=step_nums))


def cat_all(runner):
    try:
        cat_from_list(runner, runner.ssh_list_all())
    except LogFetchException, e:
        print 'SSH error:', e
        cat_from_list(runner, runner.s3_list_all())

if __name__ == '__main__':
    main()
