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
"""List, display, and parse Hadoop logs associated with EMR job flows. Useful
for debugging failed jobs for which mrjob did not display a useful error
message or for inspecting jobs whose output has been lost.

Usage::

    python -m mrjob.tools.emr.fetch_logs -[l|L|a|A|--counters] [-s STEP_NUM]\
 JOB_FLOW_ID

Options::

  -a, --cat             Cat log files MRJob finds relevant
  -A, --cat-all         Cat all log files to JOB_FLOW_ID/
  -c CONF_PATH, --conf-path=CONF_PATH
                        Path to alternate mrjob.conf file to read from
  --counters            Show counters from the job flow
  --ec2-key-pair-file=EC2_KEY_PAIR_FILE
                        Path to file containing SSH key for EMR
  -h, --help            show this help message and exit
  -l, --list            List log files MRJob finds relevant
  -L, --list-all        List all log files
  --no-conf             Don't load mrjob.conf even if it's available
  -q, --quiet           Don't print anything to stderr
  -s STEP_NUM, --step-num=STEP_NUM
                        Limit results to a single step. To be used with --list
                        and --cat.
  -v, --verbose         print more messages to stderr
"""
from __future__ import with_statement

from optparse import OptionParser
import sys

from mrjob.emr import EMRJobRunner
from mrjob.emr import LogFetchError
from mrjob.job import MRJob
from mrjob.logparsers import TASK_ATTEMPT_LOGS
from mrjob.logparsers import STEP_LOGS
from mrjob.logparsers import JOB_LOGS
from mrjob.logparsers import NODE_LOGS
from mrjob.util import scrape_options_into_new_groups
from mrjob.util import log_to_stream


def main():
    usage = 'usage: %prog [options] JOB_FLOW_ID'
    description = (
        'List, display, and parse Hadoop logs associated with EMR job flows.'
        ' Useful for debugging failed jobs for which mrjob did not display a'
        ' useful error message or for inspecting jobs whose output has been'
        ' lost.')

    option_parser = OptionParser(usage=usage, description=description)

    option_parser.add_option('-f', '--find-failure', dest='find_failure',
                             action='store_true', default=False,
                             help=('Search the logs for information about why'
                                   ' the job failed'))
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
                             help=('Limit results to a single step. To be used'
                                   ' with --list and --cat.'))
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
                         mr_job.hadoop_emr_opt_group, mr_job.emr_opt_group,
                         mr_job.hadoop_opts_opt_group)
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
                       'cat_relevant', 'cat_all', 'get_counters', 'step_num',
                      'find_failure'):
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
            desc = runner._describe_jobflow()
            runner._set_s3_job_log_uri(desc)
            runner._fetch_counters(
                xrange(1, len(desc.steps) + 1), skip_s3_wait=True)
            runner.print_counters()

        if options.find_failure:
            find_failure(runner, options.step_num)


def prettyprint_paths(paths):
    for path in paths:
        print path
    print


def _prettyprint_relevant(log_type_to_uri_list):
    print 'Task attempts:'
    prettyprint_paths(log_type_to_uri_list[TASK_ATTEMPT_LOGS])
    print 'Steps:'
    prettyprint_paths(log_type_to_uri_list[STEP_LOGS])
    print 'Jobs:'
    prettyprint_paths(log_type_to_uri_list[JOB_LOGS])
    print 'Nodes:'
    prettyprint_paths(log_type_to_uri_list[NODE_LOGS])


def list_relevant(runner, step_nums):
    try:
        logs = {
            TASK_ATTEMPT_LOGS: runner.ls_task_attempt_logs_ssh(step_nums),
            STEP_LOGS: runner.ls_step_logs_ssh(step_nums),
            JOB_LOGS: runner.ls_job_logs_ssh(step_nums),
            NODE_LOGS: runner.ls_node_logs_ssh(),
        }
        _prettyprint_relevant(logs)
    except LogFetchError, e:
        print 'SSH error:', e
        logs = {
            TASK_ATTEMPT_LOGS: runner.ls_task_attempt_logs_s3(step_nums),
            STEP_LOGS: runner.ls_step_logs_s3(step_nums),
            JOB_LOGS: runner.ls_job_logs_s3(step_nums),
            NODE_LOGS: runner.ls_node_logs_s3(),
        }
        _prettyprint_relevant(logs)


def list_all(runner):
    try:
        prettyprint_paths(runner.ls_all_logs_ssh())
    except LogFetchError, e:
        print 'SSH error:', e
        prettyprint_paths(runner.ls_all_logs_s3())


def cat_from_list(runner, path_list):
    for path in path_list:
        print '===', path, '==='
        for line in runner.cat(path):
            print line.rstrip()
        print


def _cat_from_relevant(runner, log_type_to_uri_list):
    print 'Task attempts:'
    cat_from_list(runner, log_type_to_uri_list[TASK_ATTEMPT_LOGS])
    print 'Steps:'
    cat_from_list(runner, log_type_to_uri_list[STEP_LOGS])
    print 'Jobs:'
    cat_from_list(runner, log_type_to_uri_list[JOB_LOGS])
    print 'Slaves:'
    cat_from_list(runner, log_type_to_uri_list[NODE_LOGS])


def cat_relevant(runner, step_nums):
    try:
        logs = {
            TASK_ATTEMPT_LOGS: runner.ls_task_attempt_logs_ssh(step_nums),
            STEP_LOGS: runner.ls_step_logs_ssh(step_nums),
            JOB_LOGS: runner.ls_job_logs_ssh(step_nums),
            NODE_LOGS: runner.ls_node_logs_ssh(),
        }
        _cat_from_relevant(runner, logs)
    except LogFetchError, e:
        print 'SSH error:', e
        logs = {
            TASK_ATTEMPT_LOGS: runner.ls_task_attempt_logs_s3(step_nums),
            STEP_LOGS: runner.ls_step_logs_s3(step_nums),
            JOB_LOGS: runner.ls_job_logs_s3(step_nums),
            NODE_LOGS: runner.ls_node_logs_s3(),
        }
        _cat_from_relevant(runner, logs)


def cat_all(runner):
    try:
        cat_from_list(runner, runner.ls_all_logs_ssh())
    except LogFetchError, e:
        print 'SSH error:', e
        cat_from_list(runner, runner.ls_all_logs_s3())


def find_failure(runner, step_num):
    if step_num:
        step_nums = [step_num]
    else:
        job_flow = runner._describe_jobflow()
        if job_flow:
            step_nums = range(1, len(job_flow.steps) + 1)
        else:
            print 'You do not have access to that job flow.'
            sys.exit(1)

    cause = runner._find_probable_cause_of_failure(step_nums)
    if cause:
        # log cause, and put it in exception
        cause_msg = []  # lines to log and put in exception
        cause_msg.append('Probable cause of failure (from %s):' %
                   cause['log_file_uri'])
        cause_msg.extend(line.strip('\n') for line in cause['lines'])
        if cause['input_uri']:
            cause_msg.append('(while reading from %s)' %
                             cause['input_uri'])

        print '\n'.join(cause_msg)
    else:
        print 'No probable cause of failure found.'

if __name__ == '__main__':
    main()
