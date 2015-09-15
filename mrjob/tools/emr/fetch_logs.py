# Copyright 2009-2012 Yelp
# Copyright 2013 David Marin and Steve Johnson
# Copyright 2015 Yelp
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

    mrjob fetch-logs -[l|L|a|A|--counters] [-s STEP_NUM]\
 JOB_FLOW_ID
    python -m mrjob.tools.emr.fetch_logs -[l|L|a|A|--counters] [-s STEP_NUM]\
 JOB_FLOW_ID

Options::

  -h, --help            show this help message and exit
  --aws-region=AWS_REGION
                        Region to connect to S3 and EMR on (e.g. us-west-1).
  -A, --cat-all         Cat all log files to JOB_FLOW_ID/
  -a, --cat             Cat log files MRJob finds relevant
  -c CONF_PATHS, --conf-path=CONF_PATHS
                        Path to alternate mrjob.conf file to read from
  --no-conf             Don't load mrjob.conf even if it's available
  --ec2-key-pair-file=EC2_KEY_PAIR_FILE
                        Path to file containing SSH key for EMR
  --emr-endpoint=EMR_ENDPOINT
                        Optional host to connect to when communicating with S3
                        (e.g. us-west-1.elasticmapreduce.amazonaws.com).
                        Default is to infer this from aws_region.
  -f, --find-failure    Search the logs for information about why the job
                        failed
  --counters            Show counters from the job flow
  -L, --list-all        List all log files
  -l, --list            List log files MRJob finds relevant
  -q, --quiet           Don't print anything to stderr
  --s3-endpoint=S3_ENDPOINT
                        Host to connect to when communicating with S3 (e.g. s3
                        -us-west-1.amazonaws.com). Default is to infer this
                        from region (see --aws-region).
  --s3-sync-wait-time=S3_SYNC_WAIT_TIME
                        How long to wait for S3 to reach eventual consistency.
                        This is typically less than a second (zero in us-west)
                        but the default is 5.0 to be safe.
  --ssh-bin=SSH_BIN     Name/path of ssh binary. Arguments are allowed (e.g.
                        --ssh-bin 'ssh -v')
  -s STEP_NUM, --step-num=STEP_NUM
                        Limit results to a single step. To be used with --list
                        and --cat.
  -v, --verbose         print more messages to stderr
"""
from __future__ import print_function

from optparse import OptionParser

from mrjob.emr import EMRJobRunner
from mrjob.job import MRJob
from mrjob.options import add_basic_opts
from mrjob.options import add_emr_connect_opts
from mrjob.options import alphabetize_options
from mrjob.util import scrape_options_into_new_groups

_RELEVANT_LOG_TYPES = ['job', 'node', 'step', 'task']

def main(args=None):
    option_parser = make_option_parser()
    options = parse_args(option_parser, args)

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)

    with EMRJobRunner(**runner_kwargs(options)) as runner:
        perform_actions(options, runner)


def perform_actions(options, runner):
    """Given the command line arguments and an :py:class:`EMRJobRunner`,
    perform various actions for this tool.
    """
    if options.step_num:
        step_nums = [options.step_num]
    else:
        step_nums = None

    if options.list_relevant:
        list_relevant(runner, step_nums)

    if options.list_all:
        list_all(runner)

    if options.cat_relevant:
        cat_relevant(runner, step_nums)

    if options.cat_all:
        cat_all(runner)

    if options.get_counters:
        cluster = runner._describe_cluster()
        runner._set_s3_job_log_uri(cluster)

        steps = runner._list_steps_for_cluster()
        runner._fetch_counters(
            range(1, len(steps) + 1), skip_s3_wait=True)
        runner.print_counters()

    if options.find_failure:
        find_failure(runner, options.step_num)


def parse_args(option_parser, cl_args=None):
    option_parser = make_option_parser()
    options, args = option_parser.parse_args(cl_args)

    # should be one argument, the job flow ID
    if len(args) != 1:
        # this exits the function
        option_parser.error(
            'Must supply one positional argument as the job flow ID')

    options.emr_job_flow_id = args[0]

    return options


def runner_kwargs(options):
    """Given the command line options, return the arguments to
    :py:class:`EMRJobRunner`
    """
    kwargs = options.__dict__.copy()
    for unused_arg in ('quiet', 'verbose', 'list_relevant', 'list_all',
                       'cat_relevant', 'cat_all', 'get_counters', 'step_num',
                       'find_failure'):
        del kwargs[unused_arg]

    return kwargs


def make_option_parser():
    usage = 'usage: %prog [options] JOB_FLOW_ID'
    description = (
        'List, display, and parse Hadoop logs associated with EMR job flows.'
        ' Useful for debugging failed jobs for which mrjob did not display a'
        ' useful error message or for inspecting jobs whose output has been'
        ' lost.')

    option_parser = OptionParser(usage=usage, description=description)

    add_basic_opts(option_parser)

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

    add_emr_connect_opts(option_parser)

    scrape_options_into_new_groups(MRJob().all_option_groups(), {
        option_parser: ('ec2_key_pair_file', 's3_sync_wait_time', 'ssh_bin')
    })

    alphabetize_options(option_parser)

    return option_parser


def prettyprint_paths(paths):
    for path in paths:
        print(path)
    print()


def _prettyprint_relevant(log_type_to_uri_list):
    print('Task attempts:')
    prettyprint_paths(log_type_to_uri_list['task'])
    print('Steps:')
    prettyprint_paths(log_type_to_uri_list['step'])
    print('Jobs:')
    prettyprint_paths(log_type_to_uri_list['job'])
    print('Nodes:')
    prettyprint_paths(log_type_to_uri_list['node'])


def list_relevant(runner, step_nums):
    _prettyprint_relevant(_ls_relevant_logs_by_type(runner, step_nums))

def _ls_relevant_logs_by_type(runner, step_nums=None):
    # TODO: integrate this into EMRJobRunner
    step_num_to_id = runner._step_num_to_id()

    return dict((log_type, runner._ls_logs(log_type,
                                           step_nums=step_nums,
                                           step_num_to_id=step_num_to_id))
                for log_type in _RELEVANT_LOG_TYPES)


def _ls_logs(runner, log_type, step_nums=None):
    # TODO: integrate this into EMRJobRunner
    step_num_to_id = runner._step_num_to_id()

    return runner._ls_logs(log_type,
                           step_nums=step_nums,
                           step_num_to_id=step_num_to_id)


def list_all(runner):
    prettyprint_paths(_ls_logs(runner, 'all'))


def cat_from_list(fs, path_list):
    for path in path_list:
        print('===', path, '===')
        for line in fs.cat(path):
            print(line.rstrip())
        print()


def _cat_from_relevant(fs, log_type_to_uri_list):
    print('Task attempts:')
    cat_from_list(fs, log_type_to_uri_list['task'])
    print('Steps:')
    cat_from_list(fs, log_type_to_uri_list['step'])
    print('Jobs:')
    cat_from_list(fs, log_type_to_uri_list['job'])
    print('Slaves:')
    cat_from_list(fs, log_type_to_uri_list['node'])


def cat_relevant(runner, step_nums):
    _cat_from_relevant(runner.fs,
                       _ls_relevant_logs_by_type(runner, step_nums))


def cat_all(runner):
    cat_from_list(runner.fs, _ls_logs(runner, 'all'))


def find_failure(runner, step_num):
    if step_num:
        step_nums = [step_num]
    else:
        steps = runner._list_steps_for_cluster()
        step_nums = range(1, len(steps) + 1)

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

        print('\n'.join(cause_msg))
    else:
        print('No probable cause of failure found.')


if __name__ == '__main__':
    main()
