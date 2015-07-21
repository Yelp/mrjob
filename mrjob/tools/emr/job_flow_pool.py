# Copyright 2009-2012 Yelp and Contributors
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
"""Inspect available job flow pools or identify job flows suitable for
running a job with the specified options.

Usage::

    python -m mrjob.tools.emr.job_flow_pool

Options::

  -h, --help            show this help message and exit
  --ami-version=AMI_VERSION
                        AMI Version to use, e.g. "2.4.11" (default "latest").
  --aws-region=AWS_REGION
                        Region to connect to S3 and EMR on (e.g. us-west-1).
  --bootstrap=BOOTSTRAP
                        A shell command to set up libraries etc. before any
                        steps (e.g. "sudo apt-get -qy install python3"). You
                        may interpolate files available via URL or locally
                        with Hadoop Distributed Cache syntax ("sudo dpkg -i
                        foo.deb#")
  --bootstrap-action=BOOTSTRAP_ACTIONS
                        Raw bootstrap action scripts to run before any of the
                        other bootstrap steps. You can use --bootstrap-action
                        more than once. Local scripts will be automatically
                        uploaded to S3. To add arguments, just use quotes:
                        "foo.sh arg1 arg2"
  --bootstrap-cmd=BOOTSTRAP_CMDS
                        Commands to run on the master node to set up
                        libraries, etc. You can use --bootstrap-cmd more than
                        once. Use mrjob.conf to specify arguments as a list to
                        be run directly.
  --bootstrap-file=BOOTSTRAP_FILES
                        File to upload to the master node before running
                        bootstrap_cmds (for example, debian packages). These
                        will be made public on S3 due to a limitation of the
                        bootstrap feature. You can use --bootstrap-file more
                        than once.
  --bootstrap-mrjob     Automatically tar up the mrjob library and install it
                        when we run the mrjob. This is the default. Use --no-
                        bootstrap-mrjob if you've already installed mrjob on
                        your Hadoop cluster.
  --no-bootstrap-mrjob  Don't automatically tar up the mrjob library and
                        install it when we run this job. Use this if you've
                        already installed mrjob on your Hadoop cluster.
  --bootstrap-python-package=BOOTSTRAP_PYTHON_PACKAGES
                        Path to a Python module to install on EMR. These
                        should be standard python module tarballs where you
                        can cd into a subdirectory and run ``sudo python
                        setup.py install``. You can use --bootstrap-python-
                        package more than once.
  --bootstrap-script=BOOTSTRAP_SCRIPTS
                        Script to upload and then run on the master node (a
                        combination of bootstrap_cmds and bootstrap_files).
                        These are run after the command from bootstrap_cmds.
                        You can use --bootstrap-script more than once.
  -c CONF_PATHS, --conf-path=CONF_PATHS
                        Path to alternate mrjob.conf file to read from
  --no-conf             Don't load mrjob.conf even if it's available
  --ec2-core-instance-bid-price=EC2_CORE_INSTANCE_BID_PRICE
                        Bid price to specify for core (or "slave") nodes when
                        setting them up as EC2 spot instances (you probably
                        only want to set a bid price for task instances).
  --ec2-core-instance-type=EC2_CORE_INSTANCE_TYPE, --ec2-slave-instance-type=EC2_CORE_INSTANCE_TYPE
                        Type of EC2 instance for core (or "slave") nodes only
  --ec2-master-instance-bid-price=EC2_MASTER_INSTANCE_BID_PRICE
                        Bid price to specify for the master node when setting
                        it up as an EC2 spot instance (you probably only want
                        to set a bid price for task instances).
  --ec2-master-instance-type=EC2_MASTER_INSTANCE_TYPE
                        Type of EC2 instance for master node only
  --ec2-task-instance-bid-price=EC2_TASK_INSTANCE_BID_PRICE
                        Bid price to specify for task nodes when setting them
                        up as EC2 spot instances.
  --ec2-task-instance-type=EC2_TASK_INSTANCE_TYPE
                        Type of EC2 instance for task nodes only
  --emr-endpoint=EMR_ENDPOINT
                        Optional host to connect to when communicating with S3
                        (e.g. us-west-1.elasticmapreduce.amazonaws.com).
                        Default is to infer this from aws_region.
  --pool-name=EMR_JOB_FLOW_POOL_NAME
                        Specify a pool name to join. Set to "default" if not
                        specified.
  --disable-emr-debugging
                        Disable storage of Hadoop logs in SimpleDB
  --enable-emr-debugging
                        Enable storage of Hadoop logs in SimpleDB
  -f, --find            Find a job flow matching the pool name, bootstrap
                        configuration, and instance number/type as specified
                        on the command line and in the configuration files
  -a, --all             List all available job flows without filtering by
                        configuration
  --num-ec2-core-instances=NUM_EC2_CORE_INSTANCES
                        Number of EC2 instances to start as core (or "slave")
                        nodes. Incompatible with --num-ec2-instances.
  --num-ec2-instances=NUM_EC2_INSTANCES
                        Total number of EC2 instances to launch
  --num-ec2-task-instances=NUM_EC2_TASK_INSTANCES
                        Number of EC2 instances to start as task nodes.
                        Incompatible with --num-ec2-instances.
  -q, --quiet           Don't print anything to stderr
  --s3-endpoint=S3_ENDPOINT
                        Host to connect to when communicating with S3 (e.g. s3
                        -us-west-1.amazonaws.com). Default is to infer this
                        from region (see --aws-region).
  -t JOB_FLOW_ID, --terminate=JOB_FLOW_ID
                        Terminate all job flows in the given pool (defaults to
                        pool "default")
  -v, --verbose         print more messages to stderr
"""
from optparse import OptionError
from optparse import OptionParser

from mrjob.emr import EMRJobRunner
from mrjob.emr import est_time_to_hour
from mrjob.job import MRJob
from mrjob.options import add_basic_opts
from mrjob.options import add_emr_connect_opts
from mrjob.options import add_emr_bootstrap_opts
from mrjob.options import add_emr_instance_opts
from mrjob.options import alphabetize_options
from mrjob.util import scrape_options_into_new_groups
from mrjob.util import strip_microseconds


def get_pools(emr_conn):
    pools = {}
    for job_flow in emr_conn.describe_jobflows():
        if job_flow.state in ('TERMINATED', 'FAILED', 'COMPLETED',
                              'SHUTTING_DOWN'):
            continue
        if not job_flow.bootstrapactions:
            continue
        args = [arg.value for arg in job_flow.bootstrapactions[-1].args]
        if len(args) != 2:
            continue
        pools.setdefault(args[1], list()).append(job_flow)

    return pools


def pprint_job_flow(jf):
    """Print a job flow to stdout in this form::

        job.flow.name
        j-JOB_FLOW_ID: 2 instances (master=m1.small, slaves=m1.small, 20 \
minutes to the hour)
    """
    instance_count = int(jf.instancecount)

    nosep_segments = [
        '%d instance' % instance_count,
    ]
    if instance_count > 1:
        nosep_segments.append('s')

    comma_segments = [
        'master=%s' % jf.masterinstancetype,
    ]

    if instance_count > 1:
        comma_segments.append('slaves=%s' % jf.slaveinstancetype)

    comma_segments.append('%s to end of hour' %
                          strip_microseconds(est_time_to_hour(jf)))

    nosep_segments += [
        ' (',
        ', '.join(comma_segments),
        ')',
    ]

    print '%s: %s' % (jf.jobflowid, jf.name)
    print ''.join(nosep_segments)
    print jf.state
    print


def pprint_pools(runner):
    pools = get_pools(runner.make_emr_conn())
    for pool_name, job_flows in pools.iteritems():
        print '-' * len(pool_name)
        print pool_name
        print '-' * len(pool_name)
        for job_flow in job_flows:
            pprint_job_flow(job_flow)


def terminate(runner, pool_name):
    emr_conn = runner.make_emr_conn()
    pools = get_pools(emr_conn)
    try:
        for job_flow in pools[pool_name]:
            emr_conn.terminate_jobflow(job_flow.jobflowid)
            print 'terminated %s' % job_flow.jobflowid
    except KeyError:
        print 'No job flows match pool name "%s"' % pool_name


def main():
    option_parser = make_option_parser()
    try:
        options = parse_args(option_parser)
    except OptionError:
        option_parser.error('This tool takes no arguments.')

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)

    log.warning('job_flow_pool is deprecated and is going away in v0.5.0')

    with EMRJobRunner(**runner_kwargs(options)) as runner:
        perform_actions(options, runner)


def make_option_parser():
    usage = '%prog [options]'
    description = (
        'Inspect available job flow pools or identify job flows suitable for'
        ' running a job with the specified options.')
    option_parser = OptionParser(usage=usage, description=description)


    option_parser.add_option('-a', '--all', action='store_true',
                             default=False, dest='list_all',
                             help=('List all available job flows without'
                                   ' filtering by configuration'))
    option_parser.add_option('-f', '--find', action='store_true',
                             default=False, dest='find',
                             help=('Find a job flow matching the pool name,'
                                   ' bootstrap configuration, and instance'
                                   ' number/type as specified on the command'
                                   ' line and in the configuration files'))
    option_parser.add_option('-t', '--terminate', action='store',
                             default=None, dest='terminate',
                             metavar='JOB_FLOW_ID',
                             help=('Terminate all job flows in the given pool'
                                   ' (defaults to pool "default")'))

    add_basic_opts(option_parser)
    add_emr_connect_opts(option_parser)
    add_emr_instance_opts(option_parser)
    add_emr_bootstrap_opts(option_parser)

    scrape_options_into_new_groups(MRJob().all_option_groups(), {
        option_parser: (
            'bootstrap_mrjob',
            'emr_job_flow_pool_name',
        ),
    })

    alphabetize_options(option_parser)
    return option_parser


def parse_args(option_parser):
    options, args = option_parser.parse_args()

    if len(args) != 0:
        raise OptionError('This program takes no arguments', option_parser)

    return options


def runner_kwargs(options):
    """Given the command line options, return the arguments to
    :py:class:`EMRJobRunner`
    """
    kwargs = options.__dict__.copy()
    for non_runner_kwarg in ('quiet', 'verbose', 'list_all', 'find',
                             'terminate'):
        del kwargs[non_runner_kwarg]

    return kwargs


def perform_actions(options, runner):
    """Given the command line arguments and an :py:class:`EMRJobRunner`,
    perform various actions for this tool.
    """
    if options.list_all:
        pprint_pools(runner)

    if options.find:
        sorted_job_flows = runner.usable_job_flows()

        if sorted_job_flows:
            jf = sorted_job_flows[-1]
            print 'You should use this one:'
            pprint_job_flow(jf)
        else:
            print 'No idle job flows match criteria'

    if options.terminate:
        terminate(runner, options.terminate)


if __name__ == '__main__':
    main()
