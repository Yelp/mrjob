# -*- coding: utf-8 -*-
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
"""Functions to populate py:class:`OptionParser` and :py:class:`OptionGroup`
objects with categorized command line parameters. This module should not be
made public until at least 0.4 if not later or never.
"""

from optparse import OptionParser
from optparse import SUPPRESS_USAGE

from mrjob.runner import CLEANUP_CHOICES


def _append_to_conf_paths(option, opt_str, value, parser):
    """conf_paths is None by default, but --no-conf or --conf-path should make
    it a list.
    """

    if parser.values.conf_paths is None:
        parser.values.conf_paths = []

    # this method is also called during generate_passthrough_arguments
    # the check below is to ensure that conf_paths are not duplicated
    if value not in parser.values.conf_paths:
        parser.values.conf_paths.append(value)


def add_protocol_opts(opt_group):
    """Add options related to choosing protocols.
    """
    return [
        opt_group.add_option(
            '--strict-protocols', dest='strict_protocols', default=None,
            action='store_true', help='If something violates an input/output '
            'protocol then raise an exception'),
    ]


def add_basic_opts(opt_group):
    """Options for all command line tools"""

    return [
        opt_group.add_option(
            '-c', '--conf-path', dest='conf_paths', action='callback',
            callback=_append_to_conf_paths, default=None, nargs=1,
            type='string',
            help='Path to alternate mrjob.conf file to read from'),

        opt_group.add_option(
            '--no-conf', dest='conf_paths', action='store_const', const=[],
            help="Don't load mrjob.conf even if it's available"),

        opt_group.add_option(
            '-q', '--quiet', dest='quiet', default=None,
            action='store_true',
            help="Don't print anything to stderr"),

        opt_group.add_option(
            '-v', '--verbose', dest='verbose', default=None,
            action='store_true', help='print more messages to stderr'),
    ]


def add_runner_opts(opt_group, default_runner='local'):
    """Options for all runners."""
    return [
        opt_group.add_option(
            '--archive', dest='upload_archives', action='append',
            default=[],
            help=('Unpack archive in the working directory of this script. You'
                  ' can use --archive multiple times.')),

        opt_group.add_option(
            '--bootstrap-mrjob', dest='bootstrap_mrjob', action='store_true',
            default=None,
            help=("Automatically tar up the mrjob library and install it when"
                  " we run the mrjob. This is the default. Use"
                  " --no-bootstrap-mrjob if you've already installed mrjob on"
                  " your Hadoop cluster.")),

        opt_group.add_option(
            '--cleanup', dest='cleanup', default=None,
            help=('Comma-separated list of which directories to delete when'
                  ' a job succeeds, e.g. SCRATCH,LOGS. Choices:'
                  ' %s (default: ALL)' % ', '.join(CLEANUP_CHOICES))),

        opt_group.add_option(
            '--cleanup-on-failure', dest='cleanup_on_failure', default=None,
            help=('Comma-separated list of which directories to delete when'
                  ' a job fails, e.g. SCRATCH,LOGS. Choices:'
                  ' %s (default: NONE)' % ', '.join(CLEANUP_CHOICES))),

        opt_group.add_option(
            '--cmdenv', dest='cmdenv', default=[], action='append',
            help='set an environment variable for your job inside Hadoop '
            'streaming. Must take the form KEY=VALUE. You can use --cmdenv '
            'multiple times.'),

        opt_group.add_option(
            '--file', dest='upload_files', action='append',
            default=[],
            help=('Copy file to the working directory of this script. You can'
                  ' use --file multiple times.')),

        opt_group.add_option(
            '--interpreter', dest='interpreter', default=None,
            help=("Interpreter to run your script, e.g. python or ruby.")),

        opt_group.add_option(
            '--no-bootstrap-mrjob', dest='bootstrap_mrjob',
            action='store_false', default=None,
            help=("Don't automatically tar up the mrjob library and install it"
                  " when we run this job. Use this if you've already installed"
                  " mrjob on your Hadoop cluster.")),

        opt_group.add_option(
            '--no-output', dest='no_output',
            default=None, action='store_true',
            help="Don't stream output after job completion"),

        opt_group.add_option(
            '-o', '--output-dir', dest='output_dir', default=None,
            help='Where to put final job output. This must be an s3:// URL ' +
            'for EMR, an HDFS path for Hadoop, and a system path for local,' +
            'and must be empty'),

        opt_group.add_option(
            '--python-archive', dest='python_archives', default=[],
            action='append',
            help=('Archive to unpack and add to the PYTHONPATH of the mr_job'
                  ' script when it runs. You can use --python-archives'
                  ' multiple times.')),

        opt_group.add_option(
            '--python-bin', dest='python_bin', default=None,
            help=("Deprecated. Name/path of alternate python binary for"
                  " wrapper script and Python mappers/reducers. You can"
                  " include arguments, e.g. --python-bin 'python -v'")),

        opt_group.add_option(
            '-r', '--runner', dest='runner', default=default_runner,
            choices=('local', 'hadoop', 'emr', 'inline'),
            help=('Where to run the job: local to run locally, hadoop to run'
                  ' on your Hadoop cluster, emr to run on Amazon'
                  ' ElasticMapReduce, and inline for local debugging. Default'
                  ' is %s.' % default_runner)),

        opt_group.add_option(
            '--setup', dest='setup', action='append',
            help=('A command to run before each mapper/reducer step in the'
                  ' shell ("touch foo"). You may interpolate files'
                  ' available via URL or on your local filesystem using'
                  ' Hadoop Distributed Cache syntax (". setup.sh#"). To'
                  ' interpolate archives, use #/: "cd foo.tar.gz#/; make')),

        opt_group.add_option(
            '--setup-cmd', dest='setup_cmds', action='append',
            default=[],
            help=('A command to run before each mapper/reducer step in the'
                  ' shell (e.g. "cd my-src-tree; make") specified as a string.'
                  ' You can use --setup-cmd more than once. Use mrjob.conf to'
                  ' specify arguments as a list to be run directly.')),

        opt_group.add_option(
            '--setup-script', dest='setup_scripts', action='append',
            default=[],
            help=('Path to file to be copied into the local working directory'
                  ' and then run. You can use --setup-script more than once.'
                  ' These are run after setup_cmds.')),

        opt_group.add_option(
            '--steps-interpreter', dest='steps_interpreter', default=None,
            help=("Name/path of alternate interpreter binary to use to query"
                  " the job about its steps, if different from --interpreter."
                  " Rarely needed.")),

        opt_group.add_option(
            '--steps-python-bin', dest='steps_python_bin', default=None,
            help=('Deprecated. Name/path of alternate python binary to use to'
                  ' query the job about its steps, if different from the'
                  ' current Python interpreter. Rarely needed.')),
    ]


def add_hadoop_shared_opts(opt_group):
    """Options for ``hadoop``, ``local``, and ``emr`` runners"""
    return [
        opt_group.add_option(
            '--hadoop-version', dest='hadoop_version', default=None,
            help=('Version of Hadoop to specify to EMR or to emulate for -r'
                  ' local. Default is 0.20.')),

        # for more info about jobconf:
        # http://hadoop.apache.org/mapreduce/docs/current/mapred-default.html
        opt_group.add_option(
            '--jobconf', dest='jobconf', default=[], action='append',
            help=('-jobconf arg to pass through to hadoop streaming; should'
                  ' take the form KEY=VALUE. You can use --jobconf multiple'
                  ' times.')),
    ]


def add_hadoop_emr_opts(opt_group):
    """Options for ``hadoop`` and ``emr`` runners"""
    return [
        opt_group.add_option(
            '--hadoop-arg', dest='hadoop_extra_args', default=[],
            action='append', help='Argument of any type to pass to hadoop '
            'streaming. You can use --hadoop-arg multiple times.'),

        opt_group.add_option(
            '--hadoop-streaming-jar', dest='hadoop_streaming_jar',
            default=None,
            help='Path of your hadoop streaming jar (locally, or on S3/HDFS)'),

        opt_group.add_option(
            '--label', dest='label', default=None,
            help='custom prefix for job name, to help us identify the job'),

        opt_group.add_option(
            '--owner', dest='owner', default=None,
            help='custom username to use, to help us identify who ran the'
            ' job'),

        opt_group.add_option(
            '--partitioner', dest='partitioner', default=None,
            help=('Hadoop partitioner class to use to determine how mapper'
                  ' output should be sorted and distributed to reducers. For'
                  ' example: org.apache.hadoop.mapred.lib.HashPartitioner')),
    ]


def add_hadoop_opts(opt_group):
    """Options for ``hadoop`` runner"""
    return [
        opt_group.add_option(
            '--hadoop-bin', dest='hadoop_bin', default=None,
            help='hadoop binary. Defaults to $HADOOP_HOME/bin/hadoop'),

        opt_group.add_option(
            '--hdfs-scratch-dir', dest='hdfs_scratch_dir',
            default=None,
            help='Scratch space on HDFS (default is tmp/)'),

        opt_group.add_option(
            '--skip-hadoop-input-check', dest='check_hadoop_input_paths',
            default=True, action='store_false',
            help='Skip the checks to ensure all input paths exist'),
    ]


def add_emr_opts(opt_group):
    """Options for ``emr`` runner"""
    return [
        opt_group.add_option(
            '--additional-emr-info', dest='additional_emr_info', default=None,
            help='A JSON string for selecting additional features on EMR'),

        opt_group.add_option(
            '--ami-version', dest='ami_version', default=None,
            help=(
                'AMI Version to use (currently 1.0, 2.0, or latest, default'
                ' latest).')),

        opt_group.add_option(
            '--aws-availability-zone', dest='aws_availability_zone',
            default=None,
            help='Availability zone to run the job flow on'),

        opt_group.add_option(
            '--aws-region', dest='aws_region', default=None,
            help='Region to connect to S3 and EMR on (e.g. us-west-1).'),

        opt_group.add_option(
            '--bootstrap-action', dest='bootstrap_actions', action='append',
            default=[],
            help=('Raw bootstrap action scripts to run before any of the other'
                  ' bootstrap steps. You can use --bootstrap-action more than'
                  ' once. Local scripts will be automatically uploaded to S3.'
                  ' To add arguments, just use quotes: "foo.sh arg1 arg2"')),

        opt_group.add_option(
            '--bootstrap-cmd', dest='bootstrap_cmds', action='append',
            default=[],
            help=('Commands to run on the master node to set up libraries,'
                  ' etc. You can use --bootstrap-cmd more than once. Use'
                  ' mrjob.conf to specify arguments as a list to be run'
                  ' directly.')),

        opt_group.add_option(
            '--bootstrap-file', dest='bootstrap_files', action='append',
            default=[],
            help=('File to upload to the master node before running'
                  ' bootstrap_cmds (for example, debian packages). These will'
                  ' be made public on S3 due to a limitation of the bootstrap'
                  ' feature. You can use --bootstrap-file more than once.')),

        opt_group.add_option(
            '--bootstrap-python-package', dest='bootstrap_python_packages',
            action='append', default=[],
            help=('Path to a Python module to install on EMR. These should be'
                  ' standard python module tarballs where you can cd into a'
                  ' subdirectory and run ``sudo python setup.py install``. You'
                  ' can use --bootstrap-python-package more than once.')),

        opt_group.add_option(
            '--bootstrap-script', dest='bootstrap_scripts', action='append',
            default=[],
            help=('Script to upload and then run on the master node (a'
                  ' combination of bootstrap_cmds and bootstrap_files). These'
                  ' are run after the command from bootstrap_cmds. You can use'
                  ' --bootstrap-script more than once.')),

        opt_group.add_option(
            '--check-emr-status-every', dest='check_emr_status_every',
            default=None, type='int',
            help='How often (in seconds) to check status of your EMR job'),

        opt_group.add_option(
            '--ec2-instance-type', dest='ec2_instance_type', default=None,
            help=('Type of EC2 instance(s) to launch (e.g. m1.small,'
                  ' c1.xlarge, m2.xlarge). See'
                  ' http://aws.amazon.com/ec2/instance-types/ for the full'
                  ' list.')),

        opt_group.add_option(
            '--ec2-key-pair', dest='ec2_key_pair', default=None,
            help='Name of the SSH key pair you set up for EMR'),

        opt_group.add_option(
            '--ec2-key-pair-file', dest='ec2_key_pair_file', default=None,
            help='Path to file containing SSH key for EMR'),

        # EMR instance types
        opt_group.add_option(
            '--ec2-core-instance-type', '--ec2-slave-instance-type',
            dest='ec2_core_instance_type', default=None,
            help='Type of EC2 instance for core (or "slave") nodes only'),

        opt_group.add_option(
            '--ec2-master-instance-type', dest='ec2_master_instance_type',
            default=None,
            help='Type of EC2 instance for master node only'),

        opt_group.add_option(
            '--ec2-task-instance-type', dest='ec2_task_instance_type',
            default=None,
            help='Type of EC2 instance for task nodes only'),

        # EMR instance bid prices
        opt_group.add_option(
            '--ec2-core-instance-bid-price',
            dest='ec2_core_instance_bid_price', default=None,
            help=(
                'Bid price to specify for core (or "slave") nodes when'
                ' setting them up as EC2 spot instances (you probably only'
                ' want to set a bid price for task instances).')
            ),

        opt_group.add_option(
            '--ec2-master-instance-bid-price',
            dest='ec2_master_instance_bid_price', default=None,
            help=(
                'Bid price to specify for the master node when setting it up '
                'as an EC2 spot instance (you probably only want to set '
                'a bid price for task instances).')
            ),

        opt_group.add_option(
            '--ec2-task-instance-bid-price',
            dest='ec2_task_instance_bid_price', default=None,
            help=(
                'Bid price to specify for task nodes when '
                'setting them up as EC2 spot instances.')
            ),

        opt_group.add_option(
            '--emr-endpoint', dest='emr_endpoint', default=None,
            help=('Optional host to connect to when communicating with S3'
                  ' (e.g. us-west-1.elasticmapreduce.amazonaws.com). Default'
                  ' is to infer this from aws_region.')),

        opt_group.add_option(
            '--emr-job-flow-id', dest='emr_job_flow_id', default=None,
            help='ID of an existing EMR job flow to use'),

        opt_group.add_option(
            '--enable-emr-debugging', dest='enable_emr_debugging',
            default=None, action='store_true',
            help='Enable storage of Hadoop logs in SimpleDB'),

        opt_group.add_option(
            '--disable-emr-debugging', dest='enable_emr_debugging',
            action='store_false',
            help='Disable storage of Hadoop logs in SimpleDB'),

        opt_group.add_option(
            '--hadoop-streaming-jar-on-emr',
            dest='hadoop_streaming_jar_on_emr', default=None,
            help=('Local path of the hadoop streaming jar on the EMR node.'
                  ' Rarely necessary.')),

        opt_group.add_option(
            '--no-pool-emr-job-flows', dest='pool_emr_job_flows',
            action='store_false',
            help="Don't try to run our job on a pooled job flow."),

        opt_group.add_option(
            '--num-ec2-instances', dest='num_ec2_instances', default=None,
            type='int',
            help='Total number of EC2 instances to launch '),

        # NB: EMR instance counts are only applicable for slave/core and
        # task, since a master count > 1 causes the EMR API to return the
        # ValidationError "A master instance group must specify a single
        # instance".
        opt_group.add_option(
            '--num-ec2-core-instances', dest='num_ec2_core_instances',
            default=None, type='int',
            help=('Number of EC2 instances to start as core (or "slave") '
                  'nodes. Incompatible with --num-ec2-instances.')),

        opt_group.add_option(
            '--num-ec2-task-instances', dest='num_ec2_task_instances',
            default=None, type='int',
            help=('Number of EC2 instances to start as task '
                  'nodes. Incompatible with --num-ec2-instances.')),

        opt_group.add_option(
            '--pool-emr-job-flows', dest='pool_emr_job_flows',
            action='store_true',
            help='Add to an existing job flow or create a new one that does'
                 ' not terminate when the job completes. Overrides other job'
                 ' flow-related options including EC2 instance configuration.'
                 ' Joins pool "default" if emr_job_flow_pool_name is not'
                 ' specified. WARNING: do not run this without'
                 ' mrjob.tools.emr.terminate_idle_job_flows in your crontab;'
                 ' job flows left idle can quickly become expensive!'),

        opt_group.add_option(
            '--pool-name', dest='emr_job_flow_pool_name', action='store',
            default=None,
            help=('Specify a pool name to join. Set to "default" if not'
                  ' specified.')),

        opt_group.add_option(
            '--pool-wait-minutes', dest='pool_wait_minutes', default=0,
            type='int',
            help=('Wait for a number of minutes for a job flow to finish'
                  ' if a job finishes, pick up their job flow. Otherwise'
                  ' create a new one. (default 0)')),

        opt_group.add_option(
            '--s3-endpoint', dest='s3_endpoint', default=None,
            help=('Host to connect to when communicating with S3 (e.g.'
                  ' s3-us-west-1.amazonaws.com). Default is to infer this from'
                  ' region (see --aws-region).')),

        opt_group.add_option(
            '--s3-log-uri', dest='s3_log_uri', default=None,
            help='URI on S3 to write logs into'),

        opt_group.add_option(
            '--s3-scratch-uri', dest='s3_scratch_uri', default=None,
            help='URI on S3 to use as our temp directory.'),

        opt_group.add_option(
            '--s3-sync-wait-time', dest='s3_sync_wait_time', default=None,
            type='float',
            help=('How long to wait for S3 to reach eventual consistency. This'
                  ' is typically less than a second (zero in us-west) but the'
                  ' default is 5.0 to be safe.')),

        opt_group.add_option(
            '--ssh-bin', dest='ssh_bin', default=None,
            help=("Name/path of ssh binary. Arguments are allowed (e.g."
                  " --ssh-bin 'ssh -v')")),

        opt_group.add_option(
            '--ssh-bind-ports', dest='ssh_bind_ports', default=None,
            help=('A list of port ranges that are safe to listen on, delimited'
                  ' by colons and commas, with syntax like'
                  ' 2000[:2001][,2003,2005:2008,etc].'
                  ' Defaults to 40001:40840.')),

        opt_group.add_option(
            '--ssh-tunnel-is-closed', dest='ssh_tunnel_is_open',
            default=None, action='store_false',
            help='Make ssh tunnel accessible from localhost only'),

        opt_group.add_option(
            '--ssh-tunnel-is-open', dest='ssh_tunnel_is_open',
            default=None, action='store_true',
            help=('Make ssh tunnel accessible from remote hosts (not just'
                  ' localhost).')),

        opt_group.add_option(
            '--ssh-tunnel-to-job-tracker', dest='ssh_tunnel_to_job_tracker',
            default=None, action='store_true',
            help='Open up an SSH tunnel to the Hadoop job tracker'),
        opt_group.add_option(
            '--visible-to-all-users', dest='visible_to_all_users',
            default=None, action='store_true',
            help='Whether the job flow is visible to all IAM users of the AWS' 
                 ' account associated with the job flow. If this value is set'
                 ' to True, all IAM users of that AWS account can view and'
                 ' (if they have the proper policy permissions set) manage'
                 ' the job flow. If it is set to False, only the IAM user'
                 ' that created the job flow can view and manage it.'),
    ]


def print_help_for_groups(*args):
    option_parser = OptionParser(usage=SUPPRESS_USAGE, add_help_option=False)
    option_parser.option_groups = args
    option_parser.print_help()
