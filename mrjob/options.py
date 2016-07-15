# -*- coding: utf-8 -*-
# Copyright 2009-2016 Yelp and Contributors
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
import json
from optparse import OptionParser
from optparse import SUPPRESS_USAGE

from mrjob.parse import parse_key_value_list
from mrjob.parse import parse_port_range_list
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


def _add_protocol_opts(opt_group):
    """Add options related to choosing protocols.
    """
    return [
        opt_group.add_option(
            '--strict-protocols', dest='strict_protocols', default=None,
            action='store_true', help='If something violates an input/output '
            'protocol then raise an exception (the default)'),
        opt_group.add_option(
            '--no-strict-protocols', dest='strict_protocols', default=None,
            action='store_false', help='If something violates an input/output '
            'protocol then increment a counter and continue'),
    ]


def _add_basic_opts(opt_group):
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


def _add_runner_opts(opt_group, default_runner='local'):
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
                  ' a job succeeds, e.g. TMP,LOGS. Choices:'
                  ' %s (default: ALL)' % ', '.join(CLEANUP_CHOICES))),

        opt_group.add_option(
            '--cleanup-on-failure', dest='cleanup_on_failure', default=None,
            help=('Comma-separated list of which directories to delete when'
                  ' a job fails, e.g. TMP,LOGS. Choices:'
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
            help=('Non-python command to run your script, e.g. "ruby".')),

        # for more info about jobconf:
        # http://hadoop.apache.org/mapreduce/docs/current/mapred-default.html
        opt_group.add_option(
            '--jobconf', dest='jobconf', default=[], action='append',
            help=('-D arg to pass through to hadoop streaming; should'
                  ' take the form KEY=VALUE. You can use --jobconf multiple'
                  ' times.')),

        opt_group.add_option(
            '--libjar', dest='libjars', default=[],
            action='append', help=(
                'Path of a JAR to pass to Hadoop with -libjar. On EMR, this'
                ' can also be a URI; use file:/// to reference JARs already'
                ' on the EMR cluster')),

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
            '--partitioner', dest='partitioner', default=None,
            help=('Hadoop partitioner class. Deprecated as of v0.5.1 and'
                  ' will be removed in v0.6.0 (specify in your job instead)')),

        opt_group.add_option(
            '--python-archive', dest='python_archives', default=[],
            action='append',
            help=('Archive to unpack and add to the PYTHONPATH of the mr_job'
                  ' script when it runs. You can use --python-archives'
                  ' multiple times.')),

        opt_group.add_option(
            '--python-bin', dest='python_bin', default=None,
            help=('Alternate python command for Python'
                  ' mappers/reducers. You can'
                  ' include arguments, e.g. --python-bin "python -v"')),

        opt_group.add_option(
            '-r', '--runner', dest='runner', default=default_runner,
            choices=('local', 'hadoop', 'emr', 'inline', 'dataproc'),
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
            help=("Non-Python command to use to query the job about its"
                  " steps, if different from --interpreter.")),

        opt_group.add_option(
            '--steps-python-bin', dest='steps_python_bin', default=None,
            help=('Name/path of alternate python command to use to'
                  ' query the job about its steps, if different from the'
                  ' current Python interpreter.')),
    ]


def _add_local_opts(opt_group):
    """Options for ``inline`` and ``local`` runners."""
    return [
        opt_group.add_option(
            '--hadoop-version', dest='hadoop_version', default=None,
            help=('Specific version of Hadoop to simulate')),
    ]


def _add_hadoop_emr_opts(opt_group):
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
            help='alternate label for the job, to help us identify it'),

        opt_group.add_option(
            '--owner', dest='owner', default=None,
            help='user who ran the job (if different from the current user)'),

        opt_group.add_option(
            '--check-input-paths', dest='check_input_paths',
            default=None, action='store_true',
            help='Check input paths exist before running (the default)'),

        opt_group.add_option(
            '--no-check-input-paths', dest='check_input_paths',
            default=None, action='store_false',
            help='Skip the checks to ensure all input paths exist'),
    ]


def _add_hadoop_opts(opt_group):
    """Options for ``hadoop`` runner"""
    return [
        opt_group.add_option(
            '--hadoop-bin', dest='hadoop_bin', default=None,
            help='path to hadoop binary'),

        opt_group.add_option(
            '--hadoop-log-dir', dest='hadoop_log_dirs', default=[],
            action='append', help='Directory to search for'
            ' hadoop logs in. You can use --hadoop-log-dir multiple times.'),

        opt_group.add_option(
            '--hadoop-home', dest='hadoop_home',
            default=None,
            help='Deprecated hint about where to find hadoop binary and'
                 ' streaming jar. In most cases mrjob will now find these on'
                 ' its own. If not, use the --hadoop-bin and'
                 ' --hadoop-streaming-jar switches.'),

        opt_group.add_option(
            '--hadoop-tmp-dir', dest='hadoop_tmp_dir',
            default=None,
            help='Temp space on HDFS (default is tmp/mrjob)'),

        opt_group.add_option(
            '--hdfs-scratch-dir', dest='hdfs_scratch_dir',
            default=None,
            help='Deprecated alias for --hadoop-tmp-dir'),
    ]


def _add_dataproc_emr_opts(opt_group):
    return [
        opt_group.add_option(
            '--cluster-id', dest='cluster_id', default=None,
            help='ID of an existing cluster to run our job on'),

        opt_group.add_option(
            '--bootstrap', dest='bootstrap', action='append',
            help=('A shell command to set up libraries etc. before any steps'
                  ' (e.g. "sudo apt-get -qy install python3"). You may'
                  ' interpolate files available via URL or locally with Hadoop'
                  ' Distributed Cache syntax ("sudo dpkg -i foo.deb#")')),

        opt_group.add_option(
            '--bootstrap-python', dest='bootstrap_python',
            action='store_true', default=None,
            help=('Attempt to install a compatible version of Python'
                  ' at bootstrap time. Currently this only does anything'
                  ' for Python 3, for which it is enabled by default.')),

        opt_group.add_option(
            '--max-hours-idle', dest='max_hours_idle',
            default=None, type='float',
            help=("If we create a cluster, have it automatically"
                  " terminate itself after it's been idle this many hours.")),
    ]


def _add_dataproc_opts(opt_group):
    """Options for ``dataproc`` runner"""
    return [
        opt_group.add_option(
            '--gcp-project', dest='gcp_project', default=None,
            help='Project to run Dataproc jobs in.'),

        opt_group.add_option(
            '--region', dest='region',
            help='GCE region to run Dataproc/EMR jobs in.'),

        opt_group.add_option(
            '--zone', dest='zone', default=None,
            help='GCE zone to run Dataproc/EMR jobs in.'),

        opt_group.add_option(
            '--image-version', dest='image_version', default=None,
            help='EMR/Dataproc image to run Dataproc/EMR jobs with.  '),

        opt_group.add_option(
            '--check-cluster-every', dest='check_cluster_every', default=None,
            help='How often (in seconds) to check status of your job/cluster'),

        # instance types
        opt_group.add_option(
            '--instance-type', dest='instance_type', default=None,
            help=('Type of GCE/EC2 instance(s) to launch \n'
                  ' GCE - e.g. n1-standard-1, n1-highcpu-4, n1-highmem-4 -'
                  ' See https://cloud.google.com/compute/docs/machine-types\n'
                  ' EC2 - e.g. m1.medium, c3.xlarge, r3.xlarge -'
                  ' See http://aws.amazon.com/ec2/instance-types/'
                  )),

        opt_group.add_option(
            '--master-instance-type', dest='master_instance_type',
            default=None,
            help='Type of GCE/EC2 master instance(s) to launch'),

        opt_group.add_option(
            '--core-instance-type', dest='core_instance_type', default=None,
            help='Type of GCE/EC2 core instance(s) to launch'),

        opt_group.add_option(
            '--task-instance-type', dest='task_instance_type', default=None,
            help='Type of GCE/EC2 task instance(s) to launch'),

        opt_group.add_option(
            '--num-core-instances', dest='num_core_instances', default=None,
            type='int',
            help='Total number of Worker instances to launch '),

        opt_group.add_option(
            '--num-task-instances', dest='num_task_instances', default=None,
            type='int',
            help='Total number of preemptible Worker instances to launch '),


        opt_group.add_option(
            '--cloud-fs-sync-secs', dest='cloud_fs_sync_secs', default=None,
            type='float',
            help=('How long to wait for remote FS to reach eventual'
                  ' consistency. This'
                  ' is typically less than a second but the'
                  ' default is 5.0 to be safe.')),

        opt_group.add_option(
            '--cloud-tmp-dir', dest='cloud_tmp_dir', default=None,
            help='URI on remote FS to use as our temp directory.'),

    ]


def _add_emr_opts(opt_group):
    """Options for ``emr`` runner"""
    return (_add_emr_connect_opts(opt_group) +
            _add_emr_launch_opts(opt_group) +
            _add_emr_run_opts(opt_group))


def _add_emr_connect_opts(opt_group):
    """Options for connecting to the EMR API."""
    return [
        opt_group.add_option(
            '--aws-region', dest='aws_region', default=None,
            help=('Region to run EMR jobs in. Default is us-west-2')),

        opt_group.add_option(
            '--emr-endpoint', dest='emr_endpoint', default=None,
            help=('Force mrjob to connect to EMR on this endpoint'
                  ' (e.g. us-west-1.elasticmapreduce.amazonaws.com). Default'
                  ' is to infer this from aws_region.')),

        opt_group.add_option(
            '--s3-endpoint', dest='s3_endpoint', default=None,
            help=("Force mrjob to connect to S3 on this endpoint (e.g."
                  " s3-us-west-1.amazonaws.com). You usually shouldn't"
                  " set this; by default mrjob will choose the correct"
                  " endpoint for each S3 bucket based on its location.")),
    ]


def _add_emr_run_opts(opt_group):
    """Options for running and monitoring a job on EMR."""
    return [
        opt_group.add_option(
            '--check-emr-status-every', dest='check_emr_status_every',
            default=None, type='int',
            help='How often (in seconds) to check status of your EMR job'),

        # --ec2-key-pair is used to launch the job, not to monitor it
        opt_group.add_option(
            '--ec2-key-pair-file', dest='ec2_key_pair_file', default=None,
            help='Path to file containing SSH key for EMR'),

        opt_group.add_option(
            '--emr-action-on-failure', dest='emr_action_on_failure',
            default=None,
            help=('Action to take when a step fails'
                  ' (e.g. TERMINATE_CLUSTER | CANCEL_AND_WAIT | CONTINUE)')),

        opt_group.add_option(
            '--emr-job-flow-id', dest='emr_job_flow_id', default=None,
            help='Deprecated alias for --cluster-id'),

        opt_group.add_option(
            '--hadoop-streaming-jar-on-emr',
            dest='hadoop_streaming_jar_on_emr', default=None,
            help=('Local path of the hadoop streaming jar on the EMR node.'
                  ' Rarely necessary.')),

        opt_group.add_option(
            '--no-ssh-tunnel', dest='ssh_tunnel',
            default=None, action='store_false',
            help=("Don't open an SSH tunnel to the Hadoop job"
                  " tracker/resource manager")),

        opt_group.add_option(
            '--pool-wait-minutes', dest='pool_wait_minutes', default=None,
            type='int',
            help=('Wait for a number of minutes for a cluster to finish'
                  ' if a job finishes, run job on its cluster. Otherwise'
                  " create a new one. (0, the default, means don't wait)")),

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
            '--ssh-tunnel', dest='ssh_tunnel',
            default=None, action='store_true',
            help=('Open an SSH tunnel to the Hadoop job tracker/resource'
                  ' manager')),

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
            help='Deprecated alias for --ssh-tunnel'),
    ]


def _add_emr_launch_opts(opt_group):
    """Options for launching a cluster (including bootstrapping)."""
    return [
        opt_group.add_option(
            '--additional-emr-info', dest='additional_emr_info', default=None,
            help='A JSON string for selecting additional features on EMR'),

        opt_group.add_option(
            '--aws-availability-zone', dest='aws_availability_zone',
            default=None,
            help='Availability zone to run the cluster on'),

        opt_group.add_option(
            '--ec2-key-pair', dest='ec2_key_pair', default=None,
            help='Name of the SSH key pair you set up for EMR'),

        opt_group.add_option(
            '--emr-api-param', dest='emr_api_params',
            default=[], action='append',
            help='Additional parameters to pass directly to the EMR API '
                 ' when creating a cluster. Should take the form KEY=VALUE.'
                 ' You can use --emr-api-param multiple times.'
        ),

        opt_group.add_option(
            '--no-emr-api-param', dest='no_emr_api_params',
            default=[], action='append',
            help='Parameters to be unset when calling EMR API.'
                 ' You can use --no-emr-api-param multiple times.'
        ),

        opt_group.add_option(
            '--emr-application', dest='emr_applications',
            default=[], action='append',
            help='Additional applications to run on 4.x AMIs (e.g. Ganglia,'
                 ' Mahout, Spark)'),

        opt_group.add_option(
            '--emr-configuration', dest='emr_configurations',
            default=[], action='append',
            help=('Configuration to use on 4.x AMIs as a JSON-encoded dict;'
                  ' see http://docs.aws.amazon.com/ElasticMapReduce/latest/'
                  'ReleaseGuide/emr-configure-apps.html for examples.')),

        opt_group.add_option(
            '--emr-tag', dest='emr_tags',
            default=[], action='append',
            help='Metadata tags to apply to the EMR cluster; '
                 'should take the form KEY=VALUE. You can use --emr-tag '
                 'multiple times.'),

        opt_group.add_option(
            '--iam-endpoint', dest='iam_endpoint', default=None,
            help=('Force mrjob to connect to IAM on this endpoint'
                  ' (e.g. iam.us-gov.amazonaws.com)')),

        opt_group.add_option(
            '--iam-instance-profile', dest='iam_instance_profile',
            default=None,
            help=('EC2 instance profile to use for the EMR cluster - see'
                  ' "Configure IAM Roles for Amazon EMR" in AWS docs')),

        opt_group.add_option(
            '--iam-service-role', dest='iam_service_role',
            default=None,
            help=('IAM service role to use for the EMR cluster -- see'
                  ' "Configure IAM Roles for Amazon EMR" in AWS docs')),

        opt_group.add_option(
            '--mins-to-end-of-hour', dest='mins_to_end_of_hour',
            default=None, type='float',
            help=("If --max-hours-idle is set, control how close to the end"
                  " of an hour the cluster can automatically"
                  " terminate itself (default is 5 minutes).")),

        opt_group.add_option(
            '--no-bootstrap-python', dest='bootstrap_python',
            action='store_false', default=None,
            help=("Don't automatically try to install a compatible version"
                  " of Python at bootstrap time.")),

        opt_group.add_option(
            '--no-pool-clusters', dest='pool_clusters',
            action='store_false',
            help="Don't run our job on a pooled cluster (the default)."),

        opt_group.add_option(
            '--no-pool-emr-job-flows', dest='pool_emr_job_flows',
            action='store_false',
            help="Deprecated alias for --no-pool-clusters"),

        opt_group.add_option(
            '--pool-clusters', dest='pool_clusters',
            action='store_true',
            help='Add to an existing cluster or create a new one that does'
                 ' not terminate when the job completes. Overrides other '
                 ' cluster-related options including EC2 instance'
                 ' configuration. Joins pool "default" if --pool-name is not'
                 ' specified. WARNING: do not run this without'
                 ' mrjob terminate-idle-clusters in your crontab;'
                 ' clusters left idle can quickly become expensive!'),

        opt_group.add_option(
            '--pool-emr-job-flows', dest='pool_emr_job_flows',
            action='store_true',
            help='Deprecated alias for --pool-clusters'),

        opt_group.add_option(
            '--pool-name', dest='pool_name', action='store',
            default=None,
            help=('Specify a pool name to join. Set to "default" if not'
                  ' specified.')),

        opt_group.add_option(
            '--s3-log-uri', dest='s3_log_uri', default=None,
            help='URI on S3 to write logs into'),

        opt_group.add_option(
            '--s3-scratch-uri', dest='s3_scratch_uri', default=None,
            help='Deprecated alias for --s3-tmp-dir.'),

        opt_group.add_option(
            '--s3-sync-wait-time', dest='s3_sync_wait_time', default=None,
            type='float',
            help=('How long to wait for S3 to reach eventual consistency. This'
                  ' is typically less than a second (zero in us-west) but the'
                  ' default is 5.0 to be safe.')),

        opt_group.add_option(
            '--s3-tmp-dir', dest='s3_tmp_dir', default=None,
            help='URI on S3 to use as our temp directory.'),

        opt_group.add_option(
            '--s3-upload-part-size', dest='s3_upload_part_size', default=None,
            type='float',
            help=('Upload files to S3 in parts no bigger than this many'
                  ' megabytes. Default is 100 MiB. Set to 0 to disable'
                  ' multipart uploading entirely.')),

        opt_group.add_option(
            '--subnet', dest='subnet', default=None,
            help=('ID of Amazon VPC subnet to launch cluster in (if not set'
                  ' or empty string, cluster is launched in the normal AWS'
                  ' cloud)')),

        opt_group.add_option(
            '--visible-to-all-users', dest='visible_to_all_users',
            default=None, action='store_true',
            help='Make your cluster is visible to all IAM users on the same'
                 ' AWS account (the default).'
        ),

        opt_group.add_option(
            '--no-visible-to-all-users', dest='visible_to_all_users',
            default=None, action='store_false',
            help='Hide your cluster from other IAM users on the same AWS'
                 ' account.'
        ),

    ] + _add_emr_bootstrap_opts(opt_group) + _add_emr_instance_opts(opt_group)


def _add_emr_bootstrap_opts(opt_group):
    """Add options having to do with bootstrapping (other than
    :mrjob-opt:`bootstrap_mrjob`, which is shared with other runners)."""
    return [

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
            '--disable-emr-debugging', dest='enable_emr_debugging',
            action='store_false',
            help='Disable storage of Hadoop logs in SimpleDB'),

        opt_group.add_option(
            '--enable-emr-debugging', dest='enable_emr_debugging',
            default=None, action='store_true',
            help='Enable storage of Hadoop logs in SimpleDB'),
    ]


def _add_emr_instance_opts(opt_group):
    """Add options having to do with instance creation"""
    return [
        # AMI
        opt_group.add_option(
            '--ami-version', dest='ami_version', default=None,
            help=('AMI Version to use, e.g. "2.4.11", "3.8.0", "4.0.0"')),

        opt_group.add_option(
            '--release-label', dest='release_label', default=None,
            help=('Release Label (e.g. "emr-4.0.0"). Overrides'
                  ' --ami-version')),

        # instance types
        opt_group.add_option(
            '--ec2-core-instance-type', '--ec2-slave-instance-type',
            dest='ec2_core_instance_type', default=None,
            help='Type of EC2 instance for core (or "slave") nodes only'),

        opt_group.add_option(
            '--ec2-instance-type', dest='ec2_instance_type', default=None,
            help=('Type of EC2 instance(s) to launch (e.g. m1.medium,'
                  ' c3.xlarge, r3.xlarge). See'
                  ' http://aws.amazon.com/ec2/instance-types/ for the full'
                  ' list.')),

        opt_group.add_option(
            '--ec2-master-instance-type', dest='ec2_master_instance_type',
            default=None,
            help='Type of EC2 instance for master node only'),

        opt_group.add_option(
            '--ec2-task-instance-type', dest='ec2_task_instance_type',
            default=None,
            help='Type of EC2 instance for task nodes only'),

        # instance number
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

        # bid price
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
    ]


def _print_help_for_groups(*args):
    option_parser = OptionParser(usage=SUPPRESS_USAGE, add_help_option=False)
    option_parser.option_groups = args
    option_parser.print_help()


def _alphabetize_options(opt_group):
    opt_group.option_list.sort(key=lambda opt: opt.dest or '')


def _fix_custom_options(options, option_parser):
    """Update *options* to handle KEY=VALUE options, etc."""
    if hasattr(options, 'cmdenv'):
        cmdenv_err = '--cmdenv argument %r is not of the form KEY=VALUE'
        options.cmdenv = parse_key_value_list(options.cmdenv,
                                              cmdenv_err,
                                              option_parser.error)

    def parse_commas(cleanup_str):
        cleanup_error = ('cleanup option %s is not one of ' +
                         ', '.join(CLEANUP_CHOICES))
        new_cleanup_options = []
        for choice in cleanup_str.split(','):
            if choice in CLEANUP_CHOICES:
                new_cleanup_options.append(choice)
            else:
                option_parser.error(cleanup_error % choice)
        if ('NONE' in new_cleanup_options and
                len(set(new_cleanup_options)) > 1):
            option_parser.error(
                'Cannot clean up both nothing and something!')

        return new_cleanup_options

    if getattr(options, 'cleanup', None):
        options.cleanup = parse_commas(options.cleanup)

    if getattr(options, 'cleanup_on_failure', None):
        options.cleanup_on_failure = parse_commas(options.cleanup_on_failure)

    if hasattr(options, 'emr_api_params'):
        emr_api_err = (
            '--emr-api-params argument %r is not of the form KEY=VALUE')
        options.emr_api_params = parse_key_value_list(options.emr_api_params,
                                                      emr_api_err,
                                                      option_parser.error)

        if hasattr(options, 'no_emr_api_params'):
                for param in options.no_emr_api_params:
                    options.emr_api_params[param] = None

    if hasattr(options, 'emr_configurations'):
        decoded_configurations = []

        for c in options.emr_configurations:
            try:
                decoded_configurations.append(json.loads(c))
            except ValueError as e:
                option_parser.error(
                    'Malformed JSON passed to --emr-configuration: %s' % (
                        str(e)))

        options.emr_configurations = decoded_configurations

    if hasattr(options, 'emr_tags'):
        emr_tag_err = '--emr-tag argument %r is not of the form KEY=VALUE'
        options.emr_tags = parse_key_value_list(options.emr_tags,
                                                emr_tag_err,
                                                option_parser.error)

    if hasattr(options, 'jobconf'):
        jobconf_err = '--jobconf argument %r is not of the form KEY=VALUE'
        options.jobconf = parse_key_value_list(options.jobconf,
                                               jobconf_err,
                                               option_parser.error)

    if getattr(options, 'ssh_bind_ports', None):
        try:
            ports = parse_port_range_list(options.ssh_bind_ports)
        except ValueError as e:
            option_parser.error('invalid port range list %r: \n%s' %
                                (options.ssh_bind_ports, e.args[0]))
            options.ssh_bind_ports = ports
