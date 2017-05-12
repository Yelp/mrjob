# -*- coding: utf-8 -*-
# Copyright 2009-2016 Yelp and Contributors
# Copyright 2017 Yelp
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
from __future__ import print_function

import json
from optparse import OptionParser
from optparse import SUPPRESS_USAGE

from mrjob.conf import combine_cmds
from mrjob.conf import combine_dicts
from mrjob.conf import combine_envs
from mrjob.conf import combine_local_envs
from mrjob.conf import combine_lists
from mrjob.conf import combine_paths
from mrjob.conf import combine_path_lists
from mrjob.parse import _parse_port_range_list

#: cleanup options:
#:
#: * ``'ALL'``: delete logs and local and remote temp files; stop cluster
#:   if on EMR and the job is not done when cleanup is run.
#: * ``'CLOUD_TMP'``: delete temp files on cloud storage (e.g. S3) only
#: * ``'CLUSTER'``: terminate the cluster if on EMR and the job is not done
#:    on cleanup
#: * ``'HADOOP_TMP'``: delete temp files on HDFS only
#: * ``'JOB'``: stop job if on EMR and the job is not done when cleanup runs
#: * ``'LOCAL_TMP'``: delete local temp files only
#: * ``'LOGS'``: delete logs only
#: * ``'NONE'``: delete nothing
#: * ``'TMP'``: delete local, HDFS, and cloud storage temp files, but not logs
#:
#: .. versionchanged:: 0.5.0
#:
#:     - ``LOCAL_TMP`` used to be ``LOCAL_SCRATCH``
#:     - ``HADOOP_TMP`` is new (and used to be covered by ``LOCAL_SCRATCH``)
#:     - ``CLOUD_TMP`` used to be ``REMOTE_SCRATCH``
#:
CLEANUP_CHOICES = [
    'ALL',
    'CLOUD_TMP',
    'CLUSTER',
    'HADOOP_TMP',
    'JOB',
    'LOCAL_TMP',
    'LOGS',
    'NONE',
    'TMP',
]

_CLEANUP_DEPRECATED_ALIASES = {
    'JOB_FLOW': 'CLUSTER',
    'LOCAL_SCRATCH': 'LOCAL_TMP',
    'REMOTE_SCRATCH': 'CLOUD_TMP',
    'SCRATCH': 'TMP',
}


### custom callbacks ###

def _default_to(parser, dest, value):
    """Helper function; set the given optino dest to *value* if it's None.

    This lets us create callbacks that don't require default to be set
    to a container."""
    if getattr(parser.values, dest) is None:
        setattr(parser.values, dest, value)


def _append_to_conf_paths(option, opt_str, value, parser):
    """conf_paths is None by default, but --no-conf or --conf-path should make
    it a list.
    """
    # make a list to append to if conf_paths is None
    _default_to(parser, 'conf_paths', [])

    # this method is also called during generate_passthrough_arguments()
    # the check below is to ensure that conf_paths are not duplicated
    if value not in parser.values.conf_paths:
        parser.values.conf_paths.append(value)


def _key_value_callback(option, opt_str, value, parser):
    """callback for KEY=VALUE pairs"""
    # used for --cmdenv, --emr-api-param, and more
    try:
        k, v = value.split('=', 1)
    except ValueError:
        parser.error('%s argument %r is not of the form KEY=VALUE' % (
            opt_str, value))

    _default_to(parser, option.dest, {})
    getattr(parser.values, option.dest)[k] = v


def _key_none_value_callback(option, opt_str, value, parser):
    """callback to set KEY to None"""
    _default_to(parser, option.dest, {})
    getattr(parser.values, option.dest)[value] = None


def _cleanup_callback(option, opt_str, value, parser):
    """callback to parse a comma-separated list of cleanup constants."""
    result = []

    for choice in value.split(','):
        if choice in CLEANUP_CHOICES:
            result.append(choice)
        else:
            parser.error(
                '%s got %s, which is not one of: %s' %
                (opt_str, choice, ', '.join(CLEANUP_CHOICES)))

        if 'NONE' in result and len(set(result)) > 1:
            parser.error(
                '%s: Cannot clean up both nothing and something!' % opt_str)

    setattr(parser.values, option.dest, result)


def _append_json_callback(option, opt_str, value, parser):
    """callback to parse JSON and append it to a list."""
    _default_to(parser, option.dest, [])

    try:
        j = json.loads(value)
    except ValueError as e:
        parser.error('Malformed JSON passed to %s: %s' % (
            opt_str, str(e)))

    getattr(parser.values, option.dest).append(j)


def _port_range_callback(option, opt_str, value, parser):
    """callback to parse --ssh-bind-ports"""
    try:
        ports = _parse_port_range_list(value)
    except ValueError as e:
        parser.error('%s: invalid port range list %r: \n%s' %
                     (opt_str, value, e.args[0]))

    setattr(parser.values, option.dest, ports)

### mux opts ###

# these are used by MRJob to determine what part of a job to run
#
# this just maps dest to the args and kwargs to OptionParser.add_option()
# (minus the dest keyword arg)
_STEP_OPTS = dict(
    run_combiner=(
        ['--combiner'],
        dict(
            action='store_true',
            default=False,
            help='run a combiner',
        ),
    ),
    run_mapper=(
        ['--mapper'],
        dict(
            action='store_true',
            default=False,
            help='run a mapper'
        ),
    ),
    run_reducer=(
        ['--reducer'],
        dict(
            action='store_true',
            default=False,
            help='run a reducer',
        ),
    ),
    run_spark=(
        ['--spark'],
        dict(
            action='store_true',
            default=False,
            help='run Spark code',
        ),
    ),
    show_steps=(
        ['--steps'],
        dict(
            action='store_true',
            default=False,
            help=('print the mappers, combiners, and reducers that this job'
                  ' defines'),
        ),
    ),
    step_num=(
        ['--step-num'],
        dict(
            type='int',
            default=0,
            help='which step to execute (default is 0)',
        ),
    ),
)

# don't show this unless someone types --help --deprecated
_DEPRECATED_NON_RUNNER_OPTS = set([
    'deprecated',
    'partitioner',
])


### runner opts ###

# map from runner option name to dict with the following keys (all optional):
# cloud_role:
#   'connect' if needed when interacting with cloud services at all
#   'launch' if needed when creating a new cluster
#   (cloud runner options with no cloud role are only needed when running jobs)
# combiner: combiner func from mrjob.conf used to combine option values.
#   (if left blank, we use combine_values())
# deprecated: if true, this option is deprecated and slated for removal
# deprecated_aliases: list of old names for this option slated for removal
# runner_combiners: map from runner alias to different combiner to use
#   for that runner (we use this to get combine_local_envs() on sim runners)
# runners: list of aliases of runners that support this option (leave out
#   for options common to all runners
# switches: list of switches to add to option parser for this option. Items
#   have the format (['--switch-names', ...], dict(**kwargs)), where kwargs
#   can be:
#     action: action to pass to option parser (e.g. 'store_true')
#     callback: option parser callback when action is 'callback'. implies
#       action='callback'
#     deprecated_aliases: list of old '--switch-names' slated for removal
#     help: help string to pass to option parser
#     nargs: number of args for callback to parse (defaults to 1 for callback)
#     type: option type for option parser to enforce (e.g. 'float'). defaults
#        to 'string' for callback
#   You can't set the option parser's default; we use [] if *action* is
#   'append' and None otherwise.
_RUNNER_OPTS = dict(
    additional_emr_info=dict(
        cloud_role='launch',
        runners=['emr'],
        switches=[
            (['--additional-emr-info'], dict(
                help='A JSON string for selecting additional features on EMR',
            )),
        ],
    ),
    aws_access_key_id=dict(
        cloud_role='connect',
        runners=['emr'],
    ),
    aws_secret_access_key=dict(
        cloud_role='connect',
        runners=['emr'],
    ),
    aws_session_token=dict(
        cloud_role='connect',
        deprecated_aliases=['aws_security_token'],
        runners=['emr'],
    ),
    bootstrap=dict(
        cloud_role='launch',
        combiner=combine_lists,
        runners=['dataproc', 'emr'],
        switches=[
            (['--bootstrap'], dict(
                action='append',
                help=('A shell command to set up libraries etc. before any'
                      ' steps (e.g. "sudo apt-get -qy install python3"). You'
                      ' may interpolate files available via URL or locally'
                      ' with Hadoop Distributed Cache syntax'
                      ' ("sudo yum install -y foo.rpm#")'),
            )),
        ],
    ),
    bootstrap_actions=dict(
        cloud_role='launch',
        combiner=combine_lists,
        runners=['emr'],
        switches=[
            (['--bootstrap-action'], dict(
                action='append',
                help=('Raw bootstrap action scripts to run before any of the'
                      ' other bootstrap steps. You can use --bootstrap-action'
                      ' more than once. Local scripts will be automatically'
                      ' uploaded to S3. To add arguments, just use quotes:'
                      ' "foo.sh arg1 arg2"'),
            )),
        ],
    ),
    bootstrap_cmds=dict(
        cloud_role='launch',
        combiner=combine_lists,
        deprecated=True,
        runners=['emr'],
        switches=[
            (['--bootstrap-cmd'], dict(
                action='append',
                help=('Commands to run on the master node to set up libraries,'
                      ' etc. You can use --bootstrap-cmd more than once. Use'
                      ' mrjob.conf to specify arguments as a list to be run'
                      ' directly.'),
            )),
        ],
    ),
    bootstrap_files=dict(
        cloud_role='launch',
        combiner=combine_path_lists,
        deprecated=True,
        runners=['emr'],
        switches=[
            (['--bootstrap-file'], dict(
                action='append',
                help=('File to upload to the master node before running'
                      ' bootstrap_cmds (for example, debian packages). You'
                      ' can use --bootstrap-file more than once.'),
            )),
        ],
    ),
    bootstrap_mrjob=dict(
        cloud_role='launch',
        switches=[
            (['--bootstrap-mrjob'], dict(
                action='store_true',
                help=("Automatically zip up the mrjob library and install it"
                      " when we run the mrjob. This is the default. Use"
                      " --no-bootstrap-mrjob if you've already installed"
                      " mrjob on your Hadoop cluster."),
            )),
            (['--no-bootstrap-mrjob'], dict(
                action='store_false',
                help=("Don't automatically zip up the mrjob library and"
                      " install it when we run this job. Use this if you've"
                      " already installed mrjob on your Hadoop cluster."),
            )),
        ],
    ),
    bootstrap_python=dict(
        cloud_role='launch',
        runners=['dataproc', 'emr'],
        switches=[
            (['--bootstrap-python'], dict(
                action='store_true',
                help=('Attempt to install a compatible version of Python'
                      ' at bootstrap time. Currently this only does anything'
                      ' for Python 3, for which it is enabled by default.'),
            )),
            (['--no-bootstrap-python'], dict(
                action='store_false',
                help=("Don't automatically try to install a compatible version"
                      " of Python at bootstrap time."),
            )),
        ],
    ),
    bootstrap_python_packages=dict(
        cloud_role='launch',
        combiner=combine_path_lists,
        deprecated=True,
        runners=['emr'],
        switches=[
            (['--bootstrap-python-package'], dict(
                action='append',
                help=('Path to a Python module to install on EMR. These should'
                      ' be standard python module tarballs where you can cd'
                      ' into a subdirectory and run "sudo python setup.py'
                      ' install". You can use --bootstrap-python-package more'
                      ' than once.'),
            )),
        ],
    ),
    bootstrap_scripts=dict(
        cloud_role='launch',
        combiner=combine_path_lists,
        deprecated=True,
        runners=['emr'],
        switches=[
            (['--bootstrap-script'], dict(
                action='append',
                help=('Script to upload and then run on the master node (a'
                      ' combination of bootstrap_cmds and bootstrap_files).'
                      ' These are run after the command from bootstrap_cmds.'
                      ' You can use --bootstrap-script more than once.'),
            )),
        ],
    ),
    bootstrap_spark=dict(
        cloud_role='launch',
        runners=['emr', 'hadoop'],
        switches=[
            (['--bootstrap-spark'], dict(
                action='store_true',
                help="Auto-install Spark on the cluster (even if not needed)."
            )),
            (['--no-bootstrap-spark'], dict(
                action='store_false',
                help="Don't auto-install Spark on the cluster."
            )),
        ],
    ),
    check_input_paths=dict(
        switches=[
            (['--check-input-paths'], dict(
                action='store_true',
                help='Check input paths exist before running (the default)',
            )),
            (['--no-check-input-paths'], dict(
                action='store_false',
                help='Skip the checks to ensure all input paths exist',
            )),
        ],
    ),
    check_cluster_every=dict(
        deprecated_aliases=['check_emr_status_every'],
        runners=['dataproc', 'emr'],
        switches=[
            (['--check-cluster-every'], dict(
                deprecated_aliases=['--check-emr-status-every'],
                help=('How often (in seconds) to check status of your'
                      ' job/cluster'),
            )),
        ],
    ),
    cleanup=dict(
        switches=[
            (['--cleanup'], dict(
                callback=_cleanup_callback,
                help=('Comma-separated list of which directories to delete'
                      ' when a job succeeds, e.g. TMP,LOGS. Choices:'
                      ' %s (default: ALL)' % ', '.join(CLEANUP_CHOICES)),
            )),
        ],
    ),
    cleanup_on_failure=dict(
        switches=[
            (['--cleanup-on-failure'], dict(
                callback=_cleanup_callback,
                help=('Comma-separated list of which directories to delete'
                      ' when a job fails, e.g. TMP,LOGS. Choices:'
                      ' %s (default: NONE)' % ', '.join(CLEANUP_CHOICES)),
            )),
        ],
    ),
    cloud_fs_sync_secs=dict(
        cloud_role='launch',
        deprecated_aliases=['s3_sync_wait_time'],
        runners=['dataproc', 'emr'],
        switches=[
            (['--cloud-fs-sync-secs'], dict(
                deprecated_aliases=['--s3-sync-wait-time'],
                help=('How long to wait for remote FS to reach eventual'
                      ' consistency. This'
                      ' is typically less than a second but the'
                      ' default is 5.0 to be safe.'),
                type='float',
            )),
        ],
    ),
    cloud_log_dir=dict(
        cloud_role='launch',
        combiner=combine_paths,
        deprecated_aliases=['s3_log_uri'],
        runners=['emr'],
        switches=[
            (['--cloud-log-dir'], dict(
                deprecated_aliases=['--s3-log-uri'],
                help='URI on remote FS to write logs into',
            )),
        ],
    ),
    cloud_tmp_dir=dict(
        cloud_role='launch',
        combiner=combine_paths,
        deprecated_aliases=['s3_scratch_uri', 's3_tmp_dir'],
        runners=['dataproc', 'emr'],
        switches=[
            (['--cloud-tmp-dir'], dict(
                deprecated_aliases=['--s3-scratch-uri', '--s3-tmp-dir'],
                help='URI on remote FS to use as our temp directory.',
            )),
        ],
    ),
    cloud_upload_part_size=dict(
        cloud_role='launch',
        deprecated_aliases=['s3_upload_part_size'],
        runners=['emr'],
        switches=[
            (['--cloud-upload-part-size'], dict(
                deprecated_aliases=['--s3-upload-part-size'],
                help=('Upload files to S3 in parts no bigger than this many'
                      ' megabytes. Default is 100 MiB. Set to 0 to disable'
                      ' multipart uploading entirely.'),
                type='float',
            )),
        ],
    ),
    cluster_id=dict(
        deprecated_aliases=['emr_job_flow_id'],
        runners=['dataproc', 'emr'],
        switches=[
            (['--cluster-id'], dict(
                deprecated_aliases=['--emr-job-flow-id'],
                help='ID of an existing cluster to run our job on',
            )),
        ],
    ),
    cmdenv=dict(
        combiner=combine_envs,
        runner_combiners=dict(
            inline=combine_local_envs,
            local=combine_local_envs,
        ),
        switches=[
            (['--cmdenv'], dict(
                callback=_key_value_callback,
                help=('Set an environment variable for your job inside Hadoop '
                      'streaming. Must take the form KEY=VALUE. You can use'
                      ' --cmdenv multiple times.'),
            )),
        ],
    ),
    core_instance_bid_price=dict(
        cloud_role='launch',
        deprecated_aliases=['ec2_core_instance_bid_price'],
        runners=['emr'],
        switches=[
            (['--core-instance-bid-price'], dict(
                deprecated_aliases=['--ec2-core-instance-bid-price'],
                help=('Bid price to specify for core nodes when'
                      ' setting them up as EC2 spot instances (you probably'
                      ' only want to do this for task instances).'),
            )),
        ],
    ),
    core_instance_type=dict(
        cloud_role='launch',
        deprecated_aliases=[
            'ec2_core_instance_type', 'ec2_slave_instance_type'],
        runners=['dataproc', 'emr'],
        switches=[
            (['--core-instance-type'], dict(
                deprecated_aliases=[
                    '--ec2-core-instance-type', '--ec2-slave-instance-type'],
                help='Type of GCE/EC2 core instance(s) to launch',
            )),
        ],
    ),
    ec2_key_pair=dict(
        cloud_role='launch',
        runners=['emr'],
        switches=[
            (['--ec2-key-pair'], dict(
                help='Name of the SSH key pair you set up for EMR',
            )),
        ],
    ),
    ec2_key_pair_file=dict(
        combiner=combine_paths,
        runners=['emr'],
        switches=[
            (['--ec2-key-pair-file'], dict(
                help='Path to file containing SSH key for EMR',
            )),
        ],
    ),
    emr_action_on_failure=dict(
        runners=['emr'],
        switches=[
            (['--emr-action-on-failure'], dict(
                help=('Action to take when a step fails'
                      ' (e.g. TERMINATE_CLUSTER, CANCEL_AND_WAIT, CONTINUE)'),
            )),
        ],
    ),
    emr_api_params=dict(
        cloud_role='launch',
        combiner=combine_dicts,
        runners=['emr'],
        switches=[
            (['--emr-api-param'], dict(
                callback=_key_value_callback,
                help=('Additional parameter to pass directly to the EMR'
                      ' API when creating a cluster. Should take the form'
                      ' KEY=VALUE. You can use --emr-api-param multiple'
                      ' times'),
            )),
            (['--no-emr-api-param'], dict(
                callback=_key_none_value_callback,
                help=('Parameter to be unset when calling EMR API.'
                      ' You can use --no-emr-api-param multiple times.'),
            )),
        ],
    ),
    applications=dict(
        cloud_role='launch',
        combiner=combine_lists,
        deprecated_aliases=['emr_applications'],
        runners=['emr'],
        switches=[
            (['--application'], dict(
                action='append',
                deprecated_aliases=['--emr-application'],
                help=('Additional applications to run on 4.x AMIs (e.g.'
                      ' Ganglia, Mahout, Spark)'),
            )),
        ],
    ),
    emr_configurations=dict(
        cloud_role='launch',
        combiner=combine_lists,
        runners=['emr'],
        switches=[
            (['--emr-configuration'], dict(
                callback=_append_json_callback,
                help=('Configuration to use on 4.x AMIs as a JSON-encoded'
                      ' dict; see'
                      ' http://docs.aws.amazon.com/ElasticMapReduce/latest/'
                      'ReleaseGuide/emr-configure-apps.html for examples'),
            )),
        ],
    ),
    emr_endpoint=dict(
        cloud_role='connect',
        runners=['emr'],
        switches=[
            (['--emr-endpoint'], dict(
                help=('Force mrjob to connect to EMR on this endpoint'
                      ' (e.g. us-west-1.elasticmapreduce.amazonaws.com).'
                      ' Default is to infer this from region.'),
            )),
        ],
    ),
    enable_emr_debugging=dict(
        cloud_role='launch',
        runners=['emr'],
        switches=[
            (['--enable-emr-debugging'], dict(
                action='store_true',
                help='Enable storage of Hadoop logs in SimpleDB',
            )),
            (['--disable-emr-debugging'], dict(
                action='store_false',
                help=('Disable storage of Hadoop logs in SimpleDB (the'
                      ' default)'),
            )),
        ],
    ),
    gcp_project=dict(
        runners=['dataproc'],
        switches=[
            (['--gcp-project'], dict(
                help='Project to run Dataproc jobs in'
            )),
        ],
    ),
    hadoop_bin=dict(
        combiner=combine_cmds,
        runners=['hadoop'],
        switches=[
            (['--hadoop-bin'], dict(help='path to hadoop binary')),
        ],
    ),
    hadoop_extra_args=dict(
        combiner=combine_lists,
        runners=['emr', 'hadoop'],
        switches=[
            (['--hadoop-arg'], dict(
                action='append',
                help=('Argument of any type to pass to hadoop '
                      'streaming. You can use --hadoop-arg multiple times.'),
            )),
        ],
    ),
    hadoop_home=dict(
        combiner=combine_paths,
        deprecated=True,
        runners=['hadoop'],
        switches=[
            (['--hadoop-home'], dict(
                help=('Deprecated hint about where to find hadoop binary and'
                      ' streaming jar. In most cases mrjob will now find these'
                      ' on its own. If not, use the --hadoop-bin and'
                      ' --hadoop-streaming-jar switches.'),
            )),
        ],
    ),
    hadoop_log_dirs=dict(
        combiner=combine_path_lists,
        runners=['hadoop'],
        switches=[
            (['--hadoop-log-dirs'], dict(
                action='append',
                help=('Directory to search for hadoop logs in. You can use'
                      ' --hadoop-log-dir multiple times.'),
            )),
        ],
    ),
    hadoop_streaming_jar=dict(
        combiner=combine_paths,
        runners=['emr', 'hadoop'],
        switches=[
            (['--hadoop-streaming-jar'], dict(
                help=('Path of your hadoop streaming jar (locally, or on'
                      ' S3/HDFS). In EMR, use a file:// URI to refer to a jar'
                      ' on the master node of your cluster.'),
            )),
        ],
    ),
    hadoop_streaming_jar_on_emr=dict(
        deprecated=True,
        runners=['emr'],
        switches=[
            (['--hadoop-streaming-jar-on-emr'], dict(
                help=("Deprecated: prepend 'file://' and pass that to"
                      " --hadoop-streaming-jar instead"),
            )),
        ],
    ),
    hadoop_tmp_dir=dict(
        combiner=combine_paths,
        deprecated_aliases=['hdfs_scratch_dir'],
        runners=['hadoop'],
        switches=[
            (['--hadoop-tmp-dir'], dict(
                deprecated_aliases=['--hdfs-scratch-dir'],
                help='Temp space on HDFS (default is tmp/mrjob)',
            )),
        ],
    ),
    hadoop_version=dict(
        runners=['inline', 'local'],
        switches=[
            (['--hadoop-version'], dict(
                help='Specific version of Hadoop to simulate',
            )),
        ],
    ),
    iam_endpoint=dict(
        cloud_role='launch',  # not 'connect'; only used to create clusters
        runners=['emr'],
        switches=[
            (['--iam-endpoint'], dict(
                help=('Force mrjob to connect to IAM on this endpoint'
                      ' (e.g. iam.us-gov.amazonaws.com)'),
            )),
        ],
    ),
    iam_instance_profile=dict(
        cloud_role='launch',
        runners=['emr'],
        switches=[
            (['--iam-instance-profile'], dict(
                help=('EC2 instance profile to use for the EMR cluster -- see'
                      ' "Configure IAM Roles for Amazon EMR" in AWS docs'),
            )),
        ],
    ),
    iam_service_role=dict(
        cloud_role='launch',
        runners=['emr'],
        switches=[
            (['--iam-service-role'], dict(
                help=('IAM service role to use for the EMR cluster -- see'
                      ' "Configure IAM Roles for Amazon EMR" in AWS docs')
            )),
        ],
    ),
    image_version=dict(
        cloud_role='launch',
        deprecated_aliases=['ami_version'],
        runners=['dataproc', 'emr'],
        switches=[
            (['--image-version'], dict(
                deprecated_aliases=['--ami-version'],
                help='EMR/Dataproc machine image to launch clusters with',
            )),
        ],
    ),
    instance_type=dict(
        cloud_role='launch',
        deprecated_aliases=['ec2_instance_type'],
        runners=['dataproc', 'emr'],
        switches=[
            (['--instance-type'], dict(
                deprecated_aliases=['--ec2-instance-type'],
                help=('Type of GCE/EC2 instance(s) to launch \n'
                      ' GCE - e.g. n1-standard-1, n1-highcpu-4, n1-highmem-4'
                      ' -- See'
                      ' https://cloud.google.com/compute/docs/machine-types\n'
                      ' EC2 - e.g. m1.medium, c3.xlarge, r3.xlarge '
                      ' -- See http://aws.amazon.com/ec2/instance-types/'),
            )),
        ],
    ),
    interpreter=dict(
        combiner=combine_cmds,
        switches=[
            (['--interpreter'], dict(
                help='Non-python command to run your script, e.g. "ruby".',
            )),
        ],
    ),
    jobconf=dict(
        combiner=combine_dicts,
        switches=[
            (['--jobconf'], dict(
                callback=_key_value_callback,
                help=('-D arg to pass through to hadoop streaming; should'
                      ' take the form KEY=VALUE. You can use --jobconf'
                      ' multiple times.'),
            )),
        ],
    ),
    label=dict(
        cloud_role='launch',
        switches=[
            (['--label'], dict(
                help='Alternate label for the job, to help us identify it.',
            )),
        ],
    ),
    libjars=dict(
        combiner=combine_path_lists,
        switches=[
            (['--libjar'], dict(
                action='append',
                help=('Path of a JAR to pass to Hadoop with -libjar. On EMR,'
                      ' this can also be a URI; use file:/// to reference JARs'
                      ' already on the EMR cluster'),
            )),
        ],
    ),
    local_tmp_dir=dict(
        combiner=combine_paths,
        deprecated_aliases=['base_tmp_dir'],
        # no switches, use $TMPDIR etc.
    ),
    master_instance_bid_price=dict(
        cloud_role='launch',
        deprecated_aliases=['ec2_master_instance_bid_price'],
        runners=['emr'],
        switches=[
            (['--master-instance-bid-price'], dict(
                deprecated_aliases=['--ec2-master-instance-bid-price'],
                help=('Bid price to specify for the master node when'
                      ' setting it up as an EC2 spot instance (you probably'
                      ' only want to do this for task instances).'),
            )),
        ],
    ),
    master_instance_type=dict(
        deprecated_aliases=['ec2_master_instance_type'],
        cloud_role='launch',
        runners=['dataproc', 'emr'],
        switches=[
            (['--master-instance-type'], dict(
                deprecated_aliases=['--ec2-master-instance-type'],
                help='Type of GCE/EC2 master instance to launch',
            )),
        ],
    ),
    max_hours_idle=dict(
        cloud_role='launch',
        runners=['dataproc', 'emr'],
        switches=[
            (['--max-hours-idle'], dict(
                help=("If we create a cluster, have it automatically"
                      " terminate itself after it's been idle this many"
                      " hours"),
                type='float',
            )),
        ],
    ),
    mins_to_end_of_hour=dict(
        cloud_role='launch',
        runners=['emr'],
        switches=[
            (['--mins-to-end-of-hour'], dict(
                help=("If --max-hours-idle is set, control how close to the"
                      " end of an hour the cluster can automatically"
                      " terminate itself (default is 5 minutes)"),
                type='float',
            )),
        ],
    ),
    num_core_instances=dict(
        cloud_role='launch',
        deprecated_aliases=['num_ec2_core_instances'],
        runners=['dataproc', 'emr'],
        switches=[
            (['--num-core-instances'], dict(
                deprecated_aliases=['--num-ec2-core-instances'],
                help='Total number of core instances to launch',
                type='int',
            )),
        ],
    ),
    num_ec2_instances=dict(
        cloud_role='launch',
        deprecated=True,
        runners=['emr'],
        switches=[
            (['--num-ec2-instances'], dict(
                help=('Deprecated: subtract one and pass that to '
                      '--num-core-instances instead'),
                type='int',
            )),
        ],
    ),
    num_task_instances=dict(
        cloud_role='launch',
        deprecated_aliases=['num_ec2_task_instances'],
        runners=['dataproc', 'emr'],
        switches=[
            (['--num-task-instances'], dict(
                deprecated_aliases=['--num-ec2-task-instances'],
                help='Total number of task instances to launch',
                type='int',
            )),
        ],
    ),
    owner=dict(
        cloud_role='launch',
        switches=[
            (['--owner'], dict(
                help='User who ran the job (default is the current user)',
            )),
        ],
    ),
    pool_clusters=dict(
        cloud_role='launch',
        deprecated_aliases=['pool_emr_job_flows'],
        runners=['emr'],
        switches=[
            (['--pool-clusters'], dict(
                deprecated_aliases=['--pool-emr-job-flows'],
                action='store_true',
                help=('Add to an existing cluster or create a new one that'
                      ' does not terminate when the job completes.\n'
                      'WARNING: do not run this without --max-hours-idle or '
                      ' with mrjob terminate-idle-clusters in your crontab;'
                      ' clusters left idle can quickly become expensive!'),
            )),
            (['--no-pool-clusters'], dict(
                deprecated_aliases=['--no-pool-emr-job-flows'],
                action='store_false',
                help="Don't run job on a pooled cluster (the default)",
            )),
        ],
    ),
    pool_name=dict(
        cloud_role='launch',
        deprecated_aliases=['emr_job_flow_pool_name'],
        runners=['emr'],
        switches=[
            (['--pool-name'], dict(
                deprecated_aliases=['--emr-job-flow-pool-name'],
                help='Specify a pool name to join. Default is "default"',
            )),
        ],
    ),
    pool_wait_minutes=dict(
        runners=['emr'],
        switches=[
            (['--pool-wait-minutes'], dict(
                help=('Wait for a number of minutes for a cluster to finish'
                      ' if a job finishes, run job on its cluster. Otherwise'
                      " create a new one. (0, the default, means don't wait)"),
                type='int',
            )),
        ],
    ),
    py_files=dict(
        combiner=combine_path_lists,
        switches=[
            (['--py-file'], dict(
                action='append',
                help='.zip or .egg file to add to PYTHONPATH'
            )),
        ],
    ),
    python_archives=dict(
        combiner=combine_path_lists,
        deprecated=True,
        switches=[
            (['--python-archive'], dict(
                action='append',
                help=('Archive to unpack and add to the PYTHONPATH of the'
                      ' MRJob script when it runs. This is deprecated;'
                      ' try --py-file instead'),
            )),
        ],
    ),
    python_bin=dict(
        combiner=combine_cmds,
        switches=[
            (['--python-bin'], dict(
                help=('Alternate python command for Python mappers/reducers.'
                      ' You can include arguments, e.g. --python-bin "python'
                      ' -v"'),
            )),
        ],
    ),
    region=dict(
        cloud_role='connect',
        deprecated_aliases=['aws_region'],
        runners=['dataproc', 'emr'],
        switches=[
            (['--region'], dict(
                deprecated_aliases=['--aws-region'],
                help='GCE/AWS region to run Dataproc/EMR jobs in.',
            )),
        ],
    ),
    release_label=dict(
        cloud_role='launch',
        runners=['emr'],
        switches=[
            (['--release-label'], dict(
                help=('Release Label (e.g. "emr-4.0.0"). Overrides'
                      ' --image-version'),
            )),
        ],
    ),
    s3_endpoint=dict(
        cloud_role='connect',
        runners=['emr'],
        switches=[
            (['--s3-endpoint'], dict(
                help=("Force mrjob to connect to S3 on this endpoint (e.g."
                      " s3-us-west-1.amazonaws.com). You usually shouldn't"
                      " set this; by default mrjob will choose the correct"
                      " endpoint for each S3 bucket based on its location."),
            )),
        ],
    ),
    setup=dict(
        combiner=combine_lists,
        switches=[
            (['--setup'], dict(
                action='append',
                help=('A command to run before each mapper/reducer step in the'
                      ' shell ("touch foo"). You may interpolate files'
                      ' available via URL or on your local filesystem using'
                      ' Hadoop Distributed Cache syntax (". setup.sh#"). To'
                      ' interpolate archives, use #/: "cd foo.tar.gz#/; make'),
            )),
        ],
    ),
    setup_cmds=dict(
        combiner=combine_lists,
        deprecated=True,
        switches=[
            (['--setup-cmd'], dict(
                action='append',
                help=('A command to run before each mapper/reducer step in the'
                      ' shell (e.g. "cd my-src-tree; make") specified as a'
                      ' string. You can use --setup-cmd more than once. Use'
                      ' mrjob.conf to specify arguments as a list to be run'
                      ' directly.'),
            )),
        ],
    ),
    setup_scripts=dict(
        combiner=combine_path_lists,
        deprecated=True,
        switches=[
            (['--setup-script'], dict(
                action='append',
                help=('Path to file to be copied into the local working'
                      ' directory and then run. You can use --setup-script'
                      ' more than once. These are run after setup_cmds.'),
            )),
        ],
    ),
    sh_bin=dict(
        combiner=combine_cmds,
        switches=[
            (['--sh-bin'], dict(
                help=('Alternate shell command for setup scripts. You may'
                      ' include arguments, e.g. --sh-bin "bash -ex"'),
            )),
        ],
    ),
    spark_args=dict(
        combiner=combine_lists,
        switches=[
            (['--spark-arg'], dict(
                action='append',
                help=('Argument of any type to pass to spark-submit.'
                      ' You can use --spark-arg multiple times.'),
            )),
        ],
    ),
    spark_master=dict(
        runners=['hadoop'],
        switches=[
            (['--spark-master'], dict(
                help=('--master argument to spark-submit (e.g. '
                      'spark://host:port, local. Default is yarn'),
            )),
        ],
    ),
    spark_submit_bin=dict(
        combiner=combine_cmds,
        switches=[
            (['--spark-submit-bin'], dict(
                help='spark-submit binary. You may include arguments.'
            )),
        ],
    ),
    ssh_bin=dict(
        combiner=combine_cmds,
        runners=['emr'],
        switches=[
            (['--ssh-bin'], dict(
                help=("Name/path of ssh binary. Arguments are allowed (e.g."
                      " --ssh-bin 'ssh -v')"),
            )),
        ],
    ),
    ssh_bind_ports=dict(
        runners=['emr'],
        switches=[
            (['--ssh-bind-ports'], dict(
                callback=_port_range_callback,
                help=('A list of port ranges that are safe to listen on,'
                      ' delimited by colons and commas, with syntax like'
                      ' 2000[:2001][,2003,2005:2008,etc].'
                      ' Defaults to 40001:40840.'),
            )),
        ],
    ),
    ssh_tunnel=dict(
        deprecated_aliases=['ssh_tunnel_to_job_tracker'],
        runners=['emr'],
        switches=[
            (['--ssh-tunnel'], dict(
                action='store_true',
                deprecated_aliases=['--ssh-tunnel-to-job-tracker'],
                help=('Open an SSH tunnel to the Hadoop job tracker/resource'
                      ' manager'),
            )),
            (['--no-ssh-tunnel'], dict(
                action='store_false',
                deprecated_aliases=['--no-ssh-tunnel-to-job-tracker'],
                help=("Don't open an SSH tunnel to the Hadoop job"
                      " tracker/resource manager (the default)"),
            )),
        ],
    ),
    ssh_tunnel_is_open=dict(
        runners=['emr'],
        switches=[
            (['--ssh-tunnel-is-open'], dict(
                action='store_true',
                help=('Make ssh tunnel accessible from remote hosts (not just'
                      ' localhost)'),
            )),
            (['--ssh-tunnel-is-closed'], dict(
                action='store_false',
                help=('Make ssh tunnel accessible from localhost only (the'
                      ' default)'),
            )),
        ],
    ),
    steps_interpreter=dict(
        combiner=combine_cmds,
        switches=[
            (['--steps-interpreter'], dict(
                help=("Non-Python command to use to query the job about its"
                      " steps, if different from --interpreter."),
            )),
        ],
    ),
    steps_python_bin=dict(
        combiner=combine_cmds,
        switches=[
            (['--steps-python-bin'], dict(
                help=('Name/path of alternate python command to use to'
                      ' query the job about its steps, if different from the'
                      ' current Python interpreter.'),
            )),
        ],
    ),
    strict_protocols=dict(
        deprecated=True,
        switches=[
            (['--strict-protocols'], dict(
                action='store_true',
                help=('If something violates an input/output '
                      'protocol then raise an exception (the default)'),
            )),
            (['--no-strict-protocols'], dict(
                action='store_false',
                help=('If something violates an input/output '
                      'protocol then increment a counter and continue'),
            )),
        ],
    ),
    subnet=dict(
        cloud_role='launch',
        runners=['emr'],
        switches=[
            (['--subnet'], dict(
                help=('ID of Amazon VPC subnet to launch cluster in. If not'
                      ' set or empty string, cluster is launched in the normal'
                      ' AWS cloud'),
            )),
        ],
    ),
    tags=dict(
        cloud_role='launch',
        combiner=combine_dicts,
        deprecated_aliases=['emr_tags'],
        runners=['emr'],
        switches=[
            (['--tag'], dict(
                callback=_key_value_callback,
                deprecated_aliases=['--emr-tag'],
                help=('Metadata tags to apply to the EMR cluster; '
                      'should take the form KEY=VALUE. You can use --tag '
                      'multiple times'),
            )),
        ],
    ),
    task_instance_bid_price=dict(
        cloud_role='launch',
        deprecated_aliases=['ec2_task_instance_bid_price'],
        runners=['emr'],
        switches=[
            (['--task-instance-bid-price'], dict(
                deprecated_aliases=['--ec2-task-instance-bid-price'],
                help=('Bid price to specify for task nodes when'
                      ' setting them up as EC2 spot instances'),
            )),
        ],
    ),
    task_instance_type=dict(
        cloud_role='launch',
        deprecated_aliases=[
            'ec2_task_instance_type', 'ec2_slave_instance_type'],
        runners=['dataproc', 'emr'],
        switches=[
            (['--task-instance-type'], dict(
                deprecated_aliases=['--ec2-task-instance-type'],
                help='Type of GCE/EC2 task instance(s) to launch',
            )),
        ],
    ),
    task_python_bin=dict(
        combiner=combine_cmds,
        switches=[
            (['--task-python-bin'], dict(
                help=('Name/path of alternate python command to use to'
                      " run tasks (e.g. mappers); doesn't affect setup"
                      ' wrapper scripts. Defaults to'
                      ' current Python interpreter.'),
            )),
        ],
    ),
    upload_archives=dict(
        combiner=combine_path_lists,
        switches=[
            (['--archive'], dict(
                action='append',
                help=('Unpack archive in the working directory of this script.'
                      ' You can use --archive multiple times.'),
            )),
        ],
    ),
    upload_dirs=dict(
        combiner=combine_path_lists,
        switches=[
            (['--dir'], dict(
                action='append',
                help=('Tarball the given directory and unpack the resulting'
                      ' archive in the working directory of this script.'
                      ' You can use --dir multiple times'),
            )),
        ],
    ),
    upload_files=dict(
        combiner=combine_path_lists,
        switches=[
            (['--file'], dict(
                action='append',
                help=('Copy file to the working directory of this script. You'
                      ' can use --file multiple times.'),
            )),
        ],
    ),
    visible_to_all_users=dict(
        cloud_role='launch',
        runners=['emr'],
        switches=[
            (['--visible-to-all-users'], dict(
                action='store_true',
                help=('Make your cluster is visible to all IAM users on the'
                      ' same AWS account (the default)'),
            )),
            (['--no-visible-to-all-users'], dict(
                action='store_false',
                help=('Hide your cluster from other IAM users on the same AWS'
                      ' account'),
            )),
        ],
    ),
    zone=dict(
        cloud_role='launch',
        deprecated_aliases=['aws_availability_zone'],
        runners=['dataproc', 'emr'],
        switches=[
            (['--zone'], dict(
                deprecated_aliases=['--aws-availability-zone'],
                help=('GCE zone/AWS availability zone to run Dataproc/EMR jobs'
                      ' in.'),
            )),
        ],
    ),
)


def _for_runner(config, runner_alias):
    return not config.get('runners') or runner_alias in config['runners']


def _allowed_keys(runner_alias):
    return set(
        name for name, config in _RUNNER_OPTS.items()
        if _for_runner(config, runner_alias),
    )


def _combiners(runner_alias):
    results = {}

    for name, config in _RUNNER_OPTS.items():
        if not _for_runner(config, runner_alias):
            continue

        combiner = (config.get('runner_combiners', {}).get(runner_alias) or
                    config.get('combiner'))

        if combiner:
            results[name] = combiner

    return results


def _deprecated_aliases(runner_alias):
    results = {}

    for name, config in _RUNNER_OPTS.items():
        if not _for_runner(config, runner_alias):
            continue

        if config.get('deprecated_aliases'):
            for alias in config['deprecated_aliases']:
                results[alias] = name

    return results


def _pick_runner_opts(runner_alias=None, cloud_role=None):
    """Return a set of option names that work for the given runner
    (if specified) and fullfill the given cloud roles (if specified).

    By convention, you can use runner_alias ``'base'`` to get options
    available to all runners.
    """
    return set(
        opt_name for opt_name, conf in _RUNNER_OPTS.items()
        if ((runner_alias is None or
             conf.get('runners') is None or
             runner_alias in conf['runners']) and
            (cloud_role is None or
             cloud_role == conf.get('cloud_role')))
    )


def _add_runner_options(parser, opt_names, include_deprecated=True):
    # add options (switches) for the given runner opts to the given
    # options parser, alphabetically by destination.
    for opt_name in sorted(opt_names):
        _add_runner_options_for_opt(
            parser, opt_name, include_deprecated=include_deprecated)


def _add_runner_options_for_opt(parser, opt_name, include_deprecated=True):
    """Add switches for a single option (*opt_name*) to the given parser."""
    conf = _RUNNER_OPTS[opt_name]

    if conf.get('deprecated') and not include_deprecated:
        return

    switches = conf.get('switches') or []

    for args, kwargs in switches:
        kwargs = dict(kwargs)

        deprecated_aliases = kwargs.pop('deprecated_aliases', None)

        kwargs['dest'] = opt_name

        if kwargs.get('callback'):
            kwargs.setdefault('action', 'callback')
            kwargs.setdefault('nargs', 1)
            kwargs.setdefault('type', 'string')

        if kwargs.get('action') == 'append':
            kwargs['default'] = []
        else:
            kwargs['default'] = None

        parser.add_option(*args, **kwargs)

        # add a switch for deprecated aliases
        if deprecated_aliases and include_deprecated:
            help = 'Deprecated alias%s for %s' % (
                ('es' if len(deprecated_aliases) > 1 else ''),
                args[-1])
            parser.add_option(
                *deprecated_aliases,
                **combine_dicts(kwargs, dict(help=help)))


### non-runner switches ###

def _add_basic_options(opt_group):
    """Options for all command line tools"""

    opt_group.add_option(
        '-c', '--conf-path', dest='conf_paths', action='callback',
        callback=_append_to_conf_paths, default=None, nargs=1,
        type='string',
        help='Path to alternate mrjob.conf file to read from')

    opt_group.add_option(
        '--no-conf', dest='conf_paths', action='store_const', const=[],
        help="Don't load mrjob.conf even if it's available")

    opt_group.add_option(
        '-q', '--quiet', dest='quiet', default=None,
        action='store_true',
        help="Don't print anything to stderr")

    opt_group.add_option(
        '-v', '--verbose', dest='verbose', default=None,
        action='store_true', help='print more messages to stderr')


def _add_job_options(opt_group):
    opt_group.add_option(
        '--no-output', dest='no_output',
        default=None, action='store_true',
        help="Don't stream output after job completion")

    opt_group.add_option(
        '-o', '--output-dir', dest='output_dir', default=None,
        help='Where to put final job output. This must be an s3:// URL ' +
        'for EMR, an HDFS path for Hadoop, and a system path for local,' +
        'and must be empty')

    opt_group.add_option(
        '--partitioner', dest='partitioner', default=None,
        help=('Hadoop partitioner class. Deprecated as of v0.5.1 and'
              ' will be removed in v0.6.0 (specify in your job instead)'))

    opt_group.add_option(
        '-r', '--runner', dest='runner', default=None,
        choices=('local', 'hadoop', 'emr', 'inline', 'dataproc'),
        help=('Where to run the job; one of dataproc, emr, hadoop, inline,'
              ' or local'))

    opt_group.add_option(
        '--step-output-dir', dest='step_output_dir', default=None,
        help=('A directory to store output from job steps other than'
              ' the last one. Useful for debugging. Currently'
              ' ignored by local runners.'))


def _add_step_options(opt_group):
    """Add options that determine what part of the job a MRJob runs."""
    for dest, (args, kwargs) in _STEP_OPTS.items():
        kwargs = dict(dest=dest, **kwargs)
        opt_group.add_option(*args, **kwargs)


### other utilities for switches ###

def _print_help_for_runner(runner_alias, include_deprecated=False):
    help_parser = OptionParser(usage=SUPPRESS_USAGE, add_help_option=False)

    opt_names = _pick_runner_opts(runner_alias)
    _add_runner_options(help_parser, opt_names,
                        include_deprecated=include_deprecated)

    _alphabetize_options(help_parser)
    help_parser.print_help()


def _print_help_for_steps():
    help_parser = OptionParser(usage=SUPPRESS_USAGE, add_help_option=False)

    _add_step_options(help_parser)

    _alphabetize_options(help_parser)
    help_parser.print_help()


def _print_basic_help(option_parser, usage, include_deprecated=False):
    """Print all help for the parser. Unlike similar functions, this needs a
    parser so that it can include custom options added by a
    :py:class:`~mrjob.job.MRJob`.
    """
    help_parser = OptionParser(usage=usage, add_help_option=False)

    for option in option_parser._get_all_options():
        if option.dest in _RUNNER_OPTS:
            continue

        if option.dest in _STEP_OPTS:
            continue

        if (option.dest in _DEPRECATED_NON_RUNNER_OPTS and
                not include_deprecated):
            continue

        help_parser.add_option(
            *(option._short_opts + option._long_opts),
            dest=option.dest,
            help=option.help)

    _alphabetize_options(help_parser)
    help_parser.print_help()

    print()
    print('To see help for a specific runner, use --help -r <runner name>')
    print()
    print('To see help for options that control what part of a job runs,'
          ' use --help --steps')
    print()
    if not include_deprecated:
        print('To include help for deprecated options, add --deprecated')
        print()


def _alphabetize_options(opt_group):
    opt_group.option_list.sort(key=lambda opt: opt.dest or '')
