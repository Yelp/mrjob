# Copyright 2009-2013 Yelp and Contributors
# Copyright 2015-2016 Yelp
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
"""Create a persistent EMR cluster to run clusters in, and print its ID to
stdout.

.. warning::

    Do not run this without ``mrjob terminate-idle-clusters`` in
    your crontab; clusters left idle can quickly become expensive!

Usage::

    mrjob create-cluster

Options::

  -h, --help            show this help message and exit
  --additional-emr-info=ADDITIONAL_EMR_INFO
                        A JSON string for selecting additional features on EMR
  --ami-version=AMI_VERSION
                        AMI Version to use, e.g. "2.4.11" (default "latest").
  --aws-availability-zone=AWS_AVAILABILITY_ZONE
                        Availability zone to run the cluster on
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
  --ec2-core-instance-type=EC2_CORE_INSTANCE_TYPE,
  --ec2-slave-instance-type=EC2_CORE_INSTANCE_TYPE
                        Type of EC2 instance for core (or "slave") nodes only
  --ec2-instance-type=EC2_INSTANCE_TYPE
                        Type of EC2 instance(s) to launch (e.g. m1.small,
                        c1.xlarge, m2.xlarge). See http://aws.amazon.com/ec2
                        /instance-types/ for the full list.
  --ec2-key-pair=EC2_KEY_PAIR
                        Name of the SSH key pair you set up for EMR
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
  --emr-api-param=EMR_API_PARAMS
                        Additional parameters to pass directly to the EMR API
                        when creating a cluster. Should take the form
                        KEY=VALUE. You can use --emr-api-param multiple times.
  --emr-endpoint=EMR_ENDPOINT
                        Optional host to connect to when communicating with S3
                        (e.g. us-west-1.elasticmapreduce.amazonaws.com).
                        Default is to infer this from aws_region.
  --pool-name=POOL_NAME
                        Specify a pool name to join. Set to "default" if not
                        specified.
  --disable-emr-debugging
                        Disable storage of Hadoop logs in SimpleDB
  --enable-emr-debugging
                        Enable storage of Hadoop logs in SimpleDB
  --iam-instance-profile=IAM_INSTANCE_PROFILE
                        EC2 instance profile to use for the EMR cluster - see
                        "Configure IAM Roles for Amazon EMR" in AWS docs
  --iam-service-role=IAM_SERVICE_ROLE
                        IAM service role to use for the EMR cluster - see
                        "Configure IAM Roles for Amazon EMR" in AWS docs
  --label=LABEL         custom prefix for job name, to help us identify the
                        job
  --max-hours-idle=MAX_HOURS_IDLE
                        If we create a persistent cluster, have it
                        automatically terminate itself after it's been idle
                        this many hours.
  --mins-to-end-of-hour=MINS_TO_END_OF_HOUR
                        If --max-hours-idle is set, control how close to the
                        end of an EC2 billing hour the cluster can
                        automatically terminate itself (default is 5 minutes).
  --no-emr-api-param=NO_EMR_API_PARAMS
                        Parameters to be unset when calling EMR API. You can
                        use --no-emr-api-param multiple times.
  --num-ec2-core-instances=NUM_EC2_CORE_INSTANCES
                        Number of EC2 instances to start as core (or "slave")
                        nodes. Incompatible with --num-ec2-instances.
  --num-ec2-instances=NUM_EC2_INSTANCES
                        Total number of EC2 instances to launch
  --num-ec2-task-instances=NUM_EC2_TASK_INSTANCES
                        Number of EC2 instances to start as task nodes.
                        Incompatible with --num-ec2-instances.
  --owner=OWNER         custom username to use, to help us identify who ran
                        the job
  --no-pool-clusters
                        Don't try to run our job on a pooled cluster.
  --pool-clusters       Add to an existing cluster or create a new one that
                        does not terminate when the job completes. Overrides
                        other cluster-related options including EC2 instance
                        configuration. Joins pool "default" if
                        --pool-name is not specified. WARNING: do
                        not run this without
                        mrjob terminate-idle-clusters in your
                        crontab; clusters left idle can quickly become
                        expensive!
  -q, --quiet           Don't print anything to stderr
  --s3-endpoint=S3_ENDPOINT
                        Host to connect to when communicating with S3 (e.g. s3
                        -us-west-1.amazonaws.com). Default is to infer this
                        from region (see --aws-region).
  --s3-log-uri=S3_LOG_URI
                        URI on S3 to write logs into
  --s3-scratch-uri=S3_SCRATCH_URI
                        URI on S3 to use as our temp directory.
  --s3-sync-wait-time=S3_SYNC_WAIT_TIME
                        How long to wait for S3 to reach eventual consistency.
                        This is typically less than a second (zero in us-west)
                        but the default is 5.0 to be safe.
  --s3-upload-part-size=S3_UPLOAD_PART_SIZE
                        Upload files to S3 in parts no bigger than this many
                        megabytes. Default is 100 MiB. Set to 0 to disable
                        multipart uploading entirely.
  -v, --verbose         print more messages to stderr
  --visible-to-all-users
                        Whether the cluster is visible to all IAM users of
                        the AWS account associated with the cluster. If this
                        value is set to True, all IAM users of that AWS
                        account can view and (if they have the proper policy
                        permissions set) manage the cluster. If it is set to
                        False, only the IAM user that created the cluster can
                        view and manage it. This option can be overridden by
                        --emr-api-param VisibleToAllUsers=true|false.
"""
from __future__ import print_function

from optparse import OptionParser

from mrjob.emr import EMRJobRunner
from mrjob.job import MRJob
from mrjob.options import _add_basic_opts
from mrjob.options import _add_dataproc_emr_opts
from mrjob.options import _add_emr_connect_opts
from mrjob.options import _add_emr_launch_opts
from mrjob.options import _alphabetize_options
from mrjob.options import _fix_custom_options
from mrjob.util import scrape_options_into_new_groups


def main(args=None):
    """Run the create_cluster tool with arguments from ``sys.argv`` and
    printing to ``sys.stdout``."""
    runner = EMRJobRunner(**_runner_kwargs(args))
    cluster_id = runner.make_persistent_cluster()
    print(cluster_id)


def _runner_kwargs(cl_args=None):
    """Parse command line arguments into arguments for
    :py:class:`EMRJobRunner`
    """
    # parser command-line args
    option_parser = _make_option_parser()
    options, args = option_parser.parse_args(cl_args)

    # fix emr_api_params and emr_tags
    _fix_custom_options(options, option_parser)

    if args:
        option_parser.error('takes no arguments')

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)

    # create the persistent job
    kwargs = options.__dict__.copy()

    del kwargs['quiet']
    del kwargs['verbose']
    del kwargs['no_emr_api_params']

    return kwargs


def _make_option_parser():
    usage = '%prog [options]'
    description = (
        'Create a persistent EMR cluster to run jobs in, and print its ID to'
        ' stdout. WARNING: Do not run'
        ' this without mrjob terminate-idle-clusters in your'
        ' crontab; clusters left idle can quickly become expensive!')
    option_parser = OptionParser(usage=usage, description=description)

    _add_basic_opts(option_parser)
    # these aren't nicely broken down, just scrape specific options
    scrape_options_into_new_groups(MRJob().all_option_groups(), {
        option_parser: (
            'bootstrap_mrjob',
            'label',
            'owner',
        ),
    })

    _add_emr_connect_opts(option_parser)
    _add_emr_launch_opts(option_parser)
    _add_dataproc_emr_opts(option_parser)

    _alphabetize_options(option_parser)
    return option_parser


if __name__ == '__main__':
    main()
