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
  --no-bootstrap-python
                        Don't automatically try to install a compatible
                        version of Python at bootstrap time.
  --bootstrap-python    Attempt to install a compatible version of Python at
                        bootstrap time. Currently this only does anything for
                        Python 3, for which it is enabled by default.
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
  --check-cluster-every=CHECK_CLUSTER_EVERY
                        How often (in seconds) to check status of your
                        job/cluster
  --s3-sync-wait-time=CLOUD_FS_SYNC_SECS
                        Deprecated alias for --cloud-fs-sync-secs
  --cloud-fs-sync-secs=CLOUD_FS_SYNC_SECS
                        How long to wait for remote FS to reach eventual
                        consistency. This is typically less than a second but
                        the default is 5.0 to be safe.
  --cloud-log-dir=CLOUD_LOG_DIR
                        URI on remote FS to write logs into
  --s3-log-uri=CLOUD_LOG_DIR
                        Deprecated alias for --cloud-log-dir
  --s3-scratch-uri=CLOUD_TMP_DIR
                        Deprecated alias for --cloud-tmp-dir
  --s3-tmp-dir=CLOUD_TMP_DIR
                        Deprecated alias for --cloud-tmp-dir
  --cloud-tmp-dir=CLOUD_TMP_DIR
                        URI on remote FS to use as our temp directory.
  --cloud-upload-part-size=CLOUD_UPLOAD_PART_SIZE
                        Upload files to S3 in parts no bigger than this many
                        megabytes. Default is 100 MiB. Set to 0 to disable
                        multipart uploading entirely.
  --s3-upload-part-size=CLOUD_UPLOAD_PART_SIZE
                        Deprecated alias for --cloud-upload-part-size
  --cluster-id=CLUSTER_ID
                        ID of an existing cluster to run our job on
  -c CONF_PATHS, --conf-path=CONF_PATHS
                        Path to alternate mrjob.conf file to read from
  --no-conf             Don't load mrjob.conf even if it's available
  --core-instance-bid-price=CORE_INSTANCE_BID_PRICE
                        Bid price to specify for core nodes when setting them
                        up as EC2 spot instances (you probably only want to
                        set a bid price for task instances).
  --ec2-core-instance-bid-price=CORE_INSTANCE_BID_PRICE
                        Deprecated alias for --core-instance-bid-price
  --ec2-core-instance-type=CORE_INSTANCE_TYPE
                        Deprecated alias for --core-instance-type
  --ec2-slave-instance-type=CORE_INSTANCE_TYPE
                        Deprecated alias for --core-instance-type
  --core-instance-type=CORE_INSTANCE_TYPE
                        Type of GCE/EC2 core instance(s) to launch
  --ec2-key-pair=EC2_KEY_PAIR
                        Name of the SSH key pair you set up for EMR
  --emr-api-param=EMR_API_PARAMS
                        Additional parameters to pass directly to the EMR API
                        when creating a cluster. Should take the form
                        KEY=VALUE. You can use --emr-api-param multiple times.
  --emr-application=EMR_APPLICATIONS
                        Additional applications to run on 4.x AMIs (e.g.
                        Ganglia, Mahout, Spark)
  --emr-configuration=EMR_CONFIGURATIONS
                        Configuration to use on 4.x AMIs as a JSON-encoded
                        dict; see http://docs.aws.amazon.com/ElasticMapReduce/
                        latest/ReleaseGuide/emr-configure-apps.html for
                        examples.
  --emr-endpoint=EMR_ENDPOINT
                        Force mrjob to connect to EMR on this endpoint (e.g.
                        us-west-1.elasticmapreduce.amazonaws.com). Default is
                        to infer this from region.
  --disable-emr-debugging
                        Disable storage of Hadoop logs in SimpleDB
  --enable-emr-debugging
                        Enable storage of Hadoop logs in SimpleDB
  --iam-endpoint=IAM_ENDPOINT
                        Force mrjob to connect to IAM on this endpoint (e.g.
                        iam.us-gov.amazonaws.com)
  --iam-instance-profile=IAM_INSTANCE_PROFILE
                        EC2 instance profile to use for the EMR cluster - see
                        "Configure IAM Roles for Amazon EMR" in AWS docs
  --iam-service-role=IAM_SERVICE_ROLE
                        IAM service role to use for the EMR cluster -- see
                        "Configure IAM Roles for Amazon EMR" in AWS docs
  --ami-version=IMAGE_VERSION
                        Deprecated alias for --image-version
  --image-version=IMAGE_VERSION
                        EMR/Dataproc machine image to launch clusters with
  --ec2-instance-type=INSTANCE_TYPE
                        Deprecated alias for --instance-type
  --instance-type=INSTANCE_TYPE
                        Type of GCE/EC2 instance(s) to launch   GCE - e.g.
                        n1-standard-1, n1-highcpu-4, n1-highmem-4 - See
                        https://cloud.google.com/compute/docs/machine-types
                        EC2 - e.g. m1.medium, c3.xlarge, r3.xlarge - See
                        http://aws.amazon.com/ec2/instance-types/
  --label=LABEL         alternate label for the job, to help us identify it
  --master-instance-bid-price=MASTER_INSTANCE_BID_PRICE
                        Bid price to specify for the master node when setting
                        it up as an EC2 spot instance (you probably only want
                        to set a bid price for task instances).
  --ec2-master-instance-bid-price=MASTER_INSTANCE_BID_PRICE
                        Deprecated alias for --master-instance-bid-price
  --ec2-master-instance-type=MASTER_INSTANCE_TYPE
                        Deprecated alias for --master-instance-type
  --master-instance-type=MASTER_INSTANCE_TYPE
                        Type of GCE/EC2 master instance(s) to launch
  --max-hours-idle=MAX_HOURS_IDLE
                        If we create a cluster, have it automatically
                        terminate itself after it's been idle this many hours.
  --mins-to-end-of-hour=MINS_TO_END_OF_HOUR
                        If --max-hours-idle is set, control how close to the
                        end of an hour the cluster can automatically terminate
                        itself (default is 5 minutes).
  --no-emr-api-param=NO_EMR_API_PARAMS
                        Parameters to be unset when calling EMR API. You can
                        use --no-emr-api-param multiple times.
  --num-ec2-core-instances=NUM_CORE_INSTANCES
                        Deprecated alias for --num-core-instances
  --num-core-instances=NUM_CORE_INSTANCES
                        Total number of Worker instances to launch
  --num-ec2-instances=NUM_EC2_INSTANCES
                        Deprecated: subtract one and pass that to --num-core-
                        instances instead
  --num-ec2-task-instances=NUM_TASK_INSTANCES
                        Deprecated alias for --num-task-instances
  --num-task-instances=NUM_TASK_INSTANCES
                        Total number of preemptible Worker instances to launch
  --owner=OWNER         user who ran the job (if different from the current
                        user)
  --no-pool-clusters    Don't run our job on a pooled cluster (the default).
  --no-pool-emr-job-flows
                        Deprecated alias for --no-pool-clusters
  --pool-clusters       Add to an existing cluster or create a new one that
                        does not terminate when the job completes. Overrides
                        other  cluster-related options including EC2 instance
                        configuration. Joins pool "default" if --pool-name is
                        not specified. WARNING: do not run this without mrjob
                        terminate-idle-clusters in your crontab; clusters left
                        idle can quickly become expensive!
  --pool-emr-job-flows  Deprecated alias for --pool-clusters
  --pool-name=POOL_NAME
                        Specify a pool name to join. Set to "default" if not
                        specified.
  -q, --quiet           Don't print anything to stderr
  --aws-region=REGION   Deprecated alias for --region
  --region=REGION       GCE/AWS region to run Dataproc/EMR jobs in.
  --release-label=RELEASE_LABEL
                        Release Label (e.g. "emr-4.0.0"). Overrides --image-
                        version
  --s3-endpoint=S3_ENDPOINT
                        Force mrjob to connect to S3 on this endpoint (e.g. s3
                        -us-west-1.amazonaws.com). You usually shouldn't set
                        this; by default mrjob will choose the correct
                        endpoint for each S3 bucket based on its location.
  --subnet=SUBNET       ID of Amazon VPC subnet to launch cluster in (if not
                        set or empty string, cluster is launched in the normal
                        AWS cloud)
  --emr-tag=TAGS        Deprecated alias for --tag
  --tag=TAGS            Metadata tags to apply to the EMR cluster; should take
                        the form KEY=VALUE. You can use --tag multiple times.
  --task-instance-bid-price=TASK_INSTANCE_BID_PRICE
                        Bid price to specify for task nodes when setting them
                        up as EC2 spot instances.
  --ec2-task-instance-bid-price=TASK_INSTANCE_BID_PRICE
                        Deprecated alias for --task-instance-bid-price
  --ec2-task-instance-type=TASK_INSTANCE_TYPE
                        Deprecated alias for --task-instance-type
  --task-instance-type=TASK_INSTANCE_TYPE
                        Type of GCE/EC2 task instance(s) to launch
  -v, --verbose         print more messages to stderr
  --visible-to-all-users
                        Make your cluster is visible to all IAM users on the
                        same AWS account (the default).
  --no-visible-to-all-users
                        Hide your cluster from other IAM users on the same AWS
                        account.
  --aws-availability-zone=ZONE
                        Deprecated alias for --zone
  --zone=ZONE           GCE zone/AWS availability zone to run Dataproc/EMR
                        jobs in.
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

    # fix emr_api_params and tags
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
