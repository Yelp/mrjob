# Copyright 2009-2013 Yelp and Contributors
# Copyright 2015-2017 Yelp
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

Usage::

    mrjob create-cluster

Options::

  --additional-emr-info ADDITIONAL_EMR_INFO
                        A JSON string for selecting additional features on EMR
  --application APPLICATIONS
                        Additional applications to run on 4.x AMIs (e.g.
                        Ganglia, Mahout, Spark)
  --bootstrap BOOTSTRAP
                        A shell command to set up libraries etc. before any
                        steps (e.g. "sudo apt-get -qy install python3"). You
                        may interpolate files available via URL or locally
                        with Hadoop Distributed Cache syntax ("sudo yum
                        install -y foo.rpm#")
  --bootstrap-action BOOTSTRAP_ACTIONS
                        Raw bootstrap action scripts to run before any of the
                        other bootstrap steps. You can use --bootstrap-action
                        more than once. Local scripts will be automatically
                        uploaded to S3. To add arguments, just use quotes:
                        "foo.sh arg1 arg2"
  --bootstrap-mrjob     Automatically zip up the mrjob library and install it
                        when we run the mrjob. This is the default. Use --no-
                        bootstrap-mrjob if you've already installed mrjob on
                        your Hadoop cluster.
  --no-bootstrap-mrjob  Don't automatically zip up the mrjob library and
                        install it when we run this job. Use this if you've
                        already installed mrjob on your Hadoop cluster.
  --bootstrap-python    Attempt to install a compatible version of Python at
                        bootstrap time. Currently this only does anything for
                        Python 3, for which it is enabled by default.
  --no-bootstrap-python
                        Don't automatically try to install a compatible
                        version of Python at bootstrap time.
  --bootstrap-spark     Auto-install Spark on the cluster (even if not
                        needed).
  --no-bootstrap-spark  Don't auto-install Spark on the cluster.
  --cloud-fs-sync-secs CLOUD_FS_SYNC_SECS
                        How long to wait for remote FS to reach eventual
                        consistency. This is typically less than a second but
                        the default is 5.0 to be safe.
  --cloud-log-dir CLOUD_LOG_DIR
                        URI on remote FS to write logs into
  --cloud-tmp-dir CLOUD_TMP_DIR
                        URI on remote FS to use as our temp directory.
  --cloud-upload-part-size CLOUD_UPLOAD_PART_SIZE
                        Upload files to S3 in parts no bigger than this many
                        megabytes. Default is 100 MiB. Set to 0 to disable
                        multipart uploading entirely.
  -c CONF_PATHS, --conf-path CONF_PATHS
                        Path to alternate mrjob.conf file to read from
  --no-conf             Don't load mrjob.conf even if it's available
  --core-instance-bid-price CORE_INSTANCE_BID_PRICE
                        Bid price to specify for core nodes when setting them
                        up as EC2 spot instances (you probably only want to do
                        this for task instances).
  --core-instance-type CORE_INSTANCE_TYPE
                        Type of GCE/EC2 core instance(s) to launch
  --ec2-key-pair EC2_KEY_PAIR
                        Name of the SSH key pair you set up for EMR
  --emr-api-param EMR_API_PARAMS
                        deprecated. Use --extra-cluster-param instead
  --no-emr-api-param EMR_API_PARAMS
                        deprecated. Use --extra-cluster-param instead
  --emr-configuration EMR_CONFIGURATIONS
                        Configuration to use on 4.x AMIs as a JSON-encoded
                        dict; see http://docs.aws.amazon.com/ElasticMapReduce/
                        latest/ReleaseGuide/emr-configure-apps.html for
                        examples
  --emr-endpoint EMR_ENDPOINT
                        Force mrjob to connect to EMR on this endpoint (e.g.
                        us-west-1.elasticmapreduce.amazonaws.com). Default is
                        to infer this from region.
  --enable-emr-debugging
                        Enable storage of Hadoop logs in SimpleDB
  --disable-emr-debugging
                        Disable storage of Hadoop logs in SimpleDB (the
                        default)
  --extra-cluster-param EXTRA_CLUSTER_PARAMS
                        extra parameter to pass to cloud API when creating a
                        cluster, to access features not currently supported by
                        mrjob. Takes the form <param>=<value>, where value is
                        JSON or a string. Use <param>=null to unset a
                        parameter
  -h, --help            show this help message and exit
  --iam-endpoint IAM_ENDPOINT
                        Force mrjob to connect to IAM on this endpoint (e.g.
                        iam.us-gov.amazonaws.com)
  --iam-instance-profile IAM_INSTANCE_PROFILE
                        EC2 instance profile to use for the EMR cluster -- see
                        "Configure IAM Roles for Amazon EMR" in AWS docs
  --iam-service-role IAM_SERVICE_ROLE
                        IAM service role to use for the EMR cluster -- see
                        "Configure IAM Roles for Amazon EMR" in AWS docs
  --image-version IMAGE_VERSION
                        EMR/Dataproc machine image to launch clusters with
  --instance-fleets INSTANCE_FLEETS
                        detailed JSON list of instance fleets, including EBS
                        configuration. See docs for --instance-fleets at
                        http://docs.aws.amazon.com/cli/latest/reference/emr
                        /create-cluster.html
  --instance-groups INSTANCE_GROUPS
                        detailed JSON list of instance configs, including EBS
                        configuration. See docs for --instance-groups at
                        http://docs.aws.amazon.com/cli/latest/reference/emr
                        /create-cluster.html
  --instance-type INSTANCE_TYPE
                        Type of GCE/EC2 instance(s) to launch GCE - e.g.
                        n1-standard-1, n1-highcpu-4, n1-highmem-4 -- See
                        https://cloud.google.com/compute/docs/machine-types
                        EC2 - e.g. m1.medium, c3.xlarge, r3.xlarge -- See
                        http://aws.amazon.com/ec2/instance-types/
  --label LABEL         Alternate label for the job, to help us identify it.
  --master-instance-bid-price MASTER_INSTANCE_BID_PRICE
                        Bid price to specify for the master node when setting
                        it up as an EC2 spot instance (you probably only want
                        to do this for task instances).
  --master-instance-type MASTER_INSTANCE_TYPE
                        Type of GCE/EC2 master instance to launch
  --max-hours-idle MAX_HOURS_IDLE
                        Please use --max-mins-idle instead
  --max-mins-idle MAX_MINS_IDLE
                        If we create a cluster, have it automatically
                        terminate itself after it's been idle this many
                        minutes
  --mins-to-end-of-hour MINS_TO_END_OF_HOUR
                        If --max-mins-idle is set, control how close to the
                        end of an hour the cluster can automatically terminate
                        itself (default is 5 minutes)
  --num-core-instances NUM_CORE_INSTANCES
                        Total number of core instances to launch
  --num-task-instances NUM_TASK_INSTANCES
                        Total number of task instances to launch
  --owner OWNER         User who ran the job (default is the current user)
  --pool-clusters       Add to an existing cluster or create a new one that
                        does not terminate when the job completes.
  --no-pool-clusters    Don't run job on a pooled cluster (the default)
  --pool-name POOL_NAME
                        Specify a pool name to join. Default is "default"
  -q, --quiet           Don't print anything to stderr
  --region REGION       GCE/AWS region to run Dataproc/EMR jobs in.
  --release-label RELEASE_LABEL
                        Release Label (e.g. "emr-4.0.0"). Overrides --image-
                        version
  --s3-endpoint S3_ENDPOINT
                        Force mrjob to connect to S3 on this endpoint (e.g. s3
                        -us-west-1.amazonaws.com). You usually shouldn't set
                        this; by default mrjob will choose the correct
                        endpoint for each S3 bucket based on its location.
  --subnet SUBNET       ID of Amazon VPC subnet to launch cluster in. If not
                        set or empty string, cluster is launched in the normal
                        AWS cloud.
  --subnets SUBNET      Like --subnets, but with a comma-separated list, to
                        specify multiple subnets in conjunction with
                        --instance-fleets
  --tag TAGS            Metadata tags to apply to the EMR cluster; should take
                        the form KEY=VALUE. You can use --tag multiple times
  --task-instance-bid-price TASK_INSTANCE_BID_PRICE
                        Bid price to specify for task nodes when setting them
                        up as EC2 spot instances
  --task-instance-type TASK_INSTANCE_TYPE
                        Type of GCE/EC2 task instance(s) to launch
  -v, --verbose         print more messages to stderr
  --visible-to-all-users
                        Make your cluster is visible to all IAM users on the
                        same AWS account (the default)
  --no-visible-to-all-users
                        Hide your cluster from other IAM users on the same AWS
                        account
  --zone ZONE           GCE zone/AWS availability zone to run Dataproc/EMR
                        jobs in.
"""
from __future__ import print_function

from argparse import ArgumentParser

from mrjob.emr import EMRJobRunner
from mrjob.job import MRJob
from mrjob.options import _add_basic_args
from mrjob.options import _add_runner_args
from mrjob.options import _alphabetize_actions
from mrjob.options import _filter_by_role


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
    arg_parser = _make_arg_parser()
    options = arg_parser.parse_args(cl_args)

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)

    # create the persistent job
    kwargs = options.__dict__.copy()

    del kwargs['quiet']
    del kwargs['verbose']

    return kwargs


def _make_arg_parser():
    usage = '%(prog)s [options]'
    description = (
        'Create a persistent EMR cluster to run jobs in, and print its ID to'
        ' stdout. WARNING: Do not run'
        ' this without mrjob terminate-idle-clusters in your'
        ' crontab; clusters left idle can quickly become expensive!')
    arg_parser = ArgumentParser(usage=usage, description=description)

    _add_basic_args(arg_parser)
    _add_runner_args(
        arg_parser,
        _filter_by_role(EMRJobRunner.OPT_NAMES, 'connect', 'launch'))

    _alphabetize_actions(arg_parser)

    return arg_parser


if __name__ == '__main__':
    main()
