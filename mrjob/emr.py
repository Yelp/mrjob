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
from __future__ import with_statement

from collections import defaultdict
from datetime import datetime
from datetime import timedelta
import fnmatch
import logging
import os
import posixpath
import random
import re
import shlex
import signal
import socket
from subprocess import Popen
from subprocess import PIPE
import time
import urllib2

try:
    from cStringIO import StringIO
    StringIO  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    from StringIO import StringIO

try:
    import simplejson as json  # preferred because of C speedups
    json  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import json  # built in to Python 2.6 and later

try:
    import boto
    import boto.ec2
    import boto.emr
    import boto.exception
    import boto.utils
    from mrjob import boto_2_1_1_83aae37b
    boto  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    # don't require boto; MRJobs don't actually need it when running
    # inside hadoop streaming
    boto = None

import mrjob
from mrjob import compat
from mrjob.conf import combine_cmds
from mrjob.conf import combine_dicts
from mrjob.conf import combine_lists
from mrjob.conf import combine_paths
from mrjob.conf import combine_path_lists
from mrjob.logparsers import TASK_ATTEMPTS_LOG_URI_RE
from mrjob.logparsers import STEP_LOG_URI_RE
from mrjob.logparsers import EMR_JOB_LOG_URI_RE
from mrjob.logparsers import NODE_LOG_URI_RE
from mrjob.logparsers import scan_for_counters_in_files
from mrjob.logparsers import scan_logs_in_order
from mrjob.parse import is_s3_uri
from mrjob.parse import parse_s3_uri
from mrjob.pool import est_time_to_hour
from mrjob.pool import pool_hash_and_name
from mrjob.retry import RetryWrapper
from mrjob.runner import MRJobRunner
from mrjob.runner import GLOB_RE
from mrjob.ssh import ssh_cat
from mrjob.ssh import ssh_ls
from mrjob.ssh import ssh_copy_key
from mrjob.ssh import ssh_slave_addresses
from mrjob.ssh import SSHException
from mrjob.ssh import SSH_PREFIX
from mrjob.ssh import SSH_LOG_ROOT
from mrjob.ssh import SSH_URI_RE
from mrjob.util import buffer_iterator_to_line_iterator
from mrjob.util import cmd_line
from mrjob.util import extract_dir_for_tar
from mrjob.util import hash_object
from mrjob.util import read_file


log = logging.getLogger('mrjob.emr')

JOB_TRACKER_RE = re.compile('(\d{1,3}\.\d{2})%')

# if EMR throttles us, how long to wait (in seconds) before trying again?
EMR_BACKOFF = 20
EMR_BACKOFF_MULTIPLIER = 1.5
EMR_MAX_TRIES = 20  # this takes about a day before we run out of tries

# the port to tunnel to
EMR_JOB_TRACKER_PORT = 9100
EMR_JOB_TRACKER_PATH = '/jobtracker.jsp'

MAX_SSH_RETRIES = 20

# ssh should fail right away if it can't bind a port
WAIT_FOR_SSH_TO_FAIL = 1.0

# sometimes AWS gives us seconds as a decimal, which we can't parse
# with boto.utils.ISO8601
SUBSECOND_RE = re.compile('\.[0-9]+')

# map from AWS region to EMR endpoint. See
# http://docs.amazonwebservices.com/ElasticMapReduce/latest/DeveloperGuide/index.html?ConceptsRequestEndpoints.html
REGION_TO_EMR_ENDPOINT = {
    'EU': 'eu-west-1.elasticmapreduce.amazonaws.com',
    'us-east-1': 'us-east-1.elasticmapreduce.amazonaws.com',
    'us-west-1': 'us-west-1.elasticmapreduce.amazonaws.com',
    '': 'elasticmapreduce.amazonaws.com',  # when no region specified
}

# map from AWS region to S3 endpoint. See
# http://docs.amazonwebservices.com/AmazonS3/latest/dev/MakingRequests.html#RequestEndpoints
REGION_TO_S3_ENDPOINT = {
    'EU': 's3-eu-west-1.amazonaws.com',
    'us-east-1': 's3.amazonaws.com',  # no region-specific endpoint
    'us-west-1': 's3-us-west-1.amazonaws.com',
    'ap-southeast-1': 's3-ap-southeast-1.amazonaws.com',  # no EMR endpoint yet
    '': 's3.amazonaws.com',
}

# map from AWS region to S3 LocationConstraint parameter for regions whose
# location constraints differ from their AWS regions. See
# http://docs.amazonwebservices.com/AmazonS3/latest/API/index.html?RESTBucketPUT.html
REGION_TO_S3_LOCATION_CONSTRAINT = {
    'us-east-1': '',
}


# map from instance type to number of compute units
# from http://aws.amazon.com/ec2/instance-types/
EC2_INSTANCE_TYPE_TO_COMPUTE_UNITS = {
    't1.micro': 2,
    'm1.small': 1,
    'm1.large': 4,
    'm1.xlarge': 8,
    'm2.xlarge': 6.5,
    'm2.2xlarge': 13,
    'm2.4xlarge': 26,
    'c1.medium': 5,
    'c1.xlarge': 20,
    'cc1.4xlarge': 33.5,
    'cg1.4xlarge': 33.5,
}


# map from instance type to GB of memory
# from http://aws.amazon.com/ec2/instance-types/
EC2_INSTANCE_TYPE_TO_MEMORY = {
    't1.micro': 0.6,
    'm1.small': 1.7,
    'm1.large': 7.5,
    'm1.xlarge': 15,
    'm2.xlarge': 17.5,
    'm2.2xlarge': 34.2,
    'm2.4xlarge': 68.4,
    'c1.medium': 1.7,
    'c1.xlarge': 7,
    'cc1.4xlarge': 23,
    'cg1.4xlarge': 22,
}

# Use this to figure out which hadoop version we're using if it's not
# explicitly specified, so we can keep from passing deprecated command-line
# options to Hadoop. If we encounter an AMI version we don't recognize,
# we use whatever version matches 'latest'.
#
# The reason we don't just create a job flow and then query its Hadoop version
# is that for most jobs, we create the steps and the job flow at the same time.
AMI_VERSION_TO_HADOOP_VERSION = {
    None: '0.18',  # ami_version not specified means version 1.0
    '1.0': '0.18',
    '2.0': '0.20.205',
    'latest': '0.20.205',
}

# EMR's hard limit on number of steps in a job flow
MAX_STEPS_PER_JOB_FLOW = 256


def s3_key_to_uri(s3_key):
    """Convert a boto Key object into an ``s3://`` URI"""
    return 's3://%s/%s' % (s3_key.bucket.name, s3_key.name)


# AWS actually gives dates in two formats, and we only recently started using
# API calls that return the second. So the date parsing function is called
# iso8601_to_*, but it also parses RFC1123.
# Until boto starts seamlessly parsing these, we check for them ourselves.

# Thu, 29 Mar 2012 04:55:44 GMT
RFC1123 = '%a, %d %b %Y %H:%M:%S %Z'

def iso8601_to_timestamp(iso8601_time):
    iso8601_time = SUBSECOND_RE.sub('', iso8601_time)
    try:
        return time.mktime(time.strptime(iso8601_time, boto.utils.ISO8601))
    except ValueError:
        return time.mktime(time.strptime(iso8601_time, RFC1123))


def iso8601_to_datetime(iso8601_time):
    iso8601_time = SUBSECOND_RE.sub('', iso8601_time)
    try:
        return datetime.strptime(iso8601_time, boto.utils.ISO8601)
    except ValueError:
        return datetime.strptime(iso8601_time, RFC1123)


def describe_all_job_flows(emr_conn, states=None, jobflow_ids=None,
                           created_after=None, created_before=None):
    """Iteratively call ``EmrConnection.describe_job_flows()`` until we really
    get all the available job flow information. Currently, 2 months of data
    is available through the EMR API.

    This is a way of getting around the limits of the API, both on number
    of job flows returned, and how far back in time we can go.

    :type states: list
    :param states: A list of strings with job flow states wanted
    :type jobflow_ids: list
    :param jobflow_ids: A list of job flow IDs
    :type created_after: datetime
    :param created_after: Bound on job flow creation time
    :type created_before: datetime
    :param created_before: Bound on job flow creation time
    """
    all_job_flows = []
    ids_seen = set()

    # weird things can happen if we send no args the DescribeJobFlows API
    # (see Issue #346), so if nothing else is set, set created_before
    # to a day in the future.
    if not (states or jobflow_ids or created_after or created_before):
        created_before = datetime.utcnow() + timedelta(days=1)

    while True:
        if created_before and created_after and created_before < created_after:
            break

        log.debug('Calling describe_jobflows(states=%r, jobflow_ids=%r,'
                  ' created_after=%r, created_before=%r)' %
                  (states, jobflow_ids, created_after, created_before))
        try:
            results = emr_conn.describe_jobflows(
                states=states, jobflow_ids=jobflow_ids,
                created_after=created_after, created_before=created_before)
        except boto.exception.BotoServerError, ex:
            if 'ValidationError' in ex.body:
                log.debug(
                    '  reached earliest allowed created_before time, done!')
                break
            else:
                raise

        # don't count the same job flow twice
        job_flows = [jf for jf in results if jf.jobflowid not in ids_seen]
        log.debug('  got %d results (%d new)' % (len(results), len(job_flows)))

        all_job_flows.extend(job_flows)
        ids_seen.update(jf.jobflowid for jf in job_flows)

        if job_flows:
            # set created_before to be just after the start time of
            # the first job returned, to deal with job flows started
            # in the same second
            min_create_time = min(iso8601_to_datetime(jf.creationdatetime)
                                  for jf in job_flows)
            created_before = min_create_time + timedelta(seconds=1)
            # if someone managed to start 501 job flows in the same second,
            # they are still screwed (the EMR API only returns up to 500),
            # but this seems unlikely. :)
        else:
            if not created_before:
                created_before = datetime.utcnow()
            created_before -= timedelta(weeks=2)

    return all_job_flows


def make_lock_uri(s3_tmp_uri, emr_job_flow_id, step_num):
    """Generate the URI to lock the job flow ``emr_job_flow_id``"""
    return s3_tmp_uri + 'locks/' + emr_job_flow_id + '/' + str(step_num)


def _lock_acquire_step_1(s3_conn, lock_uri, job_name, mins_to_expiration=None):
    bucket_name, key_prefix = parse_s3_uri(lock_uri)
    bucket = s3_conn.get_bucket(bucket_name)
    key = bucket.get_key(key_prefix)

    # EMRJobRunner should start using a job flow within about a second of
    # locking it, so if it's been a while, then it probably crashed and we
    # can just use this job flow.
    key_expired = False
    if key and mins_to_expiration is not None:
        last_modified = iso8601_to_datetime(key.last_modified)
        age = datetime.utcnow() - last_modified
        if age > timedelta(minutes=mins_to_expiration):
            key_expired = True

    if key is None or key_expired:
        key = bucket.new_key(key_prefix)
        key.set_contents_from_string(job_name)
        return key
    else:
        return None


def _lock_acquire_step_2(key, job_name):
    key_value = key.get_contents_as_string()
    return (key_value == job_name)


def attempt_to_acquire_lock(s3_conn, lock_uri, sync_wait_time, job_name,
                            mins_to_expiration=None):
    """Returns True if this session successfully took ownership of the lock
    specified by ``lock_uri``.
    """
    key = _lock_acquire_step_1(s3_conn, lock_uri, job_name, mins_to_expiration)
    if key is not None:
        time.sleep(sync_wait_time)
        success = _lock_acquire_step_2(key, job_name)
        if success:
            return True

    return False


class LogFetchError(Exception):
    pass


class EMRJobRunner(MRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` on Amazon Elastic MapReduce.

    :py:class:`EMRJobRunner` runs your job in an EMR job flow, which is
    basically a temporary Hadoop cluster. Normally, it creates a job flow
    just for your job; it's also possible to run your job in a specific
    job flow by setting *emr_job_flow_id* or to automatically choose a
    waiting job flow, creating one if none exists, by setting
    *pool_emr_job_flows*.

    Input, support, and jar files can be either local or on S3; use
    ``s3://...`` URLs to refer to files on S3.

    This class has some useful utilities for talking directly to S3 and EMR,
    so you may find it useful to instantiate it without a script::

        from mrjob.emr import EMRJobRunner

        emr_conn = EMRJobRunner().make_emr_conn()
        job_flows = emr_conn.describe_jobflows()
        ...

    See also: :py:meth:`~EMRJobRunner.__init__`.
    """
    alias = 'emr'

    def __init__(self, **kwargs):
        """:py:class:`~mrjob.emr.EMRJobRunner` takes the same arguments as
        :py:class:`~mrjob.runner.MRJobRunner`, plus some additional options
        which can be defaulted in :ref:`mrjob.conf <mrjob.conf>`.

        *aws_access_key_id* and *aws_secret_access_key* are required if you
        haven't set them up already for boto (e.g. by setting the environment
        variables :envvar:`AWS_ACCESS_KEY_ID` and
        :envvar:`AWS_SECRET_ACCESS_KEY`)

        Additional options:

        :type additional_emr_info: JSON str, None, or JSON-encodable object
        :param additional_emr_info: Special parameters to select additional
                                    features, mostly to support beta EMR
                                    features. Pass a JSON string on the command
                                    line or use data structures in the config
                                    file (which is itself basically JSON).
        :type ami_version: str
        :param ami_version: EMR AMI version to use. This controls which Hadoop
                            version(s) are available and which version of
                            Python is installed, among other things; see \
http://docs.amazonwebservices.com/ElasticMapReduce/latest/DeveloperGuideindex.html?EnvironmentConfig_AMIVersion.html
                            for details. Implicitly defaults to AMI version
                            1.0 (this will change to 2.0 in mrjob v0.4).
        :type aws_access_key_id: str
        :param aws_access_key_id: "username" for Amazon web services.
        :type aws_availability_zone: str
        :param aws_availability_zone: availability zone to run the job in
        :type aws_secret_access_key: str
        :param aws_secret_access_key: your "password" on AWS
        :type aws_region: str
        :param aws_region: region to connect to S3 and EMR on (e.g.
                           ``us-west-1``). If you want to use separate regions
                           for S3 and EMR, set *emr_endpoint* and
                           *s3_endpoint*.
        :type bootstrap_actions: list of str
        :param bootstrap_actions: a list of raw bootstrap actions (essentially
                                  scripts) to run prior to any of the other
                                  bootstrap steps. Any arguments should be
                                  separated from the command by spaces (we use
                                  :py:func:`shlex.split`). If the action is on
                                  the local filesystem, we'll automatically
                                  upload it to S3.
        :type bootstrap_cmds: list
        :param bootstrap_cmds: a list of commands to run on the master node to
                               set up libraries, etc. Like *setup_cmds*, these
                               can be strings, which will be run in the shell,
                               or lists of args, which will be run directly.
                               Prepend ``sudo`` to commands to do things that
                               require root privileges.
        :type bootstrap_files: list of str
        :param bootstrap_files: files to download to the bootstrap working
                                directory on the master node before running
                                *bootstrap_cmds* (for example, Debian
                                packages). May be local files for mrjob to
                                upload to S3, or any URI that ``hadoop fs``
                                can handle.
        :type bootstrap_mrjob: boolean
        :param bootstrap_mrjob: This is actually an option in the base
                                :py:class:`~mrjob.job.MRJobRunner` class. If
                                this is ``True`` (the default), we'll tar up
                                :py:mod:`mrjob` from the local filesystem, and
                                install it on the master node.
        :type bootstrap_python_packages: list of str
        :param bootstrap_python_packages: paths of python modules to install
                                          on EMR. These should be standard
                                          Python module tarballs. If a module
                                          is named ``foo.tar.gz``, we expect to
                                          be able to run ``tar xfz foo.tar.gz;
                                          cd foo;
                                          sudo python setup.py install``.
        :type bootstrap_scripts: list of str
        :param bootstrap_scripts: scripts to upload and then run on the master
                                  node (a combination of *bootstrap_cmds* and
                                  *bootstrap_files*). These are run after the
                                  command from bootstrap_cmds.
        :type check_emr_status_every: float
        :param check_emr_status_every: How often to check on the status of EMR
                                       jobs. Default is 30 seconds (too often
                                       and AWS will throttle you anyway).
        :type ec2_instance_type: str
        :param ec2_instance_type: What sort of EC2 instance(s) to use on the
                                  nodes that actually run tasks (see
                                  http://aws.amazon.com/ec2/instance-types/).
                                  When you run multiple instances (see
                                  *num_ec2_instances*), the master node is just
                                  coordinating the other nodes, so usually the
                                  default instance type (``m1.small``) is fine,
                                  and using larger instances is wasteful.
        :type ec2_key_pair: str
        :param ec2_key_pair: name of the SSH key you set up for EMR.
        :type ec2_key_pair_file: str
        :param ec2_key_pair_file: path to file containing the SSH key for EMR
        :type ec2_core_instance_type: str
        :param ec2_core_instance_type: like *ec2_instance_type*, but only
                                       for the core (also know as "slave")
                                       Hadoop nodes; these nodes run tasks and
                                       host HDFS. Usually you just want to use
                                       *ec2_instance_type*. Defaults to
                                       ``'m1.small'``.
        :type ec2_core_instance_bid_price: str
        :param ec2_core_instance_bid_price: when specified and not "0", this
                                            creates the master Hadoop node as
                                            a spot instance at this bid price.
                                            You usually only want to set
                                            bid price for task instances.
        :type ec2_master_instance_type: str
        :param ec2_master_instance_type: like *ec2_instance_type*, but only
                                         for the master Hadoop node. This node
                                         hosts the task tracker and HDFS, and
                                         runs tasks if there are no other
                                         nodes. Usually you just want to use
                                         *ec2_instance_type*. Defaults to
                                         ``'m1.small'``.
        :type ec2_master_instance_bid_price: str
        :param ec2_master_instance_bid_price: when specified and not "0", this
                                              creates the master Hadoop node as
                                              a spot instance at this bid
                                              price. You usually only want to
                                              set bid price for task instances
                                              unless the master instance is
                                              your only instance.
        :type ec2_slave_instance_type: str
        :param ec2_slave_instance_type: An alias for *ec2_core_instance_type*,
                                        for consistency with the EMR API.
        :type ec2_task_instance_type: str
        :param ec2_task_instance_type: like *ec2_instance_type*, but only
                                       for the task Hadoop nodes; these nodes
                                       run tasks but do not host HDFS. Usually
                                       you just want to use
                                       *ec2_instance_type*. Defaults to
                                       the same instance type as
                                       *ec2_core_instance_type*.
        :param ec2_task_instance_bid_price: when specified and not "0", this
                                            creates the master Hadoop node as
                                            a spot instance at this bid price.
                                            (You usually only want to set
                                            bid price for task instances.)
        :type emr_endpoint: str
        :param emr_endpoint: optional host to connect to when communicating
                             with S3 (e.g.
                             ``us-west-1.elasticmapreduce.amazonaws.com``).
                             Default is to infer this from *aws_region*.
        :type emr_job_flow_id: str
        :param emr_job_flow_id: the ID of a persistent EMR job flow to run jobs
                                in (normally we launch our own job flow). It's
                                fine for other jobs to be using the job flow;
                                we give our job's steps a unique ID.
        :type emr_job_flow_pool_name: str
        :param emr_job_flow_pool_name: Specify a pool name to join. Is set to
                                       ``'default'`` if not specified. Does not
                                       imply ``pool_emr_job_flows``.
        :type enable_emr_debugging: str
        :param enable_emr_debugging: store Hadoop logs in SimpleDB
        :type hadoop_streaming_jar: str
        :param hadoop_streaming_jar: This is actually an option in the base
                                     :py:class:`~mrjob.runner.MRJobRunner`
                                     class. Points to a custom hadoop streaming
                                     jar on the local filesystem or S3. If you
                                     want to point to a streaming jar already
                                     installed on the EMR instances (perhaps
                                     through a bootstrap action?), use
                                     *hadoop_streaming_jar_on_emr*.
        :type hadoop_streaming_jar_on_emr: str
        :param hadoop_streaming_jar_on_emr: Like *hadoop_streaming_jar*, except
                                            that it points to a path on the EMR
                                            instance, rather than to a local
                                            file or one on S3. Rarely necessary
                                            to set this by hand.
        :type hadoop_version: str
        :param hadoop_version: Set the version of Hadoop to use on EMR.
                               Consider setting *ami_version* instead; only AMI
                               version 1.0 supports multiple versions of Hadoop
                               anyway. If *ami_version* is not set, we'll
                               default to Hadoop 0.20 for backwards
                               compatibility with :py:mod:`mrjob` v0.3.0.
        :type num_ec2_core_instances: int
        :param num_ec2_core_instances: Number of core (or "slave") instances to
                                       start up. These run your job and host
                                       HDFS. Incompatible with
                                       *num_ec2_instances*. This is in addition
                                       to the single master instance.
        :type num_ec2_instances: int
        :param num_ec2_instances: Total number of instances to start up;
                                  basically the number of core instance you
                                  want, plus 1 (there is always one master
                                  instance). Default is ``1``. Incompatible
                                  with *num_ec2_core_instances* and
                                  *num_ec2_task_instances*.
        :type num_ec2_task_instances: int
        :param num_ec2_task_instances: number of task instances to start up.
                                       These run your job but do not host
                                       HDFS. Incompatible with
                                       *num_ec2_instances*. If you use this,
                                       you must set *num_ec2_core_instances*;
                                       EMR does not allow you to run task
                                       instances without core instances (because
                                       there's nowhere to host HDFS).
        :type pool_emr_job_flows: bool
        :param pool_emr_job_flows: Try to run the job on a ``WAITING`` pooled
                                   job flow with the same bootstrap
                                   configuration. Prefer the one with the most
                                   compute units. Use S3 to "lock" the job flow
                                   and ensure that the job is not scheduled
                                   behind another job. If no suitable job flow
                                   is `WAITING`, create a new pooled job flow.
                                   **WARNING**: do not run this without having\
        :py:mod:`mrjob.tools.emr.terminate.idle_job_flows`
                                   in your crontab; job flows left idle can
                                   quickly become expensive!
        :type s3_endpoint: str
        :param s3_endpoint: Host to connect to when communicating with S3 (e.g.
                            ``s3-us-west-1.amazonaws.com``). Default is to
                            infer this from *aws_region*.
        :type s3_log_uri: str
        :param s3_log_uri: where on S3 to put logs, for example
                           ``s3://yourbucket/logs/``. Logs for your job flow
                           will go into a subdirectory, e.g.
                           ``s3://yourbucket/logs/j-JOBFLOWID/``. in this
                           example s3://yourbucket/logs/j-YOURJOBID/). Default
                           is to append ``logs/`` to *s3_scratch_uri*.
        :type s3_scratch_uri: str
        :param s3_scratch_uri: S3 directory (URI ending in ``/``) to use as
                               scratch space, e.g. ``s3://yourbucket/tmp/``.
                               Default is ``tmp/mrjob/`` in the first bucket
                               belonging to you.
        :type s3_sync_wait_time: float
        :param s3_sync_wait_time: How long to wait for S3 to reach eventual
                                  consistency. This is typically less than a
                                  second (zero in U.S. West) but the default is
                                  5.0 to be safe.
        :type ssh_bin: str or list
        :param ssh_bin: path to the ssh binary; may include switches (e.g.
                        ``'ssh -v'`` or ``['ssh', '-v']``). Defaults to
                        :command:`ssh`
        :type ssh_bind_ports: list of int
        :param ssh_bind_ports: a list of ports that are safe to listen on.
                               Defaults to ports ``40001`` thru ``40840``.
        :type ssh_tunnel_to_job_tracker: bool
        :param ssh_tunnel_to_job_tracker: If True, create an ssh tunnel to the
                                          job tracker and listen on a randomly
                                          chosen port. This requires you to set
                                          *ec2_key_pair* and
                                          *ec2_key_pair_file*. See
                                          :ref:`ssh-tunneling` for detailed
                                          instructions.
        :type ssh_tunnel_is_open: bool
        :param ssh_tunnel_is_open: if True, any host can connect to the job
                                   tracker through the SSH tunnel you open.
                                   Mostly useful if your browser is running on
                                   a different machine from your job runner.
        """
        super(EMRJobRunner, self).__init__(**kwargs)

        # make aws_region an instance variable; we might want to set it
        # based on the scratch bucket
        self._aws_region = self._opts['aws_region'] or ''

        # if we're going to create a bucket to use as temp space, we don't
        # want to actually create it until we run the job (Issue #50).
        # This variable helps us create the bucket as needed
        self._s3_temp_bucket_to_create = None

        self._fix_s3_scratch_and_log_uri_opts()

        self._fix_ec2_instance_opts()

        # pick a tmp dir based on the job name
        self._s3_tmp_uri = self._opts['s3_scratch_uri'] + self._job_name + '/'

        # pick/validate output dir
        if self._output_dir:
            self._output_dir = self._check_and_fix_s3_dir(self._output_dir)
        else:
            self._output_dir = self._s3_tmp_uri + 'output/'

        # add the bootstrap files to a list of files to upload
        self._bootstrap_actions = []
        for action in self._opts['bootstrap_actions']:
            args = shlex.split(action)
            if not args:
                raise ValueError('bad bootstrap action: %r' % (action,))
            # don't use _add_bootstrap_file() because this is a raw bootstrap
            # action, not part of mrjob's bootstrap utilities
            file_dict = self._add_file(args[0])
            file_dict['args'] = args[1:]
            self._bootstrap_actions.append(file_dict)

        for path in self._opts['bootstrap_files']:
            self._add_bootstrap_file(path)

        self._bootstrap_scripts = []
        for path in self._opts['bootstrap_scripts']:
            file_dict = self._add_bootstrap_file(path)
            self._bootstrap_scripts.append(file_dict)

        self._bootstrap_python_packages = []
        for path in self._opts['bootstrap_python_packages']:
            name, path = self._split_path(path)
            if not path.endswith('.tar.gz'):
                raise ValueError(
                    'bootstrap_python_packages only accepts .tar.gz files!')
            file_dict = self._add_bootstrap_file(path)
            self._bootstrap_python_packages.append(file_dict)

        self._streaming_jar = None
        if self._opts.get('hadoop_streaming_jar'):
            self._streaming_jar = self._add_file_for_upload(
                self._opts['hadoop_streaming_jar'])

        if not (isinstance(self._opts['additional_emr_info'], basestring)
                or self._opts['additional_emr_info'] is None):
            self._opts['additional_emr_info'] = json.dumps(
                self._opts['additional_emr_info'])

        # if we're bootstrapping mrjob, keep track of the file_dict
        # for mrjob.tar.gz
        self._mrjob_tar_gz_file = None

        # where our own logs ended up (we'll find this out once we run the job)
        self._s3_job_log_uri = None

        # where to get input from. We'll fill this later. Once filled,
        # this must be a list (not some other sort of container)
        self._s3_input_uris = None

        # we'll create the script later
        self._master_bootstrap_script = None

        # the ID assigned by EMR to this job (might be None)
        self._emr_job_flow_id = self._opts['emr_job_flow_id']

        # when did our particular task start?
        self._emr_job_start = None

        # ssh state
        self._ssh_proc = None
        self._gave_cant_ssh_warning = False
        self._ssh_key_name = None

        # cache for SSH address
        self._address = None
        self._ssh_slave_addrs = None

        # store the tracker URL for completion status
        self._tracker_url = None

        # turn off tracker progress until tunnel is up
        self._show_tracker_progress = False

        # default requested hadoop version if AMI version is not set
        if not (self._opts['ami_version'] or self._opts['hadoop_version']):
            self._opts['hadoop_version'] = '0.20'

        # init hadoop version cache
        self._inferred_hadoop_version = None

    @classmethod
    def _allowed_opts(cls):
        """A list of which keyword args we can pass to __init__()"""
        return super(EMRJobRunner, cls)._allowed_opts() + [
            'additional_emr_info',
            'ami_version',
            'aws_access_key_id',
            'aws_availability_zone',
            'aws_region',
            'aws_secret_access_key',
            'bootstrap_actions',
            'bootstrap_cmds',
            'bootstrap_files',
            'bootstrap_python_packages',
            'bootstrap_scripts',
            'check_emr_status_every',
            'ec2_core_instance_bid_price',
            'ec2_core_instance_type',
            'ec2_instance_type',
            'ec2_key_pair',
            'ec2_key_pair_file',
            'ec2_master_instance_bid_price',
            'ec2_master_instance_type',
            'ec2_slave_instance_type',
            'ec2_task_instance_bid_price',
            'ec2_task_instance_type',
            'emr_endpoint',
            'emr_job_flow_id',
            'emr_job_flow_pool_name',
            'enable_emr_debugging',
            'enable_emr_debugging',
            'hadoop_streaming_jar_on_emr',
            'hadoop_version',
            'num_ec2_core_instances',
            'num_ec2_instances',
            'num_ec2_task_instances',
            'pool_emr_job_flows',
            's3_endpoint',
            's3_log_uri',
            's3_scratch_uri',
            's3_sync_wait_time',
            'ssh_bin',
            'ssh_bind_ports',
            'ssh_tunnel_is_open',
            'ssh_tunnel_to_job_tracker',
        ]

    @classmethod
    def _default_opts(cls):
        """A dictionary giving the default value of options."""
        return combine_dicts(super(EMRJobRunner, cls)._default_opts(), {
            'check_emr_status_every': 30,
            'ec2_core_instance_type': 'm1.small',
            'ec2_master_instance_type': 'm1.small',
            'emr_job_flow_pool_name': 'default',
            'hadoop_version': None,  # defaulted in __init__()
            'hadoop_streaming_jar_on_emr':
                '/home/hadoop/contrib/streaming/hadoop-streaming.jar',
            'num_ec2_core_instances': 0,
            'num_ec2_instances': 1,
            'num_ec2_task_instances': 0,
            's3_sync_wait_time': 5.0,
            'ssh_bin': ['ssh'],
            'ssh_bind_ports': range(40001, 40841),
            'ssh_tunnel_to_job_tracker': False,
            'ssh_tunnel_is_open': False,
        })

    @classmethod
    def _opts_combiners(cls):
        """Map from option name to a combine_*() function used to combine
        values for that option. This allows us to specify that some options
        are lists, or contain environment variables, or whatever."""
        return combine_dicts(super(EMRJobRunner, cls)._opts_combiners(), {
            'bootstrap_actions': combine_lists,
            'bootstrap_cmds': combine_lists,
            'bootstrap_files': combine_path_lists,
            'bootstrap_python_packages': combine_path_lists,
            'bootstrap_scripts': combine_path_lists,
            'ec2_key_pair_file': combine_paths,
            's3_log_uri': combine_paths,
            's3_scratch_uri': combine_paths,
            'ssh_bin': combine_cmds,
        })

    def _fix_ec2_instance_opts(self):
        """If the *ec2_instance_type* option is set, override instance
        type for the nodes that actually run tasks (see Issue #66). Allow
        command-line arguments to override defaults and arguments
        in mrjob.conf (see Issue #311).

        Also, make sure that core and slave instance type are the same,
        total number of instances matches number of master, core, and task
        instances, and that bid prices of zero are converted to None.

        Helper for __init__.
        """
        # Make sure slave and core instance type have the same value
        # Within EMRJobRunner we only ever use ec2_core_instance_type,
        # but we want ec2_slave_instance_type to be correct in the
        # options dictionary.
        if (self._opts['ec2_slave_instance_type'] and
            (self._opt_priority['ec2_slave_instance_type'] >
             self._opt_priority['ec2_core_instance_type'])):
            self._opts['ec2_core_instance_type'] = (
                self._opts['ec2_slave_instance_type'])
        else:
            self._opts['ec2_slave_instance_type'] = (
                self._opts['ec2_core_instance_type'])

        # If task instance type is not set, use core instance type
        # (This is mostly so that we don't inadvertently join a pool
        # with task instance types with too little memory.)
        if not self._opts['ec2_task_instance_type']:
            self._opts['ec2_task_instance_type'] = (
                self._opts['ec2_core_instance_type'])

        # Within EMRJobRunner, we use num_ec2_core_instances and
        # num_ec2_task_instances, not num_ec2_instances. (Number
        # of master instances is always 1.)
        if (self._opt_priority['num_ec2_instances'] >
            max(self._opt_priority['num_ec2_core_instances'],
                self._opt_priority['num_ec2_task_instances'])):
            # assume 1 master, n - 1 core, 0 task
            self._opts['num_ec2_core_instances'] = (
                self._opts['num_ec2_instances'] - 1)
            self._opts['num_ec2_task_instances'] = 0
        else:
            # issue a warning if we used both kinds of instance number
            # options on the command line or in mrjob.conf
            if (self._opt_priority['num_ec2_instances'] >= 2 and
                self._opt_priority['num_ec2_instances'] <=
                max(self._opt_priority['num_ec2_core_instances'],
                    self._opt_priority['num_ec2_task_instances'])):
                log.warn('Mixing num_ec2_instances and'
                         ' num_ec2_{core,task}_instances does not make sense;'
                         ' ignoring num_ec2_instances')
            # recalculate number of EC2 instances
            self._opts['num_ec2_instances'] = (
                1 +
                self._opts['num_ec2_core_instances'] +
                self._opts['num_ec2_task_instances'])

        # Allow ec2 instance type to override other instance types
        ec2_instance_type = self._opts['ec2_instance_type']
        if ec2_instance_type:
            # core (slave) instances
            if (self._opt_priority['ec2_instance_type'] >
                max(self._opt_priority['ec2_core_instance_type'],
                    self._opt_priority['ec2_slave_instance_type'])):
                self._opts['ec2_core_instance_type'] = ec2_instance_type
                self._opts['ec2_slave_instance_type'] = ec2_instance_type

            # master instance only does work when it's the only instance
            if (self._opts['num_ec2_core_instances'] <= 0 and
                self._opts['num_ec2_task_instances'] <= 0 and
                (self._opt_priority['ec2_instance_type'] >
                 self._opt_priority['ec2_master_instance_type'])):
                self._opts['ec2_master_instance_type'] = ec2_instance_type

            # task instances
            if (self._opt_priority['ec2_instance_type'] >
                self._opt_priority['ec2_task_instance_type']):
                self._opts['ec2_task_instance_type'] = ec2_instance_type

        # convert a bid price of '0' to None
        for role in ('core', 'master', 'task'):
            opt_name = 'ec2_%s_instance_bid_price' % role
            if not self._opts[opt_name]:
                self._opts[opt_name] = None
            else:
                # convert "0", "0.00" etc. to None
                try:
                    value = float(self._opts[opt_name])
                    if value == 0:
                        self._opts[opt_name] = None
                except ValueError:
                    pass  # maybe EMR will accept non-floats?

    def _fix_s3_scratch_and_log_uri_opts(self):
        """Fill in s3_scratch_uri and s3_log_uri (in self._opts) if they
        aren't already set.

        Helper for __init__.
        """
        s3_conn = self.make_s3_conn()
        # check s3_scratch_uri against aws_region if specified
        if self._opts['s3_scratch_uri']:
            bucket_name, _ = parse_s3_uri(self._opts['s3_scratch_uri'])
            bucket_loc = s3_conn.get_bucket(bucket_name).get_location()

            # make sure they can communicate if both specified
            if (self._aws_region and bucket_loc and
                self._aws_region != bucket_loc):
                log.warning('warning: aws_region (%s) does not match bucket'
                            ' region (%s). Your EC2 instances may not be able'
                            ' to reach your S3 buckets.' %
                            (self._aws_region, bucket_loc))

            # otherwise derive aws_region from bucket_loc
            elif bucket_loc and not self._aws_region:
                log.info(
                    "inferring aws_region from scratch bucket's region (%s)" %
                    bucket_loc)
                self._aws_region = bucket_loc
        # set s3_scratch_uri by checking for existing buckets
        else:
            self._set_s3_scratch_uri(s3_conn)
            log.info('using %s as our scratch dir on S3' %
                     self._opts['s3_scratch_uri'])

        self._opts['s3_scratch_uri'] = self._check_and_fix_s3_dir(
            self._opts['s3_scratch_uri'])

        # set s3_log_uri
        if self._opts['s3_log_uri']:
            self._opts['s3_log_uri'] = self._check_and_fix_s3_dir(
                self._opts['s3_log_uri'])
        else:
            self._opts['s3_log_uri'] = self._opts['s3_scratch_uri'] + 'logs/'

    def _set_s3_scratch_uri(self, s3_conn):
        """Helper for _fix_s3_scratch_and_log_uri_opts"""
        buckets = s3_conn.get_all_buckets()
        mrjob_buckets = [b for b in buckets if b.name.startswith('mrjob-')]

        # Loop over buckets until we find one that is not region-
        #   restricted, matches aws_region, or can be used to
        #   infer aws_region if no aws_region is specified
        for scratch_bucket in mrjob_buckets:
            scratch_bucket_name = scratch_bucket.name
            scratch_bucket_location = scratch_bucket.get_location()

            if scratch_bucket_location:
                if scratch_bucket_location == self._aws_region:
                    # Regions are both specified and match
                    log.info("using existing scratch bucket %s" %
                             scratch_bucket_name)
                    self._opts['s3_scratch_uri'] = (
                        's3://%s/tmp/' % scratch_bucket_name)
                    return
                elif not self._aws_region:
                    # aws_region not specified, so set it based on this
                    #   bucket's location and use this bucket
                    self._aws_region = scratch_bucket_location
                    log.info("inferring aws_region from scratch bucket's"
                             " region (%s)" % self._aws_region)
                    self._opts['s3_scratch_uri'] = (
                        's3://%s/tmp/' % scratch_bucket_name)
                    return
                elif scratch_bucket_location != self._aws_region:
                    continue
            elif not self._aws_region:
                # Only use regionless buckets if the job flow is regionless
                log.info("using existing scratch bucket %s" %
                         scratch_bucket_name)
                self._opts['s3_scratch_uri'] = (
                    's3://%s/tmp/' % scratch_bucket_name)
                return

        # That may have all failed. If so, pick a name.
        scratch_bucket_name = 'mrjob-%016x' % random.randint(0, 2 ** 64 - 1)
        self._s3_temp_bucket_to_create = scratch_bucket_name
        log.info("creating new scratch bucket %s" % scratch_bucket_name)
        self._opts['s3_scratch_uri'] = 's3://%s/tmp/' % scratch_bucket_name

    def _set_s3_job_log_uri(self, job_flow):
        """Given a job flow description, set self._s3_job_log_uri. This allows
        us to call self.ls(), etc. without running the job.
        """
        log_uri = getattr(job_flow, 'loguri', '')
        if log_uri:
            self._s3_job_log_uri = '%s%s/' % (
                log_uri.replace('s3n://', 's3://'), self._emr_job_flow_id)

    def _create_s3_temp_bucket_if_needed(self):
        """Make sure temp bucket exists"""
        if self._s3_temp_bucket_to_create:
            s3_conn = self.make_s3_conn()
            log.info('creating S3 bucket %r to use as scratch space' %
                     self._s3_temp_bucket_to_create)
            location = REGION_TO_S3_LOCATION_CONSTRAINT.get(
                self._aws_region, self._aws_region)
            s3_conn.create_bucket(self._s3_temp_bucket_to_create,
                                  location=(location or ''))
            self._s3_temp_bucket_to_create = None

    def _check_and_fix_s3_dir(self, s3_uri):
        """Helper for __init__"""
        if not is_s3_uri(s3_uri):
            raise ValueError('Invalid S3 URI: %r' % s3_uri)
        if not s3_uri.endswith('/'):
            s3_uri = s3_uri + '/'

        return s3_uri

    def _run(self):
        self._prepare_for_launch()

        self._launch_emr_job()
        self._wait_for_job_to_complete()

    def _prepare_for_launch(self):
        self._setup_input()
        self._create_wrapper_script()
        self._create_master_bootstrap_script()
        self._upload_non_input_files()

    def _setup_input(self):
        """Copy local input files (if any) to a special directory on S3.

        Set self._s3_input_uris
        Helper for _run
        """
        self._create_s3_temp_bucket_if_needed()
        # winnow out s3 files from local ones
        self._s3_input_uris = []
        local_input_paths = []
        for path in self._input_paths:
            if is_s3_uri(path):
                # Don't even bother running the job if the input isn't there,
                # since it's costly to spin up instances.
                if not self.path_exists(path):
                    raise AssertionError(
                        'Input path %s does not exist!' % (path,))
                self._s3_input_uris.append(path)
            else:
                local_input_paths.append(path)

        # copy local files into an input directory, with names like
        # 00000-actual_name.ext
        if local_input_paths:
            s3_input_dir = self._s3_tmp_uri + 'input/'
            log.info('Uploading input to %s' % s3_input_dir)

            s3_conn = self.make_s3_conn()
            for file_num, path in enumerate(local_input_paths):
                if path == '-':
                    path = self._dump_stdin_to_local_file()

                target = '%s%05d-%s' % (
                    s3_input_dir, file_num, os.path.basename(path))
                log.debug('uploading %s -> %s' % (path, target))
                s3_key = self.make_s3_key(target, s3_conn)
                s3_key.set_contents_from_filename(path)

            self._s3_input_uris.append(s3_input_dir)

    def _add_bootstrap_file(self, path):
        name, path = self._split_path(path)
        file_dict = {'path': path, 'name': name, 'bootstrap': 'file'}
        self._files.append(file_dict)
        return file_dict

    def _pick_s3_uris_for_files(self):
        """Decide where each file will be uploaded on S3.

        Okay to call this multiple times.
        """
        self._assign_unique_names_to_files(
            's3_uri', prefix=self._s3_tmp_uri + 'files/',
            match=is_s3_uri)

    def _upload_non_input_files(self):
        """Copy files to S3

        Pick S3 URIs for them if we haven't already."""
        self._create_s3_temp_bucket_if_needed()
        self._pick_s3_uris_for_files()

        s3_files_dir = self._s3_tmp_uri + 'files/'
        log.info('Copying non-input files into %s' % s3_files_dir)

        s3_conn = self.make_s3_conn()
        for file_dict in self._files:
            path = file_dict['path']

            # don't bother with files that are already on s3
            if is_s3_uri(path):
                continue

            s3_uri = file_dict['s3_uri']

            log.debug('uploading %s -> %s' % (path, s3_uri))
            s3_key = self.make_s3_key(s3_uri, s3_conn)
            s3_key.set_contents_from_filename(file_dict['path'])

    def setup_ssh_tunnel_to_job_tracker(self, host):
        """setup the ssh tunnel to the job tracker, if it's not currently
        running.

        Args:
        host -- hostname of the EMR master node.
        """
        REQUIRED_OPTS = ['ec2_key_pair', 'ec2_key_pair_file', 'ssh_bind_ports']
        for opt_name in REQUIRED_OPTS:
            if not self._opts[opt_name]:
                if not self._gave_cant_ssh_warning:
                    log.warning(
                        "You must set %s in order to ssh to the job tracker!" %
                        opt_name)
                    self._gave_cant_ssh_warning = True
                return

        # if there was already a tunnel, make sure it's still up
        if self._ssh_proc:
            self._ssh_proc.poll()
            if self._ssh_proc.returncode is None:
                return
            else:
                log.warning('Oops, ssh subprocess exited with return code %d,'
                            ' restarting...' % self._ssh_proc.returncode)
                self._ssh_proc = None

        log.info('Opening ssh tunnel to Hadoop job tracker')

        # if ssh detects that a host key has changed, it will silently not
        # open the tunnel, so make a fake empty known_hosts file and use that.
        # (you can actually use /dev/null as your known hosts file, but
        # that's UNIX-specific)
        fake_known_hosts_file = os.path.join(
            self._get_local_tmp_dir(), 'fake_ssh_known_hosts')
        # blank out the file, if it exists
        f = open(fake_known_hosts_file, 'w')
        f.close()
        log.debug('Created empty ssh known-hosts file: %s' % (
            fake_known_hosts_file,))

        bind_port = None
        for bind_port in self._pick_ssh_bind_ports():
            args = self._opts['ssh_bin'] + [
                '-o', 'VerifyHostKeyDNS=no',
                '-o', 'StrictHostKeyChecking=no',
                '-o', 'ExitOnForwardFailure=yes',
                '-o', 'UserKnownHostsFile=%s' % fake_known_hosts_file,
                '-L', '%d:localhost:%d' % (bind_port, EMR_JOB_TRACKER_PORT),
                '-N', '-q',  # no shell, no output
                '-i', self._opts['ec2_key_pair_file'],
            ]
            if self._opts['ssh_tunnel_is_open']:
                args.extend(['-g', '-4'])  # -4: listen on IPv4 only
            args.append('hadoop@' + host)
            log.debug('> %s' % cmd_line(args))

            ssh_proc = Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
            time.sleep(WAIT_FOR_SSH_TO_FAIL)
            ssh_proc.poll()
            # still running. We are golden
            if ssh_proc.returncode is None:
                self._ssh_proc = ssh_proc
                break

        if not self._ssh_proc:
            log.warning('Failed to open ssh tunnel to job tracker')
        else:
            if self._opts['ssh_tunnel_is_open']:
                bind_host = socket.getfqdn()
            else:
                bind_host = 'localhost'
            self._tracker_url = 'http://%s:%d%s' % (
                bind_host, bind_port, EMR_JOB_TRACKER_PATH)
            self._show_tracker_progress = True
            log.info('Connect to job tracker at: %s' % self._tracker_url)

    def _pick_ssh_bind_ports(self):
        """Pick a list of ports to try binding our SSH tunnel to.

        We will try to bind the same port for any given job flow (Issue #67)
        """
        # don't perturb the random number generator
        random_state = random.getstate()
        try:
            # seed random port selection on job flow ID
            random.seed(self._emr_job_flow_id)
            num_picks = min(MAX_SSH_RETRIES, len(self._opts['ssh_bind_ports']))
            return random.sample(self._opts['ssh_bind_ports'], num_picks)
        finally:
            random.setstate(random_state)

    def _enable_slave_ssh_access(self):
        if not self._ssh_key_name:
            self._ssh_key_name = self._job_name + '.pem'
            ssh_copy_key(
                self._opts['ssh_bin'],
                self._address_of_master(),
                self._opts['ec2_key_pair_file'],
                self._ssh_key_name)

    ### Running the job ###

    def cleanup(self, mode=None):
        super(EMRJobRunner, self).cleanup(mode=mode)

        # always stop our SSH tunnel if it's still running
        if self._ssh_proc:
            self._ssh_proc.poll()
            if self._ssh_proc.returncode is None:
                log.info('Killing our SSH tunnel (pid %d)' %
                         self._ssh_proc.pid)
                try:
                    os.kill(self._ssh_proc.pid, signal.SIGKILL)
                    self._ssh_proc = None
                except Exception, e:
                    log.exception(e)

        # stop the job flow if it belongs to us (it may have stopped on its
        # own already, but that's fine)
        # don't stop it if it was created due to --pool because the user
        # probably wants to use it again
        if self._emr_job_flow_id and not self._opts['emr_job_flow_id'] \
           and not self._opts['pool_emr_job_flows']:
            log.info('Terminating job flow: %s' % self._emr_job_flow_id)
            try:
                self.make_emr_conn().terminate_jobflow(self._emr_job_flow_id)
            except Exception, e:
                log.exception(e)

    def _cleanup_remote_scratch(self):
        # delete all the files we created
        if self._s3_tmp_uri:
            try:
                log.info('Removing all files in %s' % self._s3_tmp_uri)
                self.rm(self._s3_tmp_uri)
                self._s3_tmp_uri = None
            except Exception, e:
                log.exception(e)

    def _cleanup_logs(self):
        super(EMRJobRunner, self)._cleanup_logs()

        # delete the log files, if it's a job flow we created (the logs
        # belong to the job flow)
        if self._s3_job_log_uri and not self._opts['emr_job_flow_id'] \
           and not self._opts['pool_emr_job_flows']:
            try:
                log.info('Removing all files in %s' % self._s3_job_log_uri)
                self.rm(self._s3_job_log_uri)
                self._s3_job_log_uri = None
            except Exception, e:
                log.exception(e)

    def _wait_for_s3_eventual_consistency(self):
        """Sleep for a little while, to give S3 a chance to sync up.
        """
        log.info('Waiting %.1fs for S3 eventual consistency' %
                 self._opts['s3_sync_wait_time'])
        time.sleep(self._opts['s3_sync_wait_time'])

    def _wait_for_job_flow_termination(self):
        try:
            jobflow = self._describe_jobflow()
        except boto.exception.S3ResponseError:
            # mockboto throws this for some reason
            return
        if (jobflow.keepjobflowalivewhennosteps == 'true' and
            jobflow.state == 'WAITING'):
            raise Exception('Operation requires job flow to terminate, but'
                            ' it may never do so.')
        while jobflow.state not in ('TERMINATED', 'COMPLETED', 'FAILED',
                                    'SHUTTING_DOWN'):
            msg = 'Waiting for job flow to terminate (currently %s)' % \
                                                         jobflow.state
            log.info(msg)
            time.sleep(self._opts['check_emr_status_every'])
            jobflow = self._describe_jobflow()

    def _create_instance_group(self, role, instance_type, count, bid_price):
        """Helper method for creating instance groups. For use when
        creating a jobflow using a list of InstanceGroups, instead
        of the typical triumverate of
        num_instances/master_instance_type/slave_instance_type.

            - Role is either 'master', 'core', or 'task'.
            - instance_type is an EC2 instance type
            - count is an int
            - bid_price is a number, a string, or None. If None,
              this instance group will be use the ON-DEMAND market
              instead of the SPOT market.
        """

        if not instance_type:
            if self._opts['ec2_instance_type']:
                instance_type = self._opts['ec2_instance_type']
            else:
                raise ValueError('Missing instance type for %s node(s)'
                    % role)

        if bid_price:
            market = 'SPOT'
            bid_price = str(bid_price)  # must be a string
        else:
            market = 'ON_DEMAND'
            bid_price = None

        # Just name the groups "master", "task", and "core"
        name = role.lower()

        return boto_2_1_1_83aae37b.InstanceGroup(
            count, role, instance_type, market, name, bidprice=bid_price
            )

    def _create_job_flow(self, persistent=False, steps=None):
        """Create an empty job flow on EMR, and return the ID of that
        job.

        persistent -- if this is true, create the job flow with the --alive
            option, indicating the job will have to be manually terminated.
        """
        # make sure we can see the files we copied to S3
        self._wait_for_s3_eventual_consistency()

        # figure out local names and S3 URIs for our bootstrap actions, if any
        self._name_files()
        self._pick_s3_uris_for_files()

        log.info('Creating Elastic MapReduce job flow')
        args = self._job_flow_args(persistent, steps)

        emr_conn = self.make_emr_conn()
        log.debug('Calling run_jobflow(%r, %r, %s)' % (
            self._job_name, self._opts['s3_log_uri'],
            ', '.join('%s=%r' % (k, v) for k, v in args.iteritems())))
        emr_job_flow_id = emr_conn.run_jobflow(
            self._job_name, self._opts['s3_log_uri'], **args)

         # keep track of when we started our job
        self._emr_job_start = time.time()

        log.info('Job flow created with ID: %s' % emr_job_flow_id)
        return emr_job_flow_id

    def _job_flow_args(self, persistent=False, steps=None):
        """Build kwargs for emr_conn.run_jobflow()"""
        args = {}

        args['ami_version'] = self._opts['ami_version']
        args['hadoop_version'] = self._opts['hadoop_version']

        if self._opts['aws_availability_zone']:
            args['availability_zone'] = self._opts['aws_availability_zone']

        # The old, simple API, available if we're not using task instances
        # or bid prices
        if not (self._opts['num_ec2_task_instances'] or
                self._opts['ec2_core_instance_bid_price'] or
                self._opts['ec2_master_instance_bid_price'] or
                self._opts['ec2_task_instance_bid_price']):
            args['num_instances'] = self._opts['num_ec2_core_instances'] + 1
            args['master_instance_type'] = (
                self._opts['ec2_master_instance_type'])
            args['slave_instance_type'] = self._opts['ec2_core_instance_type']
        else:
            # Create a list of InstanceGroups
            args['instance_groups'] = [
                self._create_instance_group(
                    'MASTER',
                    self._opts['ec2_master_instance_type'],
                    1,
                    self._opts['ec2_master_instance_bid_price']
                    ),
            ]

            if self._opts['num_ec2_core_instances']:
                args['instance_groups'].append(
                    self._create_instance_group(
                        'CORE',
                        self._opts['ec2_core_instance_type'],
                        self._opts['num_ec2_core_instances'],
                        self._opts['ec2_core_instance_bid_price']
                    )
                )

            if self._opts['num_ec2_task_instances']:
                args['instance_groups'].append(
                    self._create_instance_group(
                        'TASK',
                        self._opts['ec2_task_instance_type'],
                        self._opts['num_ec2_task_instances'],
                        self._opts['ec2_task_instance_bid_price']
                        )
                    )

        # bootstrap actions
        bootstrap_action_args = []

        for file_dict in self._bootstrap_actions:
            # file_dict is not populated the same way by tools and real job
            # runs, so use s3_uri or path as appropriate
            s3_uri = file_dict.get('s3_uri', None) or file_dict['path']
            bootstrap_action_args.append(
                boto.emr.BootstrapAction(
                file_dict['name'], s3_uri, file_dict['args']))

        if self._master_bootstrap_script:
            master_bootstrap_script_args = []
            if self._opts['pool_emr_job_flows']:
                master_bootstrap_script_args = [
                    'pool-' + self._pool_hash(),
                    self._opts['emr_job_flow_pool_name'],
                ]
            bootstrap_action_args.append(
                boto.emr.BootstrapAction(
                    'master', self._master_bootstrap_script['s3_uri'],
                    master_bootstrap_script_args))

        if bootstrap_action_args:
            args['bootstrap_actions'] = bootstrap_action_args

        if self._opts['ec2_key_pair']:
            args['ec2_keyname'] = self._opts['ec2_key_pair']

        if self._opts['enable_emr_debugging']:
            args['enable_debugging'] = True

        if self._opts['additional_emr_info']:
            args['additional_info'] = self._opts['additional_emr_info']

        if persistent or self._opts['pool_emr_job_flows']:
            args['keep_alive'] = True

        if steps:
            args['steps'] = steps

        return args

    def _build_steps(self):
        """Return a list of boto Step objects corresponding to the
        steps we want to run."""
        assert self._script  # can't build steps if no script!

        # figure out local names for our files
        self._name_files()
        self._pick_s3_uris_for_files()

        # we're going to instruct EMR to upload the MR script and the
        # wrapper script (if any) to the job's local directory
        self._script['upload'] = 'file'
        if self._wrapper_script:
            self._wrapper_script['upload'] = 'file'

        # quick, add the other steps before the job spins up and
        # then shuts itself down (in practice this takes several minutes)
        steps = self._get_steps()
        step_list = []

        version = self.get_hadoop_version()

        for step_num, step in enumerate(steps):
            # EMR-specific stuff
            name = '%s: Step %d of %d' % (
                self._job_name, step_num + 1, len(steps))

            # don't terminate other people's job flows
            if (self._opts['emr_job_flow_id'] or
                self._opts['pool_emr_job_flows']):
                action_on_failure = 'CANCEL_AND_WAIT'
            else:
                action_on_failure = 'TERMINATE_JOB_FLOW'

            # Hadoop streaming stuff
            if 'M' not in step:  # if we have an identity mapper
                mapper = 'cat'
            else:
                mapper = cmd_line(self._mapper_args(step_num))

            if 'C' in step:
                combiner = cmd_line(self._combiner_args(step_num))
            else:
                combiner = None

            if 'R' in step:  # i.e. if there is a reducer:
                reducer = cmd_line(self._reducer_args(step_num))
            else:
                reducer = None

            input = self._s3_step_input_uris(step_num)
            output = self._s3_step_output_uri(step_num)\

            step_args, cache_files, cache_archives = self._cache_args()

            step_args.extend(self._hadoop_conf_args(step_num, len(steps)))
            jar = self._get_jar()

            if combiner is not None:
                if compat.supports_combiners_in_hadoop_streaming(version):
                    step_args.extend(['-combiner', combiner])
                else:
                    mapper = "bash -c '%s | sort | %s'" % (mapper, combiner)

            streaming_step = boto.emr.StreamingStep(
                name=name, mapper=mapper, reducer=reducer,
                action_on_failure=action_on_failure,
                cache_files=cache_files, cache_archives=cache_archives,
                step_args=step_args, input=input, output=output,
                jar=jar)

            step_list.append(streaming_step)

        return step_list

    def _cache_args(self):
        """Returns ``(step_args, cache_files, cache_archives)``, populating
        each according to the correct behavior for the current Hadoop version.

        For < 0.20, populate cache_files and cache_archives.
        For >= 0.20, populate step_args.

        step_args should be inserted into the step arguments before anything
            else.

        cache_files and cache_archives should be passed as arguments to
            StreamingStep.
        """
        version = self.get_hadoop_version()

        step_args = []
        cache_files = []
        cache_archives = []

        if compat.supports_new_distributed_cache_options(version):
            # boto doesn't support non-deprecated 0.20 options, so insert
            # them ourselves

            def escaped_paths(file_dicts):
                # return list of strings to join with commas and pass to the
                # hadoop binary
                return ["%s#%s" % (fd['s3_uri'], fd['name'])
                        for fd in file_dicts]

            # index by type
            all_files = {}
            for fd in self._files:
                all_files.setdefault(fd.get('upload'), []).append(fd)

            if 'file' in all_files:
                step_args.append('-files')
                step_args.append(','.join(escaped_paths(all_files['file'])))

            if 'archive' in all_files:
                step_args.append('-archives')
                step_args.append(','.join(escaped_paths(all_files['archive'])))
        else:
            for file_dict in self._files:
                if file_dict.get('upload') == 'file':
                    cache_files.append(
                        '%s#%s' % (file_dict['s3_uri'], file_dict['name']))
                elif file_dict.get('upload') == 'archive':
                    cache_archives.append(
                        '%s#%s' % (file_dict['s3_uri'], file_dict['name']))

        return step_args, cache_files, cache_archives

    def _get_jar(self):
        self._name_files()
        self._pick_s3_uris_for_files()

        if self._streaming_jar:
            return self._streaming_jar['s3_uri']
        else:
            return self._opts['hadoop_streaming_jar_on_emr']

    def _launch_emr_job(self):
        """Create an empty jobflow on EMR, and set self._emr_job_flow_id to
        the ID for that job."""
        self._create_s3_temp_bucket_if_needed()
        # define out steps
        steps = self._build_steps()

        # try to find a job flow from the pool. basically auto-fill
        # 'emr_job_flow_id' if possible and then follow normal behavior.
        if self._opts['pool_emr_job_flows']:
            job_flow = self.find_job_flow(num_steps=len(steps))
            if job_flow:
                self._emr_job_flow_id = job_flow.jobflowid

        # create a job flow if we're not already using an existing one
        if not self._emr_job_flow_id:
            self._emr_job_flow_id = self._create_job_flow(
                persistent=False, steps=steps)
        else:
            emr_conn = self.make_emr_conn()
            log.info('Adding our job to job flow %s' % self._emr_job_flow_id)
            log.debug('Calling add_jobflow_steps(%r, %r)' % (
                self._emr_job_flow_id, steps))
            emr_conn.add_jobflow_steps(self._emr_job_flow_id, steps)

        # keep track of when we launched our job
        self._emr_job_start = time.time()

    def _wait_for_job_to_complete(self):
        """Wait for the job to complete, and raise an exception if
        the job failed.

        Also grab log URI from the job status (since we may not know it)
        """
        success = False

        while True:
            # don't antagonize EMR's throttling
            log.debug('Waiting %.1f seconds...' %
                      self._opts['check_emr_status_every'])
            time.sleep(self._opts['check_emr_status_every'])

            job_flow = self._describe_jobflow()

            self._set_s3_job_log_uri(job_flow)

            job_state = job_flow.state
            reason = getattr(job_flow, 'laststatechangereason', '')

            # find all steps belonging to us, and get their state
            step_states = []
            running_step_name = ''
            total_step_time = 0.0
            step_nums = []  # step numbers belonging to us. 1-indexed

            steps = job_flow.steps or []
            for i, step in enumerate(steps):
                # ignore steps belonging to other jobs
                if not step.name.startswith(self._job_name):
                    continue

                step_nums.append(i + 1)

                step.state = step.state
                step_states.append(step.state)
                if step.state == 'RUNNING':
                    running_step_name = step.name

                if (hasattr(step, 'startdatetime') and
                    hasattr(step, 'enddatetime')):
                    start_time = iso8601_to_timestamp(step.startdatetime)
                    end_time = iso8601_to_timestamp(step.enddatetime)
                    total_step_time += end_time - start_time

            if not step_states:
                raise AssertionError("Can't find our steps in the job flow!")

            # if all our steps have completed, we're done!
            if all(state == 'COMPLETED' for state in step_states):
                success = True
                break

            # if any step fails, give up
            if any(state in ('FAILED', 'CANCELLED') for state in step_states):
                break

            # (the other step states are PENDING and RUNNING)

            # keep track of how long we've been waiting
            running_time = time.time() - self._emr_job_start

            # otherwise, we can print a status message
            if running_step_name:
                log.info('Job launched %.1fs ago, status %s: %s (%s)' %
                         (running_time, job_state, reason, running_step_name))
                if self._show_tracker_progress:
                    try:
                        tracker_handle = urllib2.urlopen(self._tracker_url)
                        tracker_page = ''.join(tracker_handle.readlines())
                        tracker_handle.close()
                        # first two formatted percentages, map then reduce
                        map_complete, reduce_complete = [float(complete)
                            for complete in JOB_TRACKER_RE.findall(
                                tracker_page)[:2]]
                        log.info(' map %3d%% reduce %3d%%' % (
                                 map_complete, reduce_complete))
                    except:
                        log.error('Unable to load progress from job tracker')
                        # turn off progress for rest of job
                        self._show_tracker_progress = False
                # once a step is running, it's safe to set up the ssh tunnel to
                # the job tracker
                job_host = getattr(job_flow, 'masterpublicdnsname', None)
                if job_host and self._opts['ssh_tunnel_to_job_tracker']:
                    self.setup_ssh_tunnel_to_job_tracker(job_host)

            # other states include STARTING and SHUTTING_DOWN
            elif reason:
                log.info('Job launched %.1fs ago, status %s: %s' %
                         (running_time, job_state, reason))
            else:
                log.info('Job launched %.1fs ago, status %s' %
                         (running_time, job_state,))

        if success:
            log.info('Job completed.')
            log.info('Running time was %.1fs (not counting time spent waiting'
                     ' for the EC2 instances)' % total_step_time)
            self._fetch_counters(step_nums)
            self.print_counters(range(1, len(step_nums) + 1))
        else:
            msg = 'Job failed with status %s: %s' % (job_state, reason)
            log.error(msg)
            if self._s3_job_log_uri:
                log.info('Logs are in %s' % self._s3_job_log_uri)
            # look for a Python traceback
            cause = self._find_probable_cause_of_failure(step_nums)
            if cause:
                # log cause, and put it in exception
                cause_msg = []  # lines to log and put in exception
                cause_msg.append('Probable cause of failure (from %s):' %
                           cause['log_file_uri'])
                cause_msg.extend(line.strip('\n') for line in cause['lines'])
                if cause['input_uri']:
                    cause_msg.append('(while reading from %s)' %
                                     cause['input_uri'])

                for line in cause_msg:
                    log.error(line)

                # add cause_msg to exception message
                msg += '\n' + '\n'.join(cause_msg) + '\n'

            raise Exception(msg)

    def _script_args(self):
        """How to invoke the script inside EMR"""
        # We can invoke the script by its S3 URL, but we don't really
        # gain anything from that, and EMR is touchy about distinguishing
        # python scripts from shell scripts

        assert self._script  # shouldn't call _script_args() if no script

        args = self._opts['python_bin'] + [self._script['name']]
        if self._wrapper_script:
            args = (self._opts['python_bin'] +
                    [self._wrapper_script['name']] +
                    args)

        return args

    def _mapper_args(self, step_num):
        return (self._script_args() +
                ['--step-num=%d' % step_num, '--mapper'] +
                self._mr_job_extra_args())

    def _reducer_args(self, step_num):
        return (self._script_args() +
                ['--step-num=%d' % step_num, '--reducer'] +
                self._mr_job_extra_args())

    def _combiner_args(self, step_num):
        return (self._script_args() +
                ['--step-num=%d' % step_num, '--combiner'] +
                self._mr_job_extra_args())

    def _upload_args(self):
        """Args to upload files from S3 to the local nodes that EMR runs
        on."""
        args = []
        for file_dict in self._files:
            if file_dict.get('upload') == 'file':
                args.append('--cache')
                args.append('%s#%s' % (file_dict['s3_uri'], file_dict['name']))
            elif file_dict.get('upload') == 'archive':
                args.append('--cache-archive')
                args.append('%s#%s' % (file_dict['s3_uri'], file_dict['name']))

        return args

    def _s3_step_input_uris(self, step_num):
        """Get the s3:// URIs for input for the given step."""
        if step_num == 0:
            return self._s3_input_uris
        else:
            # put intermediate data in HDFS
            return ['hdfs:///tmp/mrjob/%s/step-output/%s/' % (
                self._job_name, step_num)]

    def _s3_step_output_uri(self, step_num):
        if step_num == len(self._get_steps()) - 1:
            return self._output_dir
        else:
            # put intermediate data in HDFS
            return 'hdfs:///tmp/mrjob/%s/step-output/%s/' % (
                self._job_name, step_num + 1)

    ### LOG FETCHING/PARSING ###

    def _enforce_path_regexp(self, paths, regexp, step_nums=None):
        """Helper for log fetching functions to filter out unwanted
        logs. Only pass ``step_nums`` if ``regexp`` has a ``step_nums`` group.
        """
        for path in paths:
            m = regexp.match(path)
            if (m and
                (step_nums is None or
                 int(m.group('step_num')) in step_nums)):
                yield path
            else:
                log.debug('Ignore %s' % path)

    ## SSH LOG FETCHING

    def _ls_ssh_logs(self, relative_path):
        """List logs over SSH by path relative to log root directory"""
        full_path = SSH_PREFIX + SSH_LOG_ROOT + '/' + relative_path
        log.debug('Search %s for logs' % full_path)
        return self.ls(full_path)

    def _ls_slave_ssh_logs(self, addr, relative_path):
        """List logs over multi-hop SSH by path relative to log root directory
        """
        root_path = '%s%s!%s%s' % (SSH_PREFIX,
                                   self._address_of_master(),
                                   addr,
                                   SSH_LOG_ROOT + '/' + relative_path)
        log.debug('Search %s for logs' % root_path)
        return self.ls(root_path)

    def ls_task_attempt_logs_ssh(self, step_nums):
        all_paths = []
        try:
            all_paths.extend(self._ls_ssh_logs('userlogs/'))
        except IOError:
            # sometimes the master doesn't have these
            pass
        if not all_paths:
            # get them from the slaves instead (takes a little longer)
            try:
                for addr in self._addresses_of_slaves():
                    logs = self._ls_slave_ssh_logs(addr, 'userlogs/')
                    all_paths.extend(logs)
            except IOError:
                # sometimes the slaves don't have them either
                pass
        return self._enforce_path_regexp(all_paths,
                                         TASK_ATTEMPTS_LOG_URI_RE,
                                         step_nums)

    def ls_step_logs_ssh(self, step_nums):
        return self._enforce_path_regexp(self._ls_ssh_logs('steps/'),
                                         STEP_LOG_URI_RE,
                                         step_nums)

    def ls_job_logs_ssh(self, step_nums):
        return self._enforce_path_regexp(self._ls_ssh_logs('history/'),
                                         EMR_JOB_LOG_URI_RE,
                                         step_nums)

    def ls_node_logs_ssh(self):
        all_paths = []
        for addr in self._addresses_of_slaves():
            logs = self._ls_slave_ssh_logs(addr, '')
            all_paths.extend(logs)
        return self._enforce_path_regexp(all_paths, NODE_LOG_URI_RE)

    def ls_all_logs_ssh(self):
        """List all log files in the log root directory"""
        return self.ls(SSH_PREFIX + SSH_LOG_ROOT)

    ## S3 LOG FETCHING ##

    def _ls_s3_logs(self, relative_path):
        """List logs over S3 by path relative to log root directory"""
        if not self._s3_job_log_uri:
            self._set_s3_job_log_uri(self._describe_jobflow())

        if not self._s3_job_log_uri:
            raise LogFetchError('Could not determine S3 job log URI')

        full_path = self._s3_job_log_uri + relative_path
        log.debug('Search %s for logs' % full_path)
        return self.ls(full_path)

    def ls_task_attempt_logs_s3(self, step_nums):
        return self._enforce_path_regexp(self._ls_s3_logs('task-attempts/'),
                                         TASK_ATTEMPTS_LOG_URI_RE,
                                         step_nums)

    def ls_step_logs_s3(self, step_nums):
        return self._enforce_path_regexp(self._ls_s3_logs('steps/'),
                                         STEP_LOG_URI_RE,
                                         step_nums)

    def ls_job_logs_s3(self, step_nums):
        return  self._enforce_path_regexp(self._ls_s3_logs('jobs/'),
                                          EMR_JOB_LOG_URI_RE,
                                          step_nums)

    def ls_node_logs_s3(self):
        return self._enforce_path_regexp(self._ls_s3_logs('node/'),
                                         NODE_LOG_URI_RE)

    def ls_all_logs_s3(self):
        """List all log files in the S3 log root directory"""
        if not self._s3_job_log_uri:
            self._set_s3_job_log_uri(self._describe_jobflow())
        return self.ls(self._s3_job_log_uri)

    ## LOG PARSING ##

    def _fetch_counters(self, step_nums, skip_s3_wait=False):
        """Read Hadoop counters from S3.

        Args:
        step_nums -- the steps belonging to us, so that we can ignore counters
                     from other jobs when sharing a job flow
        """
        self._counters = []
        new_counters = {}
        if self._opts['ec2_key_pair_file']:
            try:
                new_counters = self._fetch_counters_ssh(step_nums)
            except LogFetchError:
                new_counters = self._fetch_counters_s3(step_nums, skip_s3_wait)
            except IOError:
                # Can get 'file not found' if test suite was lazy or Hadoop
                # logs moved. We shouldn't crash in either case.
                new_counters = self._fetch_counters_s3(step_nums, skip_s3_wait)
        else:
            log.info('ec2_key_pair_file not specified, going to S3')
            new_counters = self._fetch_counters_s3(step_nums, skip_s3_wait)

        # step_nums is relative to the start of the job flow
        # we only want them relative to the job
        for step_num in step_nums:
            self._counters.append(new_counters.get(step_num, {}))

    def _fetch_counters_ssh(self, step_nums):
        uris = list(self.ls_job_logs_ssh(step_nums))
        log.info('Fetching counters from SSH...')
        return scan_for_counters_in_files(uris, self,
                                          self.get_hadoop_version())

    def _fetch_counters_s3(self, step_nums, skip_s3_wait=False):
        job_flow = self._describe_jobflow()
        if job_flow.keepjobflowalivewhennosteps == 'true':
            log.info("Can't fetch counters from S3 for five more minutes. Try"
                     " 'python -m mrjob.tools.emr.fetch_logs --counters %s'"
                     " in five minutes." % job_flow.jobflowid)
            return {}

        log.info('Fetching counters from S3...')

        if not skip_s3_wait:
            self._wait_for_s3_eventual_consistency()
        self._wait_for_job_flow_termination()

        try:
            uris = self.ls_job_logs_s3(step_nums)
            return scan_for_counters_in_files(uris, self,
                                              self.get_hadoop_version())
        except LogFetchError, e:
            log.info("Unable to fetch counters: %s" % e)
            return {}

    def counters(self):
        return self._counters

    def _find_probable_cause_of_failure(self, step_nums):
        """Scan logs for Python exception tracebacks.

        Args:
        step_nums -- the numbers of steps belonging to us, so that we
            can ignore errors from other jobs when sharing a job flow

        Returns:
        None (nothing found) or a dictionary containing:
        lines -- lines in the log file containing the error message
        log_file_uri -- the log file containing the error message
        input_uri -- if the error happened in a mapper in the first
            step, the URI of the input file that caused the error
            (otherwise None)
        """
        if self._opts['ec2_key_pair_file']:
            try:
                return self._find_probable_cause_of_failure_ssh(step_nums)
            except LogFetchError:
                return self._find_probable_cause_of_failure_s3(step_nums)
        else:
            log.info('ec2_key_pair_file not specified, going to S3')
            return self._find_probable_cause_of_failure_s3(step_nums)

    def _find_probable_cause_of_failure_ssh(self, step_nums):
        task_attempt_logs = self.ls_task_attempt_logs_ssh(step_nums)
        step_logs = self.ls_step_logs_ssh(step_nums)
        job_logs = self.ls_job_logs_ssh(step_nums)
        log.info('Scanning SSH logs for probable cause of failure')
        return scan_logs_in_order(task_attempt_logs=task_attempt_logs,
                                  step_logs=step_logs,
                                  job_logs=job_logs,
                                  runner=self)

    def _find_probable_cause_of_failure_s3(self, step_nums):
        log.info('Scanning S3 logs for probable cause of failure')
        self._wait_for_s3_eventual_consistency()
        self._wait_for_job_flow_termination()

        task_attempt_logs = self.ls_task_attempt_logs_s3(step_nums)
        step_logs = self.ls_step_logs_s3(step_nums)
        job_logs = self.ls_job_logs_s3(step_nums)
        return scan_logs_in_order(task_attempt_logs=task_attempt_logs,
                                  step_logs=step_logs,
                                  job_logs=job_logs,
                                  runner=self)

    ### Bootstrapping ###

    def _create_master_bootstrap_script(self, dest='b.py'):
        """Create the master bootstrap script and write it into our local
        temp directory.

        This will do nothing if there are no bootstrap scripts or commands,
        or if _create_master_bootstrap_script() has already been called."""
        # we call the script b.py because there's a character limit on
        # bootstrap script names (or there was at one time, anyway)

        if not any(key.startswith('bootstrap_') and value
                   for (key, value) in self._opts.iteritems()):
            return

        # don't bother if we're not starting a job flow
        if self._opts['emr_job_flow_id']:
            return

        # Also don't bother if we're not pooling (and therefore don't need
        # to have a bootstrap script to attach to) and we're not bootstrapping
        # anything else
        if not (self._opts['pool_emr_job_flows'] or
            any(key.startswith('bootstrap_') and
                key != 'bootstrap_actions' and  # these are separate scripts
                value
                for (key, value) in self._opts.iteritems())):
            return

        if self._opts['bootstrap_mrjob']:
            if self._mrjob_tar_gz_file is None:
                self._mrjob_tar_gz_file = self._add_bootstrap_file(
                    self._create_mrjob_tar_gz() + '#')

        # need to know what files are called
        self._name_files()
        self._pick_s3_uris_for_files()

        path = os.path.join(self._get_local_tmp_dir(), dest)
        log.info('writing master bootstrap script to %s' % path)

        contents = self._master_bootstrap_script_content()
        for line in StringIO(contents):
            log.debug('BOOTSTRAP: ' + line.rstrip('\r\n'))

        f = open(path, 'w')
        f.write(contents)
        f.close()

        name, _ = self._split_path(path)
        self._master_bootstrap_script = {'path': path, 'name': name}
        self._files.append(self._master_bootstrap_script)

    def _master_bootstrap_script_content(self):
        """Create the contents of the master bootstrap script.

        This will give names and S3 URIs to files that don't already have them.

        This function does NOT pick S3 URIs for files or anything like
        that; _create_master_bootstrap_script() is responsible for that.
        """
        out = StringIO()

        python_bin_in_list = ', '.join(repr(opt) for opt in self._opts['python_bin'])

        def writeln(line=''):
            out.write(line + '\n')

        # shebang
        writeln('#!/usr/bin/python')
        writeln()

        # imports
        writeln('from __future__ import with_statement')
        writeln()
        writeln('import distutils.sysconfig')
        writeln('import os')
        writeln('import stat')
        writeln('from subprocess import call, check_call')
        writeln('from tempfile import mkstemp')
        writeln('from xml.etree.ElementTree import ElementTree')
        writeln()

        # download files using hadoop fs
        writeln('# download files using hadoop fs -copyToLocal')
        for file_dict in self._files:
            if file_dict.get('bootstrap'):
                writeln(
                    "check_call(['hadoop', 'fs', '-copyToLocal', %r, %r])" %
                    (file_dict['s3_uri'], file_dict['name']))
        writeln()

        # make scripts executable
        if self._bootstrap_scripts:
            writeln('# make bootstrap scripts executable')
            for file_dict in self._bootstrap_scripts:
                writeln("check_call(['chmod', 'a+rx', %r])" %
                        file_dict['name'])
            writeln()

        # bootstrap mrjob
        if self._opts['bootstrap_mrjob']:
            writeln('# bootstrap mrjob')
            writeln("site_packages = distutils.sysconfig.get_python_lib()")
            writeln(
                "check_call(['sudo', 'tar', 'xfz', %r, '-C', site_packages])" %
                self._mrjob_tar_gz_file['name'])
            # re-compile pyc files now, since mappers/reducers can't
            # write to this directory. Don't fail if there is extra
            # un-compileable crud in the tarball.
            writeln("mrjob_dir = os.path.join(site_packages, 'mrjob')")
            writeln("call(["
                    "'sudo', %s, '-m', 'compileall', '-f', mrjob_dir])" %
                    python_bin_in_list)
            writeln()

        # install our python modules
        if self._bootstrap_python_packages:
            writeln('# install python modules:')
            for file_dict in self._bootstrap_python_packages:
                writeln("check_call(['tar', 'xfz', %r])" %
                        file_dict['name'])
                # figure out name of dir to CD into
                assert file_dict['path'].endswith('.tar.gz')
                cd_into = extract_dir_for_tar(file_dict['path'])
                # install the module
                writeln("check_call(["
                        "'sudo', %s, 'setup.py', 'install'], cwd=%r)" %
                        (python_bin_in_list, cd_into))

        # run our commands
        if self._opts['bootstrap_cmds']:
            writeln('# run bootstrap cmds:')
            for cmd in self._opts['bootstrap_cmds']:
                if isinstance(cmd, basestring):
                    writeln('check_call(%r, shell=True)' % cmd)
                else:
                    writeln('check_call(%r)' % cmd)
            writeln()

        # run our scripts
        if self._bootstrap_scripts:
            writeln('# run bootstrap scripts:')
            for file_dict in self._bootstrap_scripts:
                writeln('check_call(%r)' % (
                    ['./' + file_dict['name']],))
            writeln()

        return out.getvalue()

    ### EMR JOB MANAGEMENT UTILS ###

    def make_persistent_job_flow(self):
        """Create a new EMR job flow that requires manual termination, and
        return its ID.

        You can also fetch the job ID by calling self.get_emr_job_flow_id()
        """
        if (self._emr_job_flow_id):
            raise AssertionError(
                'This runner is already associated with job flow ID %s' %
                (self._emr_job_flow_id))

        log.info('Creating persistent job flow to run several jobs in...')

        self._create_master_bootstrap_script()
        self._upload_non_input_files()

        # don't allow user to call run()
        self._ran_job = True

        self._emr_job_flow_id = self._create_job_flow(persistent=True)

        return self._emr_job_flow_id

    def get_emr_job_flow_id(self):
        return self._emr_job_flow_id

    def usable_job_flows(self, emr_conn=None, exclude=None, num_steps=1):
        """Get job flows that this runner can use.

        We basically expect to only join available job flows with the exact
        same setup as our own, that is:

        - same bootstrap setup (including mrjob version)
        - have the same Hadoop and AMI version
        - same number and type of instances

        However, we allow joining job flows where for each role, every instance
        has at least as much memory as we require, and the total number of
        compute units is at least what we require.

        There also must be room for our job in the job flow (job flows top out
        at 256 steps).

        We then sort by:
        - total compute units for core + task nodes
        - total compute units for master node
        - time left to an even instance hour

        The most desirable job flows come *last* in the list.

        :return: list of (job_minutes_float,
                 :py:class:`botoemr.emrobject.JobFlow`)
        """
        emr_conn = emr_conn or self.make_emr_conn()
        exclude = exclude or set()

        req_hash = self._pool_hash()

        # decide memory and total compute units requested for each
        # role type
        role_to_req_instance_type = {}
        role_to_req_num_instances = {}
        role_to_req_mem = {}
        role_to_req_cu = {}
        role_to_req_bid_price = {}

        for role in ('core', 'master', 'task'):
            instance_type = self._opts['ec2_%s_instance_type' % role]
            if role == 'master':
                num_instances = 1
            else:
                num_instances = self._opts['num_ec2_%s_instances' % role]

            role_to_req_instance_type[role] = instance_type
            role_to_req_num_instances[role] = num_instances

            role_to_req_bid_price[role] = (
                self._opts['ec2_%s_instance_bid_price' % role])

            # unknown instance types can only match themselves
            role_to_req_mem[role] = (
                EC2_INSTANCE_TYPE_TO_MEMORY.get(instance_type, float('Inf')))
            role_to_req_cu[role] = (
                num_instances *
                EC2_INSTANCE_TYPE_TO_COMPUTE_UNITS.get(instance_type,
                                                       float('Inf')))

        sort_keys_and_job_flows = []

        def add_if_match(job_flow):
            # this may be a retry due to locked job flows
            if job_flow.jobflowid in exclude:
                return

            # only take persistent job flows
            if job_flow.keepjobflowalivewhennosteps != 'true':
                return

            # match pool name, and (bootstrap) hash
            hash, name = pool_hash_and_name(job_flow)
            if req_hash != hash:
                return

            if self._opts['emr_job_flow_pool_name'] != name:
                return

            # match hadoop version
            if job_flow.hadoopversion != self.get_hadoop_version():
                return

            # match AMI version
            job_flow_ami_version = getattr(job_flow, 'amiversion', None)
            if job_flow_ami_version != self._opts['ami_version']:
                return

            # there is a hard limit of 256 steps per job flow
            if len(job_flow.steps) + num_steps > MAX_STEPS_PER_JOB_FLOW:
                return

            # in rare cases, job flow can be WAITING *and* have incomplete
            # steps
            if any(getattr(step, 'enddatetime', None) is None
                   for step in job_flow.steps):
                return

            # total compute units per group
            role_to_cu = defaultdict(float)
            # total number of instances of the same type in each group.
            # This allows us to match unknown instance types.
            role_to_matched_instances = defaultdict(int)

            # check memory and compute units, bailing out if we hit
            # an instance with too little memory
            for ig in job_flow.instancegroups:
                role = ig.instancerole.lower()

                # unknown, new kind of role; bail out!
                if role not in ('core', 'master', 'task'):
                    return

                req_instance_type = role_to_req_instance_type[role]
                if ig.instancetype != req_instance_type:
                    # if too little memory, bail out
                    mem = EC2_INSTANCE_TYPE_TO_MEMORY.get(ig.instancetype, 0.0)
                    req_mem = role_to_req_mem.get(role, 0.0)
                    if mem < req_mem:
                        return

                # if bid price is too low, don't count compute units
                req_bid_price = role_to_req_bid_price[role]
                bid_price = getattr(ig, 'bidprice', None)

                # if the instance is on-demand (no bid price) or bid prices
                # are the same, we're okay
                if bid_price and bid_price != req_bid_price:
                    # whoops, we didn't want spot instances at all
                    if not req_bid_price:
                        continue

                    try:
                        if float(req_bid_price) > float(bid_price):
                            continue
                    except ValueError:
                        # we don't know what to do with non-float bid prices,
                        # and we know it's not equal to what we requested
                        continue

                # don't require instances to be running; we'd be worse off if
                # we started our own job flow from scratch. (This can happen if
                # the previous job finished while some task instances were
                # still being provisioned.)
                cu = (int(ig.instancerequestcount) *
                      EC2_INSTANCE_TYPE_TO_COMPUTE_UNITS.get(
                          ig.instancetype, 0.0))
                role_to_cu.setdefault(role, 0.0)
                role_to_cu[role] += cu

                # track number of instances of the same type
                if ig.instancetype == req_instance_type:
                    role_to_matched_instances[role] += (
                        int(ig.instancerequestcount))

            # check if there are enough compute units
            for role, req_cu in role_to_req_cu.iteritems():
                req_num_instances = role_to_req_num_instances[role]
                # if we have at least as many units of the right type,
                # don't bother counting compute units
                if req_num_instances > role_to_matched_instances[role]:
                    cu = role_to_cu.get(role, 0.0)
                    if cu < req_cu:
                        return

            # make a sort key
            sort_key = (role_to_cu['core'] + role_to_cu['task'],
                        role_to_cu['master'],
                        est_time_to_hour(job_flow))

            sort_keys_and_job_flows.append((sort_key, job_flow))

        for job_flow in emr_conn.describe_jobflows(states=['WAITING']):
            add_if_match(job_flow)

        return [job_flow for (sort_key, job_flow)
                in sorted(sort_keys_and_job_flows)]

    def find_job_flow(self, num_steps=1):
        """Find a job flow that can host this runner. Prefer flows with more
        compute units. Break ties by choosing flow with longest idle time.
        Return ``None`` if no suitable flows exist.
        """
        chosen_job_flow = None
        exclude = set()
        emr_conn = self.make_emr_conn()
        s3_conn = self.make_s3_conn()
        while chosen_job_flow is None:
            sorted_tagged_job_flows = self.usable_job_flows(
                emr_conn=emr_conn,
                exclude=exclude,
                num_steps=num_steps)
            if sorted_tagged_job_flows:
                job_flow = sorted_tagged_job_flows[-1]
                status = attempt_to_acquire_lock(
                    s3_conn, self._lock_uri(job_flow),
                    self._opts['s3_sync_wait_time'], self._job_name)
                if status:
                    return sorted_tagged_job_flows[-1]
                else:
                    exclude.add(job_flow.jobflowid)
            else:
                return None

    def _lock_uri(self, job_flow):
        return make_lock_uri(self._opts['s3_scratch_uri'],
                             job_flow.jobflowid,
                             len(job_flow.steps) + 1)

    def _pool_hash(self):
        """Generate a hash of the bootstrap configuration so it can be used to
        match jobs and job flows. This first argument passed to the bootstrap
        script will be ``'pool-'`` plus this hash.
        """
        def should_include_file(info):
            # Bootstrap scripts will always have a different checksum
            if 'name' in info and info['name'] in ('b.py', 'wrapper.py'):
                return False

            # Also do not include script used to spin up job
            if self._script and info['path'] == self._script['path']:
                return False

            # Only include bootstrap files
            if 'bootstrap' not in info:
                return False

            # mrjob.tar.gz is covered by the bootstrap_mrjob variable.
            # also, it seems to be different every time, causing an
            # undesirable hash mismatch.
            if (self._opts['bootstrap_mrjob']
                and info is self._mrjob_tar_gz_file):
                return False

            # Ignore job-specific files
            if info['path'] in self._input_paths:
                return False

            return True

        # strip unique s3 URI if there is one
        cleaned_bootstrap_actions = [dict(path=fd['path'], args=fd['args'])
                                     for fd in self._bootstrap_actions]

        things_to_hash = [
            [self.md5sum(fd['path'])
             for fd in self._files if should_include_file(fd)],
            self._opts['additional_emr_info'],
            self._opts['bootstrap_mrjob'],
            self._opts['bootstrap_cmds'],
            cleaned_bootstrap_actions,
        ]
        if self._opts['bootstrap_mrjob']:
            things_to_hash.append(mrjob.__version__)
        return hash_object(things_to_hash)

    ### GENERAL FILESYSTEM STUFF ###

    def du(self, path_glob):
        """Get the size of all files matching path_glob."""
        if not is_s3_uri(path_glob):
            return super(EMRJobRunner, self).du(path_glob)

        return sum(self.get_s3_key(uri).size for uri in self.ls(path_glob))

    def ls(self, path_glob):
        """Recursively list files locally or on S3.

        This doesn't list "directories" unless there's actually a
        corresponding key ending with a '/' (which is weird and confusing;
        don't make S3 keys ending in '/')

        To list a directory, path_glob must end with a trailing
        slash (foo and foo/ are different on S3)
        """
        if SSH_URI_RE.match(path_glob):
            for item in self._ssh_ls(path_glob):
                yield item
            return

        if not is_s3_uri(path_glob):
            for path in super(EMRJobRunner, self).ls(path_glob):
                yield path
            return

        # support globs
        glob_match = GLOB_RE.match(path_glob)

        # if it's a "file" (doesn't end with /), just check if it exists
        if not glob_match and not path_glob.endswith('/'):
            uri = path_glob
            if self.get_s3_key(uri):
                yield uri
            return

        # we're going to search for all keys starting with base_uri
        if glob_match:
            # cut it off at first wildcard
            base_uri = glob_match.group(1)
        else:
            base_uri = path_glob

        for uri in self._s3_ls(base_uri):
            # enforce globbing
            if glob_match and not fnmatch.fnmatchcase(uri, path_glob):
                continue

            yield uri

    def _ssh_ls(self, uri):
        """Helper for ls(); obeys globbing"""
        m = SSH_URI_RE.match(uri)
        try:
            addr = m.group('hostname') or self._address_of_master()
            if '!' in addr:
                self._enable_slave_ssh_access()
            output = ssh_ls(
                self._opts['ssh_bin'],
                addr,
                self._opts['ec2_key_pair_file'],
                m.group('filesystem_path'),
                self._ssh_key_name,
            )
            for line in output:
                # skip directories, we only want to return downloadable files
                if line and not line.endswith('/'):
                    yield SSH_PREFIX + addr + line
        except SSHException, e:
            raise LogFetchError(e)

    def _s3_ls(self, uri):
        """Helper for ls(); doesn't bother with globbing or directories"""
        s3_conn = self.make_s3_conn()
        bucket_name, key_name = parse_s3_uri(uri)

        bucket = s3_conn.get_bucket(bucket_name)
        for key in bucket.list(key_name):
            yield s3_key_to_uri(key)

    def md5sum(self, path, s3_conn=None):
        if is_s3_uri(path):
            k = self.get_s3_key(path, s3_conn=s3_conn)
            return k.etag.strip('"')
        else:
            return super(EMRJobRunner, self).md5sum(path)

    def _cat_file(self, filename):
        ssh_match = SSH_URI_RE.match(filename)
        if is_s3_uri(filename):
            # stream lines from the s3 key
            s3_key = self.get_s3_key(filename)
            buffer_iterator = read_file(s3_key_to_uri(s3_key), fileobj=s3_key)
            return buffer_iterator_to_line_iterator(buffer_iterator)
        elif ssh_match:
            try:
                addr = ssh_match.group('hostname') or self._address_of_master()
                if '!' in addr:
                    self._enable_slave_ssh_access()
                output = ssh_cat(
                    self._opts['ssh_bin'],
                    addr,
                    self._opts['ec2_key_pair_file'],
                    ssh_match.group('filesystem_path'),
                    self._ssh_key_name,
                )
                return read_file(filename, fileobj=StringIO(output))
            except SSHException, e:
                raise LogFetchError(e)
        else:
            # read from local filesystem
            return super(EMRJobRunner, self)._cat_file(filename)

    def mkdir(self, dest):
        """Make a directory. This does nothing on S3 because there are
        no directories.
        """
        if not is_s3_uri(dest):
            super(EMRJobRunner, self).mkdir(dest)

    def path_exists(self, path_glob):
        """Does the given path exist?

        If dest is a directory (ends with a "/"), we check if there are
        any files starting with that path.
        """
        if not is_s3_uri(path_glob):
            return super(EMRJobRunner, self).path_exists(path_glob)

        # just fall back on ls(); it's smart
        return any(self.ls(path_glob))

    def path_join(self, dirname, filename):
        if is_s3_uri(dirname):
            return posixpath.join(dirname, filename)
        else:
            return os.path.join(dirname, filename)

    def rm(self, path_glob):
        """Remove all files matching the given glob."""
        if not is_s3_uri(path_glob):
            return super(EMRJobRunner, self).rm(path_glob)

        s3_conn = self.make_s3_conn()
        for uri in self.ls(path_glob):
            key = self.get_s3_key(uri, s3_conn)
            if key:
                log.debug('deleting ' + uri)
                key.delete()

            # special case: when deleting a directory, also clean up
            # the _$folder$ files that EMR creates.
            if uri.endswith('/'):
                folder_uri = uri[:-1] + '_$folder$'
                folder_key = self.get_s3_key(folder_uri, s3_conn)
                if folder_key:
                    log.debug('deleting ' + folder_uri)
                    folder_key.delete()

    def touchz(self, dest):
        """Make an empty file in the given location. Raises an error if
        a non-empty file already exists in that location."""
        if not is_s3_uri(dest):
            super(EMRJobRunner, self).touchz(dest)

        key = self.get_s3_key(dest)
        if key and key.size != 0:
            raise OSError('Non-empty file %r already exists!' % (dest,))

        self.make_s3_key(dest).set_contents_from_string('')

    ### EMR-specific STUFF ###

    def _wrap_aws_conn(self, raw_conn):
        """Wrap a given boto Connection object so that it can retry when
        throttled."""
        def retry_if(ex):
            """Retry if we get a server error indicating throttling. Also
            handle spurious 505s that are thought to be part of a load
            balancer issue inside AWS."""
            return ((isinstance(ex, boto.exception.BotoServerError) and
                     ('Throttling' in ex.body or
                      'RequestExpired' in ex.body or
                      ex.status == 505)) or
                    (isinstance(ex, socket.error) and
                     ex.args in ((104, 'Connection reset by peer'),
                                 (110, 'Connection timed out'))))

        return RetryWrapper(raw_conn,
                            retry_if=retry_if,
                            backoff=EMR_BACKOFF,
                            multiplier=EMR_BACKOFF_MULTIPLIER,
                            max_tries=EMR_MAX_TRIES)

    def make_emr_conn(self):
        """Create a connection to EMR.

        :return: a :py:class:`mrjob.boto_2_1_1_83aae37b.EmrConnection`, a
                 subclass of :py:class:`boto.emr.connection.EmrConnection`,
                 wrapped in a :py:class:`mrjob.retry.RetryWrapper`
        """
        # ...which is then wrapped in bacon! Mmmmm!

        # give a non-cryptic error message if boto isn't installed
        if boto is None:
            raise ImportError('You must install boto to connect to EMR')

        region = self._get_region_info_for_emr_conn()
        log.debug('creating EMR connection (to %s)' % region.endpoint)

        raw_emr_conn = boto_2_1_1_83aae37b.EmrConnection(
            aws_access_key_id=self._opts['aws_access_key_id'],
            aws_secret_access_key=self._opts['aws_secret_access_key'],
            region=region)
        return self._wrap_aws_conn(raw_emr_conn)

    def _get_region_info_for_emr_conn(self):
        """Get a :py:class:`boto.ec2.regioninfo.RegionInfo` object to
        initialize EMR connections with.

        This is kind of silly because all
        :py:class:`boto.emr.connection.EmrConnection` ever does with
        this object is extract the hostname, but that's how boto rolls.
        """
        if self._opts['emr_endpoint']:
            endpoint = self._opts['emr_endpoint']
        else:
            # look up endpoint in our table
            try:
                endpoint = REGION_TO_EMR_ENDPOINT[self._aws_region]
            except KeyError:
                raise Exception(
                    "Don't know the EMR endpoint for %s;"
                    " try setting emr_endpoint explicitly" % self._aws_region)

        return boto.ec2.regioninfo.RegionInfo(None, self._aws_region, endpoint)

    def _describe_jobflow(self, emr_conn=None):
        emr_conn = emr_conn or self.make_emr_conn()
        return emr_conn.describe_jobflow(self._emr_job_flow_id)

    def get_hadoop_version(self):
        if not self._inferred_hadoop_version:
            if self._emr_job_flow_id:
                # if joining a job flow, infer the version
                self._inferred_hadoop_version = (
                    self._describe_jobflow().hadoopversion)
            else:
                # otherwise, read it from hadoop_version/ami_version
                hadoop_version = self._opts['hadoop_version']
                if hadoop_version:
                    self._inferred_hadoop_version = hadoop_version
                else:
                    ami_version = self._opts['ami_version']
                    # don't explode if we see an AMI version that's
                    # newer than what we know about.
                    self._inferred_hadoop_version = (
                        AMI_VERSION_TO_HADOOP_VERSION.get(ami_version) or
                        AMI_VERSION_TO_HADOOP_VERSION['latest'])

        return self._inferred_hadoop_version

    def _address_of_master(self, emr_conn=None):
        """Get the address of the master node so we can SSH to it"""
        # cache address of master to avoid redundant calls to describe_jobflow
        # also convenient for testing (pretend we can SSH when we really can't
        # by setting this to something not False)
        if self._address:
            return self._address

        try:
            jobflow = self._describe_jobflow(emr_conn)
            if jobflow.state not in ('WAITING', 'RUNNING'):
                raise LogFetchError(
                    'Cannot ssh to master; job flow is not waiting or running')
        except boto.exception.S3ResponseError:
            # This error is raised by mockboto when the jobflow doesn't exist
            raise LogFetchError('Could not get job flow information')

        self._address = jobflow.masterpublicdnsname
        return self._address

    def _addresses_of_slaves(self):
        if not self._ssh_slave_addrs:
            self._ssh_slave_addrs = ssh_slave_addresses(
                self._opts['ssh_bin'],
                self._address_of_master(),
                self._opts['ec2_key_pair_file'])
        return self._ssh_slave_addrs

    ### S3-specific FILESYSTEM STUFF ###

    # Utilities for interacting with S3 using S3 URIs.

    # Try to use the more general filesystem interface unless you really
    # need to do something S3-specific (e.g. setting file permissions)

    def make_s3_conn(self):
        """Create a connection to S3.

        :return: a :py:class:`boto.s3.connection.S3Connection`, wrapped in a
                 :py:class:`mrjob.retry.RetryWrapper`
        """
        # give a non-cryptic error message if boto isn't installed
        if boto is None:
            raise ImportError('You must install boto to connect to S3')

        s3_endpoint = self._get_s3_endpoint()
        log.debug('creating S3 connection (to %s)' % s3_endpoint)

        raw_s3_conn = boto.connect_s3(
            aws_access_key_id=self._opts['aws_access_key_id'],
            aws_secret_access_key=self._opts['aws_secret_access_key'],
            host=s3_endpoint)
        return self._wrap_aws_conn(raw_s3_conn)

    def _get_s3_endpoint(self):
        if self._opts['s3_endpoint']:
            return self._opts['s3_endpoint']
        else:
            # look it up in our table
            try:
                return REGION_TO_S3_ENDPOINT[self._aws_region]
            except KeyError:
                raise Exception(
                    "Don't know the S3 endpoint for %s;"
                    " try setting s3_endpoint explicitly" % self._aws_region)

    def get_s3_key(self, uri, s3_conn=None):
        """Get the boto Key object matching the given S3 uri, or
        return None if that key doesn't exist.

        uri is an S3 URI: ``s3://foo/bar``

        You may optionally pass in an existing s3 connection through
        ``s3_conn``.
        """
        if not s3_conn:
            s3_conn = self.make_s3_conn()
        bucket_name, key_name = parse_s3_uri(uri)

        return s3_conn.get_bucket(bucket_name).get_key(key_name)

    def make_s3_key(self, uri, s3_conn=None):
        """Create the given S3 key, and return the corresponding
        boto Key object.

        uri is an S3 URI: ``s3://foo/bar``

        You may optionally pass in an existing S3 connection through
        ``s3_conn``.
        """
        if not s3_conn:
            s3_conn = self.make_s3_conn()
        bucket_name, key_name = parse_s3_uri(uri)

        return s3_conn.get_bucket(bucket_name).new_key(key_name)

    def get_s3_keys(self, uri, s3_conn=None):
        """Get a stream of boto Key objects for each key inside
        the given dir on S3.

        uri is an S3 URI: ``s3://foo/bar``

        You may optionally pass in an existing S3 connection through s3_conn
        """
        if not s3_conn:
            s3_conn = self.make_s3_conn()

        bucket_name, key_prefix = parse_s3_uri(uri)
        bucket = s3_conn.get_bucket(bucket_name)
        for key in bucket.list(key_prefix):
            yield key

    def get_s3_folder_keys(self, uri, s3_conn=None):
        """Background: S3 is even less of a filesystem than HDFS in that it
        doesn't have directories. EMR fakes directories by creating special
        ``*_$folder$`` keys in S3.

        For example if your job outputs ``s3://walrus/tmp/output/part-00000``,
        EMR will also create these keys:

        - ``s3://walrus/tmp_$folder$``
        - ``s3://walrus/tmp/output_$folder$``

        If you want to grant another Amazon user access to your files so they
        can use them in S3, you must grant read access on the actual keys,
        plus any ``*_$folder$`` keys that "contain" your keys; otherwise
        EMR will error out with a permissions error.

        This gets all the ``*_$folder$`` keys associated with the given URI,
        as boto Key objects.

        This does not support globbing.

        You may optionally pass in an existing S3 connection through
        ``s3_conn``.
        """
        if not s3_conn:
            s3_conn = self.make_s3_conn()

        bucket_name, key_name = parse_s3_uri(uri)
        bucket = s3_conn.get_bucket(bucket_name)

        dirs = key_name.split('/')
        for i in range(len(dirs)):
            folder_name = '/'.join(dirs[:i]) + '_$folder$'
            key = bucket.get_key(folder_name)
            if key:
                yield key
