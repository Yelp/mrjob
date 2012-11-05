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

import logging
import os
import os.path
import posixpath
import random
import re
import signal
import socket
import time
import urllib2
from collections import defaultdict
from datetime import datetime
from datetime import timedelta
from subprocess import Popen
from subprocess import PIPE

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
    import boto.emr.connection
    import boto.emr.instance_group
    import boto.exception
    import boto.utils
    boto  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    # don't require boto; MRJobs don't actually need it when running
    # inside hadoop streaming
    boto = None

import mrjob
from mrjob.setup import BootstrapWorkingDirManager
from mrjob.setup import UploadDirManager
from mrjob.setup import parse_legacy_hash_path
from mrjob.compat import supports_new_distributed_cache_options
from mrjob.conf import combine_cmds
from mrjob.conf import combine_dicts
from mrjob.conf import combine_lists
from mrjob.conf import combine_paths
from mrjob.conf import combine_path_lists
from mrjob.fs.local import LocalFilesystem
from mrjob.fs.composite import CompositeFilesystem
from mrjob.fs.s3 import S3Filesystem
from mrjob.fs.s3 import wrap_aws_conn
from mrjob.fs.ssh import SSHFilesystem
from mrjob.logparsers import TASK_ATTEMPTS_LOG_URI_RE
from mrjob.logparsers import STEP_LOG_URI_RE
from mrjob.logparsers import EMR_JOB_LOG_URI_RE
from mrjob.logparsers import NODE_LOG_URI_RE
from mrjob.logparsers import best_error_from_logs
from mrjob.logparsers import scan_for_counters_in_files
from mrjob.parse import HADOOP_STREAMING_JAR_RE
from mrjob.parse import is_s3_uri
from mrjob.parse import is_uri
from mrjob.parse import parse_s3_uri
from mrjob.pool import est_time_to_hour
from mrjob.pool import pool_hash_and_name
from mrjob.runner import MRJobRunner
from mrjob.runner import RunnerOptionStore
from mrjob.ssh import ssh_copy_key
from mrjob.ssh import ssh_terminate_single_job
from mrjob.ssh import ssh_slave_addresses
from mrjob.ssh import SSH_PREFIX
from mrjob.ssh import SSH_LOG_ROOT
from mrjob.util import cmd_line
from mrjob.util import extract_dir_for_tar
from mrjob.util import hash_object
from mrjob.util import shlex_split


log = logging.getLogger('mrjob.emr')

JOB_TRACKER_RE = re.compile(r'(\d{1,3}\.\d{2})%')

# not all steps generate task attempt logs. for now, conservatively check for
# streaming steps, which always generate them.
LOG_GENERATING_STEP_NAME_RE = HADOOP_STREAMING_JAR_RE

# the port to tunnel to
EMR_JOB_TRACKER_PORT = 9100
EMR_JOB_TRACKER_PATH = '/jobtracker.jsp'

MAX_SSH_RETRIES = 20

# ssh should fail right away if it can't bind a port
WAIT_FOR_SSH_TO_FAIL = 1.0

# amount of time to wait between checks for available pooled job flows
JOB_FLOW_SLEEP_INTERVAL = 30.01  # Add .1 seconds so minutes arent spot on.

# sometimes AWS gives us seconds as a decimal, which we can't parse
# with boto.utils.ISO8601
SUBSECOND_RE = re.compile('\.[0-9]+')

# map from AWS region to EMR endpoint. See
# http://docs.amazonwebservices.com/general/latest/gr/rande.html#emr_region
REGION_TO_EMR_ENDPOINT = {
    'us-east-1': 'elasticmapreduce.us-east-1.amazonaws.com',
    'us-west-1': 'elasticmapreduce.us-west-1.amazonaws.com',
    'us-west-2': 'elasticmapreduce.us-west-2.amazonaws.com',
    'EU': 'elasticmapreduce.eu-west-1.amazonaws.com',  # for compatibility
    'eu-west-1': 'elasticmapreduce.eu-west-1.amazonaws.com',
    'ap-southeast-1': 'elasticmapreduce.ap-southeast-1.amazonaws.com',
    'ap-northeast-1': 'elasticmapreduce.ap-northeast-1.amazonaws.com',
    'sa-east-1': 'elasticmapreduce.sa-east-1.amazonaws.com',
    '': 'elasticmapreduce.amazonaws.com',  # when no region specified
}

# map from AWS region to S3 endpoint. See
# http://docs.amazonwebservices.com/general/latest/gr/rande.html#s3_region
REGION_TO_S3_ENDPOINT = {
    'us-east-1': 's3.amazonaws.com',  # no region-specific endpoint
    'us-west-1': 's3-us-west-1.amazonaws.com',
    'us-west-2': 's3-us-west-2.amazonaws.com',
    'EU': 's3-eu-west-1.amazonaws.com',
    'eu-west-1': 's3-eu-west-1.amazonaws.com',
    'ap-southeast-1': 's3-ap-southeast-1.amazonaws.com',
    'ap-northeast-1': 's3-ap-northeast-1.amazonaws.com',
    'sa-east-1': 's3-sa-east-1.amazonaws.com',
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
    None: '0.20.205',  # The default is 'latest' now, but you can still pass
                       # None in the runner kwargs, and that should default us
                       # to 'latest' in boto. But we really can't know, so
                       # caveat programmor.
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


class EMRRunnerOptionStore(RunnerOptionStore):

    ALLOWED_KEYS = RunnerOptionStore.ALLOWED_KEYS.union(set([
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
        'hadoop_streaming_jar_on_emr',
        'hadoop_version',
        'num_ec2_core_instances',
        'pool_wait_minutes',
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
    ]))

    COMBINERS = combine_dicts(RunnerOptionStore.COMBINERS, {
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

    def __init__(self, alias, opts, conf_path):
        super(EMRRunnerOptionStore, self).__init__(alias, opts, conf_path)
        self._fix_ec2_instance_opts()

    def default_options(self):
        super_opts = super(EMRRunnerOptionStore, self).default_options()
        return combine_dicts(super_opts, {
            'ami_version': 'latest',
            'check_emr_status_every': 30,
            'ec2_core_instance_type': 'm1.small',
            'ec2_master_instance_type': 'm1.small',
            'emr_job_flow_pool_name': 'default',
            'hadoop_version': None,
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

    def _fix_ec2_instance_opts(self):
        """If the *ec2_instance_type* option is set, override instance
        type for the nodes that actually run tasks (see Issue #66). Allow
        command-line arguments to override defaults and arguments
        in mrjob.conf (see Issue #311).

        Also, make sure that core and slave instance type are the same,
        total number of instances matches number of master, core, and task
        instances, and that bid prices of zero are converted to None.
        """
        # Make sure slave and core instance type have the same value
        # Within EMRJobRunner we only ever use ec2_core_instance_type,
        # but we want ec2_slave_instance_type to be correct in the
        # options dictionary.
        if (self['ec2_slave_instance_type'] and
            (self._opt_priority['ec2_slave_instance_type'] >
             self._opt_priority['ec2_core_instance_type'])):
            self['ec2_core_instance_type'] = (
                self['ec2_slave_instance_type'])
        else:
            self['ec2_slave_instance_type'] = (
                self['ec2_core_instance_type'])

        # If task instance type is not set, use core instance type
        # (This is mostly so that we don't inadvertently join a pool
        # with task instance types with too little memory.)
        if not self['ec2_task_instance_type']:
            self['ec2_task_instance_type'] = (
                self['ec2_core_instance_type'])

        # Within EMRJobRunner, we use num_ec2_core_instances and
        # num_ec2_task_instances, not num_ec2_instances. (Number
        # of master instances is always 1.)
        if (self._opt_priority['num_ec2_instances'] >
            max(self._opt_priority['num_ec2_core_instances'],
                self._opt_priority['num_ec2_task_instances'])):
            # assume 1 master, n - 1 core, 0 task
            self['num_ec2_core_instances'] = (
                self['num_ec2_instances'] - 1)
            self['num_ec2_task_instances'] = 0
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
            self['num_ec2_instances'] = (
                1 +
                self['num_ec2_core_instances'] +
                self['num_ec2_task_instances'])

        # Allow ec2 instance type to override other instance types
        ec2_instance_type = self['ec2_instance_type']
        if ec2_instance_type:
            # core (slave) instances
            if (self._opt_priority['ec2_instance_type'] >
                max(self._opt_priority['ec2_core_instance_type'],
                    self._opt_priority['ec2_slave_instance_type'])):
                self['ec2_core_instance_type'] = ec2_instance_type
                self['ec2_slave_instance_type'] = ec2_instance_type

            # master instance only does work when it's the only instance
            if (self['num_ec2_core_instances'] <= 0 and
                self['num_ec2_task_instances'] <= 0 and
                (self._opt_priority['ec2_instance_type'] >
                 self._opt_priority['ec2_master_instance_type'])):
                self['ec2_master_instance_type'] = ec2_instance_type

            # task instances
            if (self._opt_priority['ec2_instance_type'] >
                self._opt_priority['ec2_task_instance_type']):
                self['ec2_task_instance_type'] = ec2_instance_type

        # convert a bid price of '0' to None
        for role in ('core', 'master', 'task'):
            opt_name = 'ec2_%s_instance_bid_price' % role
            if not self[opt_name]:
                self[opt_name] = None
            else:
                # convert "0", "0.00" etc. to None
                try:
                    value = float(self[opt_name])
                    if value == 0:
                        self[opt_name] = None
                except ValueError:
                    pass  # maybe EMR will accept non-floats?


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
    """
    alias = 'emr'

    OPTION_STORE_CLASS = EMRRunnerOptionStore

    def __init__(self, **kwargs):
        """:py:class:`~mrjob.emr.EMRJobRunner` takes the same arguments as
        :py:class:`~mrjob.runner.MRJobRunner`, plus some additional options
        which can be defaulted in :ref:`mrjob.conf <mrjob.conf>`.

        *aws_access_key_id* and *aws_secret_access_key* are required if you
        haven't set them up already for boto (e.g. by setting the environment
        variables :envvar:`AWS_ACCESS_KEY_ID` and
        :envvar:`AWS_SECRET_ACCESS_KEY`)

        A lengthy list of additional options can be found in
        :doc:`guides/emr-opts.rst`.
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

        # pick a tmp dir based on the job name
        self._s3_tmp_uri = self._opts['s3_scratch_uri'] + self._job_name + '/'

        # pick/validate output dir
        if self._output_dir:
            self._output_dir = self._check_and_fix_s3_dir(self._output_dir)
        else:
            self._output_dir = self._s3_tmp_uri + 'output/'

        # manage working dir for bootstrap script
        self._bootstrap_dir_mgr = BootstrapWorkingDirManager()

        # manage local files that we want to upload to S3. We'll add them
        # to this manager just before we need them.
        s3_files_dir = self._s3_tmp_uri + 'files/'
        self._upload_mgr = UploadDirManager(s3_files_dir)

        # add the bootstrap files to a list of files to upload
        self._bootstrap_actions = []
        for action in self._opts['bootstrap_actions']:
            args = shlex_split(action)
            if not args:
                raise ValueError('bad bootstrap action: %r' % (action,))
            # don't use _add_bootstrap_file() because this is a raw bootstrap
            self._bootstrap_actions.append({
                'path': args[0],
                'args': args[1:],
            })

        for path in self._opts['bootstrap_files']:
            self._bootstrap_dir_mgr.add(**parse_legacy_hash_path(
                'file', path, must_name='bootstrap_files'))

        self._bootstrap_scripts = []
        for path in self._opts['bootstrap_scripts']:
            bootstrap_script = parse_legacy_hash_path('file', path)
            self._bootstrap_scripts.append(bootstrap_script)
            self._bootstrap_dir_mgr.add(**bootstrap_script)

        self._bootstrap_python_packages = []
        for path in self._opts['bootstrap_python_packages']:
            bpp = parse_legacy_hash_path('file', path)

            if not bpp['path'].endswith('.tar.gz'):
                raise ValueError(
                    'bootstrap_python_packages only accepts .tar.gz files!')
            self._bootstrap_python_packages.append(bpp)
            self._bootstrap_dir_mgr.add(**bpp)

        if not (isinstance(self._opts['additional_emr_info'], basestring)
                or self._opts['additional_emr_info'] is None):
            self._opts['additional_emr_info'] = json.dumps(
                self._opts['additional_emr_info'])

        # where our own logs ended up (we'll find this out once we run the job)
        self._s3_job_log_uri = None

        # we'll create the script later
        self._master_bootstrap_script_path = None

        # the ID assigned by EMR to this job (might be None)
        self._emr_job_flow_id = self._opts['emr_job_flow_id']

        # when did our particular task start?
        self._emr_job_start = None

        # ssh state
        self._ssh_proc = None
        self._gave_cant_ssh_warning = False
        # we don't upload the ssh key to master until it's needed
        self._ssh_key_is_copied = False

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

    @property
    def _ssh_key_name(self):
        return self._job_name + '.pem'

    @property
    def fs(self):
        """:py:class:`~mrjob.fs.base.Filesystem` object for SSH, S3, and the
        local filesystem.
        """
        if self._fs is None:
            if self._opts['s3_endpoint']:
                s3_endpoint = self._opts['s3_endpoint']
            else:
                # look it up in our table
                try:
                    s3_endpoint = REGION_TO_S3_ENDPOINT[self._aws_region]
                except KeyError:
                    raise Exception(
                        "Don't know the S3 endpoint for %s;"
                        " try setting s3_endpoint explicitly" % (
                            self._aws_region))

            self._s3_fs = S3Filesystem(self._opts['aws_access_key_id'],
                                       self._opts['aws_secret_access_key'],
                                       s3_endpoint)

            if self._opts['ec2_key_pair_file']:
                self._ssh_fs = SSHFilesystem(self._opts['ssh_bin'],
                                             self._opts['ec2_key_pair_file'],
                                             self._ssh_key_name)
                self._fs = CompositeFilesystem(self._ssh_fs, self._s3_fs,
                                               LocalFilesystem())
            else:
                self._ssh_fs = None
                self._fs = CompositeFilesystem(self._s3_fs, LocalFilesystem())

        return self._fs

    def _run(self):
        self._prepare_for_launch()

        self._launch_emr_job()
        self._wait_for_job_to_complete()

    def _prepare_for_launch(self):
        self._check_input_exists()
        self._create_wrapper_script()
        self._add_bootstrap_files_for_upload()
        self._add_job_files_for_upload()
        self._upload_local_files_to_s3()

    def _check_input_exists(self):
        """Make sure all input exists before continuing with our job.
        """
        for path in self._input_paths:
            if path == '-':
                continue  # STDIN always exists

            if is_uri(path) and not is_s3_uri(path):
                continue  # can't check non-S3 URIs, hope for the best

            if not self.path_exists(path):
                raise AssertionError(
                    'Input path %s does not exist!' % (path,))

    def _add_bootstrap_files_for_upload(self):
        """Add files needed by the bootstrap script to self._upload_mgr.

        Tar up mrjob if bootstrap_mrjob is True.

        Create the master bootstrap script if necessary.
        """
        # lazily create mrjob.tar.gz
        if self._opts['bootstrap_mrjob']:
            self._create_mrjob_tar_gz()
            self._bootstrap_dir_mgr.add('file', self._mrjob_tar_gz_path)

        # all other files needed by the script are already in _bootstrap_dir_mgr
        for path in self._bootstrap_dir_mgr.paths():
            self._upload_mgr.add(path)

        # now that we know where the above files live, we can create
        # the master bootstrap script
        self._create_master_bootstrap_script_if_needed()
        if self._master_bootstrap_script_path:
            self._upload_mgr.add(self._master_bootstrap_script_path)

        # finally, make sure bootstrap action scripts are on S3
        for bootstrap_action in self._bootstrap_actions:
            self._upload_mgr.add(bootstrap_action['path'])

    def _add_job_files_for_upload(self):
        """Add files needed for running the job (setup and input)
        to self._upload_mgr."""
        for path in self._get_input_paths():
            self._upload_mgr.add(path)

        for path in self._working_dir_mgr.paths():
            self._upload_mgr.add(path)

        if self._opts['hadoop_streaming_jar']:
            self._upload_mgr.add(path)

    def _upload_local_files_to_s3(self):
        """Copy local files tracked by self._upload_mgr to S3."""
        self._create_s3_temp_bucket_if_needed()

        log.info('Copying non-input files into %s' % self._upload_mgr.prefix)

        s3_conn = self.make_s3_conn()

        for path, s3_uri in self._upload_mgr.path_to_uri().iteritems():
            log.debug('uploading %s -> %s' % (path, s3_uri))
            s3_key = self.make_s3_key(s3_uri, s3_conn)
            s3_key.set_contents_from_filename(path)

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
        if self._ssh_fs and not self._ssh_key_is_copied:
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

    def _cleanup_job(self):
        # kill the job if we won't be taking down the whole job flow
        if not (self._emr_job_flow_id or
                self._opts['emr_job_flow_id'] or
                self._opts['pool_emr_job_flows']):
            # we're taking down the job flow, don't bother
            return

        error_msg = ('Unable to kill job without terminating job flow and'
                     ' job is still running. You may wish to terminate it'
                     ' yourself with "python -m mrjob.tools.emr.terminate_job_'
                     'flow %s".' % self._emr_job_flow_id)

        try:
            addr = self._address_of_master()
        except IOError:
            return

        if not self._ran_job:
            try:
                log.info("Attempting to terminate job...")
                had_job = ssh_terminate_single_job(
                    self._opts['ssh_bin'],
                    addr,
                    self._opts['ec2_key_pair_file'])
                if had_job:
                    log.info("Succeeded in terminating job")
                else:
                    log.info("Job appears to have already been terminated")
            except IOError:
                log.info(error_msg)

    def _cleanup_job_flow(self):
        if not self._emr_job_flow_id:
            # If we don't have a job flow, then we can't terminate it.
            return

        emr_conn = self.make_emr_conn()
        try:
            log.info("Attempting to terminate job flow")
            emr_conn.terminate_jobflow(self._emr_job_flow_id)
        except Exception, e:
            # Something happened with boto and the user should know.
            log.exception(e)
            return
        log.info('Job flow %s successfully terminated' % self._emr_job_flow_id)

    def _wait_for_s3_eventual_consistency(self):
        """Sleep for a little while, to give S3 a chance to sync up.
        """
        log.info('Waiting %.1fs for S3 eventual consistency' %
                 self._opts['s3_sync_wait_time'])
        time.sleep(self._opts['s3_sync_wait_time'])

    def _job_flow_is_done(self, job_flow):
        return job_flow.state in ('TERMINATED', 'COMPLETED', 'FAILED',
                                  'SHUTTING_DOWN')

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
        while not self._job_flow_is_done(jobflow):
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

        return boto.emr.instance_group.InstanceGroup(
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

        for i, bootstrap_action in enumerate(self._bootstrap_actions):
            s3_uri = self._upload_mgr.uri(bootstrap_action['path'])
            bootstrap_action_args.append(
                boto.emr.BootstrapAction(
                'action %d' % i, s3_uri, bootstrap_action['args']))

        if self._master_bootstrap_script_path:
            master_bootstrap_script_args = []
            if self._opts['pool_emr_job_flows']:
                master_bootstrap_script_args = [
                    'pool-' + self._pool_hash(),
                    self._opts['emr_job_flow_pool_name'],
                ]
            bootstrap_action_args.append(
                boto.emr.BootstrapAction(
                    'master',
                    self._upload_mgr.uri(self._master_bootstrap_script_path),
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
        # quick, add the other steps before the job spins up and
        # then shuts itself down (in practice this takes several minutes)
        steps = self._get_steps()
        step_list = []

        for step_num, step in enumerate(steps):
            if step['type'] == 'streaming':
                step_list.append(
                    self._build_streaming_step(step, step_num, len(steps)))
            elif step['type'] == 'jar':
                step_list.append(
                    self._build_jar_step(step, step_num, len(steps)))

        return step_list

    @property
    def _action_on_failure(self):
        # don't terminate other people's job flows
        if (self._opts['emr_job_flow_id'] or
            self._opts['pool_emr_job_flows']):
            return 'CANCEL_AND_WAIT'
        else:
            return 'TERMINATE_JOB_FLOW'

    def _executable(self, steps=False):
        # detect executable files so we can discard the explicit interpreter if
        # possible
        if os.access(self._script_path, os.X_OK):
            if steps:
                return [os.path.abspath(self._script_path)]
            else:
                return ['./' +
                        self._working_dir_mgr.name('file', self._script_path)]
        else:
            return super(EMRJobRunner, self)._executable(steps)

    def _build_streaming_step(self, step, step_num, num_steps):
        streaming_step_kwargs = {
            'name': '%s: Step %d of %d' % (
                self._job_name, step_num + 1, num_steps),
            'input': self._s3_step_input_uris(step_num),
            'output': self._s3_step_output_uri(step_num),
            'jar': self._get_jar(),
            'action_on_failure': self._action_on_failure,
        }

        streaming_step_kwargs.update(self._cache_kwargs())

        streaming_step_kwargs['step_args'].extend(
            self._hadoop_conf_args(step, step_num, num_steps))

        mapper, combiner, reducer = (
            self._hadoop_streaming_commands(step, step_num))

        streaming_step_kwargs['mapper'] = mapper

        if combiner:
            streaming_step_kwargs['combiner'] = combiner

        streaming_step_kwargs['reducer'] = reducer

        return boto.emr.StreamingStep(**streaming_step_kwargs)

    def _build_jar_step(self, step):
        return boto.emr.JarStep(
            name=step['name'],
            jar=step['jar'],
            main_class=step['main_class'],
            step_args=step['step_args'],
            action_on_failure=self._action_on_failure)

    def _cache_kwargs(self):
        """Returns
        ``{'step_args': [..], 'cache_files': [..], 'cache_archives': [..])``,
        populating each according to the correct behavior for the current
        Hadoop version.

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

        if supports_new_distributed_cache_options(version):
            # boto doesn't support non-deprecated 0.20 options, so insert
            # them ourselves
            step_args.extend(self._new_upload_args(self._upload_mgr))
        else:
            cache_files.extend(
                self._arg_hash_paths('file', self._upload_mgr))
            cache_archives.extend(
                self._arg_hash_paths('archive', self._upload_mgr))

        return {
            'step_args': step_args,
            'cache_files': cache_files,
            'cache_archives': cache_archives,
        }

    def _get_jar(self):
        if self._opts['hadoop_streaming_jar']:
            return self._upload_mgr.uri(self._opts['hadoop_streaming_jar'])
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
        if self._opts['pool_emr_job_flows'] and not self._emr_job_flow_id:
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
            lg_step_num_mapping = {}

            steps = job_flow.steps or []
            latest_lg_step_num = 0
            for i, step in enumerate(steps):
                if LOG_GENERATING_STEP_NAME_RE.match(
                    posixpath.basename(step.jar)):
                    latest_lg_step_num += 1

                # ignore steps belonging to other jobs
                if not step.name.startswith(self._job_name):
                    continue

                step_nums.append(i + 1)
                if LOG_GENERATING_STEP_NAME_RE.match(
                    posixpath.basename(step.jar)):
                    lg_step_num_mapping[i + 1] = latest_lg_step_num

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
            self._fetch_counters(step_nums, lg_step_num_mapping)
            self.print_counters(range(1, len(step_nums) + 1))
        else:
            msg = 'Job on job flow %s failed with status %s: %s' % (
                job_flow.jobflowid, job_state, reason)
            log.error(msg)
            if self._s3_job_log_uri:
                log.info('Logs are in %s' % self._s3_job_log_uri)
            # look for a Python traceback
            cause = self._find_probable_cause_of_failure(
                step_nums, sorted(lg_step_num_mapping.values()))
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

    def _s3_step_input_uris(self, step_num):
        """Get the s3:// URIs for input for the given step."""
        if step_num == 0:
            return [self._upload_mgr.uri(path)
                    for path in self._get_input_paths()]
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

    def _ssh_path(self, relative):
        return SSH_PREFIX + self._address_of_master() + SSH_LOG_ROOT + '/' + relative

    def _ls_ssh_logs(self, relative_path):
        """List logs over SSH by path relative to log root directory"""
        try:
            self._enable_slave_ssh_access()
            log.debug('Search %s for logs' % self._ssh_path(relative_path))
            return self.ls(self._ssh_path(relative_path))
        except IOError, e:
            raise LogFetchError(e)

    def _ls_slave_ssh_logs(self, addr, relative_path):
        """List logs over multi-hop SSH by path relative to log root directory
        """
        self._enable_slave_ssh_access()
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
        self._enable_slave_ssh_access()
        return self._enforce_path_regexp(
            self._ls_ssh_logs('steps/'),
            STEP_LOG_URI_RE,
            step_nums)

    def ls_job_logs_ssh(self, step_nums):
        self._enable_slave_ssh_access()
        return self._enforce_path_regexp(self._ls_ssh_logs('history/'),
                                         EMR_JOB_LOG_URI_RE,
                                         step_nums)

    def ls_node_logs_ssh(self):
        self._enable_slave_ssh_access()
        all_paths = []
        for addr in self._addresses_of_slaves():
            logs = self._ls_slave_ssh_logs(addr, '')
            all_paths.extend(logs)
        return self._enforce_path_regexp(all_paths, NODE_LOG_URI_RE)

    def ls_all_logs_ssh(self):
        """List all log files in the log root directory"""
        return self._ls_ssh_logs('')

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

    def _fetch_counters(self, step_nums, lg_step_num_mapping=None,
                        skip_s3_wait=False):
        """Read Hadoop counters from S3.

        Args:
        step_nums -- the steps belonging to us, so that we can ignore counters
                     from other jobs when sharing a job flow
        """
        # empty list is a valid value for lg_step_nums, but it is an optional
        # parameter
        if lg_step_num_mapping is None:
            lg_step_num_mapping = dict((n, n) for n in step_nums)
        lg_step_nums = list(sorted(lg_step_num_mapping[k] for k in step_nums))

        self._counters = []
        new_counters = {}
        if self._opts['ec2_key_pair_file']:
            try:
                new_counters = self._fetch_counters_ssh(lg_step_nums)
            except LogFetchError:
                new_counters = self._fetch_counters_s3(
                    lg_step_nums, skip_s3_wait)
            except IOError:
                # Can get 'file not found' if test suite was lazy or Hadoop
                # logs moved. We shouldn't crash in either case.
                new_counters = self._fetch_counters_s3(
                    lg_step_nums, skip_s3_wait)
        else:
            log.info('ec2_key_pair_file not specified, going to S3')
            new_counters = self._fetch_counters_s3(lg_step_nums, skip_s3_wait)

        # step_nums is relative to the start of the job flow
        # we only want them relative to the job
        for step_num in step_nums:
            if step_num in lg_step_num_mapping:
                self._counters.append(
                    new_counters.get(lg_step_num_mapping[step_num], {}))
            else:
                self._counters.append({})

    def _fetch_counters_ssh(self, step_nums):
        uris = list(self.ls_job_logs_ssh(step_nums))
        log.info('Fetching counters from SSH...')
        return scan_for_counters_in_files(uris, self,
                                          self.get_hadoop_version())

    def _fetch_counters_s3(self, step_nums, skip_s3_wait=False):
        log.info('Fetching counters from S3...')

        if not skip_s3_wait:
            self._wait_for_s3_eventual_consistency()

        try:
            uris = self.ls_job_logs_s3(step_nums)
            results = scan_for_counters_in_files(uris, self,
                                                 self.get_hadoop_version())

            if not results:
                job_flow = self._describe_jobflow()
                if not self._job_flow_is_done(job_flow):
                    log.info("Counters may not have been uploaded to S3 yet."
                             " Try again in 5 minutes with:"
                             " mrjob fetch-logs --counters %s" %
                             job_flow.jobflowid)
            return results
        except LogFetchError, e:
            log.info("Unable to fetch counters: %s" % e)
            return {}

    def counters(self):
        return self._counters

    def _find_probable_cause_of_failure(self, step_nums, lg_step_nums=None):
        """Scan logs for Python exception tracebacks.

        :param step_nums: the numbers of steps belonging to us, so that we
                          can ignore errors from other jobs when sharing a job
                          flow
        :param lg_step_nums: "Log generating step numbers" - list of
                             (job flow step num, hadoop job num) mapping a job
                             flow step number to the number hadoop sees.
                             Necessary because not all steps generate task
                             attempt logs, and when there are steps that don't,
                             the number in the log path differs from the job
                             flow step number.

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
                return self._find_probable_cause_of_failure_ssh(
                    step_nums, lg_step_nums)
            except LogFetchError:
                return self._find_probable_cause_of_failure_s3(
                    step_nums, lg_step_nums)
        else:
            log.info('ec2_key_pair_file not specified, going to S3')
            return self._find_probable_cause_of_failure_s3(
                step_nums, lg_step_nums)

    def _find_probable_cause_of_failure_ssh(self, step_nums,
                                            lg_step_nums=None):
        # empty list is a valid value for lg_step_nums, but it is an optional
        # parameter
        if lg_step_nums is None:
            lg_step_nums = step_nums
        self._enable_slave_ssh_access()
        task_attempt_logs = self.ls_task_attempt_logs_ssh(step_nums)
        step_logs = self.ls_step_logs_ssh(lg_step_nums)
        job_logs = self.ls_job_logs_ssh(step_nums)
        log.info('Scanning SSH logs for probable cause of failure')
        return best_error_from_logs(self, task_attempt_logs, step_logs,
                                    job_logs)

    def _find_probable_cause_of_failure_s3(self, step_nums, lg_step_nums):
        # empty list is a valid value for lg_step_nums, but it is an optional
        # parameter
        if lg_step_nums is None:
            lg_step_nums = step_nums
        log.info('Scanning S3 logs for probable cause of failure')
        self._wait_for_s3_eventual_consistency()
        self._wait_for_job_flow_termination()

        task_attempt_logs = self.ls_task_attempt_logs_s3(step_nums)
        step_logs = self.ls_step_logs_s3(step_nums)
        job_logs = self.ls_job_logs_s3(lg_step_nums)
        return best_error_from_logs(self, task_attempt_logs, step_logs,
                                    job_logs)

    ### Bootstrapping ###

    def _create_master_bootstrap_script_if_needed(self):
        """Create the master bootstrap script and write it into our local
        temp directory. Set self._master_bootstrap_script_path.

        This will do nothing if there are no bootstrap scripts or commands,
        or if it has already been called."""
        if self._master_bootstrap_script_path:
            return

        # don't bother if we're not starting a job flow
        if self._opts['emr_job_flow_id']:
            return

        # Also don't bother if we're not bootstrapping
        if not any(key.startswith('bootstrap_') and
               key != 'bootstrap_actions' and  # these are separate scripts
               value
               for (key, value) in self._opts.iteritems()):
            return

        # we call the script b.py because there's a character limit on
        # bootstrap script names (or there was at one time, anyway)
        path = os.path.join(self._get_local_tmp_dir(), 'b.py')
        log.info('writing master bootstrap script to %s' % path)

        contents = self._master_bootstrap_script_content()
        for line in StringIO(contents):
            log.debug('BOOTSTRAP: ' + line.rstrip('\r\n'))

        with open(path, 'w') as f:
            f.write(contents)

        self._master_bootstrap_script_path = path

    def _master_bootstrap_script_content(self):
        """Create the contents of the master bootstrap script.
        """
        out = StringIO()

        python_bin_in_list = ', '.join(
            repr(arg) for arg in self._opts['python_bin'])

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
        for name, path in self._bootstrap_dir_mgr.name_to_path('file').iteritems():
            s3_uri = self._upload_mgr.uri(path)
            writeln(
                "check_call(['hadoop', 'fs', '-copyToLocal', %r, %r])" %
                (s3_uri, name))
        writeln()

        # make scripts executable
        if self._bootstrap_scripts:
            writeln('# make bootstrap scripts executable')
            for path_dict in self._bootstrap_scripts:
                writeln("check_call(['chmod', 'a+rx', %r])" %
                        path_dict['name'])
            writeln()

        # bootstrap mrjob
        if self._opts['bootstrap_mrjob']:
            name = self._bootstrap_dir_mgr.name('file', self._mrjob_tar_gz_path)
            writeln('# bootstrap mrjob')
            writeln("site_packages = distutils.sysconfig.get_python_lib()")
            writeln(
                "check_call(['sudo', 'tar', 'xfz', %r, '-C', site_packages])" %
                name)
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
            for path_dict in self._bootstrap_python_packages:
                writeln("check_call(['tar', 'xfz', %r])" %
                        self._bootstrap_dir_mgr.name(**path_dict))
                # figure out name of dir to CD into
                assert path_dict['path'].endswith('.tar.gz')
                cd_into = extract_dir_for_tar(path_dict['path'])
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
            for path_dict in self._bootstrap_scripts:
                writeln('check_call(%r)' % (
                    ['./' + self._bootstrap_dir_mgr.name(**path_dict)],))
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

        self._add_bootstrap_files_for_upload()
        self._upload_local_files_to_s3()

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
        exclude = set()
        emr_conn = self.make_emr_conn()
        s3_conn = self.make_s3_conn()
        max_wait_time = self._opts['pool_wait_minutes']
        now = datetime.now()
        end_time = now + timedelta(minutes=max_wait_time)
        time_sleep = timedelta(seconds=JOB_FLOW_SLEEP_INTERVAL)
        log.info("Attempting to find an available job flow...")
        while now <= end_time:
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
            elif max_wait_time == 0:
                return None
            else:
                # Reset the exclusion set since it is possible to reclaim a
                # lock that was previously unavailable.
                exclude = set()
                log.info("No job flows available in pool '%s'. Checking again"
                         " in %d seconds." % (
                             self._opts['emr_job_flow_pool_name'],
                             int(JOB_FLOW_SLEEP_INTERVAL)))
                time.sleep(JOB_FLOW_SLEEP_INTERVAL)
                now += time_sleep
        return None

    def _lock_uri(self, job_flow):
        return make_lock_uri(self._opts['s3_scratch_uri'],
                             job_flow.jobflowid,
                             len(job_flow.steps) + 1)

    def _pool_hash(self):
        """Generate a hash of the bootstrap configuration so it can be used to
        match jobs and job flows. This first argument passed to the bootstrap
        script will be ``'pool-'`` plus this hash.

        The way the hash is calculated may vary between point releases
        (pooling requires the exact same version of :py:mod:`mrjob` anyway).
        """
        things_to_hash = [
            # exclude mrjob.tar.gz because it's only created if the
            # job starts its own job flow (also, its hash changes every time
            # since the tarball contains different timestamps)
            dict((name, self.md5sum(path)) for name, path
                 in self._bootstrap_dir_mgr.name_to_path('file').iteritems()
                 if not path == self._mrjob_tar_gz_path),
            self._opts['additional_emr_info'],
            self._opts['bootstrap_mrjob'],
            self._opts['bootstrap_cmds'],
            self._bootstrap_actions,
        ]

        if self._opts['bootstrap_mrjob']:
            things_to_hash.append(mrjob.__version__)
        return hash_object(things_to_hash)

    ### EMR-specific STUFF ###

    def make_emr_conn(self):
        """Create a connection to EMR.

        :return: a :py:class:`boto.emr.connection.EmrConnection`,
                 wrapped in a :py:class:`mrjob.retry.RetryWrapper`
        """
        # ...which is then wrapped in bacon! Mmmmm!

        # give a non-cryptic error message if boto isn't installed
        if boto is None:
            raise ImportError('You must install boto to connect to EMR')

        region = self._get_region_info_for_emr_conn()
        log.debug('creating EMR connection (to %s)' % region.endpoint)

        raw_emr_conn = boto.emr.connection.EmrConnection(
            aws_access_key_id=self._opts['aws_access_key_id'],
            aws_secret_access_key=self._opts['aws_secret_access_key'],
            region=region)
        return wrap_aws_conn(raw_emr_conn)

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
                raise IOError(
                    'Cannot ssh to master; job flow is not waiting or running')
        except boto.exception.S3ResponseError:
            # This error is raised by some versions of boto when the jobflow
            # doesn't exist
            raise IOError('Could not get job flow information')
        except boto.exception.EmrResponseError:
            # This error is raised by other version of boto when the jobflow
            # doesn't exist (some time before 2.4)
            raise IOError('Could not get job flow information')

        self._address = jobflow.masterpublicdnsname
        return self._address

    def _addresses_of_slaves(self):
        if not self._ssh_slave_addrs:
            self._ssh_slave_addrs = ssh_slave_addresses(
                self._opts['ssh_bin'],
                self._address_of_master(),
                self._opts['ec2_key_pair_file'])
        return self._ssh_slave_addrs
