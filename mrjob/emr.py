# -*- coding: utf-8 -*-
# Copyright 2009-2017 Yelp and Contributors
# Copyright 2018 Yelp
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
import hashlib
import json
import logging
import os
import os.path
import pipes
import posixpath
import random
import re
import signal
import socket
import time
from collections import defaultdict
from datetime import datetime
from datetime import timedelta
from itertools import islice
from subprocess import Popen
from subprocess import PIPE

try:
    import boto
    import boto.ec2
    import boto.emr
    import boto.emr.connection
    import boto.emr.instance_group
    import boto.emr.emrobject
    import boto.exception
    import boto.https_connection
    import boto.regioninfo
    import boto.utils
    boto  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    # don't require boto; MRJobs don't actually need it when running
    # inside hadoop streaming
    boto = None

try:
    import filechunkio
except ImportError:
    # that's cool; filechunkio is only for multipart uploading
    filechunkio = None

import mrjob
import mrjob.step
from mrjob.aws import EC2_INSTANCE_TYPE_TO_COMPUTE_UNITS
from mrjob.aws import EC2_INSTANCE_TYPE_TO_MEMORY
from mrjob.aws import emr_endpoint_for_region
from mrjob.aws import emr_ssl_host_for_region
from mrjob.aws import s3_location_constraint_for_region
from mrjob.compat import map_version
from mrjob.compat import version_gte
from mrjob.conf import combine_dicts
from mrjob.fs.composite import CompositeFilesystem
from mrjob.fs.local import LocalFilesystem
from mrjob.fs.s3 import S3Filesystem
from mrjob.fs.s3 import wrap_aws_conn
from mrjob.fs.ssh import SSHFilesystem
from mrjob.iam import _FALLBACK_INSTANCE_PROFILE
from mrjob.iam import _FALLBACK_SERVICE_ROLE
from mrjob.iam import get_or_create_mrjob_instance_profile
from mrjob.iam import get_or_create_mrjob_service_role
from mrjob.logs.bootstrap import _check_for_nonzero_return_code
from mrjob.logs.bootstrap import _interpret_emr_bootstrap_stderr
from mrjob.logs.bootstrap import _ls_emr_bootstrap_stderr_logs
from mrjob.logs.counters import _format_counters
from mrjob.logs.counters import _pick_counters
from mrjob.logs.errors import _format_error
from mrjob.logs.mixin import LogInterpretationMixin
from mrjob.logs.step import _interpret_emr_step_stderr
from mrjob.logs.step import _interpret_emr_step_syslog
from mrjob.logs.step import _ls_emr_step_stderr_logs
from mrjob.logs.step import _ls_emr_step_syslogs
from mrjob.options import _allowed_keys
from mrjob.options import _combiners
from mrjob.options import _deprecated_aliases
from mrjob.parse import is_s3_uri
from mrjob.parse import is_uri
from mrjob.parse import iso8601_to_datetime
from mrjob.parse import iso8601_to_timestamp
from mrjob.parse import parse_s3_uri
from mrjob.parse import _parse_progress_from_job_tracker
from mrjob.parse import _parse_progress_from_resource_manager
from mrjob.patched_boto import _patched_describe_cluster
from mrjob.patched_boto import _patched_describe_step
from mrjob.patched_boto import _patched_list_steps
from mrjob.pool import _est_time_to_hour
from mrjob.pool import _pool_hash_and_name
from mrjob.py2 import PY2
from mrjob.py2 import string_types
from mrjob.py2 import urlopen
from mrjob.py2 import xrange
from mrjob.retry import RetryGoRound
from mrjob.runner import MRJobRunner
from mrjob.runner import RunnerOptionStore
from mrjob.setup import BootstrapWorkingDirManager
from mrjob.setup import UploadDirManager
from mrjob.setup import parse_legacy_hash_path
from mrjob.setup import parse_setup_cmd
from mrjob.ssh import _ssh_run
from mrjob.step import StepFailedException
from mrjob.step import _is_spark_step_type
from mrjob.util import cmd_line
from mrjob.util import shlex_split
from mrjob.util import random_identifier


log = logging.getLogger(__name__)

# how to set up the SSH tunnel for various AMI versions
_IMAGE_VERSION_TO_SSH_TUNNEL_CONFIG = {
    '2': dict(
        localhost=True,
        name='job tracker',
        path='/jobtracker.jsp',
        port=9100,
    ),
    '3': dict(
        localhost=False,
        name='resource manager',
        path='/cluster',
        port=9026,
    ),
    '4': dict(
        localhost=False,
        name='resource manager',
        path='/cluster',
        port=8088,
    ),
}

# if we SSH into a node, default place to look for logs
_EMR_LOG_DIR = '/mnt/var/log'

# no longer limited to 256 steps starting with 2.4.8/3.1.1
# (# of steps is actually unlimited, but API only shows 1000; see #1462)
_IMAGE_VERSION_TO_MAX_STEPS = {
    '2': 256,
    '2.4.8': 1000,
    '3': 256,
    '3.1.1': 1000,
}

# breaking this out as a separate constant since 4.x AMIs don't report
# RunningAmiVersion, they report ReleaseLabel
_4_X_MAX_STEPS = 1000

_MAX_SSH_RETRIES = 20

# ssh should fail right away if it can't bind a port
_WAIT_FOR_SSH_TO_FAIL = 1.0

# amount of time to wait between checks for available pooled clusters
_POOLING_SLEEP_INTERVAL = 30.01  # Add .1 seconds so minutes arent spot on.

# bootstrap action which automatically terminates idle clusters
_MAX_HOURS_IDLE_BOOTSTRAP_ACTION_PATH = os.path.join(
    os.path.dirname(mrjob.__file__),
    'bootstrap',
    'terminate_idle_cluster.sh')

# default AWS region to use for EMR. Using us-west-2 because it is the default
# for new (since October 10, 2012) accounts (see #1025)
_DEFAULT_REGION = 'us-west-2'

# default AMI to use on EMR. This will be updated with each version
_DEFAULT_IMAGE_VERSION = '4.8.2'

# EMR translates the dead/deprecated "latest" AMI version to 2.4.2
# (2.4.2 isn't actually the latest version by a long shot)
_IMAGE_VERSION_LATEST = '2.4.2'

# first AMI version that we can't run bash -e on (see #1548)
_BAD_BASH_IMAGE_VERSION = '5.2.0'

# use this if bash -e works (/bin/sh is actually bash)
_GOOD_BASH_SH_BIN = ['/bin/sh', '-ex']

# use this if bash -e doesn't work
_BAD_BASH_SH_BIN = ['/bin/sh', '-x']

# Hadoop streaming jar on 1-3.x AMIs
_PRE_4_X_STREAMING_JAR = '/home/hadoop/contrib/streaming/hadoop-streaming.jar'

# intermediary jar used on 4.x AMIs
_4_X_COMMAND_RUNNER_JAR = 'command-runner.jar'

# path to spark-submit on 3.x AMIs. (On 4.x, it's just 'spark-submit')
_3_X_SPARK_SUBMIT = '/home/hadoop/spark/bin/spark-submit'

# bootstrap action to install Spark on 3.x AMIs (On 4.x+, we use
# Applications instead)
_3_X_SPARK_BOOTSTRAP_ACTION = (
    'file:///usr/share/aws/emr/install-spark/install-spark')

# first AMI version to support Spark
_MIN_SPARK_AMI_VERSION = '3.8.0'

# first AMI version with Spark that supports Python 3
_MIN_SPARK_PY3_AMI_VERSION = '4.0.0'

# always use these args with spark-submit
_EMR_SPARK_ARGS = ['--master', 'yarn', '--deploy-mode', 'cluster']

# we have to wait this many minutes for logs to transfer to S3 (or wait
# for the cluster to terminate). Docs say logs are transferred every 5
# minutes, but I've seen it take longer on the 4.3.0 AMI. Probably it's
# 5 minutes plus time to copy the logs, or something like that.
_S3_LOG_WAIT_MINUTES = 10

# cheapest instance type that can run Spark
_CHEAPEST_SPARK_INSTANCE_TYPE = 'm1.large'

# minimum amount of memory to run spark jobs
#
# (it's possible that we could get by with slightly less memory, but
# m1.medium definitely doesn't work)
_MIN_SPARK_INSTANCE_MEMORY = (
    EC2_INSTANCE_TYPE_TO_MEMORY[_CHEAPEST_SPARK_INSTANCE_TYPE])

# cheapest instance type that can run resource manager and non-Spark tasks
# on 3.x AMI and above
_CHEAPEST_INSTANCE_TYPE = 'm1.medium'

# cheapest instance type that can run on 2.x AMIs
_CHEAPEST_2_X_INSTANCE_TYPE = 'm1.small'

# these are the only kinds of instance roles that exist
_INSTANCE_ROLES = ('master', 'core', 'task')


# used to bail out and retry when a pooled cluster self-terminates
class _PooledClusterSelfTerminatedException(Exception):
    pass

# mildly flexible regex to detect cluster self-termination. Termination of
# non-master nodes won't shut down the cluster, so don't need to match that.
_CLUSTER_SELF_TERMINATED_RE = re.compile(
    '^.*The master node was terminated.*$', re.I)


def s3_key_to_uri(s3_key):
    """Convert a boto Key object into an ``s3://`` URI"""
    return 's3://%s/%s' % (s3_key.bucket.name, s3_key.name)


def _repeat(api_call, *args, **kwargs):
    """Make the same API call repeatedly until we've seen every page
    of the response (sets *marker* automatically).

    Yields one or more responses.
    """
    # the _delay kwarg sleeps that many seconds before each API call
    # (including the first). This was added for audit-emr-usage; should
    # re-visit if we need to use more generally. See #1091

    marker = None

    _delay = kwargs.pop('_delay', None)

    while True:
        if _delay:
            time.sleep(_delay)

        resp = api_call(*args, marker=marker, **kwargs)
        yield resp

        # go to next page, if any
        marker = getattr(resp, 'marker', None)
        if not marker:
            return


def _yield_all_clusters(emr_conn, *args, **kwargs):
    """Make successive API calls, yielding cluster summaries."""
    for resp in _repeat(emr_conn.list_clusters, *args, **kwargs):
        for cluster in getattr(resp, 'clusters', []):
            yield cluster


def _yield_all_bootstrap_actions(emr_conn, cluster_id, *args, **kwargs):
    for resp in _repeat(emr_conn.list_bootstrap_actions,
                        cluster_id, *args, **kwargs):
        for action in getattr(resp, 'actions', []):
            yield action


def _yield_all_instances(emr_conn, cluster_id, *args, **kwargs):
    """Get information about all instances for the given cluster."""
    for resp in _repeat(emr_conn.list_instances,
                        cluster_id, *args, **kwargs):
        for instance in getattr(resp, 'instances', []):
            yield instance


def _yield_all_instance_groups(emr_conn, cluster_id, *args, **kwargs):
    """Get all instance groups for the given cluster.

    Not sure what order the API returns instance groups in (see #1316);
    please treat it as undefined (make a dictionary, etc.)
    """
    for resp in _repeat(emr_conn.list_instance_groups,
                        cluster_id, *args, **kwargs):
        for group in getattr(resp, 'instancegroups', []):
            yield group


def _yield_all_steps(emr_conn, cluster_id, *args, **kwargs):
    """Get all steps for the cluster, making successive API calls
    if necessary.

    Calls :py:func:`~mrjob.patched_boto._patched_list_steps`, to work around
    `boto's StartDateTime bug <https://github.com/boto/boto/issues/3268>`__.

    Note that this returns steps in *reverse* order, because that's what
    the ``ListSteps`` API call does (see #1316). If you want to see all steps
    in chronological order, use :py:func:`_list_all_steps`.
    """
    for resp in _repeat(_patched_list_steps, emr_conn, cluster_id,
                        *args, **kwargs):
        for step in getattr(resp, 'steps', []):
            yield step


def _list_all_steps(emr_conn, cluster_id, *args, **kwargs):
    """Return all steps for the cluster as a list, in chronological order
    (the reverse of :py:func:`_yield_all_steps`).
    """
    return list(reversed(list(
        _yield_all_steps(emr_conn, cluster_id, *args, **kwargs))))


def _step_ids_for_job(steps, job_key):
    """Given a list of steps for a cluster, return a list of the (EMR) step
    IDs for the job with the given key, in the same order.

    Note that :py:func:`_yield_all_steps` returns steps in reverse order;
    you probably want to use the value returned by
    :pyfunc:`_list_all_steps` for *steps*.
    """
    step_ids = []

    for step in steps:
        if step.name.startswith(job_key):
            step_ids.append(step.id)

    return step_ids


def _make_lock_uri(cloud_tmp_dir, cluster_id, step_num):
    """Generate the URI to lock the cluster ``cluster_id``"""
    return cloud_tmp_dir + 'locks/' + cluster_id + '/' + str(step_num)


def _lock_acquire_step_1(s3_fs, lock_uri, job_key, mins_to_expiration=None):
    bucket_name, key_prefix = parse_s3_uri(lock_uri)
    bucket = s3_fs.get_bucket(bucket_name)
    key = bucket.get_key(key_prefix)

    # EMRJobRunner should start using a cluster within about a second of
    # locking it, so if it's been a while, then it probably crashed and we
    # can just use this cluster.
    key_expired = False
    if key and mins_to_expiration is not None:
        last_modified = iso8601_to_datetime(key.last_modified)
        age = datetime.utcnow() - last_modified
        if age > timedelta(minutes=mins_to_expiration):
            key_expired = True

    if key is None or key_expired:
        key = bucket.new_key(key_prefix)
        key.set_contents_from_string(job_key.encode('utf_8'))
        return key
    else:
        return None


def _lock_acquire_step_2(key, job_key):
    key_value = key.get_contents_as_string()
    return (key_value == job_key.encode('utf_8'))


def _attempt_to_acquire_lock(s3_fs, lock_uri, sync_wait_time, job_key,
                             mins_to_expiration=None):
    """Returns True if this session successfully took ownership of the lock
    specified by ``lock_uri``.
    """
    key = _lock_acquire_step_1(s3_fs, lock_uri, job_key, mins_to_expiration)
    if key is None:
        return False

    time.sleep(sync_wait_time)
    return _lock_acquire_step_2(key, job_key)


def _get_reason(cluster_or_step):
    """Extract statechangereason.message from a boto Cluster or Step.

    Default to ``''``."""
    return getattr(
        getattr(cluster_or_step.status, 'statechangereason', ''),
        'message', '').rstrip()


class EMRRunnerOptionStore(RunnerOptionStore):

    ALLOWED_KEYS = _allowed_keys('emr')
    COMBINERS = _combiners('emr')
    DEPRECATED_ALIASES = _deprecated_aliases('emr')

    def __init__(self, alias, opts, conf_paths):
        super(EMRRunnerOptionStore, self).__init__(alias, opts, conf_paths)

        # don't allow region to be ''
        if not self['region']:
            self['region'] = _DEFAULT_REGION

        self._fix_emr_configurations_opt()
        self._fix_hadoop_streaming_jar_on_emr_opt()
        self._fix_instance_opts()
        self._fix_image_version_latest()
        self._fix_release_label_opt()

    def default_options(self):
        super_opts = super(EMRRunnerOptionStore, self).default_options()
        return combine_dicts(super_opts, {
            'image_version': _DEFAULT_IMAGE_VERSION,
            'region': _DEFAULT_REGION,
            'bootstrap_python': None,
            'check_cluster_every': 30,
            'cleanup_on_failure': ['JOB'],
            'mins_to_end_of_hour': 60.0,  # disable by default
            'num_core_instances': 0,
            'num_ec2_instances': 1,
            'num_task_instances': 0,
            'pool_name': 'default',
            'pool_wait_minutes': 0,
            'cloud_fs_sync_secs': 5.0,
            'cloud_upload_part_size': 100,  # 100 MB
            'sh_bin': None,  # see _sh_bin(), below
            'ssh_bin': ['ssh'],
            # don't use a list because it makes it hard to read option values
            # when running in verbose mode. See #1284
            'ssh_bind_ports': xrange(40001, 40841),
            'ssh_tunnel': False,
            'ssh_tunnel_is_open': False,
            'visible_to_all_users': True,
        })

    def _fix_emr_configurations_opt(self):
        """Normalize emr_configurations, raising an exception if we find
        serious problems.
        """
        self['emr_configurations'] = [
            _fix_configuration_opt(c) for c in self['emr_configurations']]

    def _fix_hadoop_streaming_jar_on_emr_opt(self):
        """Translate hadoop_streaming_jar_on_emr to hadoop_streaming_jar
        and issue a warning."""
        if self['hadoop_streaming_jar_on_emr']:
            jar = 'file://' + self['hadoop_streaming_jar_on_emr']
            log.warning('hadoop_streaming_jar_on_emr is deprecated'
                        ' and will be removed in v0.6.0.'
                        ' Set hadoop_streaming_jar to %s instead' % jar)
            if not self['hadoop_streaming_jar']:
                self['hadoop_streaming_jar'] = jar

    def _fix_instance_opts(self):
        """If the *instance_type* option is set, override instance
        type for the nodes that actually run tasks (see Issue #66). Allow
        command-line arguments to override defaults and arguments
        in mrjob.conf (see Issue #311).

        Also, make sure that bid prices of zero are converted to None.
        """
        # If task instance type is not set, use core instance type
        # (This is mostly so that we don't inadvertently join a pool
        # with task instance types with too little memory.)
        if not self['task_instance_type']:
            self['task_instance_type'] = (
                self['core_instance_type'])

        # Within EMRJobRunner, we use num_core_instances and
        # num_task_instances, not num_ec2_instances. (Number
        # of master instances is always 1.)
        if (self._opt_priority['num_ec2_instances'] >
            max(self._opt_priority['num_core_instances'],
                self._opt_priority['num_task_instances'])):
            # assume 1 master, n - 1 core, 0 task
            self['num_core_instances'] = self['num_ec2_instances'] - 1
            self['num_task_instances'] = 0

            log.warning('num_ec2_instances is deprecated; set'
                        ' num_core_instances to %d instead' % (
                            self['num_core_instances']))
        else:
            # issue a warning if we used both kinds of instance number
            # options on the command line or in mrjob.conf
            if (self._opt_priority['num_ec2_instances'] >= 2 and
                self._opt_priority['num_ec2_instances'] <=
                max(self._opt_priority['num_core_instances'],
                    self._opt_priority['num_task_instances'])):
                log.warning('Mixing num_ec2_instances and'
                            ' num_{core,task}_instances does not make'
                            ' sense; ignoring num_ec2_instances')

        # Allow ec2 instance type to override other instance types
        instance_type = self['instance_type']
        if instance_type:
            # core instances
            if (self._opt_priority['instance_type'] >
                    self._opt_priority['core_instance_type']):
                self['core_instance_type'] = instance_type

            # master instance only does work when it's the only instance
            if (self['num_core_instances'] <= 0 and
                self['num_task_instances'] <= 0 and
                (self._opt_priority['instance_type'] >
                 self._opt_priority['master_instance_type'])):
                self['master_instance_type'] = instance_type

            # task instances
            if (self._opt_priority['instance_type'] >
                    self._opt_priority['task_instance_type']):
                self['task_instance_type'] = instance_type

        # convert a bid price of '0' to None
        for role in _INSTANCE_ROLES:
            opt_name = '%s_instance_bid_price' % role
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

    def _fix_image_version_latest(self):
        """Translate the dead/deprecated *image_version* value ``latest``
        to ``2.4.2`` up-front, so our code doesn't have to deal with
        non-numeric AMI versions."""
        if self['image_version'] == 'latest':
            self['image_version'] = _IMAGE_VERSION_LATEST

    def _fix_release_label_opt(self):
        """If *release_label* is not set and *image_version* is set to version
        4 or higher (which the EMR API won't accept), set *release_label*
        to "emr-" plus *image_version*. (Leave *image_version* as-is;
        *release_label* overrides it anyway.)"""
        if (version_gte(self['image_version'], '4') and
                not self['release_label']):
            self['release_label'] = 'emr-' + self['image_version']

    def _obfuscate(self, opt_key, opt_value):
        # don't need to obfuscate empty values
        if not opt_value:
            return opt_value

        if opt_key in ('aws_secret_access_key', 'aws_session_token'):
            # don't expose any part of secret credentials
            return '...'
        elif opt_key == 'aws_access_key_id':
            if isinstance(opt_value, string_types):
                return '...' + opt_value[-4:]
            else:
                # don't expose aws_access_key_id if it was accidentally
                # put in a list or something
                return '...'
        else:
            return opt_value


class EMRJobRunner(MRJobRunner, LogInterpretationMixin):
    """Runs an :py:class:`~mrjob.job.MRJob` on Amazon Elastic MapReduce.
    Invoked when you run your job with ``-r emr``.

    :py:class:`EMRJobRunner` runs your job in an EMR cluster, which is
    basically a temporary Hadoop cluster. Normally, it creates a cluster
    just for your job; it's also possible to run your job in a specific
    cluster by setting *cluster_id* or to automatically choose a
    waiting cluster, creating one if none exists, by setting
    *pool_clusters*.

    Input, support, and jar files can be either local or on S3; use
    ``s3://...`` URLs to refer to files on S3.

    This class has some useful utilities for talking directly to S3 and EMR,
    so you may find it useful to instantiate it without a script::

        from mrjob.emr import EMRJobRunner

        emr_conn = EMRJobRunner().make_emr_conn()
        clusters = emr_conn.list_clusters()
        ...
    """
    alias = 'emr'

    # Don't need to bootstrap mrjob in the setup wrapper; that's what
    # the bootstrap script is for!
    BOOTSTRAP_MRJOB_IN_SETUP = False

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

        # if we're going to create a bucket to use as temp space, we don't
        # want to actually create it until we run the job (Issue #50).
        # This variable helps us create the bucket as needed
        self._s3_tmp_bucket_to_create = None

        self._fix_s3_tmp_and_log_uri_opts()

        # use job key to make a unique tmp dir
        self._cloud_tmp_dir = self._opts['cloud_tmp_dir'] + self._job_key + '/'

        # pick/validate output dir
        if self._output_dir:
            self._output_dir = self._check_and_fix_s3_dir(self._output_dir)
        else:
            self._output_dir = self._cloud_tmp_dir + 'output/'

        # check AMI version
        if self._opts['image_version'].startswith('1.'):
            log.warning('1.x AMIs will probably not work because they use'
                        ' Python 2.5. Use a later AMI version or mrjob v0.4.2')

        # manage local files that we want to upload to S3. We'll add them
        # to this manager just before we need them.
        s3_files_dir = self._cloud_tmp_dir + 'files/'
        self._upload_mgr = UploadDirManager(s3_files_dir)

        # manage working dir for bootstrap script
        self._bootstrap_dir_mgr = BootstrapWorkingDirManager()

        if self._opts['bootstrap_files']:
            log.warning(
                "bootstrap_files is deprecated since v0.4.2 and will be"
                " removed in v0.6.0. Consider using bootstrap instead.")
        for path in self._opts['bootstrap_files']:
            self._bootstrap_dir_mgr.add(**parse_legacy_hash_path(
                'file', path, must_name='bootstrap_files'))

        self._bootstrap = self._bootstrap_python() + self._parse_bootstrap()
        self._legacy_bootstrap = self._parse_legacy_bootstrap()

        for cmd in self._bootstrap + self._legacy_bootstrap:
            for maybe_path_dict in cmd:
                if isinstance(maybe_path_dict, dict):
                    self._bootstrap_dir_mgr.add(**maybe_path_dict)

        if not (isinstance(self._opts['additional_emr_info'], string_types) or
                self._opts['additional_emr_info'] is None):
            self._opts['additional_emr_info'] = json.dumps(
                self._opts['additional_emr_info'])

        # we'll create the script later
        self._master_bootstrap_script_path = None

        # master node setup script (handled later by
        # _add_master_node_setup_files_for_upload())
        self._master_node_setup_mgr = BootstrapWorkingDirManager()
        self._master_node_setup_script_path = None

        # where our own logs ended up (we'll find this out once we run the job)
        self._s3_log_dir_uri = None

        # the ID assigned by EMR to this job (might be None)
        self._cluster_id = self._opts['cluster_id']
        # did we create this cluster?
        self._created_cluster = False

        # when did our particular task start?
        self._emr_job_start = None

        # ssh state
        self._ssh_proc = None
        self._gave_cant_ssh_warning = False
        # we don't upload the ssh key to master until it's needed
        self._ssh_key_is_copied = False

        # store the (tunneled) URL of the job tracker/resource manager
        self._ssh_tunnel_url = None

        # map from cluster ID to a dictionary containing cached info about
        # that cluster. Includes the following keys:
        # - image_version
        # - hadoop_version
        # - master_public_dns
        # - master_private_ip
        self._cluster_to_cache = defaultdict(dict)

        # List of dicts (one for each step) potentially containing
        # the keys 'history', 'step', and 'task'. These will also always
        # contain 'step_id' (the s-XXXXXXXX step ID on EMR).
        #
        # This will be filled by _wait_for_steps_to_complete()
        #
        # This might work better as a dictionary.
        self._log_interpretations = []

        # log interpretation for master node setup step (currently we don't
        # use this for anything; we just want to keep it out of
        # self._log_interpretations)
        self._mns_log_interpretation = None

        # set of step numbers (0-indexed) where we waited 5 minutes for logs to
        # transfer to S3 (so we don't do it twice)
        self._waited_for_logs_on_s3 = set()

    def _default_python_bin(self, local=False):
        """Like :py:meth:`mrjob.runner.MRJobRunner._default_python_bin`,
        except when running Python 2, we explicitly pick :command:`python2.7`
        on AMIs prior to 4.3.0 where's it's not the default.
        """
        if local or not PY2:
            return super(EMRJobRunner, self)._default_python_bin(local=local)

        if self._image_version_gte('4.3.0'):
            return ['python']
        else:
            return ['python2.7']

    def _image_version_gte(self, version):
        """Check if the requested image version is greater than
        or equal to *version*. If the *release_label* opt is set,
        look at that instead.

        If you're checking the actual image version of a cluster, just
        use :py:func:`~mrjob.compat.version_gte` and
        :py:meth:`get_image_version`.
        """
        if self._opts['release_label']:
            return version_gte(
                self._opts['release_label'].lstrip('emr-'), version)
        else:
            return version_gte(self._opts['image_version'], version)

    def _fix_s3_tmp_and_log_uri_opts(self):
        """Fill in cloud_tmp_dir and cloud_log_dir (in self._opts) if they
        aren't already set.

        Helper for __init__.
        """
        # set cloud_tmp_dir by checking for existing buckets
        if not self._opts['cloud_tmp_dir']:
            self._set_cloud_tmp_dir()
            log.info('Using %s as our temp dir on S3' %
                     self._opts['cloud_tmp_dir'])

        self._opts['cloud_tmp_dir'] = self._check_and_fix_s3_dir(
            self._opts['cloud_tmp_dir'])

        # set cloud_log_dir
        if self._opts['cloud_log_dir']:
            self._opts['cloud_log_dir'] = self._check_and_fix_s3_dir(
                self._opts['cloud_log_dir'])
        else:
            self._opts['cloud_log_dir'] = self._opts['cloud_tmp_dir'] + 'logs/'

    def _set_cloud_tmp_dir(self):
        """Helper for _fix_s3_tmp_and_log_uri_opts"""
        buckets = self.fs.get_all_buckets()
        mrjob_buckets = [b for b in buckets if b.name.startswith('mrjob-')]

        # Loop over buckets until we find one that is not region-
        #   restricted, matches region, or can be used to
        #   infer region if no region is specified
        for tmp_bucket in mrjob_buckets:
            tmp_bucket_name = tmp_bucket.name

            if (tmp_bucket.get_location() == s3_location_constraint_for_region(
                    self._opts['region'])):

                # Regions are both specified and match
                log.debug("using existing temp bucket %s" %
                          tmp_bucket_name)
                self._opts['cloud_tmp_dir'] = ('s3://%s/tmp/' %
                                               tmp_bucket_name)
                return

        # That may have all failed. If so, pick a name.
        tmp_bucket_name = 'mrjob-' + random_identifier()
        self._s3_tmp_bucket_to_create = tmp_bucket_name
        self._opts['cloud_tmp_dir'] = 's3://%s/tmp/' % tmp_bucket_name
        log.info('Auto-created temp S3 bucket %s' % tmp_bucket_name)
        self._wait_for_s3_eventual_consistency()

    def _s3_log_dir(self):
        """Get the URI of the log directory for this job's cluster."""
        if not self._s3_log_dir_uri:
            cluster = self._describe_cluster()
            log_uri = getattr(cluster, 'loguri', '')
            if log_uri:
                self._s3_log_dir_uri = '%s%s/' % (
                    log_uri.replace('s3n://', 's3://'), self._cluster_id)

        return self._s3_log_dir_uri

    def _create_s3_tmp_bucket_if_needed(self):
        """Make sure temp bucket exists"""
        if self._s3_tmp_bucket_to_create:
            log.debug('creating S3 bucket %r to use as temp space' %
                      self._s3_tmp_bucket_to_create)
            location = s3_location_constraint_for_region(
                self._opts['region'])
            self.fs.create_bucket(
                self._s3_tmp_bucket_to_create, location=location)
            self._s3_tmp_bucket_to_create = None

    def _check_and_fix_s3_dir(self, s3_uri):
        """Helper for __init__"""
        if not is_s3_uri(s3_uri):
            raise ValueError('Invalid S3 URI: %r' % s3_uri)
        if not s3_uri.endswith('/'):
            s3_uri = s3_uri + '/'

        return s3_uri

    def _bash_is_bad(self):
        # hopefully, there will eventually be an image version
        # where this issue is fixed. See #1548
        return self._image_version_gte(_BAD_BASH_IMAGE_VERSION)

    def _sh_bin(self):
        if self._opts['sh_bin']:
            return self._opts['sh_bin']
        elif self._bash_is_bad():
            return _BAD_BASH_SH_BIN
        else:
            return _GOOD_BASH_SH_BIN

    def _sh_pre_commands(self):
        if self._bash_is_bad() and not self._opts['sh_bin']:
            return ['set -e']
        else:
            return []

    @property
    def fs(self):
        """:py:class:`~mrjob.fs.base.Filesystem` object for SSH, S3, and the
        local filesystem.
        """
        if self._fs is None:
            s3_fs = S3Filesystem(
                aws_access_key_id=self._opts['aws_access_key_id'],
                aws_secret_access_key=self._opts['aws_secret_access_key'],
                aws_security_token=self._opts['aws_session_token'],
                s3_endpoint=self._opts['s3_endpoint'])

            if self._opts['ec2_key_pair_file']:
                self._ssh_fs = SSHFilesystem(
                    ssh_bin=self._opts['ssh_bin'],
                    ec2_key_pair_file=self._opts['ec2_key_pair_file'])

                self._fs = CompositeFilesystem(
                    self._ssh_fs, s3_fs, LocalFilesystem())
            else:
                self._ssh_fs = None
                self._fs = CompositeFilesystem(s3_fs, LocalFilesystem())

        return self._fs

    def _run(self):
        self._launch()
        self._finish_run()

    def _finish_run(self):
        while True:
            try:
                self._wait_for_steps_to_complete()
                break
            except _PooledClusterSelfTerminatedException:
                self._relaunch()

    def _prepare_for_launch(self):
        """Set up files needed for the job."""
        self._check_input_exists()
        self._check_output_not_exists()
        self._create_setup_wrapper_script()
        self._add_bootstrap_files_for_upload()
        self._add_master_node_setup_files_for_upload()
        self._add_job_files_for_upload()
        self._upload_local_files_to_s3()

    def _launch(self):
        """Set up files and then launch our job on EMR."""
        self._prepare_for_launch()
        self._launch_emr_job()

    def _relaunch(self):
        # files are already in place; just start with a fresh cluster
        assert not self._opts['cluster_id']
        self._cluster_id = None
        self._created_cluster = False

        # old SSH tunnel isn't valid for this cluster (see #1549)
        if self._ssh_proc:
            self._kill_ssh_tunnel()

        self._launch_emr_job()

    def _check_input_exists(self):
        """Make sure all input exists before continuing with our job.
        """
        if self._opts['check_input_paths']:
            for path in self._input_paths:
                if path == '-':
                    continue  # STDIN always exists

                if is_uri(path) and not is_s3_uri(path):
                    continue  # can't check non-S3 URIs, hope for the best

                if not self.fs.exists(path):
                    raise AssertionError(
                        'Input path %s does not exist!' % (path,))

    def _check_output_not_exists(self):
        """Verify the output path does not already exist. This avoids
        provisioning a cluster only to have Hadoop refuse to launch.
        """
        try:
            if self.fs.exists(self._output_dir):
                raise IOError(
                    'Output path %s already exists!' % (self._output_dir,))
        except boto.exception.S3ResponseError:
            pass

    def _add_bootstrap_files_for_upload(self, persistent=False):
        """Add files needed by the bootstrap script to self._upload_mgr.

        Tar up mrjob if bootstrap_mrjob is True.

        Create the master bootstrap script if necessary.

        persistent -- set by make_persistent_cluster()
        """
        # lazily create mrjob.zip
        if self._bootstrap_mrjob():
            self._create_mrjob_zip()
            self._bootstrap_dir_mgr.add('file', self._mrjob_zip_path)

        # all other files needed by the script are already in
        # _bootstrap_dir_mgr
        for path in self._bootstrap_dir_mgr.paths():
            self._upload_mgr.add(path)

        # now that we know where the above files live, we can create
        # the master bootstrap script
        self._create_master_bootstrap_script_if_needed()
        if self._master_bootstrap_script_path:
            self._upload_mgr.add(self._master_bootstrap_script_path)

        # make sure bootstrap action scripts are on S3
        for bootstrap_action in self._bootstrap_actions():
            self._upload_mgr.add(bootstrap_action['path'])

        # Add max-hours-idle script if we need it
        if (self._opts['max_hours_idle'] and
                (persistent or self._opts['pool_clusters'])):
            self._upload_mgr.add(_MAX_HOURS_IDLE_BOOTSTRAP_ACTION_PATH)

    def _add_master_node_setup_files_for_upload(self):
        """Add files necesary for the master node setup script to
        self._master_node_setup_mgr() and self._upload_mgr().

        Create the master node setup script if necessary.
        """
        # currently, only used by libjars; see #1336 for how we might open
        # this up more generally
        for path in self._opts['libjars']:
            # passthrough for libjars already on EMR
            if path.startswith('file:///'):
                continue

            self._master_node_setup_mgr.add('file', path)
            self._upload_mgr.add(path)

        self._create_master_node_setup_script_if_needed()
        if self._master_node_setup_script_path:
            self._upload_mgr.add(self._master_node_setup_script_path)

    def _add_job_files_for_upload(self):
        """Add files needed for running the job (setup and input)
        to self._upload_mgr."""
        for path in self._get_input_paths():
            self._upload_mgr.add(path)

        for path in self._working_dir_mgr.paths():
            self._upload_mgr.add(path)

        for path in self._opts['py_files']:
            self._upload_mgr.add(path)

        if self._opts['hadoop_streaming_jar']:
            self._upload_mgr.add(self._opts['hadoop_streaming_jar'])

        # upload JARs and (Python) scripts run by steps
        for step in self._get_steps():
            for key in 'jar', 'script':
                if step.get(key):
                    self._upload_mgr.add(step[key])

    def _upload_local_files_to_s3(self):
        """Copy local files tracked by self._upload_mgr to S3."""
        self._create_s3_tmp_bucket_if_needed()

        log.info('Copying local files to %s...' % self._upload_mgr.prefix)

        for path, s3_uri in self._upload_mgr.path_to_uri().items():
            log.debug('  %s -> %s' % (path, s3_uri))
            self._upload_contents(s3_uri, path)

    def _upload_contents(self, s3_uri, path):
        """Uploads the file at the given path to S3, possibly using
        multipart upload."""
        fsize = os.stat(path).st_size
        part_size = self._get_upload_part_size()

        s3_key = self.fs.make_s3_key(s3_uri)

        if self._should_use_multipart_upload(fsize, part_size, path):
            log.debug("Starting multipart upload of %s" % (path,))
            mpul = s3_key.bucket.initiate_multipart_upload(s3_key.name)

            try:
                self._upload_parts(mpul, path, fsize, part_size)
            except:
                mpul.cancel_upload()
                raise

            mpul.complete_upload()
            log.debug("Completed multipart upload of %s to %s" % (
                      path, s3_key.name))
        else:
            s3_key.set_contents_from_filename(path)

    def _upload_parts(self, mpul, path, fsize, part_size):
        offsets = range(0, fsize, part_size)

        for i, offset in enumerate(offsets):
            part_num = i + 1

            log.debug("uploading %d/%d of %s" % (
                part_num, len(offsets), path))
            chunk_bytes = min(part_size, fsize - offset)

            with filechunkio.FileChunkIO(
                    path, 'r', offset=offset, bytes=chunk_bytes) as fp:
                mpul.upload_part_from_file(fp, part_num)

    def _get_upload_part_size(self):
        # part size is in MB, as the minimum is 5 MB
        return int((self._opts['cloud_upload_part_size'] or 0) * 1024 * 1024)

    def _should_use_multipart_upload(self, fsize, part_size, path):
        """Decide if we want to use multipart uploading.

        path is only used to log warnings."""
        if not part_size:  # disabled
            return False

        if fsize <= part_size:
            return False

        if filechunkio is None:
            log.warning("Can't use S3 multipart upload for %s because"
                        " filechunkio is not installed" % path)
            return False

        return True

    def _ssh_tunnel_config(self):
        """Look up AMI version, and return a dict with the following keys:

        name: "job tracker" or "resource manager"
        path: path to start page of job tracker/resource manager
        port: port job tracker/resource manager is running on.
        """
        return map_version(self.get_image_version(),
                           _IMAGE_VERSION_TO_SSH_TUNNEL_CONFIG)

    def _job_tracker_host(self):
        """The host of the job tracker/resource manager, from the master node.
        """
        tunnel_config = self._ssh_tunnel_config()

        if tunnel_config['localhost']:
            # Issue #1311: on the 2.x AMIs, we want to tunnel to the job
            # tracker on localhost; otherwise it won't
            # work on some VPC setups.
            return 'localhost'
        else:
            # Issue #1397: on the 3.x and 4.x AMIs we want to tunnel to the
            # resource manager on the master node's *internal* IP; otherwise
            # it work won't work on some VPC setups
            return self._master_private_ip()

    def _job_tracker_url(self):
        tunnel_config = self._ssh_tunnel_config()

        return 'http://%s:%d%s' % (
            self._job_tracker_host(),
            tunnel_config['port'],
            tunnel_config['path'])

    def _set_up_ssh_tunnel(self):
        """set up the ssh tunnel to the job tracker, if it's not currently
        running.
        """
        if not self._opts['ssh_tunnel']:
            return

        host = self._address_of_master()
        if not host:
            return

        # look up what we're supposed to do on this AMI version
        tunnel_config = self._ssh_tunnel_config()

        REQUIRED_OPTS = ['ec2_key_pair', 'ec2_key_pair_file', 'ssh_bind_ports']
        for opt_name in REQUIRED_OPTS:
            if not self._opts[opt_name]:
                if not self._gave_cant_ssh_warning:
                    log.warning(
                        "  You must set %s in order to set up the SSH tunnel!"
                        % opt_name)
                    self._gave_cant_ssh_warning = True
                return

        # if there was already a tunnel, make sure it's still up
        if self._ssh_proc:
            self._ssh_proc.poll()
            if self._ssh_proc.returncode is None:
                return
            else:
                log.warning('  Oops, ssh subprocess exited with return code'
                            ' %d, restarting...' % self._ssh_proc.returncode)
                self._ssh_proc = None

        log.info('  Opening ssh tunnel to %s...' % tunnel_config['name'])

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
        popen_exception = None

        for bind_port in self._pick_ssh_bind_ports():
            args = self._opts['ssh_bin'] + [
                '-o', 'VerifyHostKeyDNS=no',
                '-o', 'StrictHostKeyChecking=no',
                '-o', 'ExitOnForwardFailure=yes',
                '-o', 'UserKnownHostsFile=%s' % fake_known_hosts_file,
                '-L', '%d:%s:%d' % (
                    bind_port,
                    self._job_tracker_host(),
                    tunnel_config['port']),
                '-N', '-n', '-q',  # no shell, no input, no output
                '-i', self._opts['ec2_key_pair_file'],
            ]
            if self._opts['ssh_tunnel_is_open']:
                args.extend(['-g', '-4'])  # -4: listen on IPv4 only
            args.append('hadoop@' + host)
            log.debug('> %s' % cmd_line(args))

            ssh_proc = None
            try:
                ssh_proc = Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
            except OSError as ex:
                # e.g. OSError(2, 'File not found')
                popen_exception = ex   # warning handled below
                break

            if ssh_proc:
                time.sleep(_WAIT_FOR_SSH_TO_FAIL)
                ssh_proc.poll()
                # still running. We are golden
                if ssh_proc.returncode is None:
                    self._ssh_proc = ssh_proc
                    break
                else:
                    ssh_proc.stdin.close()
                    ssh_proc.stdout.close()
                    ssh_proc.stderr.close()

        if not self._ssh_proc:
            if popen_exception:
                # this only happens if the ssh binary is not present
                # or not executable (so tunnel_config and the args to the
                # ssh binary don't matter)
                log.warning("    Couldn't run %s: %s" % (
                    cmd_line(self._opts['ssh_bin']), popen_exception))
            else:
                log.warning(
                    '    Failed to open ssh tunnel to %s' %
                    tunnel_config['name'])
        else:
            if self._opts['ssh_tunnel_is_open']:
                bind_host = socket.getfqdn()
            else:
                bind_host = 'localhost'
            self._ssh_tunnel_url = 'http://%s:%d%s' % (
                bind_host, bind_port, tunnel_config['path'])
            log.info('  Connect to %s at: %s' % (
                tunnel_config['name'], self._ssh_tunnel_url))

    def _kill_ssh_tunnel(self):
        """Send SIGKILL to SSH tunnel, if it's running."""
        if not self._ssh_proc:
            return

        self._ssh_proc.poll()
        if self._ssh_proc.returncode is None:
            log.info('Killing our SSH tunnel (pid %d)' %
                     self._ssh_proc.pid)

            self._ssh_proc.stdin.close()
            self._ssh_proc.stdout.close()
            self._ssh_proc.stderr.close()

            try:
                os.kill(self._ssh_proc.pid, signal.SIGKILL)
            except Exception as e:
                log.exception(e)

        self._ssh_proc = None
        self._ssh_tunnel_url = None

    def _pick_ssh_bind_ports(self):
        """Pick a list of ports to try binding our SSH tunnel to.

        We will try to bind the same port for any given cluster (Issue #67)
        """
        # don't perturb the random number generator
        random_state = random.getstate()
        try:
            # seed random port selection on cluster ID
            random.seed(self._cluster_id)
            num_picks = min(_MAX_SSH_RETRIES,
                            len(self._opts['ssh_bind_ports']))
            return random.sample(self._opts['ssh_bind_ports'], num_picks)
        finally:
            random.setstate(random_state)

    ### Running the job ###

    def cleanup(self, mode=None):
        super(EMRJobRunner, self).cleanup(mode=mode)

        # always stop our SSH tunnel if it's still running
        if self._ssh_proc:
            self._kill_ssh_tunnel()

        # stop the cluster if it belongs to us (it may have stopped on its
        # own already, but that's fine)
        # don't stop it if it was created due to --pool because the user
        # probably wants to use it again
        if self._cluster_id and not self._opts['cluster_id'] \
                and not self._opts['pool_clusters']:
            log.info('Terminating cluster: %s' % self._cluster_id)
            try:
                self.make_emr_conn().terminate_jobflow(self._cluster_id)
            except Exception as e:
                log.exception(e)

    def _cleanup_cloud_tmp(self):
        # delete all the files we created on S3
        if self._cloud_tmp_dir:
            try:
                log.info('Removing s3 temp directory %s...' %
                         self._cloud_tmp_dir)
                self.fs.rm(self._cloud_tmp_dir)
                self._cloud_tmp_dir = None
            except Exception as e:
                log.exception(e)

    def _cleanup_logs(self):
        super(EMRJobRunner, self)._cleanup_logs()

        # delete the log files, if it's a cluster we created (the logs
        # belong to the cluster)
        if self._s3_log_dir() and not self._opts['cluster_id'] \
                and not self._opts['pool_clusters']:
            try:
                log.info('Removing log files in %s...' % self._s3_log_dir())
                self.fs.rm(self._s3_log_dir())
            except Exception as e:
                log.exception(e)

    def _cleanup_cluster(self):
        if not self._cluster_id:
            # If we don't have a cluster, then we can't terminate it.
            return

        emr_conn = self.make_emr_conn()
        try:
            log.info("Attempting to terminate cluster")
            emr_conn.terminate_jobflow(self._cluster_id)
        except Exception as e:
            # Something happened with boto and the user should know.
            log.exception(e)
            return
        log.info('Cluster %s successfully terminated' % self._cluster_id)

    def _wait_for_s3_eventual_consistency(self):
        """Sleep for a little while, to give S3 a chance to sync up.
        """
        log.debug('Waiting %.1fs for S3 eventual consistency...' %
                  self._opts['cloud_fs_sync_secs'])
        time.sleep(self._opts['cloud_fs_sync_secs'])

    def _wait_for_cluster_to_terminate(self, cluster=None):
        if not cluster:
            cluster = self._describe_cluster()

        log.info('Waiting for cluster (%s) to terminate...' %
                 cluster.id)

        if (cluster.status.state == 'WAITING' and
                cluster.autoterminate != 'true'):
            raise Exception('Operation requires cluster to terminate, but'
                            ' it may never do so.')

        while True:
            log.info('  %s' % cluster.status.state)

            if cluster.status.state in (
                    'TERMINATED', 'TERMINATED_WITH_ERRORS'):
                return

            time.sleep(self._opts['check_cluster_every'])
            cluster = self._describe_cluster()

    # instance types

    def _cheapest_manager_instance_type(self):
        """What's the cheapest instance type we can get away with
        for the master node (when it's not also running jobs)?"""
        if self._image_version_gte('3'):
            return _CHEAPEST_INSTANCE_TYPE
        else:
            return _CHEAPEST_2_X_INSTANCE_TYPE

    def _cheapest_worker_instance_type(self):
        """What's the cheapest instance type we can get away with
        running tasks on?"""
        if self._uses_spark():
            return _CHEAPEST_SPARK_INSTANCE_TYPE
        else:
            return self._cheapest_manager_instance_type()

    def _instance_type(self, role):
        """What instance type should we use for the given role?
        (one of 'master', 'core', 'task')"""
        if role not in _INSTANCE_ROLES:
            raise ValueError

        # explicitly set
        if self._opts[role + '_instance_type']:
            return self._opts[role + '_instance_type']

        elif self._instance_is_worker(role):
            # using *instance_type* here is defensive programming;
            # if set, it should have already been popped into the worker
            # instance type option(s) by _fix_instance_opts() above
            return (self._opts['instance_type'] or
                    self._cheapest_worker_instance_type())

        else:
            return self._cheapest_manager_instance_type()

    def _instance_is_worker(self, role):
        """Do instances of the given role run tasks? True for non-master
        instances and sole master instance."""
        if role not in _INSTANCE_ROLES:
            raise ValueError

        return (role != 'master' or
                sum(self._num_instances(role)
                    for role in _INSTANCE_ROLES) == 1)

    def _num_instances(self, role):
        """How many of the given instance type do we want?"""
        if role not in _INSTANCE_ROLES:
            raise ValueError

        if role == 'master':
            return 1  # there can be only one
        else:
            return self._opts['num_' + role + '_instances']

    def _instance_bid_price(self, role):
        """What's the bid price for the given role (if any)?"""
        if role not in _INSTANCE_ROLES:
            raise ValueError

        return self._opts[role + '_instance_bid_price']

    def _create_instance_group(
            self, role, instance_type, num_instances, bid_price):
        """Helper method for creating instance groups. For use when
        creating a cluster using a list of InstanceGroups

            - Role is either 'master', 'core', or 'task'.
            - instance_type is an EC2 instance type
            - count is an int
            - bid_price is a number, a string, or None. If None,
              this instance group will be use the ON-DEMAND market
              instead of the SPOT market.
        """
        if role not in _INSTANCE_ROLES:
            raise ValueError

        if not instance_type:
            raise ValueError

        if not num_instances:
            raise ValueError

        if bid_price:
            market = 'SPOT'
            bid_price = str(bid_price)  # must be a string
        else:
            market = 'ON_DEMAND'
            bid_price = None

        return boto.emr.instance_group.InstanceGroup(
            num_instances=num_instances,
            role=role.upper(),
            type=instance_type,
            market=market,
            name=role,  # just name the groups "core", "master", and "task"
            bidprice=bid_price)

    def _create_cluster(self, persistent=False, steps=None):
        """Create an empty cluster on EMR, and return the ID of that
        job.

        If the ``tags`` option is set, also tags the cluster (which
        is a separate API call).

        persistent -- if this is true, create the cluster with the keep_alive
            option, indicating the job will have to be manually terminated.
        """
        # make sure we can see the files we copied to S3
        self._wait_for_s3_eventual_consistency()

        log.debug('Creating Elastic MapReduce cluster')
        args = self._cluster_args(persistent, steps)

        emr_conn = self.make_emr_conn()
        log.debug('Calling run_jobflow(%r, %r, %s)' % (
            self._job_key, self._opts['cloud_log_dir'],
            ', '.join('%s=%r' % (k, v) for k, v in args.items())))
        cluster_id = emr_conn.run_jobflow(
            self._job_key, self._opts['cloud_log_dir'], **args)

         # keep track of when we started our job
        self._emr_job_start = time.time()

        log.debug('Cluster created with ID: %s' % cluster_id)

        # set EMR tags for the cluster, if any
        tags = self._opts['tags']
        if tags:
            log.info('Setting EMR tags: %s' % ', '.join(
                '%s=%s' % (tag, value or '') for tag, value in tags.items()))
            emr_conn.add_tags(cluster_id, tags)

        return cluster_id

    # TODO: could break this into sub-methods for clarity
    def _cluster_args(self, persistent=False, steps=None):
        """Build kwargs for emr_conn.run_jobflow()"""
        args = {}
        api_params = {}

        if self._opts['release_label']:
            api_params['ReleaseLabel'] = self._opts['release_label']
        else:
            args['ami_version'] = self._opts['image_version']

        if self._opts['zone']:
            args['availability_zone'] = self._opts['zone']
        # The old, simple API, available if we're not using task instances
        # or bid prices
        if not (self._num_instances('task') or
                any(self._instance_bid_price(role)
                    for role in _INSTANCE_ROLES)):
            args['num_instances'] = self._num_instances('core') + 1
            args['master_instance_type'] = self._instance_type('master')
            args['slave_instance_type'] = self._instance_type('core')
        else:
            # Create a list of InstanceGroups
            args['instance_groups'] = []

            for role in _INSTANCE_ROLES:
                num_instances = self._num_instances(role)

                if num_instances:
                    args['instance_groups'].append(
                        self._create_instance_group(
                            role=role,
                            instance_type=self._instance_type(role),
                            num_instances=self._num_instances(role),
                            bid_price=self._instance_bid_price(role)))

        # bootstrap actions
        bootstrap_action_args = []

        for i, bootstrap_action in enumerate(self._bootstrap_actions()):
            uri = self._upload_mgr.uri(bootstrap_action['path'])
            bootstrap_action_args.append(
                boto.emr.BootstrapAction(
                    'action %d' % i, uri, bootstrap_action['args']))

        if self._master_bootstrap_script_path:
            master_bootstrap_script_args = []
            if self._opts['pool_clusters']:
                master_bootstrap_script_args = [
                    'pool-' + self._pool_hash(),
                    self._opts['pool_name'],
                ]
            bootstrap_action_args.append(
                boto.emr.BootstrapAction(
                    'master',
                    self._upload_mgr.uri(self._master_bootstrap_script_path),
                    master_bootstrap_script_args))

        if persistent or self._opts['pool_clusters']:
            args['keep_alive'] = True

            # only use idle termination script on persistent clusters
            # add it last, so that we don't count bootstrapping as idle time
            if self._opts['max_hours_idle']:
                s3_uri = self._upload_mgr.uri(
                    _MAX_HOURS_IDLE_BOOTSTRAP_ACTION_PATH)
                # script takes args in (integer) seconds
                ba_args = [int(self._opts['max_hours_idle'] * 3600),
                           int(self._opts['mins_to_end_of_hour'] * 60)]
                bootstrap_action_args.append(
                    boto.emr.BootstrapAction('idle timeout', s3_uri, ba_args))

        if bootstrap_action_args:
            args['bootstrap_actions'] = bootstrap_action_args

        if self._opts['ec2_key_pair']:
            args['ec2_keyname'] = self._opts['ec2_key_pair']

        if self._opts['enable_emr_debugging']:
            args['enable_debugging'] = True

        if self._opts['additional_emr_info']:
            args['additional_info'] = self._opts['additional_emr_info']

        # boto's connect_emr() has keyword args for these, but they override
        # emr_api_params, which is not what we want.
        api_params['VisibleToAllUsers'] = str(bool(
            self._opts['visible_to_all_users'])).lower()

        api_params['JobFlowRole'] = self._instance_profile()
        api_params['ServiceRole'] = self._service_role()

        applications = self._applications()
        if applications:
            api_params['Applications'] = [
                dict(Name=a) for a in sorted(applications)]

        if self._opts['emr_configurations']:
            # Properties will be automatically converted to KeyValue objects
            api_params['Configurations'] = self._opts['emr_configurations']

        if self._opts['subnet']:
            api_params['Instances.Ec2SubnetId'] = self._opts['subnet']

        if self._opts['emr_api_params']:
            api_params.update(self._opts['emr_api_params'])

        args['api_params'] = _encode_emr_api_params(api_params)

        if steps:
            args['steps'] = steps

        return args

    def _instance_profile(self):
        try:
            return (self._opts['iam_instance_profile'] or
                    get_or_create_mrjob_instance_profile(self.make_iam_conn()))
        except boto.exception.BotoServerError as ex:
            if ex.status != 403:
                raise
            log.warning(
                "Can't access IAM API, trying default instance profile: %s" %
                _FALLBACK_INSTANCE_PROFILE)
            return _FALLBACK_INSTANCE_PROFILE

    def _service_role(self):
        try:
            return (self._opts['iam_service_role'] or
                    get_or_create_mrjob_service_role(self.make_iam_conn()))
        except boto.exception.BotoServerError as ex:
            if ex.status != 403:
                raise
            log.warning(
                "Can't access IAM API, trying default service role: %s" %
                _FALLBACK_SERVICE_ROLE)
            return _FALLBACK_SERVICE_ROLE

    def _action_on_failure(self):
        # don't terminate other people's clusters
        if (self._opts['emr_action_on_failure']):
            return self._opts['emr_action_on_failure']
        elif (self._opts['cluster_id'] or
                self._opts['pool_clusters']):
            return 'CANCEL_AND_WAIT'
        else:
            return 'TERMINATE_CLUSTER'

    def _build_steps(self):
        """Return a list of boto Step objects corresponding to the
        steps we want to run."""
        # quick, add the other steps before the job spins up and
        # then shuts itself down! (in practice that won't happen
        # for several minutes)
        steps = []

        if self._master_node_setup_script_path:
            steps.append(self._build_master_node_setup_step())

        for n in range(self._num_steps()):
            steps.append(self._build_step(n))

        return steps

    def _build_step(self, step_num):
        step = self._get_step(step_num)

        if step['type'] == 'streaming':
            return self._build_streaming_step(step_num)
        elif step['type'] == 'jar':
            return self._build_jar_step(step_num)
        elif _is_spark_step_type(step['type']):
            return self._build_spark_step(step_num)
        else:
            raise AssertionError('Bad step type: %r' % (step['type'],))

    def _build_streaming_step(self, step_num):
        jar, step_arg_prefix = self._get_streaming_jar_and_step_arg_prefix()

        streaming_step_kwargs = dict(
            action_on_failure=self._action_on_failure(),
            input=self._step_input_uris(step_num),
            jar=jar,
            name='%s: Step %d of %d' % (
                self._job_key, step_num + 1, self._num_steps()),
            output=self._step_output_uri(step_num),
        )

        step_args = []
        step_args.extend(step_arg_prefix)  # add 'hadoop-streaming' for 4.x
        step_args.extend(self._upload_args())
        step_args.extend(self._hadoop_args_for_step(step_num))

        streaming_step_kwargs['step_args'] = step_args

        mapper, combiner, reducer = (
            self._hadoop_streaming_commands(step_num))

        streaming_step_kwargs['mapper'] = mapper
        streaming_step_kwargs['combiner'] = combiner
        streaming_step_kwargs['reducer'] = reducer

        return boto.emr.StreamingStep(**streaming_step_kwargs)

    def _build_jar_step(self, step_num):
        step = self._get_step(step_num)

        jar = self._upload_uri_or_remote_path(step['jar'])
        step_args = self._interpolate_input_and_output(step['args'], step_num)

        # -libjars, -D comes before jar-specific args
        step_args = self._hadoop_generic_args_for_step(step_num) + step_args

        return boto.emr.JarStep(
            name=self._step_name(step_num),
            jar=jar,
            main_class=step['main_class'],
            step_args=step_args,
            action_on_failure=self._action_on_failure())

    def _build_spark_step(self, step_num):
        """Returns a boto.emr.JarStep suitable for running all Spark
        step types (``spark``, ``spark_jar``, and ``spark_script``).
        """
        return boto.emr.JarStep(
            name=self._step_name(step_num),
            jar=self._spark_jar(),
            step_args=self._args_for_spark_step(step_num),
            action_on_failure=self._action_on_failure())

    def _interpolate_spark_script_path(self, path):
        return self._upload_uri_or_remote_path(path)

    def get_spark_submit_bin(self):
        if self._opts['spark_submit_bin'] is not None:
            return self._opts['spark_submit_bin']
        elif version_gte(self.get_image_version(), '4'):
            return ['spark-submit']
        else:
            return [_3_X_SPARK_SUBMIT]

    def _spark_submit_arg_prefix(self):
        return _EMR_SPARK_ARGS

    def _spark_jar(self):
        if version_gte(self.get_image_version(), '4'):
            return _4_X_COMMAND_RUNNER_JAR
        else:
            return self._script_runner_jar_uri()

    def _spark_py_files(self):
        """In cluster mode, py_files can be anywhere, so point to their
        uploaded URIs."""
        # don't use hash paths with --py-files; see #1375
        return [
            self._upload_mgr.uri(path)
            for path in sorted(self._opts['py_files'])
        ]

    def _step_name(self, step_num):
        """Return something like: ``'mr_your_job Step X of Y'``"""
        return '%s: Step %d of %d' % (
            self._job_key, step_num + 1, self._num_steps())

    def _upload_uri_or_remote_path(self, path):
        """Return where *path* will be uploaded, or, if it starts with
        ``'file:///'``, a local path."""
        if path.startswith('file:///'):
            return path[7:]  # keep leading slash
        else:
            return self._upload_mgr.uri(path)

    def _build_master_node_setup_step(self):
        name = '%s: Master node setup' % self._job_key
        jar = self._script_runner_jar_uri()
        step_args = [self._upload_mgr.uri(self._master_node_setup_script_path)]

        return boto.emr.JarStep(
            name=name, jar=jar, step_args=step_args,
            action_on_failure=self._action_on_failure())

    def _libjar_paths(self):
        results = []

        # libjars should be in the working dir of the master node setup
        # script path, unless they refer to paths directly (file:///)
        for path in self._opts['libjars']:
            if path.startswith('file:///'):
                results.append(path[7:])  # keep leading slash
            else:
                results.append(posixpath.join(
                    self._master_node_setup_working_dir(),
                    self._master_node_setup_mgr.name('file', path)))

        return results

    def _get_streaming_jar_and_step_arg_prefix(self):
        if self._opts['hadoop_streaming_jar']:
            if self._opts['hadoop_streaming_jar'].startswith('file://'):
                # special case: jar is already on EMR
                # relative paths are OK (though maybe not useful)
                return self._opts['hadoop_streaming_jar'][7:], []
            else:
                return self._upload_mgr.uri(
                    self._opts['hadoop_streaming_jar']), []
        elif version_gte(self.get_image_version(), '4'):
            # 4.x AMIs use an intermediary jar
            return _4_X_COMMAND_RUNNER_JAR, ['hadoop-streaming']
        else:
            # 2.x and 3.x AMIs just use a regular old streaming jar
            return _PRE_4_X_STREAMING_JAR, []

    def _get_spark_jar_and_step_arg_prefix(self):
        # TODO: add spark_submit_bin option

        if version_gte(self.get_image_version(), '4'):
            return (_4_X_COMMAND_RUNNER_JAR, ['spark-submit'])
        else:
            return (self._script_runner_jar_uri(), [_3_X_SPARK_SUBMIT])

    def _launch_emr_job(self):
        """Create an empty cluster on EMR, and set self._cluster_id to
        its ID.
        """
        self._create_s3_tmp_bucket_if_needed()
        emr_conn = self.make_emr_conn()

        # try to find a cluster from the pool. basically auto-fill
        # 'cluster_id' if possible and then follow normal behavior.
        if (self._opts['pool_clusters'] and not self._cluster_id):
            # master node setup script is an additional step
            num_steps = self._num_steps()
            if self._master_node_setup_script_path:
                num_steps += 1

            cluster_id = self._find_cluster(num_steps=num_steps)
            if cluster_id:
                self._cluster_id = cluster_id

        # create a cluster if we're not already using an existing one
        if not self._cluster_id:
            self._cluster_id = self._create_cluster(
                persistent=False)
            self._created_cluster = True
            log.info('Created new cluster %s' % self._cluster_id)
        else:
            log.info('Adding our job to existing cluster %s' %
                     self._cluster_id)

        # now that we know which cluster it is, check for Spark support
        if self._has_spark_steps():
            self._check_cluster_spark_support()

        # define our steps
        steps = self._build_steps()
        log.debug('Calling add_jobflow_steps(%r, %r)' % (
            self._cluster_id, steps))
        emr_conn.add_jobflow_steps(self._cluster_id, steps)

        # keep track of when we launched our job
        self._emr_job_start = time.time()

        # set EMR tags for the job, if any
        tags = self._opts['tags']
        if tags:
            log.info('Setting EMR tags: %s' %
                     ', '.join('%s=%s' % (tag, value)
                               for tag, value in tags.items()))
            emr_conn.add_tags(self._cluster_id, tags)

        # SSH FS uses sudo if we're on AMI 4.3.0+ (see #1244)
        if self._ssh_fs and version_gte(self.get_image_version(), '4.3.0'):
            self._ssh_fs.use_sudo_over_ssh()

    def _job_steps(self, max_steps=None):
        """Get the steps we submitted for this job in chronological order,
        ignoring steps from other jobs.

        Generally, you want to set *max_steps*, so we can make as few API
        calls as possible.
        """
        # the API yields steps in reversed order. Once we've found the expected
        # number of steps, stop.
        #
        # This implicitly excludes the master node setup script, which
        # is what we want for now.
        return list(reversed(list(islice(
            (step for step in
             _yield_all_steps(self.make_emr_conn(), self.get_cluster_id())
             if step.name.startswith(self._job_key)),
            max_steps))))

    def _wait_for_steps_to_complete(self):
        """Wait for every step of the job to complete, one by one."""
        num_steps = len(self._get_steps())

        # if there's a master node setup script, we'll treat that as
        # step -1
        if self._master_node_setup_script_path:
            max_steps = num_steps + 1
        else:
            max_steps = num_steps

        job_steps = self._job_steps(max_steps=max_steps)

        if len(job_steps) < max_steps:
            raise AssertionError("Can't find our steps in the cluster!")

        # clear out log interpretations if they were filled somehow
        self._log_interpretations = []
        self._mns_log_interpretation = None

        # open SSH tunnel if cluster is already ready
        # (this happens with pooling). See #1115
        cluster = self._describe_cluster()
        if cluster.status.state in ('RUNNING', 'WAITING'):
            self._set_up_ssh_tunnel()

        # treat master node setup as step -1
        if self._master_node_setup_script_path:
            start = -1
        else:
            start = 0

        for step_num, step in enumerate(job_steps, start=start):
            # this will raise an exception if a step fails
            if step_num == -1:
                log.info(
                    'Waiting for master node setup step (%s) to complete...' %
                    step.id)
            else:
                log.info('Waiting for step %d of %d (%s) to complete...' % (
                    step_num + 1, num_steps, step.id))

            self._wait_for_step_to_complete(step.id, step_num, num_steps)

    def _wait_for_step_to_complete(
            self, step_id, step_num=None, num_steps=None):
        """Helper for _wait_for_step_to_complete(). Wait for
        step with the given ID to complete, and fetch counters.
        If it fails, attempt to diagnose the error, and raise an
        exception.

        :param step_id: the s-XXXXXXX step ID on EMR
        :param step_num: which step this is out of the steps
                         belonging to our job (0-indexed)
        :param num_steps: number of steps in our job

        *step_num* and *num_steps* are optional and only used when raising
        a :py:class:`~mrjob.step.StepFailedException`.

        This also adds an item to self._log_interpretations
        """
        log_interpretation = dict(step_id=step_id)

        # suppress warnings about missing job ID for script-runner.jar
        if step_num == -1:
            log_interpretation['no_job'] = True
            self._mns_log_interpretation = log_interpretation
        else:
            self._log_interpretations.append(log_interpretation)

        emr_conn = self.make_emr_conn()

        while True:
            # don't antagonize EMR's throttling
            log.debug('Waiting %.1f seconds...' %
                      self._opts['check_cluster_every'])
            time.sleep(self._opts['check_cluster_every'])

            step = _patched_describe_step(emr_conn, self._cluster_id, step_id)

            if step.status.state == 'PENDING':
                cluster = self._describe_cluster()

                reason = _get_reason(cluster)
                reason_desc = (': %s' % reason) if reason else ''

                # we can open the ssh tunnel if cluster is ready (see #1115)
                if cluster.status.state in ('RUNNING', 'WAITING'):
                    self._set_up_ssh_tunnel()

                log.info('  PENDING (cluster is %s%s)' % (
                    cluster.status.state, reason_desc))
                continue

            if step.status.state == 'RUNNING':
                time_running_desc = ''

                startdatetime = getattr(
                    getattr(step.status, 'timeline', ''), 'startdatetime', '')
                if startdatetime:
                    start = iso8601_to_timestamp(startdatetime)
                    time_running_desc = ' for %.1fs' % (time.time() - start)

                # now is the time to tunnel, if we haven't already
                self._set_up_ssh_tunnel()
                log.info('  RUNNING%s' % time_running_desc)

                # don't log progress for master node setup step, because
                # it doesn't appear in job tracker
                if step_num >= 0:
                    self._log_step_progress()

                continue

            # we're done, will return at the end of this
            if step.status.state == 'COMPLETED':
                log.info('  COMPLETED')
                # will fetch counters, below, and then return
            else:
                # step has failed somehow. *reason* seems to only be set
                # when job is cancelled (e.g. 'Job terminated')
                reason = _get_reason(step)
                reason_desc = (' (%s)' % reason) if reason else ''

                log.info('  %s%s' % (
                    step.status.state, reason_desc))

                # print cluster status; this might give more context
                # why step didn't succeed
                cluster = self._describe_cluster()
                reason = _get_reason(cluster)
                reason_desc = (': %s' % reason) if reason else ''
                log.info('Cluster %s %s %s%s' % (
                    cluster.id,
                    'was' if 'ED' in cluster.status.state else 'is',
                    cluster.status.state,
                    reason_desc))

                if cluster.status.state in (
                        'TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS'):
                    # was it caused by a pooled cluster self-terminating?
                    # (if so, raise _PooledClusterSelfTerminatedException)
                    self._check_for_pooled_cluster_self_termination(
                        cluster, step)
                    # was it caused by IAM roles?
                    self._check_for_missing_default_iam_roles(cluster)
                    # was it caused by a key pair from the wrong region?
                    self._check_for_key_pair_from_wrong_region(cluster)
                    # was it because a bootstrap action failed?
                    self._check_for_failed_bootstrap_action(cluster)

            # spark steps require different log parsing. The master node
            # setup script is a JAR step (albeit one that never produces
            # counters)
            step_type = (
                self._get_step(step_num)['type'] if step_num >= 0 else 'jar')

            # step is done (either COMPLETED, FAILED, INTERRUPTED). so
            # try to fetch counters. (Except for master node setup
            # and Spark, which has no counters.)
            if step.status.state != 'CANCELLED':
                if step_num >= 0 and not _is_spark_step_type(step_type):
                    counters = self._pick_counters(
                        log_interpretation, step_type)
                    if counters:
                        log.info(_format_counters(counters))
                    else:
                        log.warning('No counters found')

            if step.status.state == 'COMPLETED':
                return

            if step.status.state == 'FAILED':
                error = self._pick_error(log_interpretation, step_type)
                if error:
                    log.error('Probable cause of failure:\n\n%s\n\n' %
                              _format_error(error))

            raise StepFailedException(
                step_num=step_num, num_steps=num_steps,
                # "Step 0 of ... failed" looks weird
                step_desc=(
                    'Master node setup step' if step_num == -1 else None))

    def _log_step_progress(self):
        """Tunnel to the job tracker/resource manager and log the
        progress of the current step.

        (This takes no arguments; we just assume the most recent running
        job is ours, which should be correct for EMR.)
        """
        progress_html = (self._progress_html_from_tunnel() or
                         self._progress_html_over_ssh())
        if not progress_html:
            return

        tunnel_config = self._ssh_tunnel_config()

        if tunnel_config['name'] == 'job tracker':
            map_progress, reduce_progress = (
                _parse_progress_from_job_tracker(progress_html))
            if map_progress is not None:
                log.info('   map %3d%% reduce %3d%%' % (
                    map_progress, reduce_progress))
        else:
            progress = _parse_progress_from_resource_manager(
                progress_html)
            if progress is not None:
                log.info('   %5.1f%% complete' % progress)

    def _progress_html_from_tunnel(self):
        """Fetch progress by calling :py:func:`urlopen` on our ssh tunnel, or
        return ``None``."""
        if not self._ssh_tunnel_url:
            return None

        tunnel_config = self._ssh_tunnel_config()
        log.debug('  Fetching progress from %s at %s' % (
            tunnel_config['name'], self._ssh_tunnel_url))

        tunnel_handle = None
        try:
            tunnel_handle = urlopen(self._ssh_tunnel_url)
            return tunnel_handle.read()
        except Exception as e:
            log.debug('    failed: %s' % str(e))
            return None
        finally:
            if tunnel_handle:
                tunnel_handle.close()

    def _progress_html_over_ssh(self):
        """Fetch progress by running :command:`curl` over SSH, or return
        ``None``"""
        host = self._address_of_master()

        if not (self._opts['ssh_bin'] and
                self._opts['ec2_key_pair_file'] and
                host):
            return None

        if not host:
            return None

        tunnel_config = self._ssh_tunnel_config()
        remote_url = self._job_tracker_url()

        log.debug('  Fetching progress from %s over SSH' % (
            tunnel_config['name']))

        try:
            stdout, _ = _ssh_run(
                self._opts['ssh_bin'],
                host,
                self._opts['ec2_key_pair_file'],
                ['curl', remote_url])
            return stdout
        except Exception as e:
            log.debug('    failed: %s' % str(e))

        return None

    def _check_for_pooled_cluster_self_termination(self, cluster, step):
        """If failure could have been due to a pooled cluster self-terminating,
        raise _PooledClusterSelfTerminatedException"""
        # this check might not even be relevant
        if not self._opts['pool_clusters']:
            return

        if self._opts['cluster_id']:
            return

        # if a cluster we created self-terminated, something is wrong with
        # the way self-termination is set up (e.g. very low idle time)
        if self._created_cluster:
            return

        # don't check for max_hours_idle because it's possible to
        # join a self-terminating cluster without having max_hours_idle set
        # on this runner (pooling only cares about the master bootstrap script,
        # not other bootstrap actions)

        # our step should be CANCELLED (not failed)
        if step.status.state != 'CANCELLED':
            return

        # we *could* check if the step had a chance to start by checking if
        # step.status.timeline.startdatetime is set. This shouldn't happen in
        # practice, and if it did, we'd still be fine as long as the script
        # didn't write data to the output dir, so it's not worth the extra
        # code.

        # cluster should have stopped because master node failed
        # could also check for
        # cluster.status.statechangereason.code == 'INSTANCE_FAILURE'
        if not _CLUSTER_SELF_TERMINATED_RE.match(_get_reason(cluster)):
            return

        log.info('Pooled cluster self-terminated, trying again...')
        raise _PooledClusterSelfTerminatedException

    def _check_for_missing_default_iam_roles(self, cluster):
        """If cluster couldn't start due to missing IAM roles, tell
        user what to do."""
        if not cluster:
            cluster = self._describe_cluster()

        reason = _get_reason(cluster)
        if any(reason.endswith('/%s is invalid' % role)
               for role in (_FALLBACK_INSTANCE_PROFILE,
                            _FALLBACK_SERVICE_ROLE)):
            log.warning(
                '\n'
                'Ask your admin to create the default EMR roles'
                ' by following:\n\n'
                '    http://docs.aws.amazon.com/ElasticMapReduce/latest'
                '/DeveloperGuide/emr-iam-roles-creatingroles.html\n')

    def _check_for_key_pair_from_wrong_region(self, cluster):
        """Help users tripped up by the default AWS region changing
        in mrjob v0.5.0 (see #1111) by pointing them to the
        docs for creating EC2 key pairs."""
        if not self._opts.is_default('region'):
            return

        reason = _get_reason(cluster)
        if reason == 'The given SSH key name was invalid':
            log.warning(
                '\n'
                'The default AWS region is now %s. Create SSH keys for'
                ' %s by following:\n\n'
                '    https://pythonhosted.org/mrjob/guides'
                '/emr-quickstart.html#configuring-ssh-credentials\n' %
                (_DEFAULT_REGION, _DEFAULT_REGION))

    def _default_step_output_dir(self):
        # put intermediate data in HDFS
        return 'hdfs:///tmp/mrjob/%s/step-output' % self._job_key

    ### LOG PARSING (implementation of LogInterpretationMixin) ###

    def _check_for_failed_bootstrap_action(self, cluster):
        """If our bootstrap actions failed, parse the stderr to find
        out why."""
        reason = _get_reason(cluster)
        action_num_and_node_id = _check_for_nonzero_return_code(reason)
        if not action_num_and_node_id:
            return

        # this doesn't really correspond to a step, so
        # don't bother storing it in self._log_interpretations
        bootstrap_interpretation = _interpret_emr_bootstrap_stderr(
            self.fs, self._ls_bootstrap_stderr_logs(**action_num_and_node_id))

        # should be 0 or 1 errors, since we're checking a single stderr file
        if bootstrap_interpretation.get('errors'):
            error = bootstrap_interpretation['errors'][0]
            log.error('Probable cause of failure:\n\n%s\n\n' %
                      _format_error(error))

    def _ls_bootstrap_stderr_logs(self, action_num=None, node_id=None):
        """_ls_bootstrap_stderr_logs(), with logging for each log we parse."""
        for match in _ls_emr_bootstrap_stderr_logs(
                self.fs,
                self._stream_bootstrap_log_dirs(
                    action_num=action_num, node_id=node_id),
                action_num=action_num,
                node_id=node_id):
            log.info('  Parsing boostrap stderr log: %s' % match['path'])
            yield match

    def _stream_bootstrap_log_dirs(self, action_num=None, node_id=None):
        """Stream a single directory on S3 containing the relevant bootstrap
        stderr. Optionally, use *action_num* and *node_id* to narrow it down
        further.
        """
        if action_num is None or node_id is None:
            s3_dir_name = 'node'
        else:
            s3_dir_name = posixpath.join(
                'node', node_id, 'bootstrap-actions', str(action_num + 1))

        # dir_name=None means don't try to SSH in.
        #
        # TODO: If the failure is on the master node, we could just look in
        # /mnt/var/log/bootstrap-actions. However, if it's on a slave node,
        # we'd have to look up its internal IP using the ListInstances
        # API call. This *would* be a bit faster though. See #1346.
        return self._stream_log_dirs(
            'bootstrap logs',
            dir_name=None,  # don't SSH in
            s3_dir_name=s3_dir_name)

    def _stream_history_log_dirs(self, output_dir=None):
        """Yield lists of directories to look for the history log in."""
        # History logs have different paths on the 4.x AMIs.
        #
        # Disabling until we can effectively fetch these logs over SSH;
        # on 4.3.0 there are permissions issues (see #1244), and
        # on 4.0.0 the logs aren't on the filesystem at all (see #1253).
        #
        # Unlike on 3.x, the history logs *are* available on S3, but they're
        # not useful enough to justify the wait when SSH is set up

        # if version_gte(self.get_image_version(), '4'):
        #     # denied access on some 4.x AMIs by the yarn user, see #1244
        #     dir_name = 'hadoop-mapreduce/history'
        #     s3_dir_name = 'hadoop-mapreduce/history'
        if version_gte(self.get_image_version(), '3'):
            # on the 3.x AMIs, the history log lives inside HDFS and isn't
            # copied to S3. We don't need it anyway; everything relevant
            # is in the step log
            return iter([])
        else:
            dir_name = 'hadoop/history'
            s3_dir_name = 'jobs'

        return self._stream_log_dirs(
            'history log',
            dir_name=dir_name,
            s3_dir_name=s3_dir_name)

    def _stream_task_log_dirs(self, application_id=None, output_dir=None):
        """Get lists of directories to look for the task logs in."""
        if version_gte(self.get_image_version(), '4'):
            # denied access on some 4.x AMIs by the yarn user, see #1244
            dir_name = 'hadoop-yarn/containers'
            s3_dir_name = 'containers'
        else:
            dir_name = 'hadoop/userlogs'
            s3_dir_name = 'task-attempts'

        if application_id:
            dir_name = posixpath.join(dir_name, application_id)
            s3_dir_name = posixpath.join(s3_dir_name, application_id)

        return self._stream_log_dirs(
            'task logs',
            dir_name=dir_name,
            s3_dir_name=s3_dir_name,
            ssh_to_workers=True)  # TODO: does this make sense on YARN?

    def _get_step_log_interpretation(self, log_interpretation, step_type):
        """Fetch and interpret the step log."""
        step_id = log_interpretation.get('step_id')
        if not step_id:
            log.warning("Can't fetch step log; missing step ID")
            return

        if _is_spark_step_type(step_type):
            # Spark also has a "controller" log4j log, but it doesn't
            # contain errors or anything else we need
            return _interpret_emr_step_syslog(
                self.fs, self._ls_step_stderr_logs(step_id=step_id))
        else:
            return (
                _interpret_emr_step_syslog(
                    self.fs, self._ls_step_syslogs(step_id=step_id)) or
                _interpret_emr_step_stderr(
                    self.fs, self._ls_step_stderr_logs(step_id=step_id))
            )

    def _ls_step_syslogs(self, step_id):
        """Yield step log matches, logging a message for each one."""
        for match in _ls_emr_step_syslogs(
                self.fs, self._stream_step_log_dirs(step_id=step_id),
                step_id=step_id):
            log.info('  Parsing step log: %s' % match['path'])
            yield match

    def _ls_step_stderr_logs(self, step_id):
        """Yield step log matches, logging a message for each one."""
        for match in _ls_emr_step_stderr_logs(
                self.fs, self._stream_step_log_dirs(step_id=step_id),
                step_id=step_id):
            log.info('  Parsing step log: %s' % match['path'])
            yield match

    def _stream_step_log_dirs(self, step_id):
        """Get lists of directories to look for the step log in."""
        return self._stream_log_dirs(
            'step log',
            dir_name=posixpath.join('hadoop', 'steps', step_id),
            s3_dir_name=posixpath.join('steps', step_id))

    def _stream_log_dirs(self, log_desc, dir_name, s3_dir_name,
                         ssh_to_workers=False):
        """Stream log dirs for any kind of log.

        Our general strategy is first, if SSH is enabled, to SSH into the
        master node (and possibly slaves, if *ssh_to_workers* is set).

        If this doesn't work, we have to look on S3. If the cluster is
        TERMINATING, we first wait for it to terminate (since that
        will trigger copying logs over).
        """
        if dir_name and self.fs.can_handle_path('ssh:///'):
            ssh_host = self._address_of_master()
            if ssh_host:
                hosts = [ssh_host]
                host_desc = ssh_host
                if ssh_to_workers:
                    try:
                        hosts.extend(self._ssh_worker_hosts())
                        host_desc += ' and task/core nodes'
                    except IOError:
                        log.warning('Could not get slave addresses for %s' %
                                    ssh_host)

                path = posixpath.join(_EMR_LOG_DIR, dir_name)
                log.info('Looking for %s in %s on %s...' % (
                    log_desc, path, host_desc))
                yield ['ssh://%s%s%s' % (
                    ssh_host, '!' + host if host != ssh_host else '',
                    path) for host in hosts]

        # wait for logs to be on S3
        self._wait_for_logs_on_s3()

        s3_dir_name = s3_dir_name or dir_name

        if s3_dir_name and self._s3_log_dir():
            cloud_log_dir = posixpath.join(self._s3_log_dir(), s3_dir_name)
            log.info('Looking for %s in %s...' % (log_desc, cloud_log_dir))
            yield [cloud_log_dir]

    def _ssh_worker_hosts(self):
        """Get the hostnames of all core and task nodes,
        that are currently running, so we can SSH to them through the master
        nodes and read their logs.

        (This currently returns IP addresses rather than full hostnames
        because they're shorter.)
        """
        return [
            instance.privateipaddress for instance in
            _yield_all_instances(
                self.make_emr_conn(),
                self._cluster_id,
                instance_group_types=['CORE', 'TASK'])
            if instance.status.state == 'RUNNING'
        ]

    def _wait_for_logs_on_s3(self):
        """If the cluster is already terminating, wait for it to terminate,
        so that logs will be transferred to S3.

        Don't print anything unless cluster is in the TERMINATING state.
        """
        cluster = self._describe_cluster()

        if cluster.status.state in (
                'TERMINATED', 'TERMINATED_WITH_ERRORS'):
            return  # already terminated

        if cluster.status.state != 'TERMINATING':
            # going to need to wait for logs to get archived to S3

            # "step_num" is just a unique ID for the step; using -1
            # for master node setup script
            if (self._master_node_setup_script_path and
                    self._mns_log_interpretation is None):
                step_num = -1
            else:
                step_num = len(self._log_interpretations)

            # already did this for this step
            if step_num in self._waited_for_logs_on_s3:
                return

            try:
                log.info('Waiting %d minutes for logs to transfer to S3...'
                         ' (ctrl-c to skip)' % _S3_LOG_WAIT_MINUTES)

                if not self.fs.can_handle_path('ssh:///'):
                    log.info(
                        '\n'
                        'To fetch logs immediately next time, set up SSH.'
                        ' See:\n'
                        'https://pythonhosted.org/mrjob/guides'
                        '/emr-quickstart.html#configuring-ssh-credentials\n')

                time.sleep(60 * _S3_LOG_WAIT_MINUTES)
            except KeyboardInterrupt:
                pass

            # do this even if they ctrl-c'ed; don't make them do it
            # for every log for this step
            self._waited_for_logs_on_s3.add(step_num)
            return

        self._wait_for_cluster_to_terminate()

    def counters(self):
        # not using self._pick_counters() because we don't want to
        # initiate a log fetch
        return [_pick_counters(log_interpretation)
                for log_interpretation in self._log_interpretations]

    ### Bootstrapping ###

    def _create_master_bootstrap_script_if_needed(self):
        """Helper for :py:meth:`_add_bootstrap_files_for_upload`.

        Create the master bootstrap script and write it into our local
        temp directory. Set self._master_bootstrap_script_path.

        This will do nothing if there are no bootstrap scripts or commands,
        or if it has already been called."""
        if self._master_bootstrap_script_path:
            return

        # don't bother if we're not starting a cluster
        if self._opts['cluster_id']:
            return

        # Also don't bother if we're not bootstrapping
        # (unless we're pooling, in which case we need the bootstrap
        # script to attach the pool hash too; see #1503).
        if not (self._bootstrap or self._legacy_bootstrap or
                self._opts['bootstrap_files'] or
                self._bootstrap_mrjob() or
                self._opts['pool_clusters']):
            return

        # create mrjob.zip if we need it, and add commands to install it
        mrjob_bootstrap = []
        if self._bootstrap_mrjob():
            # _add_bootstrap_files_for_upload() should have done this
            assert self._mrjob_zip_path
            path_dict = {
                'type': 'file', 'name': None, 'path': self._mrjob_zip_path}
            self._bootstrap_dir_mgr.add(**path_dict)

            # find out where python keeps its libraries
            mrjob_bootstrap.append([
                "__mrjob_PYTHON_LIB=$(%s -c "
                "'from distutils.sysconfig import get_python_lib;"
                " print(get_python_lib())')" %
                cmd_line(self._python_bin())])

            # remove anything that might be in the way (see #1567)
            mrjob_bootstrap.append(['sudo rm -rf $__mrjob_PYTHON_LIB/mrjob'])

            # copy mrjob.zip over
            mrjob_bootstrap.append(
                ['sudo unzip ', path_dict, ' -d $__mrjob_PYTHON_LIB'])

            # re-compile pyc files now, since mappers/reducers can't
            # write to this directory. Don't fail if there is extra
            # un-compileable crud in the tarball (this would matter if
            # sh_bin were 'sh -e')
            mrjob_bootstrap.append(
                ['sudo %s -m compileall -q -f $__mrjob_PYTHON_LIB/mrjob'
                 ' && true' % cmd_line(self._python_bin())])

        # TODO: shouldn't it be b.sh now?
        # we call the script b.py because there's a character limit on
        # bootstrap script names (or there was at one time, anyway)
        path = os.path.join(self._get_local_tmp_dir(), 'b.sh')
        log.debug('writing master bootstrap script to %s' % path)

        contents = self._master_bootstrap_script_content(
            self._bootstrap + mrjob_bootstrap + self._legacy_bootstrap)
        for line in contents:
            log.debug('BOOTSTRAP: ' + line.rstrip('\r\n'))

        # TODO: Windows line endings?
        with open(path, 'w') as f:
            for line in contents:
                f.write(line)

        self._master_bootstrap_script_path = path

    def _bootstrap_python(self):
        """Return a (possibly empty) list of parsed commands (in the same
        format as returned by parse_setup_cmd())'"""
        if PY2:
            # Python 2 and pip are basically already installed everywhere
            # (Okay, there's no pip on AMIs prior to 2.4.3, but there's no
            # longer an easy way to get it now that apt-get is broken.)
            return []

        # if bootstrap_python is None, install it for all AMIs up to 4.6.0,
        # and warn if it's an AMI before 3.7.0
        if self._opts['bootstrap_python'] or (
                self._opts['bootstrap_python'] is None and
                not self._image_version_gte('4.6.0')):

            # we have to have at least on AMI 3.7.0. But give it a shot
            if not self._image_version_gte('3.7.0'):
                log.warning(
                    'bootstrapping Python 3 will probably not work on'
                    ' AMIs prior to 3.7.0. For an alternative, see:'
                    ' https://pythonhosted.org/mrjob/guides/emr-bootstrap'
                    '-cookbook.html#installing-python-from-source')

            return [[
                'sudo yum install -y python34 python34-devel python34-pip'
            ]]
        else:
            return []

    def _should_bootstrap_spark(self):
        """Return *bootstrap_spark* option if set; otherwise return
        true if our job has Spark steps."""
        if self._opts['bootstrap_spark'] is None:
            return self._has_spark_steps()
        else:
            return bool(self._opts['bootstrap_spark'])

    def _applications(self, add_spark=True):
        """Returns applications (*applications* option) as a set. Adds
        in ``Hadoop`` and ``Spark`` as needed."""
        applications = set(self._opts['applications'])

        # release_label implies 4.x AMI and later
        if (add_spark and self._should_bootstrap_spark() and
                self._opts['release_label']):
            # EMR allows us to have both "spark" and "Spark" applications,
            # which is probably not what we want
            if not self._has_spark_application():
                applications.add('Spark')

        # patch in "Hadoop" unless applications is empty (e.g. 3.x AMIs)
        if applications:
            # don't add both "Hadoop" and "hadoop"
            if not any(a.lower() == 'hadoop' for a in applications):
                applications.add('Hadoop')

        return applications

    def _bootstrap_actions(self, add_spark=True):
        """Parse *bootstrap_actions* option into dictionaries with
        keys *path*, *args*, adding Spark bootstrap action if needed.

        (This doesn't handle the master bootstrap script.)
        """
        actions = list(self._opts['bootstrap_actions'])

        # no release_label implies AMIs prior to 4.x
        if (add_spark and self._should_bootstrap_spark() and
                not self._opts['release_label']):

            # running this action twice apparently breaks Spark's
            # ability to output to S3 (see #1367)
            if not self._has_spark_install_bootstrap_action():
                actions.append(_3_X_SPARK_BOOTSTRAP_ACTION)

        results = []
        for action in actions:
            args = shlex_split(action)
            if not args:
                raise ValueError('bad bootstrap action: %r' % (action,))

            results.append(dict(path=args[0], args=args[1:]))

        return results

    def _parse_bootstrap(self):
        """Parse the *bootstrap* option with
        :py:func:`mrjob.setup.parse_setup_cmd()`.
        """
        return [parse_setup_cmd(cmd) for cmd in self._opts['bootstrap']]

    def _parse_legacy_bootstrap(self):
        """Parse the deprecated
        options *bootstrap_python_packages*, and *bootstrap_cmds*
        *bootstrap_scripts* as bootstrap commands, in that order.

        This is a separate method from _parse_bootstrap() because bootstrapping
        mrjob happens after the new bootstrap commands (so you can upgrade
        Python) but before the legacy commands (for backwards compatibility).
        """
        bootstrap = []

        # bootstrap_python_packages. Deprecated but still works, except
        if self._opts['bootstrap_python_packages']:
            log.warning(
                'bootstrap_python_packages is deprecated since v0.4.2'
                ' and will be removed in v0.6.0. Consider using'
                ' bootstrap instead.')

            # bootstrap_python_packages won't work on AMI 3.0.0 (out-of-date
            # SSL keys) and AMI 2.4.2 and earlier (no pip, and have to fix
            # sources.list to apt-get it). These AMIs are so old it's probably
            # not worth dedicating code to this, but can add a warning if
            # need be.

            for path in self._opts['bootstrap_python_packages']:
                path_dict = parse_legacy_hash_path('file', path)

                python_bin = cmd_line(self._python_bin())

                # More robust to use Python than pip binary; for example, there
                # is a python3 binary but no pip-3 (only pip-3.4)
                bootstrap.append(
                    ['sudo %s -m pip install ' % python_bin, path_dict])

        # setup_cmds
        if self._opts['bootstrap_cmds']:
            log.warning(
                "bootstrap_cmds is deprecated since v0.4.2 and will be"
                " removed in v0.6.0. Consider using bootstrap instead.")
        for cmd in self._opts['bootstrap_cmds']:
            if not isinstance(cmd, string_types):
                cmd = cmd_line(cmd)
            bootstrap.append([cmd])

        # bootstrap_scripts
        if self._opts['bootstrap_scripts']:
            log.warning(
                "bootstrap_scripts is deprecated since v0.4.2 and will be"
                " removed in v0.6.0. Consider using bootstrap instead.")

        for path in self._opts['bootstrap_scripts']:
            path_dict = parse_legacy_hash_path('file', path)
            bootstrap.append([path_dict])

        return bootstrap

    # helper for _master_*_script_content() methods
    def _write_start_of_sh_script(self, writeln):
        # shebang
        sh_bin = self._sh_bin()
        if not sh_bin[0].startswith('/'):
            sh_bin = ['/usr/bin/env'] + sh_bin
        writeln('#!' + cmd_line(sh_bin))

        # hook for 'set -e', etc. (see #1549)
        for cmd in self._sh_pre_commands():
            writeln(cmd)

        writeln()

    def _master_bootstrap_script_content(self, bootstrap):
        """Create the contents of the master bootstrap script.
        """
        out = []

        def writeln(line=''):
            out.append(line + '\n')

        # shebang, etc.
        self._write_start_of_sh_script(writeln)

        # store $PWD
        writeln('# store $PWD')
        writeln('__mrjob_PWD=$PWD')
        writeln()

        # run commands in a block so we can redirect stdout to stderr
        # (e.g. to catch errors from compileall). See #370
        writeln('{')

        # download files
        writeln('  # download files and mark them executable')

        if self._opts['release_label']:
            # on the 4.x AMIs, hadoop isn't yet installed, so use AWS CLI
            cp_to_local = 'aws s3 cp'
        else:
            # on the 2.x and 3.x AMIs, use hadoop
            cp_to_local = 'hadoop fs -copyToLocal'

        # TODO: why bother with $__mrjob_PWD here, since we're already in it?
        for name, path in sorted(
                self._bootstrap_dir_mgr.name_to_path('file').items()):
            uri = self._upload_mgr.uri(path)
            writeln('  %s %s $__mrjob_PWD/%s' %
                    (cp_to_local, pipes.quote(uri), pipes.quote(name)))
            # make everything executable, like Hadoop Distributed Cache
            writeln('  chmod a+x $__mrjob_PWD/%s' % pipes.quote(name))
        writeln()

        # run bootstrap commands
        writeln('  # bootstrap commands')
        for cmd in bootstrap:
            # reconstruct the command line, substituting $__mrjob_PWD/<name>
            # for path dicts
            line = '  '
            for token in cmd:
                if isinstance(token, dict):
                    # it's a path dictionary
                    line += '$__mrjob_PWD/'
                    line += pipes.quote(self._bootstrap_dir_mgr.name(**token))
                else:
                    # it's raw script
                    line += token
            writeln(line)

        writeln('} 1>&2')  # stdout -> stderr for ease of error log parsing

        return out

    ### master node setup script ###

    def _create_master_node_setup_script_if_needed(self):
        """Helper for :py:meth:`_add_bootstrap_files_for_upload`.

        If we need a master node setup script and write it into our local
        temp directory. Set self._master_node_setup_script_path.
        """
        # already created
        if self._master_node_setup_script_path:
            return

        # currently, the only thing this script does is upload files
        if not self._master_node_setup_mgr.paths():
            return

        # create script
        path = os.path.join(self._get_local_tmp_dir(), 'mns.sh')
        log.debug('writing master node setup script to %s' % path)

        contents = self._master_node_setup_script_content()
        for line in contents:
            log.debug('MASTER NODE SETUP: ' + line.rstrip('\r\n'))

        with open(path, 'wb') as f:
            for line in contents:
                f.write(line.encode('utf-8'))

        # the script itself doesn't need to be on the master node, just S3
        self._master_node_setup_script_path = path
        self._upload_mgr.add(path)

    def _master_node_setup_script_content(self):
        """Create the contents of the master node setup script as an
        array of strings.

        (prepare self._master_node_setup_mgr first)
        """
        # TODO: this is very similar to _master_bootstrap_script_content();
        # merge common code
        out = []

        def writeln(line=''):
            out.append(line + '\n')

        # shebang, etc.
        self._write_start_of_sh_script(writeln)

        # run commands in a block so we can redirect stdout to stderr
        # (e.g. to catch errors from compileall). See #370
        writeln('{')

        # make working dir
        working_dir = self._master_node_setup_working_dir()
        writeln('  mkdir -p %s' % pipes.quote(working_dir))
        writeln('  cd %s' % pipes.quote(working_dir))
        writeln()

        # download files
        if self._opts['release_label']:
            # on the 4.x AMIs, hadoop isn't yet installed, so use AWS CLI
            cp_to_local = 'aws s3 cp'
        else:
            # on the 2.x and 3.x AMIs, use hadoop
            cp_to_local = 'hadoop fs -copyToLocal'

        for name, path in sorted(
                self._master_node_setup_mgr.name_to_path('file').items()):
            uri = self._upload_mgr.uri(path)
            writeln('  %s %s %s' % (
                cp_to_local, pipes.quote(uri), pipes.quote(name)))
            # make everything executable, like Hadoop Distributed Cache
            writeln('  chmod a+x %s' % pipes.quote(name))

        # at some point we will probably run commands as well (see #1336)

        writeln('} 1>&2')  # stdout -> stderr for ease of error log parsing

        return out

    def _master_node_setup_working_dir(self):
        """Where to place files used by the master node setup script."""
        return '/home/hadoop/%s' % self._job_key

    def _script_runner_jar_uri(self):
        return (
            's3://%s.elasticmapreduce/libs/script-runner/script-runner.jar' %
            self._opts['region'])

    ### EMR JOB MANAGEMENT UTILS ###

    def make_persistent_job_flow(self):
        """Create a new EMR cluster that requires manual termination, and
        return its ID.

        You can also fetch the job ID by calling self.get_cluster_id()
        """
        log.warning(
            'make_persistent_job_flow() has been renamed to'
            ' make_persistent_cluster(). This alias will be removed in v0.6.0')

        return self.make_persistent_cluster()

    def make_persistent_cluster(self):
        if (self._cluster_id):
            raise AssertionError(
                'This runner is already associated with cluster ID %s' %
                (self._cluster_id))

        log.info('Creating persistent cluster to run several jobs in...')

        self._add_bootstrap_files_for_upload(persistent=True)
        self._upload_local_files_to_s3()

        # don't allow user to call run()
        self._ran_job = True

        self._cluster_id = self._create_cluster(persistent=True)

        return self._cluster_id

    def get_emr_job_flow_id(self):
        log.warning(
            'get_emr_job_flow_id() has been renamed to get_cluster_id().'
            ' This alias will be removed in v0.6.0')

        return self.get_cluster_id()

    def get_cluster_id(self):
        return self._cluster_id

    def _usable_clusters(self, emr_conn=None, exclude=None, num_steps=1):
        """Get clusters that this runner can join.

        We basically expect to only join available clusters with the exact
        same setup as our own, that is:

        - same bootstrap setup (including mrjob version)
        - have the same AMI version
        - install the same applications (if we requested any)
        - same number and type of instances

        However, we allow joining clusters where for each role, every instance
        has at least as much memory as we require, and the total number of
        compute units is at least what we require.

        There also must be room for our job in the cluster (clusters top out
        at 256 steps).

        We then sort by:
        - total compute units for core + task nodes
        - total compute units for master node
        - time left to an even instance hour

        The most desirable clusters come *last* in the list.

        :return: tuple of (:py:class:`botoemr.emrobject.Cluster`,
                           num_steps_in_cluster)
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

        for role in _INSTANCE_ROLES:
            instance_type = self._instance_type(role)
            num_instances = self._num_instances(role)

            role_to_req_instance_type[role] = instance_type
            role_to_req_num_instances[role] = num_instances
            role_to_req_bid_price[role] = self._instance_bid_price(role)

            # unknown instance types can only match themselves
            role_to_req_mem[role] = (
                EC2_INSTANCE_TYPE_TO_MEMORY.get(instance_type, float('Inf')))
            role_to_req_cu[role] = (
                num_instances *
                EC2_INSTANCE_TYPE_TO_COMPUTE_UNITS.get(instance_type,
                                                       float('Inf')))

        # list of (sort_key, cluster_id, num_steps)
        key_cluster_steps_list = []

        def add_if_match(cluster):
            log.debug('  Considering joining cluster %s...' % cluster.id)

            # skip if user specified a key pair and it doesn't match
            if (self._opts['ec2_key_pair'] and
                self._opts['ec2_key_pair'] !=
                getattr(getattr(cluster,
                                'ec2instanceattributes', None),
                        'ec2keyname', None)):
                log.debug('    ec2 key pair mismatch')
                return

            # this may be a retry due to locked clusters
            if cluster.id in exclude:
                log.debug('    excluded')
                return

            # only take persistent clusters
            if cluster.autoterminate != 'false':
                log.debug('    not persistent')
                return

            # match pool name, and (bootstrap) hash
            bootstrap_actions = list(_yield_all_bootstrap_actions(
                emr_conn, cluster.id))
            pool_hash, pool_name = _pool_hash_and_name(bootstrap_actions)

            if req_hash != pool_hash:
                log.debug('    pool hash mismatch')
                return

            if self._opts['pool_name'] != pool_name:
                log.debug('    pool name mismatch')
                return

            if self._opts['release_label']:
                # just check for exact match. EMR doesn't have a concept
                # of partial release labels like it does for AMI versions.
                release_label = getattr(cluster, 'releaselabel', '')

                if release_label != self._opts['release_label']:
                    log.debug('    release label mismatch')
                    return

                # used below
                max_steps = _4_X_MAX_STEPS
            else:
                # match actual AMI version
                image_version = getattr(cluster, 'runningamiversion', '')
                # Support partial matches, e.g. let a request for
                # '2.4' pass if the version is '2.4.2'. The version
                # extracted from the existing cluster should always
                # be a full major.minor.patch, so checking matching
                # prefixes should be sufficient.
                if not image_version.startswith(self._opts['image_version']):
                    log.debug('    image version mismatch')
                    return

                max_steps = map_version(
                    image_version, _IMAGE_VERSION_TO_MAX_STEPS)

            applications = self._applications()
            if applications:
                # use case-insensitive mapping (see #1417)
                cluster_applications = set(
                    a.name.lower() for a in cluster.applications)

                expected_applications = set(
                    a.lower() for a in applications)

                if not expected_applications <= cluster_applications:
                    log.debug('    missing applications: %s' % ', '.join(
                        sorted(expected_applications - cluster_applications)))
                    return

            emr_configurations = _decode_configurations_from_api(
                getattr(cluster, 'configurations', []))
            if self._opts['emr_configurations'] != emr_configurations:
                log.debug('    emr configurations mismatch')
                return

            subnet = getattr(
                cluster.ec2instanceattributes, 'ec2subnetid', None)
            if subnet != (self._opts['subnet'] or None):
                log.debug('    subnet mismatch')
                return

            steps = _list_all_steps(emr_conn, cluster.id)

            # don't add more steps than EMR will allow/display through the API
            if len(steps) + num_steps > max_steps:
                log.debug('    no room for our steps')
                return

            # in rare cases, cluster can be WAITING *and* have incomplete
            # steps. We could just check for PENDING steps, but we're
            # trying to be defensive about EMR adding a new step state.
            #
            # TODO: checking for PENDING steps seems pretty safe
            for step in steps:
                if ((getattr(step.status, 'timeline', None) is None or
                     getattr(step.status.timeline, 'enddatetime', None)
                     is None) and
                    getattr(step.status, 'state', None) not in
                        ('CANCELLED', 'INTERRUPTED')):
                    log.debug('    unfinished steps')
                    return

            # total compute units per group
            role_to_cu = defaultdict(float)
            # total number of instances of the same type in each group.
            # This allows us to match unknown instance types.
            role_to_matched_instances = defaultdict(int)

            # check memory and compute units, bailing out if we hit
            # an instance with too little memory

            try:
                igs = list(_yield_all_instance_groups(emr_conn, cluster.id))
            except boto.exception.EmrResponseError:
                # don't error if we encounter an instance fleet (see #1639)
                return

            for ig in igs:
                # if you edit this code, please don't rely on any particular
                # ordering of instance groups (see #1316)
                role = ig.instancegrouptype.lower()

                # unknown, new kind of role; bail out!
                if role not in ('core', 'master', 'task'):
                    log.debug('    unknown instance group role: %s' % role)
                    return

                req_instance_type = role_to_req_instance_type[role]
                if ig.instancetype != req_instance_type:
                    # if too little memory, bail out
                    mem = EC2_INSTANCE_TYPE_TO_MEMORY.get(ig.instancetype, 0.0)
                    req_mem = role_to_req_mem.get(role, 0.0)
                    if mem < req_mem:
                        log.debug('    too little memory')
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
                # we started our own cluster from scratch. (This can happen if
                # the previous job finished while some task instances were
                # still being provisioned.)
                cu = (int(ig.requestedinstancecount) *
                      EC2_INSTANCE_TYPE_TO_COMPUTE_UNITS.get(
                          ig.instancetype, 0.0))
                role_to_cu.setdefault(role, 0.0)
                role_to_cu[role] += cu

                # track number of instances of the same type
                if ig.instancetype == req_instance_type:
                    role_to_matched_instances[role] += (
                        int(ig.requestedinstancecount))

            # check if there are enough compute units
            for role, req_cu in role_to_req_cu.items():
                req_num_instances = role_to_req_num_instances[role]
                # if we have at least as many units of the right type,
                # don't bother counting compute units
                if req_num_instances > role_to_matched_instances[role]:
                    cu = role_to_cu.get(role, 0.0)
                    if cu < req_cu:
                        log.debug('    too few compute units')
                        return

            # make a sort key
            sort_key = (role_to_cu['core'] + role_to_cu['task'],
                        role_to_cu['master'],
                        _est_time_to_hour(cluster))

            log.debug('    OK')
            key_cluster_steps_list.append((sort_key, cluster.id, len(steps)))

        for cluster_summary in _yield_all_clusters(
                emr_conn, cluster_states=['WAITING']):
            cluster = _patched_describe_cluster(emr_conn, cluster_summary.id)
            add_if_match(cluster)

        return [(cluster_id, cluster_num_steps) for
                (sort_key, cluster_id, cluster_num_steps)
                in sorted(key_cluster_steps_list)]

    def _find_cluster(self, num_steps=1):
        """Find a cluster that can host this runner. Prefer clusters with more
        compute units. Break ties by choosing cluster with longest idle time.
        Return ``None`` if no suitable clusters exist.
        """
        exclude = set()
        emr_conn = self.make_emr_conn()
        max_wait_time = self._opts['pool_wait_minutes']
        now = datetime.now()
        end_time = now + timedelta(minutes=max_wait_time)
        time_sleep = timedelta(seconds=_POOLING_SLEEP_INTERVAL)

        log.info('Attempting to find an available cluster...')
        while now <= end_time:
            cluster_info_list = self._usable_clusters(
                emr_conn=emr_conn,
                exclude=exclude,
                num_steps=num_steps)
            log.debug(
                '  Found %d usable clusters%s%s' % (
                    len(cluster_info_list),
                    ': ' if cluster_info_list else '',
                    ', '.join(c for c, n in reversed(cluster_info_list))))
            if cluster_info_list:
                cluster_id, num_steps = cluster_info_list[-1]
                status = _attempt_to_acquire_lock(
                    self.fs, self._lock_uri(cluster_id, num_steps),
                    self._opts['cloud_fs_sync_secs'], self._job_key)
                if status:
                    log.debug('Acquired lock on cluster %s', cluster_id)
                    return cluster_id
                else:
                    log.debug("Can't acquire lock on cluster %s", cluster_id)
                    exclude.add(cluster_id)
            elif max_wait_time == 0:
                return None
            else:
                # Reset the exclusion set since it is possible to reclaim a
                # lock that was previously unavailable.
                exclude = set()
                log.info('No clusters available in pool %r. Checking again'
                         ' in %d seconds...' % (
                             self._opts['pool_name'],
                             int(_POOLING_SLEEP_INTERVAL)))
                time.sleep(_POOLING_SLEEP_INTERVAL)
                now += time_sleep
        return None

    def _lock_uri(self, cluster_id, num_steps):
        return _make_lock_uri(self._opts['cloud_tmp_dir'],
                              cluster_id,
                              num_steps + 1)

    def _pool_hash(self):
        """Generate a hash of the bootstrap configuration so it can be used to
        match jobs and clusters. This first argument passed to the bootstrap
        script will be ``'pool-'`` plus this hash.

        The way the hash is calculated may vary between point releases
        (pooling requires the exact same version of :py:mod:`mrjob` anyway).
        """
        things_to_hash = [
            # exclude mrjob.zip because it's only created if the
            # job starts its own cluster (also, its hash changes every time
            # since the zip file contains different timestamps).
            # The filenames/md5sums are sorted because we need to
            # ensure the order they're added doesn't affect the hash
            # here. Previously this used a dict, but Python doesn't
            # guarantee the ordering of dicts -- they can vary
            # depending on insertion/deletion order.
            sorted(
                (name, self.fs.md5sum(path)) for name, path
                in self._bootstrap_dir_mgr.name_to_path('file').items()
                if not path == self._mrjob_zip_path),
            self._opts['additional_emr_info'],
            self._bootstrap,
            self._bootstrap_actions(),
            self._opts['bootstrap_cmds'],
            self._bootstrap_mrjob(),
        ]

        if self._bootstrap_mrjob():
            things_to_hash.append(mrjob.__version__)
            things_to_hash.append(self._python_bin())
            things_to_hash.append(self._task_python_bin())

        things_json = json.dumps(things_to_hash, sort_keys=True)
        if not isinstance(things_json, bytes):
            things_json = things_json.encode('utf_8')

        m = hashlib.md5()
        m.update(things_json)
        return m.hexdigest()

    ### EMR-specific Stuff ###

    def make_emr_conn(self):
        """Create a connection to EMR.

        :return: a :py:class:`boto.emr.connection.EmrConnection`,
                 wrapped in a :py:class:`mrjob.retry.RetryWrapper`
        """
        # ...which is then wrapped in bacon! Mmmmm!

        # give a non-cryptic error message if boto isn't installed
        if boto is None:
            raise ImportError('You must install boto to connect to EMR')

        def emr_conn_for_endpoint(endpoint):
            conn = boto.emr.connection.EmrConnection(
                aws_access_key_id=self._opts['aws_access_key_id'],
                aws_secret_access_key=self._opts['aws_secret_access_key'],
                region=boto.regioninfo.RegionInfo(
                    name=self._opts['region'], endpoint=endpoint,
                    connection_cls=boto.emr.connection.EmrConnection),
                security_token=self._opts['aws_session_token'])

            return conn

        endpoint = (self._opts['emr_endpoint'] or
                    emr_endpoint_for_region(self._opts['region']))

        log.debug('creating EMR connection (to %s)' % endpoint)
        conn = emr_conn_for_endpoint(endpoint)

        # Issue #621: if we're using a region-specific endpoint,
        # try both the canonical version of the hostname and the one
        # that matches the SSL cert
        if not self._opts['emr_endpoint']:

            ssl_host = emr_ssl_host_for_region(self._opts['region'])
            fallback_conn = emr_conn_for_endpoint(ssl_host)

            conn = RetryGoRound(
                [conn, fallback_conn],
                lambda ex: isinstance(
                    ex, boto.https_connection.InvalidCertificateException))

        return wrap_aws_conn(conn)

    def _describe_cluster(self):
        emr_conn = self.make_emr_conn()
        return _patched_describe_cluster(emr_conn, self._cluster_id)

    def get_hadoop_version(self):
        return self._get_app_versions().get('hadoop')

    def get_ami_version(self):
        log.warning('get_ami_version() is a depreacated alias for'
                    ' get_image_version() and will be removed in'
                    ' mrjob v0.6.0')
        return self.get_image_version()

    def get_image_version(self):
        """Get the AMI that our cluster is running.

        .. versionchanged:: 0.5.4

           This used to be called :py:meth:`get_ami_version`

        .. versionadded:: 0.4.5
        """
        return self._get_cluster_info('image_version')

    def _address_of_master(self):
        """Get the address of the master node so we can SSH to it"""
        return self._get_cluster_info('master_public_dns')

    def _master_private_ip(self):
        """Get the internal ("private") address of the master node, so we
        can direct our SSH tunnel to it."""
        return self._get_cluster_info('master_private_ip')

    def _get_app_versions(self):
        """Returns a map from lowercase app name to version for our cluster.

        This only works for non-'hadoop' apps on 4.x AMIs and later.
        """
        return self._get_cluster_info('app_versions')

    def _get_cluster_info(self, key):
        if not self._cluster_id:
            raise AssertionError('cluster has not yet been created')
        cache = self._cluster_to_cache[self._cluster_id]

        if not cache.get(key):
            if key == 'master_private_ip':
                self._store_master_instance_info()
            else:
                self._store_cluster_info()

        return cache.get(key)

    def _store_cluster_info(self):
        """Describe our cluster, and cache image_version, hadoop_version,
        and master_public_dns"""
        if not self._cluster_id:
            raise AssertionError('cluster has not yet been created')

        cache = self._cluster_to_cache[self._cluster_id]

        cluster = self._describe_cluster()

        # AMI version might be in RunningAMIVersion (2.x, 3.x)
        # or ReleaseLabel (4.x)
        cache['image_version'] = getattr(cluster, 'runningamiversion', None)
        if not cache['image_version']:
            release_label = getattr(cluster, 'releaselabel', None)
            if release_label:
                cache['image_version'] = release_label.lstrip('emr-')

        cache['app_versions'] = dict(
            (a.name.lower(), a.version) for a in cluster.applications)

        if cluster.status.state in ('RUNNING', 'WAITING'):
            cache['master_public_dns'] = cluster.masterpublicdnsname

    def _store_master_instance_info(self):
        """List master instance for our cluster, and cache
        master_private_ip."""
        if not self._cluster_id:
            raise AssertionError('cluster has not yet been created')

        cache = self._cluster_to_cache[self._cluster_id]

        emr_conn = self.make_emr_conn()

        instances = emr_conn.list_instances(
            self._cluster_id, instance_group_types=['MASTER']).instances

        if not instances:
            return

        master = instances[0]

        # can also get private DNS and public IP/DNS, but we don't use this
        if hasattr(master, 'privateipaddress'):
            cache['master_private_ip'] = master.privateipaddress

    def make_iam_conn(self):
        """Create a connection to S3.

        :return: a :py:class:`boto.iam.connection.IAMConnection`, wrapped in a
                 :py:class:`mrjob.retry.RetryWrapper`
        """
        # give a non-cryptic error message if boto isn't installed
        if boto is None:
            raise ImportError('You must install boto to connect to IAM')

        host = self._opts['iam_endpoint'] or 'iam.amazonaws.com'

        log.debug('creating IAM connection to %s' % host)

        raw_iam_conn = boto.connect_iam(
            aws_access_key_id=self._opts['aws_access_key_id'],
            aws_secret_access_key=self._opts['aws_secret_access_key'],
            host=host,
            security_token=self._opts['aws_session_token'])

        return wrap_aws_conn(raw_iam_conn)

    # Spark

    def _uses_spark(self):
        """Does this runner use Spark, based on steps, bootstrap actions,
        and EMR applications? If so, we'll need more memory."""
        return (self._has_spark_steps() or
                self._has_spark_install_bootstrap_action() or
                self._has_spark_application() or
                self._opts['bootstrap_spark'])

    def _has_spark_install_bootstrap_action(self):
        """Does it look like this runner has a spark bootstrap install
        action set? (Anything ending in "/install-spark" counts.)"""
        return any(ba['path'].endswith('/install-spark')
                   for ba in self._bootstrap_actions(add_spark=False))

    def _has_spark_application(self):
        """Does this runner have "Spark" in its *applications* option?"""
        return any(a.lower() == 'spark'
                   for a in self._applications(add_spark=False))

    def _check_cluster_spark_support(self):
        """Issue a warning if our cluster doesn't support Spark.

        This should only be called if you are going to run one or more
        Spark steps.
        """
        message = self._cluster_spark_support_warning()
        if message:
            log.warning(message)

    def _cluster_spark_support_warning(self):
        """Helper for _check_cluster_spark_support()."""
        image_version = self.get_image_version()

        if not version_gte(image_version, _MIN_SPARK_AMI_VERSION):
            suggested_version = (
                _MIN_SPARK_AMI_VERSION if PY2 else _MIN_SPARK_PY3_AMI_VERSION)
            return ('  AMI version %s does not support Spark;\n'
                    '  (try --image-version %s or later)' % (
                        image_version, suggested_version))

        if not version_gte(image_version, _MIN_SPARK_PY3_AMI_VERSION):
            if PY2:
                # even though this version of Spark "works" with Python 2,
                # it doesn't work well
                return ('  AMI version %s has an old version of Spark\n'
                        ' and does not correctly determine when a Spark'
                        ' job has failed\n'
                        'Try --image-version %s or later)' % (
                            image_version, _MIN_SPARK_PY3_AMI_VERSION))
            else:
                # this version of Spark doesn't support Python 3 at all!
                return ('  AMI version %s does not support Python 3 on Spark\n'
                        '  (try --image-version %s or later)' % (
                            image_version, _MIN_SPARK_PY3_AMI_VERSION))

        # make sure there's enough memory to run Spark
        try:
            instance_groups = list(_yield_all_instance_groups(
                self.make_emr_conn(), self.get_cluster_id()))
        except boto.exception.EmrResponseError:
            # don't error if we encounter an instance fleet (see #1639)
            return None

        for ig in instance_groups:
            # master doesn't matter if it's not running tasks
            if ig.instancegrouptype.lower() == 'master' and (
                    len(instance_groups) > 1):
                continue

            mem = EC2_INSTANCE_TYPE_TO_MEMORY.get(ig.instancetype)
            if mem and mem < _MIN_SPARK_INSTANCE_MEMORY:
                return ('  instance type %s is too small for Spark;'
                        ' your job may stall forever' % ig.instancetype)

        return None


def _encode_emr_api_params(x):
    """Recursively unpack parameters to the EMR API."""
    # recursively unpack values, and flatten into main dict
    if isinstance(x, dict):
        result = {}

        for key, value in x.items():
            # special case for Properties dicts, which have to be
            # represented as KeyValue objects
            if key == 'Properties' and isinstance(value, dict):
                value = [{'Key': k, 'Value': v}
                         for k, v in sorted(value.items())]

            unpacked_value = _encode_emr_api_params(value)
            if isinstance(unpacked_value, dict):
                for subkey, subvalue in unpacked_value.items():
                    result['%s.%s' % (key, subkey)] = subvalue
            else:
                result[key] = unpacked_value

        return result

    # treat lists like dicts mapping "member.N" (1-indexed) to value
    if isinstance(x, (list, tuple)):
        return _encode_emr_api_params(dict(
            ('member.%d' % (i + 1), item)
            for i, item in enumerate(x)))

    # base case, not a dict or list
    return x


def _fix_configuration_opt(c):
    """Return copy of *c* with *Properties* is always set
    (defaults to {}) and with *Configurations* is not set if empty.
    Convert all values to strings.

    Raise exception on more serious problems (extra fields, wrong data
    type, etc).

    This allows us to use :py:func:`_decode_configurations_from_api`
    to match configurations against the API, *and* catches bad configurations
    before they result in cryptic API errors.
    """
    if not isinstance(c, dict):
        raise TypeError('configurations must be dicts, not %r' % (c,))

    c = dict(c)  # make a copy

    # extra keys
    extra_keys = (
        set(c) - set(['Classification', 'Configurations', 'Properties']))
    if extra_keys:
        raise ValueError('configuration opt has extra keys: %s' % ', '.join(
            sorted(extra_keys)))

    # Classification
    if 'Classification' not in c:
        raise ValueError('configuration opt has no Classification')

    if not isinstance(c['Classification'], string_types):
        raise TypeError('Classification must be string')

    # Properties
    c.setdefault('Properties', {})
    if not isinstance(c['Properties'], dict):
        raise TypeError('Properties must be a dict')

    c['Properties'] = dict(
        (str(k), str(v)) for k, v in c['Properties'].items())

    # sub-Configurations
    if 'Configurations' in c:
        if c['Configurations']:
            if not isinstance(c['Configurations'], list):
                raise TypeError('Configurations must be a list')
            # recursively fix subconfigurations
            c['Configurations'] = [
                _fix_configuration_opt(sc) for sc in c['Configurations']]
        else:
            # don't keep empty configurations around
            del c['Configurations']

    return c


def _decode_configurations_from_api(configurations):
    """Recursively convert configurations object from describe_cluster()
    back into simple data structure."""
    results = []

    for c in configurations:
        result = {}

        if hasattr(c, 'classification'):
            result['Classification'] = c.classification

        # don't decode empty configurations (which API shouldn't return)
        if getattr(c, 'configurations', None):
            result['Configurations'] = _decode_configurations_from_api(
                c.configurations)

        # Properties should always be set (API should do this anyway)
        result['Properties'] = dict(
            (kv.key, kv.value) for kv in getattr(c, 'properties', []))

        results.append(result)

    return results
