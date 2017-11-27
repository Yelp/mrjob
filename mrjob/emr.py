# -*- coding: utf-8 -*-
# Copyright 2009-2017 Yelp and Contributors
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
from subprocess import Popen
from subprocess import PIPE

try:
    import botocore.client
    botocore  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    botocore = None

try:
    import boto3
    import boto3.s3.transfer
    boto3  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    # don't require boto3; MRJobs don't actually need it when running
    # inside hadoop streaming
    boto3 = None


import mrjob
import mrjob.step
from mrjob.aws import _DEFAULT_AWS_REGION
from mrjob.aws import EC2_INSTANCE_TYPE_TO_COMPUTE_UNITS
from mrjob.aws import EC2_INSTANCE_TYPE_TO_MEMORY
from mrjob.aws import _boto3_now
from mrjob.aws import _boto3_paginate
from mrjob.cloud import HadoopInTheCloudJobRunner
from mrjob.compat import map_version
from mrjob.compat import version_gte
from mrjob.conf import combine_dicts
from mrjob.fs.composite import CompositeFilesystem
from mrjob.fs.local import LocalFilesystem
from mrjob.fs.s3 import S3Filesystem
from mrjob.fs.s3 import _client_error_status
from mrjob.fs.s3 import _endpoint_url
from mrjob.fs.s3 import _get_bucket_region
from mrjob.fs.s3 import _wrap_aws_client
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
from mrjob.parse import is_s3_uri
from mrjob.parse import _parse_progress_from_job_tracker
from mrjob.parse import _parse_progress_from_resource_manager
from mrjob.pool import _instance_fleets_satisfy
from mrjob.pool import _instance_groups_satisfy
from mrjob.pool import _pool_hash_and_name
from mrjob.py2 import PY2
from mrjob.py2 import string_types
from mrjob.py2 import urlopen
from mrjob.py2 import xrange
from mrjob.setup import UploadDirManager
from mrjob.setup import WorkingDirManager
from mrjob.step import StepFailedException
from mrjob.step import _is_spark_step_type
from mrjob.util import cmd_line
from mrjob.util import shlex_split
from mrjob.util import strip_microseconds
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
_MAX_MINS_IDLE_BOOTSTRAP_ACTION_PATH = os.path.join(
    os.path.dirname(mrjob.__file__),
    'bootstrap',
    'terminate_idle_cluster.sh')

# default AWS region to use for EMR. Using us-west-2 because it is the default
# for new (since October 10, 2012) accounts (see #1025)
_DEFAULT_EMR_REGION = 'us-west-2'

# default AMI to use on EMR. This will be updated with each version
_DEFAULT_IMAGE_VERSION = '5.8.0'

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
_INSTANCE_ROLES = ('MASTER', 'CORE', 'TASK')

# use to disable multipart uploading
_HUGE_PART_THRESHOLD = 2 ** 256


# used to bail out and retry when a pooled cluster self-terminates
class _PooledClusterSelfTerminatedException(Exception):
    pass

# mildly flexible regex to detect cluster self-termination. Termination of
# non-master nodes won't shut down the cluster, so don't need to match that.
_CLUSTER_SELF_TERMINATED_RE = re.compile(
    '^.*The master node was terminated.*$', re.I)


def _make_lock_uri(cloud_tmp_dir, cluster_id, step_num):
    """Generate the URI to lock the cluster ``cluster_id``"""
    return cloud_tmp_dir + 'locks/' + cluster_id + '/' + str(step_num)


def _attempt_to_acquire_lock(s3_fs, lock_uri, sync_wait_time, job_key,
                             mins_to_expiration=None):
    """Returns True if this session successfully took ownership of the lock
    specified by ``lock_uri``.
    """
    s3_key = s3_fs._get_s3_key(lock_uri)

    # check if the lock already exists
    try:
        key_data = s3_key.get()
    except botocore.exceptions.ClientError as ex:
        if _client_error_status(ex) != 404:
            raise
        key_data = None

    # if there's an unexpired lock, give up
    if key_data:
        if mins_to_expiration is None:
            return False
        else:
            # dateutil is a boto3 dependency
            age = _boto3_now() - key_data['LastModified']
            if age <= timedelta(minutes=mins_to_expiration):
                return False

    # try to write our job's key
    s3_key.put(Body=job_key.encode('utf_8'))

    # wait for S3
    time.sleep(sync_wait_time)

    # make sure it's still our key there, not someone else's
    key_value = s3_key.get()['Body'].read()
    return (key_value == job_key.encode('utf_8'))


class EMRJobRunner(HadoopInTheCloudJobRunner, LogInterpretationMixin):
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

        emr_client = EMRJobRunner().make_emr_client()
        clusters = emr_client.list_clusters()
        ...
    """
    alias = 'emr'

    OPT_NAMES = HadoopInTheCloudJobRunner.OPT_NAMES | {
        'additional_emr_info',
        'applications',
        'aws_access_key_id',
        'aws_secret_access_key',
        'aws_session_token',
        'bootstrap_actions',
        'bootstrap_spark',
        'cloud_log_dir',
        'cloud_upload_part_size',
        'core_instance_bid_price',
        'ec2_key_pair',
        'ec2_key_pair_file',
        'emr_action_on_failure',
        'emr_api_params',
        'emr_configurations',
        'emr_endpoint',
        'enable_emr_debugging',
        'hadoop_extra_args',
        'hadoop_streaming_jar',
        'iam_endpoint',
        'iam_instance_profile',
        'iam_service_role',
        'instance_fleets',
        'instance_groups',
        'master_instance_bid_price',
        'mins_to_end_of_hour',
        'pool_clusters',
        'pool_name',
        'pool_wait_minutes',
        'release_label',
        's3_endpoint',
        'ssh_bin',
        'ssh_bind_ports',
        'ssh_tunnel',
        'ssh_tunnel_is_open',
        'subnet',
        'tags',
        'task_instance_bid_price',
        'visible_to_all_users',
    }

    # everything that controls instances number, type, or price
    _INSTANCE_OPT_NAMES = {
        name for name in OPT_NAMES
        if 'instance' in name and 'iam' not in name
    }

    def __init__(self, **kwargs):
        """:py:class:`~mrjob.emr.EMRJobRunner` takes the same arguments as
        :py:class:`~mrjob.runner.MRJobRunner`, plus some additional options
        which can be defaulted in :ref:`mrjob.conf <mrjob.conf>`.

        *aws_access_key_id* and *aws_secret_access_key* are required if you
        haven't set them up already for boto3 (e.g. by setting the environment
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
        elif not version_gte(self._opts['image_version'], '2.4.3'):
            log.warning("AMIs prior to 2.4.3 probably will not work because"
                        " they don't support Python 2.7. Use a later AMI"
                        " version or mrjob v0.5.11")

        if self._opts['emr_api_params'] is not None:
            log.warning('emr_api_params is deprecated and does nothing.'
                        ' Please use extra_cluster_params instead')

        # manage local files that we want to upload to S3. We'll add them
        # to this manager just before we need them.
        s3_files_dir = self._cloud_tmp_dir + 'files/'
        self._upload_mgr = UploadDirManager(s3_files_dir)

        # master node setup script (handled later by
        # _add_master_node_setup_files_for_upload())
        self._master_node_setup_mgr = WorkingDirManager()
        self._master_node_setup_script_path = None

        # where our own logs ended up (we'll find this out once we run the job)
        self._s3_log_dir_uri = None

        # did we create the cluster we're running on?
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

    ### Options ###

    def _default_opts(self):
        return combine_dicts(
            super(EMRJobRunner, self)._default_opts(),
            dict(
                bootstrap_python=None,
                check_cluster_every=30,
                cleanup_on_failure=['JOB'],
                cloud_fs_sync_secs=5.0,
                cloud_upload_part_size=100,  # 100 MB
                image_version=_DEFAULT_IMAGE_VERSION,
                num_core_instances=0,
                num_task_instances=0,
                pool_clusters=False,
                pool_name='default',
                pool_wait_minutes=0,
                region=_DEFAULT_EMR_REGION,
                sh_bin=None,  # see _sh_bin(), below
                ssh_bin=['ssh'],
                # don't use a list because it makes it hard to read option
                # values when running in verbose mode. See #1284
                ssh_bind_ports=xrange(40001, 40841),
                ssh_tunnel=False,
                ssh_tunnel_is_open=False,
                visible_to_all_users=True,
            )
        )

    def _combine_opts(self, opt_list):
        """Blank out overriden *instance_fleets* and *instance_groups*

        Convert image_version of 4.x and later to release_label."""
        # copy opt_list so we can modify it
        opt_list = [dict(opts) for opts in opt_list]

        # blank out any instance_fleets/groups before the last config
        # where they are set
        blank_out = False
        for opts in reversed(opt_list):
            if blank_out:
                opts['instance_fleets'] = None
                opts['instance_groups'] = None
            elif any(opts.get(k) is not None
                     for k in self._INSTANCE_OPT_NAMES):
                blank_out = True

        # now combine opts, with instance_groups/fleets blanked out
        opts = super(EMRJobRunner, self)._combine_opts(opt_list)

        # set release_label based on image_version
        if (version_gte(opts['image_version'], '4') and
                not opts['release_label']):
            opts['release_label'] = 'emr-' + opts['image_version']

        return opts

    def _fix_opt(self, opt_key, opt_value, source):
        """Fix and check various EMR-specific options"""
        opt_value = super(EMRJobRunner, self)._fix_opt(
            opt_key, opt_value, source)

        # *_instance_bid_price
        if opt_key.endswith('_instance_bid_price'):
            if not opt_value:  # don't allow blank bid price
                return None

            try:
                if not float(opt_value):
                    return None
            except ValueError:  # maybe EMR allows non-floats?
                pass

            return str(opt_value)  # should be str, not a number

        # additional_emr_info
        elif opt_key == 'additional_emr_info' and not isinstance(
                opt_value, string_types):
            return json.dumps(opt_value)

        # emr_configurations
        elif opt_key == 'emr_configurations':
            return [_fix_configuration_opt(c) for c in opt_value]

        # region
        elif opt_key == 'region':
            # don't allow blank region
            return opt_value or _DEFAULT_EMR_REGION

        # subnet should be None, a string, or a multi-item list
        elif opt_key == 'subnet':
            return _fix_subnet_opt(opt_value)

        else:
            return opt_value

    def _obfuscate_opt(self, opt_key, opt_value):
        """Obfuscate AWS credentials."""
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
        client = self.fs.make_s3_client()

        for bucket_name in self.fs.get_all_bucket_names():
            if not bucket_name.startswith('mrjob-'):
                continue

            bucket_region = _get_bucket_region(client, bucket_name)
            if bucket_region == self._opts['region']:
                # Regions are both specified and match
                log.debug("using existing temp bucket %s" % bucket_name)
                self._opts['cloud_tmp_dir'] = 's3://%s/tmp/' % bucket_name
                return

        # That may have all failed. If so, pick a name.
        bucket_name = 'mrjob-' + random_identifier()
        self._s3_tmp_bucket_to_create = bucket_name
        self._opts['cloud_tmp_dir'] = 's3://%s/tmp/' % bucket_name
        log.info('Auto-created temp S3 bucket %s' % bucket_name)
        self._wait_for_s3_eventual_consistency()

    def _s3_log_dir(self):
        """Get the URI of the log directory for this job's cluster."""
        if not self._s3_log_dir_uri:
            cluster = self._describe_cluster()
            log_uri = cluster.get('LogUri')
            if log_uri:
                self._s3_log_dir_uri = '%s%s/' % (
                    log_uri.replace('s3n://', 's3://'), self._cluster_id)

        return self._s3_log_dir_uri

    def _create_s3_tmp_bucket_if_needed(self):
        """Make sure temp bucket exists"""
        if self._s3_tmp_bucket_to_create:
            log.debug('creating S3 bucket %r to use as temp space' %
                      self._s3_tmp_bucket_to_create)
            self.fs.create_bucket(self._s3_tmp_bucket_to_create,
                                  self._opts['region'])
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
                aws_session_token=self._opts['aws_session_token'],
                s3_endpoint=self._opts['s3_endpoint'],
                s3_region=self._opts['region'])

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

    def _check_output_not_exists(self):
        """Verify the output path does not already exist. This avoids
        provisioning a cluster only to have Hadoop refuse to launch.
        """
        try:
            if self.fs.exists(self._output_dir):
                raise IOError(
                    'Output path %s already exists!' % (self._output_dir,))
        except botocore.exceptions.ClientError:
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

        # Add max-mins-idle script if we need it
        if persistent or self._opts['pool_clusters']:
            self._upload_mgr.add(_MAX_MINS_IDLE_BOOTSTRAP_ACTION_PATH)

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
        s3_key = self.fs._get_s3_key(s3_uri)

        # use _HUGE_PART_THRESHOLD to disable multipart uploading
        # (could use put() directly, but that would be another code path)
        part_size = self._get_upload_part_size() or _HUGE_PART_THRESHOLD

        s3_key.upload_file(
            path,
            Config=boto3.s3.transfer.TransferConfig(
                multipart_chunksize=part_size,
                multipart_threshold=part_size,
            ),
        )

    def _get_upload_part_size(self):
        # part size is in MB, as the minimum is 5 MB
        return int((self._opts['cloud_upload_part_size'] or 0) * 1024 * 1024)

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
            # this could be refactored to use SSHFilesystem._ssh_launch(),
            # but that would probably just make this code less readable
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
                self.make_emr_client().terminate_job_flows(
                    JobFlowIds=[self._cluster_id]
                )
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

        emr_client = self.make_emr_client()
        try:
            log.info("Attempting to terminate cluster")
            emr_client.terminate_job_flows(
                JobFlowIds=[self._cluster_id]
            )
        except Exception as e:
            # Something happened with boto3 and the user should know.
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
                 cluster['Id'])

        if (cluster['Status']['State'] == 'WAITING' and
                cluster['AutoTerminate']):
            raise Exception('Operation requires cluster to terminate, but'
                            ' it may never do so.')

        while True:
            log.info('  %s' % cluster['Status']['State'])

            if cluster['Status']['State'] in (
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
        (one of 'MASTER', 'CORE', 'TASK')"""
        if role not in _INSTANCE_ROLES:
            raise ValueError

        # explicitly set
        if self._opts[role.lower() + '_instance_type']:
            return self._opts[role.lower() + '_instance_type']

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

        return (role != 'MASTER' or
                sum(self._num_instances(role)
                    for role in _INSTANCE_ROLES) == 1)

    def _num_instances(self, role):
        """How many of the given instance type do we want?"""
        if role not in _INSTANCE_ROLES:
            raise ValueError

        if role == 'MASTER':
            return 1  # there can be only one
        else:
            return self._opts['num_' + role.lower() + '_instances']

    def _instance_bid_price(self, role):
        """What's the bid price for the given role (if any)?"""
        if role not in _INSTANCE_ROLES:
            raise ValueError

        return self._opts[role.lower() + '_instance_bid_price']

    def _instance_groups(self):
        """Which instance groups do we want to request?

        Returns the value of the ``InstanceGroups`` parameter
        passed to the EMR API.
        """
        if self._opts['instance_groups']:
            return self._opts['instance_groups']

        return [
            _build_instance_group(
                role=role,
                instance_type=self._instance_type(role),
                num_instances=self._num_instances(role),
                bid_price=self._instance_bid_price(role),
            )
            for role in _INSTANCE_ROLES
            if self._num_instances(role)
        ]

    def _create_cluster(self, persistent=False):
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
        emr_client = self.make_emr_client()

        kwargs = self._cluster_kwargs(persistent)
        log.debug('Calling run_job_flow(%s)' % (
            ', '.join('%s=%r' % (k, v)
                      for k, v in sorted(kwargs.items()))))
        cluster_id = emr_client.run_job_flow(**kwargs)['JobFlowId']

         # keep track of when we started our job
        self._emr_job_start = time.time()

        log.info('Created new cluster %s' % cluster_id)

        # set EMR tags for the cluster
        tags = dict(self._opts['tags'])

        # patch in version
        tags['__mrjob_version'] = mrjob.__version__

        # add pooling tags
        if self._opts['pool_clusters']:
            tags['__mrjob_pool_hash'] = self._pool_hash()
            tags['__mrjob_pool_name'] = self._opts['pool_name']

        self._add_tags(tags, cluster_id)

        return cluster_id

    def _add_tags(self, tags, cluster_id):
        """Add tags in the dict *tags* to cluster *cluster_id*. Do nothing
        if *tags* is empty or ``None``"""
        if not tags:
            return

        tags_items = sorted(tags.items())

        self.make_emr_client().add_tags(
            ResourceId=cluster_id,
            Tags=[dict(Key=k, Value=v) for k, v in tags_items])

        log.info('Added EMR tags to cluster %s: %s' % (
            cluster_id,
            ', '.join('%s=%s' % (tag, value) for tag, value in tags_items)))

    # TODO: could break this into sub-methods for clarity
    def _cluster_kwargs(self, persistent=False):
        """Build kwargs for emr_client.run_job_flow()"""
        kwargs = {}

        kwargs['Name'] = self._job_key

        kwargs['LogUri'] = self._opts['cloud_log_dir']

        if self._opts['release_label']:
            kwargs['ReleaseLabel'] = self._opts['release_label']
        else:
            kwargs['AmiVersion'] = self._opts['image_version']

        # capitalizing Instances because it's just an API parameter
        kwargs['Instances'] = Instances = {}

        if self._opts['zone']:
            Instances['Placement'] = dict(AvailabilityZone=self._opts['zone'])

        if self._opts['instance_fleets']:
            Instances['InstanceFleets'] = self._opts['instance_fleets']
        else:
            Instances['InstanceGroups'] = self._instance_groups()

        # bootstrap actions
        kwargs['BootstrapActions'] = BootstrapActions = []

        for i, bootstrap_action in enumerate(self._bootstrap_actions()):
            uri = self._upload_mgr.uri(bootstrap_action['path'])
            BootstrapActions.append(dict(
                Name=('action %d' % i),
                ScriptBootstrapAction=dict(
                    Path=uri,
                    Args=bootstrap_action['args'])))

        if self._master_bootstrap_script_path:
            uri = self._upload_mgr.uri(self._master_bootstrap_script_path)

            BootstrapActions.append(dict(
                Name='master',
                ScriptBootstrapAction=dict(
                    Path=uri,
                    Args=[])))

        if persistent or self._opts['pool_clusters']:
            Instances['KeepJobFlowAliveWhenNoSteps'] = True

            # use idle termination script on persistent clusters
            # add it last, so that we don't count bootstrapping as idle time
            uri = self._upload_mgr.uri(
                _MAX_MINS_IDLE_BOOTSTRAP_ACTION_PATH)

            # script takes args in (integer) seconds
            ba_args = [str(int(self._opts['max_mins_idle'] * 60))]
            BootstrapActions.append(dict(
                Name='idle timeout',
                ScriptBootstrapAction=dict(
                    Path=uri,
                    Args=ba_args)))

        if self._opts['ec2_key_pair']:
            Instances['Ec2KeyName'] = self._opts['ec2_key_pair']

        kwargs['Steps'] = Steps = []

        if self._opts['enable_emr_debugging']:
            # other steps are added separately
            Steps.append(self._build_debugging_step())

        if self._opts['additional_emr_info']:
            kwargs['AdditionalInfo'] = self._opts['additional_emr_info']

        kwargs['VisibleToAllUsers'] = bool(
            self._opts['visible_to_all_users'])

        kwargs['JobFlowRole'] = self._instance_profile()
        kwargs['ServiceRole'] = self._service_role()

        applications = self._applications()
        if applications:
            kwargs['Applications'] = [
                dict(Name=a) for a in sorted(applications)]

        if self._opts['emr_configurations']:
            kwargs['Configurations'] = self._opts['emr_configurations']

        if self._opts['subnet']:
            # handle lists of subnets (for instance fleets)
            if isinstance(self._opts['subnet'], list):
                Instances['Ec2SubnetIds'] = self._opts['subnet']
            else:
                Instances['Ec2SubnetId'] = self._opts['subnet']

        return self._add_extra_cluster_params(kwargs)

    def _instance_profile(self):
        try:
            return (self._opts['iam_instance_profile'] or
                    get_or_create_mrjob_instance_profile(
                        self.make_iam_client()))
        except botocore.exceptions.ClientError as ex:
            if _client_error_status(ex) != 403:
                raise
            log.warning(
                "Can't access IAM API, trying default instance profile: %s" %
                _FALLBACK_INSTANCE_PROFILE)
            return _FALLBACK_INSTANCE_PROFILE

    def _service_role(self):
        try:
            return (self._opts['iam_service_role'] or
                    get_or_create_mrjob_service_role(self.make_iam_client()))
        except botocore.exceptions.ClientError as ex:
            if _client_error_status(ex) != 403:
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
        """Return a step data structures to pass to ``boto3``"""
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
            method = self._streaming_step_hadoop_jar_step
        elif step['type'] == 'jar':
            method = self._jar_step_hadoop_jar_step
        elif _is_spark_step_type(step['type']):
            method = self._spark_step_hadoop_jar_step
        else:
            raise AssertionError('Bad step type: %r' % (step['type'],))

        hadoop_jar_step = method(step_num)

        return dict(
            ActionOnFailure=self._action_on_failure(),
            HadoopJarStep=hadoop_jar_step,
            Name=self._step_name(step_num),
        )

    def _streaming_step_hadoop_jar_step(self, step_num):
        jar, step_arg_prefix = self._get_streaming_jar_and_step_arg_prefix()

        args = (step_arg_prefix +
                self._hadoop_streaming_jar_args(step_num))

        return dict(Jar=jar, Args=args)

    def _jar_step_hadoop_jar_step(self, step_num):
        step = self._get_step(step_num)

        jar = self._upload_uri_or_remote_path(step['jar'])

        args = (
            # -libjars, -D comes before jar-specific args
            self._hadoop_generic_args_for_step(step_num) +
            self._interpolate_input_and_output(step['args'], step_num))

        hadoop_jar_step = dict(Jar=jar, Args=args)

        if step.get('main_class'):
            hadoop_jar_step['MainClass'] = step['main_class']

        return hadoop_jar_step

    def _spark_step_hadoop_jar_step(self, step_num):
        return dict(
            Jar=self._spark_jar(),
            Args=self._args_for_spark_step(step_num))

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

        return dict(
            Name=name,
            ActionOnFailure=self._action_on_failure(),
            HadoopJarStep=dict(
                Jar=jar,
                Args=step_args,
            )
        )

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

    def _launch_emr_job(self):
        """Create an empty cluster on EMR, and set self._cluster_id to
        its ID.
        """
        self._create_s3_tmp_bucket_if_needed()
        emr_client = self.make_emr_client()

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
        else:
            log.info('Adding our job to existing cluster %s' %
                     self._cluster_id)

        # now that we know which cluster it is, check for Spark support
        if self._has_spark_steps():
            self._check_cluster_spark_support()

        # define our steps
        steps = self._build_steps()
        steps_kwargs = dict(JobFlowId=self._cluster_id, Steps=steps)
        log.debug('Calling add_job_flow_steps(%s)' % ','.join(
            ('%s=%r' % (k, v)) for k, v in steps_kwargs.items()))
        emr_client.add_job_flow_steps(**steps_kwargs)

        # keep track of when we launched our job
        self._emr_job_start = time.time()

        # SSH FS uses sudo if we're on AMI 4.3.0+ (see #1244)
        if self._ssh_fs and version_gte(self.get_image_version(), '4.3.0'):
            self._ssh_fs.use_sudo_over_ssh()

    def get_job_steps(self):
        """Fetch the steps submitted by this runner from the EMR API.

        .. versionadded:: 0.6.1
        """
        return _get_job_steps(
            self.make_emr_client(), self.get_cluster_id(), self.get_job_key())

    def _wait_for_steps_to_complete(self):
        """Wait for every step of the job to complete, one by one."""
        # get info about expected number of steps
        num_steps = len(self._get_steps())

        expected_num_steps = num_steps
        if self._master_node_setup_script_path:
            expected_num_steps += 1

        # get info about steps submitted to cluster
        steps = self.get_job_steps()

        if len(steps) < expected_num_steps:
            log.warning('Expected to find %d steps on cluster, found %d' %
                        (expected_num_steps, len(steps)))

        # clear out log interpretations if they were filled somehow
        self._log_interpretations = []
        self._mns_log_interpretation = None

        # open SSH tunnel if cluster is already ready
        # (this happens with pooling). See #1115
        cluster = self._describe_cluster()
        if cluster['Status']['State'] in ('RUNNING', 'WAITING'):
            self._set_up_ssh_tunnel()

        # treat master node setup as step -1
        start = 0
        if self._master_node_setup_script_path:
            start -= 1

        for step_num, step in enumerate(steps, start=start):
            step_id = step['Id']
            # don't include job_key in logging messages
            step_name = step['Name'].split(': ')[-1]

            log.info('Waiting for %s (%s) to complete...' %
                     (step_name, step_id))

            self._wait_for_step_to_complete(step_id, step_num, num_steps)

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

        emr_client = self.make_emr_client()

        while True:
            # don't antagonize EMR's throttling
            log.debug('Waiting %.1f seconds...' %
                      self._opts['check_cluster_every'])
            time.sleep(self._opts['check_cluster_every'])

            step = emr_client.describe_step(
                ClusterId=self._cluster_id, StepId=step_id)['Step']

            if step['Status']['State'] == 'PENDING':
                cluster = self._describe_cluster()

                reason = _get_reason(cluster)
                reason_desc = (': %s' % reason) if reason else ''

                # we can open the ssh tunnel if cluster is ready (see #1115)
                if cluster['Status']['State'] in ('RUNNING', 'WAITING'):
                    self._set_up_ssh_tunnel()

                log.info('  PENDING (cluster is %s%s)' % (
                    cluster['Status']['State'], reason_desc))
                continue

            elif step['Status']['State'] == 'RUNNING':
                time_running_desc = ''

                start = step['Status']['Timeline'].get('StartDateTime')
                if start:
                    time_running_desc = ' for %s' % strip_microseconds(
                        _boto3_now() - start)

                # now is the time to tunnel, if we haven't already
                self._set_up_ssh_tunnel()
                log.info('  RUNNING%s' % time_running_desc)

                # don't log progress for master node setup step, because
                # it doesn't appear in job tracker
                if step_num >= 0:
                    self._log_step_progress()

                continue

            # we're done, will return at the end of this
            elif step['Status']['State'] == 'COMPLETED':
                log.info('  COMPLETED')
                # will fetch counters, below, and then return
            else:
                # step has failed somehow. *reason* seems to only be set
                # when job is cancelled (e.g. 'Job terminated')
                reason = _get_reason(step)
                reason_desc = (' (%s)' % reason) if reason else ''

                log.info('  %s%s' % (
                    step['Status']['State'], reason_desc))

                # print cluster status; this might give more context
                # why step didn't succeed
                cluster = self._describe_cluster()
                reason = _get_reason(cluster)
                reason_desc = (': %s' % reason) if reason else ''
                log.info('Cluster %s %s %s%s' % (
                    cluster['Id'],
                    'was' if 'ED' in cluster['Status']['State'] else 'is',
                    cluster['Status']['State'],
                    reason_desc))

                if cluster['Status']['State'] in (
                        'TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS'):
                    # was it caused by a pooled cluster self-terminating?
                    # (if so, raise _PooledClusterSelfTerminatedException)
                    self._check_for_pooled_cluster_self_termination(
                        cluster, step)
                    # was it caused by IAM roles?
                    self._check_for_missing_default_iam_roles(cluster)
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
            if step['Status']['State'] != 'CANCELLED':
                if step_num >= 0 and not _is_spark_step_type(step_type):
                    counters = self._pick_counters(
                        log_interpretation, step_type)
                    if counters:
                        log.info(_format_counters(counters))
                    else:
                        log.warning('No counters found')

            if step['Status']['State'] == 'COMPLETED':
                return

            if step['Status']['State'] == 'FAILED':
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
            stdout, _ = self.fs._ssh_run(host, ['curl', remote_url])
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

        # don't check for max_mins_idle because it's possible to
        # join a self-terminating cluster without having max_mins_idle set
        # on this runner (pooling only cares about the master bootstrap script,
        # not other bootstrap actions)

        # our step should be CANCELLED (not failed)
        if step['Status']['State'] != 'CANCELLED':
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
        # /mnt/var/log/bootstrap-actions. However, if it's on a worker node,
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
        master node (and possibly workers, if *ssh_to_workers* is set).

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
                        log.warning('Could not get worker addresses for %s' %
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
        emr_client = self.make_emr_client()

        instances = _boto3_paginate(
            'Instances', emr_client, 'list_instances',
            ClusterId=self._cluster_id,
            InstanceGroupTypes=['CORE', 'TASK'],
            InstanceStates=['RUNNING'])

        hosts = []

        for instance in instances:
            hosts.append(instance['PrivateIpAddress'])

        return hosts

    def _wait_for_logs_on_s3(self):
        """If the cluster is already terminating, wait for it to terminate,
        so that logs will be transferred to S3.

        Don't print anything unless cluster is in the TERMINATING state.
        """
        cluster = self._describe_cluster()

        if cluster['Status']['State'] in (
                'TERMINATED', 'TERMINATED_WITH_ERRORS'):
            return  # already terminated

        if cluster['Status']['State'] != 'TERMINATING':
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

    def _cp_to_local_cmd(self):
        """Command to copy files from the cloud to the local directory."""
        if self._opts['release_label']:
            # on the 4.x AMIs, hadoop isn't yet installed, so use AWS CLI
            return 'aws s3 cp'
        else:
            # on the 2.x and 3.x AMIs, use hadoop
            return 'hadoop fs -copyToLocal'

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
        for line in self._start_of_sh_script():
            writeln(line)
        writeln()

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
            # imitate Hadoop Distributed Cache
            writeln('  chmod u+rx %s' % pipes.quote(name))

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

    def _build_debugging_step(self):
        if self._opts['release_label']:
            jar = _4_X_COMMAND_RUNNER_JAR
            args = ['state-pusher-script']
        else:
            jar = self._script_runner_jar_uri()
            args = (
                's3://%s.elasticmapreduce/libs/state-pusher/0.1/fetch' %
                self._opts['region'])

        return dict(
            Name='Setup Hadoop Debugging',
            HadoopJarStep=dict(Jar=jar, Args=args),
        )

    def _debug_script_uri(self):
        return (
            's3://%s.elasticmapreduce/libs/state-pusher/0.1/fetch' %
            self._opts['region'])

    ### EMR JOB MANAGEMENT UTILS ###

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

    def get_cluster_id(self):
        """Get the ID of the cluster our job is running on, or ``None``."""
        return self._cluster_id

    def _usable_clusters(self, exclude=None, num_steps=1):
        """Get clusters that this runner can join, returning a list of
        ``(cluster_id, num_steps)`` (number of steps is used for locking).

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
        emr_client = self.make_emr_client()
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
            log.debug('  Considering joining cluster %s...' % cluster['Id'])

            # skip if user specified a key pair and it doesn't match
            if (self._opts['ec2_key_pair'] and
                    self._opts['ec2_key_pair'] !=
                    cluster['Ec2InstanceAttributes'].get('Ec2KeyName')):
                log.debug('    ec2 key pair mismatch')
                return

            # this may be a retry due to locked clusters
            if cluster['Id'] in exclude:
                log.debug('    excluded')
                return

            # only take persistent clusters
            if cluster['AutoTerminate']:
                log.debug('    not persistent')
                return

            # match pool name, and (bootstrap) hash
            pool_hash, pool_name = _pool_hash_and_name(cluster)

            if req_hash != pool_hash:
                log.debug('    pool hash mismatch')
                return

            if self._opts['pool_name'] != pool_name:
                log.debug('    pool name mismatch')
                return

            if self._opts['release_label']:
                # just check for exact match. EMR doesn't have a concept
                # of partial release labels like it does for AMI versions.
                release_label = cluster.get('ReleaseLabel')

                if release_label != self._opts['release_label']:
                    log.debug('    release label mismatch')
                    return

                # used below
                max_steps = _4_X_MAX_STEPS
            else:
                # match actual AMI version
                image_version = cluster.get('RunningAmiVersion', '')
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
                expected_applications = set(a.lower() for a in applications)

                cluster_applications = set(
                    a['Name'].lower() for a in cluster.get('Applications', []))

                if not expected_applications <= cluster_applications:
                    log.debug('    missing applications: %s' % ', '.join(
                        sorted(expected_applications - cluster_applications)))
                    return

            emr_configurations = cluster.get('Configurations', [])
            if self._opts['emr_configurations'] != emr_configurations:
                log.debug('    emr configurations mismatch')
                return

            subnet = cluster['Ec2InstanceAttributes'].get('Ec2SubnetId')
            if isinstance(self._opts['subnet'], list):
                matches = (subnet in self._opts['subnet'])
            else:
                matches = (subnet == self._opts['subnet'])

            if not matches:
                log.debug('    subnet mismatch')
                return

            steps = list(_boto3_paginate(
                'Steps',
                emr_client, 'list_steps', ClusterId=cluster['Id']))

            # don't add more steps than EMR will allow/display through the API
            if len(steps) + num_steps > max_steps:
                log.debug('    no room for our steps')
                return

            # in rare cases, cluster can be WAITING *and* have incomplete
            # steps. We could just check for PENDING steps, but we're
            # trying to be defensive about EMR adding a new step state.
            # Not entirely sure what to make of CANCEL_PENDING
            for step in steps:
                if (step['Status']['State'] not in ('CANCELLED', 'INTERRUPTED')
                        and not step['Status'].get('Timeline', {}).get(
                            'EndDateTime')):
                    log.debug('    unfinished steps')
                    return

            collection_type = cluster.get('InstanceCollectionType',
                                          'INSTANCE_GROUP')

            instance_sort_key = None

            if self._opts['instance_fleets']:
                if collection_type != 'INSTANCE_FLEET':
                    log.debug('    does not use instance fleets')
                    return

                actual_fleets = list(_boto3_paginate(
                    'InstanceFleets', emr_client, 'list_instance_fleets',
                    ClusterId=cluster['Id']))

                req_fleets = self._opts['instance_fleets']

                instance_sort_key = _instance_fleets_satisfy(
                    actual_fleets, req_fleets)
            else:
                if collection_type != 'INSTANCE_GROUP':
                    log.debug('    does not use instance groups')
                    return

                # check memory and compute units, bailing out if we hit
                # an instance with too little memory
                actual_igs = list(_boto3_paginate(
                    'InstanceGroups', emr_client, 'list_instance_groups',
                    ClusterId=cluster['Id']))

                requested_igs = self._instance_groups()

                instance_sort_key = _instance_groups_satisfy(
                    actual_igs, requested_igs)

            if not instance_sort_key:
                return

            log.debug('    OK')
            key_cluster_steps_list.append(
                (instance_sort_key, cluster['Id'], len(steps)))

        for cluster_summary in _boto3_paginate(
                'Clusters', emr_client, 'list_clusters',
                ClusterStates=['WAITING']):

            cluster = emr_client.describe_cluster(
                ClusterId=cluster_summary['Id'])['Cluster']

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
        max_wait_time = self._opts['pool_wait_minutes']
        now = datetime.now()
        end_time = now + timedelta(minutes=max_wait_time)
        time_sleep = timedelta(seconds=_POOLING_SLEEP_INTERVAL)

        log.info('Attempting to find an available cluster...')
        while now <= end_time:
            cluster_info_list = self._usable_clusters(
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

    def make_emr_client(self):
        """Create a :py:mod:`boto3` EMR client.

        :return: a :py:class:`botocore.client.EMR` wrapped in a
                :py:class:`mrjob.retry.RetryWrapper`
        """
        # ...which is then wrapped in bacon! Mmmmm!
        if boto3 is None:
            raise ImportError('You must install boto3 to connect to EMR')

        raw_emr_client = boto3.client(
            'emr',
            aws_access_key_id=self._opts['aws_access_key_id'],
            aws_secret_access_key=self._opts['aws_secret_access_key'],
            aws_session_token=self._opts['aws_session_token'],
            endpoint_url=_endpoint_url(self._opts['emr_endpoint']),
            region_name=self._opts['region'],
        )

        return _wrap_aws_client(raw_emr_client)

    def _describe_cluster(self):
        emr_client = self.make_emr_client()
        return emr_client.describe_cluster(
            ClusterId=self._cluster_id)['Cluster']

    def get_hadoop_version(self):
        return self._get_app_versions().get('hadoop')

    def get_image_version(self):
        """Get the version of the AMI that our cluster is running, or ``None``.

        .. versionchanged:: 0.5.4

           This used to be called :py:meth:`get_ami_version`
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

    def _get_collection_type(self):
        """Return the collection type of the cluster (either
        ``'INSTANCE_FLEET'`` or ``'INSTANCE_GROUP'``)."""
        return self._get_cluster_info('collection_type')

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
        cache['image_version'] = cluster.get('RunningAmiVersion')
        if not cache['image_version']:
            release_label = cluster.get('ReleaseLabel')
            if release_label:
                cache['image_version'] = release_label.lstrip('emr-')

        cache['app_versions'] = dict(
            (a['Name'].lower(), a.get('Version'))
            for a in cluster['Applications'])

        cache['collection_type'] = cluster.get(
            'InstanceCollectionType', 'INSTANCE_GROUP')

        if cluster['Status']['State'] in ('RUNNING', 'WAITING'):
            cache['master_public_dns'] = cluster['MasterPublicDnsName']

    def _store_master_instance_info(self):
        """List master instance for our cluster, and cache
        master_private_ip."""
        if not self._cluster_id:
            raise AssertionError('cluster has not yet been created')

        cache = self._cluster_to_cache[self._cluster_id]

        emr_client = self.make_emr_client()

        instances = emr_client.list_instances(
            ClusterId=self._cluster_id,
            InstanceGroupTypes=['MASTER'])['Instances']

        if not instances:
            return

        master = instances[0]

        # can also get private DNS and public IP/DNS, but we don't use this
        master_private_ip = master.get('PrivateIpAddress')
        if master_private_ip:  # may not have been assigned yet
            cache['master_private_ip'] = master_private_ip

    def make_iam_client(self):
        """Create a :py:mod:`boto3` IAM client.

        :return: a :py:class:`botocore.client.IAM` wrapped in a
                :py:class:`mrjob.retry.RetryWrapper`
        """
        if boto3 is None:
            raise ImportError('You must install boto3 to connect to IAM')

        # special logic for setting IAM endpoint (which you don't usually
        # want to do, because IAM is regionless).
        endpoint_url = _endpoint_url(self._opts['iam_endpoint'])
        if endpoint_url:
            # keep boto3 from loading a nonsensical region name from configs
            # (see https://github.com/boto/boto3/issues/985)
            region_name = _DEFAULT_AWS_REGION
            log.debug('creating IAM client to %s' % endpoint_url)
        else:
            region_name = None
            log.debug('creating IAM client')

        raw_iam_client = boto3.client(
            'iam',
            aws_access_key_id=self._opts['aws_access_key_id'],
            aws_secret_access_key=self._opts['aws_secret_access_key'],
            aws_session_token=self._opts['aws_session_token'],
            endpoint_url=endpoint_url,
            region_name=region_name,
        )

        return _wrap_aws_client(raw_iam_client)

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

        emr_client = self.make_emr_client()

        too_small_msg = ('  instance type %s is too small for Spark;'
                         ' your job may stall forever')

        if self._get_collection_type() == 'INSTANCE_FLEET':
            fleets = list(_boto3_paginate(
                'InstanceFleets', emr_client, 'list_instance_fleets',
                ClusterId=self.get_cluster_id()))

            for fleet in fleets:
                # master doesn't matter if it's not running tasks
                if fleet['InstanceFleetType'] == 'MASTER' and len(fleets) > 1:
                    continue

                for spec in fleet['InstanceTypeSpecifications']:
                    mem = EC2_INSTANCE_TYPE_TO_MEMORY.get(spec['InstanceType'])
                    if mem and mem < _MIN_SPARK_INSTANCE_MEMORY:
                        return (too_small_msg % spec['InstanceType'])
        else:
            # instance groups
            igs = list(_boto3_paginate(
                'InstanceGroups', emr_client, 'list_instance_groups',
                ClusterId=self.get_cluster_id()))

            for ig in igs:
                # master doesn't matter if it's not running tasks
                if ig['InstanceGroupType'] == 'MASTER' and len(igs) > 1:
                    continue

                mem = EC2_INSTANCE_TYPE_TO_MEMORY.get(ig['InstanceType'])
                if mem and mem < _MIN_SPARK_INSTANCE_MEMORY:
                    return (too_small_msg % ig['InstanceType'])

        return None


def _get_job_steps(emr_client, cluster_id, job_key):
    """Efficiently fetch steps for a particular mrjob run from the EMR API.

    :param emr_client: a boto3 EMR client. See
                       :py:meth:`~mrjob.emr.EMRJobRunner.make_emr_client`
    :param cluster_id: ID of EMR cluster to fetch steps from. See
                       :py:meth:`~mrjob.emr.EMRJobRunner.get_cluster_id`
    :param job_key: Unique key for a mrjob run. See
                    :py:meth:`~mrjob.runner.MRJobRunner.get_job_key`
    """
    steps = []

    for step in _boto3_paginate('Steps', emr_client, 'list_steps',
                                ClusterId=cluster_id):
        if step['Name'].startswith(job_key):
            steps.append(step)
        elif steps:
            # all steps for job will be together, so stop
            # when we find a non-job step
            break

    return list(reversed(list(steps)))


def _get_reason(cluster_or_step):
    """Get state change reason message."""
    # StateChangeReason is {} before the first state change
    return cluster_or_step['Status']['StateChangeReason'].get('Message', '')


def _fix_configuration_opt(c):
    """Return copy of *c* with *Properties* is always set
    (defaults to {}) and with *Configurations* is not set if empty.
    Convert all values to strings.

    Raise exception on more serious problems (extra fields, wrong data
    type, etc).

    This allows us to match configurations against the API, *and* catches bad
    configurations before they result in cryptic API errors.
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


def _fix_subnet_opt(subnet):
    """Return either None, a string, or a list with at least two items."""
    if not subnet:
        return None

    if isinstance(subnet, string_types):
        return subnet

    subnet = list(subnet)
    if len(subnet) == 1:
        return subnet[0]
    else:
        return subnet


def _build_instance_group(role, instance_type, num_instances, bid_price):
    """Helper method for creating instance groups. For use when
    creating a cluster using a list of InstanceGroups

        - role is either 'MASTER', 'CORE', or 'TASK'.
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

    ig = dict(
        InstanceCount=num_instances,
        InstanceRole=role,
        InstanceType=instance_type,
        Market='ON_DEMAND',
        Name=role.lower(),  # just name the groups "core", "master", and "task"
    )

    if bid_price:
        ig['Market'] = 'SPOT'
        ig['BidPrice'] = str(bid_price)  # must be a string

    return ig
