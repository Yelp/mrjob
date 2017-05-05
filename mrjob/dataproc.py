# -*- coding: utf-8 -*-
# Copyright 2016 Google Inc. and Yelp
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
import io
import logging
import os
import os.path
import pipes
import time
import re
import subprocess

try:
    from oauth2client.client import GoogleCredentials
    from googleapiclient import discovery
    from googleapiclient import errors as google_errors
except ImportError:
    # don't require googleapiclient; MRJobs don't actually need it when running
    # inside hadoop streaming
    GoogleCredentials = None
    discovery = None
    google_errors = None

try:
    # Python 2
    import ConfigParser as configparser
except ImportError:
    # Python 3
    import configparser

import mrjob
from mrjob.compat import map_version
from mrjob.conf import combine_dicts
from mrjob.fs.composite import CompositeFilesystem
from mrjob.fs.local import LocalFilesystem
from mrjob.fs.gcs import GCSFilesystem
from mrjob.logs.counters import _pick_counters
from mrjob.fs.gcs import parse_gcs_uri
from mrjob.fs.gcs import is_gcs_uri
from mrjob.options import _allowed_keys
from mrjob.options import _combiners
from mrjob.options import _deprecated_aliases
from mrjob.parse import is_uri
from mrjob.py2 import PY2
from mrjob.runner import MRJobRunner
from mrjob.runner import RunnerOptionStore
from mrjob.setup import BootstrapWorkingDirManager
from mrjob.setup import UploadDirManager
from mrjob.setup import parse_setup_cmd
from mrjob.step import StepFailedException
from mrjob.util import cmd_line
from mrjob.util import random_identifier

log = logging.getLogger(__name__)

_DATAPROC_API_ENDPOINT = 'dataproc'
_DATAPROC_API_VERSION = 'v1'
_DATAPROC_API_REGION = 'global'
_DATAPROC_MIN_WORKERS = 2
_GCE_API_VERSION = 'v1'

_DEFAULT_INSTANCE_TYPE = 'n1-standard-1'

# default imageVersion to use on Dataproc. This may be updated with each
# version of mrjob
_DEFAULT_IMAGE_VERSION = '1.0'
_DEFAULT_CHECK_CLUSTER_EVERY = 10.0
_DEFAULT_MAX_HOURS_IDLE = 0.1
_DEFAULT_CLOUD_FS_SYNC_SECS = 5.0
_DEFAULT_CLOUD_TMP_DIR_OBJECT_TTL_DAYS = 90

# https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.clusters#GceClusterConfig  # noqa
# NOTE - added cloud-platform so we can invoke gcloud commands from the cluster master (used for auto termination script)  # noqa
_DEFAULT_GCE_SERVICE_ACCOUNT_SCOPES = [
    'https://www.googleapis.com/auth/cloud.useraccounts.readonly',
    'https://www.googleapis.com/auth/devstorage.read_write',
    'https://www.googleapis.com/auth/logging.write',
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/bigtable.admin.table',
    'https://www.googleapis.com/auth/bigtable.data',
    'https://www.googleapis.com/auth/devstorage.full_control',
    'https://www.googleapis.com/auth/cloud-platform',
]

_DATAPROC_CLUSTER_STATES_READY = frozenset(['UPDATING', 'RUNNING'])
_DATAPROC_CLUSTER_STATES_ERROR = frozenset(['ERROR', 'DELETING'])

_DATAPROC_JOB_STATES_ACTIVE = frozenset(
    ['PENDING', 'RUNNING', 'SETUP_DONE', 'CANCEL_PENDING'])

_DATAPROC_JOB_STATES_INACTIVE = frozenset(['CANCELLED', 'DONE', 'ERROR'])

# Dataproc images where Hadoop version changed (we use map_version() on this)
#
# This will need to be updated by hand if we want it to be fully accurate
# (it doesn't really matter to mrjob though, which only cares about
# major version)
#
# See https://cloud.google.com/dataproc/docs/concepts/dataproc-versions
# for the full list.
_DATAPROC_IMAGE_TO_HADOOP_VERSION = {
    '0.1': '2.7.1',
    '1.0': '2.7.2'
}

# bootstrap action which automatically terminates idle clusters
_MAX_HOURS_IDLE_BOOTSTRAP_ACTION_PATH = os.path.join(
    os.path.dirname(mrjob.__file__),
    'bootstrap',
    'terminate_idle_cluster_dataproc.sh')

_HADOOP_STREAMING_JAR_URI = (
    'file:///usr/lib/hadoop-mapreduce/hadoop-streaming.jar')

# TODO - mtai @ davidmarin - Re-implement logs parsing?  See dataproc metainfo and driver output - ask Dennis Huo for more details  # noqa
# 'gs://dataproc-801485be-0997-40e7-84a7-00926031747c-us/google-cloud-dataproc-metainfo/8b76d95e-ebdc-4b81-896d-b2c5009b3560/jobs/mr_most_used_word-taim-20160228-172000-034993---Step-2-of-2/driveroutput'  # noqa

_GCP_CLUSTER_NAME_REGEX = '(?:[a-z](?:[-a-z0-9]{0,53}[a-z0-9])?).'


########## BEGIN - Helper fxns for _cluster_create_args ##########
def _gcp_zone_uri(project, zone):
    return (
        'https://www.googleapis.com/compute/%(gce_api_version)s/projects/'
        '%(project)s/zones/%(zone)s' % dict(
            gce_api_version=_GCE_API_VERSION, project=project, zone=zone))


def _gcp_instance_group_config(
        project, zone, count, instance_type, is_preemptible=False):
    zone_uri = _gcp_zone_uri(project, zone)
    machine_uri = "%(zone_uri)s/machineTypes/%(machine_type)s" % dict(
        zone_uri=zone_uri, machine_type=instance_type)

    return dict(
        numInstances=count,
        machineTypeUri=machine_uri,
        isPreemptible=is_preemptible
    )
########## END -  Helper fxns for _cluster_create_args ###########


def _wait_for(msg, sleep_secs):
    log.info("Waiting for %s - sleeping %.1f second(s)", msg, sleep_secs)
    time.sleep(sleep_secs)


def _cleanse_gcp_job_id(job_id):
    return re.sub('[^a-zA-Z0-9_\-]', '-', job_id)


def _check_and_fix_fs_dir(gcs_uri):
    """Helper for __init__"""
    # TODO - mtai @ davidmarin - push this to fs/*.py
    if not is_gcs_uri(gcs_uri):
        raise ValueError('Invalid GCS URI: %r' % gcs_uri)
    if not gcs_uri.endswith('/'):
        gcs_uri += '/'

    return gcs_uri


def _read_gcloud_config():
    """
    Read in gcloud SDK config defaults
    """
    gcloud_output = subprocess.check_output('gcloud config list', shell=True)
    gcloud_output_as_unicode = gcloud_output.decode('utf-8')

    cloud_cfg = configparser.RawConfigParser(allow_no_value=True)
    cloud_cfg.readfp(io.StringIO(gcloud_output_as_unicode))

    return _cfg_to_dot_path_dict(cloud_cfg)


def _cfg_to_dot_path_dict(cfg_parser):
    config_dict = dict()
    for current_section in cfg_parser.sections():
        for current_option, current_value in cfg_parser.items(current_section):
            varname = '{}.{}'.format(current_section, current_option)
            config_dict[varname] = current_value

    return config_dict


class DataprocException(Exception):
    pass


class DataprocRunnerOptionStore(RunnerOptionStore):
    ALLOWED_KEYS = _allowed_keys('dataproc')
    COMBINERS = _combiners('dataproc')
    DEPRECATED_ALIASES = _deprecated_aliases('dataproc')

    DEFAULT_FALLBACKS = {
        'core_instance_type': 'instance_type',
        'task_instance_type': 'instance_type'
    }

    def __init__(self, alias, opts, conf_paths):
        super(DataprocRunnerOptionStore, self).__init__(
            alias, opts, conf_paths)

        # Dataproc requires a master and >= 2 core instances
        # num_core_instances refers ONLY to number of CORE instances and does
        # NOT include the required 1 instance for master
        # In other words, minimum cluster size is 3 machines, 1 master and 2
        # "num_core_instances" workers
        if self['num_core_instances'] < _DATAPROC_MIN_WORKERS:
            raise DataprocException(
                'Dataproc expects at LEAST %d workers' % _DATAPROC_MIN_WORKERS)

        for varname, fallback_varname in self.DEFAULT_FALLBACKS.items():
            self[varname] = self[varname] or self[fallback_varname]

        if self['core_instance_type'] != self['task_instance_type']:
            raise DataprocException(
                'Dataproc v1 expects core/task instance types to be identical')

    def default_options(self):
        super_opts = super(DataprocRunnerOptionStore, self).default_options()
        return combine_dicts(super_opts, {
            'bootstrap_python': True,
            'image_version': _DEFAULT_IMAGE_VERSION,
            'check_cluster_every': _DEFAULT_CHECK_CLUSTER_EVERY,

            'instance_type': _DEFAULT_INSTANCE_TYPE,
            'master_instance_type': _DEFAULT_INSTANCE_TYPE,

            'num_core_instances': _DATAPROC_MIN_WORKERS,
            'num_task_instances': 0,

            'cloud_fs_sync_secs': _DEFAULT_CLOUD_FS_SYNC_SECS,

            'max_hours_idle': _DEFAULT_MAX_HOURS_IDLE,
            'sh_bin': ['/bin/sh', '-ex'],

            'cleanup': ['CLUSTER', 'JOB', 'LOCAL_TMP']
        })


class DataprocJobRunner(MRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` on Google Cloud Dataproc.
    Invoked when you run your job with ``-r dataproc``.

    :py:class:`DataprocJobRunner` runs your job in an Dataproc cluster, which
    is basically a temporary Hadoop cluster.

    Input, support, and jar files can be either local or on GCS; use
    ``gs://...`` URLs to refer to files on GCS.

    This class has some useful utilities for talking directly to GCS and
    Dataproc, so you may find it useful to instantiate it without a script::

        from mrjob.dataproc import DataprocJobRunner
        ...
    """
    alias = 'dataproc'

    # Don't need to bootstrap mrjob in the setup wrapper; that's what
    # the bootstrap script is for!
    BOOTSTRAP_MRJOB_IN_SETUP = False

    OPTION_STORE_CLASS = DataprocRunnerOptionStore

    def __init__(self, **kwargs):
        """:py:class:`~mrjob.dataproc.DataprocJobRunner` takes the same
        arguments as
        :py:class:`~mrjob.runner.MRJobRunner`, plus some additional options
        which can be defaulted in :ref:`mrjob.conf <mrjob.conf>`.
        """
        super(DataprocJobRunner, self).__init__(**kwargs)

        # Lazy-load gcloud config as needed - invocations fail in PyCharm
        # debugging
        self._gcloud_config = None

        # Google Cloud Platform - project
        self._gcp_project = (
            self._opts['gcp_project'] or self.gcloud_config()['core.project'])

        # Google Compute Engine - Region / Zone
        self._gce_region = (
            self._opts['region'] or self.gcloud_config()['compute.region'])
        self._gce_zone = (
            self._opts['zone'] or self.gcloud_config()['compute.zone'])

        # cluster_id can be None here
        self._cluster_id = self._opts['cluster_id']

        self._api_client = None
        self._gcs_fs = None
        self._fs = None

        # BEGIN - setup directories
        base_tmpdir = self._get_tmpdir(self._opts['cloud_tmp_dir'])

        self._cloud_tmp_dir = _check_and_fix_fs_dir(base_tmpdir)

        # use job key to make a unique tmp dir
        self._job_tmpdir = self._cloud_tmp_dir + self._job_key + '/'

        # pick/validate output dir
        if self._output_dir:
            self._output_dir = _check_and_fix_fs_dir(self._output_dir)
        else:
            self._output_dir = self._job_tmpdir + 'output/'
        # END - setup directories

        # manage working dir for bootstrap script
        self._bootstrap_dir_mgr = BootstrapWorkingDirManager()

        # manage local files that we want to upload to GCS. We'll add them
        # to this manager just before we need them.
        fs_files_dir = self._job_tmpdir + 'files/'
        self._upload_mgr = UploadDirManager(fs_files_dir)

        self._bootstrap = self._bootstrap_python() + self._parse_bootstrap()

        for cmd in self._bootstrap:
            for maybe_path_dict in cmd:
                if isinstance(maybe_path_dict, dict):
                    self._bootstrap_dir_mgr.add(**maybe_path_dict)

        # we'll create the script later
        self._master_bootstrap_script_path = None

        # when did our particular task start?
        self._dataproc_job_start = None

        # init hadoop, ami version caches
        self._image_version = None
        self._hadoop_version = None

        # This will be filled by _run_steps()
        # NOTE - log_interpretations will be empty except job_id until we
        # parse task logs
        self._log_interpretations = []

    def gcloud_config(self):
        """Lazy load gcloud SDK configs"""
        if not self._gcloud_config:
            self._gcloud_config = _read_gcloud_config()

        return self._gcloud_config

    @property
    def api_client(self):
        if not self._api_client:
            credentials = GoogleCredentials.get_application_default()

            api_client = discovery.build(
                _DATAPROC_API_ENDPOINT, _DATAPROC_API_VERSION,
                credentials=credentials)
            self._api_client = api_client.projects().regions()

        return self._api_client

    @property
    def fs(self):
        """:py:class:`~mrjob.fs.base.Filesystem` object for SSH, S3, GCS, and
        the local filesystem.
        """
        if self._fs is not None:
            return self._fs

        self._gcs_fs = GCSFilesystem()

        self._fs = CompositeFilesystem(self._gcs_fs, LocalFilesystem())
        return self._fs

    def _get_tmpdir(self, given_tmpdir):
        """Helper for _fix_tmpdir"""
        if given_tmpdir:
            return given_tmpdir

        mrjob_buckets = self.fs.list_buckets(
            self._gcp_project, prefix='mrjob-')

        # Loop over buckets until we find one that matches region
        # NOTE - because this is a tmpdir, we look for a GCS bucket in the
        # same GCE region
        chosen_bucket_name = None
        gce_lower_location = self._gce_region.lower()
        for tmp_bucket in mrjob_buckets:
            tmp_bucket_name = tmp_bucket['name']

            # NOTE - GCP ambiguous Behavior - Bucket location is being
            # returned as UPPERCASE, ticket filed as of Apr 23, 2016 as docs
            # suggest lowercase
            lower_location = tmp_bucket['location'].lower()
            if lower_location == gce_lower_location:
                # Regions are both specified and match
                log.info("using existing temp bucket %s" % tmp_bucket_name)
                chosen_bucket_name = tmp_bucket_name
                break

        # Example default - "mrjob-us-central1-RANDOMHEX"
        if not chosen_bucket_name:
            chosen_bucket_name = '-'.join(
                ['mrjob', gce_lower_location, random_identifier()])

        return 'gs://%s/tmp/' % chosen_bucket_name

    def _run(self):
        self._launch()
        self._run_steps()

    def _launch(self):
        self._prepare_for_launch()
        self._launch_cluster()

    def _prepare_for_launch(self):
        self._check_input_exists()
        self._check_output_not_exists()
        self._create_setup_wrapper_script()
        self._add_bootstrap_files_for_upload()
        self._add_job_files_for_upload()
        self._upload_local_files_to_fs()

    def _check_input_exists(self):
        """Make sure all input exists before continuing with our job.
        """
        if not self._opts['check_input_paths']:
            return

        for path in self._input_paths:
            if path == '-':
                continue  # STDIN always exists

            if is_uri(path) and not is_gcs_uri(path):
                continue  # can't check non-GCS URIs, hope for the best

            if not self.fs.exists(path):
                raise AssertionError(
                    'Input path %s does not exist!' % (path,))

    def _check_output_not_exists(self):
        """Verify the output path does not already exist. This avoids
        provisioning a cluster only to have Hadoop refuse to launch.
        """
        if self.fs.exists(self._output_dir):
            raise IOError(
                'Output path %s already exists!' % (self._output_dir,))

    def _add_bootstrap_files_for_upload(self):
        """Add files needed by the bootstrap script to self._upload_mgr.

        Tar up mrjob if bootstrap_mrjob is True.

        Create the master bootstrap script if necessary.

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
            self._upload_mgr.add(_MAX_HOURS_IDLE_BOOTSTRAP_ACTION_PATH)

    def _add_job_files_for_upload(self):
        """Add files needed for running the job (setup and input)
        to self._upload_mgr."""
        for path in self._get_input_paths():
            self._upload_mgr.add(path)

        for path in self._working_dir_mgr.paths():
            self._upload_mgr.add(path)

        # TODO - mtai @ davidmarin - hadoop_streaming_jar is currently ignored,
        # see _HADOOP_STREAMING_JAR_URI
        # if self._opts['hadoop_streaming_jar']:
        #     self._upload_mgr.add(self._opts['hadoop_streaming_jar'])

        for step in self._get_steps():
            if step.get('jar'):
                self._upload_mgr.add(step['jar'])

    def _upload_local_files_to_fs(self):
        """Copy local files tracked by self._upload_mgr to FS."""
        bucket_name, _ = parse_gcs_uri(self._job_tmpdir)
        self._create_fs_tmp_bucket(bucket_name)

        log.info('Copying non-input files into %s' % self._upload_mgr.prefix)

        for path, gcs_uri in self._upload_mgr.path_to_uri().items():
            log.debug('uploading %s -> %s' % (path, gcs_uri))

            # TODO - mtai @ davidmarin - Implement put function for other FSs
            self.fs.put(path, gcs_uri)

        self._wait_for_fs_sync()

    def _create_fs_tmp_bucket(self, bucket_name, location=None):
        """Create a temp bucket if missing

        Tie the temporary bucket to the same region as the GCE job and set a
        28-day TTL
        """
        # Return early if our bucket already exists
        try:
            self.fs.get_bucket(bucket_name)
            return
        except google_errors.HttpError as e:
            if not e.resp.status == 404:
                raise

        log.info('creating FS bucket %r' % bucket_name)

        location = location or self._gce_region

        # NOTE - By default, we create a bucket in the same GCE region as our
        # job (tmp buckets ONLY)
        # https://cloud.google.com/storage/docs/bucket-locations
        self.fs.create_bucket(
            self._gcp_project, bucket_name, location=location,
            object_ttl_days=_DEFAULT_CLOUD_TMP_DIR_OBJECT_TTL_DAYS)

        self._wait_for_fs_sync()

    ### Running the job ###
    def cleanup(self, mode=None):
        super(DataprocJobRunner, self).cleanup(mode=mode)

        # stop the cluster if it belongs to us (it may have stopped on its
        # own already, but that's fine)
        if self._cluster_id and not self._opts['cluster_id']:
            self._cleanup_cluster()

    def _cleanup_cloud_tmp(self):
        # delete all the files we created
        if not self._job_tmpdir:
            return

        try:
            log.info('Removing all files in %s' % self._job_tmpdir)
            self.fs.rm(self._job_tmpdir)
            self._job_tmpdir = None
        except Exception as e:
            log.exception(e)

    # TODO - mtai @ davidmarin - Re-enable log support and supporting cleanup
    def _cleanup_logs(self):
        super(DataprocJobRunner, self)._cleanup_logs()

    def _cleanup_job(self):
        job_prefix = self._dataproc_job_prefix()
        for current_job in self._api_job_list(
                cluster_name=self._cluster_id, state_matcher='ACTIVE'):
            # Kill all active jobs with the same job_prefix as this job
            current_job_id = current_job['reference']['jobId']

            if not current_job_id.startswith(job_prefix):
                continue

            self._api_job_cancel(current_job_id)
            self._wait_for_api('job cancellation')

    def _cleanup_cluster(self):
        if not self._cluster_id:
            # If we don't have a cluster, then we can't terminate it.
            return

        try:
            log.info("Attempting to terminate cluster")
            self._api_cluster_delete(self._cluster_id)
        except Exception as e:
            log.exception(e)
            return
        log.info('cluster %s successfully terminated' % self._cluster_id)

    def _wait_for_api(self, msg):
        _wait_for(msg, self._opts['check_cluster_every'])

    def _wait_for_fs_sync(self):
        """Sleep for a little while, to give FS a chance to sync up.
        """
        _wait_for('GCS sync (eventual consistency)',
                  self._opts['cloud_fs_sync_secs'])

    def _build_dataproc_hadoop_job(self, step_num):
        """This function creates a "HadoopJob" to be passed to
        self._api_job_submit_hadoop

        :param step_num:
        :return: output_hadoop_job
        """
        # Reference: https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs#HadoopJob  # noqa

        args = list()
        file_uris = list()
        archive_uris = list()
        properties = dict()

        step = self._get_step(step_num)

        assert step['type'] in ('streaming', 'jar'), (
            'Bad step type: %r' % (step['type'],))

        # TODO - mtai @ davidmarin - Might be trivial to support jar running,
        # see "mainJarFileUri" of variable "output_hadoop_job" in this function
        #         https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs#HadoopJob  # noqa

        assert step['type'] == 'streaming', 'Jar not implemented'
        main_jar_uri = _HADOOP_STREAMING_JAR_URI

        # TODO - mtai @ davidmarin - Not clear if we should move _upload_args
        # to file_uris, currently works fine as-is

        # TODO - dmarin @ mtai - Probably a little safer to do the API's way,
        # assuming the API supports distributed cache syntax (so we can pick
        # the names of the uploaded files).
        args.extend(self._upload_args())

        args.extend(self._hadoop_args_for_step(step_num))

        mapper, combiner, reducer = (self._hadoop_streaming_commands(step_num))

        if mapper:
            args += ['-mapper', mapper]

        if combiner:
            args += ['-combiner', combiner]

        if reducer:
            args += ['-reducer', reducer]

        for current_input_uri in self._step_input_uris(step_num):
            args += ['-input', current_input_uri]

        args += ['-output', self._step_output_uri(step_num)]

        # TODO - mtai @ davidmarin - Add back support to specify a different
        # mainJarFileURI
        output_hadoop_job = dict(
            args=args,
            fileUris=file_uris,
            archiveUris=archive_uris,
            properties=properties,
            mainJarFileUri=main_jar_uri
        )
        return output_hadoop_job

    def _launch_cluster(self):
        """Create an empty cluster on Dataproc, and set self._cluster_id to
        its ID."""
        bucket_name, _ = parse_gcs_uri(self._job_tmpdir)
        self._create_fs_tmp_bucket(bucket_name)

        # clusterName must be a match of
        # regex '(?:[a-z](?:[-a-z0-9]{0,53}[a-z0-9])?).'
        # as documented in an API error message
        # (not currently documented in the Dataproc docs)
        if not self._cluster_id:
            self._cluster_id = '-'.join(
                ['mrjob', self._gce_zone.lower(), random_identifier()])

        # Create the cluster if it's missing, otherwise join an existing one
        try:
            self._api_cluster_get(self._cluster_id)
            log.info('Adding job to existing cluster - %s' % self._cluster_id)
        except google_errors.HttpError as e:
            if not e.resp.status == 404:
                raise

            log.info(
                'Creating Dataproc Hadoop cluster - %s' % self._cluster_id)

            cluster_data = self._cluster_create_args()

            self._api_cluster_create(cluster_data)

            self._wait_for_cluster_ready(self._cluster_id)

        # keep track of when we launched our job
        self._dataproc_job_start = time.time()
        return self._cluster_id

    def _wait_for_cluster_ready(self, cluster_id):
        # See https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.clusters#State  # noqa
        cluster_state = None

        # Poll until cluster is ready
        while cluster_state not in _DATAPROC_CLUSTER_STATES_READY:
            result_describe = self.api_client.clusters().get(
                projectId=self._gcp_project,
                region=_DATAPROC_API_REGION,
                clusterName=cluster_id).execute()

            cluster_state = result_describe['status']['state']
            if cluster_state in _DATAPROC_CLUSTER_STATES_ERROR:
                raise DataprocException(result_describe)

            self._wait_for_api('cluster to accept jobs')

        assert cluster_state in _DATAPROC_CLUSTER_STATES_READY
        log.info("Cluster %s ready", cluster_id)

        return cluster_id

    def _dataproc_job_prefix(self):
        return _cleanse_gcp_job_id(self._job_key)

    def _run_steps(self):
        """Wait for every step of the job to complete, one by one."""
        total_steps = self._num_steps()
        # define out steps
        for step_num in range(total_steps):
            job_id = self._launch_step(step_num)

            self._wait_for_step_to_complete(
                job_id, step_num=step_num, num_steps=total_steps)

            log.info('Completed Dataproc Hadoop Job - %s', job_id)

        # After all steps completed, wait for the last output (which is
        # usually written to GCS) to sync
        self._wait_for_fs_sync()

    def _launch_step(self, step_num):
        # Build each step
        hadoop_job = self._build_dataproc_hadoop_job(step_num)

        # Clean-up step name
        step_name = '%s---step-%05d-of-%05d' % (
            self._dataproc_job_prefix(), step_num + 1, self._num_steps())

        # Submit it
        log.info('Submitting Dataproc Hadoop Job - %s', step_name)
        result = self._api_job_submit_hadoop(step_name, hadoop_job)
        log.info('Submitted Dataproc Hadoop Job - %s', step_name)

        job_id = result['reference']['jobId']
        assert job_id == step_name

        return job_id

    def _wait_for_step_to_complete(
            self, job_id, step_num=None, num_steps=None):
        """Helper for _wait_for_step_to_complete(). Wait for
        step with the given ID to complete, and fetch counters.
        If it fails, attempt to diagnose the error, and raise an
        exception.

        This also adds an item to self._log_interpretations
        """
        log_interpretation = dict(job_id=job_id)
        self._log_interpretations.append(log_interpretation)

        while True:
            # https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs#JobStatus  # noqa
            job_result = self._api_job_get(job_id)
            job_state = job_result['status']['state']

            log.info('%s => %s' % (job_id, job_state))

            # https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs#State  # noqa
            if job_state in _DATAPROC_JOB_STATES_ACTIVE:
                self._wait_for_api('job completion')
                continue

            # we're done, will return at the end of this
            elif job_state == 'DONE':
                break

            raise StepFailedException(step_num=step_num, num_steps=num_steps)

    def _default_step_output_dir(self):
        # put intermediate data in HDFS
        return 'hdfs:///tmp/mrjob/%s/step-output' % self._job_key

    def counters(self):
        # TODO - mtai @ davidmarin - Counters are currently always empty as we
        # are not processing task logs
        return [_pick_counters(log_interpretation)
                for log_interpretation in self._log_interpretations]

    ### Bootstrapping ###

    def get_hadoop_version(self):
        if self._hadoop_version is None:
            self._store_cluster_info()
        return self._hadoop_version

    def get_image_version(self):
        """Get the version that our cluster is running.
        """
        if self._image_version is None:
            self._store_cluster_info()
        return self._image_version

    def _store_cluster_info(self):
        """Set self._image_version and self._hadoop_version."""
        if not self._cluster_id:
            raise AssertionError('cluster has not yet been created')

        cluster = self._api_cluster_get(self._cluster_id)
        self._image_version = (
            cluster['config']['softwareConfig']['imageVersion'])
        # protect against new versions, including patch versions
        # we didn't explicitly request. See #1428
        self._hadoop_version = map_version(
            self._image_version, _DATAPROC_IMAGE_TO_HADOOP_VERSION)

    ### Bootstrapping ###

    # TODO: merge code shared with emr.py

    def _create_master_bootstrap_script_if_needed(self):
        """Helper for :py:meth:`_add_bootstrap_files_for_upload`.

        Create the master bootstrap script and write it into our local
        temp directory. Set self._master_bootstrap_script_path.

        This will do nothing if there are no bootstrap scripts or commands,
        or if it has already been called."""
        if self._master_bootstrap_script_path:
            return

        # don't bother if we're not starting a cluster
        if self._cluster_id:
            return

        # Also don't bother if we're not bootstrapping
        if not (self._bootstrap or self._bootstrap_mrjob()):
            return

        # create mrjob.zip if we need it, and add commands to install it
        mrjob_bootstrap = []
        if self._bootstrap_mrjob():
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

            # unzip mrjob.zip
            mrjob_bootstrap.append(
                ['sudo unzip ', path_dict, ' -d $__mrjob_PYTHON_LIB'])

            # re-compile pyc files now, since mappers/reducers can't
            # write to this directory. Don't fail if there is extra
            # un-compileable crud in the tarball (this would matter if
            # sh_bin were 'sh -e')
            mrjob_bootstrap.append(
                ['sudo %s -m compileall -q'
                 ' -f $__mrjob_PYTHON_LIB/mrjob && true' %
                 cmd_line(self._python_bin())])

        path = os.path.join(self._get_local_tmp_dir(), 'b.sh')
        log.info('writing master bootstrap script to %s' % path)

        contents = self._master_bootstrap_script_content(
            self._bootstrap + mrjob_bootstrap)
        for line in contents:
            log.debug('BOOTSTRAP: ' + line.rstrip('\r\n'))

        with open(path, 'w') as f:
            for line in contents:
                f.write(line)

        self._master_bootstrap_script_path = path

    def _bootstrap_python(self):
        """Return a (possibly empty) list of parsed commands (in the same
        format as returned by parse_setup_cmd())'"""
        if not self._opts['bootstrap_python']:
            return []

        if PY2:
            # Python 2 is already installed; install pip and dev packages
            return [
                ['sudo apt-get install -y python-pip python-dev'],
            ]
        else:
            return [
                ['sudo apt-get install -y python3 python3-pip python3-dev'],
            ]

    def _parse_bootstrap(self):
        """Parse the *bootstrap* option with
        :py:func:`mrjob.setup.parse_setup_cmd()`.
        """
        return [parse_setup_cmd(cmd) for cmd in self._opts['bootstrap']]

    def _master_bootstrap_script_content(self, bootstrap):
        """Create the contents of the master bootstrap script.
        """
        out = []

        def writeln(line=''):
            out.append(line + '\n')

        # shebang
        sh_bin = self._sh_bin()
        if not sh_bin[0].startswith('/'):
            sh_bin = ['/usr/bin/env'] + sh_bin
        writeln('#!' + cmd_line(sh_bin))

        # unused hook for 'set -e', etc. (see #1549)
        for cmd in self._sh_pre_commands():
            writeln(cmd)
        writeln()

        # store $PWD
        writeln('# store $PWD')
        writeln('__mrjob_PWD=$PWD')

        # FYI - mtai @ davidmarin - begin section, mtai had to add this
        # otherwise initialization didn't work
        # // kept blowing up in all subsequent invocations of $__mrjob_PWD/
        writeln('if [ $__mrjob_PWD = "/" ]; then')
        writeln('  __mrjob_PWD=""')
        writeln('fi')
        # FYI - mtai @ davidmarin - end section

        writeln()

        # download files
        writeln('# download files and mark them executable')

        cp_to_local = 'hadoop fs -copyToLocal'

        for name, path in sorted(
                self._bootstrap_dir_mgr.name_to_path('file').items()):
            uri = self._upload_mgr.uri(path)

            output_string = '%s %s $__mrjob_PWD/%s' % (
                cp_to_local, pipes.quote(uri), pipes.quote(name))

            writeln(output_string)
            # make everything executable, like Hadoop Distributed Cache
            writeln('chmod a+x $__mrjob_PWD/%s' % pipes.quote(name))
        writeln()

        # run bootstrap commands
        writeln('# bootstrap commands')
        for cmd in bootstrap:
            # reconstruct the command line, substituting $__mrjob_PWD/<name>
            # for path dicts
            line = ''
            for token in cmd:
                if isinstance(token, dict):
                    # it's a path dictionary
                    line += '$__mrjob_PWD/'
                    line += pipes.quote(self._bootstrap_dir_mgr.name(**token))
                else:
                    # it's raw script
                    line += token
            writeln(line)
        writeln()

        return out

    def get_cluster_id(self):
        return self._cluster_id

    def _cluster_create_args(self):
        gcs_init_script_uris = []
        if self._master_bootstrap_script_path:
            gcs_init_script_uris.append(
                self._upload_mgr.uri(self._master_bootstrap_script_path))

            # always add idle termination script
            # add it last, so that we don't count bootstrapping as idle time
            gcs_init_script_uris.append(
                self._upload_mgr.uri(_MAX_HOURS_IDLE_BOOTSTRAP_ACTION_PATH))

        # NOTE - Cluster initializationActions can only take scripts with no
        # script args, so the auto-term script receives 'mrjob-max-secs-idle'
        # via metadata instead of as an arg
        cluster_metadata = dict()
        cluster_metadata['mrjob-version'] = mrjob.__version__
        cluster_metadata['mrjob-max-secs-idle'] = str(int(
            self._opts['max_hours_idle'] * 3600))

        cluster_config = dict(
            gceClusterConfig=dict(
                zoneUri=_gcp_zone_uri(
                    project=self._gcp_project, zone=self._gce_zone),
                serviceAccountScopes=_DEFAULT_GCE_SERVICE_ACCOUNT_SCOPES,
                metadata=cluster_metadata
            ),
            initializationActions=[
                dict(executableFile=init_script_uri)
                for init_script_uri in gcs_init_script_uris
            ]
        )

        # Task tracker
        master_conf = _gcp_instance_group_config(
            project=self._gcp_project, zone=self._gce_zone,
            count=1, instance_type=self._opts['master_instance_type']
        )

        # Compute + storage
        worker_conf = _gcp_instance_group_config(
            project=self._gcp_project, zone=self._gce_zone,
            count=self._opts['num_core_instances'],
            instance_type=self._opts['core_instance_type']
        )

        # Compute ONLY
        secondary_worker_conf = _gcp_instance_group_config(
            project=self._gcp_project, zone=self._gce_zone,
            count=self._opts['num_task_instances'],
            instance_type=self._opts['task_instance_type'],
            is_preemptible=True
        )

        cluster_config['masterConfig'] = master_conf
        cluster_config['workerConfig'] = worker_conf
        if self._opts['num_task_instances']:
            cluster_config['secondaryWorkerConfig'] = secondary_worker_conf

        # See - https://cloud.google.com/dataproc/dataproc-versions
        if self._opts['image_version']:
            cluster_config['softwareConfig'] = dict(
                imageVersion=self._opts['image_version'])

        return dict(projectId=self._gcp_project,
                    clusterName=self._cluster_id,
                    config=cluster_config)

    ### Dataproc-specific Stuff ###

    def _api_cluster_get(self, cluster_id):
        return self.api_client.clusters().get(
            projectId=self._gcp_project,
            region=_DATAPROC_API_REGION,
            clusterName=cluster_id
        ).execute()

    def _api_cluster_create(self, cluster_data):
        # https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.clusters/create  # noqa
        # https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.clusters/get  # noqa
        return self.api_client.clusters().create(
            projectId=self._gcp_project,
            region=_DATAPROC_API_REGION,
            body=cluster_data
        ).execute()

    def _api_cluster_delete(self, cluster_id):
        return self.api_client.clusters().delete(
            projectId=self._gcp_project,
            region=_DATAPROC_API_REGION,
            clusterName=cluster_id
        ).execute()

    def _api_job_list(self, cluster_name=None, state_matcher=None):
        # https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs/list#JobStateMatcher  # noqa
        list_kwargs = dict(
            projectId=self._gcp_project,
            region=_DATAPROC_API_REGION,
        )
        if cluster_name:
            list_kwargs['clusterName'] = cluster_name

        if state_matcher:
            list_kwargs['jobStateMatcher'] = state_matcher

        list_request = self.api_client.jobs().list(**list_kwargs)
        while list_request:
            try:
                resp = list_request.execute()
            except google_errors.HttpError as e:
                if e.resp.status == 404:
                    return
                raise

            for current_item in resp['items']:
                yield current_item

            list_request = self.api_client.jobs().list_next(list_request, resp)

    def _api_job_get(self, job_id):
        return self.api_client.jobs().get(
            projectId=self._gcp_project,
            region=_DATAPROC_API_REGION,
            jobId=job_id
        ).execute()

    def _api_job_cancel(self, job_id):
        return self.api_client.jobs().cancel(
            projectId=self._gcp_project,
            region=_DATAPROC_API_REGION,
            jobId=job_id
        ).execute()

    def _api_job_delete(self, job_id):
        return self.api_client.jobs().delete(
            projectId=self._gcp_project,
            region=_DATAPROC_API_REGION,
            jobId=job_id
        ).execute()

    def _api_job_submit_hadoop(self, step_name, hadoop_job):
        # https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs/submit  # noqa
        # https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs#HadoopJob  # noqa
        # https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs#JobReference  # noqa
        job_data = dict(
            reference=dict(projectId=self._gcp_project, jobId=step_name),
            placement=dict(clusterName=self._cluster_id),
            hadoopJob=hadoop_job
        )

        jobs_submit_kwargs = dict(
            projectId=self._gcp_project,
            region=_DATAPROC_API_REGION,
            body=dict(job=job_data)
        )
        return self.api_client.jobs().submit(**jobs_submit_kwargs).execute()
