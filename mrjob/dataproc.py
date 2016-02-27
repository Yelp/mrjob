# -*- coding: utf-8 -*-
# Copyright 2016 Google Inc.
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
import time
import re

try:
    from oauth2client.client import GoogleCredentials
    from googleapiclient import discovery
except ImportError:
    # don't require googleapiclient; MRJobs don't actually need it when running
    # inside hadoop streaming
    GoogleCredentials = None
    discovery = None

try:
    import filechunkio
except ImportError:
    # that's cool; filechunkio is only for multipart uploading
    filechunkio = None

from mrjob.conf import combine_dicts
from mrjob.conf import combine_lists
from mrjob.conf import combine_paths

from mrjob.fs.composite import CompositeFilesystem
from mrjob.fs.local import LocalFilesystem

from mrjob.fs.gcs import GCSFilesystem

from mrjob.logs.counters import _pick_counters
from mrjob.fs.gcs import is_gcs_uri
from mrjob.parse import is_uri

from mrjob.runner import MRJobRunner
from mrjob.runner import RunnerOptionStore
from mrjob.setup import BootstrapWorkingDirManager
from mrjob.setup import UploadDirManager
from mrjob.setup import parse_setup_cmd
from mrjob.step import StepFailedException
from mrjob.util import cmd_line
from mrjob.util import random_identifier

log = logging.getLogger(__name__)

_DEFAULT_GCP_REGION = 'global'
_HADOOP_STREAMING_JAR_URI = 'file:///usr/lib/hadoop-mapreduce/hadoop-streaming.jar'

# TODO - mtai @ davidmarin - Example invocation options for mrjob.py
#   --runner dataproc --gcp-project google.com:pm-hackathon --gcp-zone us-central1-b --gcp-cluster vbp04 --no-output gs://boulder-input-data/*.txt --output gs://boulder-input-data/mrjob-boulder

# TODO - mtai @ davidmarin - Re-implement SSH tunneling? - Content pulled from job run output
# The url to track the job: http://default-taim-m:8088/proxy/application_1456679373146_0002/

# TODO - mtai @ davidmarin - Re-implement logs parsing?  See dataproc metainfo and driver output - ask Dennis Huo for more details
# 'gs://dataproc-801485be-0997-40e7-84a7-00926031747c-us/google-cloud-dataproc-metainfo/8b76d95e-ebdc-4b81-896d-b2c5009b3560/jobs/mr_most_used_word-taim-20160228-172000-034993---Step-2-of-2/driveroutput'

# TODO - mtai @ davidmarin - We should re-instance the bucket creation logic used in EMR
_DEFAULT_GCS_BUCKET = 'boulder-input-data'

_GCP_CLUSTER_NAME_REGEX = '(?:[a-z](?:[-a-z0-9]{0,53}[a-z0-9])?).'

def _cleanse_gcp_job_id(job_id):
    return re.sub('[^a-zA-Z0-9_\-]', '-', job_id)

def _cleanse_gcp_cluster_name(cluster_name):
    pass


class DataprocRunnerOptionStore(RunnerOptionStore):
    ALLOWED_KEYS = RunnerOptionStore.ALLOWED_KEYS.union(set([
        'gcp_project',
        'gcp_region',
        'gcp_zone',
        'gcp_cluster',

        'gce_machine_type',
        'gce_num_instances',

        'gcs_sync_wait_time',
        'gcs_tmp_dir',

        'check_api_status_every',
        'bootstrap',
        'bootstrap_python'

    ]))

    COMBINERS = combine_dicts(RunnerOptionStore.COMBINERS, {
        'bootstrap': combine_lists,
        'gcs_tmp_dir': combine_paths,
    })

    DEPRECATED_ALIASES = combine_dicts(RunnerOptionStore.DEPRECATED_ALIASES, {
    })

    def __init__(self, alias, opts, conf_paths):
        super(DataprocRunnerOptionStore, self).__init__(alias, opts, conf_paths)

        # TODO - mtai @ davidmarin - As of 2016-02-26, the ONLY valid region is "global"
        self['gcp_region'] = _DEFAULT_GCP_REGION

        # Dataproc requires a master and >= 2 worker instances
        # gce_num_instances refers ONLY to number of WORKER instances and does NOT include the required 1 instance for master
        # In other words, minimum cluster size is 3 machines, 1 master and "gce_num_instances" workers
        if self['gce_num_instances'] < 2:
            self['gce_num_instances'] = 2

    def default_options(self):
        super_opts = super(DataprocRunnerOptionStore, self).default_options()
        return combine_dicts(super_opts, {
            'bootstrap_python': True,
            'check_api_status_every': 30,

            'gce_machine_type': 'n1-standard-1',
            'gce_num_instances': 2,

            'gcs_sync_wait_time': 5.0,
            'sh_bin': ['/bin/sh', '-ex'],
        })

class DataprocJobRunner(MRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` on Google Cloud Dataproc.
    Invoked when you run your job with ``-r dataproc``.

    :py:class:`DataprocJobRunner` runs your job in an Dataproc cluster, which is
    basically a temporary Hadoop cluster.

    Input, support, and jar files can be either local or on GCS; use
    ``gs://...`` URLs to refer to files on GCS.

    This class has some useful utilities for talking directly to GCS and Dataproc,
    so you may find it useful to instantiate it without a script::

        from mrjob.dataproc import DataprocJobRunner
        ...
    """
    alias = 'dataproc'

    # Don't need to bootstrap mrjob in the setup wrapper; that's what
    # the bootstrap script is for!
    BOOTSTRAP_MRJOB_IN_SETUP = False

    OPTION_STORE_CLASS = DataprocRunnerOptionStore

    def __init__(self, **kwargs):
        """:py:class:`~mrjob.dataproc.DataprocJobRunner` takes the same arguments as
        :py:class:`~mrjob.runner.MRJobRunner`, plus some additional options
        which can be defaulted in :ref:`mrjob.conf <mrjob.conf>`.
\
        """
        super(DataprocJobRunner, self).__init__(**kwargs)

        # if we're going to create a bucket to use as temp space, we don't
        # want to actually create it until we run the job (Issue #50).
        # This variable helps us create the bucket as needed

        # TODO - mtai @ davidmarin - We should re-instance the bucket creation logic used in EMR, instead creating buckets in GCS
        self._fix_gcs_tmp_opts()

        # use job key to make a unique tmp dir
        self._gcs_tmp_dir = self._opts['gcs_tmp_dir'] + self._job_key + '/'

        # pick/validate output dir
        if self._output_dir:
            self._output_dir = self._check_and_fix_gcs_dir(self._output_dir)
        else:
            self._output_dir = self._gcs_tmp_dir + 'output/'

        # manage working dir for bootstrap script
        self._bootstrap_dir_mgr = BootstrapWorkingDirManager()

        # manage local files that we want to upload to GCS. We'll add them
        # to this manager just before we need them.
        gcs_files_dir = self._gcs_tmp_dir + 'files/'
        self._upload_mgr = UploadDirManager(gcs_files_dir)

        self._bootstrap = self._bootstrap_python() + self._parse_bootstrap()

        for cmd in self._bootstrap:
            for maybe_path_dict in cmd:
                if isinstance(maybe_path_dict, dict):
                    self._bootstrap_dir_mgr.add(**maybe_path_dict)

        # we'll create the script later
        self._master_bootstrap_script_path = None

        self._gcp_project = self._opts['gcp_project']
        self._gcp_region = self._opts['gcp_region']
        self._gcp_zone = self._opts['gcp_zone']
        self._gcp_cluster = self._opts['gcp_cluster']

        self._dataproc_client = None

        # when did our particular task start?
        self._dataproc_job_start = None

        # This will be filled by _wait_for_steps_to_complete()
        self._log_interpretations = []

    @property
    def dataproc_client(self):
        if not self._dataproc_client:
            credentials = GoogleCredentials.get_application_default()

            dataproc_client = discovery.build('dataproc', 'v1', credentials=credentials)
            self._dataproc_client = dataproc_client.projects().regions()

        return self._dataproc_client

    def _fix_gcs_tmp_opts(self):
        """Fill in gcs_tmp_dir (in self._opts) if they
        aren't already set.

        Helper for __init__.
        """
        # set gcs_tmp_dir by checking for existing buckets
        if not self._opts['gcs_tmp_dir']:
            self._set_gcs_tmp_dir()
            log.info('using %s as our temp dir on GCS' %
                     self._opts['gcs_tmp_dir'])

        self._opts['gcs_tmp_dir'] = self._check_and_fix_gcs_dir(
            self._opts['gcs_tmp_dir'])

    def _set_gcs_tmp_dir(self):
        """Helper for _fix_gcs_tmp_dir"""
        mrjob_tmp = 'mrjob-%s' % random_identifier()
        self._opts['gcs_tmp_dir'] = ('gs://%s/tmp/%s' % (_DEFAULT_GCS_BUCKET, mrjob_tmp))
        return

    def _check_and_fix_gcs_dir(self, gcs_uri):
        """Helper for __init__"""
        if not is_gcs_uri(gcs_uri):
            raise ValueError('Invalid GCS URI: %r' % gcs_uri)
        if not gcs_uri.endswith('/'):
            gcs_uri = gcs_uri + '/'

        return gcs_uri

    @property
    def fs(self):
        """:py:class:`~mrjob.fs.base.Filesystem` object for SSH, GCS, and the
        local filesystem.
        """
        if self._fs is not None:
            return self._fs

        self._gcs_fs = GCSFilesystem()

        self._fs = CompositeFilesystem(self._gcs_fs, LocalFilesystem())
        return self._fs

    def _run(self):
        self._launch()
        self._wait_for_steps_to_complete()

    def _launch(self):
        self._prepare_for_launch()
        self._launch_dataproc_cluster()

    def _prepare_for_launch(self):
        self._check_input_exists()
        self._check_output_not_exists()
        self._create_setup_wrapper_script()
        self._add_bootstrap_files_for_upload()
        self._add_job_files_for_upload()
        self._upload_local_files_to_gcs()

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
        # lazily create mrjob.tar.gz
        if self._bootstrap_mrjob():
            self._create_mrjob_tar_gz()
            self._bootstrap_dir_mgr.add('file', self._mrjob_tar_gz_path)

        # all other files needed by the script are already in
        # _bootstrap_dir_mgr
        for path in self._bootstrap_dir_mgr.paths():
            self._upload_mgr.add(path)

        # now that we know where the above files live, we can create
        # the master bootstrap script
        self._create_master_bootstrap_script_if_needed()
        if self._master_bootstrap_script_path:
            self._upload_mgr.add(self._master_bootstrap_script_path)

    def _add_job_files_for_upload(self):
        """Add files needed for running the job (setup and input)
        to self._upload_mgr."""
        for path in self._get_input_paths():
            self._upload_mgr.add(path)

        for path in self._working_dir_mgr.paths():
            self._upload_mgr.add(path)

        # TODO - mtai @ davidmarin - hadoop_streaming_jar is currently ignored, see _HADOOP_STREAMING_JAR_URI
        # if self._opts['hadoop_streaming_jar']:
        #     self._upload_mgr.add(self._opts['hadoop_streaming_jar'])

        for step in self._get_steps():
            if step.get('jar'):
                self._upload_mgr.add(step['jar'])

    def _upload_local_files_to_gcs(self):
        """Copy local files tracked by self._upload_mgr to GCS."""
        log.info('Copying non-input files into %s' % self._upload_mgr.prefix)

        for path, gcs_uri in self._upload_mgr.path_to_uri().items():
            log.debug('uploading %s -> %s' % (path, gcs_uri))

            # TODO - mtai @ davidmarin - Implement upload function for other FSs
            self.fs.upload(path, gcs_uri)

    ### Running the job ###

    def cleanup(self, mode=None):
        super(DataprocJobRunner, self).cleanup(mode=mode)

        # TODO - mtai @ davidmarin - Clusters are NOT being automatically cleaned up
        # self._cleanup_cluster()

    def _cleanup_remote_tmp(self):
        # delete all the files we created
        if self._gcs_tmp_dir:
            try:
                log.info('Removing all files in %s' % self._gcs_tmp_dir)
                self.fs.rm(self._gcs_tmp_dir)
                self._gcs_tmp_dir = None
            except Exception as e:
                log.exception(e)

    # TODO - mtai @ davidmarin - Re-enable log support and supporting cleanup
    def _cleanup_logs(self):
        super(DataprocJobRunner, self)._cleanup_logs()

    # TODO - mtai @ davidmarin - Revisit
    def _cleanup_job(self):
        pass

    def _cleanup_cluster(self):
        try:
            log.info("Attempting to terminate cluster")
            self._delete_cluster()
        except Exception as e:
            log.exception(e)
            return
        log.info('cluster %s successfully terminated' % self._gcp_cluster)

    def _wait_for_gcs_eventual_consistency(self):
        """Sleep for a little while, to give GCS a chance to sync up.
        """
        log.info('Waiting %.1fs for GCS eventual consistency' %
                 self._opts['gcs_sync_wait_time'])
        time.sleep(self._opts['gcs_sync_wait_time'])

    def _build_dataproc_hadoop_job(self, step_num):
        """
        NOTE - mtai @ davidmarin - this function creates a "HadoopJob" to be passed to self._submit_hadoop_job

        Reference...
        https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs#HadoopJob

        :param step_num:
        :return:
        """
        args = list()
        file_uris = list()
        archive_uris = list()
        properties = dict()

        step = self._get_step(step_num)

        assert step['type'] in ('streaming', 'jar'), 'Bad step type: %r' % (step['type'],)

        # TODO - mtai @ davidmarin - Might be trivial to support jar running, see "mainJarFileUri" of variable "output_hadoop_job" in this function
        #         https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs#HadoopJob
        assert step['type'] == 'streaming', 'Jar not implemented'
        main_jar_uri = _HADOOP_STREAMING_JAR_URI


        # TODO - mtai @ davidmarin - Not clear if we should move _upload_args to file_uris, currently works fine as-is
        args.extend(self._upload_args(self._upload_mgr))

        # TODO - mtai @ davidmarin - Not clear if we should move some of these to properties dict to file_uris, currently works fine as-is
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

        # TODO - mtai @ davidmarin - Add back support to specify a different jar URI
        output_hadoop_job = {
            "args": args,
            "fileUris": file_uris,
            "archiveUris": archive_uris,
            "properties": properties,
            "mainJarFileUri": main_jar_uri
        }
        return output_hadoop_job

    def _launch_dataproc_cluster(self):
        """Create an empty cluster on Dataproc, and set self._gcp_cluster to
        its ID."""
        self._wait_for_gcs_eventual_consistency()

        self._gcp_cluster_start = time.time()

        # make sure we can see the files we copied to GCS
        log.info('Creating Dataproc Hadoop cluster')

        # NOTE - mtai @ davidmarin - We create the cluster if its missing, otherwise we join an existing one
        # NOTE - mtai @ davidmarin - The cluster MUST be created before submitting jobs, otherwise job submission fails

        # TODO - mtai @ davidmarin - change the way we auto-create cluster names
        # "clusterName must be a match of regex '(?:[a-z](?:[-a-z0-9]{0,53}[a-z0-9])?).'"
        self._gcp_cluster = self._gcp_cluster or "default-%s" % self._opts['owner']

        try:
            self._get_cluster()
            log.info('Adding our job to existing cluster %s' % self._gcp_cluster)
        # TODO - mtai @ davidmarin - Change Exception to HttpError and check 404 for cluster does not exist
        except Exception as e:
            self._create_cluster()
            log.info('Created new cluster %s' % self._gcp_cluster)

        # keep track of when we launched our job
        self._dataproc_job_start = time.time()

    def _wait_for_steps_to_complete(self):
        """Wait for every step of the job to complete, one by one."""

        total_steps = self._num_steps()
        # define out steps
        for step_num in xrange(total_steps):
            # NOTE - mtai @ davidmarin - Dataproc accepts multiple jobs at once
            # NOTE - mtai @ davidmarin - In this particular implementation, we must build each step on the fly and submit it and WAIT for step completion
            # NOTE - mtai @ davidmarin - we THEN iterate through the loop and continue on

            # Build each step
            hadoop_job = self._build_dataproc_hadoop_job(step_num)

            # Clean-up step name
            step_name = '%s - Step %d of %d' % (self._job_key, step_num + 1, self._num_steps())
            cleansed_step_name = _cleanse_gcp_job_id(step_name)

            # Submit it
            log.info('Submitting Dataproc Hadoop Job - %s', cleansed_step_name)
            job_id = self._submit_hadoop_job(cleansed_step_name, hadoop_job)
            log.info('Submitted Dataproc Hadoop Job - %s', job_id)

            assert job_id == cleansed_step_name

            # Wait for Job completion
            self._wait_for_step_to_complete(job_id, step_num=step_num, num_steps=total_steps)

            log.info('Completed Dataproc Hadoop Job - %s', job_id)

    def _wait_for_step_to_complete(self, job_id, step_num=None, num_steps=None):
        """Helper for _wait_for_step_to_complete(). Wait for
        step with the given ID to complete, and fetch counters.
        If it fails, attempt to diagnose the error, and raise an
        exception.

        This also adds an item to self._log_interpretations
        """
        log_interpretation = dict(job_id=job_id)
        self._log_interpretations.append(log_interpretation)

        while True:
            # don't antagonize Dataproc's throttling
            log.debug('Waiting %.1f seconds...' %
                      self._opts['check_api_status_every'])
            time.sleep(self._opts['check_api_status_every'])

            # https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs#JobStatus
            job_result = self._get_job(job_id)

            job_start_time = job_result['status']['stateStartTime']
            job_state = job_result['status']['state']

            log.info('%s  %s - %s' % (job_id, job_start_time, job_state))

            # TODO - mtai @ davidmarin - State checking is NOT tested at this point, see included URL for Dataproc Job states
            # NOTE -  mtai @ davidmarin - https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs#State
            if job_state not in ('DONE', 'ERROR', 'CANCELLED'):
                continue

            # we're done, will return at the end of this
            if job_state == 'DONE':
                break

            elif job_state == 'ERROR':
                raise StepFailedException(step_num=step_num, num_steps=num_steps)

    def _step_input_uris(self, step_num):
        """Get the gs:// URIs for input for the given step."""
        if step_num == 0:
            return [self._upload_mgr.uri(path)
                    for path in self._get_input_paths()]
        else:
            # put intermediate data in HDFS
            return ['hdfs:///tmp/mrjob/%s/step-output/%s/' % (
                self._job_key, step_num)]

    def _step_output_uri(self, step_num):
        if step_num == len(self._get_steps()) - 1:
            return self._output_dir
        else:
            # put intermediate data in HDFS
            return 'hdfs:///tmp/mrjob/%s/step-output/%s/' % (
                self._job_key, step_num + 1)

    def counters(self):
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

        # TODO - mtai @ davidmarin - Don't know when we don't need to do this...
        # TODO - mtai @ davidmarin - for now, we're always creating the master_bootstrap_script
        #  don't bother if we're not starting a cluster
        # if self._gcp_cluster:
        #     return

        # Also don't bother if we're not bootstrapping
        if not (self._bootstrap or self._bootstrap_mrjob()):
            return

        # create mrjob.tar.gz if we need it, and add commands to install it
        mrjob_bootstrap = []
        if self._bootstrap_mrjob():
            assert self._mrjob_tar_gz_path
            path_dict = {
                'type': 'file', 'name': None, 'path': self._mrjob_tar_gz_path}
            self._bootstrap_dir_mgr.add(**path_dict)

            # find out where python keeps its libraries
            mrjob_bootstrap.append([
                "__mrjob_PYTHON_LIB=$(%s -c "
                "'from distutils.sysconfig import get_python_lib;"
                " print(get_python_lib())')" %
                cmd_line(self._python_bin())])
            # un-tar mrjob.tar.gz
            mrjob_bootstrap.append(
                ['sudo tar xfz ', path_dict, ' -C $__mrjob_PYTHON_LIB'])
            # re-compile pyc files now, since mappers/reducers can't
            # write to this directory. Don't fail if there is extra
            # un-compileable crud in the tarball (this would matter if
            # sh_bin were 'sh -e')
            mrjob_bootstrap.append(
                ['sudo %s -m compileall -f $__mrjob_PYTHON_LIB/mrjob && true' %
                 cmd_line(self._python_bin())])

        # we call the script b.py because there's a character limit on
        # bootstrap script names (or there was at one time, anyway)
        path = os.path.join(self._get_local_tmp_dir(), 'b.py')
        log.info('writing master bootstrap script to %s' % path)

        contents = self._master_bootstrap_script_content(self._bootstrap + mrjob_bootstrap)
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

        # Python 2 is already installed; install pip and ujson

        # (We also install python-pip for bootstrap_python_packages,
        # but there's no harm in running these commands twice, and
        # bootstrap_python_packages is deprecated anyway.)

        # NOTE - mtai @ davidmarin - Dataproc uses apt-get, ujson needs python-dev installed
        return [
            ['sudo apt-get install -y python-pip python-dev'],
            ['sudo pip install --upgrade ujson'],
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
        sh_bin = self._opts['sh_bin']
        if not sh_bin[0].startswith('/'):
            sh_bin = ['/usr/bin/env'] + sh_bin
        writeln('#!' + cmd_line(sh_bin))
        writeln()

        # store $PWD
        writeln('# store $PWD')
        writeln('__mrjob_PWD=$PWD')

        # TODO - mtai @ davidmarin - begin section, investigate why mtai needed to add this
        writeln('if [ $__mrjob_PWD = "/" ]; then')
        writeln('  __mrjob_PWD=""')
        writeln('fi')
        # TODO - mtai @ davidmarin - end section

        writeln()

        # download files
        writeln('# download files and mark them executable')

        cp_to_local = 'hadoop fs -copyToLocal'

        for name, path in sorted(
                self._bootstrap_dir_mgr.name_to_path('file').items()):
            uri = self._upload_mgr.uri(path)

            output_string = '%s %s $__mrjob_PWD/%s' % (cp_to_local, pipes.quote(uri), pipes.quote(name))

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

    ### Dataproc-specific Stuff ###
    #

    def _get_cluster(self):
        return self.dataproc_client.clusters().get(
            projectId=self._gcp_project,
            region=self._gcp_region,
            clusterName=self._gcp_cluster
        ).execute()

    def _create_cluster(self):
        # https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.clusters/create
        # https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.clusters/get

        # TODO - documentation error, friendly error message when cluster_name has caps
        # clusterName can't have caps, limited to alphanumeric, up to 54 characters
        # clusterName must be a match of regex '(?:[a-z](?:[-a-z0-9]{0,53}[a-z0-9])?).

        # TODO - very odd to use / notation
        # gcloud config set core/project google.com:pm-hackathon

        gce_num_instances = int(self._opts['gce_num_instances'])
        assert gce_num_instances >= 2
        bootstrap_cmds = [self._upload_mgr.uri(self._master_bootstrap_script_path)]

        # Change machine type
        zone_uri = 'https://www.googleapis.com/compute/v1/projects/%(project)s/zones/%(zone)s' % dict(project=self._gcp_project, zone=self._gcp_zone)
        machine_uri = "%(zone_uri)s/machineTypes/%(machine_type)s" % dict(zone_uri=zone_uri, machine_type=self._opts['gce_machine_type'])

        # Task tracker
        master_instance_group_config = {
            "numInstances": 1,
            "machineTypeUri": machine_uri,
            "isPreemptible": False,
        }

        # Compute + storage
        worker_instance_group_config = {
            "numInstances": gce_num_instances,
            "machineTypeUri": machine_uri,
            "isPreemptible": False,
        }

        # TODO - mtai @ davidmarin - Add support for secondary worker configs
        # Compute ONLY
        secondary_worker_instance_group_config = {
            "numInstances": 0,
            "machineTypeUri": machine_uri,
            "isPreemptible": True,
        }

        cluster_data = {
            'projectId': self._gcp_project,
            'clusterName': self._gcp_cluster,
            'config': {
                'gceClusterConfig': {
                    'zoneUri': zone_uri
                },
                'masterConfig': master_instance_group_config,
                'workerConfig': worker_instance_group_config,
                # 'secondaryWorkerConfig': secondary_worker_instance_group_config,
                'initializationActions': [
                    dict(executableFile=bootstrap_cmd) for bootstrap_cmd in bootstrap_cmds
                ]
            }
        }

        self.dataproc_client.clusters().create(
            projectId=self._gcp_project,
            region=self._gcp_region,
            body=cluster_data).execute()
        cluster_state = None

        # TODO - mtai @ davidmarin - See https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.clusters#State
        # TODO - mtai @ davidmarin - We should probably pull these strings to constants

        # TODO - mtai @ davidmarin - Can end up in infinite loop if cluster_state doesn't end up in (RUNNING,ERROR)
        while bool(cluster_state != 'RUNNING'):
            result_describe =  self.dataproc_client.clusters().get(projectId=self._gcp_project,
                                               region=self._gcp_region,
                                               clusterName=self._gcp_cluster).execute()
    
            cluster_state = result_describe['status']['state']
            if cluster_state == 'ERROR':
                raise Exception(result_describe)
    
            log.info('Waiting for cluster to launch... sleeping 5 second(s)')
            time.sleep(5.0)

        # TODO - mtai @ davidmarin - We should probably do something nicer here...
        assert cluster_state == 'RUNNING'
        return self._gcp_project
    
    def _delete_cluster(self):
        return self.dataproc_client.clusters().delete(
            projectId=self._gcp_project,
            region=self._gcp_region,
            clusterName=self._gcp_cluster
        ).execute()

    def _get_job(self, job_id):
        return self.dataproc_client.jobs().get(
            projectId=self._gcp_project,
            region=self._gcp_region,
            jobId=job_id
        ).execute()
    
    def _cancel_job(self, job_id):
        return self.dataproc_client.jobs().cancel(
            projectId=self._gcp_project,
            region=self._gcp_region,
            jobId=job_id
        ).execute()

    def _delete_job(self, job_id):
        return self.dataproc_client.jobs().delete(
            projectId=self._gcp_project,
            region=self._gcp_region,
            jobId=job_id
        ).execute()

    def _submit_hadoop_job(self, step_name, hadoop_job):
        # https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs/submit
        # https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs#HadoopJob
        # https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs#JobReference
        job_data = {
            "job": {
                "reference": {
                    "projectId": self._gcp_project,
                    "jobId": step_name,
                },
                "placement": {
                    "clusterName": self._gcp_cluster
                },
                "hadoopJob": hadoop_job
            }
        }

        result = self.dataproc_client.jobs().submit(
            projectId=self._gcp_project,
            region=self._gcp_region,
            body=job_data).execute()

        # TODO - mtai @ davidmarin - Add error checking in case job submission fails
        # TODO - mtai @ davidmarin, since step_name SHOULD equal job_id, perhaps remove this return statement
        job_id = result['reference']['jobId']
        return job_id
