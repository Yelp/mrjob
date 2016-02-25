# -*- coding: utf-8 -*-
# Copyright 2009-2016 Yelp and Contributors
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


try:
    from oauth2client.client import GoogleCredentials
    from googleapiclient import discovery
except ImportError:
    # don't require boto; MRJobs don't actually need it when running
    # inside hadoop streaming
    GoogleCredentials = None
    discovery = None

try:
    import filechunkio
except ImportError:
    # that's cool; filechunkio is only for multipart uploading
    filechunkio = None

import mrjob
import mrjob.step

from mrjob.conf import combine_cmds
from mrjob.conf import combine_dicts
from mrjob.conf import combine_lists
from mrjob.conf import combine_path_lists
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
_HADOOP_STREAMING_JAR = "/usr/lib/hadoop-mapreduce/hadoop-streaming.jar"

_DEFAULT_GCS_BUCKET = 'boulder-input-data'

class DataprocRunnerOptionStore(RunnerOptionStore):

    # documentation of these options is in docs/guides/emr-opts.rst
    #     cluster_name = create_cluster(project=project, region=region, cluster_name=cluster_name, zone=zone)

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

        # don't allow aws_region to be ''
        if not self['gcp_region']:
            self['gcp_region'] = _DEFAULT_GCP_REGION

    def default_options(self):
        super_opts = super(DataprocRunnerOptionStore, self).default_options()
        return combine_dicts(super_opts, {
            'bootstrap_python': True,
            'check_api_status_every': 30,

            'gce_machine_type': 'n1-standard-1',
            'gce_num_instances': 2,

            'gcs_sync_wait_time': 5.0,
            'sh_bin': ['/bin/sh', '-ex'],
            'ssh_bin': ['ssh'],
            'ssh_bind_ports': list(range(40001, 40841)),
            'ssh_tunnel': False,
            'ssh_tunnel_is_open': False,
            'visible_to_all_users': True,
        })

class DataprocJobRunner(MRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` on Amazon Elastic MapReduce.
    Invoked when you run your job with ``-r emr``.

    :py:class:`DataprocJobRunner` runs your job in an EMR cluster, which is
    basically a temporary Hadoop cluster. Normally, it creates a cluster
    just for your job; it's also possible to run your job in a specific
    cluster by setting *gcp_cluster* or to automatically choose a
    waiting cluster, creating one if none exists, by setting
    *pool_clusters*.

    Input, support, and jar files can be either local or on GCS; use
    ``gs://...`` URLs to refer to files on GCS.

    This class has some useful utilities for talking directly to GCS and EMR,
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
        """:py:class:`~mrjob.emr.DataprocJobRunner` takes the same arguments as
        :py:class:`~mrjob.runner.MRJobRunner`, plus some additional options
        which can be defaulted in :ref:`mrjob.conf <mrjob.conf>`.

        *aws_access_key_id* and *aws_secret_access_key* are required if you
        haven't set them up already for boto (e.g. by setting the environment
        variables :envvar:`AWS_ACCESS_KEY_ID` and
        :envvar:`AWS_SECRET_ACCESS_KEY`)

        A lengthy list of additional options can be found in
        :doc:`guides/emr-opts.rst`.
        """
        super(DataprocJobRunner, self).__init__(**kwargs)

        # if we're going to create a bucket to use as temp space, we don't
        # want to actually create it until we run the job (Issue #50).
        # This variable helps us create the bucket as needed

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

        # the ID assigned by EMR to this job (might be None)
        # self._gcp_project = self._opts['gcp_project']
        # self._gcp_region = self._opts['gcp_region']
        # self._gcp_zone = self._opts['gcp_zone']
        # self._gcp_cluster = self._opts['gcp_cluster']
        self._gcp_project = "google.com:pm-hackathon"
        self._gcp_region = "global"
        self._gcp_zone = "us-central1-b"
        self._gcp_cluster = "vbp04"

        self._dataproc_client = None

        # when did our particular task start?
        self._emr_job_start = None

        # List of dicts (one for each step) potentially containing
        # the keys 'history', 'step', and 'task'. These will also always
        # contain 'step_id' (the s-XXXXXXXX step ID on EMR).
        #
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

        if self._opts['hadoop_streaming_jar']:
            self._upload_mgr.add(self._opts['hadoop_streaming_jar'])

        for step in self._get_steps():
            if step.get('jar'):
                self._upload_mgr.add(step['jar'])

    def _upload_local_files_to_gcs(self):
        """Copy local files tracked by self._upload_mgr to GCS."""
        log.info('Copying non-input files into %s' % self._upload_mgr.prefix)

        for path, gcs_uri in self._upload_mgr.path_to_uri().items():
            log.debug('uploading %s -> %s' % (path, gcs_uri))

            self.fs.upload(path, gcs_uri)

    ### Running the job ###

    def cleanup(self, mode=None):
        super(DataprocJobRunner, self).cleanup(mode=mode)


    def _cleanup_remote_tmp(self):
        # delete all the files we created
        if self._gcs_tmp_dir:
            try:
                log.info('Removing all files in %s' % self._gcs_tmp_dir)
                self.fs.rm(self._gcs_tmp_dir)
                self._gcs_tmp_dir = None
            except Exception as e:
                log.exception(e)

    def _cleanup_logs(self):
        super(DataprocJobRunner, self)._cleanup_logs()

    def _cleanup_job(self):
        cancel_job()
        # kill the job if we won't be taking down the whole cluster
        if not self._gcp_cluster:
            # Nothing we can do.
            return

        if not (self._opts['gcp_cluster'] or
                self._opts['pool_clusters']):
            # we're taking down the cluster, don't bother
            return



    def _cleanup_cluster(self):
        if not self._gcp_cluster:
            # If we don't have a cluster, then we can't terminate it.
            return
        try:
            log.info("Attempting to terminate cluster")
            delete_cluster(project, self._gcp_cluster)
        except Exception as e:
            log.exception(e)
            return
        log.info('cluster %s successfully terminated' % self._gcp_cluster)

    def _wait_for_s3_eventual_consistency(self):
        """Sleep for a little while, to give GCS a chance to sync up.
        """
        log.info('Waiting %.1fs for GCS eventual consistency' %
                 self._opts['gcs_sync_wait_time'])
        time.sleep(self._opts['gcs_sync_wait_time'])


    def _build_steps(self):
        """Return a list of boto Step objects corresponding to the
        steps we want to run."""
        # quick, add the other steps before the job spins up and
        # then shuts itself down (in practice this takes several minutes)
        return [self._build_step(n) for n in xrange(self._num_steps())]

    def _build_step(self, step_num):
        step = self._get_step(step_num)

        if step['type'] == 'streaming':
            return self._build_streaming_step(step_num)
        elif step['type'] == 'jar':
            return self._build_jar_step(step_num)
        else:
            raise AssertionError('Bad step type: %r' % (step['type'],))

    def _build_streaming_step(self, step_num):
        jar, step_arg_prefix = self._get_streaming_jar_and_step_arg_prefix()

        streaming_step_kwargs = dict(
            input=self._step_input_uris(step_num),
            jar=jar,
            name='%s: Step %d of %d' % (
                self._job_key, step_num + 1, self._num_steps()),
            output=self._step_output_uri(step_num),
        )

        step_args = []
        step_args.extend(step_arg_prefix)  # add 'hadoop-streaming' for 4.x

        step_args.extend(self._upload_args(self._upload_mgr))
        step_args.extend(self._hadoop_args_for_step(step_num))

        streaming_step_kwargs['step_args'] = step_args

        mapper, combiner, reducer = (
            self._hadoop_streaming_commands(step_num))

        streaming_step_kwargs['mapper'] = mapper
        streaming_step_kwargs['combiner'] = combiner
        streaming_step_kwargs['reducer'] = reducer

        return streaming_step_kwargs

    def _build_jar_step(self, step_num):
        raise NotImplementedError

    def _get_streaming_jar_and_step_arg_prefix(self):
        return _HADOOP_STREAMING_JAR, []

    def _launch_dataproc_cluster(self):
        """Create an empty cluster on EMR, and set self._gcp_cluster to
        its ID."""
        self._wait_for_s3_eventual_consistency()

        self._emr_cluster_start = time.time()

        # make sure we can see the files we copied to GCS

        log.info('Creating Dataproc Hadoop cluster')

        try:
            self._get_cluster()
            log.info('Adding our job to existing cluster %s' %
                     self._gcp_cluster)
        except:
            self._create_cluster()
            log.info('Created new cluster %s' %
                     self._gcp_cluster)

        # keep track of when we launched our job
        self._emr_job_start = time.time()

    def _wait_for_steps_to_complete(self):
        """Wait for every step of the job to complete, one by one."""

        # define out steps
        steps = self._build_steps()
        for current_step in steps:
            step_args = []
            step_args += current_step['step_args']
            step_args += ['-mapper', current_step['mapper']]
            step_args += ['-combiner', current_step['combiner']]
            step_args += ['-reducer', current_step['reducer']]

            for current_input_uri in current_step['input']:
                step_args += ['-input', current_input_uri]

            step_args += ['-output', current_step['output']]

            # def _submit_hadoop_job(self, args=None, file_uris=None, archive_uris=None, properties=None):
            submit_result = self._submit_hadoop_job(args=step_args)
            job_id = submit_result['reference']['jobId']

            self._wait_for_job_to_complete(job_id)

    def _wait_for_job_to_complete(self, job_id):
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
        log_interpretation = dict(job_id=job_id)
        self._log_interpretations.append(log_interpretation)

        while True:
            # don't antagonize EMR's throttling
            log.debug('Waiting %.1f seconds...' %
                      self._opts['check_api_status_every'])
            time.sleep(self._opts['check_api_status_every'])

            job_result = self._get_job(job_id)

            job_start_time = job_result['status']['stateStartTime']
            job_state = job_result['status']['state']

            # https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs#JobStatus
            log.info('  %s - %s' % (job_start_time, job_state))

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

        # don't bother if we're not starting a cluster
        if self._opts['gcp_cluster']:
            return

        # Also don't bother if we're not bootstrapping
        if not (self._bootstrap or
                self._bootstrap_mrjob()):
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

        writeln('if [ $__mrjob_PWD = "/" ]; then')
        writeln('  __mrjob_PWD=""')
        writeln('fi')

        writeln()

        # download files
        writeln('# download files and mark them executable')

        cp_to_local = 'hadoop fs -copyToLocal'

        for name, path in sorted(
                self._bootstrap_dir_mgr.name_to_path('file').items()):
            uri = self._upload_mgr.uri(path)

            # output_string = '%s %s $__mrjob_PWD/%s' % (cp_to_local, pipes.quote(uri), pipes.quote(name))
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

    ### EMR-specific Stuff ###
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

        # TODO - How does gcloud self-update
    
        # TODO - documentation error, friendly error message when cluster_name has caps
        # clusterName must be a match of regex '(?:[a-z](?:[-a-z0-9]{0,53}[a-z0-9])?).
        # clusterName can't have caps, limited to alphanumeric, up to 54 characters
    
        # TODO - documentation error, region MUST be set to "global" not "[global]"
        # region - error message suggests region "[global]" when it means "global"
    
        # TODO - documentation error, workerConfig can be set to 1 worker and 1 master via API
        # TODO - UI mismatch, 2 worker minimum
        # masterConfig - not clear how to configure a simple node
        # workerConfig - not clear how to configure a simple node
        # secondaryWorkerConfig - not clear how to configure a simple node
    
        # TODO - very odd to use / notation
        # gcloud config set core/project google.com:pm-hackathon
    
        # git config uses dot-notation for setting INI-style files
        # not sure if any other config system uses dot-notation

        gce_num_instances = int(self._opts['gce_num_instances'])
        assert gce_num_instances >= 2
        bootstrap_cmds = [self._upload_mgr.uri(self._master_bootstrap_script_path)]

        # Change CPU type
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

        cluster_state = None
        api_clusters = self.dataproc_client.clusters()
        try:
            result_describe = api_clusters.get(projectId=self._gcp_project,
                             region=self._gcp_region,
                             clusterName=self._gcp_cluster).execute()

            cluster_state = result_describe['status']['state']
        # TODO - Chang Exception to HttpError and check 404
        except Exception, e:
            api_clusters.create(
                projectId=self._gcp_project,
                region=self._gcp_region,
                body=cluster_data).execute()

        # TODO - documentation error, cluster states are not clearly defined
        # TODO - documentation error, we should directly show results returned from "gcloud dataproc clusters describe test-test-test"
        while bool(cluster_state != 'RUNNING'):
            result_describe = api_clusters.get(projectId=self._gcp_project,
                                               region=self._gcp_region,
                                               clusterName=self._gcp_cluster).execute()
    
            cluster_state = result_describe['status']['state']
            if cluster_state == 'ERROR':
                raise Exception(result_describe)
    
            log.info('Waiting for cluster to launch... sleeping 5 second(s)')
            time.sleep(5)
    
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

    def _submit_hadoop_job(self, args=None, file_uris=None, archive_uris=None, properties=None):
        # https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs#HadoopJob
        args = args or list()
        file_uris = file_uris or list()
        archive_uris = archive_uris or list()
        properties = properties or dict()

        # TODO - extract file_uris from args
        # TODO - extract archive_uris from args
        # TODO - extract properties from args
        #
        # here are my configs for an EMR Jar step
        #             "Jar": "/home/hadoop/contrib/streaming/hadoop-streaming.jar",
        #            "Properties": {},
        #            "Args": [
        #                "-files",
        #                "gs://mrjob-35cdec11663cb1cb/tmp/mr_wc.davidmarin.20160223.010040.063848/files/mr_wc.py#mr_wc.py",
        #                "-mapper",
        #                "python3 mr_wc.py --step-num=0 --mapper",
        #                "-reducer",
        #                "python3 mr_wc.py --step-num=0 --reducer",
        #                "-input",
        #                "gs://mrjob-35cdec11663cb1cb/tmp/mr_wc.davidmarin.20160223.010040.063848/files/Makefile",
        #                "-output",
        #                "gs://mrjob-35cdec11663cb1cb/tmp/mr_wc.davidmarin.20160223.010040.063848/output/"
        #            ]

        # hadoop jar -files gs://bucket/path/to/mr_script.py#mr_script.py -mapper "python mr_script.py --step-num=0 --mapper" -reducer "python mr_script.py --step-num=0 --reducer" -input "gs://bucket/path/to/input" -output "gs://bucket/path/to/output_dir/"
        job_data = {
            "projectId": self._gcp_project,
            "job": {
                "placement": {
                    "clusterName": self._gcp_cluster
                },
                "hadoopJob": {
                    "args": args,
                    "fileUris": file_uris,
                    "archiveUris": archive_uris,
                    "properties": properties,
                    "mainJarFileUri": 'file://%s' % _HADOOP_STREAMING_JAR
                }
            }
        }
        result = self.dataproc_client.jobs().submit(
            projectId=self._gcp_project,
            region=self._gcp_region,
            body=job_data).execute()
        # Print the Job ID
        print(result['reference']['jobId'])
        return result


if __name__ == '__main__':
    credentials = GoogleCredentials.get_application_default()
    dataproc = discovery.build('dataproc', 'v1', credentials=credentials)


    # TODO - pull this from args
    project = 'google.com:pm-hackathon'
    region = 'global'
    cluster_name = 'vbp03'
    zone = 'us-central1-b'


    resp =  dataproc.projects().regions().clusters().get(
        projectId=project,
        region=region,
        clusterName='bla',
    )
    
    answer = resp.execute()

    print "hello"
    #
    #
    # # TODO - Is this documentation out of date?
    # project = 'google.com:pm-hackathon'
    # region = 'global'
    # cluster_name = 'test-test-test'
    # zone = 'us-central1-b'
    #
    # machine_worker_count = 1
    # machine_type = 'n1-standard-1'
    # assert result['done']
    #
    # import pprint
    # pprint.pprint(result['metadata'])