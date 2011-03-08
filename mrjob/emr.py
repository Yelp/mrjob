# Copyright 2009-2011 Yelp and Contributors
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

from cStringIO import StringIO
import datetime
import fnmatch
import logging
import os
import posixpath
import random
import re
import signal
import socket
from subprocess import Popen, PIPE
import time
import urllib2

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

try:
    import boto
    import boto.ec2
    import boto.exception
    import boto.utils
    from mrjob import botoemr
except ImportError:
    # don't require boto; MRJobs don't actually need it when running
    # inside hadoop streaming
    boto = None
    botoemr = None

from mrjob.conf import combine_dicts, combine_lists, combine_paths, combine_path_lists
from mrjob.parse import find_python_traceback, find_hadoop_java_stack_trace, find_input_uri_for_mapper, find_interesting_hadoop_streaming_error
from mrjob.retry import RetryWrapper
from mrjob.runner import MRJobRunner, GLOB_RE
from mrjob.util import cmd_line


log = logging.getLogger('mrjob.emr')

S3_URI_RE = re.compile(r'^s3://([A-Za-z0-9-\.]+)/(.*)$')
JOB_TRACKER_RE = re.compile('(\d{1,3}\.\d{2})%')

# if EMR throttles us, how long to wait (in seconds) before trying again?
EMR_BACKOFF = 20
EMR_BACKOFF_MULTIPLIER = 1.5
EMR_MAX_TRIES = 20 # this takes about a day before we run out of tries

# the port to tunnel to
EMR_JOB_TRACKER_PORT = 9100
EMR_JOB_TRACKER_PATH = '/jobtracker.jsp'

MAX_SSH_RETRIES = 20

# ssh should fail right away if it can't bind a port
WAIT_FOR_SSH_TO_FAIL = 1.0

# regex for matching task-attempts log URIs
TASK_ATTEMPTS_LOG_URI_RE = re.compile(r'^.*/task-attempts/attempt_(?P<timestamp>\d+)_(?P<step_num>\d+)_(?P<node_type>m|r)_(?P<node_num>\d+)_(?P<attempt_num>\d+)/(?P<stream>stderr|syslog)$')

# regex for matching step log URIs
STEP_LOG_URI_RE = re.compile(r'^.*/steps/(?P<step_num>\d+)/syslog$')

# map from AWS region to EMR endpoint
# see http://docs.amazonwebservices.com/ElasticMapReduce/latest/DeveloperGuide/index.html?ConceptsRequestEndpoints.html
REGION_TO_EMR_ENDPOINT = {
    'EU': 'eu-west-1.elasticmapreduce.amazonaws.com',
    'us-east-1': 'us-east-1.elasticmapreduce.amazonaws.com',
    'us-west-1': 'us-west-1.elasticmapreduce.amazonaws.com',
    '': 'elasticmapreduce.amazonaws.com', # when no region specified
}

# map from AWS region to S3 endpoint
# see http://docs.amazonwebservices.com/AmazonS3/latest/dev/index.html?RequestEndpoints.html
REGION_TO_S3_ENDPOINT = {
    'EU': 's3-eu-west-1.amazonaws.com',
    'us-east-1': 's3.amazonaws.com', # no region-specific endpoint
    'us-west-1': 's3-us-west-1.amazonaws.com',
    'ap-southeast-1': 's3-ap-southeast-1.amazonaws.com', # no EMR endpoint yet
    '': 's3.amazonaws.com',
}


def parse_s3_uri(uri):
    """Parse an S3 URI into (bucket, key)

    >>> parse_s3_uri('s3://walrus/tmp/')
    ('walrus', 'tmp/')

    If ``uri`` is not an S3 URI, raise a ValueError
    """
    match = S3_URI_RE.match(uri)
    if match:
        return match.groups()
    else:
        raise ValueError('Invalid S3 URI: %s' % uri)


def s3_key_to_uri(s3_key):
    """Convert a boto Key object into an ``s3://`` URI"""
    return 's3://%s/%s' % (s3_key.bucket.name, s3_key.name)


def _to_timestamp(iso8601_time):
    return time.mktime(time.strptime(iso8601_time, boto.utils.ISO8601))


def _to_datetime(iso8601_time):
    return datetime.datetime.strptime(iso8601_time, boto.utils.ISO8601)


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

    while True:
        if created_before and created_after and created_before < created_after:
            break

        log.debug('Calling describe_jobflows(states=%r, jobflow_ids=%r, created_after=%r, created_before=%r)' % (states, jobflow_ids, created_after, created_before))
        try:
            results = emr_conn.describe_jobflows(
                states=states, jobflow_ids=jobflow_ids,
                created_after=created_after, created_before=created_before)
        except boto.exception.BotoServerError, ex:
            if 'ValidationError' in ex.body:
                log.debug('  reached earliest allowed created_before time, done!')
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
            min_create_time = min(_to_datetime(jf.creationdatetime)
                                  for jf in job_flows)
            created_before = min_create_time + datetime.timedelta(seconds=1)
            # if someone managed to start 501 job flows in the same second,
            # they are still screwed (the EMR API only returns up to 500),
            # but this seems unlikely. :)
        else:
            if not created_before:
                created_before = datetime.datetime.utcnow()
            created_before -= datetime.timedelta(weeks=2)

    return all_job_flows


class EMRJobRunner(MRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` on Amazon Elastic MapReduce.

    :py:class:`EMRJobRunner` runs your job in an EMR job flow, which is
    basically a temporary Hadoop cluster. Normally, it creates a job flow
    just for your job; it's also possible to run your job in an existing
    job flow by setting *emr_job_flow_id* (or :option:`--emr-job-flow-id`).

    Input and support files can be either local or on S3; use ``s3://...``
    URLs to refer to files on S3.

    This class has some useful utilities for talking directly to S3 and EMR,
    so you may find it useful to instantiate it without a script::

        from mrjob.emr import EMRJobRunner

        emr_conn = EMRJobRunner().make_emr_conn()
        job_flows = emr_conn.describe_jobflows()
        ...

    See also: :py:meth:`EMRJobRunner.__init__`.
    """
    alias = 'emr'

    def __init__(self, **kwargs):
        """:py:class:`EMRJobRunner` takes the same arguments as
        :py:class:`~mrjob.runner.MRJobRunner`, plus some additional options
        which can be defaulted in :py:mod:`mrjob.conf`.

        *aws_access_key_id* and *aws_secret_access_key* are required if you
        haven't set them up already for boto (e.g. by setting the environment
        variables :envvar:`AWS_ACCESS_KEY_ID` and
        :envvar:`AWS_SECRET_ACCESS_KEY`)

        Additional options:

        :type aws_access_key_id: str
        :param aws_access_key_id: "username" for Amazon web services.
        :type aws_secret_access_key: str
        :param aws_secret_access_key: your "password" on AWS
        :type aws_region: str
        :param aws_region: region to connect to S3 and EMR on (e.g. ``us-west-1``). If you want to use separate regions for S3 and EMR, set *emr_endpoint* and *s3_endpoint*.
        :type bootstrap_cmds: list
        :param bootstrap_cmds: a list of commands to run on the master node to set up libraries, etc. Like *setup_cmds*, these can be strings, which will be run in the shell, or lists of args, which will be run directly. Prepend ``sudo`` to commands to do things that require root privileges.
        :type bootstrap_files: list of str
        :param bootstrap_files: files to upload to the master node before running *bootstrap_cmds* (for example, debian packages). These will be made public on S3 due to a limitation of the bootstrap feature.
        :type bootstrap_mrjob: boolean
        :param bootstrap_mrjob: This is actually an option in the base MRJobRunner class. If this is ``True`` (the default), we'll tar up :mod:`mrjob` from the local filesystem, and install it on the master node.
        :type bootstrap_python_packages: list of str
        :param bootstrap_python_packages: paths of python modules to install on EMR. These should be standard python module tarballs. If a module is named ``foo.tar.gz``, we expect to be able to run ``tar xfz foo.tar.gz; cd foo; sudo python setup.py install``.
        :type bootstrap_scripts: list of str
        :param bootstrap_scripts: scripts to upload and then run on the master node (a combination of *bootstrap_cmds* and *bootstrap_files*). These are run after the command from bootstrap_cmds.
        :type check_emr_status_every: float
        :param check_emr_status_every: How often to check on the status of EMR jobs.Default is 30 seconds (too often and AWS will throttle you).
        :type ec2_instance_type: str
        :param ec2_instance_type: what sort of EC2 instance(s) to use (see http://aws.amazon.com/ec2/instance-types/). Default is ``m1.small``
        :type ec2_key_pair: str
        :param ec2_key_pair: name of the SSH key you set up for EMR.
        :type ec2_key_pair_file: str
        :param ec2_key_pair_file: path to file containing the SSH key for EMR
        :type ec2_master_instance_type: str
        :param ec2_master_instance_type: same as *ec2_instance_type*, but only for the master Hadoop node
        :type ec2_slave_instance_type: str
        :param ec2_slave_instance_type: same as *ec2_instance_type*, but only for the slave Hadoop nodes
        :type emr_endpoint: str
        :param emr_endpoint: optional host to connect to when communicating with S3 (e.g. ``us-west-1.elasticmapreduce.amazonaws.com``). Default is to infer this from *aws_region*.
        :type emr_job_flow_id: str
        :param emr_job_flow_id: the ID of a persistent EMR job flow to run jobs in (normally we launch our own job). It's fine for other jobs to be using the job flow; we give our job's steps a unique ID.
        :type num_ec2_instances: int
        :param num_ec2_instances: number of instances to start up. Default is ``1``.
        :type s3_endpoint: str
        :param s3_endpoint: Host to connect to when communicating with S3 (e.g. ``s3-us-west-1.amazonaws.com``). Default is to infer this from *aws_region*.
        :type s3_log_uri: str
        :param s3_log_uri:  where on S3 to put logs, for example ``s3://yourbucket/logs/``. Logs for your job flow will go into a subdirectory, e.g. ``s3://yourbucket/logs/j-JOBFLOWID/``. in this example s3://yourbucket/logs/j-YOURJOBID/). Default is to append ``logs/`` to *s3_scratch_uri*.
        :type s3_scratch_uri: str
        :param s3_scratch_uri: S3 directory (URI ending in ``/``) to use as scratch space, e.g. ``s3://yourbucket/tmp/``. Default is ``tmp/mrjob/`` in the first bucket belonging to you.
        :type ssh_bin: str
        :param ssh_bin: path to the ssh binary. Defaults to ``ssh``
        :type ssh_bind_ports: list of int
        :param ssh_bind_ports: a list of ports that are safe to listen on. Defaults to ports ``40001`` thru ``40840``.
        :type ssh_tunnel_to_job_tracker: bool
        :param ssh_tunnel_to_job_tracker: If True, create an ssh tunnel to the job tracker and listen on a randomly chosen port. This requires you to set *ec2_key_pair* and *ec2_key_pair_file*.
        :type ssh_tunnel_is_open: bool
        :param ssh_tunnel_is_open: if True, any host can connect to the job tracker through the SSH tunnel you open. Mostly useful if your browser is running on a different machine from your job.
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

        # add the bootstrap files to a list of files to upload
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
                raise ValueError('bootstrap_python_packages only accepts .tar.gz files!')
            file_dict = self._add_bootstrap_file(path)
            self._bootstrap_python_packages.append(file_dict)

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

        # cache for _download_log_file()
        self._uri_of_downloaded_log_file = None

        # store the tracker URL for completion status
        self._tracker_url = None

        # turn off tracker progress until tunnel is up
        self._show_tracker_progress = False

    @classmethod
    def _allowed_opts(cls):
        """A list of which keyword args we can pass to __init__()"""
        return super(EMRJobRunner, cls)._allowed_opts() + [
            'aws_access_key_id', 'aws_secret_access_key', 'aws_region',
            'bootstrap_cmds', 'bootstrap_files', 'bootstrap_python_packages',
            'bootstrap_scripts', 'check_emr_status_every', 'ec2_instance_type',
            'ec2_key_pair', 'ec2_key_pair_file', 'ec2_master_instance_type',
            'ec2_slave_instance_type', 'emr_endpoint', 'emr_job_flow_id',
            'num_ec2_instances', 's3_endpoint', 's3_log_uri',
            's3_scratch_uri', 's3_sync_wait_time', 'ssh_bin',
            'ssh_bind_ports', 'ssh_tunnel_is_open',
            'ssh_tunnel_to_job_tracker']

    @classmethod
    def _default_opts(cls):
        """A dictionary giving the default value of options."""
        return combine_dicts(super(EMRJobRunner, cls)._default_opts(), {
            'check_emr_status_every': 30,
            'ec2_instance_type': 'm1.small',
            'num_ec2_instances': 1,
            's3_sync_wait_time': 5.0,
            'ssh_bin': 'ssh',
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
            'bootstrap_cmds': combine_lists,
            'bootstrap_files': combine_path_lists,
            'bootstrap_python_packages': combine_path_lists,
            'bootstrap_scripts': combine_path_lists,
            'ec2_key_pair_file': combine_paths,
            's3_log_uri': combine_paths,
            's3_scratch_uri': combine_paths,
            'ssh_bin': combine_paths,
        })

    def _fix_s3_scratch_and_log_uri_opts(self):
        """Fill in s3_scratch_uri and s3_log_uri (in self._opts) if they
        aren't already set.
        """
        # set s3_scratch_uri
        if not self._opts['s3_scratch_uri']:
            s3_conn = self.make_s3_conn()
            buckets = s3_conn.get_all_buckets()
            mrjob_buckets = [b for b in buckets if b.name.startswith('mrjob-')]
            if mrjob_buckets:
                scratch_bucket = mrjob_buckets[0]
                scratch_bucket_name = scratch_bucket.name
                # if we're not using an ancient version of boto, set region
                # based on the bucket's region
                if (hasattr(scratch_bucket, 'get_location')):
                    self._aws_region = scratch_bucket.get_location() or ''
                    if self._aws_region:
                        log.info("using scratch bucket's region (%s) to connect to AWS" %
                                 self._aws_region)
            else:
                # We'll need to create a bucket if and when we need to use
                # scratch space.
                scratch_bucket_name = 'mrjob-%016x' % random.randint(0, 2**64-1)
                self._s3_temp_bucket_to_create = scratch_bucket_name

            self._opts['s3_scratch_uri'] = 's3://%s/tmp/' % scratch_bucket_name
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

    def _create_s3_temp_bucket_if_needed(self):
        if self._s3_temp_bucket_to_create:
            s3_conn = self.make_s3_conn()
            log.info('creating S3 bucket %r to use as scratch space' %
                     self._s3_temp_bucket_to_create)
            s3_conn.create_bucket(self._s3_temp_bucket_to_create,
                                  location=(self._aws_region or ''))
            self._s3_temp_bucket_to_create = None

    def _check_and_fix_s3_dir(self, s3_uri):
        if not S3_URI_RE.match(s3_uri):
            raise ValueError('Invalid S3 URI: %r' % s3_uri)
        if not s3_uri.endswith('/'):
            s3_uri = s3_uri + '/'

        return s3_uri

    def _add_bootstrap_file(self, path):
        name, path = self._split_path(path)
        file_dict = {'path': path, 'name': name, 'bootstrap': 'file'}
        self._files.append(file_dict)
        return file_dict

    def _run(self):
        self._setup_input()
        self._create_wrapper_script()
        self._create_master_bootstrap_script()
        self._upload_non_input_files()

        self._launch_emr_job()
        self._wait_for_job_to_complete()

    def _setup_input(self):
        """Copy local input files (if any) to a special directory on S3.

        Set self._s3_input_uris
        """
        self._create_s3_temp_bucket_if_needed()
        # winnow out s3 files from local ones
        self._s3_input_uris = []
        local_input_paths = []
        for path in self._input_paths:
            if S3_URI_RE.match(path):
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

    def _setup_output(self):
        """Set self._output_dir if it's not set already."""
        if not self._output_dir:
            self._output_dir = self._s3_tmp_uri + 'output/'
        log.info('Job will output -> %s' % self._output_dir)

    def _pick_s3_uris_for_files(self):
        """Decide where each file will be uploaded on S3.

        Okay to call this multiple times.
        """
        self._assign_unique_names_to_files(
            's3_uri', prefix=self._s3_tmp_uri + 'files/', match=S3_URI_RE.match)

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
            if S3_URI_RE.match(path):
                continue

            s3_uri = file_dict['s3_uri']

            log.debug('uploading %s -> %s' % (path, s3_uri))
            s3_key = self.make_s3_key(s3_uri, s3_conn)
            s3_key.set_contents_from_filename(file_dict['path'])
            if file_dict.get('bootstrap'):
                s3_key.make_public()

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
                    log.warning("You must set %s in order to ssh to the job tracker!" % opt_name)
                    self._gave_cant_ssh_warning = True
                return

        # if there was already a tunnel, make sure it's still up
        if self._ssh_proc:
            self._ssh_proc.poll()
            if self._ssh_proc.returncode is None:
                return
            else:
                log.warning('Oops, ssh subprocess exited with return code %d, restarting...' % self._ssh_proc.returncode)
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
            args = [
                self._opts['ssh_bin'],
                '-o', 'VerifyHostKeyDNS=no',
                '-o', 'StrictHostKeyChecking=no',
                '-o', 'ExitOnForwardFailure=yes',
                '-o', 'UserKnownHostsFile=%s' % fake_known_hosts_file,
                '-L', '%d:localhost:%d' % (bind_port, EMR_JOB_TRACKER_PORT),
                '-N', '-q', # no shell, no output
                '-i', self._opts['ec2_key_pair_file'],
            ]
            if self._opts['ssh_tunnel_is_open']:
                args.extend(['-g', '-4']) # -4: listen on IPv4 only
            args.append('hadoop@'+host)
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
            log.info( 'Connect to job tracker at: %s' % (self._tracker_url))

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

    def cleanup(self, mode=None):
        super(EMRJobRunner, self).cleanup(mode=mode)

        # always stop our SSH tunnel if it's still running
        if self._ssh_proc:
            self._ssh_proc.poll()
            if self._ssh_proc.returncode is None:
                log.info('Killing our SSH tunnel (pid %d)' % self._ssh_proc.pid)
                try:
                    os.kill(self._ssh_proc.pid, signal.SIGKILL)
                    self._ssh_proc = None
                except Exception, e:
                    log.exception(e)

        # stop the job flow if it belongs to us (it may have stopped on its
        # own already, but that's fine)
        if self._emr_job_flow_id and not self._opts['emr_job_flow_id']:
            log.info('Terminating job flow: %s' % self._emr_job_flow_id)
            try:
                self.make_emr_conn().terminate_jobflow(self._emr_job_flow_id)
            except Exception, e:
                log.exception(e)

    def _cleanup_scratch(self):
        super(EMRJobRunner, self)._cleanup_scratch()

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
        if self._s3_job_log_uri and not self._opts['emr_job_flow_id']:
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

    def _create_job_flow(self, persistent=False, steps=None):
        """Create an empty job flow on EMR, and return the ID of that
        job.

        persistent -- if this is true, create the job flow with the --alive
            option, indicating the job will have to be manually terminated.
        """
        # make sure we can see the files we copied to S3
        self._wait_for_s3_eventual_consistency()

        # figure out local names and S3 URIs for our bootstrap files, if any
        self._name_files()
        self._pick_s3_uris_for_files()

        log.info('Creating Elastic MapReduce job flow')
        args = {}

        if self._opts['num_ec2_instances']:
            args['num_instances'] = str(self._opts['num_ec2_instances'])

        if self._opts['ec2_instance_type']:
            args['master_instance_type'] = self._opts['ec2_instance_type']
            args['slave_instance_type'] = self._opts['ec2_instance_type']

        if self._opts['ec2_master_instance_type']:
            args['master_instance_type'] = (
                self._opts['ec2_master_instance_type'])

        if self._opts['ec2_slave_instance_type']:
            args['slave_instance_type'] = (
                self._opts['ec2_slave_instance_type'])

        if self._master_bootstrap_script:
            args['bootstrap_actions'] = [botoemr.BootstrapAction(
                'master', self._master_bootstrap_script['s3_uri'], [])]

        if self._opts['ec2_key_pair']:
            args['ec2_keyname'] = self._opts['ec2_key_pair']

        if persistent:
            args['keep_alive'] = True

        if steps:
            args['steps'] = steps

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
    def _build_steps(self):
        """Return a list of boto Step objects corresponding to the
        steps we want to run."""
        assert self._script # can't build steps if no script!

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

        for step_num, step in enumerate(steps):
            # EMR-specific stuff
            name = '%s: Step %d of %d' % (
                self._job_name, step_num + 1, len(steps))

            # don't terminate a job flow that we didn't create ourselves!
            if self._opts['emr_job_flow_id']:
                action_on_failure = 'CANCEL_AND_WAIT'
            else:
                action_on_failure = 'TERMINATE_JOB_FLOW'

            # Hadoop streaming stuff
            mapper = cmd_line(self._mapper_args(step_num))
            if 'R' in step: # i.e. if there is a reducer:
                reducer = cmd_line(self._reducer_args(step_num))
            else:
                reducer = None

            cache_files = []
            cache_archives = []

            for file_dict in self._files:
                if file_dict.get('upload') == 'file':
                    cache_files.append(
                        '%s#%s' % (file_dict['s3_uri'], file_dict['name']))
                elif file_dict.get('upload') == 'archive':
                    cache_archives.append(
                        '%s#%s' % (file_dict['s3_uri'], file_dict['name']))

            input = self._s3_step_input_uris(step_num)
            output = self._s3_step_output_uri(step_num)

            # other arguments passed directly to hadoop streaming
            step_args = []

            for key, value in sorted(self._get_cmdenv().iteritems()):
                step_args.extend(['-cmdenv', '%s=%s' % (key, value)])

            for key, value in sorted(self._opts['jobconf'].iteritems()):
                step_args.extend(['-jobconf', '%s=%s' % (key, value)])

            step_args.extend(self._opts['hadoop_extra_args'])

            step_list.append(botoemr.StreamingStep(
                name=name, mapper=mapper, reducer=reducer,
                action_on_failure=action_on_failure,
                cache_files=cache_files, cache_archives=cache_archives,
                step_args=step_args, input=input, output=output))

        return step_list

    def _launch_emr_job(self):
        """Create an empty jobflow on EMR, and set self._emr_job_flow_id to
        the ID for that job."""
        self._create_s3_temp_bucket_if_needed()
        # define out steps
        steps = self._build_steps()

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

            emr_conn = self.make_emr_conn()
            job_flow = emr_conn.describe_jobflow(self._emr_job_flow_id)

            job_state = job_flow.state
            reason = getattr(job_flow, 'laststatechangereason', '')
            log_uri = job_flow.loguri

            # find all steps belonging to us, and get their state
            step_states = []
            running_step_name = ''
            total_step_time = 0.0
            step_nums = [] # step numbers belonging to us. 1-indexed

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
                    start_time = _to_timestamp(step.startdatetime)
                    end_time = _to_timestamp(step.enddatetime)
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
                        log.info(' map %3.0f%% reduce %3.0f%%' % (
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

        if log_uri:
            self._s3_job_log_uri = '%s%s/' % (
                log_uri.replace('s3n://', 's3://'), self._emr_job_flow_id)

        if success:
            log.info('Job completed.')
            log.info('Running time was %.1fs (not counting time spent waiting for the EC2 instances)' % total_step_time)
        else:
            msg = 'Job failed with status %s: %s' % (job_state, reason)
            log.error(msg)
            if self._s3_job_log_uri:
                log.info('Logs are in %s' % self._s3_job_log_uri)
            # look for a Python traceback
            cause = self._find_probable_cause_of_failure(step_nums)
            if cause:
                # log cause, and put it in exception
                cause_msg = [] # lines to log and put in exception
                cause_msg.append('Probable cause of failure (from %s):' %
                           cause['s3_log_file_uri'])
                cause_msg.extend(line.strip('\n') for line in cause['lines'])
                if cause['input_uri']:
                    cause_msg.append('(while reading from %s)' %
                                     cause['input_uri'])

                for line in cause_msg:
                    log.error(line)

                # add cause_msg to exception message
                msg += '\n' + '\n'.join(cause_msg) + '\n'

            raise Exception(msg)

    def _stream_output(self):
        log.info('Streaming final output from %s' % self._output_dir)

        # make sure the job had a chance to copy all our data to S3
        self._wait_for_s3_eventual_consistency()

        # boto Keys are theoretically iterable, but they don't actually
        # give you a line at a time.
        for s3_key in self.get_s3_keys(self._output_dir):
            if not posixpath.basename(s3_key.name).startswith('part-'):
                log.debug('skipping non-output file: %s' %
                          s3_key_to_uri(s3_key))
                continue

            output_dir = os.path.join(self._get_local_tmp_dir(), 'output')
            log.debug('downloading %s -> %s' % (
                s3_key_to_uri(s3_key), output_dir))
            # boto Keys are theoretically iterable, but they don't actually
            # give you a line at a time, so download their contents.
            # Compress the network traffic if we can.
            s3_key.get_contents_to_filename(
                output_dir, headers={'Accept-Encoding': 'gzip'})
            log.debug('reading lines from %s' % output_dir)
            for line in open(output_dir):
                yield line

    def _script_args(self):
        """How to invoke the script inside EMR"""
        # We can invoke the script by its S3 URL, but we don't really
        # gain anything from that, and EMR is touchy about distinguishing
        # python scripts from shell scripts

        assert self._script # shouldn't call _script_args() if no script

        args = [self._opts['python_bin'], self._script['name']]
        if self._wrapper_script:
            args = [self._opts['python_bin'],
                    self._wrapper_script['name']] + args

        return args

    def _mapper_args(self, step_num):
        return (self._script_args() +
                ['--step-num=%d' % step_num, '--mapper'] +
                self._mr_job_extra_args())

    def _reducer_args(self, step_num):
        return (self._script_args() +
                ['--step-num=%d' % step_num, '--reducer'] +
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

    def _public_http_url(self, file_dict):
        """Get the public HTTP URL for a file on S3. (The URL will only
        work if we've uploaded the file and set it to public.)"""
        bucket_name, path = parse_s3_uri(file_dict['s3_uri'])
        return 'http://%s.s3.amazonaws.com/%s' % (bucket_name, path)

    def _find_probable_cause_of_failure(self, step_nums):
        """Scan logs for Python exception tracebacks.

        Args:
        step_nums -- the numbers of steps belonging to us, so that we
            can ignore errors from other jobs when sharing a job flow

        Returns:
        None (nothing found) or a dictionary containing:
        lines -- lines in the log file containing the error message
        s3_log_file_uri -- the log file containing the error message
        input_uri -- if the error happened in a mapper in the first
            step, the URI of the input file that caused the error
            (otherwise None)
        """
        if not self._s3_job_log_uri:
            return None

        log.info('Scanning logs for probable cause of failure')
        self._wait_for_s3_eventual_consistency()

        s3_log_file_uris = set(self.ls(self._s3_job_log_uri))

        # give priority to task-attempts/ logs as they contain more useful
        # error messages. this may take a while.
        s3_conn = self.make_s3_conn()
        return (
            self._scan_task_attempt_logs(s3_log_file_uris, step_nums, s3_conn)
            or self._scan_step_logs(s3_log_file_uris, step_nums, s3_conn))

    def _scan_task_attempt_logs(self, s3_log_file_uris, step_nums, s3_conn):
        """Scan task-attempts/*/{syslog,stderr} for Python exceptions
        and Java stack traces.

        Helper for _find_probable_cause_of_failure()
        """
        relevant_logs = [] # list of (sort key, info, URI)
        for s3_log_file_uri in s3_log_file_uris:
            match = TASK_ATTEMPTS_LOG_URI_RE.match(s3_log_file_uri)
            if not match:
                continue

            info = match.groupdict()

            if not int(info['step_num']) in step_nums:
                continue

            # sort so we can go through the steps in reverse order
            # prefer stderr to syslog (Python exceptions are more
            # helpful than Java ones)
            sort_key = (info['step_num'], info['node_type'],
                        info['attempt_num'],
                        info['stream'] == 'stderr',
                        info['node_num'])

            relevant_logs.append((sort_key, info, s3_log_file_uri))

        relevant_logs.sort(reverse=True)

        tasks_seen = set()

        for sort_key, info, s3_log_file_uri in relevant_logs:
            # Issue #31: Don't bother with errors from tasks that
            # later succeeded
            task_info = (info['step_num'], info['node_type'],
                         info['node_num'], info['stream'])
            if task_info in tasks_seen:
                continue
            tasks_seen.add(task_info)

            log_path = self._download_log_file(s3_log_file_uri, s3_conn)
            if not log_path:
                continue

            lines = None
            if info['stream'] == 'stderr':
                log.debug('scanning %s for Python tracebacks' % log_path)
                with open(log_path) as log_file:
                    lines = find_python_traceback(log_file)
            else:
                log.debug('scanning %s for Java stack traces' % log_path)
                with open(log_path) as log_file:
                    lines = find_hadoop_java_stack_trace(log_file)

            if lines is not None:
                result = {
                    'lines': lines,
                    's3_log_file_uri': s3_log_file_uri,
                    'input_uri': None
                }

                # if this is a mapper, figure out which input file we
                # were reading from.
                if info['node_type'] == 'm':
                    result['input_uri'] = self._scan_for_input_uri(
                        s3_log_file_uri, s3_conn)

                return result

        return None

    def _scan_for_input_uri(self, s3_log_file_uri, s3_conn):
        """Scan the syslog file corresponding to s3_log_file_uri for
        information about the input file.

        Helper function for _scan_task_attempt_logs()
        """
        s3_syslog_uri = posixpath.join(
            posixpath.dirname(s3_log_file_uri), 'syslog')

        syslog_path = self._download_log_file(s3_syslog_uri, s3_conn)

        if syslog_path:
            log.debug('scanning %s for input URI' % syslog_path)
            with open(syslog_path) as syslog_file:
                return find_input_uri_for_mapper(syslog_file)
        else:
            return None

    def _scan_step_logs(self, s3_log_file_uris, step_nums, s3_conn):
        """Scan steps/*/syslog for hadoop streaming errors.

        Helper for _find_probable_cause_of_failure()
        """
        for s3_log_file_uri in sorted(s3_log_file_uris, reverse=True):
            match = STEP_LOG_URI_RE.match(s3_log_file_uri)
            if not match:
                continue

            step_num = int(match.group(1))
            if not step_num in step_nums:
                continue

            log_path = self._download_log_file(s3_log_file_uri, s3_conn)
            if log_path:
                with open(log_path) as log_file:
                    msg = find_interesting_hadoop_streaming_error(log_file)
                    if msg:
                        return {
                            'lines': [msg + '\n'],
                            's3_log_file_uri': s3_log_file_uri,
                            'input_uri': None,
                        }

    def _download_log_file(self, s3_log_file_uri, s3_conn):
        """Download a log file to our local tmp dir so we can scan it.

        Takes a log file URI, and returns a local path. We'll dump all
        log files to the same file, on the assumption that we'll scan them
        one at a time.
        """
        log_path = os.path.join(self._get_local_tmp_dir(), 'log')

        if self._uri_of_downloaded_log_file != s3_log_file_uri:
            s3_log_file = self.get_s3_key(s3_log_file_uri, s3_conn)
            if not s3_log_file:
                return None

            log.debug('downloading %s -> %s' % (s3_log_file_uri, log_path))
            s3_log_file.get_contents_to_filename(log_path)
            self._uri_of_downloaded_log_file = s3_log_file

        return log_path

    def _create_master_bootstrap_script(self, dest='b.py'):
        """Create the master bootstrap script and write it into our local
        temp directory.

        This will do nothing if there are no bootstrap scripts or commands,
        or if _create_bootstrap_script() has already been called."""
        # we call the script b.py because there's a character limit on
        # bootstrap script names (or there was at one time, anyway)

        # need to know what files are called
        if not (self._opts['bootstrap_cmds'] or
                self._bootstrap_python_packages or
                self._bootstrap_scripts or
                self._opts['bootstrap_mrjob']):
            return

        if self._master_bootstrap_script:
            return

        if self._opts['bootstrap_mrjob']:
            if self._mrjob_tar_gz_file is None:
                self._mrjob_tar_gz_file = self._add_bootstrap_file(
                    self._create_mrjob_tar_gz() + '#')

        path = os.path.join(self._get_local_tmp_dir(), dest)
        log.info('writing master bootstrap script to %s' % path)

        contents = self._master_bootstrap_script_content()
        for line in StringIO(contents):
            log.debug('BOOTSTRAP: ' + line.rstrip('\n'))

        f = open(path, 'w')
        f.write(contents)
        f.close()

        self._master_bootstrap_script = {'path': path}
        self._files.append(self._master_bootstrap_script)

    def _master_bootstrap_script_content(self):
        """Create the contents of the master bootstrap script.

        This will give names and S3 URIs to files that don't already have them
        """
        self._name_files()
        self._pick_s3_uris_for_files()

        out = StringIO()
        def writeln(line=''):
            out.write(line + '\n')

        # shebang
        writeln('#!/usr/bin/python')
        writeln()

        # imports
        writeln('import distutils.sysconfig')
        writeln('import os')
        writeln('import stat')
        writeln('from subprocess import call, check_call')
        writeln()

        # download all our files using wget
        writeln('# download files using wget')
        for file_dict in self._files:
            if file_dict.get('bootstrap'):
                args = ['wget', '-S', '-T', '10', '-t', '5',
                        self._public_http_url(file_dict),
                        '-O', file_dict['name']]
                writeln('check_call(%r)' % (args,))
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
            writeln("call(['sudo', 'python', '-m', 'compileall', '-f', mrjob_dir])")
            writeln()

        # install our python modules
        if self._bootstrap_python_packages:
            writeln('# install python modules:')
            for file_dict in self._bootstrap_python_packages:
                writeln("check_call(['tar', 'xfz', %r])" %
                        file_dict['name'])
                # figure out name of dir to CD into
                orig_name = os.path.basename(file_dict['path'])
                assert orig_name.endswith('.tar.gz')
                cd_into = orig_name[:-7]
                # install the module
                writeln("check_call(['sudo', 'python', 'setup.py', 'install'], cwd=%r)" % cd_into)

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
            raise AssertionError('This runner is already associated with job flow ID %s' % (self._emr_job_flow_id))

        log.info('Creating persistent job flow to run several jobs in...')

        self._create_master_bootstrap_script()
        self._upload_non_input_files()

        # don't allow user to call run()
        self._ran_job = True

        self._emr_job_flow_id = self._create_job_flow(persistent=True)

        return self._emr_job_flow_id

    def get_emr_job_flow_id(self):
        return self._emr_job_flow_id

    ### GENERAL FILESYSTEM STUFF ###

    def du(self, path_glob):
        """Get the size of all files matching path_glob."""
        if not S3_URI_RE.match(path_glob):
            return super(EMRJobRunner, self).getsize(path_glob)

        return sum(self.get_s3_key(uri).size for uri in self.ls(path_glob))

    def ls(self, path_glob):
        """Recursively list files locally or on S3.

        This doesn't list "directories" unless there's actually a
        corresponding key ending with a '/' (which is weird and confusing;
        don't make S3 keys ending in '/')

        To list a directory, path_glob must end with a trailing
        slash (foo and foo/ are different on S3)
        """
        if not S3_URI_RE.match(path_glob):
            for path in super(EMRJobRunner, self).ls(path_glob):
                yield path

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
            if glob_match and not fnmatch.fnmatch(uri, path_glob):
                continue

            yield uri

    def _s3_ls(self, uri):
        """Helper for ls(); doesn't bother with globbing or directories"""
        s3_conn = self.make_s3_conn()
        bucket_name, key_name = parse_s3_uri(uri)

        bucket = s3_conn.get_bucket(bucket_name)
        for key in bucket.list(key_name):
            yield s3_key_to_uri(key)

    def mkdir(self, dest):
        """Make a directory. This does nothing on S3 because there are
        no directories.
        """
        if not S3_URI_RE.match(dest):
            super(EMRJobRunner, self).mkdir(dest)

    def path_exists(self, path_glob):
        """Does the given path exist?

        If dest is a directory (ends with a "/"), we check if there are
        any files starting with that path.
        """
        if not S3_URI_RE.match(path_glob):
            return super(EMRJobRunner, self).path_exists(path_glob)

        # just fall back on ls(); it's smart
        return any(self.ls(path_glob))

    def path_join(self, dirname, filename):
        if S3_URI_RE.match(dirname):
            return posixpath.join(dirname, filename)
        else:
            return os.path.join(dirname, filename)

    def rm(self, path_glob):
        """Remove all files matching the given glob."""
        if not S3_URI_RE.match(path_glob):
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
        if not S3_URI_RE.match(dest):
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

        :return: a :py:class:`mrjob.botoemr.connection.EmrConnection`, wrapped in a :py:class:`mrjob.retry.RetryWrapper`
        """
        region = self._get_region_info_for_emr_conn()
        log.debug('creating EMR connection (to %s)' % region.endpoint)
        raw_emr_conn = botoemr.EmrConnection(
            aws_access_key_id=self._opts['aws_access_key_id'],
            aws_secret_access_key=self._opts['aws_secret_access_key'],
            region=region)
        return self._wrap_aws_conn(raw_emr_conn)

    def _get_region_info_for_emr_conn(self):
        """Get a :py:class:`boto.ec2.regioninfo.RegionInfo` object to
        initialize EMR connections with.

        This is kind of silly because all EmrConnection ever does with
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
                    "Don't know the EMR endpoint for %s; try setting emr_endpoint explicitly" % self._aws_region)

        return boto.ec2.regioninfo.RegionInfo(None, self._aws_region, endpoint)

    ### S3-specific FILESYSTEM STUFF ###

    # Utilities for interacting with S3 using S3 URIs.

    # Try to use the more general filesystem interface unless you really
    # need to do something S3-specific (e.g. setting file permissions)

    def make_s3_conn(self):
        """Create a connection to S3.

        :return: a :py:class:`boto.s3.connection.S3Connection`, wrapped in a :py:class:`mrjob.retry.RetryWrapper`
        """
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
                    "Don't know the S3 endpoint for %s; try setting s3_endpoint explicitly" % self._aws_region)

    def get_s3_key(self, uri, s3_conn=None):
        """Get the boto Key object matching the given S3 uri, or
        return None if that key doesn't exist.

        uri is an S3 URI: ``s3://foo/bar``

        You may optionally pass in an existing s3 connection through ``s3_conn``
        """
        if not s3_conn:
            s3_conn = self.make_s3_conn()
        bucket_name, key_name = parse_s3_uri(uri)

        return s3_conn.get_bucket(bucket_name).get_key(key_name)

    def make_s3_key(self, uri, s3_conn=None):
        """Create the given S3 key, and return the corresponding
        boto Key object.

        uri is an S3 URI: ``s3://foo/bar``

        You may optionally pass in an existing S3 connection through ``s3_conn``
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

        You may optionally pass in an existing S3 connection through ``s3_conn``
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
