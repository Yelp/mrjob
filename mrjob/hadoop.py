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

import getpass
import logging
import os
import posixpath
import re
from subprocess import Popen
from subprocess import PIPE
from subprocess import CalledProcessError

from mrjob import compat
from mrjob.conf import combine_cmds
from mrjob.conf import combine_dicts
from mrjob.conf import combine_paths
from mrjob.fs.hadoop import HadoopFilesystem
from mrjob.fs.local import LocalFilesystem
from mrjob.fs.composite import CompositeFilesystem
from mrjob.logparsers import TASK_ATTEMPTS_LOG_URI_RE
from mrjob.logparsers import STEP_LOG_URI_RE
from mrjob.logparsers import HADOOP_JOB_LOG_URI_RE
from mrjob.logparsers import scan_for_counters_in_files
from mrjob.logparsers import scan_logs_in_order
from mrjob.parse import HADOOP_STREAMING_JAR_RE
from mrjob.parse import is_uri
from mrjob.runner import MRJobRunner
from mrjob.runner import RunnerOptionStore
from mrjob.util import cmd_line


log = logging.getLogger('mrjob.hadoop')

# to filter out the log4j stuff that hadoop streaming prints out
HADOOP_STREAMING_OUTPUT_RE = re.compile(r'^(\S+ \S+ \S+ \S+: )?(.*)$')

# used by mkdir()
HADOOP_FILE_EXISTS_RE = re.compile(r'.*File exists.*')

# used by ls()
HADOOP_LSR_NO_SUCH_FILE = re.compile(
    r'^lsr: Cannot access .*: No such file or directory.')

# used by rm() (see below)
HADOOP_RMR_NO_SUCH_FILE = re.compile(r'^rmr: hdfs://.*$')

# used to extract the job timestamp from stderr
HADOOP_JOB_TIMESTAMP_RE = re.compile(
    r'(INFO: )?Running job: job_(?P<timestamp>\d+)_(?P<step_num>\d+)')

# find version string in "Hadoop 0.20.203" etc.
HADOOP_VERSION_RE = re.compile(r'^.*?(?P<version>(\d|\.)+).*?$')


def find_hadoop_streaming_jar(path):
    """Return the path of the hadoop streaming jar inside the given
    directory tree, or None if we can't find it."""
    for (dirpath, _, filenames) in os.walk(path):
        for filename in filenames:
            if HADOOP_STREAMING_JAR_RE.match(filename):
                return os.path.join(dirpath, filename)
    else:
        return None


def fully_qualify_hdfs_path(path):
    """If path isn't an ``hdfs://`` URL, turn it into one."""
    if path.startswith('hdfs://') or path.startswith('s3n:/'):
        return path
    elif path.startswith('/'):
        return 'hdfs://' + path
    else:
        return 'hdfs:///user/%s/%s' % (getpass.getuser(), path)


def hadoop_log_dir(hadoop_home=None):
    """Return the path where Hadoop stores logs.

    :param hadoop_home: putative value of :envvar:`HADOOP_HOME`, or None to
                        default to the actual value if used. This is only used
                        if :envvar:`HADOOP_LOG_DIR` is not defined.
    """
    try:
        return os.environ['HADOOP_LOG_DIR']
    except KeyError:
        # Defaults to $HADOOP_HOME/logs
        # http://wiki.apache.org/hadoop/HowToConfigure
        if hadoop_home is None:
            hadoop_home = os.environ['HADOOP_HOME']
        return os.path.join(hadoop_home, 'logs')


class HadoopRunnerOptionStore(RunnerOptionStore):

    ALLOWED_KEYS = RunnerOptionStore.ALLOWED_KEYS.union(set([
        'hadoop_bin',
        'hadoop_home',
        'hdfs_scratch_dir',
    ]))

    COMBINERS = combine_dicts(RunnerOptionStore.COMBINERS, {
        'hadoop_bin': combine_cmds,
        'hadoop_home': combine_paths,
        'hdfs_scratch_dir': combine_paths,
    })

    def __init__(self, alias, opts, conf_path):
        super(HadoopRunnerOptionStore, self).__init__(alias, opts, conf_path)

        # fix hadoop_home
        if not self['hadoop_home']:
            raise Exception(
                'you must set $HADOOP_HOME, or pass in hadoop_home explicitly')
        self['hadoop_home'] = os.path.abspath(self['hadoop_home'])

        # fix hadoop_bin
        if not self['hadoop_bin']:
            self['hadoop_bin'] = [
                os.path.join(self['hadoop_home'], 'bin/hadoop')]

        # fix hadoop_streaming_jar
        if not self['hadoop_streaming_jar']:
            log.debug('Looking for hadoop streaming jar in %s' %
                      self['hadoop_home'])
            self['hadoop_streaming_jar'] = find_hadoop_streaming_jar(
                self['hadoop_home'])

            if not self['hadoop_streaming_jar']:
                raise Exception(
                    "Couldn't find streaming jar in %s, bailing out" %
                    self['hadoop_home'])

        log.debug('Hadoop streaming jar is %s' %
                  self['hadoop_streaming_jar'])

    def default_options(self):
        super_opts = super(HadoopRunnerOptionStore, self).default_options()
        return combine_dicts(super_opts, {
            'hadoop_home': os.environ.get('HADOOP_HOME'),
            'hdfs_scratch_dir': 'tmp/mrjob',
        })


class HadoopJobRunner(MRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` on your Hadoop cluster.

    Input and support files can be either local or on HDFS; use ``hdfs://...``
    URLs to refer to files on HDFS.
    """
    alias = 'hadoop'

    OPTION_STORE_CLASS = HadoopRunnerOptionStore

    def __init__(self, **kwargs):
        """:py:class:`~mrjob.hadoop.HadoopJobRunner` takes the same arguments
        as :py:class:`~mrjob.runner.MRJobRunner`, plus some additional options
        which can be defaulted in :ref:`mrjob.conf <mrjob.conf>`.
        """
        super(HadoopJobRunner, self).__init__(**kwargs)

        self._hdfs_tmp_dir = fully_qualify_hdfs_path(
            posixpath.join(
            self._opts['hdfs_scratch_dir'], self._job_name))

        # Set output dir if it wasn't set explicitly
        self._output_dir = fully_qualify_hdfs_path(
            self._output_dir or
            posixpath.join(self._hdfs_tmp_dir, 'output'))

        # we'll set this up later
        self._hdfs_input_files = None
        # temp dir for input
        self._hdfs_input_dir = None

        self._hadoop_log_dir = hadoop_log_dir(self._opts['hadoop_home'])

        # Running jobs via hadoop assigns a new timestamp to each job.
        # Running jobs via mrjob only adds steps.
        # Store both of these values to enable log parsing.
        self._job_timestamp = None
        self._start_step_num = 0

        # init hadoop version cache
        self._hadoop_version = None

    @property
    def fs(self):
        """:py:class:`mrjob.fs.base.Filesystem` object for HDFS and the local
        filesystem.
        """
        if self._fs is None:
            self._fs = CompositeFilesystem(
                HadoopFilesystem(self._opts['hadoop_bin']),
                LocalFilesystem())
        return self._fs

    def get_hadoop_version(self):
        """Invoke the hadoop executable to determine its version"""
        if not self._hadoop_version:
            stdout = self.invoke_hadoop(['version'], return_stdout=True)
            if stdout:
                first_line = stdout.split('\n')[0]
                m = HADOOP_VERSION_RE.match(first_line)
                if m:
                    self._hadoop_version = m.group('version')
                    log.info("Using Hadoop version %s" % self._hadoop_version)
                    return self._hadoop_version
            self._hadoop_version = '0.20.203'
            log.info("Unable to determine Hadoop version. Assuming 0.20.203.")
        return self._hadoop_version

    def _run(self):
        if self._opts['bootstrap_mrjob']:
            self._add_python_archive(self._create_mrjob_tar_gz() + '#')

        self._setup_input()
        self._upload_non_input_files()
        self._run_job_in_hadoop()

    def _setup_input(self):
        """Copy local input files (if any) to a special directory on HDFS.

        Set self._hdfs_input_files
        """
        # winnow out HDFS files from local ones
        self._hdfs_input_files = []
        local_input_files = []

        for path in self._input_paths:
            if is_uri(path):
                # Don't even bother running the job if the input isn't there.
                if not self.ls(path):
                    raise AssertionError(
                        'Input path %s does not exist!' % (path,))
                self._hdfs_input_files.append(path)
            else:
                local_input_files.append(path)

        # copy local files into an input directory, with names like
        # 00000-actual_name.ext
        if local_input_files:
            hdfs_input_dir = posixpath.join(self._hdfs_tmp_dir, 'input')
            log.info('Uploading input to %s' % hdfs_input_dir)
            self._mkdir_on_hdfs(hdfs_input_dir)

            for i, path in enumerate(local_input_files):
                if path == '-':
                    path = self._dump_stdin_to_local_file()

                target = '%s/%05i-%s' % (
                    hdfs_input_dir, i, os.path.basename(path))
                self._upload_to_hdfs(path, target)

            self._hdfs_input_files.append(hdfs_input_dir)

    def _pick_hdfs_uris_for_files(self):
        """Decide where each file will be uploaded on S3.

        Okay to call this multiple times.
        """
        hdfs_files_dir = posixpath.join(self._hdfs_tmp_dir, 'files', '')
        self._assign_unique_names_to_files(
            'hdfs_uri', prefix=hdfs_files_dir, match=is_uri)

    def _upload_non_input_files(self):
        """Copy files to HDFS, and set the 'hdfs_uri' field for each file.
        """
        self._pick_hdfs_uris_for_files()

        hdfs_files_dir = posixpath.join(self._hdfs_tmp_dir, 'files', '')
        self._mkdir_on_hdfs(hdfs_files_dir)
        log.info('Copying non-input files into %s' % hdfs_files_dir)

        for file_dict in self._files:
            path = file_dict['path']

            # don't bother with files already in HDFS
            if is_uri(path):
                continue

            self._upload_to_hdfs(path, file_dict['hdfs_uri'])

    def _mkdir_on_hdfs(self, path):
        log.debug('Making directory %s on HDFS' % path)
        self.invoke_hadoop(['fs', '-mkdir', path])

    def _upload_to_hdfs(self, path, target):
        log.debug('Uploading %s -> %s on HDFS' % (path, target))
        self.invoke_hadoop(['fs', '-put', path, target])

    def _dump_stdin_to_local_file(self):
        """Dump sys.stdin to a local file, and return the path to it."""
        stdin_path = os.path.join(self._get_local_tmp_dir(), 'STDIN')
         # prompt user, so they don't think the process has stalled
        log.info('reading from STDIN')

        log.debug('dumping stdin to local file %s' % stdin_path)
        stdin_file = open(stdin_path, 'w')
        for line in self._stdin:
            stdin_file.write(line)

        return stdin_path

    def _run_job_in_hadoop(self):
        # figure out local names for our files
        self._name_files()

        # send script and wrapper script (if any) to working dir
        assert self._script  # shouldn't be able to run if no script
        self._script['upload'] = 'file'
        if self._wrapper_script:
            self._wrapper_script['upload'] = 'file'

        self._counters = []
        steps = self._get_steps()

        for step_num, step in enumerate(steps):
            log.debug('running step %d of %d' % (step_num + 1, len(steps)))

            streaming_args = self._streaming_args(step, step_num, len(steps))

            log.debug('> %s' % cmd_line(streaming_args))
            step_proc = Popen(streaming_args, stdout=PIPE, stderr=PIPE)

            # TODO: use a pty or something so that the hadoop binary
            # won't buffer the status messages
            self._process_stderr_from_streaming(step_proc.stderr)

            # there shouldn't be much output to STDOUT
            for line in step_proc.stdout:
                log.error('STDOUT: ' + line.strip('\n'))

            returncode = step_proc.wait()
            if returncode == 0:
                # parsing needs step number for whole job
                self._fetch_counters([step_num + self._start_step_num])
                # printing needs step number relevant to this run of mrjob
                self.print_counters([step_num + 1])
            else:
                msg = ('Job failed with return code %d: %s' %
                       (step_proc.returncode, streaming_args))
                log.error(msg)
                # look for a Python traceback
                cause = self._find_probable_cause_of_failure(
                    [step_num + self._start_step_num])
                if cause:
                    # log cause, and put it in exception
                    cause_msg = []  # lines to log and put in exception
                    cause_msg.append('Probable cause of failure (from %s):' %
                               cause['log_file_uri'])
                    cause_msg.extend(line.strip('\n')
                                     for line in cause['lines'])
                    if cause['input_uri']:
                        cause_msg.append('(while reading from %s)' %
                                         cause['input_uri'])

                    for line in cause_msg:
                        log.error(line)

                    # add cause_msg to exception message
                    msg += '\n' + '\n'.join(cause_msg) + '\n'

                raise Exception(msg)
                raise CalledProcessError(step_proc.returncode, streaming_args)

    def _process_stderr_from_streaming(self, stderr):
        for line in stderr:
            line = HADOOP_STREAMING_OUTPUT_RE.match(line).group(2)
            log.info('HADOOP: ' + line)

            if 'Streaming Job Failed!' in line:
                raise Exception(line)

            # The job identifier is printed to stderr. We only want to parse it
            # once because we know how many steps we have and just want to know
            # what Hadoop thinks the first step's number is.
            m = HADOOP_JOB_TIMESTAMP_RE.match(line)
            if m and self._job_timestamp is None:
                self._job_timestamp = m.group('timestamp')
                self._start_step_num = int(m.group('step_num'))

    def _streaming_args(self, step, step_num, num_steps):
        version = self.get_hadoop_version()

        streaming_args = (self._opts['hadoop_bin'] +
                          ['jar', self._opts['hadoop_streaming_jar']])

        # -files/-archives (generic options, new-style)
        if compat.supports_new_distributed_cache_options(version):
            # set up uploading from HDFS to the working dir
            streaming_args.extend(self._new_upload_args())

        # Add extra hadoop args first as hadoop args could be a hadoop
        # specific argument (e.g. -libjar) which must come before job
        # specific args.
        streaming_args.extend(self._hadoop_conf_args(step_num, num_steps))

        # set up input
        for input_uri in self._hdfs_step_input_files(step_num):
            streaming_args.extend(['-input', input_uri])

        # set up output
        streaming_args.append('-output')
        streaming_args.append(self._hdfs_step_output_dir(step_num))

        # -cacheFile/-cacheArchive (streaming options, old-style)
        if not compat.supports_new_distributed_cache_options(version):
            # set up uploading from HDFS to the working dir
            streaming_args.extend(self._old_upload_args())

        mapper, combiner, reducer = (
            self._hadoop_streaming_commands(step, step_num))

        streaming_args.append('-mapper')
        streaming_args.append(mapper)

        if combiner:
            streaming_args.append('-combiner')
            streaming_args.append(combiner)

        if reducer:
            streaming_args.append('-reducer')
            streaming_args.append(reducer)
        else:
            streaming_args.extend(['-jobconf', 'mapred.reduce.tasks=0'])

        return streaming_args

    def _hdfs_step_input_files(self, step_num):
        """Get the hdfs:// URI for input for the given step."""
        if step_num == 0:
            return self._hdfs_input_files
        else:
            return [posixpath.join(
                self._hdfs_tmp_dir, 'step-output', str(step_num))]

    def _hdfs_step_output_dir(self, step_num):
        if step_num == len(self._get_steps()) - 1:
            return self._output_dir
        else:
            return posixpath.join(
                self._hdfs_tmp_dir, 'step-output', str(step_num + 1))

    def _new_upload_args(self):
        """Args to upload files from HDFS to the hadoop nodes using new
        distributed cache options.
        """
        args = []

        # return list of strings ready for comma-joining for passing to the
        # hadoop binary
        def escaped_paths(file_dicts):
            return ["%s#%s" % (fd['hdfs_uri'], fd['name'])
                    for fd in file_dicts]

        # index by type
        all_files = {}
        for fd in self._files:
            all_files.setdefault(fd.get('upload'), []).append(fd)

        if 'file' in all_files:
            args.append('-files')
            args.append(','.join(escaped_paths(all_files['file'])))

        if 'archive' in all_files:
            args.append('-archives')
            args.append(','.join(escaped_paths(all_files['archive'])))

        return args

    def _old_upload_args(self):
        """Args to upload files from HDFS to the hadoop nodes using old cache
        options.
        """
        args = []

        for file_dict in self._files:
            if file_dict.get('upload') == 'file':
                args.append('-cacheFile')
                args.append(
                    '%s#%s' % (file_dict['hdfs_uri'], file_dict['name']))

            elif file_dict.get('upload') == 'archive':
                args.append('-cacheArchive')
                args.append(
                    '%s#%s' % (file_dict['hdfs_uri'], file_dict['name']))

        return args

    def _cleanup_local_scratch(self):
        super(HadoopJobRunner, self)._cleanup_local_scratch()

        if self._hdfs_tmp_dir:
            log.info('deleting %s from HDFS' % self._hdfs_tmp_dir)

            try:
                self.invoke_hadoop(['fs', '-rmr', self._hdfs_tmp_dir])
            except Exception, e:
                log.exception(e)

    ### LOG FETCHING/PARSING ###

    def _enforce_path_regexp(self, paths, regexp, step_nums):
        """Helper for log fetching functions to filter out unwanted
        logs. Keyword arguments are checked against their corresponding
        regex groups.
        """
        for path in paths:
            m = regexp.match(path)
            if (m
                and (step_nums is None or
                     int(m.group('step_num')) in step_nums)
                and (self._job_timestamp is None or
                     m.group('timestamp') == self._job_timestamp)):
                yield path

    def _ls_logs(self, relative_path):
        """List logs on the local filesystem by path relative to log root
        directory
        """
        return self.ls(os.path.join(self._hadoop_log_dir, relative_path))

    def _fetch_counters(self, step_nums, skip_s3_wait=False):
        """Read Hadoop counters from local logs.

        Args:
        step_nums -- the steps belonging to us, so that we can ignore errors
                     from other jobs run with the same timestamp
        """
        job_logs = self._enforce_path_regexp(self._ls_logs('history/'),
                                             HADOOP_JOB_LOG_URI_RE,
                                             step_nums)
        uris = list(job_logs)
        new_counters = scan_for_counters_in_files(uris, self,
                                                  self.get_hadoop_version())

        # only include steps relevant to the current job
        for step_num in step_nums:
            self._counters.append(new_counters.get(step_num, {}))

    def counters(self):
        return self._counters

    def _find_probable_cause_of_failure(self, step_nums):
        all_task_attempt_logs = []
        try:
            all_task_attempt_logs.extend(self._ls_logs('userlogs/'))
        except IOError:
            # sometimes the master doesn't have these
            pass
        # TODO: get these logs from slaves if possible
        task_attempt_logs = self._enforce_path_regexp(all_task_attempt_logs,
                                                      TASK_ATTEMPTS_LOG_URI_RE,
                                                      step_nums)
        step_logs = self._enforce_path_regexp(self._ls_logs('steps/'),
                                              STEP_LOG_URI_RE,
                                              step_nums)
        job_logs = self._enforce_path_regexp(self._ls_logs('history/'),
                                             HADOOP_JOB_LOG_URI_RE,
                                             step_nums)
        log.info('Scanning logs for probable cause of failure')
        return scan_logs_in_order(task_attempt_logs=task_attempt_logs,
                                  step_logs=step_logs,
                                  job_logs=job_logs,
                                  runner=self)
