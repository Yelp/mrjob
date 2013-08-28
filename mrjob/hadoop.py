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
import errno
import getpass
import logging
import os
import posixpath
import re
from subprocess import Popen
from subprocess import PIPE
from subprocess import CalledProcessError

try:
    import pty
    pty  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    pty = None

from mrjob.setup import UploadDirManager
from mrjob.compat import supports_new_distributed_cache_options
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
from mrjob.logparsers import best_error_from_logs
from mrjob.parse import HADOOP_STREAMING_JAR_RE
from mrjob.parse import is_uri
from mrjob.runner import MRJobRunner
from mrjob.runner import RunnerOptionStore
from mrjob.util import cmd_line


log = logging.getLogger(__name__)

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
    if is_uri(path):
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
        'check_hadoop_input_paths'
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
            'check_hadoop_input_paths': True
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

        # Keep track of local files to upload to HDFS. We'll add them
        # to this manager just before we need them.
        hdfs_files_dir = posixpath.join(self._hdfs_tmp_dir, 'files', '')
        self._upload_mgr = UploadDirManager(hdfs_files_dir)

        # Set output dir if it wasn't set explicitly
        self._output_dir = fully_qualify_hdfs_path(
            self._output_dir or
            posixpath.join(self._hdfs_tmp_dir, 'output'))

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
        self._check_input_exists()
        self._create_setup_wrapper_script()
        self._add_job_files_for_upload()
        self._upload_local_files_to_hdfs()
        self._run_job_in_hadoop()

    def _check_input_exists(self):
        """Make sure all input exists before continuing with our job.
        """
        for path in self._input_paths:
            if path == '-':
                continue  # STDIN always exists

            if self._opts['check_hadoop_input_paths']:
                if not self.path_exists(path):
                    raise AssertionError(
                        'Input path %s does not exist!' % (path,))

    def _add_job_files_for_upload(self):
        """Add files needed for running the job (setup and input)
        to self._upload_mgr."""
        for path in self._get_input_paths():
            self._upload_mgr.add(path)

        for path in self._working_dir_mgr.paths():
            self._upload_mgr.add(path)

    def _upload_local_files_to_hdfs(self):
        """Copy files managed by self._upload_mgr to HDFS
        """
        self._mkdir_on_hdfs(self._upload_mgr.prefix)

        log.info('Copying local files into %s' % self._upload_mgr.prefix)
        for path, uri in self._upload_mgr.path_to_uri().iteritems():
            self._upload_to_hdfs(path, uri)

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
        self._counters = []
        steps = self._get_steps()

        for step_num, step in enumerate(steps):
            log.debug('running step %d of %d' % (step_num + 1, len(steps)))

            streaming_args = self._streaming_args(step, step_num, len(steps))

            log.debug('> %s' % cmd_line(streaming_args))

            # try to use a PTY if it's available
            try:
                pid, master_fd = pty.fork()
            except (AttributeError, OSError):
                # no PTYs, just use Popen
                step_proc = Popen(streaming_args, stdout=PIPE, stderr=PIPE)

                self._process_stderr_from_streaming(step_proc.stderr)

                # there shouldn't be much output to STDOUT
                for line in step_proc.stdout:
                    log.error('STDOUT: ' + line.strip('\n'))

                returncode = step_proc.wait()
            else:
                # we have PTYs
                if pid == 0:  # we are the child process
                    os.execvp(streaming_args[0], streaming_args)
                else:
                    master = os.fdopen(master_fd)
                    # reading from master gives us the subprocess's
                    # stderr and stdout (it's a fake terminal)
                    self._process_stderr_from_streaming(master)
                    _, returncode = os.waitpid(pid, 0)
                    master.close()

            if returncode == 0:
                # parsing needs step number for whole job
                self._fetch_counters([step_num + self._start_step_num])
                # printing needs step number relevant to this run of mrjob
                self.print_counters([step_num + 1])
            else:
                msg = ('Job failed with return code %d: %s' %
                       (returncode, streaming_args))
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

                raise CalledProcessError(returncode, streaming_args)

    def _process_stderr_from_streaming(self, stderr):

        def treat_eio_as_eof(iter):
            # on Linux, the PTY gives us a specific IOError when the
            # when the child process exits, rather than EOF.
            while True:
                try:
                    yield iter.next()  # okay for StopIteration to bubble up
                except IOError, e:
                    if e.errno == errno.EIO:
                        return
                    else:
                        raise

        for line in treat_eio_as_eof(stderr):
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
        if supports_new_distributed_cache_options(version):
            # set up uploading from HDFS to the working dir
            streaming_args.extend(
                self._new_upload_args(self._upload_mgr))

        # Add extra hadoop args first as hadoop args could be a hadoop
        # specific argument (e.g. -libjar) which must come before job
        # specific args.
        streaming_args.extend(
            self._hadoop_conf_args(step, step_num, num_steps))

        # set up input
        for input_uri in self._hdfs_step_input_files(step_num):
            streaming_args.extend(['-input', input_uri])

        # set up output
        streaming_args.append('-output')
        streaming_args.append(self._hdfs_step_output_dir(step_num))

        # -cacheFile/-cacheArchive (streaming options, old-style)
        if not supports_new_distributed_cache_options(version):
            # set up uploading from HDFS to the working dir
            streaming_args.extend(
                self._old_upload_args(self._upload_mgr))

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
            return [self._upload_mgr.uri(p)
                    for p in self._get_input_paths()]
        else:
            return [posixpath.join(
                self._hdfs_tmp_dir, 'step-output', str(step_num))]

    def _hdfs_step_output_dir(self, step_num):
        if step_num == len(self._get_steps()) - 1:
            return self._output_dir
        else:
            return posixpath.join(
                self._hdfs_tmp_dir, 'step-output', str(step_num + 1))

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
        return best_error_from_logs(self, task_attempt_logs, step_logs,
                                    job_logs)
