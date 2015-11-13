# Copyright 2009-2015 Yelp and Contributors
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

import mrjob.step
from mrjob.setup import UploadDirManager
from mrjob.compat import supports_new_distributed_cache_options
from mrjob.conf import combine_cmds
from mrjob.conf import combine_dicts
from mrjob.conf import combine_paths
from mrjob.fs.hadoop import HadoopFilesystem
from mrjob.fs.local import LocalFilesystem
from mrjob.fs.composite import CompositeFilesystem
from mrjob.logs.ls import ls_logs
from mrjob.logparsers import scan_for_counters_in_files
from mrjob.logparsers import best_error_from_logs
from mrjob.parse import HADOOP_STREAMING_JAR_RE
from mrjob.py2 import to_string
from mrjob.parse import is_uri
from mrjob.runner import MRJobRunner
from mrjob.runner import RunnerOptionStore
from mrjob.util import cmd_line
from mrjob.util import unique
from mrjob.util import which


log = logging.getLogger(__name__)

# to filter out the log4j stuff that hadoop streaming prints out
HADOOP_STREAMING_OUTPUT_RE = re.compile(br'^(\S+ \S+ \S+ \S+: )?(.*)$')

# used to extract the job timestamp from stderr
HADOOP_JOB_TIMESTAMP_RE = re.compile(
    br'(INFO: )?Running job: job_(?P<timestamp>\d+)_(?P<step_num>\d+)')

# don't look for the hadoop streaming jar here!
_BAD_HADOOP_HOMES = ['/', '/usr', '/usr/local']

# places to look for the Hadoop streaming jar if we're inside EMR
_EMR_HADOOP_STREAMING_JAR_DIRS = [
    # for the 2.x and 3.x AMIs (the 2.x AMIs also set $HADOOP_HOME properly)
    '/home/hadoop/contrib',
    # for the 4.x AMIs
    '/usr/lib/hadoop-mapreduce',
]


def fully_qualify_hdfs_path(path):
    """If path isn't an ``hdfs://`` URL, turn it into one."""
    if is_uri(path):
        return path
    elif path.startswith('/'):
        return 'hdfs://' + path
    else:
        return 'hdfs:///user/%s/%s' % (getpass.getuser(), path)


def hadoop_prefix_from_bin(hadoop_bin):
    """Given a path to the hadoop binary, return the path of the implied
    hadoop home, or None if we don't know.

    Don't return the parent directory of directories in the default
    path (not ``/``, ``/usr``, or ``/usr/local``).
    """
    # resolve unqualified binary name (relative paths are okay)
    if '/' not in hadoop_bin:
        hadoop_bin = which(hadoop_bin)
        if not hadoop_bin:
            return None

    # use parent of hadoop_bin's directory
    hadoop_home = posixpath.abspath(
        posixpath.join(posixpath.realpath(posixpath.dirname(hadoop_bin)), '..')
    )

    if hadoop_home in _BAD_HADOOP_HOMES:
        return None

    return hadoop_home


class HadoopRunnerOptionStore(RunnerOptionStore):

    ALLOWED_KEYS = RunnerOptionStore.ALLOWED_KEYS.union(set([
        'hadoop_bin',
        'hadoop_home',
        'hadoop_tmp_dir',
    ]))

    COMBINERS = combine_dicts(RunnerOptionStore.COMBINERS, {
        'hadoop_bin': combine_cmds,
        'hadoop_home': combine_paths,
        'hadoop_tmp_dir': combine_paths,
    })

    DEPRECATED_ALIASES = combine_dicts(RunnerOptionStore.DEPRECATED_ALIASES, {
        'hdfs_scratch_dir': 'hadoop_tmp_dir',
    })

    def default_options(self):
        super_opts = super(HadoopRunnerOptionStore, self).default_options()
        return combine_dicts(super_opts, {
            'hadoop_tmp_dir': 'tmp/mrjob',
        })


class HadoopJobRunner(MRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` on your Hadoop cluster.
    Invoked when you run your job with ``-r hadoop``.

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

        self._hadoop_tmp_dir = fully_qualify_hdfs_path(
            posixpath.join(
                self._opts['hadoop_tmp_dir'], self._job_key))

        # Keep track of local files to upload to HDFS. We'll add them
        # to this manager just before we need them.
        hdfs_files_dir = posixpath.join(self._hadoop_tmp_dir, 'files', '')
        self._upload_mgr = UploadDirManager(hdfs_files_dir)

        # Set output dir if it wasn't set explicitly
        self._output_dir = fully_qualify_hdfs_path(
            self._output_dir or
            posixpath.join(self._hadoop_tmp_dir, 'output'))

        # Running jobs via hadoop assigns a new timestamp to each job.
        # Running jobs via mrjob only adds steps.
        # Store both of these values to enable log parsing.
        self._job_timestamp = None
        self._start_step_num = 0

        # Keep track of where the hadoop streaming jar is
        self._hadoop_streaming_jar = self._opts['hadoop_streaming_jar']
        self._searched_for_hadoop_streaming_jar = False

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
        return self.fs.get_hadoop_version()

    def get_hadoop_bin(self):
        """Find the hadoop binary. A list: binary followed by arguments."""
        return self.fs.get_hadoop_bin()

    def get_hadoop_streaming_jar(self):
        """Find the path of the hadoop streaming jar, or None if not found."""
        if not (self._hadoop_streaming_jar or
                self._searched_for_hadoop_streaming_jar):

            self._hadoop_streaming_jar = self._find_hadoop_streaming_jar()

            if self._hadoop_streaming_jar:
                log.info('Found Hadoop streaming jar: %s' %
                         self._hadoop_streaming_jar)
            else:
                log.warning('Hadoop streaming jar not found. Use'
                            ' --hadoop-streaming-jar')

            self._searched_for_hadoop_streaming_jar = True

        return self._hadoop_streaming_jar

    def _find_hadoop_streaming_jar(self):
        """Search for the hadoop streaming jar. See
        :py:meth:`_hadoop_streaming_jar_dirs` for where we search."""
        for path in unique(self._hadoop_streaming_jar_dirs()):
            log.info('Looking for Hadoop streaming jar in %s' % path)

            streaming_jars = []
            for path in self.fs.ls(path):
                if HADOOP_STREAMING_JAR_RE.match(posixpath.basename(path)):
                    streaming_jars.append(path)

            if streaming_jars:
                # prefer shorter names and shallower paths
                def sort_key(p):
                    return (len(p.split('/')),
                            len(posixpath.basename(p)),
                            p)

                streaming_jars.sort(key=sort_key)

                return streaming_jars[0]

        return None

    def _hadoop_streaming_jar_dirs(self):
        """Yield all possible places to look for the Hadoop streaming jar."""
        if self._opts['hadoop_home']:
            yield self._opts['hadoop_home']

        for name in ('HADOOP_PREFIX', 'HADOOP_HOME', 'HADOOP_INSTALL',
                     'HADOOP_MAPRED_HOME'):
            path = os.environ.get(name)
            if path:
                yield path

        # guess it from the path of the Hadoop binary
        hadoop_home = hadoop_prefix_from_bin(self.get_hadoop_bin()[0])
        if hadoop_home:
            yield hadoop_home

        # try HADOOP_*_HOME
        for name, path in sorted(os.environ.items()):
            if name.startswith('HADOOP_') and name.endswith('_HOME'):
                yield path

        # use hard-coded paths to work out-of-the-box on EMR
        for path in _EMR_HADOOP_STREAMING_JAR_DIRS:
            yield path

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

            if self._opts['check_input_paths']:
                if not self.fs.exists(path):
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
        self.fs.mkdir(self._upload_mgr.prefix)

        log.info('Copying local files into %s' % self._upload_mgr.prefix)
        for path, uri in self._upload_mgr.path_to_uri().items():
            self._upload_to_hdfs(path, uri)

    def _upload_to_hdfs(self, path, target):
        log.debug('Uploading %s -> %s on HDFS' % (path, target))
        self.fs._put(path, target)

    def _dump_stdin_to_local_file(self):
        """Dump sys.stdin to a local file, and return the path to it."""
        stdin_path = posixpath.join(self._get_local_tmp_dir(), 'STDIN')
         # prompt user, so they don't think the process has stalled
        log.info('reading from STDIN')

        log.debug('dumping stdin to local file %s' % stdin_path)
        stdin_file = open(stdin_path, 'wb')
        for line in self._stdin:
            stdin_file.write(line)

        return stdin_path

    def _run_job_in_hadoop(self):
        self._counters = []

        for step_num in range(self._num_steps()):
            log.debug('running step %d of %d' %
                      (step_num + 1, self._num_steps()))

            step_args = self._args_for_step(step_num)

            log.debug('> %s' % cmd_line(step_args))

            # try to use a PTY if it's available
            try:
                pid, master_fd = pty.fork()
            except (AttributeError, OSError):
                # no PTYs, just use Popen
                step_proc = Popen(step_args, stdout=PIPE, stderr=PIPE)

                self._process_stderr_from_streaming(step_proc.stderr)

                # there shouldn't be much output to STDOUT
                for line in step_proc.stdout:
                    log.error('STDOUT: ' + to_string(line.strip(b'\n')))

                step_proc.stdout.close()
                step_proc.stderr.close()

                returncode = step_proc.wait()
            else:
                # we have PTYs
                if pid == 0:  # we are the child process
                    os.execvp(step_args[0], step_args)
                else:
                    with os.fdopen(master_fd, 'rb') as master:
                        # reading from master gives us the subprocess's
                        # stderr and stdout (it's a fake terminal)
                        self._process_stderr_from_streaming(master)
                        _, returncode = os.waitpid(pid, 0)

            if returncode == 0:
                # parsing needs step number for whole job
                self._fetch_counters([step_num + self._start_step_num])
                # printing needs step number relevant to this run of mrjob
                self.print_counters([step_num + 1])
            else:
                msg = ('Job failed with return code %d: %s' %
                       (returncode, step_args))
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

                raise CalledProcessError(returncode, step_args)

    def _process_stderr_from_streaming(self, stderr):

        def treat_eio_as_eof(iter):
            # on Linux, the PTY gives us a specific IOError when the
            # when the child process exits, rather than EOF.
            while True:
                try:
                    yield next(iter)  # okay for StopIteration to bubble up
                except IOError as e:
                    if e.errno == errno.EIO:
                        return
                    else:
                        raise

        for line in treat_eio_as_eof(stderr):
            line = HADOOP_STREAMING_OUTPUT_RE.match(line).group(2)
            log.info('HADOOP: ' + to_string(line))

            if b'Streaming Job Failed!' in line:
                raise Exception(line)

            # The job identifier is printed to stderr. We only want to parse it
            # once because we know how many steps we have and just want to know
            # what Hadoop thinks the first step's number is.
            m = HADOOP_JOB_TIMESTAMP_RE.match(line)
            if m and self._job_timestamp is None:
                self._job_timestamp = m.group('timestamp')
                self._start_step_num = int(m.group('step_num'))

    def _args_for_step(self, step_num):
        step = self._get_step(step_num)

        if step['type'] == 'streaming':
            return self._args_for_streaming_step(step_num)
        elif step['type'] == 'jar':
            return self._args_for_jar_step(step_num)
        else:
            raise AssertionError('Bad step type: %r' % (step['type'],))

    def _args_for_streaming_step(self, step_num):
        version = self.get_hadoop_version()

        hadoop_streaming_jar = self.get_hadoop_streaming_jar()
        if not hadoop_streaming_jar:
            raise Exception('no Hadoop streaming jar')

        args = self.get_hadoop_bin() + ['jar', hadoop_streaming_jar]

        # -files/-archives (generic options, new-style)
        if supports_new_distributed_cache_options(version):
            # set up uploading from HDFS to the working dir
            args.extend(
                self._upload_args(self._upload_mgr))

        # Add extra hadoop args first as hadoop args could be a hadoop
        # specific argument (e.g. -libjar) which must come before job
        # specific args.
        args.extend(self._hadoop_args_for_step(step_num))

        # set up input
        for input_uri in self._hdfs_step_input_files(step_num):
            args.extend(['-input', input_uri])

        # set up output
        args.append('-output')
        args.append(self._hdfs_step_output_dir(step_num))

        # -cacheFile/-cacheArchive (streaming options, old-style)
        if not supports_new_distributed_cache_options(version):
            # set up uploading from HDFS to the working dir
            args.extend(
                self._pre_0_20_upload_args(self._upload_mgr))

        mapper, combiner, reducer = (
            self._hadoop_streaming_commands(step_num))

        args.append('-mapper')
        args.append(mapper)

        if combiner:
            args.append('-combiner')
            args.append(combiner)

        if reducer:
            args.append('-reducer')
            args.append(reducer)
        else:
            args.extend(['-jobconf', 'mapred.reduce.tasks=0'])

        return args

    def _args_for_jar_step(self, step_num):
        step = self._get_step(step_num)

        # special case for consistency with EMR runner.
        #
        # This might look less like duplicated code if we ever
        # implement #780 (fetching jars from URIs)
        if step['jar'].startswith('file:///'):
            jar = step['jar'][7:]  # keep leading slash
        else:
            jar = step['jar']

        args = self.get_hadoop_bin() + ['jar', jar]

        if step.get('main_class'):
            args.append(step['main_class'])

        # TODO: merge with logic in mrjob/emr.py
        def interpolate(arg):
            if arg == mrjob.step.JarStep.INPUT:
                return ','.join(self._hdfs_step_input_files(step_num))
            elif arg == mrjob.step.JarStep.OUTPUT:
                return self._hdfs_step_output_dir(step_num)
            else:
                return arg

        if step.get('args'):
            args.extend(interpolate(arg) for arg in step['args'])

        return args

    def _hdfs_step_input_files(self, step_num):
        """Get the hdfs:// URI for input for the given step."""
        if step_num == 0:
            return [self._upload_mgr.uri(p)
                    for p in self._get_input_paths()]
        else:
            return [posixpath.join(
                self._hadoop_tmp_dir, 'step-output', str(step_num))]

    def _hdfs_step_output_dir(self, step_num):
        if step_num == len(self._get_steps()) - 1:
            return self._output_dir
        else:
            return posixpath.join(
                self._hadoop_tmp_dir, 'step-output', str(step_num + 1))

    def _cleanup_local_tmp(self):
        super(HadoopJobRunner, self)._cleanup_local_tmp()

        if self._hadoop_tmp_dir:
            log.info('deleting %s from HDFS' % self._hadoop_tmp_dir)
            try:
                self.fs.rm(self._hadoop_tmp_dir)
            except Exception as e:
                log.exception(e)

    ### LOG FETCHING/PARSING ###

    def _enforce_path_regexp(self, paths, regexp, step_nums):
        """Helper for log fetching functions to filter out unwanted
        logs. Keyword arguments are checked against their corresponding
        regex groups.
        """
        for path in paths:
            m = regexp.match(path)
            if (m and
                (step_nums is None or
                 int(m.group('step_num')) in step_nums) and
                (self._job_timestamp is None or
                 m.group('timestamp') == self._job_timestamp)):

                yield path

    def _ls_logs(self, log_type, step_nums=None):
        """List logs on the local filesystem by path relative to log root
        directory
        """
        return []
        # in YARN, you can just ask the yarn bin:
        # http://hortonworks.com/blog/simplifying-user-logs-management-and-access-in-yarn/  # noqa

        # TODO: redo this to look in
        # $HADOOP_LOG_DIR
        # dirname(hadoop_bin[0])/../logs
        # <output_dir>/_logs
        # ??? other places ???

    def _fetch_counters(self, step_nums, skip_s3_wait=False):
        """Read Hadoop counters from local logs.

        Args:
        step_nums -- the steps belonging to us, so that we can ignore errors
                     from other jobs run with the same timestamp
        """
        uris = self._ls_logs('job', step_nums)
        new_counters = scan_for_counters_in_files(uris, self,
                                                  self.get_hadoop_version())

        # only include steps relevant to the current job
        for step_num in step_nums:
            self._counters.append(new_counters.get(step_num, {}))

    def counters(self):
        return self._counters

    def _find_probable_cause_of_failure(self, step_nums):
        task_attempt_logs = self._ls_logs('task')
        step_logs = self._ls_logs('step')
        job_logs = self._ls_logs('job')

        log.info('Scanning logs for probable cause of failure')
        return best_error_from_logs(self, task_attempt_logs, step_logs,
                                    job_logs)
