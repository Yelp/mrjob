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

try:
    from cStringIO import StringIO
    StringIO  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    from StringIO import StringIO

from mrjob import compat
from mrjob.conf import combine_cmds
from mrjob.conf import combine_dicts
from mrjob.conf import combine_paths
from mrjob.logparsers import TASK_ATTEMPTS_LOG_URI_RE
from mrjob.logparsers import STEP_LOG_URI_RE
from mrjob.logparsers import HADOOP_JOB_LOG_URI_RE
from mrjob.logparsers import scan_for_counters_in_files
from mrjob.logparsers import scan_logs_in_order
from mrjob.parse import HADOOP_STREAMING_JAR_RE
from mrjob.parse import is_uri
from mrjob.parse import urlparse
from mrjob.runner import MRJobRunner
from mrjob.util import cmd_line
from mrjob.util import read_file


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


def hadoop_log_dir():
    """Return the path where Hadoop stores logs"""
    try:
        return os.environ['HADOOP_LOG_DIR']
    except KeyError:
        # Defaults to $HADOOP_HOME/logs
        # http://wiki.apache.org/hadoop/HowToConfigure
        return os.path.join(os.environ['HADOOP_HOME'], 'logs')


class HadoopJobRunner(MRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` on your Hadoop cluster.

    Input and support files can be either local or on HDFS; use ``hdfs://...``
    URLs to refer to files on HDFS.

    It's rare to need to instantiate this class directly (see
    :py:meth:`~HadoopJobRunner.__init__` for details).
    """
    alias = 'hadoop'

    def __init__(self, **kwargs):
        """:py:class:`~mrjob.hadoop.HadoopJobRunner` takes the same arguments
        as :py:class:`~mrjob.runner.MRJobRunner`, plus some additional options
        which can be defaulted in :ref:`mrjob.conf <mrjob.conf>`.

        *output_dir* and *hdfs_scratch_dir* need not be fully qualified
        ``hdfs://`` URIs because it's understood that they have to be on
        HDFS (e.g. ``tmp/mrjob/`` would be okay)

        Additional options:

        :type hadoop_bin: str or list
        :param hadoop_bin: name/path of your hadoop program (may include
                           arguments). Defaults to *hadoop_home* plus
                           ``bin/hadoop``.
        :type hadoop_home: str
        :param hadoop_home: alternative to setting the :envvar:`HADOOP_HOME`
                            environment variable
        :type hdfs_scratch_dir: str
        :param hdfs_scratch_dir: temp directory on HDFS. Default is
                                 ``tmp/mrjob``.

        *hadoop_streaming_jar* is optional; by default, we'll search for it
        inside :envvar:`HADOOP_HOME`
        """
        super(HadoopJobRunner, self).__init__(**kwargs)

        # fix hadoop_home
        if not self._opts['hadoop_home']:
            raise Exception(
                'you must set $HADOOP_HOME, or pass in hadoop_home explicitly')
        self._opts['hadoop_home'] = os.path.abspath(self._opts['hadoop_home'])

        # fix hadoop_bin
        if not self._opts['hadoop_bin']:
            self._opts['hadoop_bin'] = [
                os.path.join(self._opts['hadoop_home'], 'bin/hadoop')]

        # fix hadoop_streaming_jar
        if not self._opts['hadoop_streaming_jar']:
            log.debug('Looking for hadoop streaming jar in %s' %
                      self._opts['hadoop_home'])
            self._opts['hadoop_streaming_jar'] = find_hadoop_streaming_jar(
                self._opts['hadoop_home'])

            if not self._opts['hadoop_streaming_jar']:
                raise Exception(
                    "Couldn't find streaming jar in %s, bailing out" %
                    self._opts['hadoop_home'])

        log.debug('Hadoop streaming jar is %s' %
                  self._opts['hadoop_streaming_jar'])

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

        self._hadoop_log_dir = hadoop_log_dir()

        # Running jobs via hadoop assigns a new timestamp to each job.
        # Running jobs via mrjob only adds steps.
        # Store both of these values to enable log parsing.
        self._job_timestamp = None
        self._start_step_num = None

        # init hadoop version cache
        self._hadoop_version = None

    @classmethod
    def _allowed_opts(cls):
        """A list of which keyword args we can pass to __init__()"""
        return super(HadoopJobRunner, cls)._allowed_opts() + [
            'hadoop_bin',
            'hadoop_home',
            'hdfs_scratch_dir',
        ]

    @classmethod
    def _default_opts(cls):
        """A dictionary giving the default value of options."""
        return combine_dicts(super(HadoopJobRunner, cls)._default_opts(), {
            'hadoop_home': os.environ.get('HADOOP_HOME'),
            'hdfs_scratch_dir': 'tmp/mrjob',
        })

    @classmethod
    def _opts_combiners(cls):
        """Map from option name to a combine_*() function used to combine
        values for that option. This allows us to specify that some options
        are lists, or contain environment variables, or whatever."""
        return combine_dicts(super(HadoopJobRunner, cls)._opts_combiners(), {
            'hadoop_bin': combine_cmds,
            'hadoop_home': combine_paths,
            'hdfs_scratch_dir': combine_paths,
        })

    def get_hadoop_version(self):
        """Invoke the hadoop executable to determine its version"""
        if not self._hadoop_version:
            stdout = self._invoke_hadoop(['version'], return_stdout=True)
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
        self._invoke_hadoop(['fs', '-mkdir', path])

    def _upload_to_hdfs(self, path, target):
        log.debug('Uploading %s -> %s on HDFS' % (path, target))
        self._invoke_hadoop(['fs', '-put', path, target])

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

        version = self.get_hadoop_version()

        for step_num, step in enumerate(steps):
            log.debug('running step %d of %d' % (step_num + 1, len(steps)))

            streaming_args = (self._opts['hadoop_bin'] +
                              ['jar', self._opts['hadoop_streaming_jar']])

            # -files/-archives (generic options, new-style)
            if compat.supports_new_distributed_cache_options(version):
                # set up uploading from HDFS to the working dir
                streaming_args.extend(self._upload_args())

            # Add extra hadoop args first as hadoop args could be a hadoop
            # specific argument (e.g. -libjar) which must come before job
            # specific args.
            streaming_args.extend(
                self._hadoop_conf_args(step_num, len(steps)))

            # set up input
            for input_uri in self._hdfs_step_input_files(step_num):
                streaming_args.extend(['-input', input_uri])

            # set up output
            streaming_args.append('-output')
            streaming_args.append(self._hdfs_step_output_dir(step_num))

            # -cacheFile/-cacheArchive (streaming options, old-style)
            if not compat.supports_new_distributed_cache_options(version):
                # set up uploading from HDFS to the working dir
                streaming_args.extend(self._upload_args())

            # set up mapper and reducer
            if 'M' not in step:
                mapper = 'cat'
            else:
                mapper = cmd_line(self._mapper_args(step_num))

            if 'C' in step:
                combiner_cmd = cmd_line(self._combiner_args(step_num))
                version = self.get_hadoop_version()
                if compat.supports_combiners_in_hadoop_streaming(version):
                    combiner = combiner_cmd
                else:
                    mapper = ("bash -c '%s | sort | %s'" %
                              (mapper, combiner_cmd))
                    combiner = None
            else:
                combiner = None

            streaming_args.append('-mapper')
            streaming_args.append(mapper)

            if combiner:
                streaming_args.append('-combiner')
                streaming_args.append(combiner)

            if 'R' in step:
                streaming_args.append('-reducer')
                streaming_args.append(cmd_line(self._reducer_args(step_num)))
            else:
                streaming_args.extend(['-jobconf', 'mapred.reduce.tasks=0'])

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

    def _script_args(self):
        """How to invoke the script inside Hadoop"""
        assert self._script  # shouldn't be able to run if no script

        args = self._opts['python_bin'] + [self._script['name']]
        if self._wrapper_script:
            args = (self._opts['python_bin'] +
                    [self._wrapper_script['name']]
                    + args)

        return args

    def _mapper_args(self, step_num):
        return (self._script_args() +
                ['--step-num=%d' % step_num, '--mapper'] +
                self._mr_job_extra_args())

    def _combiner_args(self, step_num):
        return (self._script_args() +
                ['--step-num=%d' % step_num, '--combiner'] +
                self._mr_job_extra_args())

    def _reducer_args(self, step_num):
        return (self._script_args() +
                ['--step-num=%d' % step_num, '--reducer'] +
                self._mr_job_extra_args())

    def _upload_args(self):
        """Args to upload files from HDFS to the hadoop nodes."""
        args = []

        version = self.get_hadoop_version()

        if compat.supports_new_distributed_cache_options(version):

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

        else:
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

    def _invoke_hadoop(self, args, ok_returncodes=None, ok_stderr=None,
                       return_stdout=False):
        """Run the given hadoop command, raising an exception on non-zero
        return code. This only works for commands whose output we don't
        care about.

        Args:
        ok_returncodes -- a list/tuple/set of return codes we expect to
            get back from hadoop (e.g. [0,1]). By default, we only expect 0.
            If we get an unexpected return code, we raise a CalledProcessError.
        ok_stderr -- don't log STDERR or raise CalledProcessError if stderr
            matches a regex in this list (even if the returncode is bad)
        return_stdout -- return the stdout from the hadoop command rather
            than logging it. If this is False, we return the returncode
            instead.
        """
        args = self._opts['hadoop_bin'] + args

        log.debug('> %s' % cmd_line(args))

        proc = Popen(args, stdout=PIPE, stderr=PIPE)
        stdout, stderr = proc.communicate()

        log_func = log.debug if proc.returncode == 0 else log.error
        if not return_stdout:
            for line in StringIO(stdout):
                log_func('STDOUT: ' + line.rstrip('\r\n'))

        # check if STDERR is okay
        stderr_is_ok = False
        if ok_stderr:
            for stderr_re in ok_stderr:
                if stderr_re.match(stderr):
                    stderr_is_ok = True
                    break

        if not stderr_is_ok:
            for line in StringIO(stderr):
                log_func('STDERR: ' + line.rstrip('\r\n'))

        ok_returncodes = ok_returncodes or [0]

        if not stderr_is_ok and proc.returncode not in ok_returncodes:
            raise CalledProcessError(proc.returncode, args)

        if return_stdout:
            return stdout
        else:
            return proc.returncode

    def _cleanup_local_scratch(self):
        super(HadoopJobRunner, self)._cleanup_local_scratch()

        if self._hdfs_tmp_dir:
            log.info('deleting %s from HDFS' % self._hdfs_tmp_dir)

            try:
                self._invoke_hadoop(['fs', '-rmr', self._hdfs_tmp_dir])
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

    ### FILESYSTEM STUFF ###

    def du(self, path_glob):
        """Get the size of a file, or None if it's not a file or doesn't
        exist."""
        if not is_uri(path_glob):
            return super(HadoopJobRunner, self).du(path_glob)

        stdout = self._invoke_hadoop(['fs', '-dus', path_glob],
                                     return_stdout=True)

        try:
            return sum(int(line.split()[1])
                       for line in stdout.split('\n')
                       if line.strip())
        except (ValueError, TypeError, IndexError):
            raise Exception(
                'Unexpected output from hadoop fs -du: %r' % stdout)

    def ls(self, path_glob):
        if not is_uri(path_glob):
            for path in super(HadoopJobRunner, self).ls(path_glob):
                yield path
            return

        components = urlparse(path_glob)
        hdfs_prefix = '%s://%s' % (components.scheme, components.netloc)

        stdout = self._invoke_hadoop(
            ['fs', '-lsr', path_glob],
            return_stdout=True,
            ok_stderr=[HADOOP_LSR_NO_SUCH_FILE])

        for line in StringIO(stdout):
            fields = line.rstrip('\r\n').split()
            # expect lines like:
            # -rw-r--r--   3 dave users       3276 2010-01-13 14:00 /foo/bar
            if len(fields) < 8:
                raise Exception('unexpected ls line from hadoop: %r' % line)
            # ignore directories
            if fields[0].startswith('d'):
                continue
            # not sure if you can have spaces in filenames; just to be safe
            path = ' '.join(fields[7:])
            yield hdfs_prefix + path

    def _cat_file(self, filename):
        if is_uri(filename):
            # stream from HDFS
            cat_args = self._opts['hadoop_bin'] + ['fs', '-cat', filename]
            log.debug('> %s' % cmd_line(cat_args))

            cat_proc = Popen(cat_args, stdout=PIPE, stderr=PIPE)

            def stream():
                for line in cat_proc.stdout:
                    yield line

                # there shouldn't be any stderr
                for line in cat_proc.stderr:
                    log.error('STDERR: ' + line)

                returncode = cat_proc.wait()

                if returncode != 0:
                    raise CalledProcessError(returncode, cat_args)

            return read_file(filename, stream())
        else:
            # read from local filesystem
            return super(HadoopJobRunner, self)._cat_file(filename)

    def mkdir(self, path):
        self._invoke_hadoop(
            ['fs', '-mkdir', path], ok_stderr=[HADOOP_FILE_EXISTS_RE])

    def path_exists(self, path_glob):
        """Does the given path exist?

        If dest is a directory (ends with a "/"), we check if there are
        any files starting with that path.
        """
        if not is_uri(path_glob):
            return super(HadoopJobRunner, self).path_exists(path_glob)

        return bool(self._invoke_hadoop(['fs', '-test', '-e', path_glob],
                                        ok_returncodes=(0, 1)))

    def path_join(self, dirname, filename):
        if is_uri(dirname):
            return posixpath.join(dirname, filename)
        else:
            return os.path.join(dirname, filename)

    def rm(self, path_glob):
        if not is_uri(path_glob):
            super(HadoopJobRunner, self).rm(path_glob)

        if self.path_exists(path_glob):
            # hadoop fs -rmr will print something like:
            # Moved to trash: hdfs://hdnamenode:54310/user/dave/asdf
            # to STDOUT, which we don't care about.
            #
            # if we ask to delete a path that doesn't exist, it prints
            # to STDERR something like:
            # rmr: <path>
            # which we can safely ignore
            self._invoke_hadoop(
                ['fs', '-rmr', path_glob],
                return_stdout=True, ok_stderr=[HADOOP_RMR_NO_SUCH_FILE])

    def touchz(self, dest):
        if not is_uri(dest):
            super(HadoopJobRunner, self).touchz(dest)

        self._invoke_hadoop(['fs', '-touchz', dest])
