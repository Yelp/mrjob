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
import logging
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

from mrjob.fs.base import Filesystem
from mrjob.parse import is_uri
from mrjob.parse import urlparse
from mrjob.util import cmd_line
from mrjob.util import read_file


log = logging.getLogger('mrjob.fs.hadoop')

# used by mkdir()
HADOOP_FILE_EXISTS_RE = re.compile(r'.*File exists.*')

# used by ls()
HADOOP_LSR_NO_SUCH_FILE = re.compile(
    r'^lsr: Cannot access .*: No such file or directory.')

# used by rm() (see below)
HADOOP_RMR_NO_SUCH_FILE = re.compile(r'^rmr: hdfs://.*$')


class HadoopFilesystem(Filesystem):
    """Filesystem for URIs accepted by ``hadoop fs``. Typically you will get
    one of these via ``HadoopJobRunner().fs``, composed with
    :py:class:`~mrjob.fs.local.LocalFilesystem`.
    """

    def __init__(self, hadoop_bin):
        """:param hadoop_bin: path to ``hadoop`` binary"""
        super(HadoopFilesystem, self).__init__()
        self._hadoop_bin = hadoop_bin

    def can_handle_path(self, path):
        return is_uri(path)

    def invoke_hadoop(self, args, ok_returncodes=None, ok_stderr=None,
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
        args = self._hadoop_bin + args

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

    def du(self, path_glob):
        """Get the size of a file, or None if it's not a file or doesn't
        exist."""
        try:
            stdout = self.invoke_hadoop(['fs', '-dus', path_glob],
                                        return_stdout=True)
        except CalledProcessError:
            raise IOError(path_glob)

        try:
            return sum(int(line.split()[1])
                       for line in stdout.split('\n')
                       if line.strip())
        except (ValueError, TypeError, IndexError):
            raise IOError(
                'Unexpected output from hadoop fs -du: %r' % stdout)

    def ls(self, path_glob):
        components = urlparse(path_glob)
        hdfs_prefix = '%s://%s' % (components.scheme, components.netloc)

        try:
            stdout = self.invoke_hadoop(
                ['fs', '-lsr', path_glob],
                return_stdout=True,
                ok_stderr=[HADOOP_LSR_NO_SUCH_FILE])
        except CalledProcessError:
            raise IOError("Could not ls %s" % path_glob)

        for line in StringIO(stdout):
            line = line.rstrip('\r\n')
            fields = line.split(' ')

            # Throw out directories
            if fields[0].startswith('d'):
                continue

            # Try to figure out which part of the line is the path
            # Expected lines:
            # -rw-r--r--   3 dave users       3276 2010-01-13 14:00 /foo/bar # HDFS
            # -rwxrwxrwx   1          3276 010-01-13 14:00 /foo/bar # S3
            path_index = None
            for index, field in enumerate(fields):
                if len(field) == 5 and field[2] == ':':
                    path_index = (index + 1)
            if not path_index:
                raise IOError("Could not locate path in string '%s'" % line)

            path = line.split(' ', path_index)[-1]
            # handle fully qualified URIs from newer versions of Hadoop ls
            # (see Pull Request #577)
            if is_uri(path):
                yield path
            else:
                yield hdfs_prefix + path

    def _cat_file(self, filename):
        # stream from HDFS
        cat_args = self._hadoop_bin + ['fs', '-cat', filename]
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
                raise IOError("Could not stream %s" % filename)

        return read_file(filename, stream())

    def mkdir(self, path):
        try:
            self.invoke_hadoop(
                ['fs', '-mkdir', path], ok_stderr=[HADOOP_FILE_EXISTS_RE])
        except CalledProcessError:
            raise IOError("Could not mkdir %s" % path)

    def path_exists(self, path_glob):
        """Does the given path exist?

        If dest is a directory (ends with a "/"), we check if there are
        any files starting with that path.
        """
        try:
            return_code = self.invoke_hadoop(['fs', '-ls', path_glob],
                                             ok_returncodes=[0,-1,255])
            return (return_code == 0)
        except CalledProcessError:
            raise IOError("Could not check path %s" % path_glob)

    def path_join(self, dirname, filename):
        return posixpath.join(dirname, filename)

    def rm(self, path_glob):
        if not is_uri(path_glob):
            super(HadoopFilesystem, self).rm(path_glob)

        if self.path_exists(path_glob):
            # hadoop fs -rmr will print something like:
            # Moved to trash: hdfs://hdnamenode:54310/user/dave/asdf
            # to STDOUT, which we don't care about.
            #
            # if we ask to delete a path that doesn't exist, it prints
            # to STDERR something like:
            # rmr: <path>
            # which we can safely ignore
            try:
                self.invoke_hadoop(
                    ['fs', '-rmr', path_glob],
                    return_stdout=True, ok_stderr=[HADOOP_RMR_NO_SUCH_FILE])
            except CalledProcessError:
                raise IOError("Could not rm %s" % path_glob)

    def touchz(self, dest):
        try:
            self.invoke_hadoop(['fs', '-touchz', dest])
        except CalledProcessError:
            raise IOError("Could not touchz %s" % dest)
