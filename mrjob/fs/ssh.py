import fnmatch
import logging
import os
import posixpath

try:
    from cStringIO import StringIO
    StringIO  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    from StringIO import StringIO

from mrjob.ssh import ssh_cat
from mrjob.ssh import ssh_copy_key
from mrjob.ssh import ssh_ls
from mrjob.ssh import SSHException
from mrjob.ssh import SSH_PREFIX
from mrjob.ssh import SSH_URI_RE
from mrjob.util import buffer_iterator_to_line_iterator
from mrjob.util import read_file


log = logging.getLogger('mrjob.fs.ssh')


class S3Filesystem(LocalFilesystem):

    def __init__(self, opts, aws_region, ssh_key_name):
        super(S3Filesystem, self).__init__()
        self._opts = opts
        self._aws_region = aws_region
        self._ssh_key_name = ssh_key_name

        # this will be populated by EMRJobRunner when possible
        self.address_of_master = None

    def can_handle_path(self):
        return SSH_URI_RE.match(filename) is not None

    def du(self, path_glob):
        raise IOError() # not implemented

    def ls(self, path_glob):
        if SSH_URI_RE.match(path_glob):
            for item in self._ssh_ls(path_glob):
                yield item
            return

    def _ssh_ls(self, uri):
        """Helper for ls(); obeys globbing"""
        m = SSH_URI_RE.match(uri)
        try:
            addr = m.group('hostname')

            output = ssh_ls(
                self._opts['ssh_bin'],
                addr,
                self._opts['ec2_key_pair_file'],
                m.group('filesystem_path'),
                self._ssh_key_name,
            )

            for line in output:
                # skip directories, we only want to return downloadable files
                if line and not line.endswith('/'):
                    yield SSH_PREFIX + addr + line
        except SSHException, e:
            raise IOError(e)

    def md5sum(self, path, s3_conn=None):
        raise IOError() # not implemented

    def _cat_file(self, filename):
        ssh_match = SSH_URI_RE.match(filename)
        try:
            addr = ssh_match.group('hostname') or self._address_of_master()
            if '!' in addr:
                self._enable_slave_ssh_access()
            output = ssh_cat(
                self._opts['ssh_bin'],
                addr,
                self._opts['ec2_key_pair_file'],
                ssh_match.group('filesystem_path'),
                self._ssh_key_name,
            )
            return read_file(filename, fileobj=StringIO(output))
        except SSHException, e:
            raise IOError(e)

    def mkdir(self, dest):
        raise IOError() # not implemented

    def path_exists(self, path_glob):
        # just fall back on ls(); it's smart
        return any(self.ls(path_glob))

    def path_join(self, dirname, filename):
        return posixpath.join(dirname, filename)

    def rm(self, path_glob):
        raise IOError() # not implemented

    def touchz(self, dest):
        raise IOError() # not implemented
