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

try:
    from cStringIO import StringIO
    StringIO  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    from StringIO import StringIO

from mrjob.fs.base import Filesystem
from mrjob.ssh import ssh_cat
from mrjob.ssh import ssh_ls
from mrjob.ssh import SSH_PREFIX
from mrjob.ssh import SSH_URI_RE
from mrjob.util import read_file


log = logging.getLogger(__name__)


class SSHFilesystem(Filesystem):
    """Filesystem for remote systems accessed via SSH. Typically you will get
    one of these via ``EMRJobRunner().fs``, composed with
    :py:class:`~mrjob.fs.s3.S3Filesystem` and
    :py:class:`~mrjob.fs.local.LocalFilesystem`.
    """

    def __init__(self, ssh_bin, ec2_key_pair_file, key_name):
        """
        :param ssh_bin: path to ``ssh`` binary
        :param ec2_key_pair_file: path to an SSH keyfile
        :param key_name: Name of keyfile existing on servers, used to access
                         slaves after '!' in hostname. Generally set by
                         :py:class:`~mrjob.emr.EMRJobRunner`, which copies the
                         key itself, to use for log fetching.
        """
        super(SSHFilesystem, self).__init__()
        self._ssh_bin = ssh_bin
        self._ec2_key_pair_file = ec2_key_pair_file
        self.ssh_key_name = key_name
        if self._ec2_key_pair_file is None:
            raise ValueError('ec2_key_pair_file must be a path')

    def can_handle_path(self, path):
        return SSH_URI_RE.match(path) is not None

    def du(self, path_glob):
        raise IOError()  # not implemented

    def ls(self, path_glob):
        if SSH_URI_RE.match(path_glob):
            for item in self._ssh_ls(path_glob):
                yield item
            return

    def _ssh_ls(self, uri):
        """Helper for ls(); obeys globbing"""
        m = SSH_URI_RE.match(uri)
        addr = m.group('hostname')
        if not addr:
            raise ValueError

        if '!' in addr and self.ssh_key_name is None:
            raise ValueError('ssh_key_name must not be None')

        output = ssh_ls(
            self._ssh_bin,
            addr,
            self._ec2_key_pair_file,
            m.group('filesystem_path'),
            self.ssh_key_name,
        )

        for line in output:
            # skip directories, we only want to return downloadable files
            if line and not line.endswith('/'):
                yield SSH_PREFIX + addr + line

    def md5sum(self, path, s3_conn=None):
        raise IOError()  # not implemented

    def _cat_file(self, filename):
        ssh_match = SSH_URI_RE.match(filename)
        addr = ssh_match.group('hostname') or self._address_of_master()
        if '!' in addr and self.ssh_key_name is None:
            raise ValueError('ssh_key_name must not be None')
        output = ssh_cat(
            self._ssh_bin,
            addr,
            self._ec2_key_pair_file,
            ssh_match.group('filesystem_path'),
            self.ssh_key_name,
        )
        return read_file(filename, fileobj=StringIO(output))

    def mkdir(self, dest):
        raise IOError()  # not implemented

    def path_exists(self, path_glob):
        # just fall back on ls(); it's smart
        paths = self.ls(path_glob)
        try:
            path_exists = any(paths)
        except IOError:
            path_exists = False
        return path_exists

    def path_join(self, dirname, filename):
        return posixpath.join(dirname, filename)

    def rm(self, path_glob):
        raise IOError()  # not implemented

    def touchz(self, dest):
        raise IOError()  # not implemented
