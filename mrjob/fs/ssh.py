# Copyright 2009-2012 Yelp and Contributors
# Copyright 2015-2016 Yelp
# Copyright 2017 Yelp
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
import os
import re
from subprocess import Popen
from subprocess import PIPE

from mrjob.cat import decompress
from mrjob.fs.base import Filesystem
from mrjob.py2 import to_unicode
from mrjob.util import cmd_line
from mrjob.util import random_identifier


_SSH_URI_RE = re.compile(
    r'^ssh://(?P<hostname>[^/]+)?(?P<filesystem_path>/.*)$')

log = logging.getLogger(__name__)


class SSHFilesystem(Filesystem):
    """Filesystem for remote systems accessed via SSH. Typically you will get
    one of these via ``EMRJobRunner().fs``, composed with
    :py:class:`~mrjob.fs.s3.S3Filesystem` and
    :py:class:`~mrjob.fs.local.LocalFilesystem`.
    """

    def __init__(self, ssh_bin, ec2_key_pair_file):
        """
        :param ssh_bin: path to ``ssh`` binary
        :param ec2_key_pair_file: path to an SSH keyfile
        """
        super(SSHFilesystem, self).__init__()
        self._ssh_bin = ssh_bin
        self._ec2_key_pair_file = ec2_key_pair_file
        if self._ec2_key_pair_file is None:
            raise ValueError('ec2_key_pair_file must be a path')

        # use this name for all remote copies of the key pair file
        self._remote_key_pair_file = '.mrjob-%s.pem' % random_identifier()

        # keep track of hosts we've already copied the key pair to
        self._hosts_with_key_pair_file = set()

        # keep track of which hosts we've copied our key to, and
        # what the (random) name of the key file is on that host
        self._host_to_key_filename = {}

        # should we use sudo (for EMR)? Enable with use_sudo_over_ssh().
        self._sudo = False

    def _ssh_cmd_args(self, address, cmd_args):
        """Return an ssh command that would run the given command on
        the given *address*.

        Address consists of one or most hosts, joined by '!' (so that
        we can reach hosts only accessible through an internal network).
         Before running the command returned, you should run
        ``self._copy_key_pair_files(address)`` to ensure that it's possible
        to ssh from the first host in *address*.

        We assume that any host we SSH into is a UNIX system, and that
        we don't need sudo to run ssh itself. We also assume the username
        is always ``hadoop``.
        """
        args = []

        for i, host in enumerate(address.split('!')):

            if i == 0:
                key_pair_file = self._ec2_key_pair_file
                known_hosts_file = os.devnull
            else:
                key_pair_file = self._remote_key_pair_file
                known_hosts_file = '/dev/null'

            args.extend(
                self._ssh_bin + [
                    '-i', key_pair_file,
                    '-o', 'UserKnownHostsFile=' + known_hosts_file,
                    '-o', 'StrictHostKeyChecking=no',
                    '-o', 'VerifyHostKeyDNS=no',
                    'hadoop@' + host,
                ]
            )

        if self._sudo:
            args.append('sudo')

        args.extend(cmd_args)

        return args

    def _ssh_launch(self, address, cmd_args, stdin=None):
        """Copy SSH keys if necessary, then launch the given command
        over SSH and return a Popen."""
        self._ssh_copy_key(address)

        args = self._ssh_cmd_args(address, cmd_args)

        log.debug('  > ' + cmd_line(args))
        try:
            return Popen(args, stdout=PIPE, stderr=PIPE, stdin=stdin)
        except OSError as ex:
            raise IOError(ex.strerror)

    def _ssh_run(self, address, cmd_args, stdin=None):
        """Run the given SSH command, and raise an IOError if it fails.
        Return ``(stdout, stderr)``

        Use this for commands with a bounded amount of output.
        """
        p = self._ssh_launch(address, cmd_args, stdin=stdin)

        stdout, stderr = p.communicate()

        if p.returncode != 0:
            raise IOError(to_unicode(stderr))

        return stdout, stderr

    def _ssh_finish_run(self, p):
        """Close file handles and do error handling on a ``Popen``
        who we've read stdout from but done nothing else."""
        stderr = p.stderr.read()

        p.stdout.close()
        p.stderr.close()

        returncode = p.wait()

        if returncode != 0:
            raise IOError(stderr)

    def _ssh_copy_key(self, address):
        """Copy ``self._ec2_key_pair_file`` to all hosts in *address*
        that need it. ``'master!core1!foo'``, we'll first copy a key
        pair file to ``core1`` (via ``master``) and then to ``foo``
        (via ``master`` and ``core``).

        If there isn't a ``!`` in *address*, do nothing.

        """
        if '!' not in address:
            return

        key_addr = '!'.join(address.split('!')[:-1])

        if key_addr not in self._hosts_with_key_pair_file:
            key_pair_file = self._remote_key_pair_file
            cmd_args = [
                'sh', '-c', 'cat > %s && chmod 600 %s' % (
                    key_pair_file, key_pair_file)]

            with open(self._ec2_key_pair_file, 'rb') as key_pair:
                # any previous hosts in the chain will get key pairs first
                # because _ssh_run() calls _ssh_copy_key()
                self._ssh_run(key_addr, cmd_args, stdin=key_pair)

            self._hosts_with_key_pair_file.add(key_addr)

    def can_handle_path(self, path):
        return _SSH_URI_RE.match(path) is not None

    def du(self, path_glob):
        raise IOError()  # not implemented

    def ls(self, uri_glob):
        m = _SSH_URI_RE.match(uri_glob)
        addr = m.group('hostname')
        path_to_ls = m.group('filesystem_path')

        p = self._ssh_launch(
            addr, ['find', '-L', path_to_ls, '-type', 'f'])

        for line in p.stdout:
            path = to_unicode(line).rstrip('\n')
            yield 'ssh://%s%s' % (addr, path)

        self._ssh_finish_run(p)

    def md5sum(self, path):
        raise IOError()  # not implemented

    def _cat_file(self, filename):
        m = _SSH_URI_RE.match(filename)
        addr = m.group('hostname')
        path = m.group('filesystem_path')

        p = self._ssh_launch(addr, ['cat', path])

        for chunk in decompress(p.stdout, path):
            yield chunk

        self._ssh_finish_run(p)

    def mkdir(self, dest):
        raise IOError()  # not implemented

    def exists(self, path_glob):
        # just fall back on ls(); it's smart
        try:
            return any(self.ls(path_glob))
        except IOError:
            return False

    def rm(self, path_glob):
        raise IOError()  # not implemented

    def touchz(self, dest):
        raise IOError()  # not implemented

    def use_sudo_over_ssh(self, sudo=True):
        """Use this to turn on *sudo* (we do this depending on the AMI
        version on EMR)."""
        self._sudo = sudo
