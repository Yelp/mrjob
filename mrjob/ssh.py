# Copyright 2009-2012 Yelp
# Copyright 2013-2016 Yelp and Contributors
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
"""Shortcuts for SSH operations"""

# don't add imports here that aren't part of the standard Python library,
# since MRJobs need to run in Amazon's generic EMR environment

import logging
import os
from subprocess import Popen
from subprocess import PIPE

from mrjob.py2 import to_string
from mrjob.util import cmd_line

log = logging.getLogger(__name__)


def _ssh_args(ssh_bin, address, ec2_key_pair_file):
    """Helper method for :py:func:`_ssh_run` to build an argument list for
    ``subprocess``. Specifies an identity, disables strict host key checking,
    and adds the ``hadoop`` username.
    """
    if ec2_key_pair_file is None:
        raise ValueError('SSH key file path is None')
    return ssh_bin + [
        '-i', ec2_key_pair_file,
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        'hadoop@%s' % (address,),
    ]


def _check_output(out, err):
    if err:
        if (b'No such file or directory' in err or
            b'Warning: Permanently added' not in err):  # noqa

            raise IOError(err.rstrip())

    if b'Permission denied' in out:
        raise IOError(out.rstrip())

    return out


def _ssh_run(ssh_bin, address, ec2_key_pair_file, cmd_args, stdin=''):
    """Shortcut to call ssh on a Hadoop node via ``subprocess``.

    :param ssh_bin: Path to ``ssh`` binary
    :param address: Address of your job's master node
    :param ec2_key_pair_file: Path to the key pair file (argument to ``-i``)
    :param cmd_args: The command you want to run
    :param stdin: String to pass to the process's standard input

    :return: (stdout, stderr)
    """
    args = _ssh_args(ssh_bin, address, ec2_key_pair_file) + list(cmd_args)
    log.debug('> %s' % cmd_line(args))
    p = Popen(args, stdout=PIPE, stderr=PIPE, stdin=PIPE)
    return p.communicate(stdin)


def _ssh_run_with_recursion(ssh_bin, address, ec2_key_pair_file, keyfile,
                            cmd_args):
    """Some files exist on the master and can be accessed directly via SSH,
    but some files are on the slaves which can only be accessed via the master
    node. To differentiate between hosts, we adopt the UUCP "bang path" syntax
    to specify "SSH hops." Specifically, ``host1!host2`` forms the command to
    be run on ``host2``, then wraps that in a call to ``ssh`` from ``host``,
    and finally executes that ``ssh`` call on ``host1`` from ``localhost``.

    Confused yet?

    For bang paths to work, :py:func:`_ssh_copy_key` must have been run, and
    the ``keyfile`` argument must be the same as was passed to that function.
    """
    if '!' in address:
        if keyfile is None:
            raise ValueError('SSH key file path cannot be None')
        host1, host2 = address.split('!')
        more_args = [
            'ssh', '-i', keyfile,
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'UserKnownHostsFile=/dev/null',
            'hadoop@%s' % (host2,),
        ]
        return _ssh_run(ssh_bin, host1, ec2_key_pair_file,
                        more_args + list(cmd_args))
    else:
        return _ssh_run(ssh_bin, address, ec2_key_pair_file, cmd_args)


def _ssh_copy_key(ssh_bin, master_address, ec2_key_pair_file, keyfile):
    """Prepare master to SSH to slaves by copying the EMR private key to the
    master node. This is done via ``cat`` to avoid having to store an
    ``scp_bin`` variable.

    :param ssh_bin: Path to ``ssh`` binary
    :param master_address: Address of node to copy keyfile to
    :param ec2_key_pair_file: Path to the key pair file (argument to ``-i``)
    :param keyfile: What to call the key file on the master
    """
    with open(ec2_key_pair_file, 'rb') as f:
        args = ['bash -c "cat > %s" && chmod 600 %s' % (keyfile, keyfile)]
        _check_output(*_ssh_run(ssh_bin, master_address, ec2_key_pair_file,
                                args, stdin=f.read()))


def _ssh_slave_addresses(ssh_bin, master_address, ec2_key_pair_file):
    """Get the IP addresses of the slave nodes. Fails silently because it
    makes testing easier and if things are broken they will fail before this
    function is called.
    """
    if not ec2_key_pair_file or not os.path.exists(ec2_key_pair_file):
        return []   # this is a testing environment

    cmd = "hadoop dfsadmin -report | grep ^Name | cut -f2 -d: | cut -f2 -d' '"
    args = ['bash -c "%s"' % cmd]
    ips = to_string(_check_output(
        *_ssh_run(ssh_bin, master_address, ec2_key_pair_file, args)))
    return [ip for ip in ips.split('\n') if ip]


def _ssh_cat(ssh_bin, address, ec2_key_pair_file, path,
             keyfile=None, sudo=False):
    """Return the file at ``path`` as a string. Raises ``IOError`` if the
    file doesn't exist or SSH access fails.

    :param ssh_bin: Path to ``ssh`` binary
    :param address: Address of your job's master node
    :param ec2_key_pair_file: Path to the key pair file (argument to ``-i``)
    :param path: Path on the remote host to get
    :param keyfile: Name of the EMR private key file on the master node in case
                    ``path`` exists on one of the slave nodes
    :param sudo: if true, run command with ``sudo``
    """
    cmd_args = ['cat', path]
    if sudo:
        cmd_args = ['sudo'] + cmd_args

    out = _check_output(*_ssh_run_with_recursion(
        ssh_bin, address, ec2_key_pair_file, keyfile, cmd_args))
    return out


def _ssh_ls(ssh_bin, address, ec2_key_pair_file, path,
            keyfile=None, sudo=False):
    """Recursively list files under ``path`` on the specified SSH host.
    Return the file at ``path`` as a string. Raises ``IOError`` if the
    path doesn't exist or SSH access fails.

    :param ssh_bin: Path to ``ssh`` binary
    :param address: Address of your job's master node
    :param ec2_key_pair_file: Path to the key pair file (argument to ``-i``)
    :param path: Path on the remote host to list
    :param keyfile: Name of the EMR private key file on the master node in case
                    ``path`` exists on one of the slave nodes
    :param sudo: if true, run command with ``sudo``
    """
    cmd_args = ['find', '-L', path, '-type', 'f']
    if sudo:
        cmd_args = ['sudo'] + cmd_args

    out = to_string(_check_output(*_ssh_run_with_recursion(
        ssh_bin, address, ec2_key_pair_file, keyfile, cmd_args)))
    if 'No such file or directory' in out:
        raise IOError("No such file or directory: %s" % path)
    return out.split('\n')
