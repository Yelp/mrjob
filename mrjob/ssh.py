# Copyright 2009-2011 Yelp
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
from __future__ import with_statement

from collections import defaultdict
import os
from subprocess import Popen, PIPE


SSH_PREFIX = 'ssh://'
SSH_LOG_ROOT = '/mnt/var/log/hadoop'


class SSHException(Exception):
    pass


def _ssh_args(ssh_bin, address, ec2_key_pair_file):
    """Helper method for :py:func:`ssh_run` to build an argument list for
    ``subprocess``. Specifies an identity, disables strict host key checking,
    and adds the ``hadoop`` username.
    """
    return ssh_bin + [
        '-i', ec2_key_pair_file,
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        'hadoop@%s' % (address,),
    ]


def ssh_run(ssh_bin, address, ec2_key_pair_file, cmd_args, stdin=''):
    """Shortcut to call ssh on a Hadoop node via ``subprocess``.

    :param ssh_bin: Path to ``ssh`` binary
    :param address: Address of your job's master node (obtained via EmrConnection.describe_jobflow())
    :param ec2_key_pair_file: Path to the key pair file (argument to ``-i``)
    :param cmd_args: The command you want to run
    :param stdin: String to pass to the process's standard input

    :return: (stdout, stderr)
    """
    args = _ssh_args(ssh_bin, address, ec2_key_pair_file) + list(cmd_args)
    # print ' '.join(args)
    p = Popen(args, stdout=PIPE, stderr=PIPE, stdin=PIPE)
    out, err = p.communicate(stdin)

    if err:
        if 'No such file or directory' in err:
            raise IOError(err)
        elif 'Warning: Permanently added' not in err:
            raise SSHException(err)

    if 'Permission denied' in out:
        raise SSHException(out)

    return out


def _ssh_run_with_recursion(ssh_bin, address, ec2_key_pair_file,
                            keyfile, cmd_args):
    """Some files exist on the master and can be accessed directly via SSH,
    but some files are on the slaves which can only be accessed via the master
    node. To differentiate between hosts, we adopt the UUCP "bang path" syntax
    to specify "SSH hops." Specifically, ``host1!host2`` forms the command to
    be run on ``host2``, then wraps that in a call to ``ssh`` from ``host``,
    and finally executes that ``ssh`` call on ``host1`` from ``localhost``.

    Confused yet?

    For bang paths to work, :py:func:`ssh_copy_key` must have been run, and
    the ``keyfile`` argument must be the same as was passed to that function.
    """
    if '!' in address:
        host1, host2 = address.split('!')
        more_args = [
           'ssh', '-i', keyfile,
           '-o', 'StrictHostKeyChecking=no',
           '-o', 'UserKnownHostsFile=/dev/null',
           'hadoop@%s' % host2,
        ]
        return ssh_run(ssh_bin, host1, ec2_key_pair_file,
                       more_args + list(cmd_args))
    else:
        return ssh_run(ssh_bin, address, ec2_key_pair_file, cmd_args)



def _poor_mans_scp(ssh_bin, addr, ec2_key_pair_file, src, dest):
    """Copy a file from ``src`` on the local machine to ``dest`` on ``addr``.

    We use this to avoid having to remember where ``scp`` lives.
    """
    with open(src, 'rb') as f:
        args = ['bash -c "cat > %s" && chmod 600 %s' % (dest, dest)]
        ssh_run(ssh_bin, addr, ec2_key_pair_file, args, stdin=f.read())


def ssh_copy_key(ssh_bin, master_address, ec2_key_pair_file, keyfile):
    """Prepare master to SSH to slaves by copying the EMR private key to the
    master node.
    """
    if not ec2_key_pair_file or not os.path.exists(ec2_key_pair_file):
        return  # this is a testing environment

    _poor_mans_scp(ssh_bin, master_address, ec2_key_pair_file,
                   ec2_key_pair_file, keyfile)


def ssh_slave_addresses(ssh_bin, master_address, ec2_key_pair_file):
    """Get the IP addresses of the slave nodes. Fails silently because it
    makes testing easier and if things are broken they will fail before this
    function is called.
    """
    if not ec2_key_pair_file or not os.path.exists(ec2_key_pair_file):
        return []   # this is a testing environment

    cmd = "hadoop dfsadmin -report | grep ^Name | cut -f2 -d: | cut -f2 -d' '"
    args = ['bash -c "%s"' % cmd]
    ips = ssh_run(ssh_bin, master_address, ec2_key_pair_file, args)
    return [ip for ip in ips.split('\n') if ip]


def _handle_cat_out(out):
    """Detect errors in ``cat`` output"""
    if 'No such file or directory' in out:
        raise IOError("File not found: %s" % path)
    return out


_mock_ssh_cat_values = None
def ssh_cat(ssh_bin, address, ec2_key_pair_file, path, keyfile=None):
    """Return the file at ``path`` as a string. Raises ``IOError`` if the
    file doesn't exist or ``SSHException if SSH access fails.

    :param ssh_bin: Path to ``ssh`` binary
    :param address: Address of your job's master node (obtained via EmrConnection.describe_jobflow())
    :param ec2_key_pair_file: Path to the key pair file (argument to ``-i``)
    :param path: Path on the remote host to get
    :param keyfile: Name of the EMR private key file on the master node in case ``path`` exists on one of the slave nodes
    """
    # Allow mocking of output
    # Mocking kept separate from ssh_ls() to make testing more intuitive
    if _mock_ssh_cat_values is not None:
        if _mock_ssh_cat_values.has_key(path):
            return _handle_cat_out(_mock_ssh_cat_values[path])
        else:
            raise IOError('File not found: %s' % path)
    else:
        out = _ssh_run_with_recursion(ssh_bin, address, ec2_key_pair_file,
                                      keyfile, ['cat', path])
        return _handle_cat_out(out)


def mock_ssh_cat(new_values):
    """Pass in a dictionary mapping path (not including host) to a string"""
    global _mock_ssh_cat_values
    _mock_ssh_cat_values = new_values


def _handle_ls_out(out):
    """Detect errors in ``ls`` output"""
    if 'No such file or directory' in out:
        raise IOError("No such file or directory: %s" % path)
    return out.split('\n')


_mock_ssh_ls_values = None
def ssh_ls(ssh_bin, address, ec2_key_pair_file, path, keyfile=None):
    """Recursively list files under ``path`` on the specified SSH host.
    Return the file at ``path`` as a string. Raises ``IOError`` if the
    path doesn't exist or ``SSHException if SSH access fails.

    :param ssh_bin: Path to ``ssh`` binary
    :param address: Address of your job's master node (obtained via EmrConnection.describe_jobflow())
    :param ec2_key_pair_file: Path to the key pair file (argument to ``-i``)
    :param path: Path on the remote host to list
    :param keyfile: Name of the EMR private key file on the master node in case ``path`` exists on one of the slave nodes
    """
    # Allow mocking of output
    if _mock_ssh_ls_values is not None:
        if _mock_ssh_ls_values.has_key(path):
            return _handle_ls_out('\n'.join(_mock_ssh_ls_values[path]))
        else:
            raise IOError("No such file or directory: %s" % path)
    else:
        out = _ssh_run_with_recursion(ssh_bin, address, ec2_key_pair_file,
                                      keyfile, ['find', path, '-type', 'f'])
        return _handle_ls_out(out)


def mock_ssh_ls(new_values):
    """Pass in a dictionary mapping path (not including host) to a list of 
    strings
    """
    global _mock_ssh_ls_values
    _mock_ssh_ls_values = new_values


def mock_ssh_ls_empty():
    """For when you don't want to bother with logs of any sort. Necessary
    because EMRJobRunner.wait_for_job_flow_termination() never succeeds in
    tests if the job flow isn't already shut down.
    """
    global _mock_ssh_ls_values
    _mock_ssh_ls_values = defaultdict(list)
