# Copyright 2009-2012 Yelp
# Copyright 2014 Ed Schofield
# Copyright 2015-2016 Yelp
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
"""A mock version of the ssh binary that actually manipulates the
filesystem. This imitates only things that mrjob actually uses.

Relies on these environment variables:
MOCK_SSH_ROOTS -- specify directories for hosts in the form:
                  host1=/tmp/dir1:host2=/tmp/dir2
MOCK_SSH_VERIFY_KEY_FILE -- set to 'true' if the script should print an error
                            when the key file does not exist

You can optionally set MOCK_SSH_REQUIRES_SUDO to 1 (or any nonempty value)
to raise an error unless ls and cat are preceded by sudo.

This is designed to run as: python -m tests.mockssh <ssh args>

mrjob requires a single binary (no args) to stand in for ssh, so
use create_mock_hadoop_script() to write out a shell script that runs
mockssh.
"""
from __future__ import print_function

import os
import pipes
import posixpath
import re
import stat
import sys


def create_mock_ssh_script(path):
    """Dump a wrapper script to the given file object that runs this
    python script."""
    # make this work even if $PATH or $PYTHONPATH changes
    with open(path, 'w') as f:
        f.write('#!/bin/sh\n')
        f.write('%s %s "$@"\n' % (
            pipes.quote(sys.executable),
            pipes.quote(os.path.abspath(__file__))))
    os.chmod(path, stat.S_IREAD | stat.S_IEXEC)


def mock_ssh_dir(host, path):
    """Create a directory at ``path`` relative to the temp directory for
    ``host``, where ``path`` is a POSIX path
    """
    dest = rel_posix_to_abs_local(host, path)
    if not os.path.exists(dest):
        os.makedirs(dest)


def mock_ssh_file(host, path, contents):
    """Create a directory at ``path`` relative to the temp directory for
    ``host``, where ``path`` is a POSIX path.

    Returns the path of the resulting file on the filesystem for sanity
    checking.
    """
    if not isinstance(contents, bytes):
        raise TypeError('mock SSH file contents must be bytes')

    path = rel_posix_to_abs_local(host, path)

    basename, name = os.path.split(path)
    if not os.path.exists(basename):
        os.makedirs(basename)

    with open(path, 'wb') as f:
        f.write(contents)
    return path


def path_for_host(host, environ=None):
    """Get the filesystem path that the given host is being faked at"""
    if environ is None:
        environ = os.environ
    for kv_pair in environ['MOCK_SSH_ROOTS'].split(':'):
        this_host, this_path = kv_pair.split('=')
        if this_host == host:
            return os.path.abspath(this_path)
    raise KeyError('Host %s is not specified in $MOCK_SSH_ROOTS (%s)' %
                   (host, environ['MOCK_SSH_ROOTS']))


def rel_posix_to_abs_local(host, path, environ=None):
    """Convert a POSIX path to the current system's format and prepend the
    tmp directory the host's files are in
    """
    if environ is None:
        environ = os.environ
    if path.startswith('/'):
        path = path[1:]
    root = path_for_host(host, environ)
    return os.path.join(root, *path.split('/'))


_SLAVE_ADDR_RE = re.compile(r'^(?P<master>.*?)!(?P<slave>.*?)=(?P<dir>.*)$')
_SCP_RE = re.compile(r'^.*"cat > (?P<filename>.*?)".*$')


def main(stdin, stdout, stderr, args, environ):

    def slave_addresses():
        """Get the addresses for slaves based on :envvar:`MOCK_SSH_ROOTS`"""
        for kv_pair in environ['MOCK_SSH_ROOTS'].split(':'):
            m = _SLAVE_ADDR_RE.match(kv_pair)
            if m:
                print(m.group('slave'), file=stdout)
        return 0

    def receive_poor_mans_scp(host, args):
        """Mock SSH behavior for :py:func:`~mrjob.ssh.poor_mans_scp()`"""
        dest = _SCP_RE.match(args[0]).group('filename')
        try:
            path = os.path.join(path_for_host(host, environ), dest)
            with open(path, 'w') as f:
                f.writelines(stdin)
            return 0
        except IOError:
            print('No such file or directory:', dest, file=stderr)
            return 1

    def ls(host, args):
        """Mock SSH behavior for :py:func:`~mrjob.ssh._ssh_ls()`"""
        dest = args[1]
        if dest == '-L':
            dest = args[2]
        root = path_for_host(host, environ)
        local_dest = rel_posix_to_abs_local(host, dest, environ)

        prefix_length = len(path_for_host(host, environ))
        if not os.path.exists(local_dest):
            print('No such file or directory:', local_dest, file=stderr)
            return 1
        if not os.path.isdir(local_dest):
            print(dest, file=stdout)
        for root, dirs, files in os.walk(local_dest):
            components = root.split(os.sep)
            new_root = posixpath.join(*components)
            for filename in files:
                print(
                    '/' + posixpath.join(new_root, filename)[prefix_length:],
                    file=stdout)
        return 0

    def cat(host, args):
        """Mock SSH behavior for :py:func:`~mrjob.ssh._ssh_cat()`"""
        local_dest = rel_posix_to_abs_local(host, args[1], environ)
        if not os.path.exists(local_dest):
            print('No such file or directory:', local_dest, file=stderr)
            return 1

        # in Python 3, binary data has to go to sys.stdout.buffer
        stdout_buffer = getattr(stdout, 'buffer', stdout)

        with open(local_dest, 'rb') as f:
            for line in f:
                stdout_buffer.write(line)

        return 0

    def run(host, remote_args, stdout, stderr, environ, slave_key_file=None):
        """Execute a command as a "host." Recursively call for slave if
        necessary.
        """
        remote_arg_pos = 0

        # handle sudo
        if remote_args[0] == 'sudo':
            remote_args = remote_args[1:]
        elif environ.get('MOCK_SSH_REQUIRES_SUDO'):
            if remote_args[0] in ('find', 'cat'):
                print('sudo required', file=stderr)
                return 1

        # Get slave addresses (this is 'bash -c "hadoop dfsadmn ...')
        if remote_args[0].startswith('bash -c "hadoop'):
            return slave_addresses()

        # Accept stdin for a file transfer (this is 'bash -c "cat > ...')
        if remote_args[0].startswith('bash -c "cat'):
            return receive_poor_mans_scp(host, remote_args)

        # ls (this is 'find -type f ...')
        if remote_args[0] == 'find':
            return ls(host, remote_args)

        # cat (this is 'cat ...')
        if remote_args[0] == 'cat':
            return cat(host, remote_args)

        # Recursively call for slaves
        if remote_args[0] == 'ssh':
            # Actually check the existence of the key file on the master node
            while not remote_args[remote_arg_pos] == '-i':
                remote_arg_pos += 1

            slave_key_file = remote_args[remote_arg_pos + 1]

            if not os.path.exists(
                    os.path.join(path_for_host(host, environ),
                                 slave_key_file)):
                # This is word-for-word what SSH says.
                print(('Warning: Identity file %s not accessible.'
                       ' No such file or directory.' %
                       slave_key_file), file=stderr)

                print('Permission denied (publickey).', file=stderr)
                return 1

            while not remote_args[remote_arg_pos].startswith('hadoop@'):
                remote_arg_pos += 1

            slave_host = (
                host + '!%s' % remote_args[remote_arg_pos].split('@')[1])

            # build bang path
            return run(slave_host, remote_args[remote_arg_pos + 1:],
                       stdout, stderr, environ, slave_key_file)

        print(("Command line not recognized: %s" %
               ' '.join(remote_args)), file=stderr)
        return 1

    # Find where the user's commands begin
    arg_pos = 0

    # skip to key file path
    while args[arg_pos] != '-i':
        arg_pos += 1

    arg_pos += 1

    # verify existence of key pair file if necessary
    if environ.get('MOCK_SSH_VERIFY_KEY_FILE', 'false') == 'true' \
       and not os.path.exists(args[arg_pos]):
        print('Warning: Identity file', end='', file=stderr)
        args[arg_pos], 'not accessible: No such file or directory.'
        return 1

    # skip to host address
    while not args[arg_pos].startswith('hadoop@'):
        arg_pos += 1

    host = args[arg_pos].split('@')[1]

    # the rest are arguments are what to run on the remote machine

    arg_pos += 1
    return run(host, args[arg_pos:], stdout, stderr, environ, None)


if __name__ == '__main__':
    sys.exit(main(sys.stdin, sys.stdout, sys.stderr, sys.argv, os.environ))
