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

"""A mock version of the ssh binary that actually manipulates the
filesystem. This imitates only things that mrjob actually uses.

Relies on these environment variables:
MOCK_SSH_ROOTS -- specify directories for hosts in the form:
                  host1=/tmp/dir1:host2=/tmp/dir2
MOCK_SSH_VERIFY_KEY_FILE -- set to 'true' if the script should print an error
                            when the key file does not exist

This is designed to run as: python -m tests.mockssh <ssh args>

mrjob requires a single binary (no args) to stand in for ssh, so
use create_mock_hadoop_script() to write out a shell script that runs
mockssh.
"""

from __future__ import with_statement

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


def rel_posix_to_rel_local(path):
    """Convert a POSIX path to the current system's format"""
    return os.path.join(*path.split('/'))


def rel_posix_to_abs_local(host, path, path_map):
    """Convert a POSIX path to the current system's format and prepend the
    tmp directory the host's files are in
    """
    if path.startswith('/'):
        path = path[1:]
    root = path_map[host]
    return os.path.join(root, *path.split('/'))


def mock_ssh_dir(host, path, path_map):
    """Create a directory at ``path`` relative to the temp directory for
    ``host``, where ``path`` is a POSIX path
    """
    dest = rel_posix_to_abs_local(host, path, path_map)
    if not os.path.exists(dest):
        os.makedirs(dest)


def mock_ssh_file(host, path, contents, path_map):
    """Create a directory at ``path`` relative to the temp directory for
    ``host``, where ``path`` is a POSIX path.

    Returns the path of the resulting file on the filesystem for sanity
    checking.
    """
    path = rel_posix_to_abs_local(host, path, path_map)

    basename, name = os.path.split(path)
    if not os.path.exists(basename):
        os.makedirs(basename)

    with open(path, 'w') as f:
        f.write(contents)
    return path


_SLAVE_ADDR_RE = re.compile(r'^(?P<master>.*?)!(?P<slave>.*?)$')


def slave_addresses(stdout, stderr, path_map):
    """Get the addresses for slaves based on :envvar:`MOCK_SSH_ROOTS`"""
    for key in path_map:
        m = _SLAVE_ADDR_RE.match(key)
        if m:
            print >> stdout, m.group('slave')


_SCP_RE = re.compile(r'^.*"cat > (?P<filename>.*?)".*$')


def receive_poor_mans_scp(host, args, stdin, stdout, stderr, path_map):
    """Mock SSH behavior for :py:func:`~mrjob.ssh.poor_mans_scp()`"""
    dest = _SCP_RE.match(args[0]).group('filename')
    try:
        with open(os.path.join(path_map[host], dest), 'w') as f:
            f.writelines(stdin)
    except IOError:
        print >> stderr, 'No such file or directory:', dest


def ls(host, args, stdout, stderr, path_map):
    """Mock SSH behavior for :py:func:`~mrjob.ssh.ssh_ls()`"""
    dest = args[1]
    root = path_map[host]
    local_dest = rel_posix_to_abs_local(host, dest, path_map)

    prefix_length = len(root)
    if not os.path.exists(local_dest):
        print >> stderr, 'No such file or directory:', local_dest
        sys.exit(1)
    if not os.path.isdir(local_dest):
        print >> stdout, dest
    for root, dirs, files in os.walk(local_dest):
        components = root.split(os.sep)
        new_root = posixpath.join(*components)
        for filename in files:
            print >> stdout, '/' + posixpath.join(new_root,
                                                  filename)[prefix_length:]


def cat(host, args, stdout, stderr, path_map):
    """Mock SSH behavior for :py:func:`~mrjob.ssh.ssh_cat()`"""
    local_dest = rel_posix_to_abs_local(host, args[1], path_map)
    if not os.path.exists(local_dest):
        print >> stderr, 'No such file or directory:', local_dest
        sys.exit(1)
    with open(local_dest, 'r') as f:
        print >> stdout, f.read()


def run(host, remote_args, slave_key_file, stdin, stdout, stderr, path_map):
    """Execute a command as a "host." Recursively call for slave if necessary.
    """
    remote_arg_pos = 0

    # Get slave addresses (this is 'bash -c "hadoop dfsadmn ...')
    if remote_args[0].startswith('bash -c "hadoop'):
        slave_addresses(stdout, stderr, path_map)
        return

    # Accept stdin for a file transfer (this is 'bash -c "cat > ...')
    if remote_args[0].startswith('bash -c "cat'):
        receive_poor_mans_scp(host, remote_args, stdin, stdout, stderr,
                              path_map)
        return

    # ls (this is 'find -type f ...')
    if remote_args[0] == 'find':
        ls(host, remote_args, stdout, stderr, path_map)
        return

    # cat (this is 'cat ...')
    if remote_args[0] == 'cat':
        cat(host, remote_args, stdout, stderr, path_map)
        return

    # Recursively call for slaves
    if remote_args[0] == 'ssh':
        # Actually check the existence of the key file on the master node
        while not remote_args[remote_arg_pos] == '-i':
            remote_arg_pos += 1

        slave_key_file = remote_args[remote_arg_pos + 1]

        if not os.path.exists(
            os.path.join(path_map[host], slave_key_file)):
            # This is word-for-word what SSH says.
            print >> stderr, 'Warning: Identity file',
            slave_key_file, 'not accessible: No such file or directory.'

            print >> stderr, 'Permission denied (publickey).'
            sys.exit(1)

        while not remote_args[remote_arg_pos].startswith('hadoop@'):
            remote_arg_pos += 1

        slave_host = host + '!%s' % remote_args[remote_arg_pos].split('@')[1]

        # build bang path
        run(slave_host,
            remote_args[remote_arg_pos + 1:],
            slave_key_file, stdin, stdout, stderr, path_map)
        return

    print >> stderr, ("Command line not recognized: %s" %
                      ' '.join(remote_args))
    sys.exit(1)


def main(args=None, path_map=None, verify_key_file=None,
         stdin=None, stdout=None, stderr=None):
    args = args if args is not None else sys.argv

    # Find where the user's commands begin
    arg_pos = 0

    # skip to key file path
    while args[arg_pos] != '-i':
        arg_pos += 1

    arg_pos += 1

    # verify existence of key pair file if necessary
    if os.environ.get('MOCK_SSH_VERIFY_KEY_FILE', 'false') == 'true' \
       and not os.path.exists(args[arg_pos]):
        print >> stderr, 'Warning: Identity file',
        args[arg_pos], 'not accessible: No such file or directory.'
        sys.exit(1)

    # skip to host address
    while not args[arg_pos].startswith('hadoop@'):
        arg_pos += 1

    host = args[arg_pos].split('@')[1]

    # the rest are arguments are what to run on the remote machine

    arg_pos += 1
    run(host, args[arg_pos:], slave_key_file=None,
        stdin=stdin, stdout=stdout, stderr=stderr,
        path_map=path_map)


if __name__ == '__main__':
    path_map = {}
    for kv_pair in os.environ['MOCK_SSH_ROOTS'].split(':'):
        this_host, this_path = kv_pair.split('=')
        path_map[this_host] = os.path.abspath(this_path)

    verify_key_file_str = os.environ.get('MOCK_SSH_VERIFY_KEY_FILE', 'false')
    main(args=None, path_map=path_map,
         verify_key_file=(verify_key_file_str == 'true'),
         stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr)
