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

"""A mock version of the hadoop binary that actually manipulates the
filesystem. This imitates only things that mrjob actually uses.

Relies on these environment variables:
MOCK_HDFS_ROOT -- root dir for our fake HDFS filesystem
MOCK_HADOOP_OUTPUT -- a directory containing directories containing
fake job output (to add output, use add_mock_output())
MOCK_HADOOP_CMD_LOG -- optional: if this is set, append arguments passed
to the fake hadoop binary to this script, one line per invocation

This is designed to run as: python -m tests.mockhadoop <hadoop args>

mrjob requires a single binary (no args) to stand in for hadoop, so
use create_mock_hadoop_script() to write out a shell script that runs
mockhadoop.
"""

from __future__ import with_statement

import datetime
import glob
import os
import pipes
import shutil
import stat
import sys


def create_mock_hadoop_script(path):
    """Dump a wrapper script to the given file object that runs this
    python script."""
    # make this work even if $PATH or $PYTHONPATH changes
    with open(path, 'w') as f:
        f.write('#!/bin/sh\n')
        f.write('%s %s "$@"\n' % (
            pipes.quote(sys.executable),
            pipes.quote(os.path.abspath(__file__))))
    os.chmod(path, stat.S_IREAD | stat.S_IEXEC)


def add_mock_hadoop_output(parts):
    """Add mock output which will be used by the next fake streaming
    job that mockhadoop will run.

    Args:
    parts -- a list of the contents of parts files, which should be iterables
        that return lines (e.g. lists, StringIOs).

    The environment variable MOCK_HADOOP_OUTPUT must be set.
    """
    now = datetime.datetime.now()
    output_dir = os.path.join(
        os.environ['MOCK_HADOOP_OUTPUT'],
        '%s.%06d' % (now.strftime('%Y%m%d.%H%M%S'), now.microsecond))
    os.mkdir(output_dir)

    for i, part in enumerate(parts):
        part_path = os.path.join(output_dir, 'part-%05d' % i)
        with open(part_path, 'w') as part_file:
            for line in part:
                part_file.write(line)


def get_mock_hadoop_output():
    """Get the first directory (alphabetically) from MOCK_HADOOP_OUTPUT"""
    dirnames = sorted(os.listdir(os.environ['MOCK_HADOOP_OUTPUT']))
    if dirnames:
        return os.path.join(os.environ['MOCK_HADOOP_OUTPUT'], dirnames[0])
    else:
        return None


def hdfs_path_to_real_path(hdfs_path):
    if hdfs_path.startswith('hdfs:///'):
        hdfs_path = hdfs_path[7:]  # keep one slash

    if not hdfs_path.startswith('/'):
        hdfs_path = '/user/%s/%s' % (os.environ['USER'], hdfs_path)

    return os.path.join(os.environ['MOCK_HDFS_ROOT'], hdfs_path.lstrip('/'))


def real_path_to_hdfs_path(real_path):
    hdfs_root = os.environ['MOCK_HDFS_ROOT']

    if not real_path.startswith(hdfs_root):
        raise ValueError('path %s is not in %s' % (real_path, hdfs_root))

    hdfs_path = real_path[len(hdfs_root):]
    if not hdfs_path.startswith('/'):
        hdfs_path = '/' + hdfs_path

    return hdfs_path


def invoke_cmd(prefix, cmd, cmd_args, error_msg, error_status):
    """Helper function to call command and subcommands of the hadoop binary.

    Basically, combines prefix and cmd to make a function name, and calls
    it with cmd_args. If no such function exists, prints error_msg
    to stderr, and exits with status error_status.
    """
    func_name = prefix + cmd

    if func_name in globals():
        globals()[func_name](*cmd_args)
    else:
        sys.stderr.write(error_msg)
        sys.exit(-1)


def main():
    """Implements hadoop <args>"""

    # log what commands we ran
    if os.environ.get('MOCK_HADOOP_LOG'):
        with open(os.environ['MOCK_HADOOP_LOG'], 'a') as cmd_log:
            cmd_log.write(' '.join(pipes.quote(arg) for arg in sys.argv[1:]))
            cmd_log.write('\n')
            cmd_log.flush()

    if len(sys.argv) < 2:
        sys.stderr.write('Usage: hadoop [--config confdir] COMMAND\n')
        sys.exit(1)

    cmd = sys.argv[1]
    cmd_args = sys.argv[2:]

    invoke_cmd(
        'hadoop_', cmd, cmd_args,
        'Could not find the main class: %s.  Program will exit.\n\n' % cmd, 1)


def hadoop_fs(*args):
    """Implements hadoop fs <args>"""
    if len(args) < 1:
        sys.stderr.write('Usage: java FsShell\n')
        sys.exit(-1)

    cmd = args[0][1:]  # convert e.g. '-put' -> 'put'
    cmd_args = args[1:]

    # this doesn't have to be a giant switch statement, but it's a
    # bit easier to understand this way. :)
    invoke_cmd('hadoop_fs_', cmd, cmd_args,
               '%s: Unknown command\nUsage: java FsShell\n' % cmd, -1)


def hadoop_fs_cat(*args):
    """Implements hadoop fs -cat <src>"""
    if len(args) < 1:
        sys.stderr.write('Usage: java FsShell [-cat <src>]\n')
        sys.exit(-1)

    failed = False
    for hdfs_path_glob in args:
        real_path_glob = hdfs_path_to_real_path(hdfs_path_glob)
        paths = glob.glob(real_path_glob)
        if not paths:
            sys.stderr.write('cat: File does not exist: %s\n' % hdfs_path_glob)
            failed = True
        else:
            for path in paths:
                with open(path) as f:
                    for line in f:
                        sys.stdout.write(line)

    if failed:
        sys.exit(-1)


def hadoop_fs_lsr(*args):
    """Implements hadoop fs -lsr."""
    hdfs_path_globs = args or ['']

    def ls_line(real_path):
        hdfs_path = real_path_to_hdfs_path(real_path)
        # we could actually implement ls here, but mrjob only cares about
        # the path
        path_is_dir = os.path.isdir(real_path)
        return (
            '%srwxrwxrwx - dave supergroup      18321 2010-10-01 15:16 %s' %
            ('d' if path_is_dir else '-', hdfs_path))

    failed = False
    for hdfs_path_glob in hdfs_path_globs:
        real_path_glob = hdfs_path_to_real_path(hdfs_path_glob)
        real_paths = glob.glob(real_path_glob)
        if not real_paths:
            print >> sys.stderr, (
                'lsr: Cannot access %s: No such file or directory.' %
                hdfs_path_glob)
            failed = True
        else:
            for real_path in real_paths:
                if os.path.isdir(real_path):
                    for dirpath, dirnames, filenames in os.walk(real_path):
                        print ls_line(dirpath)
                        for filename in filenames:
                            print ls_line(os.path.join(dirpath, filename))
                else:
                    print ls_line(real_path)

    if failed:
        sys.exit(-1)


def hadoop_fs_mkdir(*args):
    """Implements hadoop fs -mkdir"""
    if len(args) < 1:
        sys.stderr.write('Usage: java FsShell [-mkdir <path>]\n')
        sys.exit(-1)

    failed = False
    for path in args:
        real_path = hdfs_path_to_real_path(path)
        if os.path.exists(real_path):
            sys.stderr.write(
                'mkdir: cannot create directory %s: File exists' % path)
            # continue to make directories on failure
            failed = True

    if failed:
        sys.exit(-1)


def hadoop_fs_put(*args):
    """Implements hadoop fs -put"""
    if len(args) < 2:
        sys.stderr.write('Usage: java FsShell [-put <localsrc> ... <dst>]')
        sys.exit(-1)

    srcs = args[:-1]
    dst = args[-1]

    real_dst = hdfs_path_to_real_path(dst)
    real_dir = os.path.dirname(real_dst)
    # dst could be a dir or a filename; we don't know
    if not (os.path.isdir(real_dst) or os.path.isdir(real_dir)):
        os.makedirs(real_dir)

    for src in srcs:
        shutil.copy(src, real_dst)


def hadoop_fs_rmr(*args):
    """Implements hadoop fs -rmr."""
    if len(args) < 1:
        sys.stderr.write('Usage: java FsShell [-rmr [-skipTrash] <src>]')

    if args[0] == '-skipTrash':
        args = args[1:]

    failed = False
    for path in args:
        real_path = hdfs_path_to_real_path(path)
        if os.path.exists(real_path):
            shutil.rmtree(real_path)
        else:
            sys.stderr.write(
                'rmr: cannot remove %s: No such file or directory.' % path)
            failed = True

    if failed:
        sys.exit(-1)


def hadoop_jar(*args):
    if len(args) < 1:
        sys.stderr.write('RunJar jarFile [mainClass] args...\n')
        sys.exit(-1)

    jar_path = args[0]
    if not os.path.exists(jar_path):
        sys.stderr.write(
            'Exception in thread "main" java.io.IOException: Error opening job'
            ' jar: %s\n' % jar_path)
        sys.exit(-1)

    streaming_args = args[1:]
    output_idx = list(streaming_args).index('-output')
    assert output_idx != -1
    output_dir = streaming_args[output_idx + 1]
    real_output_dir = hdfs_path_to_real_path(output_dir)

    mock_output_dir = get_mock_hadoop_output()
    if mock_output_dir is None:
        sys.stderr.write('Job failed!')
        sys.exit(-1)

    if os.path.isdir(real_output_dir):
        os.rmdir(real_output_dir)

    shutil.move(mock_output_dir, real_output_dir)

    now = datetime.datetime.now()
    sys.stderr.write(now.strftime('Running job: job_%Y%m%d%H%M_0001\n'))
    sys.stderr.write('Job succeeded!\n')


def hadoop_version(*args):
    sys.stderr.write("""Hadoop 0.20.2
Subversion https://svn.apache.org/repos/asf/hadoop/common/branches/branch-0.20\
 -r 911707
Compiled by chrisdo on Fri Feb 19 08:07:34 UTC 2010
""")


if __name__ == '__main__':
    main()
