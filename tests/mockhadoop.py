# Copyright 2009-2012 Yelp
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
MOCK_HDFS_ROOT -- root dir for our fake filesystem(s). Used regardless of
URI scheme or host (so this is also the root of every S3 bucket).
MOCK_HADOOP_OUTPUT -- a directory containing directories containing
fake job output (to add output, use add_mock_output())
MOCK_HADOOP_CMD_LOG -- optional: if this is set, append arguments passed
to the fake hadoop binary to this script, one line per invocation
MOCK_HADOOP_LS_RETURNS_FULL_URIS -- optional: if true, ls returns full URIs
when passed URIs.

This is designed to run as: python -m tests.mockhadoop <hadoop args>

mrjob requires a single binary (no args) to stand in for hadoop, so
use create_mock_hadoop_script() to write out a shell script that runs
mockhadoop.
"""

from __future__ import with_statement

import datetime
import glob
import os
import os.path
import pipes
import shutil
import stat
import sys

from mrjob.compat import version_gte
from mrjob.parse import HADOOP_STREAMING_JAR_RE
from mrjob.parse import urlparse


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


def hdfs_path_to_real_path(hdfs_path, environ):
    components = urlparse(hdfs_path)

    scheme = components.scheme
    path = components.path

    if not scheme and not path.startswith('/'):
        path = '/user/%s/%s' % (environ['USER'], path)

    return os.path.join(environ['MOCK_HDFS_ROOT'], path.lstrip('/'))


def real_path_to_hdfs_path(real_path, environ):
    if environ is None: # user may have passed empty dict
        environ = os.environ
    hdfs_root = environ['MOCK_HDFS_ROOT']

    if not real_path.startswith(hdfs_root):
        raise ValueError('path %s is not in %s' % (real_path, hdfs_root))

    # janky version of os.path.relpath() (Python 2.6):
    hdfs_path = real_path[len(hdfs_root):]
    if not hdfs_path.startswith('/'):
        hdfs_path = '/' + hdfs_path

    return hdfs_path


def invoke_cmd(stdout, stderr, environ, prefix, cmd, cmd_args, error_msg,
               error_status):
    """Helper function to call command and subcommands of the hadoop binary.

    Basically, combines prefix and cmd to make a function name, and calls
    it with cmd_args. If no such function exists, prints error_msg
    to stderr, and exits with status error_status.
    """
    func_name = prefix + cmd

    if func_name in globals():
        return globals()[func_name](stdout, stderr, environ, *cmd_args)
    else:
        stderr.write(error_msg)
        return -1


def main(stdin, stdout, stderr, argv, environ):
    """Implements hadoop <args>"""

    # log what commands we ran
    if environ.get('MOCK_HADOOP_LOG'):
        with open(environ['MOCK_HADOOP_LOG'], 'a') as cmd_log:
            cmd_log.write(' '.join(pipes.quote(arg) for arg in argv[1:]))
            cmd_log.write('\n')
            cmd_log.flush()

    if len(argv) < 2:
        stderr.write('Usage: hadoop [--config confdir] COMMAND\n')
        return 1

    cmd = argv[1]
    cmd_args = argv[2:]

    return invoke_cmd(
        stdout, stderr, environ, 'hadoop_', cmd, cmd_args,
        'Could not find the main class: %s.  Program will exit.\n\n' % cmd, 1)


def hadoop_fs(stdout, stderr, environ, *args):
    """Implements hadoop fs <args>"""
    if len(args) < 1:
        stderr.write('Usage: java FsShell\n')
        return -1

    cmd = args[0][1:]  # convert e.g. '-put' -> 'put'
    cmd_args = args[1:]

    # this doesn't have to be a giant switch statement, but it's a
    # bit easier to understand this way. :)
    return invoke_cmd(stdout, stderr, environ, 'hadoop_fs_', cmd, cmd_args,
               '%s: Unknown command\nUsage: java FsShell\n' % cmd, -1)


def hadoop_fs_cat(stdout, stderr, environ, *args):
    """Implements hadoop fs -cat <src>"""
    if len(args) < 1:
        stderr.write('Usage: java FsShell [-cat <src>]\n')
        return -1

    failed = False
    for hdfs_path_glob in args:
        real_path_glob = hdfs_path_to_real_path(hdfs_path_glob, environ)
        paths = glob.glob(real_path_glob)
        if not paths:
            stderr.write('cat: File does not exist: %s\n' % hdfs_path_glob)
            failed = True
        else:
            for path in paths:
                with open(path) as f:
                    for line in f:
                        stdout.write(line)

    if failed:
        return -1
    else:
        return 0

def _hadoop_ls_line(real_path, scheme, netloc, size=0, max_size=0, environ={}):
    hdfs_path = real_path_to_hdfs_path(real_path, environ)

    # we could actually implement ls here, but mrjob only cares about
    # the path
    if os.path.isdir(real_path):
        file_type = 'd'
    else:
        file_type = '-'

    if scheme in ('s3', 's3n'):
        # no user and group on S3 (see Pull Request #573)
        user_and_group = ''
    else:
        user_and_group = 'dave supergroup'

    # newer Hadoop returns fully qualified URIs (see Pull Request #577)
    if scheme and environ.get('MOCK_HADOOP_LS_RETURNS_FULL_URIS'):
        hdfs_path = '%s://%s%s' % (scheme, netloc, hdfs_path)

    # figure out the padding
    size = str(size).rjust(len(str(max_size)))

    return (
        '%srwxrwxrwx - %s %s 2010-10-01 15:16 %s' %
        (file_type, user_and_group, size, hdfs_path))

def hadoop_fs_lsr(stdout, stderr, environ, *args):
    """Implements hadoop fs -lsr."""
    hdfs_path_globs = args or ['']

    failed = False
    for hdfs_path_glob in hdfs_path_globs:
        parsed = urlparse(hdfs_path_glob)
        scheme = parsed.scheme
        netloc = parsed.netloc

        real_path_glob = hdfs_path_to_real_path(hdfs_path_glob, environ)
        real_paths = glob.glob(real_path_glob)

        paths = []
        max_size = 0

        if not real_paths:
            print >> stderr, (
                'lsr: Cannot access %s: No such file or directory.' %
                hdfs_path_glob)
            failed = True
        else:
            for real_path in real_paths:
                if os.path.isdir(real_path):
                    for dirpath, dirnames, filenames in os.walk(real_path):
                        paths.append((dirpath, scheme, netloc, 0))
                        for filename in filenames:
                            path = os.path.join(dirpath, filename)
                            size = os.path.getsize(path)
                            max_size = size if size > max_size else max_size
                            paths.append((path, scheme, netloc, size))
                else:
                    paths.append((real_path, scheme, netloc, 0))

        for path in paths:
            print >> stdout, _hadoop_ls_line(*path + (max_size, environ))

    if failed:
        return -1
    else:
        return 0


def hadoop_fs_ls(stdout, stderr, environ, *args):
    """Implements hadoop fs -ls."""
    hdfs_path_globs = args or ['']

    failed = False
    for hdfs_path_glob in hdfs_path_globs:
        parsed = urlparse(hdfs_path_glob)
        scheme = parsed.scheme
        netloc = parsed.netloc

        real_path_glob = hdfs_path_to_real_path(hdfs_path_glob, environ)
        real_paths = glob.glob(real_path_glob)

        paths = []
        max_size = 0

        if not real_paths:
            print >> stderr, (
                'ls: Cannot access %s: No such file or directory.' %
                hdfs_path_glob)
            failed = True
        else:
            for real_path in real_paths:
                paths.append((real_path, scheme, netloc, 0))

        for path in paths:
            print >> stdout, _hadoop_ls_line(*path + (max_size, environ))

    if failed:
        return -1
    else:
        return 0


def hadoop_fs_mkdir(stdout, stderr, environ, *args):
    """Implements hadoop fs -mkdir"""
    if len(args) < 1:
        stderr.write('Usage: java FsShell [-mkdir <path>]\n')
        return -1

    failed = False
    for path in args:
        real_path = hdfs_path_to_real_path(path, environ)
        if os.path.exists(real_path):
            stderr.write(
                'mkdir: cannot create directory %s: File exists' % path)
            # continue to make directories on failure
            failed = True
        else:
            os.makedirs(real_path)

    if failed:
        return -1
    else:
        return 0


def hadoop_fs_dus(stdout, stderr, environ, *args):
    """Implements hadoop fs -dus."""
    hdfs_path_globs = args or ['']

    failed = False
    for hdfs_path_glob in hdfs_path_globs:
        real_path_glob = hdfs_path_to_real_path(hdfs_path_glob, environ)
        real_paths = glob.glob(real_path_glob)
        if not real_paths:
            print >> stderr, (
                'lsr: Cannot access %s: No such file or directory.' %
                hdfs_path_glob)
            failed = True
        else:
            for real_path in real_paths:
                total_size = 0
                if os.path.isdir(real_path):
                    for dirpath, dirnames, filenames in os.walk(real_path):
                        for filename in filenames:
                            total_size += os.path.getsize(
                                os.path.join(dirpath, filename))
                else:
                    total_size += os.path.getsize(real_path)
                print >> stdout, "%s    %d" % (real_path, total_size)

    if failed:
        return -1
    else:
        return 0


def hadoop_fs_put(stdout, stderr, environ, *args):
    """Implements hadoop fs -put"""
    if len(args) < 2:
        stderr.write('Usage: java FsShell [-put <localsrc> ... <dst>]')
        return -1

    srcs = args[:-1]
    dst = args[-1]

    real_dst = hdfs_path_to_real_path(dst, environ)
    real_dir = os.path.dirname(real_dst)
    # dst could be a dir or a filename; we don't know
    if not (os.path.isdir(real_dst) or os.path.isdir(real_dir)):
        os.makedirs(real_dir)

    for src in srcs:
        shutil.copy(src, real_dst)
    return 0


def hadoop_fs_rmr(stdout, stderr, environ, *args):
    """Implements hadoop fs -rmr."""
    if len(args) < 1:
        stderr.write('Usage: java FsShell [-rmr [-skipTrash] <src>]')

    if args[0] == '-skipTrash':
        args = args[1:]

    failed = False
    for path in args:
        real_path = hdfs_path_to_real_path(path, environ)
        if os.path.isdir(real_path):
            shutil.rmtree(real_path)
        elif os.path.exists(real_path):
            os.remove(real_path)
        else:
            stderr.write(
                'rmr: cannot remove %s: No such file or directory.' % path)
            failed = True

    if failed:
        return -1
    else:
        return 0


def hadoop_fs_test(stdout, stderr, environ, *args):
    """Implements hadoop fs -test."""
    if len(args) < 1:
        stderr.write('Usage: java FsShell [-test -[ezd] <src>]')

    if os.path.exists(hdfs_path_to_real_path(args[1], environ)):
        return 0
    else:
        return 1


def hadoop_jar(stdout, stderr, environ, *args):
    if len(args) < 1:
        stderr.write('RunJar jarFile [mainClass] args...\n')
        return -1

    jar_path = args[0]
    if not os.path.exists(jar_path):
        stderr.write(
            'Exception in thread "main" java.io.IOException: Error opening job'
            ' jar: %s\n' % jar_path)
        return -1

    # only simulate for streaming steps
    if HADOOP_STREAMING_JAR_RE.match(os.path.basename(jar_path)):
        streaming_args = args[1:]
        output_idx = list(streaming_args).index('-output')
        assert output_idx != -1
        output_dir = streaming_args[output_idx + 1]
        real_output_dir = hdfs_path_to_real_path(output_dir, environ)

        mock_output_dir = get_mock_hadoop_output()
        if mock_output_dir is None:
            stderr.write('Job failed!')
            return -1

        if os.path.isdir(real_output_dir):
            os.rmdir(real_output_dir)

        shutil.move(mock_output_dir, real_output_dir)

    now = datetime.datetime.now()
    stderr.write(now.strftime('Running job: job_%Y%m%d%H%M_0001\n'))
    stderr.write('Job succeeded!\n')
    return 0


def hadoop_version(stdout, stderr, environ, *args):
    stderr.write("""Hadoop 0.20.2
Subversion https://svn.apache.org/repos/asf/hadoop/common/branches/branch-0.20\
 -r 911707
Compiled by chrisdo on Fri Feb 19 08:07:34 UTC 2010
""")
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.stdin, sys.stdout, sys.stderr, sys.argv, os.environ))
