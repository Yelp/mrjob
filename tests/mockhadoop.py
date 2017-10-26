# Copyright 2009-2012 Yelp
# Copyright 2013 Tom Arnfeld and David Marin
# Copyright 2014 Contributors
# Copyright 2015-2016 Yelp
# Copyright 2017 Yelp and Contributors
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

Requires these environment variables:
  MOCK_HADOOP_TMP: temp directory used by mock hadoop
  MOCK_HADOOP_VERSION: version of Hadoop to emulate (e.g. '2.7.1').

This is designed to run as: python -m tests.mockhadoop <hadoop args>

mrjob requires a single binary (no args) to stand in for hadoop, so
use create_mock_hadoop_script() to write out a shell script that runs
mockhadoop.

To add mock output for streaming steps, use add_mock_output()
To add mock counters for steps, use add_mock_counters()
To see how the hadoop binary has been invoked, use get_mock_hadoop_cmd_args()
To see what's in HDFS, use get_mock_hdfs_root()
"""
from __future__ import print_function

import datetime
import glob
import json
import os
import os.path
import pipes
import shutil
import stat
import sys

from mrjob.compat import uses_yarn
from mrjob.hadoop import _HADOOP_STREAMING_JAR_RE
from mrjob.parse import urlparse
from mrjob.util import shlex_split

# layout of $MOCK_HADOOP_TMP:
#   cmd.log: single file containing one line per invocation of mock hadoop
#     binary, with lines containing space-separated args passed
#   counters/YYmmdd.HH:MM:SS.uuuuuu.json: files containing JSON map from
#     group to counter to amount. When we need to output counters, we read
#     the first file alphabetically, and then delete it
#   hdfs/: The root directory of all filesystems (HDFS and otherwise) accessed
#     by mock hadoop binary (we ignore scheme and host)
#   output/YYmmdd.HH:MM:SS.uuuuuu/part-nnnnn: sudirectories containing
#     part files containing output to copy into HDFS to simulate job output.
#     When we need output, we pick the first directory alphabetically, and then
#     delete it


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


def iso_now():
    """Return the current time as YYmmdd.HH:MM:SS.uuuuuu. Use this to ensure
    that mock data added stays in alphabetical order.
    """
    now = datetime.datetime.now()
    return '%s.%06d' % (now.strftime('%Y%m%d.%H%M%S'), now.microsecond)


def get_mock_dir(*subdirs, **kwargs):
    """Get a directory within $MOCK_HADOOP_TMP, creating it if it doesn't
    already exist.

    Takes optional *environ* keyword arg (defaults to ``os.environ``)."""
    environ = kwargs.get('environ') or os.environ

    path = os.path.join(environ['MOCK_HADOOP_TMP'], *subdirs)

    if not os.path.exists(path):
        os.makedirs(os.path.abspath(path))

    return path


def add_mock_hadoop_output(parts):
    """Add mock output which will be used by the next fake streaming
    job that mockhadoop will run.

    Args:
    parts -- a list of the contents of parts files, which should be iterables
        that return lines as bytes (e.g. lists, BytesIOs).
    """
    output_subdir = get_mock_dir('output', iso_now())

    for i, part in enumerate(parts):
        part_path = os.path.join(output_subdir, 'part-%05d' % i)
        with open(part_path, 'wb') as part_file:
                part_file.write(part)


def get_mock_hadoop_output():
    """Get a path to a directory containing part files to use as mock output.
    """
    output_dir = get_mock_dir('output')

    dirnames = sorted(os.listdir(output_dir))
    if dirnames:
        return os.path.join(output_dir, dirnames[0])
    else:
        return None


def add_mock_hadoop_counters(counters):
    """Add counters for a subsequent run of the hadoop jar command (streaming
    or otherwise), as a map from group name to counter name to amount."""
    counter_path = os.path.join(get_mock_dir('counters'), iso_now() + '.json')

    with open(counter_path, 'w') as f:
        json.dump(counters, f)


def next_mock_hadoop_counters():
    """Read the next set of counters from the filesystem, and delete it.

    If no counters available, return {}
    """
    counters_dir = get_mock_dir('counters')
    filenames = sorted(os.listdir(counters_dir))

    if filenames:
        counters_path = os.path.join(counters_dir, filenames[0])
        try:
            with open(counters_path) as f:
                return json.load(f)
        finally:
            os.remove(counters_path)
    else:
        return {}


def get_mock_hdfs_root(environ=None):
    """Get the path of mock root of HDFS. Creates the directory if it
    doesn't already exist."""
    return get_mock_dir('hdfs', environ=environ)


def mock_hadoop_uses_yarn(environ):
    return uses_yarn(environ['MOCK_HADOOP_VERSION'])


def get_mock_hadoop_cmd_args():
    """Get a list for each invocation of hadoop, each containing a list of
    arguments (not including the hadoop binary's path)."""
    cmd_log = os.path.join(get_mock_dir(), 'cmd.log')

    if not os.path.exists(cmd_log):
        return []

    with open(cmd_log) as f:
        return [shlex_split(cmd) for cmd in f]


def hdfs_uri_to_real_path(hdfs_uri, environ):
    """Map an HDFS URI to a path on the filesystem."""
    components = urlparse(hdfs_uri)

    scheme = components.scheme
    path = components.path

    if not scheme and not path.startswith('/'):
        path = '/user/%s/%s' % (environ['USER'], path)

    return os.path.join(get_mock_hdfs_root(environ=environ), path.lstrip('/'))


def real_path_to_hdfs_uri(real_path, environ):
    """Map a real path to an hdfs:/// URI."""
    hdfs_root = get_mock_hdfs_root(environ=environ)

    if not real_path.startswith(hdfs_root):
        raise ValueError('path %s is not in %s' % (real_path, hdfs_root))

    # for some reason, relpath() doesn't work here
    hdfs_uri = real_path[len(hdfs_root):]
    if not hdfs_uri.startswith('/'):
        hdfs_uri = '/' + hdfs_uri

    return hdfs_uri


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
    cmd_log_path = os.path.join(get_mock_dir(environ=environ), 'cmd.log')
    with open(cmd_log_path, 'a') as cmd_log:
        cmd_log.write(' '.join(pipes.quote(arg) for arg in argv[1:]))
        cmd_log.write('\n')
        cmd_log.flush()

    if len(argv) < 2:
        print('Usage: hadoop [--config confdir] COMMAND\n', file=stderr)
        return 1

    cmd = argv[1]
    cmd_args = argv[2:]

    return invoke_cmd(
        stdout, stderr, environ, 'hadoop_', cmd, cmd_args,
        'Could not find the main class: %s.  Program will exit.\n\n' % cmd, 1)


def hadoop_fs(stdout, stderr, environ, *args):
    """Implements hadoop fs <args>"""
    if len(args) < 1:
        print('Usage: java FsShell', file=stderr)
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
        print('Usage: java FsShell [-cat <src>]', file=stderr)
        return -1

    failed = False
    for hdfs_uri_glob in args:
        real_path_glob = hdfs_uri_to_real_path(hdfs_uri_glob, environ)
        paths = glob.glob(real_path_glob)
        if not paths:
            print('cat: File does not exist: %s' % hdfs_uri_glob, file=stderr)
            failed = True
        else:
            for path in paths:
                with open(path, 'rb') as f:
                    # use binary interface if available
                    stdout_buffer = getattr(stdout, 'buffer', stdout)
                    for line in f:
                        stdout_buffer.write(line)

    if failed:
        return -1
    else:
        return 0


def hadoop_fs_du(stdout, stderr, environ, *args):
    if mock_hadoop_uses_yarn(environ) and args and args[0] == '-s':
        aggregate = True
        path_args = args[1:]
    else:
        aggregate = False
        path_args = args

    return _hadoop_fs_du('du', stdout, stderr, environ,
                         path_args=path_args, aggregate=aggregate)


def hadoop_fs_dus(stdout, stderr, environ, *args):
    """Implements hadoop fs -dus."""
    yarn = mock_hadoop_uses_yarn(environ)

    if yarn:
        print("dus: DEPRECATED: Please use 'du -s' instead.", file=stderr)

    return _hadoop_fs_du('dus', stdout, stderr, environ, path_args=args,
                         aggregate=True, pre_yarn_dus_format=not yarn)


def _hadoop_fs_du(cmd_name, stdout, stderr, environ, path_args,
                  aggregate, pre_yarn_dus_format=False):
    """Implements fs -du and fs -dus."""
    hdfs_uri_globs = path_args or ['']

    failed = False

    for hdfs_uri_glob in hdfs_uri_globs:
        real_path_glob = hdfs_uri_to_real_path(hdfs_uri_glob, environ)
        real_paths = glob.glob(real_path_glob)
        if not real_paths:
            if mock_hadoop_uses_yarn(environ):
                msg = "%s: `%s': No such file or directory" % (
                    cmd_name, hdfs_uri_glob)
            else:
                msg = '%s: Cannot access %s: No such file or directory.' % (
                    cmd_name, hdfs_uri_glob)
            print(msg, file=stderr)

            failed = True
            continue

        for real_path in real_paths:
            # if we're not in -s mode, it finds size of every item in dir
            if os.path.isdir(real_path) and not aggregate:
                paths = [os.path.join(real_path, name)
                         for name in os.listdir(real_path)]
            else:
                paths = [real_path]

            for path in paths:
                hdfs_uri = real_path_to_hdfs_uri(path, environ)
                size = _du(path)

                # prior to YARN, the du commands put a variable number
                # of spaces between path and size. Not emulating this
                # because it doesn't matter.

                if pre_yarn_dus_format:  # old fs -dus only, not fs -du
                    print('%s  %d' % (hdfs_uri, size), file=stdout)
                else:
                    print('%d  %s' % (size, hdfs_uri), file=stdout)

    if failed:
        if mock_hadoop_uses_yarn(environ):
            return 1  # -1 is only for arg-parsing errors
        else:
            return -1
    else:
        return 0


def _du(real_path):
    """Get total size of file or files in dir (recursive)."""
    # Hadoop also seems to have some rule for size of directory
    # that varies across versions and that I don't fully understand.
    # Ignoring this for testing.
    total_size = 0

    if os.path.isdir(real_path):
        for dirpath, dirnames, filenames in os.walk(real_path):
            for filename in filenames:
                total_size += os.path.getsize(
                    os.path.join(dirpath, filename))
    else:
        total_size += os.path.getsize(real_path)

    return total_size


def _hadoop_ls_line(real_path, scheme, netloc, size=0, max_size=0, environ={}):
    hdfs_uri = real_path_to_hdfs_uri(real_path, environ)

    # we could actually implement ls here, but mrjob only cares about
    # the path
    if os.path.isdir(real_path):
        file_type = 'd'
    else:
        file_type = '-'

    if scheme in ('s3', 's3n', 's3a'):
        # no user and group on S3 (see Pull Request #573)
        user_and_group = ''
    else:
        user_and_group = 'dave supergroup'

    # newer Hadoop returns fully qualified URIs (see Pull Request #577)
    if scheme and mock_hadoop_uses_yarn(environ):
        hdfs_uri = '%s://%s%s' % (scheme, netloc, hdfs_uri)

    # figure out the padding
    size = str(size).rjust(len(str(max_size)))

    return (
        '%srwxrwxrwx - %s %s 2010-10-01 15:16 %s' %
        (file_type, user_and_group, size, hdfs_uri))


def hadoop_fs_ls(stdout, stderr, environ, *args):
    """Implements hadoop fs -ls."""
    if mock_hadoop_uses_yarn(environ) and args and args[0] == '-R':
        path_args = args[1:]
        recursive = True
    else:
        path_args = args
        recursive = False

    return _hadoop_fs_ls('ls', stdout, stderr, environ,
                         path_args=path_args, recursive=recursive)


def hadoop_fs_lsr(stdout, stderr, environ, *args):
    """Implements hadoop fs -lsr."""
    if mock_hadoop_uses_yarn(environ):
        print("lsr: DEPRECATED: Please use 'ls -R' instead.", file=stderr)

    return _hadoop_fs_ls(
        'lsr', stdout, stderr, environ, path_args=args, recursive=True)


def _hadoop_fs_ls(cmd_name, stdout, stderr, environ, path_args, recursive):
    """Helper for hadoop_fs_ls() and hadoop_fs_lsr()."""
    hdfs_uri_globs = path_args or ['']

    failed = False
    for hdfs_uri_glob in hdfs_uri_globs:
        parsed = urlparse(hdfs_uri_glob)
        scheme = parsed.scheme
        netloc = parsed.netloc

        real_path_glob = hdfs_uri_to_real_path(hdfs_uri_glob, environ)
        real_paths = glob.glob(real_path_glob)

        paths = []

        if not real_paths:
            print('%s: Cannot access %s: No such file or directory.' %
                  (cmd_name, hdfs_uri_glob), file=stderr)
            failed = True
        else:
            for real_path in real_paths:
                if os.path.isdir(real_path):
                    if recursive:
                        for dirpath, dirnames, filenames in os.walk(real_path):
                            paths.append((dirpath, scheme, netloc, 0))
                            for filename in filenames:
                                path = os.path.join(dirpath, filename)
                                size = os.path.getsize(path)
                                paths.append((path, scheme, netloc, size))
                    else:
                        for filename in os.listdir(real_path):
                            path = os.path.join(real_path, filename)
                            if os.path.isdir(path):
                                size = 0
                            else:
                                size = os.path.getsize(path)
                            paths.append((path, scheme, netloc, size))
                else:
                    size = os.path.getsize(real_path)
                    paths.append((real_path, scheme, netloc, size))

        if paths:
            print('Found %d items' % len(paths), file=stdout)
            max_size = max(size for _, __, ___, size in paths)
            for path in paths:
                print(_hadoop_ls_line(*path + (max_size, environ)),
                      file=stdout)

    if failed:
        return -1
    else:
        return 0


def hadoop_fs_mkdir(stdout, stderr, environ, *args):
    """Implements hadoop fs -mkdir"""
    if len(args) < 1:
        print('Usage: java FsShell [-mkdir <path>]', file=stderr)
        return -1

    failed = False

    if mock_hadoop_uses_yarn(environ):
        # expect a -p parameter for mkdir
        if args[0] == '-p':
            args = args[1:]
        else:
            failed = True

    for path in args:
        real_path = hdfs_uri_to_real_path(path, environ)
        if os.path.exists(real_path):
            print('mkdir: cannot create directory %s: File exists' % path,
                  file=stderr)
            # continue to make directories on failure
            failed = True
        else:
            os.makedirs(real_path)

    if failed:
        return -1
    else:
        return 0


def hadoop_fs_put(stdout, stderr, environ, *args):
    """Implements hadoop fs -put"""
    if len(args) < 2:
        print('Usage: java FsShell [-put <localsrc> ... <dst>]', file=stderr)
        return -1

    srcs = args[:-1]
    dst = args[-1]

    real_dst = hdfs_uri_to_real_path(dst, environ)
    real_dir = os.path.dirname(real_dst)
    # dst could be a dir or a filename; we don't know
    if not (os.path.isdir(real_dst) or os.path.isdir(real_dir)):
        os.makedirs(real_dir)

    for src in srcs:
        shutil.copy(src, real_dst)
    return 0


def hadoop_fs_rm(stdout, stderr, environ, *args):
    """Implements hadoop fs -rm."""
    # parse args
    recursive = False
    force = False

    yarn = mock_hadoop_uses_yarn(environ)

    for i, arg in enumerate(args):
        # -r/-R and -f are only available in YARN
        if yarn and arg in ('-r', '-R'):
            recursive = True
        elif yarn and arg == '-f':
            force = True
        elif arg == '-skipTrash':
            pass  # we don't emulate trash
        elif arg.startswith('-'):
            # don't know what the pre-YARN version of this, doesn't matter
            # It's really '-rm', not 'rm'. Because it's about args?
            print('-rm: Illegal option %s' % arg, file=stderr)
            return -1
        else:
            # BSD-style args: all switches at the beginning
            path_args = args[i:]
            break
    else:
        # no path arguments
        if yarn:
            print('-rm: Not enough arguments: expected 1 but got 0',
                  file=stderr)
            print('Usage: hadoop fs [generic options] -rm [-f] [-r|-R]'
                  ' [-skipTrash] <src> ...', file=stderr)
        else:
            print('Usage: java FsShell [-rm [-skipTrash] <src>]', file=stderr)

        return -1

    return _hadoop_fs_rm('rm', stdout, stderr, environ,
                         path_args=path_args, recursive=recursive,
                         force=force)


def hadoop_fs_rmr(stdout, stderr, environ, *args):
    yarn = mock_hadoop_uses_yarn(environ)

    if yarn:
        print("rmr: DEPRECATED: Please use 'rm -r' instead.", file=stderr)

    if args and args[0] == '-skipTrash':
        path_args = args[1:]
    else:
        path_args = args

    if not path_args:
        if yarn:
            print('-rmr: Not enough arguments: expected 1 but got 0',
                  file=stderr)
            # -skipTrash isn't mentioned in usage, at least in 2.5.2
            print('Usage: hadoop fs [generic options] -rmr'
                  ' <src> ...', file=stderr)
        else:
            print('Usage: java FsShell [-rmr [-skipTrash] <src>]', file=stderr)

        return -1

    return _hadoop_fs_rm('rmr', stdout, stderr, environ,
                         path_args=path_args, recursive=True, force=False)


def _hadoop_fs_rm(cmd_name, stdout, stderr, environ,
                  path_args, recursive, force):
    """Helper for hadoop_fs_rm() and hadoop_fs_rmr()."""
    # use an array so that fail() can update it
    failed = []

    def fail(path, msg):
        if mock_hadoop_uses_yarn(environ):
            print('%s `%s`: %s' % (cmd_name, path, msg), file=stderr)
        else:
            print('%s: cannot remove %s: %s.' % (cmd_name, path, msg),
                  file=stderr)
        failed.append(True)

    for path in path_args:
        real_path = hdfs_uri_to_real_path(path, environ)
        if os.path.isdir(real_path):
            if recursive:
                shutil.rmtree(real_path)
            else:
                fail(path, 'Is a directory')
        elif os.path.exists(real_path):
            os.remove(real_path)
        else:
            if not force:
                fail(path, 'No such file or directory')

    if failed:
        return -1
    else:
        return 0


def hadoop_fs_test(stdout, stderr, environ, *args):
    """Implements hadoop fs -test."""
    if len(args) < 1:
        print('Usage: java FsShell [-test -[ezd] <src>]', file=stderr)

    if os.path.exists(hdfs_uri_to_real_path(args[1], environ)):
        return 0
    else:
        return 1


def hadoop_jar(stdout, stderr, environ, *args):
    if len(args) < 1:
        print('RunJar jarFile [mainClass] args...', file=stderr)
        return -1

    jar_path = args[0]
    if not os.path.exists(jar_path):
        print('Exception in thread "main" java.io.IOException: Error opening'
              ' job jar: %s' % jar_path, file=stderr)
        return -1

    # use this to simulate log4j
    def mock_log4j(message, level='INFO', logger='mapreduce.JOB', now=None):
        now = now or datetime.datetime.now()
        line = '%s %s %s: %s' % (now.strftime('%Y/%m/%d %H:%M:%S'),
                                 level, logger, message)
        print(line, file=stderr)

    # simulate counters
    counters = next_mock_hadoop_counters()
    if counters:
        num_counters = sum(len(g) for g in counters.values())
        mock_log4j('Counters: %d' % num_counters)
        # subsequent lines are actually part of same log record
        for group, group_counters in sorted(counters.items()):
            print(('\t%s' % group), file=stderr)
            for counter, amount in sorted(group_counters.items()):
                print(('\t\t%s=%d' % (counter, amount)), file=stderr)

    # simulate output for streaming steps
    if _HADOOP_STREAMING_JAR_RE.match(os.path.basename(jar_path)):
        streaming_args = args[1:]
        output_idx = list(streaming_args).index('-output')
        assert output_idx != -1
        output_dir = streaming_args[output_idx + 1]
        real_output_dir = hdfs_uri_to_real_path(output_dir, environ)

        mock_output_dir = get_mock_hadoop_output()
        if mock_output_dir is None:
            mock_log4j('Job failed!')
            return -1

        if os.path.isdir(real_output_dir):
            os.rmdir(real_output_dir)

        shutil.move(mock_output_dir, real_output_dir)

    now = datetime.datetime.now()
    mock_log4j(now.strftime('Running job: job_%Y%m%d%H%M_0001'))
    mock_log4j('Job succeeded!')
    return 0


def hadoop_version(stdout, stderr, environ, *args):
    stdout.write("Hadoop " + environ['MOCK_HADOOP_VERSION'])
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.stdin, sys.stdout, sys.stderr, sys.argv, os.environ))
