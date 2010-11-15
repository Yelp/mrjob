# Copyright 2009-2010 Yelp
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

"""Utility functions for MRJob that have no external dependencies."""
# don't add imports here that aren't part of the standard Python library,
# since MRJobs need to run in Amazon's generic EMR environment
from __future__ import with_statement

import bz2
import contextlib
import glob
import gzip
import logging
import os
import pipes
import sys
import tarfile
import zipfile

def cmd_line(args):
    """build a command line that works in a shell.
    """
    args = [str(x) for x in args]
    return ' '.join(pipes.quote(x) for x in args)

def expand_path(path):
    """Resolve ``~`` (home dir) and environment variables in *path*.

    If *path* is ``None``, return ``None``.
    """
    if path is None:
        return None
    else:
        return os.path.expanduser(os.path.expandvars(path))

def file_ext(path):
    """return the file extension, including the ``.``

    >>> file_ext('foo.tar.gz')
    '.tar.gz'
    """
    filename = os.path.basename(path)
    dot_index = filename.find('.')
    if dot_index == -1:
        return ''
    return filename[dot_index:]

def log_to_stream(name=None, stream=None, format=None, level=None, debug=False):
    """Set up logging.

    :type name: str
    :param name: name of the logger, or ``None`` for the root logger
    :type stderr: file object
    :param stderr:  stream to log to (default is ``sys.stderr``)
    :type format: str
    :param format: log message format (default is '%(message)s')
    :param level: log level to use
    :type debug: bool
    :param debug: quick way of setting the log level; if true, use ``logging.DEBUG``; otherwise use ``logging.INFO``
    """
    if level is None:
        level = logging.DEBUG if debug else logging.INFO

    if format is None:
        format = '%(message)s'

    if stream is None:
        stream = sys.stderr

    handler = logging.StreamHandler(stream)
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(format))

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

def read_input(path, stdin=None):
    """Stream input the way Hadoop would.

    - Resolve globs (``foo_*.gz``).
    - Decompress ``.gz`` and ``.bz2`` files.
    - If path is ``'-'``, read from stdin
    - If path is a directory, recursively read its contents.

    You can redefine *stdin* for ease of testing. *stdin* can actually be
    any iterable that yields lines (e.g. a list).
    """
    if stdin is None:
        stdin = sys.stdin
    
    # handle '-' (special case)
    if path == '-':
        for line in stdin:
            yield line
        return

    # resolve globs
    paths = glob.glob(path)
    if not paths:
        raise IOError(2, 'No such file or directory: %r' % path)
    elif len(paths) > 1:
        for path in paths:
            for line in read_input(path, stdin=stdin):
                yield line
        return
    else:
        path = paths[0]

    # recurse through directories
    if os.path.isdir(path):
        for dirname, _, filenames in os.walk(path):
            for filename in filenames:
                for line in read_input(os.path.join(dirname, filename),
                                       stdin=stdin):
                    yield line
        return

    # read from files
    if path.endswith('.bz2'):
        f = bz2.BZ2File(path)
    elif path.endswith('.gz'):
        f = gzip.GzipFile(path)
    else:
        f = open(path)

    for line in f:
        yield line

# Thanks to http://lybniz2.sourceforge.net/safeeval.html for
# explaining how to do this!
def safeeval(expr, globals=None, locals=None):
    """Like eval, but with nearly everything in the environment
    blanked out, so that it's difficult to cause mischief.

    *globals* and *locals* are optional dictionaries mapping names to
    values for those names (just like in :py:func:`eval`).
    """
    # blank out builtins, but keep None, True, and False
    safe_globals = {'__builtins__': None, 'True': True, 'False': False,
                    'None': None, 'set': set, 'xrange': xrange}

    # add the user-specified global variables
    if globals:
        safe_globals.update(globals)

    return eval(expr, safe_globals, locals)


def tar_and_gzip(dir, out_path, filter=None):
    """Tar and gzip the given *dir* to a tarball at *out_path*.

    If we encounter symlinks, include the actual file, not the symlink.

    :type dir: str
    :param dir: dir to tar up
    :type out_path: str
    :param out_path: where to write the tarball too
    :param filter: if defined, a function that takes paths (relative to *dir* and returns ``True`` if we should keep them
    """
    if not os.path.isdir(dir):
        raise IOError('Not a directory: %r' % (dir,))

    if not filter:
        filter = lambda path: True

    # supposedly you can also call tarfile.TarFile(), but I couldn't
    # get this to work in Python 2.5.1. Please leave as-is.
    tar_gz = tarfile.open(out_path, mode='w:gz')

    for dirpath, dirnames, filenames in os.walk(dir):
        for filename in filenames:
            path = os.path.join(dirpath, filename)
            # janky version of os.path.relpath (Python 2.6):
            path_in_tar_gz = path[len(os.path.join(dir, '')):]
            if filter(path_in_tar_gz):
                # copy over real files, not symlinks
                real_path = os.path.realpath(path)
                tar_gz.add(real_path, arcname=path_in_tar_gz, recursive=False)

    tar_gz.close()

def unarchive(archive_path, dest):
    """Extract the contents of a tar or zip file at *archive_path* into the directory *dest*.

    :type archive_path: str
    :param archive_path: path to archive file
    :type dest: str
    :param dest: path to directory where archive will be extracted

    *dest* will be created if it doesn't already exist.

    tar files can be gzip compressed, bzip2 compressed, or uncompressed. Files within zip
    files can be deflated or stored.
    """
    if tarfile.is_tarfile(archive_path):
        with contextlib.closing(tarfile.open(archive_path, 'r')) as archive:
            archive.extractall(dest)
    elif zipfile.is_zipfile(archive_path):
        with contextlib.closing(zipfile.ZipFile(archive_path, 'r')) as archive:
            for name in archive.namelist():
                # the zip spec specifies that front slashes are always
                # used as directory separators
                dest_path = os.path.join(dest, *name.split('/'))

                # now, split out any dirname and filename and create
                # one and/or the other
                dirname, filename = os.path.split(dest_path)
                if dirname and not os.path.exists(dirname):
                    os.makedirs(dirname)
                if filename:
                    with open(dest_path, 'wb') as dest_file:
                        dest_file.write(archive.read(name))
    else:
        raise IOError('Unknown archive type: %s' % (archive_path,))
