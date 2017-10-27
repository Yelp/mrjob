# Copyright 2009-2016 Yelp and Contributors
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
"""Utility functions for MRJob
"""

# don't add imports here that aren't part of the standard Python library,
# since MRJobs need to run in Amazon's generic EMR environment
import contextlib
import glob
import logging
import os
import os.path
import pipes
import random
import shlex
import shutil
import sys
import tarfile
from datetime import timedelta
from distutils.spawn import find_executable
from logging import getLogger
from optparse import OptionParser
from zipfile import ZIP_DEFLATED
from zipfile import ZIP_STORED
from zipfile import ZipFile
from zipfile import is_zipfile

from mrjob.cat import decompress
from mrjob.py2 import PY2
from mrjob.py2 import string_types

log = getLogger(__name__)


class NullHandler(logging.Handler):
    def emit(self, record):
        pass


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


def file_ext(filename):
    """return the file extension, including the ``.``

    >>> file_ext('foo.tar.gz')
    '.tar.gz'
    """
    dot_index = filename.find('.')
    if dot_index == -1:
        return ''
    return filename[dot_index:]


def log_to_null(name=None):
    """Set up a null handler for the given stream, to suppress
    "no handlers could be found" warnings."""
    logger = logging.getLogger(name)
    logger.addHandler(NullHandler())


def log_to_stream(name=None, stream=None, format=None, level=None,
                  debug=False):
    """Set up logging.

    :type name: str
    :param name: name of the logger, or ``None`` for the root logger
    :type stream: file object
    :param stream:  stream to log to (default is ``sys.stderr``)
    :type format: str
    :param format: log message format (default is '%(message)s')
    :param level: log level to use
    :type debug: bool
    :param debug: quick way of setting the log level: if true, use
                  ``logging.DEBUG``, otherwise use ``logging.INFO``
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


def parse_and_save_options(option_parser, args):
    """Return a map from option name (``dest``) to a list of the arguments
    in *args* that correspond to that *dest*.

    This won't modify *option_parser*.

    .. deprecated:: 0.6.0
    """
    arg_map = {}

    def sim_callback(option, opt_str, value, parser):
        dest = option.dest
        arg_map.setdefault(dest, [])

        arg_map[dest].append(opt_str)
        if isinstance(value, string_types):
            arg_map[dest].append(value)
        elif value:
            arg_map[dest].extend(value)

    sim_parser = OptionParser(add_help_option=False)

    # optparse is no longer being maintained, so it's safe to access
    # hidden methods and attributes
    for option in option_parser._get_all_options():
        sim_parser.add_option(
            *(option._short_opts + option._long_opts),
            dest=option.dest,
            nargs=option.nargs,
            action='callback',
            type=('string' if option.type else None),
            callback=sim_callback)

    sim_parser.parse_args(args)

    return arg_map


def random_identifier():
    """A random 16-digit hex string."""
    return '%016x' % random.randint(0, 2 ** 64 - 1)


def read_file(path, fileobj=None, yields_lines=True, cleanup=None):
    """Yields lines from a file, possibly decompressing it based on file
    extension.

    Currently we handle compressed files with the extensions ``.gz`` and
    ``.bz2``.

    :param string path: file path. Need not be a path on the local filesystem
                        (URIs are okay) as long as you specify *fileobj* too.
    :param fileobj: file object to read from. Need not be seekable. If this
                    is omitted, we ``open(path)``.
    :param yields_lines: Does iterating over *fileobj* yield lines (like
                         file objects are supposed to)? If not, set this to
                         ``False`` (useful for objects that correspond
                         to objects on cluster filesystems)
    :param cleanup: Optional callback to call with no arguments when EOF is
                    reached or an exception is thrown.

    .. deprecated:: 0.6.0
    """
    log.warning('read_file() is deprecated and will be removed in v0.7.0.'
                ' Try mrjob.cat.decompress() and mrjob.util.to_lines()')

    # sometimes values declared in the ``try`` block aren't accessible from the
    # ``finally`` block. not sure why.
    f = None
    try:
        # open path if we need to
        if fileobj is None:
            f = open(path, 'rb')
        else:
            f = fileobj

        decompressed_f = decompress(f, path)

        if decompressed_f is f and yields_lines:
            # this could be important; iterating over to_lines(f) is about 8x
            # slower than iterating over f
            lines = f
        else:
            lines = to_lines(decompressed_f)

        for line in lines:
            yield line
    finally:
        try:
            if f and f is not fileobj:
                f.close()
        finally:
            if cleanup:
                cleanup()


def read_input(path, stdin=None):
    """Stream input the way Hadoop would.

    - Resolve globs (``foo_*.gz``).
    - Decompress ``.gz`` and ``.bz2`` files.
    - If path is ``'-'``, read from stdin
    - If path is a directory, recursively read its contents.

    You can redefine *stdin* for ease of testing. *stdin* can actually be
    any iterable that yields lines (e.g. a list).

    .. deprecated:: 0.6.0
    """
    log.warning('read_input() is deprecated and will be removed in v0.7.0.'
                ' Try mrjob.cat.decompress() and mrjob.util.to_lines()')

    for line in _read_input(path, stdin=stdin):
        yield line


def _read_input(path, stdin=None):
    """Helper function for _read_input() (to avoid getting recursive
    deprecation warnings)"""
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
        for dirname, _, filenames in os.walk(path, followlinks=True):
            for filename in filenames:
                for line in read_input(os.path.join(dirname, filename),
                                       stdin=stdin):
                    yield line
        return

    # read from files
    for line in read_file(path):
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
    safe_globals = {
        'False': False,
        'None': None,
        'True': True,
        '__builtin__': None,
        '__builtins__': None,
        'set': set
    }

    # xrange is range in Python 3
    if PY2:
        safe_globals['xrange'] = xrange
    else:
        safe_globals['range'] = range

    # PyPy needs special magic
    def open(*args, **kwargs):
        raise NameError("name 'open' is not defined")
    safe_globals['open'] = open

    # add the user-specified global variables
    if globals:
        safe_globals.update(globals)

    return eval(expr, safe_globals, locals)


@contextlib.contextmanager
def save_current_environment():
    """ Context manager that saves os.environ and loads
        it back again after execution
    """
    original_environ = os.environ.copy()

    try:
        yield

    finally:
        os.environ.clear()
        os.environ.update(original_environ)


@contextlib.contextmanager
def save_cwd():
    """Context manager that saves the current working directory,
    and chdir's back to it after execution."""
    original_cwd = os.getcwd()

    try:
        yield

    finally:
        os.chdir(original_cwd)


def shlex_split(s):
    """Wrapper around shlex.split(), but convert to str if Python version <
    2.7.3 when unicode support was added.
    """
    if sys.version_info < (2, 7, 3):
        return shlex.split(str(s))
    else:
        return shlex.split(s)


def strip_microseconds(delta):
    """Return the given :py:class:`datetime.timedelta`, without microseconds.

    Useful for printing :py:class:`datetime.timedelta` objects.
    """
    return timedelta(delta.days, delta.seconds)


def to_lines(chunks):
    """Take in data as a sequence of bytes, and yield it, one line at a time.

    Only breaks lines on ``\\n`` (not ``\\r``), and does not add
    a trailing newline.

    For efficiency, passes through anything with a ``readline()`` attribute.
    """
    # hopefully this is good enough for anything mrjob will encounter
    if hasattr(chunks, 'readline'):
        return chunks
    else:
        return _to_lines(chunks)


def _to_lines(chunks):
    """Take in data as a sequence of bytes, and yield it, one line at a time.

    Only breaks lines on ``\\n`` (not ``\\r``), and does not add
    a trailing newline.

    Exception: if we encounter an empty bytestring ``b''``, immediately yield
    what we have so far rather than joining it to the next chunk. This allows
    us to handle bytes from multiple files without joining the end of one
    file to the beginning of the next one.

    Optimizes for:

    * chunks bigger than lines (e.g. reading test files)
    * chunks that are lines (idempotency)
    """
    # list of chunks with no final newline
    leftovers = []

    for chunk in chunks:
        # special case for b'' standing for EOF
        if chunk == b'':
            if leftovers:
                yield b''.join(leftovers)
                leftovers = []

            continue

        start = 0

        while start < len(chunk):
            end = chunk.find(b'\n', start) + 1

            if end == 0:  # no newlines found
                leftovers.append(chunk[start:])
                break

            if leftovers:
                leftovers.append(chunk[start:end])
                yield b''.join(leftovers)
                leftovers = []
            else:
                yield chunk[start:end]

            start = end

    if leftovers:
        yield b''.join(leftovers)


def unique(items):
    """Yield items from *item* in order, skipping duplicates."""
    seen = set()

    for item in items:
        if item in seen:
            continue
        else:
            yield item
            seen.add(item)


def unarchive(archive_path, dest):
    """Extract the contents of a tar or zip file at *archive_path* into the
    directory *dest*.

    :type archive_path: str
    :param archive_path: path to archive file
    :type dest: str
    :param dest: path to directory where archive will be extracted

    *dest* will be created if it doesn't already exist.

    tar files can be gzip compressed, bzip2 compressed, or uncompressed. Files
    within zip files can be deflated or stored.
    """
    if tarfile.is_tarfile(archive_path):
        with tarfile.open(archive_path, 'r') as archive:
            archive.extractall(dest)
    elif is_zipfile(archive_path):
        with ZipFile(archive_path, 'r') as archive:
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


def which(cmd, path=None):
    """Like the UNIX which command: search in *path* for the executable named
    *cmd*. *path* defaults to :envvar:`PATH`. Returns ``None`` if no
    such executable found.

    This is basically ``shutil.which()`` (which was introduced in Python 3.3)
    without the *mode* argument. Best practice is to always specify *path*
    as a keyword argument.
    """
    if hasattr(shutil, 'which'):
        # added in Python 3.3
        return shutil.which(cmd, path=path)
    elif path is None and os.environ.get('PATH') is None:
        # find_executable() errors if neither path nor $PATH is set
        return None
    else:
        return find_executable(cmd, path=path)


def zip_dir(dir, out_path, filter=None, prefix=''):
    """Compress the given *dir* into a zip file at *out_path*.

    If we encounter symlinks, include the actual file, not the symlink.

    :type dir: str
    :param dir: dir to tar up
    :type out_path: str
    :param out_path: where to write the tarball too
    :param filter: if defined, a function that takes paths (relative to *dir*
                   and returns ``True`` if we should keep them
    :type prefix: str
    :param prefix: subdirectory inside the tarball to put everything into (e.g.
                   ``'mrjob'``)
    """
    if not os.path.isdir(dir):
        raise IOError('Not a directory: %r' % (dir,))

    if not filter:
        filter = lambda path: True

    def create_zip_file():
        try:
            return ZipFile(out_path, mode='w', compression=ZIP_DEFLATED)
        except RuntimeError:  # zlib not available
            return ZipFile(out_path, mode='w', compression=ZIP_STORED)

    with create_zip_file() as zip_file:
        for dirpath, dirnames, filenames in os.walk(dir, followlinks=True):
            for filename in filenames:
                path = os.path.join(dirpath, filename)
                rel_path = os.path.relpath(path, dir)

                if filter(rel_path):
                    # copy over real files, not symlinks
                    real_path = os.path.realpath(path)
                    path_in_zip_file = os.path.join(prefix, rel_path)
                    zip_file.write(real_path, arcname=path_in_zip_file)
