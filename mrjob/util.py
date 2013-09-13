# Copyright 2009-2013 Yelp and Contributors
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

from collections import defaultdict
import contextlib
from copy import deepcopy
from datetime import timedelta
import glob
import hashlib
import itertools
import logging
import os
import pipes
import shlex
import sys
import tarfile
import zipfile
import zlib

try:
    import bz2
    bz2  # redefine bz2 for pepflakes
except ImportError:
    bz2 = None

#: .. deprecated:: 0.4
is_ironpython = "IronPython" in sys.version


class NullHandler(logging.Handler):
    def emit(self, record):
        pass


def bash_wrap(cmd_str):
    """Escape single quotes in a shell command string and wrap it with ``bash
    -c '<string>'``.

    This low-tech replacement works because we control the surrounding string
    and single quotes are the only character in a single-quote string that
    needs escaping.
    """
    return "bash -c '%s'" % cmd_str.replace("'", "'\\''")


def buffer_iterator_to_line_iterator(iterator):
    """boto's file iterator splits by buffer size instead of by newline. This
    wrapper puts them back into lines.
    """
    buf = iterator.next()  # might raise StopIteration, but that's okay
    while True:
        if '\n' in buf:
            (line, buf) = buf.split('\n', 1)
            yield line + '\n'
        else:
            try:
                more = iterator.next()
                buf += more
            except StopIteration:
                if buf:
                    yield buf + '\n'
                return


def cmd_line(args):
    """build a command line that works in a shell.
    """
    args = [str(x) for x in args]
    return ' '.join(pipes.quote(x) for x in args)


def extract_dir_for_tar(archive_path, compression='gz'):
    """Get the name of the directory the tar at *archive_path* extracts into.

    :type archive_path: str
    :param archive_path: path to archive file
    :type compression: str
    :param compression: Compression type to use. This can be one of ``''``,
                        ``bz2``, or ``gz``.
    """
    # Open the file for read-only streaming (no random seeks)
    tar = tarfile.open(archive_path, mode='r|%s' % compression)
    # Grab the first item
    first_member = tar.next()
    tar.close()
    # Return the first path component of the item's name
    return first_member.name.split('/')[0]


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


def hash_object(obj):
    """Generate a hash (currently md5) of the ``repr`` of the object"""
    m = hashlib.md5()
    m.update(repr(obj))
    return m.hexdigest()


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
    :type stderr: file object
    :param stderr:  stream to log to (default is ``sys.stderr``)
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


def _process_long_opt(option_parser, rargs, values, dests):
    """Mimic function of the same name in ``OptionParser``, capturing the
    arguments consumed in *arg_map*
    """
    arg = rargs.pop(0)

    # Value explicitly attached to arg?  Pretend it's the next
    # argument.
    if "=" in arg:
        (opt, next_arg) = arg.split("=", 1)
        rargs.insert(0, next_arg)
    else:
        opt = arg

    opt = option_parser._match_long_opt(opt)
    option = option_parser._long_opt[opt]

    # Store the 'before' value of *rargs*
    rargs_before_processing = [x for x in rargs]

    if option.takes_value():
        nargs = option.nargs
        if nargs == 1:
            value = rargs.pop(0)
        else:
            value = tuple(rargs[0:nargs])
            del rargs[0:nargs]
    else:
        value = None

    option.process(opt, value, values, option_parser)

    if dests is None or option.dest in dests:
        # Measure rargs before and after processing. Yield difference.
        length_difference = len(rargs_before_processing) - len(rargs)
        for item in [opt] + rargs_before_processing[:length_difference]:
            yield option.dest, item


def _process_short_opts(option_parser, rargs, values, dests):
    """Mimic function of the same name in ``OptionParser``, capturing the
    arguments consumed in *arg_map*
    """
    arg = rargs.pop(0)
    stop = False
    i = 1
    for ch in arg[1:]:
        opt = "-" + ch
        option = option_parser._short_opt.get(opt)
        i += 1                      # we have consumed a character

        # Store the 'before' value of *rargs*
        rargs_before_processing = [x for x in rargs]

        # We won't see a difference in rargs for things like '-pJSON', so
        # handle that edge case explicitly.
        args_from_smashed_short_opt = []

        if option.takes_value():
            # Any characters left in arg?  Pretend they're the
            # next arg, and stop consuming characters of arg.
            if i < len(arg):
                rargs.insert(0, arg[i:])
                args_from_smashed_short_opt.append(arg[i:])
                stop = True

            nargs = option.nargs
            if nargs == 1:
                value = rargs.pop(0)
            else:
                value = tuple(rargs[0:nargs])
                del rargs[0:nargs]

        else:                       # option doesn't take a value
            value = None

        option.process(opt, value, values, option_parser)

        if dests is None or option.dest in dests:
            # Measure rargs before and after processing. Yield difference.
            length_difference = len(rargs_before_processing) - len(rargs)
            for item in ([opt] + args_from_smashed_short_opt +
                         rargs_before_processing[:length_difference]):
                yield option.dest, item

        if stop:
            break


def _args_for_opt_dest_subset(option_parser, args, dests=None):
    """See docs for :py:func:`args_for_opt_dest_subset()`. This function allows
    us to write a compatibility wrapper for the old API
    (:py:func:`parse_and_save_options()`).
    """
    values = deepcopy(option_parser.get_default_values())
    rargs = [x for x in args]
    option_parser.rargs = rargs
    while rargs:
        arg = rargs[0]
        if arg == '--':
            del rargs[0]
            return
        elif arg[0:2] == '--':
            for item in _process_long_opt(option_parser, rargs, values, dests):
                yield item
        elif arg[:1] == '-' and len(arg) > 1:
            for item in _process_short_opts(option_parser, rargs, values,
                                            dests):
                yield item
        else:
            del rargs[0]


def args_for_opt_dest_subset(option_parser, args, dests=None):
    """For the given :py:class:`OptionParser` and list of command line
    arguments *args*, yield values in *args* that correspond to option
    destinations in the set of strings *dests*. If *dests* is None, return
    *args* as parsed by :py:class:`OptionParser`.
    """
    for dest, value in _args_for_opt_dest_subset(option_parser, args, dests):
        yield value


def parse_and_save_options(option_parser, args):
    """DEPRECATED. To be removed in v0.5.

    Duplicate behavior of :py:class:`OptionParser`, but capture the strings
    required to reproduce the same values. Ref. optparse.py lines 1414-1548
    (python 2.6.5)
    """
    arg_map = defaultdict(list)
    for dest, value in _args_for_opt_dest_subset(option_parser, args, None):
        arg_map[dest].append(value)
    return arg_map


def populate_option_groups_with_options(assignments, indexed_options):
    """Given a dictionary mapping :py:class:`OptionGroup` and
    :py:class:`OptionParser` objects to a list of strings represention option
    dests, populate the objects with options from ``indexed_options``
    (generated by :py:func:`scrape_options_and_index_by_dest`) in alphabetical
    order by long option name. This function primarily exists to serve
    :py:func:`scrape_options_into_new_groups`.

    :type assignments: dict of the form ``{my_option_parser: ('verbose',
                       'help', ...), my_option_group: (...)}``
    :param assignments: specification of which parsers/groups should get which
                        options
    :type indexed_options: dict generated by
                           :py:func:`util.scrape_options_and_index_by_dest`
    :param indexed_options: options to use when populating the parsers/groups
    """
    for opt_group, opt_dest_list in assignments.iteritems():
        new_options = []
        for option_dest in assignments[opt_group]:
            for option in indexed_options[option_dest]:
                new_options.append(option)
        # New options must be added using add_options() or they will not be
        # allowed by the parser on the command line
        opt_group.add_options(new_options)
        # Sort alphabetically for help
        opt_group.option_list = sorted(opt_group.option_list,
                                       key=lambda item: item.get_opt_string())


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
    for line in read_file(path):
        yield line


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
                         ``False`` (useful for :py:class:`boto.s3.Key`)
    :param cleanup: Optional callback to call with no arguments when EOF is
                    reached or an exception is thrown.
    """
    # sometimes values declared in the ``try`` block aren't accessible from the
    # ``finally`` block. not sure why.
    f = None
    try:
        # open path if we need to
        if fileobj is None:
            f = open(path)
        else:
            f = fileobj

        if path.endswith('.gz'):
            lines = buffer_iterator_to_line_iterator(gunzip_stream(f))
        elif path.endswith('.bz2'):
            if bz2 is None:
                raise Exception('bz2 module was not successfully imported'
                                ' (likely not installed).')
            else:
                lines = bunzip2_stream(f)
        else:
            if yields_lines:
                lines = f
            else:
                # handle boto.s3.Key, which yields chunks of bytes, not lines
                lines = buffer_iterator_to_line_iterator(f)

        for line in lines:
            yield line
    finally:
        try:
            if f and f is not fileobj:
                f.close()
        finally:
            if cleanup:
                cleanup()


def bunzip2_stream(fileobj):
    """Return an uncompressed bz2 stream from a file object
    """
    # decompress chunks into a buffer, then stream from the buffer
    buffer = ''
    if bz2 is None:
        raise Exception(
            'bz2 module was not successfully imported (likely not installed).')
    decomp = bz2.BZ2Decompressor()
    for part in fileobj:
        buffer = buffer.join(decomp.decompress(part))
    f = buffer.splitlines(True)
    return f


def gunzip_stream(fileobj, bufsize=1024):
    """Decompress gzipped data on the fly.

    :param fileobj: object supporting ``read()``
    :param bufsize: number of bytes to read from *fileobj* at a time. The
                    default is the same as in :py:mod:`gzip`.

    This yields decompressed chunks; it does *not* split on lines. Use
    :py:func:`buffer_iterator_to_line_iterator` for that.
    """
    # see Issue #601 for why we need this.

    # we need this flag to read gzip rather than raw zlib, but it's not
    # actually defined in zlib, so we define it here.
    READ_GZIP_DATA = 16
    d = zlib.decompressobj(READ_GZIP_DATA | zlib.MAX_WBITS)
    while True:
        chunk = fileobj.read(bufsize)
        if not chunk:
            return
        data = d.decompress(chunk)
        if data:
            yield data


@contextlib.contextmanager
def save_current_environment():
    """ Context manager that saves os.environ and loads
        it back again after execution
    """
    original_environ = os.environ.copy()

    yield

    os.environ.clear()
    os.environ.update(original_environ)


@contextlib.contextmanager
def save_cwd():
    """Context manager that saves the current working directory,
    and chdir's back to it after execution."""
    original_cwd = os.getcwd()

    yield

    os.chdir(original_cwd)


def scrape_options_and_index_by_dest(*parsers_and_groups):
    """Scrapes ``optparse`` options from :py:class:`OptionParser` and
    :py:class:`OptionGroup` objects and builds a dictionary of
    ``dest_var: [option1, option2, ...]``. This function primarily exists to
    serve :py:func:`scrape_options_into_new_groups`.

    An example return value: ``{'verbose': [<verbose_on_option>,
    <verbose_off_option>], 'files': [<file_append_option>]}``

    :type parsers_and_groups: :py:class:`OptionParser` or
                              :py:class:`OptionGroup`
    :param parsers_and_groups: Parsers and groups to scrape option objects from

    :return: dict of the form ``{dest_var: [option1, option2, ...], ...}``
    """

    # Scrape options from MRJob and index them by dest
    all_options = {}
    job_option_lists = [g.option_list for g in parsers_and_groups]
    for option in itertools.chain(*job_option_lists):
        other_options = all_options.get(option.dest, [])
        other_options.append(option)
        all_options[option.dest] = other_options
    return all_options


def scrape_options_into_new_groups(source_groups, assignments):
    """Puts options from the :py:class:`OptionParser` and
    :py:class:`OptionGroup` objects in `source_groups` into the keys of
    `assignments` according to the values of `assignments`. An example:

    :type source_groups: list of :py:class:`OptionParser` and
                         :py:class:`OptionGroup` objects
    :param source_groups: parsers/groups to scrape options from
    :type assignments: dict with keys that are :py:class:`OptionParser` and
                       :py:class:`OptionGroup` objects and values that are
                       lists of strings
    :param assignments: map empty parsers/groups to lists of destination names
                        that they should contain options for
    """
    all_options = scrape_options_and_index_by_dest(*source_groups)
    return populate_option_groups_with_options(assignments, all_options)


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


def tar_and_gzip(dir, out_path, filter=None, prefix=''):
    """Tar and gzip the given *dir* to a tarball at *out_path*.

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

    # supposedly you can also call tarfile.TarFile(), but I couldn't
    # get this to work in Python 2.5.1. Please leave as-is.
    tar_gz = tarfile.open(out_path, mode='w:gz')

    for dirpath, dirnames, filenames in os.walk(dir):
        for filename in filenames:
            path = os.path.join(dirpath, filename)
            # janky version of os.path.relpath() (Python 2.6):
            rel_path = path[len(os.path.join(dir, '')):]
            if filter(rel_path):
                # copy over real files, not symlinks
                real_path = os.path.realpath(path)
                path_in_tar_gz = os.path.join(prefix, rel_path)
                tar_gz.add(real_path, arcname=path_in_tar_gz, recursive=False)

    tar_gz.close()


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
