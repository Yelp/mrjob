# Copyright 2012 Yelp and Contributors
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
"""Utilities for setting up the environment jobs run in by uploading files
and running setup scripts.

The general idea is to use Hadoop DistributedCache-like syntax to find and
parse expressions like ``/path/to/file#name_in_working_dir`` into "path
dictionaries" like
``{'type': 'file', 'path': '/path/to/file', 'name': 'name_in_working_dir'}}``.

You can then pass these into a :py:class:`WorkingDirManager` to keep
track of which files need to be uploaded, catch name collisions, and assign
names to unnamed paths (e.g. ``/path/to/file#``). Note that
:py:meth:`WorkingDirManager.name` can take a path dictionary as keyword
arguments.

If you need to upload files from the local filesystem to a place where
Hadoop can see them (HDFS or S3), we provide :py:class:`UploadDirManager`.

Path dictionaries are meant to be immutable; all state is handled by
manager classes.
"""
from __future__ import with_statement

import itertools
import logging
import os.path
import posixpath

from mrjob.parse import is_uri


log = logging.getLogger(__name__)


_SUPPORTED_TYPES = ('archive', 'file')


# TODO: This is a model for how we expect to handle "new" hash paths to work.
# This may not need to exist as a separate function because our final
# goal is to parse these expressions out of a command-line with a regex,
# in which case most of the parsing work will already be done, and many
# of the error cases will be impossible.
def parse_hash_path(hash_path):
    """Parse Hadoop Distributed Cache-style paths into a dictionary.

    For example:
    >>> parse_hash_path('/foo/bar.py#baz.py')
    {'path': '/foo/bar.py', 'name': 'baz.py', 'type': 'file'}

    A slash at the end of the name indicates an archive:
    >>> parse_hash_path('/foo/bar.tar.gz#baz/')
    {'path': '/foo/bar.py', 'name': 'baz.py', 'type': 'archive'}

    An empty name (e.g. ``'/foo/bar.py#'``) indicates any name is acceptable.
    """
    if not '#' in hash_path:
        raise ValueError('bad hash path %r, must contain #' % (hash_path,))

    path, name = hash_path.split('#', 1)

    if name.endswith('/'):
        type = 'archive'
        name = name[:-1]
    else:
        type = 'file'

    if not path:
        raise ValueError('Path may not be empty!')

    if '/' in name or '#' in name:
        raise ValueError('bad path %r; name must not contain # or /' % (path,))

    # use None for no name
    if not name:
        name = None

    return {'path': path, 'name': name, 'type': type}


def parse_legacy_hash_path(type, path, must_name=None):
    """Parse hash paths from old setup/bootstrap options.

    This is similar to :py:func:`parse_hash_path` except that we pass in
    path type explicitly, and we don't always require the hash.

    :param type: Type of the path (``'archive'`` or ``'file'``)
    :param path: Path to parse, possibly with a ``#``
    :param must_name: If set, use *path*'s filename as its name if there
                      is no ``'#'`` in *path*, and raise an exception
                      if there is just a ``'#'`` with no name. Set *must_name*
                      to the name of the relevant option so we can print
                      a useful error message. This is intended for options
                      like ``upload_files`` that merely upload a file
                      without tracking it.
    """
    if type not in _SUPPORTED_TYPES:
        raise ValueError('bad path type %r, must be one of %s' % (
            type, ', '.join(sorted(_SUPPORTED_TYPES))))

    if '#' in path:
        path, name = path.split('#', 1)

        # allow a slash after the name of an archive because that's
        # the new-way of specifying archive paths
        if name.endswith('/') and type == 'archive':
            name = name[:-1]

        if '/' in name or '#' in name:
            raise ValueError(
                'Bad path %r; name must not contain # or /' % (path,))
    else:
        if must_name:
            name = os.path.basename(path)
        else:
            name = None

    if not path:
        raise ValueError('Path may not be empty!')

    if not name:
        if must_name:
            raise ValueError(
                'Empty name makes no sense for %s: %r' % (must_name, path))
        else:
            name = None

    return {'path': path, 'name': name, 'type': type}


def name_uniquely(path, names_taken=(), proposed_name=None):
    """Come up with a unique name for *path*.

    :param names_taken: a dictionary or set of names not to use.
    :param proposed_name: name to use if it is not taken. If this is not set,
                          we propose a name based on the filename.

    If the proposed name is taken, we add a number to the end of the
    filename, keeping the extension the same. For example:

    >>> name_uniquely('foo.tar.gz', set(['foo.tar.gz']))
    'foo-1.tar.gz'
    """
    if not proposed_name:
        proposed_name = os.path.basename(path) or '_'

    if proposed_name not in names_taken:
        return proposed_name

    dot_idx = proposed_name.find('.', 1)
    if dot_idx == -1:
        prefix, suffix = proposed_name, ''
    else:
        prefix, suffix = proposed_name[:dot_idx], proposed_name[dot_idx:]

    for i in itertools.count(1):
        name = '%s-%d%s' % (prefix, i, suffix)
        if name not in names_taken:
            return name


class UploadDirManager(object):
    """Represents a directory on HDFS or S3 where we want to upload
    local files for consumption by Hadoop.

    :py:class:`UploadDirManager` tries to give files the same name as their
    filename in the path (for ease of debugging), but handles collisions
    gracefully.

    :py:class:`UploadDirManager` assumes URIs to not need to be uploaded
    and thus does not store them. :py:meth:`uri` maps URIs to themselves.
    """
    def __init__(self, prefix):
        """Make an :py:class`UploadDirManager`.

        :param string prefix: The URI for the directory (e.g.
                              `s3://bucket/dir/`). It doesn't matter if
                              *prefix* has a trailing slash; :py:meth:`uri`
                              will do the right thing.
        """
        self.prefix = prefix

        self._path_to_name = {}
        self._names_taken = set()

    def add(self, path):
        """Add a path. If *path* hasn't been added before, assign it a name.
                       If *path* is a URI don't add it; just return the URI.

        :return: the URI assigned to the path"""
        if is_uri(path):
            return path

        if path not in self._path_to_name:
            name = name_uniquely(path, names_taken=self._names_taken)
            self._names_taken.add(name)
            self._path_to_name[path] = name

        return self.uri(path)

    def uri(self, path):
        """Get the URI for the given path. If *path* is a URI, just return it.
        """
        if is_uri(path):
            return path

        if path in self._path_to_name:
            return posixpath.join(self.prefix, self._path_to_name[path])
        else:
            raise ValueError('%r is not a URI or a known local file' % (path,))

    def path_to_uri(self):
        """Get a map from path to URI for all paths that were added,
        so we can figure out which files we need to upload."""
        return dict((path, self.uri(path))
                    for path in self._path_to_name)


class WorkingDirManager(object):
    """Represents the working directory of hadoop tasks (or bootstrap
    commands on EMR).

    To support Hadoop's distributed cache, paths can be for ordinary
    files, or for archives (which are automatically uncompressed into
    a directory by Hadoop).

    When adding a file, you may optionally assign it a name; if you don't;
    we'll lazily assign it a name as needed. Name collisions are not allowed,
    so being lazy makes it easier to avoid unintended collisions.

    If you wish, you may assign multiple names to the same file, or add
    a path as both a file and an archive (though not mapped to the same name).
    """
    _SUPPORTED_TYPES = _SUPPORTED_TYPES

    def __init__(self):
        # map from paths added without a name to None or lazily chosen name
        self._typed_path_to_auto_name = {}
        self._name_to_typed_path = {}

    def add(self, type, path, name=None):
        """Add a path as either a file or an archive, optionally
        assigning it a name.

        :param type: either ``'archive'` or `'file'`
        :param path: path/URI to add
        :param name: optional name that this path *must* be assigned, or
                     None to assign this file a name later.
        """
        self._check_name(name)
        self._check_type(type)

        # stop name collisions
        if name in self._name_to_typed_path:
            current_type, current_path = self._name_to_typed_path[name]

            if (type, path) == (current_type, current_path):
                return  # already added
            else:
                raise ValueError(
                    "%s %s#%s won't work because we already have %s %s#%s" % (
                        type, path, name, current_type, current_path, name))

        # if a name was specified, reserve it
        if name:
            self._name_to_typed_path[name] = (type, path)
        # otherwise, get ready to auto-name the file
        else:
            self._typed_path_to_auto_name.setdefault((type, path), None)

    def name(self, type, path, name=None):
        """Get the name for a path previously added to this
        :py:class:`WorkingDirManager`, assigning one as needed.

        This is primarily for getting the name of auto-named files. If
        the file was added with an assigned name, you must include it
        (and we'll just return *name*).

        We won't ever give an auto-name that's the same an assigned name
        (even for the same path and type).

        :param type: either ``'archive'` or `'file'`
        :param path: path/URI
        :param name: known name of the file
        """
        self._check_name(name)
        self._check_type(type)

        if name:
            if name not in self._name_to_typed_path:
                raise ValueError('unknown name: %r' % name)

            return name

        if (type, path) not in self._typed_path_to_auto_name:
            # print useful error message
            if (type, path) in self._name_to_typed_path.itervalues():
                raise ValueError('%s %r was never added without a name!' %
                                 (type, path))
            else:
                raise ValueError('%s %r was never added!' % (type, path))

        if not self._typed_path_to_auto_name[(type, path)]:
            name = name_uniquely(path, names_taken=self._name_to_typed_path)
            self._name_to_typed_path[name] = (type, path)
            self._typed_path_to_auto_name[(type, path)] = name

        return self._typed_path_to_auto_name[(type, path)]

    def name_to_path(self, type):
        """Get a map from name (in the setup directory) to path for
        all known files/archives, so we can build :option:`-file` and
        :option:`-archive` options to Hadoop (or fake them in a bootstrap
        script).

        :param type: either ``'archive'` or `'file'`
        """
        self._check_type(type)

        for path_type, path in self._typed_path_to_auto_name:
            if path_type == type:
                self.name(type, path)

        return dict((name, typed_path[1])
                    for name, typed_path
                    in self._name_to_typed_path.iteritems()
                    if typed_path[0] == type)

    def paths(self):
        """Get a set of all paths tracked by this WorkingDirManager."""
        paths = set()

        paths.update(p for (t, p) in self._typed_path_to_auto_name)
        paths.update(p for (t, p) in self._name_to_typed_path.itervalues())

        return paths

    def _check_name(self, name):
        if name is None:
            return

        if not isinstance(name, basestring):
            raise TypeError('name must be a string or None: %r' % (name,))

        if '/' in name:
            raise ValueError('names may not contain slashes: %r' % (name,))

    def _check_type(self, type):
        if not type in self._SUPPORTED_TYPES:
            raise ValueError('bad path type %r, must be one of %s' % (
                type, ', '.join(sorted(self._SUPPORTED_TYPES))))


class BootstrapWorkingDirManager(WorkingDirManager):
    """Manage the working dir for the master bootstrap script. Identical
    to :py:class:`WorkingDirManager` except that it doesn't support archives.
    """
    _SUPPORTED_TYPES = ('file',)
