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

# NOTE: this code is in cmd.py because it's part of an effort to integrate
# file uploads into command lines. See Issue #206.
from __future__ import with_statement

import itertools
import os.path
import posixpath


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


class ScratchDir(object):
    """Represents a directory on HDFS or S3 where we want to park files
    for consumption by Hadoop.

    :py:class:`ScratchDir` tries to give files the same name as their filename
    in the path (for ease of debugging), but handles collisions gracefully.
    """
    def __init__(self, prefix):
        """Make an :py:class`ScratchDir`.

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

        :return: the URI assigned to the path"""
        if path not in self._path_to_name:
            name = name_uniquely(path, names_taken=self._names_taken)
            self._names_taken.add(name)
            self._path_to_name[path] = name

        return self.uri(path)

    def uri(self, path):
        """Get the URI for the given path. If it's a path we don't know
        about, just return *path*.

        (This makes it simpler to skip uploading URIs.)
        """
        if path in self._path_to_name:
            return posixpath.join(self.prefix, self._path_to_name[path])
        else:
            return path

    def path_to_uri(self):
        """Get a map from path to URI for all paths that were added,
        so we can figure out which files we need to upload."""
        return dict((path, self.uri(path))
                    for path in self._path_to_name)


class WorkingDir(object):
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
    _SUPPORTED_TYPES = ('archive', 'file')

    def __init__(self):
        self._typed_paths = set()
        self._typed_path_to_default_name = {}
        self._name_to_typed_path = {}

    def add(self, type, path, name=None):
        """Add a path as either a file or an archive, optionally
        assigning it a name.

        :param type: either ``'archive'` or `'file'`
        :param path: path/URI to add
        :param name: optional name that this path *must* be assigned. If
                     the name is already taken, raise an exception. Setting
                     name to ``''`` is the same as setting it to ``None``
                     (we don't allow empty names).
        """
        self._check_name(name)
        self._check_type(type)

        # register this file/archive
        self._typed_paths.add((type, path))

        if name:
            if name in self._name_to_typed_path:
                # Name is taken! Check that it belongs to this file/archive
                current_type, current_path = self._name_to_typed_path[name]

                if (type, path) != (current_type, current_path):
                    raise ValueError(
                        "%s won't work because we already have %" % (
                            self._desc(type, path, name),
                            self._desc(current_type, current_path, name)))
            else:
                # Name is not taken! Reserve it
                self._name_to_typed_path[(type, path)] = name
                # currently, we don't update _typed_path_to_default_name
                # on the theory that it might cause surprises. For example,
                # if someone uploads foo.py#bar.py and foo.py#, and deletes
                # bar.py with a setup script, the copy we expect to use
                # for foo.py# should still be there.

    def name(self, type, path):
        """Get the name for a path previously added to this
        :py:class:`WorkingDir`. If we haven't chosen a name yet, assign one.

        :param type: either ``'archive'` or `'file'`
        :param path: path/URI
        """
        self._check_type(type)

        if (type, path) not in self._typed_path:
            raise ValueError('%s was never added!' % self._desc(type, path))

        if (type, path) not in self._typed_path_to_default_name:
            name = name_uniquely(path, names_taken=self._name_to_typed_path)
            self._name_to_typed_path[name] = (type, path)

            self._typed_path_to_default_name[(type, path)] = name

        return self._typed_path_to_default_name[(type, path)]

    def name_to_path(self, type):
        """Get a map from name (in the setup directory) to path for
        all known files/archives, so we can build :option:`-file` and
        :option:`-archive` options to Hadoop (or fake them in a bootstrap
        script).

        :param type: either ``'archive'` or `'file'`
        """
        self._check_type(type)

        for path_type, path in self._typed_paths:
            if path_type == type:
                self.name(type, path)

        return dict((name, typed_path)
                    for name, typed_path
                    in self._name_to_typed_path.iteritems()
                    if typed_path[0] == type)

    def _check_name(self, name):
        if not (name is None or isinstance(name, basestring)):
            raise TypeError('name must be a string or None: %r' % (name,))

        if '/' in name:
            raise ValueError('names may not contain slashes: %r' % (name,))

    def _check_type(self, type):
        if not type in self._SUPPORTED_TYPES:
            raise TypeError('bad path type %r, must be one of %s' % (
                type, ', '.join(sorted(self._SUPPORTED_TYPES))))

    def _desc(self, type, path, name=''):
        """Display upload params in mrjob's DistributedCache-like syntax
        (appending a / to indicate archives).
        """
        # this will almost certainly get broken out into another function
        return '%s#%s%s' % (path, name, '/' if type == 'archive' else '')
