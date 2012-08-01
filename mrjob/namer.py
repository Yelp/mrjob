# Copyright 2009-2012 Yelp and Contributors
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
from __future__ import with_statement

import os.path
from random import randint


class NamerBase(object):
    """Assigns unique names to paths."""

    def __init__(self):
        # map from path to 'archive' or 'file'. This also keeps track
        # of which files were added
        self._path_to_type = {}

        # map from path to assigned name
        self._path_to_name = {}
        # map from name to assign path (to track which names are taken)
        self._name_to_path = {}

    def add_file(self, path, name=None):
        """Add a file, optionally setting its name."""
        self._add(path, name, 'file')

    def add_archive(self, path, name=None):
        """Add an archive, optionally setting its name."""
        self._add(path, name, 'archive')

    def name(self, path):
        """Get the name assigned to path, picking one if one isn't
        already assigned. Names will never be ''.

        If path hasn't been added, return None.
        """
        if path not in self._name_to_type:
            return None

        return self._name(path)

    def archive_names(self):
        """Get a map from archive's path to its name."""
        return dict((path, self.name(path))
                    for path, type in self._path_to_name.iteritems()
                    if type == 'archive')

    def file_names(self):
        """Get a map from file's path to its name."""
        return dict((path, self.name(path))
                    for path, type in self._path_to_name.iteritems()
                    if type == 'file')

    def _add(self, path, name, type):
        """Helper for :py:meth:`add_archive` and :py:meth:`add_name`."""
        if path in self._path_to_type and self._path_to_type[path] != type:
                raise ValueError(
                    '%r is already assigned type %r' % (path, type))

        if name:
            self._name(path, name)

        self._path_to_type[path] = type

    def _name(self, path, name=None):
        """If *path* is not already named, assign a name to it

        If *name* is set, we attempt to assign that name to *path*, otherwise
        we attempt to assign *path*'s filename. If the name is taken, we
        prepend random hex to it (we prepend so it keeps the same file
        extension).

        Return the name assigned to *path*.
        """
        if path in self._path_to_name:
            current_name = self._path_to_name[path]
            if name is None or name == current_name:
                return current_name
            else:
                raise ValueError(
                    "Can't name %r %r; it's already named %r" % (
                        path, name, current_name))

        if name is None:
            name = os.path.basename(path) or '_'
        elif name == '':
            raise ValueError('name may not be empty')

        # put in subclass
        elif name in self._name_to_path and self._naming_is_strict():
            raise ValueError(
                "Can't name %r %r; already belongs to %r" % (
                    path, name, self._name_to_path[path]))

        # pick a unique name
        suffix = name

        while name in self._name_to_path:
            name = '%08x-%s' % (randint(0, 2 ** 32 - 1), suffix)

        self._path_to_name[path] = name
        self._name_to_path[name] = path

        return name


class DistributedCacheNamer(NamerBase):
    """Like NamerBase, except that we raise an error if we can't assign
    a path the name it was added with.
    """
    def _name(self, path, name=None):
        if (name is not None and
            name in self._path_to_name and
            self.name_to_path.get(path) != name):

            raise ValueError(
                "Can't name %r %r; already belongs to %r" % (
                    path, name, self._name_to_path[path]))

        return super(DistributedCacheNamer, self).name(path, name=name)


class UploadNamer(NamerBase):
    """Like NamerBase, except that it's initialized
    with a prefix which :py:meth:`name` prepends to paths' assigned names.
    """
    def __init__(self, prefix):
        super(UploadNamer, self).__init__()
        self.prefix = prefix

    def name(self, path, name=None):
        return self.prefix + super(UploadNamer, self).name(path, name=name)
