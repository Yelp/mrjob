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

import itertools
import os.path

from mrjob.parse import is_uri


def name_uniquely(path, names_taken, proposed_name=None):
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

    dot_idx = proposed_name.find('.')
    if dot_idx == -1:
        prefix, suffix = proposed_name, ''
    else:
        prefix, suffix = proposed_name[:dot_idx], proposed_name[dot_idx:]

    for i in itertools.count(1):
        name = '%s-%d%s' % (prefix, i, suffix)
        if name not in names_taken:
            return name


class UploadDir(object):

    def __init__(self, prefix):
        self.prefix = prefix

        self._paths = set()
        self._path_to_name = {}
        self._names = set()

    def add(self, path, proposed_name=None):
        self._paths.add(path)

        if proposed_name:
            self._name(path, proposed_name)

    def _name(self, path, proposed_name=None):
        assert path in self._paths

        if path in self._path_to_name:
            return self._path_to_name[path]

        name = name_uniquely(path, names_taken=self._names,
                             proposed_name=proposed_name)

        self._path_to_name[path] = name
        self._names.add(name)

        return name

    def uri(self, path):
        if path in self._paths:
            return self.prefix + self._name(path)
        else:
            return path

    def path_to_uri(self):
        return dict((path, self.uri(path))
                    for path in self._paths)


class SetupDir(object):

    SUPPORTED_TYPES = ('archive', 'file')

    def __init__(self, upload_dir=None):
        self.upload_dir = upload_dir

        self._typed_paths = set()
        self._typed_path_to_default_name = {}
        self._name_to_typed_path = {}

    def add(self, type, path, name=None):
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
                self._typed_path_to_default_name.setdefault((type, path), name)

        # add to our upload dir (doing this last in case of an exception above)
        if self.upload_dir and self._should_upload(path):
            self.upload_dir.add(path, name=name)

    def name(self, type, path):
        self._check_type(type)

        if (type, path) not in self._typed_path:
            raise ValueError('%s was never added!' % self._desc(type, path))

        if (type, path) not in self._typed_path_to_default_name:
            name = name_uniquely(path, names_taken=self._name_to_typed_path)
            self._name_to_typed_path[name] = (type, path)
            self._typed_path_to_default_name[(type, path)] = name

        return self._typed_path_to_default_name[(type, path)]

    def name_to_path(self, type):
        self._check_type(type)

        for path_type, path in self._typed_paths:
            if path_type == type:
                self.name(type, path)

        return dict((name, typed_path)
                    for name, typed_path
                    in self._name_to_typed_path.iteritems()
                    if typed_path[0] == type)

    def _check_type(self, type):
        if not type in self.SUPPORTED_TYPES:
            raise TypeError('bad path type %r, must be one of %s' % (
                type, ', '.join(sorted(self.SUPPORTED_TYPES))))

    def _desc(self, type, path, name=''):
        return '%s#%s%s' % (path, name, '/' if type == 'archive' else '')

    def _should_upload(self, path):
        return is_uri(path)
