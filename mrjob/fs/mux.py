# Copyright 2019 Yelp
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
import logging

from mrjob.fs.base import Filesystem


log = logging.getLogger(__name__)

# TODO:
#
# add back (deprecated) __getattr__ to pass through to sub-fs extensions
# unit-test MuxFilesystem
# make CompositeFilesystem a deprecated wrapper for MuxFilesystem


class MuxFilesystem(Filesystem):
    """Use one of several filesystems depending on the path/URI.

    This replaces :py:class:`~mrjob.fs.composite.CompositeFilesystem`. It
    has the ability to dynamically disable filesystems, and exposes
    sub-filesystems by name (e.g. ``fs.s3``).

    This only implements the core :py:class:`~mrjob.fs.base.Filesystem`
    interface; access extensions by calling the sub-filsystem directly
    (e.g. ``fs.s3.create_bucket(...)``).

    """
    def __init__(self):
        # names of sub-filesystems, in the order to call them. (The filesystems
        # themselves are stored in the attribute with that name.)
        self._fs_names = []

        # map from fs name to *disable_if* method (see :py:meth:`add`).
        self._disable_if = {}

        # set of names of filesystems that have been disabled
        self._disabled = set()

    def add_fs(self, name, fs, disable_if=None):
        """Add a filesystem.

        :param fs: a :py:class:~mrjob.fs.base.Filesystem to forward calls to.
        :param name string: Name of this filesystem. It will be directly
                            accessible through that the attribute with that
                            name. Recommended usage is the same name as
                            the module that contains the fs class.
        :param disable_if: A function called with a single argument, an
                           exception raised by ``fs``. If it returns true,
                           futher calls will not be forwarded to ``fs``.
        """
        if hasattr(self, name):
            raise ValueError('name %r is already taken' % name)

        self._fs_names.append(name)
        setattr(self, name, fs)

        if disable_if:
            self._disable_if[name] = disable_if

    def can_handle_path(self, path):
        """We can handle any path handled by any (non-disabled) filesystem."""
        for fs_name in self._fs_names:
            if fs_name in self._disabled:
                continue

            fs = getattr(self, fs_name)
            if fs.can_handle_path(path):
                return True

        return False

    def _do_action(self, action, path, *args, **kwargs):
        """Call method named **action** on the first (non-disabled) filesystem
        that says it can handle *path*. If it raises an exception, either
        disable the filesystem and continue, or re-raise the exception."""
        for fs_name in self._fs_names:
            if fs_name in self._disabled:
                continue

            fs = getattr(self, fs_name)
            if not fs.can_handle_path(path):
                continue

            try:
                return getattr(fs, action)(path, *args, **kwargs)
            except Exception as ex:
                if (fs_name in self._disable_if and
                        self._disable_if[fs_name](ex)):
                    log.debug('disabling %s fs: %r' % (fs_name, ex))

                    self._disabled.add(fs_name)
                else:
                    raise

        raise IOError("Can't handle path: %s" % path)

    # explicitly implement Filesystem interface. this will come in handy

    def cat(self, path_glob):
        return self._do_action('cat', path_glob)

    def _cat_file(self, path):
        # mrjob/runner.py accesses this directly for efficiency
        return self._do_action('_cat_file', path)

    def du(self, path_glob):
        return self._do_action('du', path_glob)

    def ls(self, path_glob):
        return self._do_action('ls', path_glob)

    def exists(self, path_glob):
        return self._do_action('exists', path_glob)

    def mkdir(self, path):
        return self._do_action('mkdir', path)

    def join(self, path, *paths):
        return self._do_action('join', path, *paths)

    def rm(self, path_glob):
        return self._do_action('rm', path_glob)

    def touchz(self, path):
        return self._do_action('touchz', path)

    def md5sum(self, path_glob):
        return self._do_action('md5sum', path_glob)
