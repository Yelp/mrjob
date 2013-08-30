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
import logging

from mrjob.fs.base import Filesystem


log = logging.getLogger(__name__)


class CompositeFilesystem(Filesystem):
    """Combine multiple filesystem objects to allow access to a variety of
    storage locations such as the local filesystem, S3, a remote machine via
    SSH, or HDFS.

    This class implements no filesystem functionality on its own other than
    the convenience method ``cat()``, which is a simple wrapper around ``ls()``
    and ``_cat_file()``.
    """

    def __init__(self, *filesystems):
        super(CompositeFilesystem, self).__init__()
        self.filesystems = filesystems

    def __getattr__(self, name):
        # Forward through to children for backward compatibility
        for fs in self.filesystems:
            if hasattr(fs, name):
                return getattr(fs, name)
        raise AttributeError(name)

    def _do_action(self, action, path, *args, **kwargs):
        """Call **action** on each filesystem object in turn. If one raises an
        :py:class:`IOError`, save the exception and try the rest. If none
        succeed, re-raise the first exception.
        """

        first_exception = None

        for fs in self.filesystems:
            if fs.can_handle_path(path):
                try:
                    return getattr(fs, action)(path, *args, **kwargs)
                except IOError, e:
                    if first_exception is None:
                        first_exception = e

        if first_exception is None:
            raise IOError('Could not %s: %s %s' % (action, path, args))
        else:
            raise first_exception

    def du(self, path_glob):
        return self._do_action('du', path_glob)

    def ls(self, path_glob):
        return self._do_action('ls', path_glob)

    def _cat_file(self, path):
        for line in self._do_action('_cat_file', path):
            yield line

    def mkdir(self, path):
        return self._do_action('mkdir', path)

    def path_exists(self, path_glob):
        return self._do_action('path_exists', path_glob)

    def path_join(self, dirname, filename):
        return self._do_action('path_join', dirname, filename)

    def rm(self, path_glob):
        return self._do_action('rm', path_glob)

    def touchz(self, path):
        return self._do_action('touchz', path)

    def md5sum(self, path_glob):
        return self._do_action('md5sum', path_glob)
