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


log = logging.getLogger('mrjob.fs')


class BaseFilesystem(object):
    """Basic wrapper methods for filesystem objects."""

    def cat(self, path_glob):
        """cat all files matching **path_glob**, decompressing if necessary"""
        for filename in self.ls(path_glob):
            for line in self._cat_file(filename):
                yield line

    def du(self, path_glob):
        """Get the total size of files matching ``path_glob``

        Corresponds roughly to: ``hadoop fs -dus path_glob``
        """
        raise NotImplementedError

    def ls(self, path_glob):
        """Recursively list all files in the given path.

        We don't return directories for compatibility with S3 (which
        has no concept of them)

        Corresponds roughly to: ``hadoop fs -lsr path_glob``
        """
        raise NotImplementedError

    def _cat_file(self, path):
        raise NotImplementedError

    def mkdir(self, path):
        """Create the given dir and its subdirs (if they don't already
        exist).

        Corresponds roughly to: ``hadoop fs -mkdir path``
        """
        raise NotImplementedError

    def path_exists(self, path_glob):
        """Does the given path exist?

        Corresponds roughly to: ``hadoop fs -test -e path_glob``
        """
        raise NotImplementedError

    def path_join(self, dirname, filename):
        raise NotImplementedError

    def rm(self, path_glob):
        """Recursively delete the given file/directory, if it exists

        Corresponds roughly to: ``hadoop fs -rmr path_glob``
        """
        raise NotImplementedError

    def touchz(self, path):
        """Make an empty file in the given location. Raises an error if
        a non-zero length file already exists in that location.

        Correponds to: ``hadoop fs -touchz path``
        """
        raise NotImplementedError

    def md5sum(self, path_glob):
        """Generate the md5 sum of the file at ``path``"""
        raise NotImplementedError
