# Copyright 2009-2015 Yelp and Contributors
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
import os.path
import posixpath

from mrjob.parse import is_uri
from mrjob.parse import urlparse

log = logging.getLogger(__name__)


class Filesystem(object):
    """Some simple filesystem operations that are common across the local
    filesystem, S3, HDFS, and remote machines via SSH. Different runners
    provide functionality for different filesystems via their
    :py:attr:`~mrjob.runner.MRJobRunner.fs` attribute. The ``hadoop`` and
    ``emr`` runners provide support for multiple protocols using
    :py:class:`~mrjob.job.composite.CompositeFilesystem`.

    Protocol support:

    * :py:class:`mrjob.fs.hadoop.HadoopFilesystem`: ``hdfs://``, others
    * :py:class:`mrjob.fs.local.LocalFilesystem`: ``/``
    * :py:class:`mrjob.fs.s3.S3Filesystem`: ``s3://bucket/path``,
      ``s3n://bucket/path``
    * :py:class:`mrjob.fs.ssh.SSHFilesystem`: ``ssh://hostname/path``
    """

    def can_handle_path(self, path):
        """Can we handle this path at all?"""
        False

    def cat(self, path_glob):
        """cat all files matching **path_glob**, decompressing if necessary"""
        for filename in self.ls(path_glob):
            for line in self._cat_file(filename):
                yield line

    def du(self, path_glob):
        """Get the total size of files matching ``path_glob``

        Corresponds roughly to: ``hadoop fs -du path_glob``
        """
        raise NotImplementedError

    def ls(self, path_glob):
        """Recursively list all files in the given path.

        We don't return directories for compatibility with S3 (which
        has no concept of them)

        Corresponds roughly to: ``hadoop fs -ls -R path_glob``
        """
        raise NotImplementedError

    def _cat_file(self, path):
        raise NotImplementedError

    def exists(self, path_glob):
        """Does the given path/URI exist?

        Corresponds roughly to: ``hadoop fs -test -e path_glob``
        """
        raise NotImplementedError

    def join(self, path, *paths):
        """Join *paths* onto *path* (which may be a URI)"""
        all_paths = (path,) + paths

        # if there's a URI, we only care about it and what follows
        for i in range(len(all_paths), 0, -1):
            if is_uri(all_paths[i - 1]):
                scheme, netloc, uri_path = urlparse(all_paths[i - 1])[:3]
                return '%s://%s%s' % (
                    scheme, netloc, posixpath.join(
                        uri_path or '/', *all_paths[i:]))
        else:
            return os.path.join(*all_paths)

    def mkdir(self, path):
        """Create the given dir and its subdirs (if they don't already
        exist).

        Corresponds roughly to: ``hadoop fs -mkdir -p path``
        """
        raise NotImplementedError

    def path_exists(self, path_glob):
        """Alias for :py:meth:`exists`. Going away in v0.6.0

        .. deprecated: 0.5.0
        """
        log.warning('path_exists() has been renamed to exists(). This'
                    ' alias will be removed in v0.6.0')
        return self.exists(path_glob)

    def path_join(self, dirname, filename):
        """Alias for :py:meth:`join`. Going away in v0.6.0

        .. deprecated: 0.5.0
        """
        log.warning('path_join() has been renamed to join(). This'
                    ' alias will be removed in v0.6.0')
        return self.join(dirname, filename)

    def rm(self, path_glob):
        """Recursively delete the given file/directory, if it exists

        Corresponds roughly to: ``hadoop fs -rm -R path_glob``
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
