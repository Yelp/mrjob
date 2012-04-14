# Copyright 2009-2012 Yelp and Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import with_statement

import glob
import hashlib
import logging
import os
import shutil

from mrjob.util import read_file


log = logging.getLogger('mrjob.fs.local')


class LocalFilesystem(object):

    ### file management utilties ###

    # Some simple filesystem operations that work for all runners.

    # We don't currently support ``mv()`` and ``cp()`` because S3 doesn't
    # really have directories, so the semantics get a little weird.

    def can_handle_path(self, path):
        return path.startswith('/')

    def du(self, path_glob):
        """Get the total size of files matching ``path_glob``

        Corresponds roughly to: ``hadoop fs -dus path_glob``
        """
        return sum(os.path.getsize(path) for path in self.ls(path_glob))

    def ls(self, path_glob):
        """Recursively list all files in the given path.

        We don't return directories for compatibility with S3 (which
        has no concept of them)

        Corresponds roughly to: ``hadoop fs -lsr path_glob``
        """
        for path in glob.glob(path_glob):
            if os.path.isdir(path):
                for dirname, _, filenames in os.walk(path):
                    for filename in filenames:
                        yield os.path.join(dirname, filename)
            else:
                yield path

    def _cat_file(self, filename):
        """cat a file, decompress if necessary."""
        for line in read_file(filename):
            yield line

    def cat(self, path):
        """cat output from a given path. This would automatically decompress
        .gz and .bz2 files.

        Corresponds roughly to: ``hadoop fs -cat path``
        """
        # This is the only Filesystem class that contains an actual
        # implementation of cat(). The rest are only used with MultiFilesystem,
        # which provides cat(). The rest only provide _cat_file().
        for filename in self.ls(path):
            for line in self._cat_file(filename):
                yield line

    def mkdir(self, path):
        """Create the given dir and its subdirs (if they don't already
        exist).

        Corresponds roughly to: ``hadoop fs -mkdir path``
        """
        if not os.path.isdir(path):
            os.makedirs(path)

    def path_exists(self, path_glob):
        """Does the given path exist?

        Corresponds roughly to: ``hadoop fs -test -e path_glob``
        """
        return bool(glob.glob(path_glob))

    def path_join(self, dirname, filename):
        """Join a directory name and filename."""
        return os.path.join(dirname, filename)

    def rm(self, path_glob):
        """Recursively delete the given file/directory, if it exists

        Corresponds roughly to: ``hadoop fs -rmr path_glob``
        """
        for path in glob.glob(path_glob):
            if os.path.isdir(path):
                log.debug('Recursively deleting %s' % path)
                shutil.rmtree(path)
            else:
                log.debug('Deleting %s' % path)
                os.remove(path)

    def touchz(self, path):
        """Make an empty file in the given location. Raises an error if
        a non-zero length file already exists in that location.

        Correponds to: ``hadoop fs -touchz path``
        """
        if os.path.isfile(path) and os.path.getsize(path) != 0:
            raise OSError('Non-empty file %r already exists!' % (path,))

        # zero out the file
        with open(path, 'w'):
            pass

    def _md5sum_file(self, fileobj, block_size=(512 ** 2)):  # 256K default
        md5 = hashlib.md5()
        while True:
            data = fileobj.read(block_size)
            if not data:
                break
            md5.update(data)
        return md5.hexdigest()

    def md5sum(self, path):
        """Generate the md5 sum of the file at ``path``"""
        with open(path, 'rb') as f:
            return self._md5sum_file(f)
