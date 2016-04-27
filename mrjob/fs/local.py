# Copyright 2009-2012 Yelp and Contributors
# Copyright 2015 Yelp
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
import glob
import hashlib
import logging
import os
import shutil

from mrjob.fs.base import Filesystem
from mrjob.parse import is_uri
from mrjob.util import read_file

log = logging.getLogger(__name__)


class LocalFilesystem(Filesystem):
    """Filesystem for local files. Typically you will get one of these via
    ``MRJobRunner().fs``.
    """
    def can_handle_path(self, path):
        return not is_uri(path)

    def du(self, path_glob):
        return sum(os.path.getsize(path) for path in self.ls(path_glob))

    def ls(self, path_glob):
        for path in glob.glob(path_glob):
            if os.path.isdir(path):
                for dirname, _, filenames in os.walk(path, followlinks=True):
                    for filename in filenames:
                        yield os.path.join(dirname, filename)
            else:
                yield path

    def _cat_file(self, filename):
        return read_file(filename)

    def mkdir(self, path):
        if not os.path.isdir(path):
            os.makedirs(path)

    def exists(self, path_glob):
        return bool(glob.glob(path_glob))

    def rm(self, path_glob):
        for path in glob.glob(path_glob):
            if os.path.isdir(path):
                log.debug('Recursively deleting %s' % path)
                shutil.rmtree(path)
            else:
                log.debug('Deleting %s' % path)
                os.remove(path)

    def touchz(self, path):
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
        with open(path, 'rb') as f:
            return self._md5sum_file(f)
