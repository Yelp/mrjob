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
