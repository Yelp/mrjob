# Copyright 2013 David Marin
# Copyright 2016 Yelp
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
import os
from os.path import join

from mrjob.job import MRJob


class MRUploadAttrsJob(MRJob):
    """Use the FILES, DIRS, and ARCHIVES attrs, and list contents of
    the current directory"""

    FILES = ['mr_upload_attrs_job/README.txt']

    ARCHIVES = ['mr_upload_attrs_job/empty.tar.gz']

    DIRS = ['mr_upload_attrs_job']

    def mapper(self, key, value):
        pass

    def mapper_final(self):
        for dirpath, _, filenames in os.walk('.', followlinks=True):
            yield dirpath, None
            for filename in filenames:
                yield join(dirpath, filename), None

    def reducer(self, key, values):
        # remove duplicates
        for value in values:
            yield (key, value)
            break


if __name__ == '__main__':
    MRUploadAttrsJob.run()
