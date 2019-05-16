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
"""Trivial extension to MRCountLinesByFile to ensure that Spark harness'
map_input_file emulation handles multiple mappers okay."""
from posixpath import basename

from mrjob.examples.mr_count_lines_by_file import MRCountLinesByFile
from mrjob.step import MRStep


class MRCountLinesByFilename(MRCountLinesByFile):

    def mapper2(self, path, count):
        yield basename(path), count

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(mapper=self.mapper2)
        ]

if __name__ == '__main__':
    MRCountLinesByFilename.run()
