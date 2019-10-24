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
"""Tests for JobConf Environment Variables
"""
import re

from mrjob.job import MRJob

WORD_RE = re.compile(r"[\w']+")


class MRStdinOnly(MRJob):
    """Word frequency count that job that only reads from stdin
    """
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield (word, 1)

    def reducer(self, name, counts):
        yield (name, sum(counts))

    def _read_input(self):
        for line in self.stdin:
            yield line


if __name__ == '__main__':
    MRStdinOnly.run()
