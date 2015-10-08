# Copyright 2009-2011 Yelp
# Copyright 2013 David Marin
# Copyright 2015 Yelp
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
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRNoMapper(MRJob):
    """Maps word count to a sorted list of words with that count."""
    def mapper(self, _, line):
        for word in line.split():
            yield word, 1

    def reducer(self, word, ones):
        yield sum(ones), word

    def reducer2(self, count, words):
        yield count, list(words)

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer),
                MRStep(reducer=self.reducer2)]


if __name__ == '__main__':
    MRNoMapper.run()
