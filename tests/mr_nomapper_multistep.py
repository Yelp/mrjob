# Copyright 2009-2010 Yelp
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


class MRNoMapper(MRJob):

    def mapper(self, key, value):
        yield key, value
        yield value, key

    def reducer(self, key, values):
        yield key, len(list(values))

    def reducer2(self, key, value):
        yield key, value

    def steps(self):
        return [self.mr(self.mapper, self.reducer),
                self.mr(reducer=self.reducer2)]


if __name__ == '__main__':
    MRNoMapper.run()
