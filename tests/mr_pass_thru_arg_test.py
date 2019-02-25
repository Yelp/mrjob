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
from mrjob.job import MRJob


class MRPassThruArgTest(MRJob):

    def configure_args(self):
        super(MRPassThruArgTest, self).configure_args()
        self.add_passthru_arg('--chars', action='store_true')
        self.add_passthru_arg('--ignore')

    def mapper(self, _, value):
        if self.options.ignore:
            value = value.replace(self.options.ignore, '')
        if self.options.chars:
            for c in value:
                yield c, 1
        else:
            for w in value.split(' '):
                yield w, 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRPassThruArgTest.run()
