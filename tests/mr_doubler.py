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
from mrjob.protocol import JSONProtocol
from mrjob.step import MRStep


class MRDoubler(MRJob):
    """Double the value *n* times."""
    INPUT_PROTOCOL = JSONProtocol

    def configure_args(self):
        super(MRDoubler, self).configure_args()

        self.add_passthru_arg('-n', dest='n', type=int, default=1)

    def mapper(self, key, value):
        yield key, value * 2

    def steps(self):
        return [MRStep(mapper=self.mapper)] * self.options.n


if __name__ == '__main__':
    MRDoubler.run()
