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
"""Multi-step job that reads in the number of steps from a file.

This is basically a contrived way of taking a number to the nth power,
n times."""
from __future__ import with_statement

from mrjob.job import MRJob

class MRTowerOfPowers(MRJob):

    DEFAULT_INPUT_PROTOCOL = 'json_value'
    DEFAULT_OUTPUT_PROTOCOL = 'json_value'

    def configure_options(self):
        super(MRTowerOfPowers, self).configure_options()

        self.add_file_option('--n-file')

    def load_options(self, args):
        super(MRTowerOfPowers, self).load_options(args=args)

        with open(self.options.n_file) as f:
            self.n = int(f.read().strip())

    def mapper(self, _, value):
        yield None, value ** self.n

    def steps(self):
        return [self.mr(self.mapper)] * self.n

if __name__ == '__main__':
    MRTowerOfPowers.run()
