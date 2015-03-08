# Copyright 2009-2012 Yelp
# Copyright 2013 David Marin
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
import os

from mrjob.protocol import JSONValueProtocol
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRTowerOfPowers(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol

    def configure_options(self):
        super(MRTowerOfPowers, self).configure_options()

        self.add_file_option('--n-file')

    def load_options(self, args):
        super(MRTowerOfPowers, self).load_options(args=args)

        with open(self.options.n_file) as f:
            self.n = int(f.read().strip())

    def mapper(self, _, value):
        # mapper should always be reading from the "uploaded" file
        assert self.options.n_file != os.environ['LOCAL_N_FILE_PATH']

        yield None, value ** self.n

    def reducer(self, key, values):
        # reducer should always be reading from the "uploaded" file
        assert self.options.n_file != os.environ['LOCAL_N_FILE_PATH']

        # just pass through values as-is
        for value in values:
            yield key, value

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)] * self.n

    def show_steps(self):
        # when we invoke the job with --steps, it should
        # be reading from the original version of n_file
        assert self.options.n_file == os.environ['LOCAL_N_FILE_PATH']

        super(MRTowerOfPowers, self).show_steps()


if __name__ == '__main__':
    MRTowerOfPowers.run()
