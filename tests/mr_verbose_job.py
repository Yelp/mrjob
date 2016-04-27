# Copyright 2009-2012 Yelp and Contributors
# Copyright 2014 Ed Schofield
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
"""We use this to test jobs that emit a large amount of stderr."""
from __future__ import print_function
import sys

from mrjob.job import MRJob


class MRVerboseJob(MRJob):

    def mapper_final(self):
        # the UNIX pipe buffer can hold 65536 bytes, so this should
        # definitely exceed that
        for i in range(10000):
            self.increment_counter('Foo', 'Bar')

        for i in range(100):
            self.set_status(str(i))

        print('Qux', file=sys.stderr)

        # raise an exception so we can test stacktrace finding
        raise Exception('BOOM')


if __name__ == '__main__':
    MRVerboseJob.run()
