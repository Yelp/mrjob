# Copyright 2011 Yelp
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
import sys
import warnings

# PYTHONPATH takes precedence over any sys.path hacks
if os.environ.get('PYTHONPATH'):
    sys.path = os.environ['PYTHONPATH'].split(os.pathsep) + sys.path
    warnings.simplefilter('ignore')

import mrjob
from mrjob.job import MRJob


class MRJobWhereAreYou(MRJob):
    """Output what directory the mrjob library is in."""

    def mapper_final(self):
        yield (None, None)

    def reducer(self, key, values):
        yield (None, os.path.dirname(os.path.realpath(mrjob.__file__)))


if __name__ == '__main__':
    MRJobWhereAreYou.run()
