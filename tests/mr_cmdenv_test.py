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

from mrjob.job import MRJob

class MRCmdenvTest(MRJob):
    """cmdenv test."""
    def mapper(self, key, value):
        # try adding something
        os.environ['BAR'] = 'foo'
        
        # get cmdenvs 
        yield('FOO', os.environ['FOO'])
        yield('SOMETHING', os.environ['SOMETHING'])

if __name__ == '__main__':
    MRCmdenvTest.run()
