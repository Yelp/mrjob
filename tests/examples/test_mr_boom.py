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
from mrjob.examples.mr_boom import MRBoom

from tests.job import run_job
from tests.sandbox import BasicTestCase


class MRBoomTestCase(BasicTestCase):

    def test_goes_boom_with_no_input(self):
        self.assertRaises(Exception, run_job, MRBoom())

    def test_says_boom(self):
        try:
            run_job(MRBoom([]), b'some input')
        except Exception as ex:
            self.assertIn('BOOM', str(ex))
        else:
            raise AssertionError('no exception raised')
