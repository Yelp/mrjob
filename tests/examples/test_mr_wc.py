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
from mrjob.examples.mr_wc import MRWordCountUtility

from tests.job import run_job
from tests.sandbox import BasicTestCase


class MRWordCountUtilityTestCase(BasicTestCase):

    def test_empty(self):
        self.assertEqual(
            run_job(MRWordCountUtility([])),
            dict(chars=0, lines=0, words=0))

    def test_two_lines(self):
        RAW_INPUT = b'dog dog dog\ncat cat\n'

        self.assertEqual(
            run_job(MRWordCountUtility([]), RAW_INPUT),
            dict(chars=20, words=5, lines=2))
