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
from unittest import TestCase

from mrjob.examples.mr_word_freq_count import MRWordFreqCount

from tests.job import run_job


class MRWordFreqCountTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(run_job(MRWordFreqCount()), {})

    def test_the_wheels_on_the_bus(self):
        RAW_INPUT = b"""
        The wheels on the bus go round and round,
        round and round, round and round
        The wheels on the bus go round and round,
        all through the town.
        """

        EXPECTED_OUTPUT = {
            u'all': 1,
            u'and': 4,
            u'bus': 2,
            u'go': 2,
            u'on': 2,
            u'round': 8,
            u'the': 5,
            u'through': 1,
            u'town': 1,
            u'wheels': 2,
        }

        self.assertEqual(run_job(MRWordFreqCount(), RAW_INPUT),
                         EXPECTED_OUTPUT)
