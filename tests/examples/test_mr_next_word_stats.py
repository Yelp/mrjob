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
from mrjob.examples.mr_next_word_stats import MRNextWordStats

from tests.job import run_job
from tests.sandbox import BasicTestCase


class MRNextWordStatsTestCase(BasicTestCase):

    def test_empty(self):
        self.assertEqual(run_job(MRNextWordStats([])), {})

    def test_frequency(self):
        input = (b'Oh where, oh where, has my little dog gone?\n'
                 b'Oh where, oh where can he be?')

        self.assertEqual(
            run_job(MRNextWordStats([]), input),
            {
                ('can', 'he'): [1, 1, 100.0],
                ('dog', 'gone'): [1, 1, 100.0],
                ('has', 'my'): [1, 1, 100.0],
                ('he', 'be'): [1, 1, 100.0],
                ('little', 'dog'): [1, 1, 100.0],
                ('my', 'little'): [1, 1, 100.0],
                ('oh', 'where'): [4, 4, 100.0],
                ('where', 'can'): [4, 1, 25.0],
                ('where', 'has'): [4, 1, 25.0],
                ('where', 'oh'): [4, 2, 50.0],
            }
        )
