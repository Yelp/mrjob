# Copyright 2018 Yelp
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
from tests.sandbox import BaseTestCase

from mrjob.examples.mr_most_used_word import MRMostUsedWord

from tests.job import run_job


class MRMostUsedWordTestCase(BaseTestCase):

    def test_empty(self):
        self.assertEqual(run_job(MRMostUsedWord()), {})

    def test_ignore_stop_words(self):
        RAW_INPUT = b"""
        A Car Joke:
        When is a car not a car?
        When it turns into a driveway!
        """

        EXPECTED_OUTPUT = {
            None: u'car',
        }

        self.assertEqual(run_job(MRMostUsedWord(), RAW_INPUT),
                         EXPECTED_OUTPUT)
