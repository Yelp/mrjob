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
from unittest import skipIf

from mrjob.examples.mr_spark_most_used_word import MRSparkMostUsedWord

from tests.job import run_job
from tests.sandbox import SingleSparkContextTestCase

CAR_JOKE = b'''\
     A Car Joke:
     When is a car not a car?
     When it turns into a driveway!
     '''

class MRSparkMostUsedWordTestCase(SingleSparkContextTestCase):

    def test_stop_words_not_found_on_local_master(self):
        from py4j.protocol import Py4JJavaError

        self.assertRaises(
            Py4JJavaError, run_job, MRSparkMostUsedWord([]))
