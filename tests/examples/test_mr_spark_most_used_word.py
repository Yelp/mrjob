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
from io import BytesIO
from unittest import skipIf

from mrjob.examples.mr_spark_most_used_word import MRSparkMostUsedWord
from mrjob.step import StepFailedException

from tests.sandbox import SandboxedTestCase
from tests.sandbox import SingleSparkContextTestCase

CAR_JOKE = b'''\
     A Car Joke:
     When is a car not a car?
     When it turns into a driveway!
     '''

class MRSparkMostUsedWordTestCase(SandboxedTestCase):

    def test_empty(self):
        # this doesn't work on the inline runner because
        # Spark doesn't have a working dir to upload stop_words.txt
        # to. See below for what does and doesn't work in inline
        # runner
        job = MRSparkMostUsedWord(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            # still doesn't work because you can't run max() on no records
            self.assertRaises(StepFailedException, runner.run)

    def test_car_joke(self):
        job = MRSparkMostUsedWord(['-r', 'local'])
        job.sandbox(stdin=BytesIO(CAR_JOKE))

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(b''.join(runner.cat_output()),
                             b'"car"\n')
