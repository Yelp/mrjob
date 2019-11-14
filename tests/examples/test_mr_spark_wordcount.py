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

from mrjob.examples.mr_spark_wordcount import MRSparkWordcount
from mrjob.util import safeeval
from mrjob.util import to_lines

from tests.sandbox import SingleSparkContextTestCase


class MRSparkWordcountTestCase(SingleSparkContextTestCase):

    def test_empty(self):
        job = MRSparkWordcount([])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(
                sorted(to_lines(runner.cat_output())),
                [])

    def test_count_words(self):
        job = MRSparkWordcount([])
        job.sandbox(
            stdin=BytesIO(b'Mary had a little lamb\nlittle lamb\nlittle lamb'))

        with job.make_runner() as runner:
            runner.run()

            output = sorted(
                safeeval(line) for line in to_lines(runner.cat_output()))

            self.assertEqual(
                output,
                [
                    ('a', 1),
                    ('had', 1),
                    ('lamb', 3),
                    ('little', 3),
                    ('mary', 1),
                ]
            )
