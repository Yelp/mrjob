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

from mrjob.examples.mr_spark_wordcount_script import MRSparkScriptWordcount
from mrjob.util import safeeval
from mrjob.util import to_lines

from tests.sandbox import SandboxedTestCase

try:
    import pyspark
except ImportError:
    pyspark = None

CAR_JOKE = b'''\
     A Car Joke:
     When is a car not a car?
     When it turns into a driveway!
     '''

@skipIf(pyspark is None, 'no pyspark module')
class MRSparkMostUsedWordTestCase(SandboxedTestCase):

    def test_empty(self):
        # this doesn't work on the inline runner because
        # Spark doesn't have a working dir to upload stop_words.txt
        # to. See below for what does and doesn't work in inline
        # runner
        job = MRSparkScriptWordcount(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(
                sorted(to_lines(runner.cat_output())),
                [])

    def test_count_words(self):
        job = MRSparkScriptWordcount(['-r', 'local'])
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
