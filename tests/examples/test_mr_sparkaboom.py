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
from mrjob.examples.mr_sparkaboom import MRSparKaboom
from mrjob.util import to_lines

from tests.sandbox import SingleSparkContextTestCase


class MRSparkKaboomTestCase(SingleSparkContextTestCase):

    def test_empty(self):
        # no lines means no KABOOM
        job = MRSparKaboom([])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(
                sorted(to_lines(runner.cat_output())),
                [])

    # this test is disabled because it's identical to test_spark_job_failure()
    # in tests/test_inline.py
    def _test_one_line(self):
        from py4j.protocol import Py4JJavaError

        job = MRSparKaboom(['-r', 'inline'])
        job.sandbox(stdin=BytesIO(b'line\n'))

        with job.make_runner() as runner:
            self.assertRaises(Py4JJavaError, runner.run)
