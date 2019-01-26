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
"""Test the Spark Harness."""
from io import BytesIO

from mrjob.examples.mr_word_freq_count import MRWordFreqCount
from mrjob.local import LocalMRJobRunner
from mrjob.spark import mrjob_spark_harness
from mrjob.spark.mr_spark_harness import MRSparkHarness
from mrjob.step import INPUT
from mrjob.step import OUTPUT
from mrjob.util import to_lines

from tests.sandbox import SandboxedTestCase
from tests.sandbox import SingleSparkContextTestCase


class SparkHarnessOutputComparisonTestCase(
        SandboxedTestCase, SingleSparkContextTestCase):

    def _spark_harness_path(self):
        path = mrjob_spark_harness.__file__
        if path.endswith('.pyc'):
            path = path[:-1]

        return path

    def _assert_output_matches(
            self, job_class, input_bytes=b'', input_paths=()):

        job_args = ['-r', 'inline'] + list(input_paths)

        reference_job = job_class(job_args)
        reference_job.sandbox(stdin=BytesIO(input_bytes))

        with reference_job.make_runner() as runner:
            runner.run()

            reference_output = sorted(to_lines(runner.cat_output()))

        job_class_path = '%s.%s' % (job_class.__module__, job_class.__name__)

        harness_job_args = job_args + ['--job-class', job_class_path]
        harness_job = MRSparkHarness(harness_job_args)
        harness_job.sandbox(stdin=BytesIO(input_bytes))

        with harness_job.make_runner() as runner:
            runner.run()

            harness_output = sorted(to_lines(runner.cat_output()))

        self.assertEqual(harness_output, reference_output)

    def test_mr_word_freq_count(self):
        input_bytes = b'one fish\ntwo fish\nred fish\nblue fish\n'

        self._assert_output_matches(MRWordFreqCount, input_bytes=input_bytes)





        # this code tests the harness script directly, but takes about 30s

        #harness_job_step = dict(
        #    type='spark_script',
        #    script=self._spark_harness_path(),
        #    args=[job_class_path, INPUT, OUTPUT],
        #)

        #harness_kwargs = dict(
        #    stdin=BytesIO(input_bytes),
        #    steps=[harness_job_step],
        #)

        #with LocalMRJobRunner(**harness_kwargs) as runner:
