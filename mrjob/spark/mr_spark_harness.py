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
"""A wrapper for mrjob_spark_harness.py, so we can test the harness with
the inline runner."""
from mrjob.job import MRJob
from mrjob.spark.mrjob_spark_harness import main as harness_main


class MRSparkHarness(MRJob):

    def configure_args(self):
        super(MRSparkHarness, self).configure_args()

        self.add_passthru_arg(
            '--job-class', dest='job_class', type=str,
            help='dot-separated module and class name of MRJob')

    def spark(self, input_path, output_path):
        harness_main([self.options.job_class, input_path, output_path])


if __name__ == '__main__':
    MRSparkHarness.run()
