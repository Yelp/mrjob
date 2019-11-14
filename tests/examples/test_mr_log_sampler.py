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
from mrjob.examples.mr_log_sampler import MRLogSampler
from mrjob.util import to_lines

from tests.job import run_job
from tests.sandbox import SandboxedTestCase


class MRLogSamplerTestCase(SandboxedTestCase):

    def test_sample_size_is_required(self):
        self.assertRaises(ValueError, MRLogSampler, [])

    def test_empty(self):
        self.assertEqual(run_job(MRLogSampler(['--sample-size', '100'])), {})

    def test_sampling(self):
        num_file = self.makefile('numbers.txt')

        with open(num_file, 'w') as f:
            for i in range(100000):
                f.write('%f\n' % (i * 1.0 / 100000))

        job = MRLogSampler(['--sample-size', '10000', num_file])
        with job.make_runner() as runner:
            runner.run()

            total = sum(float(line) for line in to_lines(runner.cat_output()))
            self.assertGreater(total, 4000)
            self.assertLess(total, 6000)
