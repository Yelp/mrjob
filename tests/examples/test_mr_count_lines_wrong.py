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
from mrjob.examples.mr_count_lines_wrong import MRCountLinesWrong

from tests.sandbox import SandboxedTestCase


class MRCountLinesWrongTestCase(SandboxedTestCase):

    def test_empty(self):
        job = MRCountLinesWrong([])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            output = list(job.parse_output(runner.cat_output()))
            self.assertEqual(output, [(None, 0)] * len(output))

    def test_files(self):
        cat_file = self.makefile('cats.txt', b'cats are the best')
        dog_file = self.makefile('dogs.txt', b'woof woof woof\nwoof woof')
        empty_file = self.makefile('empty.txt')

        job = MRCountLinesWrong([cat_file, dog_file, empty_file])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            output = list(job.parse_output(runner.cat_output()))

            # no reducer to combine
            self.assertGreater(len(output), 1)
            self.assertEqual([k for k, v in output], [None] * len(output))
            self.assertEqual(sum(v for k, v in output), 3)
