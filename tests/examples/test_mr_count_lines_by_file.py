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
from mrjob.examples.mr_count_lines_by_file import MRCountLinesByFile

from tests.job import run_job
from tests.sandbox import SandboxedTestCase


class MRCountLinesByFileTestCase(SandboxedTestCase):

    def test_empty(self):
        self.assertEqual(run_job(MRCountLinesByFile([])), {})

    def test_files(self):
        cat_file = self.makefile('cats.txt', b'cats are the best')
        dog_file = self.makefile('dogs.txt', b'woof woof woof\nwoof woof')
        empty_file = self.makefile('empty.txt')

        self.assertEqual(
            run_job(MRCountLinesByFile([cat_file, dog_file, empty_file])),
            {
                'file://' + cat_file: 1,
                'file://' + dog_file: 2,
            }
        )
