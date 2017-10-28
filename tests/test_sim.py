# Copyright 2017 Yelp
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
from os.path import join

from mrjob.sim import _sort_lines_in_memory

from tests.mr_group import MRGroup
from tests.mr_sort_and_group import MRSortAndGroup
from tests.sandbox import SandboxedTestCase


class SortValuesTestCase(SandboxedTestCase):
    # inline runner doesn't have its own sorting logic
    RUNNER = 'inline'

    _INPUT = b'alligator\nactuary\nbowling\nartichoke\nballoon\nbaby\n'

    def test_no_sort_values(self):
        # don't sort values if not requested (#660)

        job = MRGroup(['-r', self.RUNNER])
        job.sandbox(stdin=BytesIO(self._INPUT))

        with job.make_runner() as runner:
            runner.run()
            output = list(job.parse_output(runner.cat_output()))

            self.assertEqual(
                sorted(output),
                [('a', ['alligator', 'actuary', 'artichoke']),
                 ('b', ['bowling', 'balloon', 'baby'])])

    def test_sort_values(self):
        job = MRSortAndGroup(['-r', self.RUNNER])
        job.sandbox(stdin=BytesIO(self._INPUT))

        with job.make_runner() as runner:
            runner.run()
            output = list(job.parse_output(runner.cat_output()))

            self.assertEqual(
                sorted(output),
                [('a', ['actuary', 'alligator', 'artichoke']),
                 ('b', ['baby', 'balloon', 'bowling'])])

    def test_sorting_is_case_sensitive(self):
        job = MRSortAndGroup(['-r', self.RUNNER])
        job.sandbox(stdin=BytesIO(b'Aaron\naardvark\nABC\n'))

        with job.make_runner() as runner:
            runner.run()
            output = list(job.parse_output(runner.cat_output()))

            self.assertEqual(
                sorted(output),
                [('A', ['ABC', 'Aaron']),
                 ('a', ['aardvark'])])
