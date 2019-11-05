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

from mrjob.examples.mr_grep import MRGrepJob
from mrjob.util import to_lines
from mrjob.util import which

from tests.sandbox import SandboxedTestCase


class MRGrepJobTestCase(SandboxedTestCase):

    def test_requires_dash_e(self):
        self.assertRaises(ValueError, MRGrepJob, [])

    @skipIf(not which('grep'), 'grep command not in path')
    def test_filters(self):
        input_bytes = (b'mary had a little lamb\nlittle lamb\nlittle lamb\n'
                       b'mary had a little lamb\nits fleece was white as snow')

        job = MRGrepJob(['-r', 'local', '-e', 'mary'])
        job.sandbox(stdin=BytesIO(input_bytes))

        with job.make_runner() as runner:
            runner.run()

            output = list(to_lines(runner.cat_output()))

            self.assertEqual(output, [b'mary had a little lamb\n'] * 2)
