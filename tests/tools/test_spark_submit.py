# Copyright 2018 Yelp
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
"""Test the spark-submit script."""
from mrjob.runner import _runner_class
from mrjob.tools.spark_submit import main as spark_submit_main

from tests.py2 import Mock
from tests.sandbox import SandboxedTestCase


def SparkSubmitToolTestCase(SandboxedTestCase):

    def setUp(self):
        super(SparkSubmitToolTestCase, self).setUp()

        self.mock_runner = Mock()

        def _mock_runner_class(runner_alias):
            rc = _runner_class(runner_alias)
            self.mock_runner.OPT_NAMES = set(rc.OPT_NAMES)
            return self.mock_runner

        self.start(patch('mrjob.tools.spark_submit._runner_class',
                         mock_runner_class))

        self.runner_log = self.start(patch('mrjob.runner.log'))

    def mock_runner_kwargs(self):
        return self.mock_runner.call_args_list[-1][1]
