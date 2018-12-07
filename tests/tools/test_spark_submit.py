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
from tests.py2 import patch
from tests.sandbox import SandboxedTestCase


class SparkSubmitToolTestCase(SandboxedTestCase):

    def setUp(self):
        super(SparkSubmitToolTestCase, self).setUp()

        self.runner_class = None

        def _mock_runner_class(runner_alias):
            rc = _runner_class(runner_alias)

            self.runner_class = Mock()
            self.runner_class.alias = rc.alias
            self.runner_class.OPT_NAMES = rc.OPT_NAMES

            return self.runner_class

        self.runner_class = self.start(patch(
            'mrjob.tools.spark_submit._runner_class',
            side_effect=_mock_runner_class))

        self.runner_log = self.start(patch('mrjob.runner.log'))

    def get_runner_kwargs(self):
        return self.runner_class.call_args_list[-1][1]

    def test_basic_end_to_end(self):
        spark_submit_main(['foo.py', 'arg1', 'arg2'])

        self.assertEqual(self.runner_class.alias, 'hadoop')

        self.assertTrue(self.runner_class.called)
        self.assertTrue(self.runner_class.return_value.run.called)

        kwargs = self.get_runner_kwargs()

        self.assertEqual(kwargs['steps'], [
            dict(
                args=['arg1', 'arg2'],
                jobconf={},
                script='foo.py',
                spark_args=[],
                type='spark_script',
            )
        ])

    def test_jar_step(self):
        spark_submit_main(['dora.jar', 'arg1', 'arg2', 'arg3'])

        self.assertEqual(self.runner_class.alias, 'hadoop')

        self.assertTrue(self.runner_class.called)
        self.assertTrue(self.runner_class.return_value.run.called)

        kwargs = self.get_runner_kwargs()

        self.assertEqual(kwargs['steps'], [
            dict(
                args=['arg1', 'arg2', 'arg3'],
                jar='dora.jar',
                jobconf={},
                main_class=None,
                spark_args=[],
                type='spark_jar',
            )
        ])

    def test_jar_main_class(self):
        spark_submit_main(['dora.jar', '--class', 'Backpack',
                           'arg1', 'arg2', 'arg3'])

        kwargs = self.get_runner_kwargs()

        self.assertEqual(kwargs['steps'], [
            dict(
                args=['arg1', 'arg2', 'arg3'],
                jar='dora.jar',
                jobconf={},
                main_class='Backpack',
                spark_args=[],
                type='spark_jar',
            )
        ])
