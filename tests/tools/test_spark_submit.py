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
from __future__ import print_function

from mrjob.runner import _runner_class
from mrjob.tools.spark_submit import main as spark_submit_main

from tests.py2 import Mock
from tests.py2 import patch
from tests.sandbox import SandboxedTestCase


class MockSystemExit(Exception):
    pass


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

        # don't actually want to exit after printing help
        self.exit = self.start(patch('sys.exit', side_effect=MockSystemExit))

        # also probably better not to print
        self.print = self.start(patch('mrjob.tools.spark_submit.print'))

    def get_runner_kwargs(self):
        return self.runner_class.call_args_list[-1][1]

    def test_basic(self):
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

    def test_no_args_okay(self):
        spark_submit_main(['foo.py'])

        kwargs = self.get_runner_kwargs()

        self.assertEqual(kwargs['steps'], [
            dict(
                args=[],
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
        spark_submit_main(['--class', 'Backpack',
                           'dora.jar', 'arg1', 'arg2', 'arg3'])

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

    def test_allow_py3_extension(self):
        spark_submit_main(['foo.py3', 'arg1', 'arg2'])

        kwargs = self.get_runner_kwargs()

        self.assertEqual(kwargs['steps'], [
            dict(
                args=['arg1', 'arg2'],
                jobconf={},
                script='foo.py3',
                spark_args=[],
                type='spark_script',
            )
        ])

    def test_not_py_or_jar(self):
        self.assertRaises(ValueError, spark_submit_main,
                          ['whoo.sh', 'arg1'])

    def test_pass_through_to_step_spark_args(self):
        spark_submit_main(['--class', 'Backpack',
                           '--name', 'Backpack',
                           '--num-executors', '3',
                           '--conf', 'foo=BAR',
                           '--name', 'Mochila',
                           'dora.jar', 'arg1'])

        # --class becomes part of step
        # --conf is an alias for a mrjob opt, goes to runner
        # other args end up in spark-args as-is
        kwargs = self.get_runner_kwargs()

        self.assertEqual(kwargs['steps'], [
            dict(
                args=['arg1'],
                jar='dora.jar',
                jobconf={},
                main_class='Backpack',
                spark_args=[
                    '--name', 'Backpack',
                    '--num-executors', '3',
                    '--name', 'Mochila',
                ],
                type='spark_jar',
            )
        ])

        self.assertEqual(kwargs['jobconf'], dict(foo='BAR'))

    # test help

    def test_basic_help(self):
        with patch('mrjob.tools.spark_submit._print_basic_help') as pbh:
            self.assertRaises(MockSystemExit, spark_submit_main, ['-h'])

            self.exit.assert_called_once_with(0)
            pbh.assert_called_once_with(include_deprecated=False)

    def test_help_runner(self):
        with patch('mrjob.tools.spark_submit._print_help_for_runner') as phfr:
            self.assertRaises(MockSystemExit, spark_submit_main,
                              ['-h', '-r', 'emr'])

            self.exit.assert_called_once_with(0)
            phfr.assert_called_once_with(self.runner_class,
                                         include_deprecated=False)

    def test_no_script_prints_basic_help(self):
        with patch('mrjob.tools.spark_submit._print_basic_help') as pbh:
            self.assertRaises(MockSystemExit, spark_submit_main, [])

            self.exit.assert_called_once_with(0)
            pbh.assert_called_once_with(include_deprecated=False)

    def test_no_script_prints_basic_help_even_with_runner(self):
        # to get runner help, you have to do --help --runner ...
        with patch('mrjob.tools.spark_submit._print_basic_help') as pbh:
            self.assertRaises(MockSystemExit, spark_submit_main,
                              ['-r', 'emr'])

            self.exit.assert_called_once_with(0)
            pbh.assert_called_once_with(include_deprecated=False)


# add end-to-end test on EMR
