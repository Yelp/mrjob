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

import os
from glob import glob
from os.path import join
from os.path import exists
from unittest import skipIf

try:
    import pyspark
except ImportError:
    pyspark = None

try:
    from mrjob.examples import spark_wordcount_script
except ImportError:
    spark_wordcount_script = None

from mrjob.runner import _runner_class
from mrjob.step import StepFailedException
from mrjob.tools.spark_submit import main as spark_submit_main
from mrjob.util import safeeval

from tests.mock_boto3.case import MockBoto3TestCase
from tests.py2 import MagicMock
from tests.py2 import Mock
from tests.py2 import patch
from tests.sandbox import SandboxedTestCase


class MockSystemExit(Exception):
    pass


class SparkSubmitToolTestCase(SandboxedTestCase):

    def setUp(self):
        super(SparkSubmitToolTestCase, self).setUp()

        self.runner_class = None

        self.runner = MagicMock()

        def _mock_runner_class(runner_alias):
            rc = _runner_class(runner_alias)

            self.runner_class = Mock()
            self.runner_class.return_value = self.runner
            self.runner_class.alias = rc.alias
            self.runner_class.OPT_NAMES = rc.OPT_NAMES

            return self.runner_class

        self.runner_class = self.start(patch(
            'mrjob.tools.spark_submit._runner_class',
            side_effect=_mock_runner_class))

        self.runner_log = self.start(patch('mrjob.runner.log'))

        # don't actually want to exit after printing help
        self.exit = self.start(patch('sys.exit', side_effect=MockSystemExit))

        # don't set up logging
        self.set_up_logging = self.start(
            patch('mrjob.job.MRJob.set_up_logging'))

        # save printout, rather than actually printing
        self.printout = ''

        def _mock_print_message(self_, message, file=None):
            self.printout += message

        self.start(patch('argparse.ArgumentParser._print_message',
                         _mock_print_message))

        def _mock_print(s=''):
            self.printout += s + '\n'

        # print() isn't considered part of the module in Python 3.4
        self.start(patch('mrjob.tools.spark_submit.print',
                         _mock_print, create=True))

    def get_runner_kwargs(self):
        return self.runner_class.call_args_list[-1][1]

    def test_basic(self):
        spark_submit_main(['foo.py', 'arg1', 'arg2'])

        self.assertEqual(self.runner_class.alias, 'hadoop')

        self.assertTrue(self.runner_class.called)
        self.assertTrue(self.runner.run.called)

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
        self.assertTrue(self.runner.run.called)

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

    def test_set_runner_class(self):
        spark_submit_main(['-r', 'emr', 'foo.py', 'arg1'])

        self.assertEqual(self.runner_class.alias, 'emr')

        self.assertTrue(self.runner_class.called)
        self.assertTrue(self.runner.run.called)

    def test_no_script_args_okay(self):
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

    def test_runner_kwargs(self):
        spark_submit_main(['--hadoop-bin', 'super-hadoop',
                           '--master', 'local',
                           '--py-files', 'bar.py,baz.py',
                           'foo.py', 'arg1'])

        kwargs = self.get_runner_kwargs()

        # regular old runner arg
        self.assertEqual(kwargs['hadoop_bin'], 'super-hadoop')

        # spark alias for mrjob opt
        self.assertEqual(kwargs['spark_master'], 'local')

        # arg with custom parser
        self.assertEqual(kwargs['py_files'], ['bar.py', 'baz.py'])

    def test_filters_runner_kwargs(self):
        # may want to change this behavior; see #1898
        spark_submit_main(['-r', 'emr', 'foo.py', 'arg1'])

        kwargs = self.get_runner_kwargs()

        self.assertIn('region', kwargs)
        self.assertNotIn('hadoop_bin', kwargs)

    def test_cleanup_called(self):
        spark_submit_main(['-r', 'emr', 'foo.py', 'arg1'])
        self.assertTrue(self.runner.cleanup.called)

    def test_cleanup_called_even_if_run_failed(self):
        self.runner.run.side_effect = StepFailedException

        self.assertRaises(StepFailedException, spark_submit_main,
                          ['-r', 'emr', 'foo.py', 'arg1'])
        self.assertTrue(self.runner.cleanup.called)

    def test_hard_coded_kwargs(self):
        spark_submit_main(['foo.py', 'arg1'])

        kwargs = self.get_runner_kwargs()

        self.assertEqual(kwargs['check_input_paths'], False)
        self.assertEqual(kwargs['input_paths'], [os.devnull])
        self.assertEqual(kwargs['output_dir'], None)

    def test_no_switches_for_hard_coded_kwargs(self):
        self.assertRaises(MockSystemExit, spark_submit_main,
                          ['--check-input-paths', 'foo.py', 'arg1'])
        self.assertRaises(MockSystemExit, spark_submit_main,
                          ['--output-dir', 'out', 'foo.py', 'arg1'])

    def _assert_prints_basic_help(self, args, include_deprecated=False):
        self.assertRaises(MockSystemExit, spark_submit_main, args)
        self.exit.assert_called_once_with(0)

        self.assertTrue(self.printout.startswith('usage:'))

        # has spark switches
        self.assertIn('--jars', self.printout)
        self.assertIn('--name', self.printout)
        self.assertIn('--py-files', self.printout)
        # but not runner switches or aliases
        self.assertNotIn('--python-bin', self.printout)
        self.assertNotIn('--libjars', self.printout)

        # --deprecated appears either way; either it appears as
        # a switch, or as a message at the end explaining how
        # to use it with --help
        if include_deprecated:
            self.assertIn('--deprecated', self.printout[:-50])
        else:
            self.assertIn('--deprecated', self.printout[-50:])

    def _assert_prints_runner_help(self, args, runner_alias,
                                   expect_switches=(),
                                   dont_expect_switches=()):
        self.assertRaises(MockSystemExit, spark_submit_main, args)
        self.exit.assert_called_once_with(0)

        # doesn't show usage, should include runner name in first line
        first_line = self.printout.split('\n')[0]
        self.assertIn('optional arguments', first_line)
        self.assertIn(runner_alias, first_line)

        # has runner switches
        self.assertIn('--python-bin', self.printout)
        self.assertIn('--libjars', self.printout)

        # but not basic spark switches
        self.assertNotIn('--name', self.printout)

        for switch in expect_switches:
            self.assertIn(switch, self.printout)

        for switch in dont_expect_switches:
            self.assertNotIn(switch, self.printout)

    def test_help_arg(self):
        self._assert_prints_basic_help(['-h'])

    def test_help_arg_with_deprecated(self):
        self._assert_prints_basic_help(['-h', '--deprecated'],
                                       include_deprecated=True)

    def test_help_arg_with_runner(self):
        self._assert_prints_runner_help(
            ['-h', '-r', 'emr'],
            'emr',
            expect_switches=['--region'],
            # --hadoop-bin is not for emr, --py-file is deprecated
            dont_expect_switches=['--hadoop-bin', '--py-file'])

    def test_help_arg_with_runner_and_deprecated(self):
        self._assert_prints_runner_help(
            ['-h', '-r', 'emr', '--deprecated'],
            'emr',
            expect_switches=['--region', '--py-file'],
            # we don't actually mention --deprecated in runner help
            dont_expect_switches=['--hadoop-bin', '--deprecated'])


class SparkSubmitToEMRTestCase(MockBoto3TestCase):

    def setUp(self):
        super(SparkSubmitToEMRTestCase, self).setUp()

        # don't set up logging
        self.set_up_logging = self.start(
            patch('mrjob.job.MRJob.set_up_logging'))

    def test_end_to_end(self):
        script_path = self.makefile('foo.py')

        spark_submit_main(
            ['-r', 'emr', script_path, 'arg1'])

        emr_client = self.client('emr')

        cluster_ids = [c['Id'] for c in
                       emr_client.list_clusters()['Clusters']]
        self.assertEqual(len(cluster_ids), 1)
        cluster_id = cluster_ids[0]

        steps = emr_client.list_steps(ClusterId=cluster_id)['Steps']
        self.assertEqual(len(steps), 1)
        step = steps[0]

        self.assertEqual(step['Status']['State'], 'COMPLETED')
        step_args = step['Config']['Args']

        self.assertEqual(step_args[0], 'spark-submit')
        self.assertEqual(step_args[-1], 'arg1')
        self.assertTrue(step_args[-2].endswith('/foo.py'))


@skipIf(pyspark is None, 'no pyspark module')
class SparkSubmitLocallyTestCase(SandboxedTestCase):
    # this test is a bit slow (~30s) as it actually runs on Spark
    # in local-cluster mode

    def setUp(self):
        super(SparkSubmitLocallyTestCase, self).setUp()

        # don't set up logging
        self.set_up_logging = self.start(
            patch('mrjob.job.MRJob.set_up_logging'))

    def test_end_to_end(self):
        script_path = spark_wordcount_script.__file__
        if script_path.endswith('.pyc'):
            script_path = script_path[:-1]

        input_path = self.makefile(
            'input', b'one fish\ntwo fish\nred fish\nblue fish\n')

        # don't create this path, let Spark do it
        output_path = join(self.tmp_dir, 'output')
        self.assertFalse(exists(output_path))

        spark_submit_main(
            ['-r', 'local', script_path, input_path, output_path])

        self.assertTrue(exists(output_path))

        word_counts = {}

        for path in glob(join(output_path, 'part-*')):
            with open(path) as f:
                for line in f:
                    word, count = safeeval(line)
                    word_counts[word] = count

        self.assertEqual(word_counts,
                         dict(blue=1, fish=4, one=1, red=1, two=1))
