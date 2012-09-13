# -*- encoding: utf-8 -*-
# Copyright 2012 Yelp
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

from __future__ import with_statement

import inspect
from optparse import OptionError
import os
from subprocess import Popen
from subprocess import PIPE
import sys

try:
    import unittest2 as unittest
    unittest  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import unittest

from mock import Mock
from mock import patch

from mrjob.conf import combine_envs
from mrjob.emr import EMRJobRunner
from mrjob.hadoop import HadoopJobRunner
from mrjob.launch import MRJobLauncher
from mrjob.local import LocalMRJobRunner
from tests.quiet import no_handlers_for_logger
from tests.sandbox import patch_fs_s3


def _mock_context_mgr(m, return_value):
    m.return_value.__enter__.return_value = return_value


class MRCustomJobLauncher(MRJobLauncher):

    def configure_options(self):
        super(MRCustomJobLauncher, self).configure_options()

        self.add_passthrough_option(
            '--foo-size', '-F', type='int', dest='foo_size', default=5)
        self.add_passthrough_option(
            '--bar-name', '-B', type='string', dest='bar_name', default=None)
        self.add_passthrough_option(
            '--enable-baz-mode', '-M', action='store_true', dest='baz_mode',
            default=False)
        self.add_passthrough_option(
            '--disable-quuxing', '-Q', action='store_false', dest='quuxing',
            default=True)
        self.add_passthrough_option(
            '--pill-type', '-T', type='choice', choices=(['red', 'blue']),
            default='blue')
        self.add_passthrough_option(
            '--planck-constant', '-C', type='float', default=6.626068e-34)
        self.add_passthrough_option(
            '--extra-special-arg', '-S', action='append',
            dest='extra_special_args', default=[])

        self.add_file_option('--foo-config', dest='foo_config', default=None)
        self.add_file_option('--accordian-file', dest='accordian_files',
                             action='append', default=[])


### Test cases ###


class MakeRunnerTestCase(unittest.TestCase):

    def test_local_runner(self):
        launcher = MRJobLauncher(args=['--no-conf', '-r', 'local', ''])
        with no_handlers_for_logger('mrjob.runner'):
            with launcher.make_runner() as runner:
                self.assertIsInstance(runner, LocalMRJobRunner)

    def test_hadoop_runner(self):
        # you can't instantiate a HadoopJobRunner without Hadoop installed
        launcher = MRJobLauncher(args=['--no-conf', '-r', 'hadoop', '',
                                       '--hadoop-streaming-jar', 'HUNNY'])
        with no_handlers_for_logger('mrjob.runner'):
            with patch.dict(os.environ, {'HADOOP_HOME': '100-Acre Wood'}):
                with launcher.make_runner() as runner:
                    self.assertIsInstance(runner, HadoopJobRunner)

    def test_emr_runner(self):
        launcher = MRJobLauncher(args=['--no-conf', '-r', 'emr', ''])
        with no_handlers_for_logger('mrjob'):
            with patch_fs_s3():
                with launcher.make_runner() as runner:
                    self.assertIsInstance(runner, EMRJobRunner)


class NoOutputTestCase(unittest.TestCase):

    def test_no_output(self):
        launcher = MRJobLauncher(args=['--no-conf', '--no-output', ''])
        launcher.sandbox()
        with patch.object(launcher, 'make_runner') as m_make_runner:
            runner = Mock()
            _mock_context_mgr(m_make_runner, runner)
            runner.stream_output.return_value = ['a line']
            launcher.run_job()
            self.assertEqual(launcher.stdout.getvalue(), '')
            self.assertEqual(launcher.stderr.getvalue(), '')


class CommandLineArgsTestCase(unittest.TestCase):

    def test_shouldnt_exit_when_invoked_as_object(self):
        self.assertRaises(ValueError, MRJobLauncher, args=['--quux', 'baz'])

    def test_should_exit_when_invoked_as_script(self):
        args = [sys.executable, inspect.getsourcefile(MRJobLauncher),
                '--quux', 'baz']
        # add . to PYTHONPATH (in case mrjob isn't actually installed)
        env = combine_envs(os.environ,
                           {'PYTHONPATH': os.path.abspath('.')})
        proc = Popen(args, stderr=PIPE, stdout=PIPE, env=env)
        proc.communicate()
        self.assertEqual(proc.returncode, 2)

    def test_custom_key_value_option_parsing(self):
        # simple example
        mr_job = MRJobLauncher(args=['--cmdenv', 'FOO=bar', ''])
        self.assertEqual(mr_job.options.cmdenv, {'FOO': 'bar'})

        # trickier example
        mr_job = MRJobLauncher(args= [
            '',
            '--cmdenv', 'FOO=bar',
            '--cmdenv', 'FOO=baz',
            '--cmdenv', 'BAZ=qux=quux'])
        self.assertEqual(mr_job.options.cmdenv,
                         {'FOO': 'baz', 'BAZ': 'qux=quux'})

        # must have KEY=VALUE
        self.assertRaises(ValueError, MRJobLauncher,
                          args=['--cmdenv', 'FOO', ''])

    def test_passthrough_options_defaults(self):
        mr_job = MRCustomJobLauncher(args=[''])

        self.assertEqual(mr_job.options.foo_size, 5)
        self.assertEqual(mr_job.options.bar_name, None)
        self.assertEqual(mr_job.options.baz_mode, False)
        self.assertEqual(mr_job.options.quuxing, True)
        self.assertEqual(mr_job.options.pill_type, 'blue')
        self.assertEqual(mr_job.options.planck_constant, 6.626068e-34)
        self.assertEqual(mr_job.options.extra_special_args, [])
        # should include all --protocol options
        # should include default value of --num-items
        # should use long option names (--protocol, not -p)
        # shouldn't include --limit because it's None
        # items should be in the order they were instantiated
        self.assertEqual(mr_job.generate_passthrough_arguments(), [])

    def test_explicit_passthrough_options(self):
        mr_job = MRCustomJobLauncher(args=[
            '',
            '-v',
            '--foo-size=9',
            '--bar-name', 'Alembic',
            '--enable-baz-mode', '--disable-quuxing',
            '--pill-type', 'red',
            '--planck-constant', '1',
            '--planck-constant', '42',
            '--extra-special-arg', 'you',
            '--extra-special-arg', 'me',
            '--strict-protocols',
            ])

        self.assertEqual(mr_job.options.foo_size, 9)
        self.assertEqual(mr_job.options.bar_name, 'Alembic')
        self.assertEqual(mr_job.options.baz_mode, True)
        self.assertEqual(mr_job.options.quuxing, False)
        self.assertEqual(mr_job.options.pill_type, 'red')
        self.assertEqual(mr_job.options.planck_constant, 42)
        self.assertEqual(mr_job.options.extra_special_args, ['you', 'me'])
        self.assertEqual(mr_job.options.strict_protocols, True)
        self.assertEqual(mr_job.generate_passthrough_arguments(),
                     [
                      '--bar-name', 'Alembic',
                      '--enable-baz-mode',
                      '--extra-special-arg', 'you',
                      '--extra-special-arg', 'me',
                      '--foo-size', '9',
                      '--pill-type', 'red',
                      '--planck-constant', '1',
                      '--planck-constant', '42',
                      '--disable-quuxing',
                      '--strict-protocols',
                      ])

    def test_explicit_passthrough_options_short(self):
        mr_job = MRCustomJobLauncher(args=[
            '',
            '-v',
            '-F9', '-BAlembic', '-MQ', '-T', 'red', '-C1', '-C42',
            '--extra-special-arg', 'you',
            '--extra-special-arg', 'me',
            '--strict-protocols',
            ])

        self.assertEqual(mr_job.options.foo_size, 9)
        self.assertEqual(mr_job.options.bar_name, 'Alembic')
        self.assertEqual(mr_job.options.baz_mode, True)
        self.assertEqual(mr_job.options.quuxing, False)
        self.assertEqual(mr_job.options.pill_type, 'red')
        self.assertEqual(mr_job.options.planck_constant, 42)
        self.assertEqual(mr_job.options.extra_special_args, ['you', 'me'])
        self.assertEqual(mr_job.options.strict_protocols, True)
        self.assertEqual(mr_job.generate_passthrough_arguments(),
                     [
                        '-B', 'Alembic',
                        '-M',
                         '--extra-special-arg', 'you',
                         '--extra-special-arg', 'me',
                         '-F', '9',
                         '-T', 'red',
                         '-C', '1',
                         '-C', '42',
                         '-Q',
                         '--strict-protocols'
                     ])

    def test_bad_custom_options(self):
        self.assertRaises(ValueError,
                          MRCustomJobLauncher,
                          args=['', '--planck-constant', 'c'])
        self.assertRaises(ValueError, MRCustomJobLauncher,
                          args=['', '--pill-type=green'])

    def test_bad_option_types(self):
        mr_job = MRJobLauncher(args=[''])
        self.assertRaises(
            OptionError, mr_job.add_passthrough_option,
            '--stop-words', dest='stop_words', type='set', default=None)
        self.assertRaises(
            OptionError, mr_job.add_passthrough_option,
            '--leave-a-msg', dest='leave_a_msg', action='callback',
            default=None)

    def test_incorrect_option_types(self):
        self.assertRaises(ValueError, MRJobLauncher, args=['--cmdenv', 'cats'])
        self.assertRaises(ValueError, MRJobLauncher,
                          args=['--ssh-bind-ports', 'athens'])

    def test_default_file_options(self):
        mr_job = MRCustomJobLauncher(args=[''])
        self.assertEqual(mr_job.options.foo_config, None)
        self.assertEqual(mr_job.options.accordian_files, [])
        self.assertEqual(mr_job.generate_file_upload_args(), [])

    def test_explicit_file_options(self):
        mr_job = MRCustomJobLauncher(args=[
            '',
            '--foo-config', '/tmp/.fooconf',
            '--foo-config', '/etc/fooconf',
            '--accordian-file', 'WeirdAl.mp3',
            '--accordian-file', '/home/dave/JohnLinnell.ogg'])
        self.assertEqual(mr_job.options.foo_config, '/etc/fooconf')
        self.assertEqual(mr_job.options.accordian_files, [
            'WeirdAl.mp3', '/home/dave/JohnLinnell.ogg'])
        self.assertEqual(mr_job.generate_file_upload_args(), [
            ('--foo-config', '/etc/fooconf'),
            ('--accordian-file', 'WeirdAl.mp3'),
            ('--accordian-file', '/home/dave/JohnLinnell.ogg')])

    def test_no_conf_overrides(self):
        mr_job = MRCustomJobLauncher(args=['', '-c', 'blah.conf', '--no-conf'])
        self.assertEqual(mr_job.options.conf_paths, [])

    def test_no_conf_overridden(self):
        mr_job = MRCustomJobLauncher(args=['', '--no-conf', '-c', 'blah.conf'])
        self.assertEqual(mr_job.options.conf_paths, ['blah.conf'])
