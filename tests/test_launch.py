# -*- coding: utf-8 -*-
# Copyright 2012-2013 Yelp
# Copyright 2014 Yelp and Contributors
# Copyright 2015-2017 Yelp
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
import inspect
import logging
import os
import sys
from subprocess import Popen
from subprocess import PIPE
from unittest import TestCase

from mrjob.conf import combine_envs
from mrjob.job import MRJob
from mrjob.launch import MRJobLauncher
from mrjob.py2 import PY2
from mrjob.py2 import StringIO
from mrjob.step import StepFailedException

from tests.mr_no_runner import MRNoRunner
from tests.mr_runner import MRRunner
from tests.py2 import MagicMock
from tests.py2 import Mock
from tests.py2 import patch
from tests.quiet import no_handlers_for_logger
from tests.sandbox import SandboxedTestCase
from tests.sandbox import mrjob_pythonpath


def _mock_context_mgr(m, return_value):
    m.return_value.__enter__.return_value = return_value


class MRCustomJobLauncher(MRJobLauncher):

    def configure_args(self):
        super(MRCustomJobLauncher, self).configure_args()

        self.add_passthru_arg(
            '--foo-size', '-F', type=int, dest='foo_size', default=5)
        self.add_passthru_arg(
            '--bar-name', '-B', dest='bar_name', default=None)
        self.add_passthru_arg(
            '--enable-baz-mode', '-M', action='store_true', dest='baz_mode',
            default=False)
        self.add_passthru_arg(
            '--disable-quuxing', '-Q', action='store_false', dest='quuxing',
            default=True)
        self.add_passthru_arg(
            '--pill-type', '-T', choices=(['red', 'blue']),
            default='blue')
        self.add_passthru_arg(
            '--planck-constant', '-C', type=float, default=6.626068e-34)
        self.add_passthru_arg(
            '--extra-special-arg', '-S', action='append',
            dest='extra_special_args', default=[])

        self.pass_arg_through('--runner')

        self.add_file_arg('--foo-config', dest='foo_config', default=None)
        self.add_file_arg('--accordian-file', dest='accordian_files',
                          action='append', default=[])


# used to test old options() hooks
class MRDeprecatedCustomJobLauncher(MRJobLauncher):

    def configure_options(self):
        super(MRDeprecatedCustomJobLauncher, self).configure_options()

        self.add_passthrough_option(
            '--foo-size', '-F', type='int', dest='foo_size', default=5,
            help='default is %default')
        self.add_passthrough_option(
            '--pill-type', '-T', type='choice', choices=(['red', 'blue']),
            default='blue')

        self.pass_through_option('--runner')

        self.add_file_option('--accordian-file', dest='accordian_files',
                             action='append', default=[])

    def load_options(self, args):
        super(MRDeprecatedCustomJobLauncher, self).load_options(args)

        self._load_options_args = args


### Test cases ###


class RunJobTestCase(TestCase):

    def _make_launcher(self, *args):
        """Make a launcher, add a mock runner (``launcher.mock_runner``), and
        set it up so that ``launcher.make_runner().__enter__()`` returns
        ``launcher.mock_runner()``.
        """
        launcher = MRJobLauncher(args=['--no-conf', ''] + list(args))
        launcher.sandbox()

        launcher.mock_runner = Mock()
        launcher.mock_runner.cat_output.return_value = [b'a line\n']

        launcher.make_runner = MagicMock()  # include __enter__
        launcher.make_runner.return_value.__enter__.return_value = (
            launcher.mock_runner)

        return launcher

    def test_output(self):
        launcher = self._make_launcher()

        launcher.run_job()

        self.assertEqual(launcher.stdout.getvalue(), b'a line\n')
        self.assertEqual(launcher.stderr.getvalue(), b'')

    def test_no_output(self):
        launcher = self._make_launcher('--no-output')

        launcher.run_job()

        self.assertEqual(launcher.stdout.getvalue(), b'')
        self.assertEqual(launcher.stderr.getvalue(), b'')

    def test_exit_on_step_failure(self):
        launcher = self._make_launcher()
        launcher.mock_runner.run.side_effect = StepFailedException

        self.assertRaises(SystemExit, launcher.run_job)

        self.assertEqual(launcher.stdout.getvalue(), b'')
        self.assertIn(b'Step failed', launcher.stderr.getvalue())

    def test_pass_through_other_exceptions(self):
        launcher = self._make_launcher()
        launcher.mock_runner.run.side_effect = OSError

        self.assertRaises(OSError, launcher.run_job)

        self.assertEqual(launcher.stdout.getvalue(), b'')
        self.assertEqual(launcher.stderr.getvalue(), b'')


class CommandLineArgsTestCase(TestCase):

    def test_shouldnt_exit_when_invoked_as_object(self):
        self.assertRaises(ValueError, MRJobLauncher, args=['--quux', 'baz'])

    def test_should_exit_when_invoked_as_script(self):
        args = [sys.executable, inspect.getsourcefile(MRJobLauncher),
                '--quux', 'baz']

        # add . to PYTHONPATH (in case mrjob isn't actually installed)
        env = combine_envs(os.environ,
                           {'PYTHONPATH': mrjob_pythonpath()})
        proc = Popen(args, stderr=PIPE, stdout=PIPE, env=env)
        _, err = proc.communicate()
        self.assertEqual(proc.returncode, 2, err)

    def test_custom_key_value_option_parsing(self):
        # simple example
        mr_job = MRJobLauncher(args=['--cmdenv', 'FOO=bar', ''])
        self.assertEqual(mr_job.options.cmdenv, {'FOO': 'bar'})

        # trickier example
        mr_job = MRJobLauncher(args=[
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
        self.assertEqual(mr_job.options.runner, None)
        # should include all --protocol options
        # should include default value of --num-items
        # should use long option names (--protocol, not -p)
        # shouldn't include --limit because it's None
        # items should be in the order they were instantiated
        self.assertEqual(mr_job._non_option_kwargs()['extra_args'], [])

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
            '--runner', 'inline',
        ])

        self.assertEqual(mr_job.options.foo_size, 9)
        self.assertEqual(mr_job.options.bar_name, 'Alembic')
        self.assertEqual(mr_job.options.baz_mode, True)
        self.assertEqual(mr_job.options.quuxing, False)
        self.assertEqual(mr_job.options.pill_type, 'red')
        self.assertEqual(mr_job.options.planck_constant, 42)
        self.assertEqual(mr_job.options.extra_special_args, ['you', 'me'])
        self.assertEqual(
            mr_job._non_option_kwargs()['extra_args'],
            [
                '--foo-size', '9',
                '--bar-name', 'Alembic',
                '--enable-baz-mode',
                '--disable-quuxing',
                '--pill-type', 'red',
                '--planck-constant', '1',
                '--planck-constant', '42',
                '--extra-special-arg', 'you',
                '--extra-special-arg', 'me',
                '--runner', 'inline',
            ]
        )

    def test_explicit_passthrough_options_short(self):
        mr_job = MRCustomJobLauncher(args=[
            '',
            '-v',
            '-F9', '-BAlembic', '-MQ', '-T', 'red', '-C1', '-C42',
            '--extra-special-arg', 'you',
            '--extra-special-arg', 'me',
            '-r', 'inline',
        ])

        self.assertEqual(mr_job.options.foo_size, 9)
        self.assertEqual(mr_job.options.bar_name, 'Alembic')
        self.assertEqual(mr_job.options.baz_mode, True)
        self.assertEqual(mr_job.options.quuxing, False)
        self.assertEqual(mr_job.options.pill_type, 'red')
        self.assertEqual(mr_job.options.planck_constant, 42)
        self.assertEqual(mr_job.options.extra_special_args, ['you', 'me'])
        self.assertEqual(
            mr_job._non_option_kwargs()['extra_args'],
            # order is preserved, but args are separated from switches
            [
                '-F', '9',
                '-B', 'Alembic',
                '-M', '-Q',
                '-T', 'red',
                '-C', '1',
                '-C', '42',
                '--extra-special-arg', 'you',
                '--extra-special-arg', 'me',
                '-r', 'inline',
            ]
        )

    def test_bad_custom_options(self):
        self.assertRaises(ValueError,
                          MRCustomJobLauncher,
                          args=['', '--planck-constant', 'c'])
        self.assertRaises(ValueError, MRCustomJobLauncher,
                          args=['', '--pill-type=green'])

    def test_bad_option_types(self):
        mr_job = MRJobLauncher(args=[''])
        self.assertRaises(
            ValueError, mr_job.add_passthru_arg,
            '--stop-words', dest='stop_words', type='set', default=None)
        self.assertRaises(
            ValueError, mr_job.add_passthru_arg,
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
        self.assertEqual(mr_job._non_option_kwargs()['extra_args'], [])

    def test_explicit_file_options(self):
        mr_job = MRCustomJobLauncher(args=[
            '',
            '--foo-config', '/tmp/.fooconf#dot-fooconf',
            '--foo-config', '/etc/fooconf',
            '--accordian-file', 'WeirdAl.mp3',
            '--accordian-file', '/home/dave/JohnLinnell.ogg'])
        self.assertEqual(mr_job.options.foo_config, '/etc/fooconf')
        self.assertEqual(mr_job.options.accordian_files, [
            'WeirdAl.mp3', '/home/dave/JohnLinnell.ogg'])
        self.assertEqual(mr_job._non_option_kwargs()['extra_args'], [
            '--foo-config', dict(
                path='/tmp/.fooconf', name='dot-fooconf', type='file'),
            '--foo-config', dict(
                path='/etc/fooconf', name=None, type='file'),
            '--accordian-file', dict(
                path='WeirdAl.mp3', name=None, type='file'),
            '--accordian-file', dict(
                path='/home/dave/JohnLinnell.ogg', name=None, type='file')
        ])

    def test_no_conf_overrides(self):
        mr_job = MRCustomJobLauncher(args=['', '-c', 'blah.conf', '--no-conf'])
        self.assertEqual(mr_job.options.conf_paths, [])

    def test_no_conf_overridden(self):
        mr_job = MRCustomJobLauncher(args=['', '--no-conf', '-c', 'blah.conf'])
        self.assertEqual(mr_job.options.conf_paths, ['blah.conf'])

    def test_requires_script_path(self):
        self.assertRaises(ValueError, MRCustomJobLauncher, args=[])


class TestToolLogging(TestCase):
    """ Verify the behavior of logging configuration for CLI tools
    """
    def test_default_options(self):
        with no_handlers_for_logger('__main__'):
            with patch.object(sys, 'stderr', StringIO()) as stderr:
                MRJob.set_up_logging()
                log = logging.getLogger('__main__')
                log.info('INFO')
                log.debug('DEBUG')
                self.assertEqual(stderr.getvalue(), 'INFO\n')

    def test_verbose(self):
        with no_handlers_for_logger('__main__'):
            with patch.object(sys, 'stderr', StringIO()) as stderr:
                MRJob.set_up_logging(verbose=True)
                log = logging.getLogger('__main__')
                log.info('INFO')
                log.debug('DEBUG')
                self.assertEqual(stderr.getvalue(), 'INFO\nDEBUG\n')


class TestPassThroughRunner(TestCase):

    def get_value(self, job):
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            for _, value in job.parse_output(runner.cat_output()):
                return value

    def test_no_pass_through(self):
        self.assertEqual(self.get_value(MRNoRunner()), None)
        self.assertEqual(self.get_value(MRNoRunner(['-r', 'inline'])), None)
        self.assertEqual(self.get_value(MRNoRunner(['-r', 'local'])), None)

    def test_pass_through(self):
        self.assertEqual(self.get_value(MRRunner()), None)
        self.assertEqual(self.get_value(MRRunner(['-r', 'inline'])), 'inline')
        self.assertEqual(self.get_value(MRRunner(['-r', 'local'])), 'local')


class StdStreamTestCase(TestCase):

    def test_normal_python(self):
        launcher = MRJobLauncher(args=['/path/to/script'])

        if PY2:
            self.assertEqual(launcher.stdin, sys.stdin)
            self.assertEqual(launcher.stdout, sys.stdout)
            self.assertEqual(launcher.stderr, sys.stderr)
        else:
            self.assertEqual(launcher.stdin, sys.stdin.buffer)
            self.assertEqual(launcher.stdout, sys.stdout.buffer)
            self.assertEqual(launcher.stderr, sys.stderr.buffer)

    def test_python3_jupyter_notebook(self):
        # regression test for #1441

        # this actually works on any Python platform, since we use mocks
        mock_stdin = Mock()
        mock_stdin.buffer = Mock()

        mock_stdout = Mock()
        del mock_stdout.buffer

        mock_stderr = Mock()
        del mock_stderr.buffer

        with patch.multiple(sys, stdin=mock_stdin,
                            stdout=mock_stdout, stderr=mock_stderr):
            launcher = MRJobLauncher(args=['/path/to/script'])

        self.assertEqual(launcher.stdin, mock_stdin.buffer)
        self.assertEqual(launcher.stdout, mock_stdout)
        self.assertEqual(launcher.stderr, mock_stderr)


class DeprecatedOptionHooksTestCase(SandboxedTestCase):

    def setUp(self):
        super(DeprecatedOptionHooksTestCase, self).setUp()

        self.start(patch('mrjob.launch.log'))

    def test_load_options(self):
        mr_job = MRDeprecatedCustomJobLauncher(args=['', '-r', 'local'])

        self.assertEqual(mr_job._load_options_args, ['', '-r', 'local'])

    def test_add_passthrough_option(self):
        mr_job = MRDeprecatedCustomJobLauncher(
            args=['', '-F', '6', '-T', 'red'])

        self.assertEqual(mr_job.options.foo_size, 6)
        self.assertEqual(mr_job.options.pill_type, 'red')

        self.assertEqual(mr_job._non_option_kwargs()['extra_args'],
                         ['-F', '6', '-T', 'red'])

    def test_add_file_option(self):
        mr_job = MRDeprecatedCustomJobLauncher(
            args=['',
                  '--accordian-file', 'WeirdAl.mp3',
                  '--accordian-file', '/home/dave/JohnLinnell.ogg'])

        self.assertEqual(
            mr_job.options.accordian_files, [
                'WeirdAl.mp3', '/home/dave/JohnLinnell.ogg'])

        self.assertEqual(mr_job._non_option_kwargs()['extra_args'], [
            '--accordian-file', dict(
                path='WeirdAl.mp3', name=None, type='file'),
            '--accordian-file', dict(
                path='/home/dave/JohnLinnell.ogg', name=None, type='file'),
        ])

    def test_pass_through_option_method(self):
        mr_job = MRDeprecatedCustomJobLauncher(
            args=['', '-r', 'local'])

        self.assertEqual(mr_job.options.runner, 'local')

        self.assertEqual(mr_job._non_option_kwargs()['extra_args'],
                         ['-r', 'local'])

    def test_args_property(self):
        # argparse expects all positional args to be together
        mr_job = MRDeprecatedCustomJobLauncher(
            args=['-F', '6', '', 'input1.txt', 'input2.txt'])
        self.assertEqual(mr_job.args, ['input1.txt', 'input2.txt'])

    def test_help_compatibility(self):
        mr_job = MRDeprecatedCustomJobLauncher(args=[''])

        self.assertIn('default is 5', mr_job.arg_parser.format_help())
