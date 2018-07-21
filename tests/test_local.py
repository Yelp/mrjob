# Copyright 2009-2012 Yelp and Contributors
# Copyright 2013 David Marin and Lyft
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
"""Tests for LocalMRJobRunner"""
import gzip
import os
import shutil
import signal
import stat
import sys
import tempfile
from io import BytesIO
from unittest import TestCase
from unittest import skipIf

import mrjob
from mrjob.local import LocalMRJobRunner
from mrjob.step import StepFailedException
from mrjob.util import bash_wrap
from mrjob.util import cmd_line
from mrjob.util import read_file

from tests.mr_cmd_job import MRCmdJob
from tests.mr_counting_job import MRCountingJob
from tests.mr_exit_42_job import MRExit42Job
from tests.mr_filter_job import MRFilterJob
from tests.mr_job_where_are_you import MRJobWhereAreYou
from tests.mr_two_step_job import MRTwoStepJob
from tests.mr_verbose_job import MRVerboseJob
from tests.mr_word_count import MRWordCount
from tests.py2 import call
from tests.py2 import patch
from tests.quiet import logger_disabled
from tests.quiet import no_handlers_for_logger
from tests.sandbox import EmptyMrjobConfTestCase
from tests.sandbox import SandboxedTestCase
from tests.sandbox import mrjob_conf_patcher
from tests.test_inline import InlineMRJobRunnerFSTestCase
from tests.test_inline import InlineMRJobRunnerJobConfTestCase
from tests.test_inline import InlineMRJobRunnerNoMapperTestCase


class LocalMRJobRunnerEndToEndTestCase(SandboxedTestCase):

    def test_end_to_end(self):
        # read from STDIN, a regular file, and a .gz
        stdin = BytesIO(b'foo\nbar\n')

        input_path = os.path.join(self.tmp_dir, 'input')
        with open(input_path, 'w') as input_file:
            input_file.write('bar\nqux\n')

        input_gz_path = os.path.join(self.tmp_dir, 'input.gz')
        input_gz_glob = os.path.join(self.tmp_dir, '*.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'wb')
        input_gz.write(b'foo\n')
        input_gz.close()

        mr_job = MRTwoStepJob(['-r', 'local', '-', input_path, input_gz_glob])
        mr_job.sandbox(stdin=stdin)

        local_tmp_dir = None
        results = []

        with mr_job.make_runner() as runner:
            assert isinstance(runner, LocalMRJobRunner)
            runner.run()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

            local_tmp_dir = runner._get_local_tmp_dir()
            assert os.path.exists(local_tmp_dir)
            self.assertEqual(runner.counters()[0]['count']['combiners'], 8)

        # make sure cleanup happens
        assert not os.path.exists(local_tmp_dir)

        self.assertEqual(sorted(results),
                         [(1, 'qux'), (2, 'bar'), (2, 'foo'), (5, None)])

    def test_end_to_end_multiple_tasks(self):
        # read from STDIN, a regular file, and a .gz
        stdin = BytesIO(b'foo\nbar\n')

        input_path = os.path.join(self.tmp_dir, 'input')
        with open(input_path, 'wb') as input_file:
            input_file.write(b'bar\nqux\n')

        input_gz_path = os.path.join(self.tmp_dir, 'input.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'wb')
        input_gz.write(b'foo\n')
        input_gz.close()

        mr_job = MRTwoStepJob(['-r', 'local',
                               '--jobconf=mapred.map.tasks=2',
                               '--jobconf=mapred.reduce.tasks=2',
                               '-', input_path, input_gz_path])
        mr_job.sandbox(stdin=stdin)

        local_tmp_dir = None
        results = []

        with mr_job.make_runner() as runner:
            assert isinstance(runner, LocalMRJobRunner)
            runner.run()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

            local_tmp_dir = runner._get_local_tmp_dir()
            assert os.path.exists(local_tmp_dir)

        # make sure cleanup happens
        assert not os.path.exists(local_tmp_dir)

        self.assertEqual(sorted(results),
                         [(1, 'qux'), (2, 'bar'), (2, 'foo'), (5, None)])

    def test_get_file_splits_test(self):
        # set up input paths
        input_path = os.path.join(self.tmp_dir, 'input')
        with open(input_path, 'w') as input_file:
            input_file.write('bar\nqux\nfoo\nbar\nqux\nfoo\n')

        input_path2 = os.path.join(self.tmp_dir, 'input2')
        with open(input_path2, 'wb') as input_file:
            input_file.write(b'foo\nbar\nbar\n')

        runner = LocalMRJobRunner(conf_paths=[])

        # split into 3 files
        file_splits = runner._get_file_splits([input_path, input_path2], 3)

        # make sure we get 3 files
        self.assertEqual(len(file_splits), 3)

        # make sure all the data is preserved
        content = []
        for file_name in file_splits:
            with open(file_name, 'rb') as f:
                content.extend(f.readlines())

        self.assertEqual(sorted(content),
                         [b'bar\n', b'bar\n', b'bar\n', b'bar\n', b'foo\n',
                          b'foo\n', b'foo\n', b'qux\n', b'qux\n'])

    def test_get_file_splits_sorted_test(self):
        # set up input paths
        input_path = os.path.join(self.tmp_dir, 'input')
        with open(input_path, 'wb') as input_file:
            input_file.write(
                b'1\tbar\n1\tbar\n1\tbar\n2\tfoo\n2\tfoo\n2\tfoo\n3\tqux\n'
                b'3\tqux\n3\tqux\n')

        runner = LocalMRJobRunner(conf_paths=[])

        file_splits = runner._get_file_splits([input_path], 3,
                                              keep_sorted=True)

        # make sure we get 3 files
        self.assertEqual(len(file_splits), 3)

        # make sure all the data is preserved in sorted order
        content = []
        for file_name in sorted(file_splits.keys()):
            with open(file_name, 'rb') as f:
                content.extend(f.readlines())

        self.assertEqual(content,
                         [b'1\tbar\n', b'1\tbar\n', b'1\tbar\n',
                          b'2\tfoo\n', b'2\tfoo\n', b'2\tfoo\n',
                          b'3\tqux\n', b'3\tqux\n', b'3\tqux\n'])

    def gz_test(self, dir_path_name):
        contents_gz = [b'bar\n', b'qux\n', b'foo\n', b'bar\n',
                       b'qux\n', b'foo\n']
        contents_normal = [b'foo\n', b'bar\n', b'bar\n']
        all_contents_sorted = sorted(contents_gz + contents_normal)

        input_gz_path = os.path.join(dir_path_name, 'input.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'wb')
        input_gz.write(b''.join(contents_gz))
        input_gz.close()
        input_path2 = os.path.join(dir_path_name, 'input2')
        with open(input_path2, 'wb') as input_file:
            input_file.write(b''.join(contents_normal))

        runner = LocalMRJobRunner(conf_paths=[])

        # split into 3 files
        file_splits = runner._get_file_splits([input_gz_path, input_path2], 3)

        # Make sure that input.gz occurs in a single split that starts at
        # its beginning and ends at its end
        for split_info in file_splits.values():
            if split_info['orig_name'] == input_gz_path:
                self.assertEqual(split_info['start'], 0)
                self.assertEqual(split_info['length'],
                                 os.stat(input_gz_path)[stat.ST_SIZE])

        # make sure we get 3 files
        self.assertEqual(len(file_splits), 3)

        # make sure all the data is preserved
        content = []
        for file_name in file_splits:
            lines = list(read_file(file_name))

            # make sure the input_gz split got its entire contents
            if file_name == input_gz_path:
                self.assertEqual(lines, contents_gz)

            content.extend(lines)

        self.assertEqual(sorted(content),
                         all_contents_sorted)

    def test_dont_split_gz(self):
        self.gz_test(self.tmp_dir)

    def test_relative_gz_path(self):
        current_directory = os.getcwd()

        def change_back_directory():
            os.chdir(current_directory)

        self.addCleanup(change_back_directory)
        os.chdir(self.tmp_dir)
        self.gz_test('')

    def test_multi_step_counters(self):
        stdin = BytesIO(b'foo\nbar\n')

        mr_job = MRCountingJob(['-r', 'local', '-'])
        mr_job.sandbox(stdin=stdin)

        with mr_job.make_runner() as runner:
            runner.run()

            self.assertEqual(runner.counters(),
                             [{'group': {'counter_name': 2}},
                              {'group': {'counter_name': 2}},
                              {'group': {'counter_name': 2}}])

    def test_gz_split_regression(self):
        gz_path_1 = os.path.join(self.tmp_dir, '1.gz')
        gz_path_2 = os.path.join(self.tmp_dir, '2.gz')
        path_3 = os.path.join(self.tmp_dir, '3')

        input_gz_1 = gzip.GzipFile(gz_path_1, 'wb')
        input_gz_1.write(b'x\n')
        input_gz_1.close()

        input_gz_2 = gzip.GzipFile(gz_path_2, 'wb')
        input_gz_2.write(b'y\n')
        input_gz_2.close()

        with open(path_3, 'wb') as f:
            f.write(b'z')

        mr_job = MRCountingJob(['--no-conf', '-r', 'local', gz_path_1,
                               gz_path_2, path_3])
        with mr_job.make_runner() as r:
            splits = r._get_file_splits([gz_path_1, gz_path_2, path_3], 1)
            self.assertEqual(
                len(set(s['task_num'] for s in splits.values())), 3)


class LocalMRJobRunnerNoSymlinksTestCase(LocalMRJobRunnerEndToEndTestCase):
    """Test systems without os.symlink (e.g. Windows). See Issue #46"""

    def setUp(self):
        super(LocalMRJobRunnerNoSymlinksTestCase, self).setUp()
        self.remove_os_symlink()

    def tearDown(self):
        self.restore_os_symlink()
        super(LocalMRJobRunnerNoSymlinksTestCase, self).tearDown()

    def remove_os_symlink(self):
        if hasattr(os, 'symlink'):
            self._real_os_symlink = os.symlink
            del os.symlink  # sorry, were you using that? :)

    def restore_os_symlink(self):
        if hasattr(self, '_real_os_symlink'):
            os.symlink = self._real_os_symlink


class TimeoutException(Exception):
    pass


class LargeAmountsOfStderrTestCase(TestCase):

    def setUp(self):
        self.set_alarm()

    def tearDown(self):
        self.restore_old_alarm_handler()

    def set_alarm(self):
        # if the test fails, it'll stall forever, so set an alarm
        def alarm_handler(*args, **kwargs):
            raise TimeoutException('Stalled on large amounts of stderr;'
                                   ' probably pipe buffer is full.')
        self._old_alarm_handler = signal.signal(signal.SIGALRM, alarm_handler)
        signal.alarm(30)

    def restore_old_alarm_handler(self):
        signal.alarm(0)
        signal.signal(signal.SIGALRM, self._old_alarm_handler)

    def test_large_amounts_of_stderr(self):
        mr_job = MRVerboseJob(['--no-conf', '-r', 'local', '-v'])
        mr_job.sandbox()

        try:
            with no_handlers_for_logger():
                mr_job.run_job()
        except TimeoutException:
            raise
        except SystemExit:
            # we expect the job to throw a StepFailedException,
            # which causes run_job to call sys.exit()

            # look for expected output from MRVerboseJob
            stderr = mr_job.stderr.getvalue()
            self.assertIn(
                b"Counters: 1\n\tFoo\n\t\tBar=10000", stderr)
            self.assertIn(b'Status: 0\n', stderr)
            self.assertIn(b'Status: 99\n', stderr)
            self.assertNotIn(b'Status: 100\n', stderr)
            self.assertIn(b'STDERR: Qux\n', stderr)
            # exception should appear in exception message
            self.assertIn(b'BOOM', stderr)
        else:
            raise AssertionError()


class ExitWithoutExceptionTestCase(TestCase):

    def test_exit_42_job(self):
        mr_job = MRExit42Job(['--no-conf', '--runner=local'])
        mr_job.sandbox()

        self.assertRaises(SystemExit, mr_job.run_job)

        self.assertIn(b'returned non-zero exit status 42',
                      mr_job.stderr.getvalue())


class PythonBinTestCase(EmptyMrjobConfTestCase):

    def test_echo_as_python_bin(self):
        # "echo" is a pretty poor substitute for Python, but it
        # should be available on most systems
        mr_job = MRTwoStepJob(
            ['--python-bin', 'echo', '--steps-python-bin', sys.executable,
             '--no-conf', '-r', 'local'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            assert isinstance(runner, LocalMRJobRunner)
            runner.run()
            output = b''.join(runner.stream_output())

        # the output should basically be the command we used to
        # run the last step, which in this case is a mapper
        self.assertIn(b'mr_two_step_job.py', output)
        self.assertIn(b'--step-num=1', output)
        self.assertIn(b'--mapper', output)

    @skipIf(hasattr(sys, 'pypy_version_info'),
            "-v option doesn't work with pypy")
    def test_python_dash_v_as_python_bin(self):
        python_cmd = cmd_line([sys.executable or 'python', '-v'])
        mr_job = MRTwoStepJob(['--python-bin', python_cmd, '--no-conf',
                               '-r', 'local', '-v'])
        mr_job.sandbox(stdin=[b'bar\n'])

        with no_handlers_for_logger():
            mr_job.run_job()

        # expect debugging messages in stderr.
        stderr = mr_job.stderr.getvalue()

        # stderr is huge, so don't use assertIn()
        self.assertTrue(b'import mrjob' in stderr or     # Python 2
                        b"import 'mrjob'" in stderr)  # Python 3
        self.assertTrue(b'#' in stderr)

        # should still get expected results
        self.assertEqual(sorted(mr_job.stdout.getvalue().splitlines()),
                         sorted([b'1\tnull', b'1\t"bar"']))


class StepsPythonBinTestCase(TestCase):

    def test_echo_as_steps_python_bin(self):
        mr_job = MRTwoStepJob(
            ['--steps', '--steps-python-bin', 'echo', '--no-conf',
             '-r', 'local'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            assert isinstance(runner, LocalMRJobRunner)
            # MRTwoStepJob populates _steps in the runner, so un-populate
            # it here so that the runner actually tries to get the steps
            # via subprocess
            runner._steps = None
            self.assertRaises(ValueError, runner._get_steps)

    def test_echo_as_steps_interpreter(self):
        import logging
        logging.basicConfig()
        mr_job = MRTwoStepJob(
            ['--steps', '--steps-interpreter', 'echo', '--no-conf', '-r',
             'local'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            assert isinstance(runner, LocalMRJobRunner)
            # MRTwoStepJob populates _steps in the runner, so un-populate
            # it here so that the runner actually tries to get the steps
            # via subprocess
            runner._steps = None
            self.assertRaises(ValueError, runner._get_steps)


class LocalBootstrapMrjobTestCase(TestCase):

    def setUp(self):
        self.make_tmp_dir()

    def tearDown(self):
        self.rm_tmp_dir()

    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_loading_bootstrapped_mrjob_library(self):
        # track the dir we're loading mrjob from rather than the full path
        # to deal with edge cases where we load from the .py file,
        # and the script loads from the .pyc compiled from that .py file.
        our_mrjob_dir = os.path.dirname(os.path.realpath(mrjob.__file__))

        with mrjob_conf_patcher():
            mr_job = MRJobWhereAreYou(['-r', 'local', '--bootstrap-mrjob'])
            mr_job.sandbox()

            with mr_job.make_runner() as runner:
                # sanity check
                self.assertEqual(runner._bootstrap_mrjob(), True)
                local_tmp_dir = os.path.realpath(runner._get_local_tmp_dir())

                runner.run()

                output = list(runner.stream_output())
                self.assertEqual(len(output), 1)

                # script should load mrjob from its working dir
                _, script_mrjob_dir = mr_job.parse_output_line(output[0])

                self.assertNotEqual(our_mrjob_dir, script_mrjob_dir)
                self.assertTrue(script_mrjob_dir.startswith(local_tmp_dir))

    def test_can_turn_off_bootstrap_mrjob(self):
        with mrjob_conf_patcher(
                {'runners': {'local': {'bootstrap_mrjob': False}}}):

            mr_job = MRJobWhereAreYou(['-r', 'local'])
            mr_job.sandbox()

            with mr_job.make_runner() as runner:
                # sanity check
                self.assertEqual(runner.get_opts()['bootstrap_mrjob'], False)
                local_tmp_dir = os.path.realpath(runner._get_local_tmp_dir())
                try:
                    with no_handlers_for_logger():
                        runner.run()
                except StepFailedException:
                    # this is what happens when mrjob isn't installed elsewhere
                    return

                # however, if mrjob is installed, we need to verify that
                # we're using the installed version and not a bootstrapped copy
                output = list(runner.stream_output())

                self.assertEqual(len(output), 1)

                # script should not load mrjob from local_tmp_dir
                _, script_mrjob_dir = mr_job.parse_output_line(output[0])
                self.assertFalse(script_mrjob_dir.startswith(local_tmp_dir))


class LocalMRJobRunnerJobConfTestCase(InlineMRJobRunnerJobConfTestCase):

    RUNNER = 'local'

    def _extra_expected_local_files(self, runner):
        cat_py = runner._cat_py()
        return [(cat_py, runner._working_dir_mgr.name('file', cat_py))]


class LocalMRJobRunnerNoMapperTestCase(InlineMRJobRunnerNoMapperTestCase):

    RUNNER = 'local'


class LocalMRJobRunnerFSTestCase(InlineMRJobRunnerFSTestCase):

    RUNNER_CLASS = LocalMRJobRunner


class CompatTestCase(EmptyMrjobConfTestCase):

    def test_environment_variables_version_agnostic(self):
        job = MRWordCount(['-r', 'local'])
        with job.make_runner() as runner:
            simulated_jobconf = runner._simulate_jobconf_for_step(
                0, 'mapper', 0, '/tmp/foo')
            self.assertIn(
                'mapred.cache.localArchives', simulated_jobconf)
            self.assertIn(
                'mapreduce.job.cache.local.archives', simulated_jobconf)

    def test_environment_variables_hadoop_1(self):
        job = MRWordCount(['-r', 'local', '--hadoop-version', '1.2.1'])
        with job.make_runner() as runner:
            simulated_jobconf = runner._simulate_jobconf_for_step(
                0, 'mapper', 0, '/tmp/foo')
            self.assertIn(
                'mapred.cache.localArchives', simulated_jobconf)
            self.assertNotIn(
                'mapreduce.job.cache.local.archives', simulated_jobconf)

    def test_environment_variables_hadoop_2(self):
        job = MRWordCount(['-r', 'local', '--hadoop-version', '2.7.2'])
        with job.make_runner() as runner:
            simulated_jobconf = runner._simulate_jobconf_for_step(
                0, 'mapper', 0, '/tmp/foo')
            self.assertIn(
                'mapreduce.job.cache.local.archives', simulated_jobconf)
            self.assertNotIn(
                'mapred.cache.localArchives', simulated_jobconf)


class CommandSubstepTestCase(SandboxedTestCase):

    def test_cat_mapper(self):
        data = b'x\ny\nz\n'
        job = MRCmdJob(['--mapper-cmd=cat', '--runner=local'])
        job.sandbox(stdin=BytesIO(data))
        with job.make_runner() as r:
            self.assertEqual(
                r._get_steps(),
                [{
                    'type': 'streaming',
                    'mapper': {
                        'type': 'command',
                        'command': 'cat'}}])

            r.run()
            lines = [line.strip() for line in list(r.stream_output())]
            self.assertEqual(sorted(lines), sorted(data.split()))

    def test_uniq_combiner(self):
        data = b'x\nx\nx\nx\nx\nx\n'
        job = MRCmdJob(['--combiner-cmd=uniq', '--runner=local'])
        job.sandbox(stdin=BytesIO(data))
        with job.make_runner() as r:
            self.assertEqual(
                r._get_steps(),
                [{
                    'type': 'streaming',
                    'mapper': {
                        'type': 'script',
                    },
                    'combiner': {
                        'type': 'command',
                        'command': 'uniq'}}])

            r.run()

            # there are 2 map tasks, each of which has 1 combiner, and all rows
            # are the same, so we should end up with just 2 values

            self.assertEqual(b''.join(r.stream_output()), b'x\nx\n')

    def test_cat_reducer(self):
        data = b'x\ny\nz\n'
        job = MRCmdJob(['--reducer-cmd', 'cat -e', '--runner=local'])
        job.sandbox(stdin=BytesIO(data))
        with job.make_runner() as r:
            self.assertEqual(
                r._get_steps(),
                [{
                    'type': 'streaming',
                    'mapper': {
                        'type': 'script',
                    },
                    'reducer': {
                        'type': 'command',
                        'command': 'cat -e'}}])

            r.run()

            lines = list(r.stream_output())
            self.assertEqual(sorted(lines), [b'x$\n', b'y$\n', b'z$\n'])

    def test_multiple(self):
        data = b'x\nx\nx\nx\nx\nx\n'
        mapper_cmd = 'cat -e'
        reducer_cmd = bash_wrap('wc -l | tr -Cd "[:digit:]"')
        job = MRCmdJob([
            '--runner', 'local',
            '--mapper-cmd', mapper_cmd,
            '--combiner-cmd', 'uniq',
            '--reducer-cmd', reducer_cmd])
        job.sandbox(stdin=BytesIO(data))
        with job.make_runner() as r:
            self.assertEqual(
                r._get_steps(),
                [{
                    'type': 'streaming',
                    'mapper': {'type': 'command', 'command': mapper_cmd},
                    'combiner': {'type': 'command', 'command': 'uniq'},
                    'reducer': {'type': 'command', 'command': reducer_cmd},
                }])

            r.run()

            self.assertEqual(list(r.stream_output()), [b'2'])

    def test_multiple_2(self):
        data = b'x\ny\nz\n'
        job = MRCmdJob(['--mapper-cmd=cat', '--reducer-cmd-2', 'wc -l',
                        '--runner=local', '--no-conf'])
        job.sandbox(stdin=BytesIO(data))
        with job.make_runner() as r:
            r.run()
            self.assertEqual(sum(int(l) for l in r.stream_output()), 3)


class FilterTestCase(SandboxedTestCase):

    def test_mapper_pre_filter(self):
        data = b'x\ny\nz\n'
        job = MRFilterJob(['--mapper-filter', 'cat -e', '--runner=local'])
        job.sandbox(stdin=BytesIO(data))
        with job.make_runner() as r:
            self.assertEqual(
                r._get_steps(),
                [{
                    'type': 'streaming',
                    'mapper': {
                        'type': 'script',
                        'pre_filter': 'cat -e'}}])

            r.run()

            lines = [line.strip() for line in list(r.stream_output())]
            self.assertEqual(sorted(lines), [b'x$', b'y$', b'z$'])

    def test_combiner_pre_filter(self):
        data = b'x\ny\nz\n'
        job = MRFilterJob(['--combiner-filter', 'cat -e', '--runner=local'])
        job.sandbox(stdin=BytesIO(data))
        with job.make_runner() as r:
            self.assertEqual(
                r._get_steps(),
                [{
                    'type': 'streaming',
                    'mapper': {
                        'type': 'script',
                    },
                    'combiner': {
                        'type': 'script',
                        'pre_filter': 'cat -e',
                    }}])

            r.run()
            lines = [line.strip() for line in list(r.stream_output())]
            self.assertEqual(sorted(lines), [b'x$', b'y$', b'z$'])

    def test_reducer_pre_filter(self):
        data = b'x\ny\nz\n'
        job = MRFilterJob(['--reducer-filter', 'cat -e', '--runner=local'])
        job.sandbox(stdin=BytesIO(data))
        with job.make_runner() as r:
            self.assertEqual(
                r._get_steps(),
                [{
                    'type': 'streaming',
                    'mapper': {
                        'type': 'script',
                    },
                    'reducer': {
                        'type': 'script',
                        'pre_filter': 'cat -e'}}])

            r.run()

            lines = [line.strip() for line in list(r.stream_output())]
            self.assertEqual(sorted(lines), [b'x$', b'y$', b'z$'])

    def test_pre_filter_failure(self):
        # regression test for #1524

        data = b'x\ny\nz\n'
        # grep will return exit code 1 because there are no matches
        job = MRFilterJob(['--mapper-filter', 'grep w', '--runner=local'])
        job.sandbox(stdin=BytesIO(data))
        with job.make_runner() as r:
            self.assertEqual(
                r._get_steps(),
                [{
                    'type': 'streaming',
                    'mapper': {
                        'type': 'script',
                        'pre_filter': 'grep w'}}])

            r.run()

            lines = [line.strip() for line in list(r.stream_output())]
            self.assertEqual(sorted(lines), [])

    def test_pre_filter_on_compressed_data(self):
        # regression test for #1061
        input_gz_path = self.makefile('data.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'wb')
        input_gz.write(b'x\ny\nz\n')
        input_gz.close()

        job = MRFilterJob([
            '--mapper-filter', 'cat -e', '--runner=local', input_gz_path])
        with job.make_runner() as r:
            self.assertEqual(
                r._get_steps(),
                [{
                    'type': 'streaming',
                    'mapper': {
                        'type': 'script',
                        'pre_filter': 'cat -e'}}])

            r.run()

            lines = [line.strip() for line in list(r.stream_output())]
            self.assertEqual(sorted(lines), [b'x$', b'y$', b'z$'])


class SetupLineEncodingTestCase(TestCase):

    def test_setup_wrapper_script_uses_local_line_endings(self):
        job = MRTwoStepJob(['-r', 'local', '--setup', 'true'])
        job.sandbox(stdin=BytesIO())

        # tests #1071. Unfortunately, we mostly run these tests on machines
        # that use unix line endings anyway. So monitor open() instead
        with patch(
                'mrjob.runner.open', create=True, side_effect=open) as m_open:
            with logger_disabled('mrjob.local'):
                with job.make_runner() as runner:
                    runner.run()

                    self.assertIn(
                        call(runner._setup_wrapper_script_path, 'w'),
                        m_open.mock_calls)
