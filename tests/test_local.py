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
import stat
import sys
import tempfile
from io import BytesIO
from os.path import exists
from os.path import join
from subprocess import check_call
from unittest import TestCase
from unittest import skipIf

import mrjob
from mrjob.cat import decompress
from mrjob.launch import MRJobLauncher
from mrjob.local import LocalMRJobRunner
from mrjob.local import _sort_lines_in_memory
from mrjob.step import StepFailedException
from mrjob.util import cmd_line
from mrjob.util import to_lines

import tests.sr_wc
from tests.mr_cmd_job import MRCmdJob
from tests.mr_counting_job import MRCountingJob
from tests.mr_exit_42_job import MRExit42Job
from tests.mr_filter_job import MRFilterJob
from tests.mr_group import MRGroup
from tests.mr_job_where_are_you import MRJobWhereAreYou
from tests.mr_sort_and_group import MRSortAndGroup
from tests.mr_two_step_job import MRTwoStepJob
from tests.mr_word_count import MRWordCount
from tests.py2 import call
from tests.py2 import patch
from tests.quiet import logger_disabled
from tests.quiet import no_handlers_for_logger
from tests.sandbox import EmptyMrjobConfTestCase
from tests.sandbox import SandboxedTestCase
from tests.sandbox import mrjob_conf_patcher
from tests.test_sim import LocalFSTestCase
from tests.test_sim import SimRunnerJobConfTestCase
from tests.test_sim import SimRunnerNoMapperTestCase
from tests.test_sim import SortValuesTestCase


def _bash_wrap(cmd_str):
    """Escape single quotes in a shell command string and wrap it with ``bash
    -c '<string>'``.

    This low-tech replacement works because we control the surrounding string
    and single quotes are the only character in a single-quote string that
    needs escaping.

    .. deprecated:: 0.5.8
    """
    return "bash -c '%s'" % cmd_str.replace("'", "'\\''")


class LocalMRJobRunnerEndToEndTestCase(SandboxedTestCase):

    def test_end_to_end(self):
        # read from STDIN, a regular file, and a .gz
        stdin = BytesIO(b'foo\nbar\n')

        input_path = join(self.tmp_dir, 'input')
        with open(input_path, 'w') as input_file:
            input_file.write('bar\nqux\n')

        input_gz_path = join(self.tmp_dir, 'input.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'wb')
        input_gz.write(b'foo\n')
        input_gz.close()

        input_gz_glob = join(self.tmp_dir, '*.gz')

        mr_job = MRTwoStepJob(['-r', 'local', '-', input_path, input_gz_glob])
        mr_job.sandbox(stdin=stdin)

        local_tmp_dir = None
        results = []

        with mr_job.make_runner() as runner:
            assert isinstance(runner, LocalMRJobRunner)
            runner.run()

            results.extend(mr_job.parse_output(runner.cat_output()))

            local_tmp_dir = runner._get_local_tmp_dir()
            assert exists(local_tmp_dir)

            self.assertGreater(runner.counters()[0]['count']['combiners'], 0)

        # make sure cleanup happens
        assert not exists(local_tmp_dir)

        self.assertEqual(sorted(results),
                         [(1, 'qux'), (2, 'bar'), (2, 'foo'), (5, None)])


# TODO: these belong in tests of the sim runner
class TestsToPort:

    def test_end_to_end_multiple_tasks(self):
        # read from STDIN, a regular file, and a .gz
        stdin = BytesIO(b'foo\nbar\n')

        input_path = join(self.tmp_dir, 'input')
        with open(input_path, 'wb') as input_file:
            input_file.write(b'bar\nqux\n')

        input_gz_path = join(self.tmp_dir, 'input.gz')
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

            results.extend(mr_job.parse_output(runner.cat_output()))

            local_tmp_dir = runner._get_local_tmp_dir()
            assert exists(local_tmp_dir)

        # make sure cleanup happens
        assert not exists(local_tmp_dir)

        self.assertEqual(sorted(results),
                         [(1, 'qux'), (2, 'bar'), (2, 'foo'), (5, None)])

    def test_get_file_splits_test(self):
        # set up input paths
        input_path = join(self.tmp_dir, 'input')
        with open(input_path, 'w') as input_file:
            input_file.write('bar\nqux\nfoo\nbar\nqux\nfoo\n')

        input_path2 = join(self.tmp_dir, 'input2')
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
        input_path = join(self.tmp_dir, 'input')
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

        input_gz_path = join(dir_path_name, 'input.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'wb')
        input_gz.write(b''.join(contents_gz))
        input_gz.close()
        input_path2 = join(dir_path_name, 'input2')
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
            with open(file_name, 'rb') as f:
                lines = list(to_lines(decompress(f, file_name)))

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
        gz_path_1 = join(self.tmp_dir, '1.gz')
        gz_path_2 = join(self.tmp_dir, '2.gz')
        path_3 = join(self.tmp_dir, '3')

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
    """Test systems without os.symlink() (e.g. Python 2 on Windows).
    See Issue #46"""

    def setUp(self):
        super(LocalMRJobRunnerNoSymlinksTestCase, self).setUp()
        if hasattr(os, 'symlink'):
            self._real_os_symlink = os.symlink
            del os.symlink

    def tearDown(self):
        if hasattr(self, '_real_os_symlink'):
            os.symlink = self._real_os_symlink
        super(LocalMRJobRunnerNoSymlinksTestCase, self).tearDown()


class LocalMRJobRunnerBadOSSymlinkTestCase(LocalMRJobRunnerEndToEndTestCase):
    """Test systems with unfriendly os.symlink() (e.g. Python 3 on Windows).
    See Issue #1649."""
    def setUp(self):
        super(LocalMRJobRunnerEndToEndTestCase, self).setUp()
        self.start(patch('os.symlink', side_effect=OSError))


class TimeoutException(Exception):
    pass


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
            output = b''.join(runner.cat_output())

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
                               '-r', 'local'])
        mr_job.sandbox(stdin=[b'bar\n'])

        with no_handlers_for_logger():
            with mr_job.make_runner() as runner:
                runner.run()

                # expect python -v crud in stderr

                with open(runner._task_stderr_path('mapper', 0, 0)) as lines:
                    self.assertTrue(any(
                        'import mrjob' in line or  # Python 2
                        "import 'mrjob'" in line
                        for line in lines))

                with open(runner._task_stderr_path('mapper', 0, 0)) as lines:
                    self.assertTrue(any(
                        '#' in line for line in lines))

                # should still get expected results
                self.assertEqual(
                    sorted(to_lines(runner.cat_output())),
                    sorted([b'1\tnull\n', b'1\t"bar"\n']))


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

                output = list(to_lines(runner.cat_output()))
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
                self.assertEqual(runner._opts['bootstrap_mrjob'], False)
                local_tmp_dir = os.path.realpath(runner._get_local_tmp_dir())
                try:
                    with no_handlers_for_logger():
                        runner.run()
                except StepFailedException:
                    # this is what happens when mrjob isn't installed elsewhere
                    return

                # however, if mrjob is installed, we need to verify that
                # we're using the installed version and not a bootstrapped copy
                output = list(mr_job.parse_output(runner.cat_output()))

                self.assertEqual(len(output), 1)

                # script should not load mrjob from local_tmp_dir
                _, script_mrjob_dir = output[0]
                self.assertFalse(script_mrjob_dir.startswith(local_tmp_dir))


class LocalMRJobRunnerJobConfTestCase(SimRunnerJobConfTestCase):
    RUNNER = 'local'


class LocalMRJobRunnerNoMapperTestCase(SimRunnerNoMapperTestCase):
    RUNNER = 'local'


class LocalMRJobRunnerFSTestCase(LocalFSTestCase):
    RUNNER_CLASS = LocalMRJobRunner


class CompatTestCase(EmptyMrjobConfTestCase):

    def test_environment_variables_version_agnostic(self):
        job = MRWordCount(['-r', 'local'])
        with job.make_runner() as runner:
            simulated_jobconf = runner._simulate_jobconf_for_step(
                'mapper', 0, 0)
            self.assertIn(
                'mapred.cache.localArchives', simulated_jobconf)
            self.assertIn(
                'mapreduce.job.cache.local.archives', simulated_jobconf)

    def test_environment_variables_hadoop_1(self):
        job = MRWordCount(['-r', 'local', '--hadoop-version', '1.2.1'])
        with job.make_runner() as runner:
            simulated_jobconf = runner._simulate_jobconf_for_step(
                'mapper', 0, 0)
            self.assertIn(
                'mapred.cache.localArchives', simulated_jobconf)
            self.assertNotIn(
                'mapreduce.job.cache.local.archives', simulated_jobconf)

    def test_environment_variables_hadoop_2(self):
        job = MRWordCount(['-r', 'local', '--hadoop-version', '2.7.2'])
        with job.make_runner() as runner:
            simulated_jobconf = runner._simulate_jobconf_for_step(
                'mapper', 0, 0)
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
            lines = [line.strip() for line in to_lines(r.cat_output())]
            self.assertEqual(sorted(lines), sorted(data.split()))

    def test_uniq_combiner(self):
        # put data in a .gz to force a single map taxsk
        x_gz_path = join(self.tmp_dir, 'data.gz')
        with gzip.open(x_gz_path, 'wb') as x_gz:
            x_gz.write(b'x\nx\nx\nx\nx\nx\n')

        job = MRCmdJob(['--combiner-cmd=uniq', '--runner=local', x_gz_path])
        job.sandbox()

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

            # there is only one map task, thus only one combiner,
            # thus there should only be one value
            self.assertEqual(b''.join(r.cat_output()), b'x\n')

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

            lines = list(to_lines(r.cat_output()))
            self.assertEqual(sorted(lines), [b'x$\n', b'y$\n', b'z$\n'])

    def test_multiple(self):
        # put data in a .gz to force a single map task
        x_gz_path = join(self.tmp_dir, 'data.gz')
        with gzip.open(x_gz_path, 'wb') as x_gz:
            x_gz.write(b'x\nx\nx\nx\nx\nx\n')

        reducer_cmd = _bash_wrap('wc -l | tr -Cd "[:digit:]"')
        job = MRCmdJob([
            '--runner', 'local',
            '--mapper-cmd', 'cat -e',
            '--combiner-cmd', 'uniq',
            '--reducer-cmd', reducer_cmd,
            x_gz_path])
        job.sandbox()

        with job.make_runner() as r:
            self.assertEqual(
                r._get_steps(),
                [{
                    'type': 'streaming',
                    'mapper': {'type': 'command', 'command': 'cat -e'},
                    'combiner': {'type': 'command', 'command': 'uniq'},
                    'reducer': {'type': 'command', 'command': reducer_cmd},
                }])

            r.run()

            self.assertEqual(
                sum(int(v) for _, v in job.parse_output(r.cat_output())),
                1)

    def test_multiple_2(self):
        data = b'x\ny\nz\n'
        job = MRCmdJob(['--mapper-cmd=cat', '--reducer-cmd-2', 'wc -l',
                        '--runner=local', '--no-conf'])
        job.sandbox(stdin=BytesIO(data))
        with job.make_runner() as r:
            r.run()
            self.assertEqual(sum(int(l) for l in to_lines(r.cat_output())), 3)


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

            lines = [line.strip() for line in to_lines(r.cat_output())]
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
            lines = [line.strip() for line in to_lines(r.cat_output())]
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

            lines = [line.strip() for line in to_lines(r.cat_output())]
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

            lines = [line.strip() for line in to_lines(r.cat_output())]
            self.assertEqual(sorted(lines), [])

    def test_pre_filter_on_compressed_data(self):
        # regression test for #1061
        input_gz_path = self.makefile('data.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'wb')
        input_gz.write(b'x\ny\nz\n')
        input_gz.close()

        job = MRFilterJob([
            '--mapper-filter', 'cat -e', '--runner=local', input_gz_path])
        job.sandbox()

        with job.make_runner() as r:
            self.assertEqual(
                r._get_steps(),
                [{
                    'type': 'streaming',
                    'mapper': {
                        'type': 'script',
                        'pre_filter': 'cat -e'}}])

            r.run()

            lines = [line.strip() for line in to_lines(r.cat_output())]
            self.assertEqual(sorted(lines), [b'x$', b'y$', b'z$'])


class SetupLineEncodingTestCase(TestCase):

    def test_setup_wrapper_script_uses_local_line_endings(self):
        job = MRTwoStepJob(['-r', 'local', '--setup', 'true'])
        job.sandbox(stdin=BytesIO())

        # tests #1071. Unfortunately, we mostly run these tests on machines
        # that use unix line endings anyway. So monitor open() instead
        with patch(
                'mrjob.bin.open', create=True, side_effect=open) as m_open:
            with logger_disabled('mrjob.local'):
                with job.make_runner() as runner:
                    runner.run()

                    self.assertIn(
                        call(runner._setup_wrapper_script_path, 'w'),
                        m_open.mock_calls)


class LocalModeSortValuesTestCase(SortValuesTestCase):
    RUNNER = 'local'


class SortBinTestCase(SandboxedTestCase):

    def setUp(self):
        super(SortBinTestCase, self).setUp()

        # these patches are only okay if they don't raise an exception;
        # otherwise that hands an un-pickleable stacktrace to multiprocessing
        self.check_call = self.start(patch(
            'mrjob.local.check_call', wraps=check_call))

        self._sort_lines_in_memory = self.start(patch(
            'mrjob.local._sort_lines_in_memory',
            wraps=_sort_lines_in_memory))

    def test_default_sort_bin(self):
        job = MRGroup(['-r', 'local'])
        job.sandbox(stdin=BytesIO(
            b'apples\nbuffaloes\nbears'))

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(
                sorted(job.parse_output(runner.cat_output())),
                [('a', ['apples']), ('b', ['buffaloes', 'bears'])])

        self.assertTrue(self.check_call.called)
        self.assertFalse(self._sort_lines_in_memory.called)

        sort_args = self.check_call.call_args[0][0]
        self.assertEqual(sort_args[:6],
                         ['sort', '-t', '\t', '-k', '1,1', '-s'])

    def test_default_sort_bin_sort_values(self):
        job = MRSortAndGroup(['-r', 'local'])
        job.sandbox(stdin=BytesIO(
            b'apples\nbuffaloes\nbears'))

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(
                sorted(job.parse_output(runner.cat_output())),
                [('a', ['apples']), ('b', ['bears', 'buffaloes'])])

        self.assertTrue(self.check_call.called)
        self.assertFalse(self._sort_lines_in_memory.called)

        sort_args = self.check_call.call_args[0][0]

        self.assertEqual(sort_args[:1], ['sort'])
        self.assertNotEqual(sort_args[:6],
                            ['sort', '-t', '\t', '-k', '1,1', '-s'])

    def test_custom_sort_bin(self):
        job = MRGroup(['-r', 'local', '--sort-bin', 'sort -r'])
        job.sandbox(stdin=BytesIO(
            b'apples\nbabies\nbuffaloes\nbears\nbicycles'))

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(
                sorted(job.parse_output(runner.cat_output())),
                [('a', ['apples']),
                 ('b', ['buffaloes', 'bicycles', 'bears', 'babies'])])

        self.assertTrue(self.check_call.called)
        sort_args = self.check_call.call_args[0][0]

        self.assertEqual(sort_args[:2], ['sort', '-r'])

    def test_custom_sort_bin_overrides_sort_values(self):
        # this breaks SORT_VALUES; see #1699 for a fix
        job = MRSortAndGroup(['-r', 'local', '--sort-bin', 'sort -r'])
        job.sandbox(stdin=BytesIO(
            b'apples\nbabies\nbuffaloes\nbears\nbicycles'))

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(
                sorted(job.parse_output(runner.cat_output())),
                [('a', ['apples']),
                 ('b', ['buffaloes', 'bicycles', 'bears', 'babies'])])

        self.assertTrue(self.check_call.called)
        self.assertFalse(self._sort_lines_in_memory.called)

        sort_args = self.check_call.call_args[0][0]

        self.assertEqual(sort_args[:2], ['sort', '-r'])

    def test_sort_in_memory_on_windows(self):
        self.start(patch('platform.system', return_value='Windows'))

        job = MRGroup(['-r', 'local'])
        job.sandbox(stdin=BytesIO(
            b'apples\nbuffaloes\nbears'))

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(
                sorted(job.parse_output(runner.cat_output())),
                [('a', ['apples']), ('b', ['buffaloes', 'bears'])])

        self.assertFalse(self.check_call.called)
        # checking that _sort_lines_in_memory() was called gets messy.
        # we can assume it got called because check_call() didn't

    def test_bad_sort_bin(self):
        # patching check_call to raise an exception causes pickling issues in
        # multiprocessing, so just use the false command
        job = MRGroup(['-r', 'local', '--sort-bin', 'false'])
        job.sandbox(stdin=BytesIO(
            b'apples\nbuffaloes\nbears'))

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(
                sorted(job.parse_output(runner.cat_output())),
                [('a', ['apples']), ('b', ['buffaloes', 'bears'])])

        self.assertTrue(self.check_call.called)
        self.assertTrue(self._sort_lines_in_memory.called)

    def test_missing_sort_bin(self):
        # patching check_call to raise an exception causes pickling issues in
        # multiprocessing, so just use a binary that doesn't exist
        job = MRGroup(['-r', 'local', '--sort-bin', 'bort-xslkjfsasdf'])
        job.sandbox(stdin=BytesIO(
            b'apples\nbuffaloes\nbears'))

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(
                sorted(job.parse_output(runner.cat_output())),
                [('a', ['apples']), ('b', ['buffaloes', 'bears'])])

        self.assertTrue(self.check_call.called)
        self.assertTrue(self._sort_lines_in_memory.called)

    def _test_environment_variables(self, *args):
        job = MRGroup(['-r', 'local'])
        job.sandbox(stdin=BytesIO(
            b'apples\nbuffaloes\nbears'))

        with job.make_runner() as runner:
            runner.run()

            # don't bother with output; already tested this above

            self.assertTrue(self.check_call.called)
            env = self.check_call.call_args[1]['env']

            self.assertEqual(env['LC_ALL'], 'C')
            self.assertEqual(env['TMP'], runner._get_local_tmp_dir())
            self.assertEqual(env['TMPDIR'], runner._get_local_tmp_dir())

            self.assertNotIn('TEMP', env)  # this was for Windows sort

    def test_default_environment_variables(self):
        self._test_environment_variables()

    def test_custom_local_tmp_dir(self):
        tmp_dir = self.makedirs('ephemera')
        self._test_environment_variables('--local-tmp-dir', tmp_dir)


class InputFileArgsTestCase(SandboxedTestCase):
    # test for #567: ensure that local runner doesn't need to pass
    # file args to jobs

    def test_no_file_args_required(self):
        words1 = self.makefile('words1', b'kit and caboodle\n')
        words2 = self.makefile('words2', b'baubles\nbangles and beads\n')

        job = MRJobLauncher(
            args=['-r', 'local', tests.sr_wc.__file__, words1, words2])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            lines = list(to_lines(runner.cat_output()))
            self.assertEqual(len(lines), 1)
            self.assertEqual(int(lines[0]), 7)
