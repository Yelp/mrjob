# Copyright 2009-2012 Yelp and Contributors
# Copyright 2013 David Marin and Lyft
# Copyright 2015-2017 Yelp
# Copyright 2018 Yelp and Contributors
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
"""Tests for LocalMRJobRunner"""
import gzip
import os
import stat
import sys
from io import BytesIO
from multiprocessing import Pool
from multiprocessing import cpu_count
from os.path import exists
from os.path import join
from shutil import make_archive
from subprocess import check_call
from unittest import skipIf

from warcio.warcwriter import WARCWriter

try:
    import pyspark
except ImportError:
    pyspark = None

import mrjob
from mrjob.cat import decompress
from mrjob.examples.mr_phone_to_url import MRPhoneToURL
from mrjob.examples.mr_spark_wordcount import MRSparkWordcount
from mrjob.examples.mr_spark_wordcount_script import MRSparkScriptWordcount
from mrjob.examples.mr_sparkaboom import MRSparKaboom
from mrjob.local import LocalMRJobRunner
from mrjob.local import _sort_lines_in_memory
from mrjob.parse import is_uri
from mrjob.step import StepFailedException
from mrjob.util import cmd_line
from mrjob.util import safeeval
from mrjob.util import to_lines

from tests.examples.test_mr_phone_to_url import write_conversion_record
from tests.job import run_job
from tests.mr_cmd_job import MRCmdJob
from tests.mr_counting_job import MRCountingJob
from tests.mr_exit_42_job import MRExit42Job
from tests.mr_filter_job import MRFilterJob
from tests.mr_group import MRGroup
from tests.mr_job_where_are_you import MRJobWhereAreYou
from tests.mr_just_a_jar import MRJustAJar
from tests.mr_sort_and_group import MRSortAndGroup
from tests.mr_spark_os_walk import MRSparkOSWalk
from tests.mr_stdin_only import MRStdinOnly
from tests.mr_two_step_job import MRTwoStepJob
from tests.mr_word_count import MRWordCount
from tests.py2 import call
from tests.py2 import patch
from tests.sandbox import BasicTestCase
from tests.sandbox import EmptyMrjobConfTestCase
from tests.sandbox import SandboxedTestCase
from tests.sandbox import mrjob_conf_patcher
from tests.test_inline import InlineInputManifestTestCase
from tests.test_sim import LocalFSTestCase
from tests.test_sim import SimRunnerJobConfTestCase
from tests.test_sim import SimRunnerNoMapperTestCase
from tests.test_sim import SortValuesTestCase


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

        mr_job = MRTwoStepJob(['-r', 'local', '--num-cores', '4',
                               '-', input_path, input_gz_glob])
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


class NumCoresTestCase(SandboxedTestCase):

    def setUp(self):
        super(NumCoresTestCase, self).setUp()

        self.pool = self.start(patch('mrjob.local.Pool', wraps=Pool))

    def test_default(self):
        mr_job = MRTwoStepJob(['-r', 'local'])
        mr_job.sandbox(stdin=BytesIO(b'foo\nbar\n'))

        with mr_job.make_runner() as runner:
            self.assertEqual(runner._num_mappers(0), cpu_count())
            self.assertEqual(runner._num_reducers(0), cpu_count())

            runner.run()

            self.pool.assert_called_with(processes=None)

    def test_three_cores(self):
        mr_job = MRTwoStepJob(['-r', 'local', '--num-cores', '3'])
        mr_job.sandbox(stdin=BytesIO(b'foo\nbar\n'))

        with mr_job.make_runner() as runner:
            self.assertEqual(runner._num_mappers(0), 3)
            self.assertEqual(runner._num_reducers(0), 3)

            runner.run()

            self.pool.assert_called_with(processes=3)


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
                               '-D=mapred.map.tasks=2',
                               '-D=mapred.reduce.tasks=2',
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


class ExitWithoutExceptionTestCase(BasicTestCase):

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
            ['--python-bin', 'echo', '--no-conf', '-r', 'local'])
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


class LocalBootstrapMrjobTestCase(BasicTestCase):

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

                output = list(mr_job.parse_output((runner.cat_output())))
                self.assertEqual(len(output), 1)

                # script should load mrjob from its working dir
                _, script_mrjob_dir = output[0]

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

        reducer_cmd = '/bin/sh -c \'wc -l | tr -Cd "[:digit:]"\''
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


class SetupLineEncodingTestCase(BasicTestCase):

    def test_setup_wrapper_script_uses_local_line_endings(self):
        job = MRTwoStepJob(['-r', 'local', '--setup', 'true'])
        job.sandbox(stdin=BytesIO())

        # tests #1071. Unfortunately, we mostly run these tests on machines
        # that use unix line endings anyway. So monitor open() instead
        with patch(
                'mrjob.sim.open', create=True, side_effect=open) as m_open:
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

    def test_empty_sort_bin_means_default(self):
        job = MRGroup(['-r', 'local', '--sort-bin', ''])
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


class LocalInputManifestTestCase(InlineInputManifestTestCase):

    RUNNER = 'local'

    def test_setup_cmd(self):
        wet_path = join(self.tmp_dir, 'wet.warc.wet.gz')
        with open(wet_path, 'wb') as wet:
            writer = WARCWriter(wet)

            write_conversion_record(
                writer, 'https://big.directory/',
                b'The Time: (612) 777-9311\nJenny: (201) 867-5309\n')
            write_conversion_record(
                writer, 'https://jseventplanning.biz/',
                b'contact us at +1 201 867 5309')

        touched_path = join(self.tmp_dir, 'touched')
        setup_cmd = 'touch ' + touched_path

        self.assertFalse(exists(touched_path))

        self.assertEqual(
            run_job(MRPhoneToURL(
                ['-r', self.RUNNER, '--setup', setup_cmd, wet_path])),
            self.EXPECTED_OUTPUT)

        self.assertTrue(exists(touched_path))


class UnsupportedStepsTestCase(SandboxedTestCase):

    def test_no_jar_steps(self):
        jar_path = self.makefile('dora.jar')

        job = MRJustAJar(['-r', 'local', '--jar', jar_path])
        job.sandbox()

        self.assertRaises(NotImplementedError, job.make_runner)


class SparkMainTestCase(SandboxedTestCase):

    def test_default_spark_main(self):
        runner = LocalMRJobRunner()

        self.assertEqual(runner._spark_main(),
                         'local-cluster[%d,1,1024]' % cpu_count())

    def test_num_cores(self):
        runner = LocalMRJobRunner(num_cores=3)

        self.assertEqual(runner._spark_main(),
                         'local-cluster[3,1,1024]')

    def _test_spark_executor_memory(self, conf_value, megs):
        runner = LocalMRJobRunner(
            jobconf={'spark.executor.memory': conf_value})

        self.assertEqual(runner._spark_main(),
                         'local-cluster[%d,1,%d]' % (
                             cpu_count(), megs))

    def test_spark_exector_memory_2g(self):
        self._test_spark_executor_memory('2g', 2048)

    def test_spark_exector_memory_512m(self):
        self._test_spark_executor_memory('512m', 512)

    def test_spark_exector_memory_512M(self):
        # test case-insensitivity
        self._test_spark_executor_memory('512M', 512)

    def test_spark_exector_memory_1t(self):
        self._test_spark_executor_memory('1t', 1024 * 1024)

    def test_spark_exector_memory_1_billion(self):
        # should round up
        self._test_spark_executor_memory('1000000000', 954)

    def test_spark_exector_memory_640000k(self):
        self._test_spark_executor_memory('640000k', 625)


@skipIf(pyspark is None, 'no pyspark module')
class LocalRunnerSparkTestCase(SandboxedTestCase):
    # these tests are slow (~30s) because they run on
    # actual Spark, in local-cluster mode

    def test_spark_mrjob(self):
        text = b'one fish\ntwo fish\nred fish\nblue fish\n'

        job = MRSparkWordcount(['-r', 'local'])
        job.sandbox(stdin=BytesIO(text))

        counts = {}

        with job.make_runner() as runner:
            runner.run()

            for line in to_lines(runner.cat_output()):
                k, v = safeeval(line)
                counts[k] = v

        self.assertEqual(counts, dict(
            blue=1, fish=4, one=1, red=1, two=1))

    def test_spark_job_failure(self):
        job = MRSparKaboom(['-r', 'local'])
        job.sandbox(stdin=BytesIO(b'line\n'))

        with job.make_runner() as runner:
            self.assertRaises(StepFailedException, runner.run)

    def test_spark_script_mrjob(self):
        text = b'one fish\ntwo fish\nred fish\nblue fish\n'

        job = MRSparkScriptWordcount(['-r', 'local'])
        job.sandbox(stdin=BytesIO(text))

        counts = {}

        with job.make_runner() as runner:
            runner.run()

            for line in to_lines(runner.cat_output()):
                k, v = safeeval(line)
                counts[k] = v

        self.assertEqual(counts, dict(
            blue=1, fish=4, one=1, red=1, two=1))

    def test_upload_files_with_rename(self):
        fish_path = self.makefile('fish', b'salmon')
        fowl_path = self.makefile('fowl', b'goose')

        job = MRSparkOSWalk(['-r', 'local',
                             '--files',
                             '%s#ghoti,%s' % (fish_path, fowl_path)])
        job.sandbox()

        file_sizes = {}

        with job.make_runner() as runner:
            runner.run()

            # check working dir mirror
            wd_mirror = runner._wd_mirror()
            self.assertIsNotNone(wd_mirror)
            self.assertFalse(is_uri(wd_mirror))

            self.assertTrue(os.path.exists(wd_mirror))
            # only files which needed to be renamed should be in wd_mirror
            self.assertTrue(os.path.exists(os.path.join(wd_mirror, 'ghoti')))
            self.assertFalse(os.path.exists(os.path.join(wd_mirror, 'fish')))
            self.assertFalse(os.path.exists(os.path.join(wd_mirror, 'fowl')))

            for line in to_lines(runner.cat_output()):
                path, size = safeeval(line)
                file_sizes[path] = size

        # check that files were uploaded to working dir
        self.assertIn('fowl', file_sizes)
        self.assertEqual(file_sizes['fowl'], 5)

        self.assertIn('ghoti', file_sizes)
        self.assertEqual(file_sizes['ghoti'], 6)

        # fish was uploaded as "ghoti"
        self.assertNotIn('fish', file_sizes)

    def test_archive_emulation(self):
        f_dir = self.makedirs('f')
        self.makefile(join(f_dir, 'fish'), b'salmon')
        self.makefile(join(f_dir, 'fowl'), b'goose')

        f_tar_gz = make_archive(join(self.tmp_dir, 'f'), 'gztar', f_dir)

        job = MRSparkOSWalk(['-r', 'local',
                             '--archives', '%s#f-unpacked' % f_tar_gz,
                             '--dirs', f_dir])
        job.sandbox()

        file_sizes = {}

        with job.make_runner() as runner:
            runner.run()

            for line in to_lines(runner.cat_output()):
                path, size = safeeval(line)
                file_sizes[path] = size

        self.assertIn('f/fish', file_sizes)
        self.assertEqual(file_sizes['f/fish'], 6)
        self.assertIn('f/fowl', file_sizes)
        self.assertEqual(file_sizes['f/fowl'], 5)

        self.assertIn('f-unpacked/fish', file_sizes)
        self.assertEqual(file_sizes['f-unpacked/fish'], 6)
        self.assertIn('f-unpacked/fowl', file_sizes)
        self.assertEqual(file_sizes['f-unpacked/fowl'], 5)

        # archives should have been uploaded as files
        self.assertIn('f.tar.gz.file', file_sizes)
        self.assertIn('f-1.tar.gz.file', file_sizes)

    # TODO: add a Spark JAR to the repo, so we can test it


class InputFileArgsTestCase(SandboxedTestCase):
    # test for #567: ensure that local runner doesn't need to pass
    # file args to jobs

    def test_stdin_only(self):
        input1 = self.makefile('input1', contents='cat cat cat cat cat')
        input2 = self.makefile('input2', contents='dog dog dog')

        job = MRStdinOnly(['-r', 'local', input1, input2])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            output = dict(job.parse_output(runner.cat_output()))

            self.assertEqual(output, dict(cat=5, dog=3))
