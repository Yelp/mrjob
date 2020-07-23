# Copyright 2009-2018 Yelp
# Copyright 2019 Yelp
# Copyright 2020 Affirm, Inc.
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
"""Test the runner base class MRJobBinRunner"""
import compileall
import inspect
import os
import signal
import stat
import sys
from io import BytesIO
from os.path import exists
from os.path import getsize
from os.path import join
from platform import python_implementation
from shutil import make_archive
from unittest import skipIf
from zipfile import ZipFile
from zipfile import ZIP_DEFLATED

from mrjob.bin import MRJobBinRunner
from mrjob.bin import pty
from mrjob.bin import pyspark
from mrjob.examples.mr_spark_wordcount import MRSparkWordcount
from mrjob.local import LocalMRJobRunner
from mrjob.py2 import PY2
from mrjob.step import StepFailedException
from mrjob.util import cmd_line
from mrjob.util import which

from tests.mockhadoop import MockHadoopTestCase
from tests.mr_cmd_job import MRCmdJob
from tests.mr_filter_job import MRFilterJob
from tests.mr_just_a_jar import MRJustAJar
from tests.mr_no_mapper import MRNoMapper
from tests.mr_no_runner import MRNoRunner
from tests.mr_null_spark import MRNullSpark
from tests.mr_os_walk_job import MROSWalkJob
from tests.mr_partitioner import MRPartitioner
from tests.mr_sort_values import MRSortValues
from tests.mr_sort_values_and_more import MRSortValuesAndMore
from tests.mr_spark_jar import MRSparkJar
from tests.mr_spark_script import MRSparkScript
from tests.mr_two_step_job import MRTwoStepJob
from tests.mr_word_count import MRWordCount
from tests.py2 import Mock
from tests.py2 import patch
from tests.sandbox import BasicTestCase
from tests.sandbox import EmptyMrjobConfTestCase
from tests.sandbox import SandboxedTestCase
from tests.sandbox import mrjob_conf_patcher

# used to match command lines
is_pypy = (python_implementation() == 'PyPy')
PYTHON_BIN = (
    ('pypy' if PY2 else 'pypy3') if is_pypy else
    ('python2.7' if PY2 else 'python3')
)

# a --spark-main that has a working directory and is available from pyspark
_LOCAL_CLUSTER_MASTER = 'local-cluster[2,1,4096]'


def _mock_upload_mgr():
    def mock_uri(path):
        # use same filename as *path*
        return 'uri-of://%s' % path

    m = Mock()
    m.uri = Mock(side_effect=mock_uri)

    m.prefix = 'uri-of://files'

    return m


class GenericLocalRunnerTestCase(SandboxedTestCase):
    """Disable the local runner's custom
    :py:meth:`~mrjob.local.LocalMRJobRunner._spark_main`
    method, so we can use it to test the basic functionality
    of :py:class:`~mrjob.bin.MRJobBinRunner`.
    """
    def setUp(self):
        super(GenericLocalRunnerTestCase, self).setUp()

        # couldn't figure out how to delete a method with mock
        _spark_main_method = LocalMRJobRunner._spark_main

        def restore_spark_main_method():
            LocalMRJobRunner._spark_main = _spark_main_method

        self.addCleanup(restore_spark_main_method)

        delattr(LocalMRJobRunner, '_spark_main')


class ArgsForSparkStepTestCase(GenericLocalRunnerTestCase):
    # just test the structure of _args_for_spark_step()

    def setUp(self):
        super(ArgsForSparkStepTestCase, self).setUp()

        self.mock_get_spark_submit_bin = self.start(patch(
            'mrjob.bin.MRJobBinRunner.get_spark_submit_bin',
            return_value=['<spark-submit bin>']))

        self.mock_spark_submit_args = self.start(patch(
            'mrjob.bin.MRJobBinRunner._spark_submit_args',
            return_value=['<spark submit args>']))

        self.mock_spark_script_path = self.start(patch(
            'mrjob.bin.MRJobBinRunner._spark_script_path',
            return_value='<spark script path>'))

        self.mock_spark_script_args = self.start(patch(
            'mrjob.bin.MRJobBinRunner._spark_script_args',
            return_value=['<spark script args>']))

    def _test_step(self, step_num):
        job = MRNullSpark(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._args_for_spark_step(step_num),
                ['<spark-submit bin>',
                 '<spark submit args>',
                 '<spark script path>',
                 '<spark script args>'])

            self.mock_get_spark_submit_bin.assert_called_once_with()
            self.mock_spark_submit_args.assert_called_once_with(step_num)
            self.mock_spark_script_path.assert_called_once_with(step_num)
            self.mock_spark_script_args.assert_called_once_with(step_num, None)

    def test_step_0(self):
        self._test_step(0)

    def test_step_1(self):
        self._test_step(1)


class BootstrapMRJobTestCase(BasicTestCase):
    # this just tests _bootstrap_mrjob() (i.e. whether to bootstrap mrjob);
    # actual testing of bootstrapping is in test_local

    def test_default(self):
        runner = MRJobBinRunner(conf_paths=[])
        self.assertEqual(runner._bootstrap_mrjob(), True)

    def test_no_bootstrap_mrjob(self):
        runner = MRJobBinRunner(conf_paths=[], bootstrap_mrjob=False)
        self.assertEqual(runner._bootstrap_mrjob(), False)


class HadoopArgsForStepTestCase(EmptyMrjobConfTestCase):

    # hadoop_extra_args is tested in tests.test_hadoop.HadoopExtraArgsTestCase

    def test_empty(self):
        job = MRWordCount(['-r', 'local'])
        with job.make_runner() as runner:
            self.assertEqual(runner._hadoop_args_for_step(0), [])

    def test_cmdenv(self):
        job = MRWordCount(['-r', 'local',
                           '--cmdenv', 'FOO=bar',
                           '--cmdenv', 'BAZ=qux',
                           '--cmdenv', 'BAX=Arnold'])

        with job.make_runner() as runner:
            self.assertEqual(runner._hadoop_args_for_step(0),
                             ['-cmdenv', 'BAX=Arnold',
                              '-cmdenv', 'BAZ=qux',
                              '-cmdenv', 'FOO=bar',
                              ])

    def test_hadoop_input_format(self):
        input_format = 'org.apache.hadoop.mapred.SequenceFileInputFormat'

        # one-step job
        job1 = MRWordCount(['-r', 'local'])
        # no cmd-line argument for this because it's part of job semantics
        job1.HADOOP_INPUT_FORMAT = input_format
        with job1.make_runner() as runner1:
            self.assertEqual(runner1._hadoop_args_for_step(0),
                             ['-inputformat', input_format])

        # multi-step job: only use -inputformat on the first step
        job2 = MRTwoStepJob(['-r', 'local'])
        job2.HADOOP_INPUT_FORMAT = input_format
        with job2.make_runner() as runner2:
            self.assertEqual(runner2._hadoop_args_for_step(0),
                             ['-inputformat', input_format])
            self.assertEqual(runner2._hadoop_args_for_step(1), [])

    def test_hadoop_output_format(self):
        output_format = 'org.apache.hadoop.mapred.SequenceFileOutputFormat'

        # one-step job
        job1 = MRWordCount(['-r', 'local'])
        # no cmd-line argument for this because it's part of job semantics
        job1.HADOOP_OUTPUT_FORMAT = output_format
        with job1.make_runner() as runner1:
            self.assertEqual(runner1._hadoop_args_for_step(0),
                             ['-outputformat', output_format])

        # multi-step job: only use -outputformat on the last step
        job2 = MRTwoStepJob(['-r', 'local'])
        job2.HADOOP_OUTPUT_FORMAT = output_format
        with job2.make_runner() as runner2:
            self.assertEqual(runner2._hadoop_args_for_step(0), [])
            self.assertEqual(runner2._hadoop_args_for_step(1),
                             ['-outputformat', output_format])

    def test_jobconf(self):
        job = MRWordCount(['-r', 'local',
                           '-D', 'FOO=bar',
                           '-D', 'BAZ=qux',
                           '-D', 'BAX=Arnold'])

        with job.make_runner() as runner:
            self.assertEqual(runner._hadoop_args_for_step(0),
                             ['-D', 'BAX=Arnold',
                              '-D', 'BAZ=qux',
                              '-D', 'FOO=bar',
                              ])

    def test_empty_jobconf_values(self):
        # value of None means to omit that jobconf
        job = MRWordCount(['-r', 'local'])
        # no way to pass in None from the command line
        job.JOBCONF = {'foo': '', 'bar': None}

        with job.make_runner() as runner:
            self.assertEqual(runner._hadoop_args_for_step(0),
                             ['-D', 'foo='])

    def test_configuration_translation(self):
        job = MRWordCount(
            ['-r', 'local',
             '-D', 'mapred.jobtracker.maxtasks.per.job=1'])

        with job.make_runner() as runner:
            with patch.object(runner,
                              'get_hadoop_version', return_value='2.7.1'):
                self.assertEqual(
                    runner._hadoop_args_for_step(0),
                    ['-D', 'mapred.jobtracker.maxtasks.per.job=1',
                     '-D', 'mapreduce.jobtracker.maxtasks.perjob=1'
                     ])

    def test_jobconf_from_step(self):
        jobconf = {'FOO': 'bar', 'BAZ': 'qux'}
        # Hack in steps rather than creating a new MRJob subclass
        runner = LocalMRJobRunner(jobconf=jobconf)
        runner._steps = [{'jobconf': {'BAZ': 'quux', 'BAX': 'Arnold'}}]

        self.assertEqual(runner._hadoop_args_for_step(0),
                         ['-D', 'BAX=Arnold',
                          '-D', 'BAZ=quux',
                          '-D', 'FOO=bar',
                          ])

    def test_non_string_jobconf_values_in_mrjob_conf(self):
        # regression test for #323
        MRJOB_CONF = dict(runners=dict(local=dict(jobconf=dict(
            BAX=True,
            BAZ=False,
            FOO=None,
            QUX='null',
        ))))

        with mrjob_conf_patcher(MRJOB_CONF):
            job = MRWordCount(['-r', 'local'])
            job.sandbox()

            with job.make_runner() as runner:
                # FOO is blanked out because it's None (use "null")
                self.assertEqual(runner._hadoop_args_for_step(0),
                                 ['-D', 'BAX=true',
                                  '-D', 'BAZ=false',
                                  '-D', 'QUX=null',
                                  ])

    def test_partitioner(self):
        job = MRPartitioner(['-r', 'local'])

        with job.make_runner() as runner:
            self.assertEqual(
                runner._hadoop_args_for_step(0),
                ['-partitioner',
                 'org.apache.hadoop.mapred.lib.HashPartitioner']
            )


class TaskPythonBinTestCase(BasicTestCase):

    def test_default(self):
        runner = MRJobBinRunner()
        self.assertEqual(runner._python_bin(), [PYTHON_BIN])
        self.assertEqual(runner._task_python_bin(), [PYTHON_BIN])

    def test_python_bin(self):
        runner = MRJobBinRunner(python_bin=['python', '-v'])
        self.assertEqual(runner._python_bin(), ['python', '-v'])
        self.assertEqual(runner._task_python_bin(), ['python', '-v'])

    def test_task_python_bin(self):
        runner = MRJobBinRunner(task_python_bin=['python', '-v'])
        self.assertEqual(runner._python_bin(), [PYTHON_BIN])
        self.assertEqual(runner._task_python_bin(), ['python', '-v'])

    def test_empty_python_bin_means_default(self):
        runner = MRJobBinRunner(python_bin=[], task_python_bin=[])

        self.assertEqual(runner._python_bin(), [PYTHON_BIN])
        self.assertEqual(runner._task_python_bin(), [PYTHON_BIN])


class RenderSubstepTestCase(SandboxedTestCase):

    def test_streaming_mapper(self):
        self._test_streaming_substep('mapper')

    def test_streaming_combiner(self):
        self._test_streaming_substep('combiner')

    def test_streaming_reducer(self):
        self._test_streaming_substep('reducer')

    def _test_streaming_substep(self, mrc):
        job = MRTwoStepJob(['-r', 'local'])  # has mapper, combiner, reducer
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._render_substep(0, mrc),
                '%s mr_two_step_job.py --step-num=0 --%s' % (
                    sys.executable, mrc)
            )

    def test_step_1_streaming(self):
        # just make sure that --step-num isn't hardcoded
        job = MRTwoStepJob(['-r', 'local'])  # has step 1 mapper
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._render_substep(1, 'mapper'),
                '%s mr_two_step_job.py --step-num=1 --mapper' % sys.executable
            )

    def test_mapper_cmd(self):
        self._test_cmd('mapper')

    def test_combiner_cmd(self):
        self._test_cmd('combiner')

    def test_reducer_cmd(self):
        self._test_cmd('reducer')

    def _test_cmd(self, mrc):
        job = MRCmdJob(['-r', 'local', ('--%s-cmd' % mrc), 'grep foo'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._render_substep(0, mrc),
                'grep foo'
            )

    def test_no_mapper(self):
        job = MRNoMapper(['-r', 'local'])  # second step has no mapper
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._render_substep(1, 'mapper'),
                'cat'
            )

    def test_no_combiner(self):
        self._test_no_non_mapper('combiner')

    def test_no_reducer(self):
        self._test_no_non_mapper('reducer')

    def _test_no_non_mapper(self, mrc):
        job = MRNoRunner(['-r', 'local'])  # only has mapper
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._render_substep(0, mrc),
                None
            )

    def test_setup_wrapper_script(self):
        job = MRTwoStepJob(['-r', 'local', '--setup', 'true'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._create_setup_wrapper_scripts()

            # note that local mode uses sys.executable, not python/python3
            self.assertEqual(
                runner._render_substep(0, 'mapper'),
                '/bin/sh -ex setup-wrapper.sh %s mr_two_step_job.py'
                ' --step-num=0 --mapper' % sys.executable
            )

    def test_setup_wrapper_script_custom_sh_bin(self):
        job = MRTwoStepJob(['-r', 'local', '--setup', 'true',
                            '--sh-bin', 'sh -xv'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._create_setup_wrapper_scripts()

            # note that local mode uses sys.executable, not python/python3
            self.assertEqual(
                runner._render_substep(0, 'mapper'),
                'sh -xv setup-wrapper.sh %s mr_two_step_job.py'
                ' --step-num=0 --mapper' % sys.executable
            )

    def test_setup_wrapper_respects_sh_bin_method(self):
        job = MRTwoStepJob(['-r', 'local', '--setup', 'true'])
        job.sandbox()

        self.start(patch('mrjob.bin.MRJobBinRunner._sh_bin',
                         return_value=['bash']))

        with job.make_runner() as runner:
            runner._create_setup_wrapper_scripts()

            self.assertEqual(
                runner._render_substep(0, 'mapper'),
                'bash setup-wrapper.sh %s mr_two_step_job.py'
                ' --step-num=0 --mapper' % sys.executable
            )

    def test_mapper_pre_filter(self):
        self._test_pre_filter('mapper')

    def test_combiner_pre_filter(self):
        self._test_pre_filter('combiner')

    def test_reducer_pre_filter(self):
        self._test_pre_filter('reducer')

    def _test_pre_filter(self, mrc):
        job = MRFilterJob(['-r', 'local', ('--%s-filter' % mrc), 'cat'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._render_substep(0, mrc),
                "/bin/sh -ex -c 'cat |"
                " %s mr_filter_job.py --step-num=0 --%s"
                " --%s-filter cat'" %
                (sys.executable, mrc, mrc))

    def test_pre_filter_custom_sh_bin(self):
        job = MRFilterJob(['-r', 'local', '--mapper-filter', 'cat',
                           '--sh-bin', 'sh -xv'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._render_substep(0, 'mapper'),
                "sh -xv -c 'cat |"
                " %s mr_filter_job.py --step-num=0 --mapper"
                " --mapper-filter cat'" % sys.executable)

    def test_pre_filter_respects_sh_bin_method(self):
        job = MRFilterJob(['-r', 'local', '--mapper-filter', 'cat'])
        job.sandbox()

        self.start(patch('mrjob.bin.MRJobBinRunner._sh_bin',
                         return_value=['bash']))

        with job.make_runner() as runner:
            self.assertEqual(
                runner._render_substep(0, 'mapper'),
                "bash -c 'cat |"
                " %s mr_filter_job.py --step-num=0 --mapper"
                " --mapper-filter cat'" % sys.executable)

    def test_pre_filter_respects_sh_pre_commands_method(self):
        job = MRFilterJob(['-r', 'local', '--mapper-filter', 'cat'])
        job.sandbox()

        self.start(patch('mrjob.bin.MRJobBinRunner._sh_pre_commands',
                         return_value=['set -v']))

        with job.make_runner() as runner:
            self.assertEqual(
                runner._render_substep(0, 'mapper'),
                "/bin/sh -ex -c 'set -v;"
                " cat | %s mr_filter_job.py --step-num=0 --mapper"
                " --mapper-filter cat'" % sys.executable)


class SetupTestCase(SandboxedTestCase):

    def setUp(self):
        super(SetupTestCase, self).setUp()

        self.foo_dir = self.makedirs('foo')

        self.foo_py = join(self.tmp_dir, 'foo', 'foo.py')

        # if our job can import foo, getsize will return 2x as many bytes
        with open(self.foo_py, 'w') as foo_py:
            foo_py.write('import os.path\n'
                         'from os.path import getsize as _real_getsize\n'
                         'os.path.getsize = lambda p: _real_getsize(p) * 2')

        self.foo_sh = join(self.tmp_dir, 'foo', 'foo.sh')

        with open(self.foo_sh, 'w') as foo_sh:
            foo_sh.write('#!/bin/sh\n'
                         'touch foo.sh-made-this\n')
        os.chmod(self.foo_sh, stat.S_IRWXU)

        self.foo_tar_gz = make_archive(
            join(self.tmp_dir, 'foo'), 'gztar', self.foo_dir)

        self.foo_zip = join(self.tmp_dir, 'foo.zip')
        zf = ZipFile(self.foo_zip, 'w', ZIP_DEFLATED)
        zf.write(self.foo_py, 'foo.py')
        zf.close()

        self.foo_py_size = getsize(self.foo_py)
        self.foo_sh_size = getsize(self.foo_sh)
        self.foo_tar_gz_size = getsize(self.foo_tar_gz)
        self.foo_zip_size = getsize(self.foo_zip)

    def test_file_upload(self):
        job = MROSWalkJob(['-r', 'local',
                           '--files', '%s,%s#bar.sh' % (
                               self.foo_sh, self.foo_sh)
                           ])
        job.sandbox()

        with job.make_runner() as r:
            r.run()

            path_to_size = dict(job.parse_output(r.cat_output()))

        self.assertEqual(path_to_size.get('./foo.sh'), self.foo_sh_size)
        self.assertEqual(path_to_size.get('./bar.sh'), self.foo_sh_size)

    def test_archive_upload(self):
        job = MROSWalkJob(['-r', 'local',
                           '--archives', '%s,%s#foo' % (
                               self.foo_tar_gz, self.foo_tar_gz)
                           ])
        job.sandbox()

        with job.make_runner() as r:
            r.run()

            path_to_size = dict(job.parse_output(r.cat_output()))

        self.assertEqual(path_to_size.get('./foo.tar.gz/foo.py'),
                         self.foo_py_size)
        self.assertEqual(path_to_size.get('./foo/foo.py'),
                         self.foo_py_size)

    def test_dir_upload(self):
        job = MROSWalkJob(['-r', 'local',
                           '--dirs', '%s,%s#bar' % (
                               self.foo_dir, self.foo_dir)
                           ])
        job.sandbox()

        with job.make_runner() as r:
            r.run()

            path_to_size = dict(job.parse_output(r.cat_output()))

        self.assertEqual(path_to_size.get('./foo/foo.py'),
                         self.foo_py_size)
        self.assertEqual(path_to_size.get('./bar/foo.py'),
                         self.foo_py_size)

    def test_python_archive(self):
        job = MROSWalkJob([
            '-r', 'local',
            '--setup', 'export PYTHONPATH=%s#/:$PYTHONPATH' % self.foo_tar_gz
        ])
        job.sandbox()

        with job.make_runner() as r:
            r.run()

            path_to_size = dict(job.parse_output(r.cat_output()))

        # foo.py should be there, and getsize() should be patched to return
        # double the number of bytes
        self.assertEqual(path_to_size.get('./foo/foo.py'),
                         self.foo_py_size * 2)

    def test_python_dir_archive(self):
        job = MROSWalkJob([
            '-r', 'local',
            '--setup', 'export PYTHONPATH=%s/#:$PYTHONPATH' % self.foo_dir
        ])
        job.sandbox()

        with job.make_runner() as r:
            r.run()

            path_to_size = dict(job.parse_output(r.cat_output()))

        # foo.py should be there, and getsize() should be patched to return
        # double the number of bytes
        self.assertEqual(path_to_size.get('./foo/foo.py'),
                         self.foo_py_size * 2)

    def test_python_zip_file(self):
        job = MROSWalkJob([
            '-r', 'local',
            '--setup', 'export PYTHONPATH=%s#:$PYTHONPATH' % self.foo_zip
        ])
        job.sandbox()

        with job.make_runner() as r:
            r.run()

            path_to_size = dict(job.parse_output(r.cat_output()))

        # foo.py should be there, and getsize() should be patched to return
        # double the number of bytes
        self.assertEqual(path_to_size.get('./foo.zip'),
                         self.foo_zip_size * 2)

    def test_py_file(self):
        job = MROSWalkJob([
            '-r', 'local',
            '--py-files', self.foo_zip,
        ])
        job.sandbox()

        with job.make_runner() as r:
            r.run()

            path_to_size = dict(job.parse_output(r.cat_output()))

        # foo.py should be there, and getsize() should be patched to return
        # double the number of bytes
        self.assertEqual(path_to_size.get('./foo.zip'),
                         self.foo_zip_size * 2)

    def test_setup_command(self):
        job = MROSWalkJob(
            ['-r', 'local',
             '--setup', 'touch bar'])
        job.sandbox()

        with job.make_runner() as r:
            r.run()

            path_to_size = dict(job.parse_output(r.cat_output()))

        self.assertIn('./bar', path_to_size)

    def test_setup_script(self):
        job = MROSWalkJob(
            ['-r', 'local',
             '--setup', self.foo_sh + '#'])
        job.sandbox()

        with job.make_runner() as r:
            r.run()

            path_to_size = dict(job.parse_output(r.cat_output()))

            self.assertEqual(path_to_size.get('./foo.sh'), self.foo_sh_size)
            self.assertIn('./foo.sh-made-this', path_to_size)

    def test_bad_setup_command(self):
        bar_path = join(self.tmp_dir, 'bar')
        baz_path = join(self.tmp_dir, 'baz')

        job = MROSWalkJob([
            '-r', 'local',
            '--setup', 'touch %s' % bar_path,
            '--setup', 'false',  # always "fails"
            '--setup', 'touch %s' % baz_path,
            '--cleanup-on-failure=ALL',
        ])
        job.sandbox()

        with job.make_runner() as r:
            self.assertRaises(Exception, r.run)

            # first command got run but not third one
            self.assertTrue(exists(bar_path))
            self.assertFalse(exists(baz_path))

    def test_stdin_bypasses_wrapper_script(self):
        job = MROSWalkJob([
            '-r', 'local',
            '--setup', 'cat > stdin.txt',
        ])
        job.sandbox(stdin=BytesIO(b'some input\n'))

        # local mode doesn't currently pipe input into stdin
        # (see issue #567), so this test would hang if it failed
        def alarm_handler(*args, **kwargs):
            raise Exception('Setup script stalled on stdin')

        try:
            self._old_alarm_handler = signal.signal(
                signal.SIGALRM, alarm_handler)
            signal.alarm(10)

            with job.make_runner() as r:
                r.run()

                path_to_size = dict(job.parse_output(r.cat_output()))

                self.assertEqual(path_to_size.get('./stdin.txt'), 0)
                # input gets passed through by identity mapper
                self.assertEqual(path_to_size.get(None), 'some input')

        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, self._old_alarm_handler)

    def test_wrapper_script_only_writes_to_stderr(self):
        job = MROSWalkJob([
            '--setup', 'echo stray output',
            '-r', 'local',
        ])
        job.sandbox()

        with job.make_runner() as r:
            r.run()

            output = b''.join(r.cat_output())

            # stray ouput should be in stderr files, not the job's output
            self.assertNotIn(b'stray output', output)

            with open(r._task_stderr_path('mapper', 0, 0), 'rb') as stderr:
                self.assertIn(b'stray output', stderr.read())


class SetupWrapperScriptContentTestCase(SandboxedTestCase):

    def test_no_shebang(self):
        job = MRTwoStepJob(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            out = runner._setup_wrapper_script_content([])

            self.assertFalse(out[0].startswith('#!'))

    def test_respects_sh_pre_commands_methd(self):
        job = MRTwoStepJob(['-r', 'local'])
        job.sandbox()

        self.start(patch('mrjob.bin.MRJobBinRunner._sh_pre_commands',
                         return_value=['set -e', 'set -v']))

        with job.make_runner() as runner:
            out = runner._setup_wrapper_script_content([])

            self.assertEqual(out[:2], ['set -e', 'set -v'])


class PyFilesTestCase(GenericLocalRunnerTestCase):

    def test_default(self):
        job = MRNullSpark(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner._py_files(),
                             [runner._create_mrjob_zip()])

    def test_eggs(self):
        # by default, we pass py_files directly to Spark
        egg1_path = self.makefile('dragon.egg')
        egg2_path = self.makefile('horton.egg')

        job = MRNullSpark(['-r', 'local',
                           '--py-files', '%s,%s' % (egg1_path, egg2_path)])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._py_files(),
                [egg1_path, egg2_path, runner._create_mrjob_zip()]
            )

    def test_no_bootstrap_mrjob(self):
        job = MRNullSpark(['-r', 'local',
                           '--no-bootstrap-mrjob'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner._py_files(),
                             [])

    def test_no_hash_paths(self):
        egg_path = self.makefile('horton.egg')

        job = MRNullSpark(['-r', 'local',
                           '--py-files', egg_path + '#mayzie.egg'])
        job.sandbox()

        self.assertRaises(ValueError, job.make_runner)


class SortValuesTestCase(SandboxedTestCase):
    # test that the value of SORT_VALUES affects the Hadoop command line

    def test_no_sort_values(self):
        mr_job = MRTwoStepJob(['-r', 'local'])
        mr_job.sandbox()

        self.assertFalse(mr_job.sort_values())

        with mr_job.make_runner() as runner:
            self.assertEqual(runner._jobconf_for_step(0), {})
            self.assertNotIn('-partitioner', runner._hadoop_args_for_step(0))

    def test_sort_values_jobconf_version_agnostic(self):
        # this only happens in local runners
        mr_job = MRSortValues(['-r', 'local'])
        mr_job.sandbox()

        self.assertTrue(mr_job.sort_values())

        with mr_job.make_runner() as runner:
            self.assertEqual(runner._jobconf_for_step(0), {
                'mapred.text.key.partitioner.options': '-k1,1',
                'mapreduce.partition.keypartitioner.options': '-k1,1',
                'stream.num.map.output.key.fields': '2',
            })

    def test_sort_values_jobconf_hadoop_1(self):
        mr_job = MRSortValues(['-r', 'local', '--hadoop-version', '1.0.0'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            self.assertEqual(runner._jobconf_for_step(0), {
                'mapred.text.key.partitioner.options': '-k1,1',
                'stream.num.map.output.key.fields': '2',
            })

    def test_sort_values_jobconf_hadoop_2(self):
        mr_job = MRSortValues(['-r', 'local', '--hadoop-version', '2.0.0'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            self.assertEqual(runner._jobconf_for_step(0), {
                'mapreduce.partition.keypartitioner.options': '-k1,1',
                'stream.num.map.output.key.fields': '2',
            })

    def test_job_can_override_jobconf(self):
        mr_job = MRSortValuesAndMore(['-r', 'local',
                                      '--hadoop-version', '2.0.0'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            self.assertEqual(
                runner._jobconf_for_step(0), {
                    'mapreduce.partition.keycomparator.options': '-k1 -k2nr',
                    'mapreduce.partition.keypartitioner.options': '-k1,1',
                    'stream.num.map.output.key.fields': '3',
                }
            )

    def test_steps_can_override_jobconf(self):
        mr_job = MRSortValuesAndMore(['-r', 'local',
                                      '--hadoop-version', '2.0.0'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            self.assertEqual(
                runner._jobconf_for_step(1), {
                    'mapreduce.job.output.key.comparator.class':
                    'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                    'mapreduce.partition.keycomparator.options': '-k1 -k2nr',
                    'mapreduce.partition.keypartitioner.options': '-k1,1',
                    'stream.num.map.output.key.fields': '3',
                }
            )

    def test_cmd_line_can_override_jobconf(self):
        mr_job = MRSortValues([
            '-r', 'local',
            '--hadoop-version', '2.0.0',
            '-D', 'stream.num.map.output.key.fields=3',
        ])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            self.assertEqual(
                runner._jobconf_for_step(0), {
                    'mapreduce.partition.keypartitioner.options': '-k1,1',
                    'stream.num.map.output.key.fields': '3',
                }
            )

    def test_partitioner(self):
        mr_job = MRSortValues(['-r', 'local'])
        mr_job.sandbox()

        self.assertTrue(mr_job.sort_values())

        with mr_job.make_runner() as runner:
            hadoop_args = runner._hadoop_args_for_step(0)
            self.assertIn('-partitioner', hadoop_args)
            self.assertEqual(
                hadoop_args[hadoop_args.index('-partitioner') + 1],
                'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner')

    def test_job_can_override_partitioner(self):
        mr_job = MRSortValuesAndMore(['-r', 'local'])
        mr_job.sandbox()

        self.assertTrue(mr_job.sort_values())

        with mr_job.make_runner() as runner:
            hadoop_args = runner._hadoop_args_for_step(0)
            self.assertIn('-partitioner', hadoop_args)
            self.assertEqual(
                hadoop_args[hadoop_args.index('-partitioner') + 1],
                'org.apache.hadoop.mapred.lib.HashPartitioner')


class SparkScriptPathTestCase(GenericLocalRunnerTestCase):

    def setUp(self):
        super(SparkScriptPathTestCase, self).setUp()

        self.mock_interpolate_spark_script_path = self.start(patch(
            'mrjob.bin.MRJobBinRunner._interpolate_spark_script_path'))

    def test_spark_mr_job(self):
        job = MRNullSpark(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_script_path(0),
                self.mock_interpolate_spark_script_path(
                    inspect.getfile(MRNullSpark))
            )

    def test_spark_jar(self):
        # _spark_script_path() also works with jars
        self.fake_jar = self.makefile('fake.jar')

        job = MRSparkJar(['-r', 'local', '--jar', self.fake_jar])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_script_path(0),
                self.mock_interpolate_spark_script_path(
                    self.fake_jar)
            )

    def test_spark_script(self):
        self.fake_script = self.makefile('fake_script.py')

        job = MRSparkScript(['-r', 'local', '--script', self.fake_script])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_script_path(0),
                self.mock_interpolate_spark_script_path(
                    self.fake_script)
            )

    def test_streaming_step_not_okay(self):
        job = MRTwoStepJob(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertRaises(
                TypeError,
                runner._spark_script_path, 0)


class SparkSubmitArgsTestCase(SandboxedTestCase):
    # mostly testing on the spark runner because it doesn't override
    # _spark_submit_args(), _spark_main(), or _spark_deploy_mode()

    def setUp(self):
        super(SparkSubmitArgsTestCase, self).setUp()

        # bootstrapping mrjob is tested below in SparkPyFilesTestCase
        self.start(patch('mrjob.bin.MRJobBinRunner._bootstrap_mrjob',
                         return_value=False))

    def test_default(self):
        job = MRNullSpark(['-r', 'spark'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                [
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--main', 'local[*]',
                    '--deploy-mode', 'client',
                ]
            )

    def test_spark_main_and_deploy_mode(self):
        job = MRNullSpark([
            '-r', 'spark',
            '--spark-main', 'yoda',
            '--spark-deploy-mode', 'the-force',
        ])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                [
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--main', 'yoda',
                    '--deploy-mode', 'the-force',
                    '--files',
                    runner._dest_in_wd_mirror(runner._script_path,
                                              'mr_null_spark.py'),
                ]
            )

    def test_empty_spark_main_and_deploy_mode_mean_defaults(self):
        job = MRNullSpark([
            '-r', 'spark',
            '--spark-main', '',
            '--spark-deploy-mode', '',
        ])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                [
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--main', 'local[*]',
                    '--deploy-mode', 'client',
                ]
            )

    def test_cmdenv(self):
        job = MRNullSpark(['-r', 'spark',
                           '--cmdenv', 'FOO=bar',
                           '--cmdenv', 'BAZ=qux'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                [
                    '--conf', 'spark.executorEnv.BAZ=qux',
                    '--conf', 'spark.executorEnv.FOO=bar',
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--main', 'local[*]',
                    '--deploy-mode', 'client',
                ]
            )

    def test_custom_python_bin(self):
        job = MRNullSpark(['-r', 'spark',
                           '--python-bin', 'mypy'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                [
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=mypy',
                    '--main', 'local[*]',
                    '--deploy-mode', 'client',
                ]
            )

    def test_cmdenv_can_override_python_bin(self):
        job = MRNullSpark(['-r', 'spark', '--cmdenv', 'PYSPARK_PYTHON=ourpy'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                [
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=ourpy',
                    '--main', 'local[*]',
                    '--deploy-mode', 'client',
                ]
            )

    def test_spark_cmdenv_method(self):
        # test that _spark_submit_args() uses _spark_cmdenv(),
        # so we can just test _spark_cmdenv() in other test cases
        hard_coded_env = dict(FOO='bar')

        self.start(patch('mrjob.bin.MRJobBinRunner._spark_cmdenv',
                         return_value=hard_coded_env, create=True))

        job = MRNullSpark(['-r', 'spark'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                [
                    '--conf', 'spark.executorEnv.FOO=bar',
                    '--main', 'local[*]',
                    '--deploy-mode', 'client',
                ]
            )

    def test_jobconf(self):
        job = MRNullSpark(['-r', 'spark',
                           '-D', 'spark.executor.memory=10g'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                [
                    '--conf', 'spark.executor.memory=10g',
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--main', 'local[*]',
                    '--deploy-mode', 'client',
                ]
            )

    def test_jobconf_uses_jobconf_for_step_method(self):
        job = MRNullSpark(['-r', 'spark'])
        job.sandbox()

        with job.make_runner() as runner:
            self.start(patch.object(
                runner, '_jobconf_for_step', return_value=dict(foo='bar')))

            self.assertEqual(
                runner._spark_submit_args(0),
                [
                    '--conf', 'foo=bar',
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--main', 'local[*]',
                    '--deploy-mode', 'client',
                ]
            )

    def test_jobconf_can_override_python_bin_and_cmdenv(self):
        job = MRNullSpark(
            ['-r', 'spark',
             '--cmdenv', 'FOO=bar',
             '--python-bin', 'mypy',
             '-D', 'spark.executorEnv.FOO=baz',
             '-D', 'spark.executorEnv.PYSPARK_PYTHON=ourpy'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                [
                    '--conf', 'spark.executorEnv.FOO=baz',
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=ourpy',
                    '--main', 'local[*]',
                    '--deploy-mode', 'client',
                ]
            )

    def test_yarn_app_main_env(self):
        job = MRNullSpark(
            ['-r', 'spark',
             '--spark-main', 'yarn',
             '--cmdenv', 'FOO=bar'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                [
                    '--conf', 'spark.executorEnv.FOO=bar',
                    '--conf',
                    'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--conf', 'spark.yarn.appMainEnv.FOO=bar',
                    '--conf',
                    'spark.yarn.appMainEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--main', 'yarn',
                    '--deploy-mode', 'client',
                    '--files',
                    runner._dest_in_wd_mirror(runner._script_path,
                                              'mr_null_spark.py'),
                ]
            )

    def test_spark_main_method_triggers_yarn_app_main_env(self):
        self.start(patch('mrjob.bin.MRJobBinRunner._spark_main',
                         return_value='yarn'))

        job = MRNullSpark(['-r', 'spark'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                [
                    '--conf',
                    'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--conf',
                    'spark.yarn.appMainEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--main', 'yarn',
                    '--deploy-mode', 'client',
                    '--files',
                    runner._dest_in_wd_mirror(runner._script_path,
                                              'mr_null_spark.py')
                ]
            )

    def test_non_string_jobconf_values_in_mrjob_conf(self):
        # regression test for #323
        MRJOB_CONF = dict(runners=dict(spark=dict(jobconf=dict(
            BAX=True,
            BAZ=False,
            FOO=None,
            QUX='null',
        ))))
        self.start(mrjob_conf_patcher(MRJOB_CONF))

        job = MRNullSpark(['-r', 'spark'])
        job.sandbox()

        with job.make_runner() as runner:
            # FOO is blanked out because it's None (use "null")
            self.assertEqual(
                runner._spark_submit_args(0),
                [
                    '--conf', 'BAX=true',
                    '--conf', 'BAZ=false',
                    '--conf', 'QUX=null',
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--main', 'local[*]',
                    '--deploy-mode', 'client',
                ]
            )

    def test_libjars_option(self):
        fake_libjar = self.makefile('fake_lib.jar')

        job = MRNullSpark(
            ['-r', 'spark', '--libjars', fake_libjar])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                [
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--jars', fake_libjar,
                    '--main', 'local[*]',
                    '--deploy-mode', 'client',
                ]
            )

    def test_libjar_paths_override(self):
        job = MRNullSpark(['-r', 'spark'])
        job.sandbox()

        with job.make_runner() as runner:
            self.start(patch.object(
                runner, '_libjar_paths',
                return_value=['s3://a/a.jar', 's3://b/b.jar']))

            self.assertEqual(
                runner._spark_submit_args(0),
                [
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--jars', 's3://a/a.jar,s3://b/b.jar',
                    '--main', 'local[*]',
                    '--deploy-mode', 'client',
                ]
            )

    def test_spark_args_switch(self):
        job = MRNullSpark(['-r', 'spark',
                           '--spark-args=--name Dave'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                [
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--main', 'local[*]',
                    '--deploy-mode', 'client',
                    '--name', 'Dave',
                ]
            )

    def test_job_spark_args(self):
        # --extra-spark-arg is a passthrough option for MRNullSpark
        job = MRNullSpark(['-r', 'spark',
                           '--extra-spark-arg=-v'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0), [
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--main', 'local[*]',
                    '--deploy-mode', 'client',
                    '-v',
                ]
            )

    def test_job_spark_args_come_after_option_spark_args(self):
        job = MRNullSpark(
            ['-r', 'spark',
             '--extra-spark-arg=-v',
             '--spark-args=--name Dave'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0), [
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--main', 'local[*]',
                    '--deploy-mode', 'client',
                    '--name', 'Dave', '-v',
                ]
            )

    def test_yarn_file_args(self):
        foo1_path = self.makefile('foo1')
        foo2_path = self.makefile('foo2')
        foo3_uri = 'hdfs:///path/to/foo3'
        baz_path = self.makefile('baz.tar.gz')
        qux_path = self.makedirs('qux')

        job = MRNullSpark([
            '-r', 'spark',
            '--spark-main', 'yarn',
            '--files', '%s#foo1,%s#bar,%s#baz' % (
                foo1_path, foo2_path, foo3_uri),
            '--archives', baz_path,
            '--dirs', qux_path,
        ])
        job.sandbox()

        with job.make_runner() as runner:
            runner._upload_mgr = _mock_upload_mgr()

            self.assertEqual(
                runner._spark_submit_args(0), [
                    '--conf',
                    'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    # there's a separate conf for envvars in YARN
                    '--conf',
                    'spark.yarn.appMainEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--main', 'yarn',
                    '--deploy-mode', 'client',
                    '--files',
                    (runner._dest_in_wd_mirror(foo2_path, 'bar') +
                     ',' +
                     # we're in YARN, so we can just add #baz to rename foo3
                     'hdfs:///path/to/foo3#baz' +
                     ',' +
                     runner._dest_in_wd_mirror(foo1_path, 'foo1') +
                     ',' +
                     runner._dest_in_wd_mirror(runner._script_path,
                                               'mr_null_spark.py')),
                    '--archives',
                    # named baz-1.tar.gz in wd mirror to leave room for the
                    # name baz.tar.gz
                    (runner._dest_in_wd_mirror(
                        baz_path, 'baz-1.tar.gz') + '#baz.tar.gz' +
                     ',' +
                     runner._dest_in_wd_mirror(
                         qux_path, 'qux.tar.gz') + '#qux')
                ]
            )

    def test_non_yarn_file_args(self):
        # non-YARN runners don't support archives or hash paths
        foo1_path = self.makefile('foo1')
        foo2_path = self.makefile('foo2')
        foo3_uri = 'hdfs:///path/to/foo3'
        foo4_uri = 'hdfs:///path/to/foo4'

        job = MRNullSpark([
            '-r', 'spark',
            '--spark-main', 'mesos://host:12345',
            '--files', '%s,%s#bar,%s,%s#baz' % (
                foo1_path, foo2_path, foo3_uri, foo4_uri)
        ])
        job.sandbox()

        with job.make_runner() as runner:
            runner._upload_mgr = _mock_upload_mgr()

            self.assertEqual(
                runner._spark_submit_args(0), [
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--main', 'mesos://host:12345',
                    '--deploy-mode', 'client',
                    '--files',
                    (
                        runner._dest_in_wd_mirror(foo2_path, 'bar') +
                        ',' +
                        # URIs with different name have to be re-uploaded
                        runner._dest_in_wd_mirror(foo4_uri, 'baz') +
                        ',' +
                        runner._dest_in_wd_mirror(foo1_path, 'foo1') +
                        ',' +
                        # can use URIs with same name as-is
                        foo3_uri +
                        ',' +
                        runner._dest_in_wd_mirror(runner._script_path,
                                                  'mr_null_spark.py')
                    ),
                ]
            )

    def test_file_upload_args_in_cloud(self):
        qux_path = self.makefile('qux')

        job = MRNullSpark([
            '-r', 'spark',
            '--spark-main', 'mock',
            '--extra-file', qux_path,  # file upload arg
        ])
        job.sandbox()

        with job.make_runner() as runner:
            runner._upload_mgr = _mock_upload_mgr()

            self.assertEqual(
                runner._spark_submit_args(0), [
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--main', 'mock',
                    '--deploy-mode', 'client',
                    '--files',
                    'uri-of://files/wd/mr_null_spark.py,uri-of://files/wd/qux',
                ]
            )

    def test_file_upload_args_on_local_cluster(self):
        qux_path = self.makefile('qux')

        job = MRNullSpark([
            '-r', 'spark',
            '--spark-main', _LOCAL_CLUSTER_MASTER,
            '--extra-file', qux_path,  # file upload arg
        ])
        job.sandbox()

        with job.make_runner() as runner:
            runner._copy_files_to_wd_mirror()

            self.assertEqual(
                runner._spark_submit_args(0), [
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--main', _LOCAL_CLUSTER_MASTER,
                    '--deploy-mode', 'client',
                    '--files', ('%s,%s' % (runner._script_path, qux_path)),
                ]
            )

    def test_no_file_args_when_no_working_dir(self):
        qux_path = self.makefile('qux')

        job = MRNullSpark([
            '-r', 'spark',
            '--extra-file', qux_path,  # file upload arg
        ])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0), [
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--main', 'local[*]',
                    '--deploy-mode', 'client',
                ]
            )

            # no working directory, so no need for a mirror
            self.assertIsNone(runner._wd_mirror())

    def test_override_spark_upload_args_method(self):
        # just confirm that _spark_submit_args() uses _spark_upload_args()
        self.start(patch('mrjob.bin.MRJobBinRunner._spark_upload_args',
                         return_value=['--files', 'foo,bar']))

        job = MRNullSpark(['-r', 'spark'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0), [
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--main', 'local[*]',
                    '--deploy-mode', 'client',
                    '--files', 'foo,bar',
                ]
            )

    def test_py_files(self):
        job = MRNullSpark(['-r', 'spark'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._py_files = Mock(
                return_value=['<first py_file>', '<second py_file>']
            )

            self.assertEqual(
                runner._spark_submit_args(0), [
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--main', 'local[*]',
                    '--deploy-mode', 'client',
                    '--py-files',
                    '<first py_file>,<second py_file>'
                ]
            )

    def test_spark_jar_step(self):
        job = MRSparkJar(['-r', 'spark',
                          '--jar-main-class', 'foo.Bar',
                          '--cmdenv', 'BAZ=qux',
                          '-D', 'QUX=baz'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._py_files = Mock(
                return_value=['<first py_file>', '<second py_file>']
            )

            # should handle cmdenv and --class
            # but not set PYSPARK_PYTHON or --py-files
            self.assertEqual(
                runner._spark_submit_args(0), [
                    '--conf', 'QUX=baz',
                    '--conf', 'spark.executorEnv.BAZ=qux',
                    '--class', 'foo.Bar',
                    '--main', 'local[*]',
                    '--deploy-mode', 'client',
                ]
            )

    def test_spark_script_step(self):
        job = MRSparkScript(['-r', 'spark'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0), [
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=' + PYTHON_BIN,
                    '--main', 'local[*]',
                    '--deploy-mode', 'client',
                ]
            )


class CreateMrjobZipTestCase(SandboxedTestCase):

    def test_create_mrjob_zip(self):
        with LocalMRJobRunner(conf_paths=[]) as runner:
            mrjob_zip_path = runner._create_mrjob_zip()
            mrjob_zip = ZipFile(mrjob_zip_path)
            contents = mrjob_zip.namelist()

            for path in contents:
                self.assertEqual(path[:6], 'mrjob/')

            self.assertIn('mrjob/job.py', contents)
            for filename in contents:
                self.assertFalse(filename.endswith('.pyc'),
                                 msg="%s ends with '.pyc'" % filename)

    def test_mrjob_zip_compiles(self):
        runner = LocalMRJobRunner()
        mrjob_zip = runner._create_mrjob_zip()

        ZipFile(mrjob_zip).extractall(self.tmp_dir)

        self.assertTrue(
            compileall.compile_dir(join(self.tmp_dir, 'mrjob'),
                                   quiet=1))


class PySparkPythonTestCase(MockHadoopTestCase):

    # using MockHadoopTestCase just so we don't inadvertently call
    # subprocesses

    def setUp(self):
        super(PySparkPythonTestCase, self).setUp()

        # catch warning about lack of setup script support on non-YARN Spark
        self.log = self.start(patch('mrjob.bin.log'))

    def _env_and_runner(self, *args):
        job = MRNullSpark(['-r', 'hadoop'] + list(args))
        job.sandbox()

        with job.make_runner() as runner:
            runner._create_setup_wrapper_scripts()

            return runner._spark_cmdenv(0), runner

    def test_default(self):
        env, runner = self._env_and_runner()

        self.assertNotIn('PYSPARK_DRIVER_PYTHON', env)
        self.assertEqual(env['PYSPARK_PYTHON'], PYTHON_BIN)

        self.assertFalse(self.log.warning.called)

    def test_custom_python_bin(self):
        env, runner = self._env_and_runner('--python-bin', 'mypy')

        self.assertNotIn('PYSPARK_DRIVER_PYTHON', env)
        self.assertEqual(env['PYSPARK_PYTHON'], 'mypy')

    def test_task_python_bin(self):
        # should be possible to set different Pythons for driver and executor
        env, runner = self._env_and_runner('--task-python-bin', 'mypy')

        self.assertEqual(env['PYSPARK_DRIVER_PYTHON'], PYTHON_BIN)
        self.assertEqual(env['PYSPARK_PYTHON'], 'mypy')

    def test_setup_wrapper_script(self):
        env, runner = self._env_and_runner('--setup', 'true')

        # sanity-check main and deploy mode
        self.assertEqual(runner._spark_main(), 'yarn')
        self.assertEqual(runner._spark_deploy_mode(), 'client')

        self.assertIsNotNone(runner._spark_python_wrapper_path)
        self.assertFalse(self.log.warning.called)

        self.assertEqual(env['PYSPARK_DRIVER_PYTHON'], PYTHON_BIN)
        self.assertEqual(env['PYSPARK_PYTHON'], './python-wrapper.sh')

    def test_setup_wrapper_script_in_cluster_mode(self):
        env, runner = self._env_and_runner(
            '--setup', 'true', '--spark-deploy-mode', 'cluster')

        # in cluster mode, treat driver same as executor
        self.assertNotIn('PYSPARK_DRIVER_PYTHON', env)
        self.assertEqual(env['PYSPARK_PYTHON'], './python-wrapper.sh')

    def test_no_setup_scripts_on_local_main(self):
        env, runner = self._env_and_runner(
            '--setup', 'true', '--spark-main', 'local')

        self.assertIsNone(runner._spark_python_wrapper_path)
        self.assertTrue(self.log.warning.called)

        self.assertNotIn('PYSPARK_DRIVER_PYTHON', env)
        self.assertEqual(env['PYSPARK_PYTHON'], PYTHON_BIN)


class SparkUploadArgsTestCase(MockHadoopTestCase):

    def setUp(self):
        super(SparkUploadArgsTestCase, self).setUp()

        # catch warning about lack of setup script support on non-YARN Spark
        self.log = self.start(patch('mrjob.bin.log'))

    def test_no_setup(self):
        job = MRNullSpark(['-r', 'hadoop', '--files', 'foo'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._upload_mgr = _mock_upload_mgr()
            runner._create_setup_wrapper_scripts()

            upload_args = cmd_line(runner._spark_upload_args())

            self.assertIn('foo', upload_args)

    def test_setup_interpolation(self):
        job = MRNullSpark(
            ['-r', 'hadoop',
             '--setup', 'make -f Makefile#',
             '--files', 'foo',
             ])
        job.sandbox()

        with job.make_runner() as runner:
            runner._upload_mgr = _mock_upload_mgr()
            runner._create_setup_wrapper_scripts()

            upload_args = cmd_line(runner._spark_upload_args())

            self.assertIn('Makefile', upload_args)
            self.assertIn('python-wrapper.sh', upload_args)
            self.assertIn('foo', upload_args)

            self.assertFalse(self.log.warning.called)

    def test_setup_disabled(self):
        job = MRNullSpark(
            ['-r', 'hadoop',
             '--setup', 'make -f Makefile#',
             '--files', 'foo',
             '--spark-main', 'local[*]',
             ])
        job.sandbox()

        with job.make_runner() as runner:
            runner._upload_mgr = _mock_upload_mgr()
            runner._create_setup_wrapper_scripts()

            upload_args = cmd_line(runner._spark_upload_args())

            # nothing is uploaded in local[*] mode
            self.assertNotIn('foo', upload_args)

            self.assertNotIn('Makefile', upload_args)
            self.assertNotIn('python-wrapper.sh', upload_args)

            self.assertTrue(self.log.warning.called)


class SparkPythonSetupWrapperTestCase(MockHadoopTestCase):

    def setUp(self):
        super(SparkPythonSetupWrapperTestCase, self).setUp()

        # catch warning about lack of setup script support on non-YARN Spark
        self.log = self.start(patch('mrjob.bin.log'))

    def _get_python_wrapper_content(self, job_class, args):
        job = job_class(['-r', 'hadoop'] + list(args))
        job.sandbox()

        with job.make_runner() as runner:
            runner._create_setup_wrapper_scripts()

            if runner._spark_python_wrapper_path:
                with open(runner._spark_python_wrapper_path) as f:
                    return f.read()
            else:
                return None

    def test_default(self):
        self.assertIsNone(self._get_python_wrapper_content(
            MRNullSpark, []))

    def test_basic_setup(self):
        content = self._get_python_wrapper_content(
            MRNullSpark, ['--setup', 'echo blarg'])
        self.assertIsNotNone(content)

        # should have shebang
        first_line = content.split('\n')[0]
        self.assertEqual(first_line, ('#!/bin/sh -ex'))
        self.assertFalse(self.log.warning.called)

        # should contain command
        self.assertIn('echo blarg', content)

        # should wrap python
        self.assertIn('%s "$@"' % PYTHON_BIN, content)

    def test_env_shebang(self):
        content = self._get_python_wrapper_content(
            MRNullSpark, ['--setup', 'echo blarg', '--sh-bin', 'zsh'])
        self.assertIsNotNone(content)

        first_line = content.split('\n')[0]
        self.assertEqual(first_line, ('#!/usr/bin/env zsh'))
        self.assertFalse(self.log.warning.called)

    def test_cut_off_second_sh_bin_arg(self):
        content = self._get_python_wrapper_content(
            MRNullSpark, ['--setup', 'echo blarg',
                          '--sh-bin', '/bin/sh -v -ex'])
        self.assertIsNotNone(content)

        first_line = content.split('\n')[0]
        self.assertEqual(first_line, ('#!/bin/sh -v'))
        self.assertTrue(self.log.warning.called)

    def test_no_args_for_env_shebang(self):
        content = self._get_python_wrapper_content(
            MRNullSpark, ['--setup', 'echo blarg', '--sh-bin', 'bash -v'])
        self.assertIsNotNone(content)

        first_line = content.split('\n')[0]
        self.assertEqual(first_line, ('#!/usr/bin/env bash'))
        self.assertTrue(self.log.warning.called)

    def test_file_interpolation(self):
        makefile_path = self.makefile('Makefile')

        content = self._get_python_wrapper_content(
            MRNullSpark, ['--setup', 'make -f %s#' % makefile_path])

        self.assertIn('make -f $__mrjob_PWD/Makefile', content)

    def test_no_py_files_in_setup(self):
        # no need to add py_files to setup because Spark has --py-files
        py_file = self.makefile('alexandria.zip')

        content = self._get_python_wrapper_content(
            MRNullSpark, ['--setup', 'echo blarg',
                          '--py-files', py_file])

        self.assertIn('echo blarg', content)
        self.assertNotIn('alexandria', content)

    def test_spark_jar(self):
        jar_path = self.makefile('dora.jar')

        # no spark python wrapper because no python
        self.assertIsNone(self._get_python_wrapper_content(
            MRSparkJar, ['--jar', jar_path, '--setup', 'true']))

    def test_spark_script(self):
        script_path = self.makefile('ml.py')

        self.assertIsNotNone(self._get_python_wrapper_content(
            MRSparkScript, ['--script', script_path, '--setup', 'true']))

    def test_streaming_job(self):
        # no spark python wrapper because no spark
        self.assertIsNone(self._get_python_wrapper_content(
            MRTwoStepJob, ['--setup', 'true']))

    def test_jar_job(self):
        # no spark or python
        jar_path = self.makefile('dora.jar')

        self.assertIsNone(self._get_python_wrapper_content(
            MRJustAJar, ['--jar', jar_path, '--setup', 'true']))


class ShBinValidationTestCase(SandboxedTestCase):

    def setUp(self):
        super(ShBinValidationTestCase, self).setUp()

        self.log = self.start(patch('mrjob.bin.log'))

    def test_empty_sh_bin_means_default(self):
        runner = MRJobBinRunner(sh_bin=[])
        self.assertFalse(self.log.warning.called)

        default_sh_bin = MRJobBinRunner()._sh_bin()
        self.assertEqual(runner._sh_bin(), default_sh_bin)

    def test_absolute_sh_bin(self):
        MRJobBinRunner(sh_bin=['/bin/zsh'])

        self.assertFalse(self.log.warning.called)

    def test_absolute_sh_bin_with_one_arg(self):
        MRJobBinRunner(sh_bin=['/bin/zsh', '-v'])

        self.assertFalse(self.log.warning.called)

    def test_absolute_sh_bin_with_two_args(self):
        MRJobBinRunner(sh_bin=['/bin/zsh', '-v', '-x'])

        self.assertTrue(self.log.warning.called)

    def test_relative_sh_bin(self):
        MRJobBinRunner(sh_bin=['zsh'])

        self.assertFalse(self.log.warning.called)

    def test_relative_sh_bin_with_one_arg(self):
        MRJobBinRunner(sh_bin=['zsh', '-v'])

        self.assertTrue(self.log.warning.called)


class GetSparkSubmitBinTestCase(SandboxedTestCase):

    def setUp(self):
        super(GetSparkSubmitBinTestCase, self).setUp()

        self.find_spark_submit_bin = self.start(patch(
            'mrjob.bin.MRJobBinRunner._find_spark_submit_bin'))

    def test_do_nothing_on_init(self):
        MRJobBinRunner()
        self.assertFalse(self.find_spark_submit_bin.called)

    def test_default(self):
        runner = MRJobBinRunner()
        self.assertEqual(runner.get_spark_submit_bin(),
                         self.find_spark_submit_bin.return_value)

    def test_only_find_spark_submit_bin_once(self):
        runner = MRJobBinRunner()
        runner.get_spark_submit_bin()
        runner.get_spark_submit_bin()

        self.assertEqual(self.find_spark_submit_bin.call_count, 1)

    def test_option_short_circuits_find(self):
        runner = MRJobBinRunner(
            spark_submit_bin=['/path/to/spark-submit'])

        self.assertEqual(runner.get_spark_submit_bin(),
                         ['/path/to/spark-submit'])
        self.assertFalse(self.find_spark_submit_bin.called)

    def test_empty_string_submit_bin_means_default(self):
        job = MRNullSpark(['-r', 'local',
                           '--spark-submit-bin', ''])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner.get_spark_submit_bin(),
                             self.find_spark_submit_bin.return_value)


class FindSparkSubmitBinTestCase(SandboxedTestCase):

    def setUp(self):
        super(FindSparkSubmitBinTestCase, self).setUp()

        # track calls to which()
        self.which = self.start(patch('mrjob.bin.which', wraps=which))

        # keep which() from searching in /bin, etc.
        os.environ['PATH'] = self.tmp_dir

        # ignore whatever $SPARK_HOME might be set
        if 'SPARK_HOME' in os.environ:
            del os.environ['SPARK_HOME']

        # assume pyspark is not installed
        self.start(patch('mrjob.bin.pyspark', None))

        self.runner = MRJobBinRunner()

    def test_fallback_and_hard_coded_dirs(self):
        # don't get caught by real spark install
        self.which.return_value = None

        os.environ['SPARK_HOME'] = '/path/to/spark/home'

        pyspark = self.start(patch('mrjob.bin.pyspark'))
        pyspark.__file__ = '/path/to/site-packages/pyspark/__init__.pyc'

        self.assertEqual(self.runner._find_spark_submit_bin(),
                         ['spark-submit'])

        which_paths = [
            kwargs.get('path') for args, kwargs in self.which.call_args_list]

        self.assertEqual(which_paths, [
            '/path/to/spark/home/bin',
            None,
            '/path/to/site-packages/pyspark/bin',
            '/usr/lib/spark/bin',
            '/usr/local/spark/bin',
            '/usr/local/lib/spark/bin',
        ])

    def test_find_in_spark_home(self):
        spark_submit_bin = self.makefile(
            join(self.tmp_dir, 'spark', 'bin', 'spark-submit'),
            executable=True)

        os.environ['SPARK_HOME'] = join(self.tmp_dir, 'spark')

        self.assertEqual(self.runner._find_spark_submit_bin(),
                         [spark_submit_bin])

        self.assertEqual(self.which.call_count, 1)

    def test_find_in_path(self):
        spark_submit_bin = self.makefile(
            join(self.tmp_dir, 'bin', 'spark-submit'),
            executable=True)

        os.environ['PATH'] = join(self.tmp_dir, 'bin')

        self.assertEqual(self.runner._find_spark_submit_bin(),
                         [spark_submit_bin])

    def test_find_in_pyspark_installation(self):
        pyspark_dir = self.makedirs(join('site-packages', 'pyspark'))

        pyspark_init_py = self.makefile(
            join(pyspark_dir, '__init__.py'))

        spark_submit_bin = self.makefile(
            join(pyspark_dir, 'bin', 'spark-submit'),
            executable=True)

        pyspark = self.start(patch('mrjob.bin.pyspark'))
        pyspark.__file__ = pyspark_init_py

        self.assertEqual(self.runner._find_spark_submit_bin(),
                         [spark_submit_bin])


@skipIf(pyspark is None, 'no pyspark module')
@skipIf(not (pty and hasattr(pty, 'fork')), 'no pty.fork()')
class BadSparkSubmitAfterFork(SandboxedTestCase):

    def test_no_such_file(self):
        missing_spark_submit = os.path.join(self.tmp_dir, 'dont-spark-submit')

        job = MRSparkWordcount([
            '-r', 'local', '--spark-submit-bin', missing_spark_submit])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertRaises(StepFailedException, runner.run)

    def test_permissions_error(self):
        nonexecutable_spark_submit = os.path.join(self.tmp_dir,
                                                  'cant-spark-submit')
        job = MRSparkWordcount([
            '-r', 'local', '--spark-submit-bin', nonexecutable_spark_submit])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertRaises(StepFailedException, runner.run)

    def test_non_oserror_exception(self):
        self.start(patch('os.execvpe', side_effect=KeyboardInterrupt))

        job = MRSparkWordcount(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertRaises(StepFailedException, runner.run)
