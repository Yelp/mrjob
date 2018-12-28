# Copyright 2009-2017 Yelp
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
"""Test the runner base class MRJobBinRunner"""
import compileall
import inspect
import os
import signal
import stat
import sys
from io import BytesIO
from shutil import make_archive
from zipfile import ZipFile
from zipfile import ZIP_DEFLATED

from mrjob.bin import MRJobBinRunner
from mrjob.local import LocalMRJobRunner
from mrjob.py2 import PY2
from mrjob.step import GENERIC_ARGS
from mrjob.step import INPUT
from mrjob.step import OUTPUT
from mrjob.util import cmd_line

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
if PY2:
    PYTHON_BIN = 'python'
else:
    PYTHON_BIN = 'python3'


def _mock_upload_mgr():
    def mock_uri(path):
        return '<uri of %s>' % path

    m = Mock()
    m.uri = Mock(side_effect=mock_uri)

    return m


class AllowSparkOnLocalRunnerTestCase(SandboxedTestCase):

    # allow testing spark methods on local runner, even though
    # it doesn't (currently) support them (see #1361)

    def setUp(self):
        super(AllowSparkOnLocalRunnerTestCase, self).setUp()

        self.start(patch('mrjob.runner.MRJobRunner._check_steps'))



class ArgsForSparkStepTestCase(AllowSparkOnLocalRunnerTestCase):
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
            self.mock_spark_script_args.assert_called_once_with(step_num)

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

    def test_interpreter(self):
        runner = MRJobBinRunner(conf_paths=[], interpreter=['ruby'])
        self.assertEqual(runner._bootstrap_mrjob(), False)

    def test_bootstrap_mrjob_overrides_interpreter(self):
        runner = MRJobBinRunner(
            conf_paths=[], interpreter=['ruby'], bootstrap_mrjob=True)
        self.assertEqual(runner._bootstrap_mrjob(), True)


class GetSparkSubmitBinTestCase(AllowSparkOnLocalRunnerTestCase):

    def test_default(self):
        job = MRNullSpark(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner.get_spark_submit_bin(),
                             ['spark-submit'])

    def test_spark_submit_bin_option(self):
        job = MRNullSpark(['-r', 'local',
                           '--spark-submit-bin', 'spork-submit -kfc'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner.get_spark_submit_bin(),
                             ['spork-submit', '-kfc'])


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


class InterpreterTestCase(BasicTestCase):

    def test_default(self):
        runner = MRJobBinRunner()
        self.assertEqual(runner._python_bin(), [PYTHON_BIN])
        self.assertEqual(runner._interpreter(), [PYTHON_BIN])
        self.assertEqual(runner._interpreter(steps=True),
                         [sys.executable])

    def test_python_bin(self):
        runner = MRJobBinRunner(python_bin=['python', '-v'])
        self.assertEqual(runner._python_bin(), ['python', '-v'])
        self.assertEqual(runner._interpreter(), ['python', '-v'])
        self.assertEqual(runner._interpreter(steps=True), [sys.executable])

    def test_steps_python_bin(self):
        runner = MRJobBinRunner(steps_python_bin=['python', '-v'])
        self.assertEqual(runner._python_bin(), [PYTHON_BIN])
        self.assertEqual(runner._interpreter(), [PYTHON_BIN])
        self.assertEqual(runner._interpreter(steps=True), ['python', '-v'])

    def test_task_python_bin(self):
        runner = MRJobBinRunner(task_python_bin=['python', '-v'])
        self.assertEqual(runner._python_bin(), [PYTHON_BIN])
        self.assertEqual(runner._interpreter(), ['python', '-v'])
        self.assertEqual(runner._interpreter(steps=True),
                         [sys.executable])

    def test_interpreter(self):
        runner = MRJobBinRunner(interpreter=['ruby'])
        self.assertEqual(runner._interpreter(), ['ruby'])
        self.assertEqual(runner._interpreter(steps=True), ['ruby'])

    def test_steps_interpreter(self):
        # including whether steps_interpreter overrides interpreter
        runner = MRJobBinRunner(interpreter=['ruby', '-v'],
                                steps_interpreter=['ruby'])
        self.assertEqual(runner._interpreter(), ['ruby', '-v'])
        self.assertEqual(runner._interpreter(steps=True), ['ruby'])

    def test_interpreter_overrides_python_bin(self):
        runner = MRJobBinRunner(interpreter=['ruby'],
                                python_bin=['python', '-v'])
        self.assertEqual(runner._interpreter(), ['ruby'])
        self.assertEqual(runner._interpreter(steps=True), ['ruby'])

    def test_interpreter_overrides_steps_python_bin(self):
        runner = MRJobBinRunner(interpreter=['ruby'],
                                steps_python_bin=['python', '-v'])
        self.assertEqual(runner._interpreter(), ['ruby'])
        self.assertEqual(runner._interpreter(steps=True), ['ruby'])


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

    def test_respects_interpreter_method(self):
        # _interpreter() method is extensively tested; just
        # verify that we use it
        job = MRTwoStepJob(['-r', 'local'])
        job.sandbox()

        def mock_interpreter(steps=False):
            if steps:
                return [sys.executable]  # so _get_steps() works
            else:
                return ['run-my-task']

        self.start(patch('mrjob.bin.MRJobBinRunner._interpreter',
                         side_effect=mock_interpreter))

        with job.make_runner() as runner:
            self.assertEqual(
                runner._render_substep(0, 'mapper'),
                'run-my-task mr_two_step_job.py --step-num=0 --mapper'
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

        self.foo_py = os.path.join(self.tmp_dir, 'foo', 'foo.py')

        # if our job can import foo, getsize will return 2x as many bytes
        with open(self.foo_py, 'w') as foo_py:
            foo_py.write('import os.path\n'
                         'from os.path import getsize as _real_getsize\n'
                         'os.path.getsize = lambda p: _real_getsize(p) * 2')

        self.foo_sh = os.path.join(self.tmp_dir, 'foo', 'foo.sh')

        with open(self.foo_sh, 'w') as foo_sh:
            foo_sh.write('#!/bin/sh\n'
                         'touch foo.sh-made-this\n')
        os.chmod(self.foo_sh, stat.S_IRWXU)

        self.foo_tar_gz = make_archive(
            os.path.join(self.tmp_dir, 'foo'), 'gztar', self.foo_dir)

        self.foo_zip = os.path.join(self.tmp_dir, 'foo.zip')
        zf = ZipFile(self.foo_zip, 'w', ZIP_DEFLATED)
        zf.write(self.foo_py, 'foo.py')
        zf.close()

        self.foo_py_size = os.path.getsize(self.foo_py)
        self.foo_sh_size = os.path.getsize(self.foo_sh)
        self.foo_tar_gz_size = os.path.getsize(self.foo_tar_gz)
        self.foo_zip_size = os.path.getsize(self.foo_zip)

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
        self.assertEqual(path_to_size.get('./foo.tar.gz/foo.py'),
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
        self.assertEqual(path_to_size.get('./foo.tar.gz/foo.py'),
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
        bar_path = os.path.join(self.tmp_dir, 'bar')
        baz_path = os.path.join(self.tmp_dir, 'baz')

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
            self.assertTrue(os.path.exists(bar_path))
            self.assertFalse(os.path.exists(baz_path))

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


class PyFilesTestCase(AllowSparkOnLocalRunnerTestCase):

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

    def test_deprecated_py_file_switch(self):
        egg1_path = self.makefile('dragon.egg')

        job = MRNullSpark(['-r', 'local', '--no-bootstrap-mrjob',
                           '--py-file', egg1_path])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner._py_files(), [egg1_path])

    def test_no_bootstrap_mrjob(self):
        job = MRNullSpark(['-r', 'local',
                           '--no-bootstrap-mrjob'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner._py_files(),
                             [])

    def test_no_bootstrap_mrjob_in_py_files(self):
        job = MRNullSpark(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            # this happens in runners that run on a cluster
            runner._BOOTSTRAP_MRJOB_IN_PY_FILES = False
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


class SparkScriptPathTestCase(AllowSparkOnLocalRunnerTestCase):

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


class SparkScriptArgsTestCase(AllowSparkOnLocalRunnerTestCase):

    def setUp(self):
        super(SparkScriptArgsTestCase, self).setUp()

        # don't bother with actual input/output URIs, which
        # are tested elsewhere
        def mock_interpolate_step_args(args, step_num):
            def interpolate(arg):
                if arg == INPUT:
                    return '<step %d input>' % step_num
                elif arg == OUTPUT:
                    return '<step %d output>' % step_num
                else:
                    return arg

            return [interpolate(arg) for arg in args]

        self.start(patch(
            'mrjob.bin.MRJobBinRunner._interpolate_step_args',
            side_effect=mock_interpolate_step_args))

    def test_spark_mr_job(self):
        job = MRNullSpark(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_script_args(0),
                ['--step-num=0',
                 '--spark',
                 '<step 0 input>',
                 '<step 0 output>'])

    def test_spark_passthrough_arg(self):
        job = MRNullSpark(['-r', 'local', '--extra-spark-arg=--verbose'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_script_args(0),
                ['--step-num=0',
                 '--spark',
                 '--extra-spark-arg=--verbose',
                 '<step 0 input>',
                 '<step 0 output>'])

    def test_spark_file_arg(self):
        foo_path = self.makefile('foo')

        job = MRNullSpark(['-r', 'local', '--extra-file', foo_path])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_script_args(0),
                ['--step-num=0',
                 '--spark',
                 '--extra-file',
                 'foo',
                 '<step 0 input>',
                 '<step 0 output>'])

            name_to_path = runner._working_dir_mgr.name_to_path('file')
            self.assertIn('foo', name_to_path)
            self.assertEqual(name_to_path['foo'], foo_path)

    def test_spark_jar(self):
        job = MRSparkJar(['-r', 'local',
                          '--jar-arg', 'foo', '--jar-arg', 'bar'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_script_args(0),
                ['foo', 'bar'])

    def test_spark_jar_interpolation(self):
        job = MRSparkJar(['-r', 'local',
                          '--jar-arg', OUTPUT, '--jar-arg', INPUT])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_script_args(0),
                ['<step 0 output>', '<step 0 input>'])

    def test_spark_script(self):
        job = MRSparkScript(['-r', 'local',
                             '--script-arg', 'foo', '--script-arg', 'bar'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_script_args(0),
                ['foo', 'bar'])

    def test_spark_script_interpolation(self):
        job = MRSparkScript(['-r', 'local',
                             '--script-arg', OUTPUT, '--script-arg', INPUT])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_script_args(0),
                ['<step 0 output>', '<step 0 input>'])

    def test_generic_args_not_okay(self):
        # GENERIC_ARGS is only for JarSteps (they're Hadoop generic args)
        job = MRSparkScript(['-r', 'local',
                             '--script-arg', GENERIC_ARGS])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertRaises(
                ValueError,
                runner._spark_script_args, 0)

    def test_streaming_step_not_okay(self):
        job = MRTwoStepJob(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertRaises(
                TypeError,
                runner._spark_script_args, 0)


class SparkSubmitArgsTestCase(AllowSparkOnLocalRunnerTestCase):

    def setUp(self):
        super(SparkSubmitArgsTestCase, self).setUp()

        self.start(patch('mrjob.bin.MRJobBinRunner._python_bin',
                         return_value=['mypy']))

        # bootstrapping mrjob is tested below in SparkPyFilesTestCase
        self.start(patch('mrjob.bin.MRJobBinRunner._bootstrap_mrjob',
                         return_value=False))

    def _expected_conf_args(self, cmdenv=None, jobconf=None):
        conf = {}

        if cmdenv:
            for key, value in cmdenv.items():
                conf['spark.executorEnv.%s' % key] = value
                conf['spark.yarn.appMasterEnv.%s' % key] = value

        if jobconf:
            conf.update(jobconf)

        args = []

        for key, value in sorted(conf.items()):
            args.extend(['--conf', '%s=%s' % (key, value)])

        return args

    def test_default(self):
        job = MRNullSpark(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                self._expected_conf_args(
                    cmdenv=dict(PYSPARK_PYTHON='mypy')))

    def test_spark_master_and_deploy_mode(self):
        self.start(patch('mrjob.bin.MRJobBinRunner._spark_master',
                         return_value='yoda'))
        self.start(patch('mrjob.bin.MRJobBinRunner._spark_deploy_mode',
                         return_value='the-force'))

        job = MRNullSpark(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                ['--master', 'yoda', '--deploy-mode', 'the-force'] +
                self._expected_conf_args(
                    cmdenv=dict(PYSPARK_PYTHON='mypy')))

    def test_empty_string_spark_master_and_deploy_mode(self):
        self.start(patch('mrjob.bin.MRJobBinRunner._spark_master',
                         return_value=''))
        self.start(patch('mrjob.bin.MRJobBinRunner._spark_deploy_mode',
                         return_value=''))

        job = MRNullSpark(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                self._expected_conf_args(
                    cmdenv=dict(PYSPARK_PYTHON='mypy')))

    def test_cmdenv(self):
        job = MRNullSpark(['-r', 'local',
                           '--cmdenv', 'FOO=bar', '--cmdenv', 'BAZ=qux'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                self._expected_conf_args(
                    cmdenv=dict(PYSPARK_PYTHON='mypy', FOO='bar', BAZ='qux')))

    def test_cmdenv_can_override_python_bin(self):
        job = MRNullSpark(['-r', 'local', '--cmdenv', 'PYSPARK_PYTHON=ourpy'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                self._expected_conf_args(
                    cmdenv=dict(PYSPARK_PYTHON='ourpy')))

    def test_spark_cmdenv_method(self):
        # test that _spark_submit_args() uses _spark_cmdenv(),
        # so we can just test _spark_cmdenv() in other test cases
        hard_coded_env = dict(FOO='bar')

        self.start(patch('mrjob.local.LocalMRJobRunner._spark_cmdenv',
                         return_value=hard_coded_env, create=True))

        job = MRNullSpark(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                self._expected_conf_args(cmdenv=hard_coded_env))

    def test_jobconf(self):
        job = MRNullSpark(['-r', 'local',
                           '-D', 'spark.executor.memory=10g'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                self._expected_conf_args(
                    cmdenv=dict(PYSPARK_PYTHON='mypy'),
                    jobconf={'spark.executor.memory': '10g'}))

    def test_jobconf_uses_jobconf_for_step(self):
        job = MRNullSpark(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            with patch.object(
                    runner, '_jobconf_for_step',
                    return_value=dict(foo='bar')) as mock_jobconf_for_step:

                self.assertEqual(
                    runner._spark_submit_args(0),
                    self._expected_conf_args(
                        cmdenv=dict(PYSPARK_PYTHON='mypy'),
                        jobconf=dict(foo='bar')))

                mock_jobconf_for_step.assert_called_once_with(0)

    def test_jobconf_can_override_python_bin_and_cmdenv(self):
        job = MRNullSpark(
            ['-r', 'local',
             '--cmdenv', 'FOO=bar',
             '-D', 'spark.executorEnv.FOO=baz',
             '-D', 'spark.yarn.appMasterEnv.PYSPARK_PYTHON=ourpy'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                self._expected_conf_args(
                    jobconf={
                        'spark.executorEnv.FOO': 'baz',
                        'spark.executorEnv.PYSPARK_PYTHON': 'mypy',
                        'spark.yarn.appMasterEnv.FOO': 'bar',
                        'spark.yarn.appMasterEnv.PYSPARK_PYTHON': 'ourpy',
                    }
                )
            )

    def test_non_string_jobconf_values_in_mrjob_conf(self):
        # regression test for #323
        MRJOB_CONF = dict(runners=dict(local=dict(jobconf=dict(
            BAX=True,
            BAZ=False,
            FOO=None,
            QUX='null',
        ))))

        with mrjob_conf_patcher(MRJOB_CONF):
            job = MRNullSpark(['-r', 'local'])
            job.sandbox()

            with job.make_runner() as runner:
                # FOO is blanked out because it's None (use "null")
                self.assertEqual(
                    runner._spark_submit_args(0),
                    self._expected_conf_args(
                        cmdenv=dict(PYSPARK_PYTHON='mypy'),
                        jobconf=dict(
                            BAX='true',
                            BAZ='false',
                            QUX='null',
                        )
                    )
                )

    def test_libjars_option(self):
        fake_libjar = self.makefile('fake_lib.jar')

        job = MRNullSpark(
            ['-r', 'local', '--libjars', fake_libjar])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                ['--jars', fake_libjar] +
                self._expected_conf_args(
                    cmdenv=dict(PYSPARK_PYTHON='mypy')))

    def test_deprecated_libjar_switch(self):
        fake_libjar = self.makefile('fake_lib.jar')

        job = MRNullSpark(
            ['-r', 'local', '--libjar', fake_libjar])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                ['--jars', fake_libjar] +
                self._expected_conf_args(
                    cmdenv=dict(PYSPARK_PYTHON='mypy')))

    def test_libjar_paths_override(self):
        job = MRNullSpark(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            self.start(patch.object(
                runner, '_libjar_paths',
                return_value=['s3://a/a.jar', 's3://b/b.jar']))

            self.assertEqual(
                runner._spark_submit_args(0),
                ['--jars', 's3://a/a.jar,s3://b/b.jar'] +
                self._expected_conf_args(
                    cmdenv=dict(PYSPARK_PYTHON='mypy')))

    def test_option_spark_args(self):
        job = MRNullSpark(['-r', 'local',
                           '--spark-args=--name Dave'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0), (
                    self._expected_conf_args(
                        cmdenv=dict(PYSPARK_PYTHON='mypy')) +
                    ['--name', 'Dave']
                )
            )

    def test_deprecated_spark_arg_switch(self):
        job = MRNullSpark(['-r', 'local',
                           '--spark-arg=--name',
                           '--spark-arg=Dave'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0), (
                    self._expected_conf_args(
                        cmdenv=dict(PYSPARK_PYTHON='mypy')) +
                    ['--name', 'Dave']
                )
            )

    def test_job_spark_args(self):
        # --extra-spark-arg is a passthrough option for MRNullSpark
        job = MRNullSpark(['-r', 'local',
                           '--extra-spark-arg=-v'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0), (
                    self._expected_conf_args(
                        cmdenv=dict(PYSPARK_PYTHON='mypy')) +
                    ['-v']
                )
            )

    def test_job_spark_args_come_after_option_spark_args(self):
        job = MRNullSpark(
            ['-r', 'local',
             '--extra-spark-arg=-v',
             '--spark-args=--name Dave'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0), (
                    self._expected_conf_args(
                        cmdenv=dict(PYSPARK_PYTHON='mypy')) +
                    ['--name', 'Dave', '-v']
                )
            )

    def test_file_args(self):
        foo1_path = self.makefile('foo1')
        foo2_path = self.makefile('foo2')
        baz_path = self.makefile('baz.tar.gz')
        qux_path = self.makedirs('qux')

        job = MRNullSpark([
            '-r', 'local',
            '--files', '%s#foo1,%s#bar' % (foo1_path, foo2_path),
            '--archives', baz_path,
            '--dirs', qux_path,
        ])
        job.sandbox()

        with job.make_runner() as runner:
            runner._upload_mgr = _mock_upload_mgr()

            self.assertEqual(
                runner._spark_submit_args(0), (
                    self._expected_conf_args(
                        cmdenv=dict(PYSPARK_PYTHON='mypy')
                    ) + [
                        '--files',
                        (runner._upload_mgr.uri(foo1_path) + '#foo1' +
                         ',' +
                         runner._upload_mgr.uri(foo2_path) + '#bar'),
                        '--archives',
                        runner._upload_mgr.uri(baz_path) + '#baz.tar.gz' +
                        ',' +
                        runner._upload_mgr.uri(
                            runner._dir_archive_path(qux_path)) + '#qux'
                    ]
                )
            )

    def test_file_upload_args(self):
        qux_path = self.makefile('qux')

        job = MRNullSpark([
            '-r', 'local',
            '--extra-file', qux_path,  # file upload arg
        ])
        job.sandbox()

        with job.make_runner() as runner:
            runner._upload_mgr = _mock_upload_mgr()

            self.assertEqual(
                runner._spark_submit_args(0), (
                    self._expected_conf_args(
                        cmdenv=dict(PYSPARK_PYTHON='mypy')
                    ) + [
                        '--files',
                        runner._upload_mgr.uri(qux_path) + '#qux'
                    ]
                )
            )

    def test_override_spark_upload_args(self):
        # just confirm that _spark_submit_args() uses _spark_upload_args()
        self.start(patch('mrjob.bin.MRJobBinRunner._spark_upload_args',
                         return_value=['--files', 'foo,bar']))

        job = MRNullSpark(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0), (
                    self._expected_conf_args(
                        cmdenv=dict(PYSPARK_PYTHON='mypy')
                    ) + [
                        '--files', 'foo,bar',
                    ]
                )
            )

    def test_py_files(self):
        job = MRNullSpark(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._py_files = Mock(
                return_value=['<first py_file>', '<second py_file>']
            )

            self.assertEqual(
                runner._spark_submit_args(0), (
                    self._expected_conf_args(
                        cmdenv=dict(PYSPARK_PYTHON='mypy')
                    ) + [
                        '--py-files',
                        '<first py_file>,<second py_file>'
                    ]
                )
            )

    def test_spark_jar_step(self):
        job = MRSparkJar(['-r', 'local',
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
                runner._spark_submit_args(0), (
                    ['--class', 'foo.Bar'] +
                    self._expected_conf_args(
                        cmdenv=dict(BAZ='qux'),
                        jobconf=dict(QUX='baz')
                    )
                )
            )

    def test_spark_script_step(self):
        job = MRSparkScript(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                self._expected_conf_args(
                    cmdenv=dict(PYSPARK_PYTHON='mypy')))

    def test_streaming_step_not_allowed(self):
        job = MRTwoStepJob(['-r', 'local'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertRaises(
                TypeError,
                runner._spark_submit_args, 0)


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
            compileall.compile_dir(os.path.join(self.tmp_dir, 'mrjob'),
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

        # sanity-check master and deploy mode
        self.assertEqual(runner._spark_master(), 'yarn')
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

    def test_setup_wrapper_requires_yarn(self):
        env, runner = self._env_and_runner(
            '--setup', 'true', '--spark-master', 'local')

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
        job = MRNullSpark(['-r', 'hadoop', '--file', 'foo'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._upload_mgr = _mock_upload_mgr()
            runner._create_setup_wrapper_scripts()

            upload_args = cmd_line(runner._spark_upload_args())

            self.assertIn('#foo', upload_args)

    def test_setup_interpolation(self):
        job = MRNullSpark(
            ['-r', 'hadoop',
             '--setup', 'make -f Makefile#',
             '--file', 'foo',
             ])
        job.sandbox()

        with job.make_runner() as runner:
            runner._upload_mgr = _mock_upload_mgr()
            runner._create_setup_wrapper_scripts()

            upload_args = cmd_line(runner._spark_upload_args())

            self.assertIn('#Makefile', upload_args)
            self.assertIn('#python-wrapper.sh', upload_args)
            self.assertIn('#foo', upload_args)

            self.assertFalse(self.log.warning.called)

    def test_setup_disabled(self):
        job = MRNullSpark(
            ['-r', 'hadoop',
             '--setup', 'make -f Makefile#',
             '--file', 'foo',
             '--spark-master', 'local',
             ])
        job.sandbox()

        with job.make_runner() as runner:
            runner._upload_mgr = _mock_upload_mgr()
            runner._create_setup_wrapper_scripts()

            upload_args = cmd_line(runner._spark_upload_args())

            self.assertIn('#foo', upload_args)

            self.assertNotIn('#Makefile', upload_args)
            self.assertNotIn('#python-wrapper.sh', upload_args)

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
        self.assertTrue(first_line.startswith('#!'))
        self.assertIn('sh -ex', first_line)

        # should contain command
        self.assertIn('echo blarg', content)

        # should wrap python
        self.assertIn('%s "$@"' % PYTHON_BIN, content)

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

    def test_empty_sh_bin(self):
        self.assertRaises(ValueError, MRJobBinRunner, sh_bin=[])

    def test_absolute_sh_bin(self):
        runner = MRJobBinRunner(sh_bin=['/bin/zsh'])

        self.assertFalse(self.log.warning.called)

    def test_absolute_sh_bin_with_args(self):
        runner = MRJobBinRunner(sh_bin=['/bin/zsh', '-v'])

        self.assertFalse(self.log.warning.called)

    def test_relative_sh_bin(self):
        runner = MRJobBinRunner(sh_bin=['zsh'])

        self.assertFalse(self.log.warning.called)

    def test_relative_sh_bin_with_args(self):
        runner = MRJobBinRunner(sh_bin=['zsh', '-v'])

        self.assertTrue(self.log.warning.called)
