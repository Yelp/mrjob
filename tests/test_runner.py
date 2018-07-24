# Copyright 2009-2017 Yelp
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
"""Test the runner base class MRJobRunner"""
import compileall
import datetime
import getpass
import inspect
import os
import os.path
import shutil
import signal
import stat
import sys
import tarfile
import tempfile
from io import BytesIO
from subprocess import CalledProcessError
from time import sleep
from unittest import TestCase
from zipfile import ZipFile
from zipfile import ZIP_DEFLATED

from mrjob.emr import EMRJobRunner
from mrjob.fs.s3 import S3Filesystem
from mrjob.hadoop import HadoopJobRunner
from mrjob.inline import InlineMRJobRunner
from mrjob.job import MRJob
from mrjob.local import LocalMRJobRunner
from mrjob.py2 import PY2
from mrjob.py2 import StringIO
from mrjob.runner import MRJobRunner
from mrjob.step import INPUT
from mrjob.step import OUTPUT
from mrjob.tools.emr.audit_usage import _JOB_KEY_RE
from mrjob.util import log_to_stream
from mrjob.util import tar_and_gzip

from tests.mockboto import MockBotoTestCase
from tests.mr_cmd_job import MRCmdJob
from tests.mr_counting_job import MRCountingJob
from tests.mr_filter_job import MRFilterJob
from tests.mr_no_mapper import MRNoMapper
from tests.mr_no_runner import MRNoRunner
from tests.mr_null_spark import MRNullSpark
from tests.mr_os_walk_job import MROSWalkJob
from tests.mr_sort_values import MRSortValues
from tests.mr_sort_values_and_more import MRSortValuesAndMore
from tests.mr_spark_jar import MRSparkJar
from tests.mr_spark_script import MRSparkScript
from tests.mr_two_step_job import MRTwoStepJob
from tests.mr_word_count import MRWordCount
from tests.py2 import Mock
from tests.py2 import patch
from tests.quiet import no_handlers_for_logger
from tests.sandbox import EmptyMrjobConfTestCase
from tests.sandbox import SandboxedTestCase
from tests.sandbox import mrjob_conf_patcher

# used to match command lines
if PY2:
    PYTHON_BIN = 'python'
else:
    PYTHON_BIN = 'python3'


class WithStatementTestCase(TestCase):

    def setUp(self):
        self.local_tmp_dir = None

    def tearDown(self):
        if self.local_tmp_dir:
            shutil.rmtree(self.local_tmp_dir)
            self.local_tmp_dir = None

    def _test_cleanup_after_with_statement(self, mode, should_exist):
        with InlineMRJobRunner(cleanup=mode, conf_paths=[]) as runner:
            self.local_tmp_dir = runner._get_local_tmp_dir()
            self.assertTrue(os.path.exists(self.local_tmp_dir))

        self.assertEqual(os.path.exists(self.local_tmp_dir), should_exist)
        if not should_exist:
            self.local_tmp_dir = None

    def test_cleanup_all(self):
        self._test_cleanup_after_with_statement(['ALL'], False)

    def test_cleanup_tmp(self):
        self._test_cleanup_after_with_statement(['TMP'], False)

    def test_cleanup_local_tmp(self):
        self._test_cleanup_after_with_statement(['LOCAL_TMP'], False)

    def test_cleanup_cloud_tmp(self):
        self._test_cleanup_after_with_statement(['CLOUD_TMP'], True)

    def test_cleanup_none(self):
        self._test_cleanup_after_with_statement(['NONE'], True)

    def test_cleanup_error(self):
        self.assertRaises(ValueError, self._test_cleanup_after_with_statement,
                          ['NONE', 'ALL'], True)
        self.assertRaises(ValueError, self._test_cleanup_after_with_statement,
                          ['GARBAGE'], True)

    def test_double_none_okay(self):
        self._test_cleanup_after_with_statement(['NONE', 'NONE'], True)


class TestJobName(TestCase):

    def setUp(self):
        self.blank_out_environment()
        self.monkey_patch_getuser()

    def tearDown(self):
        self.restore_getuser()
        self.restore_environment()

    def blank_out_environment(self):
        self._old_environ = os.environ.copy()
        # don't do os.environ = {}! This won't actually set environment
        # variables; it just monkey-patches os.environ
        os.environ.clear()

    def restore_environment(self):
        os.environ.clear()
        os.environ.update(self._old_environ)

    def monkey_patch_getuser(self):
        self._real_getuser = getpass.getuser
        self.getuser_should_fail = False

        def fake_getuser():
            if self.getuser_should_fail:
                raise Exception('fake getuser() was instructed to fail')
            else:
                return self._real_getuser()

        getpass.getuser = fake_getuser

    def restore_getuser(self):
        getpass.getuser = self._real_getuser

    def test_empty(self):
        runner = InlineMRJobRunner(conf_paths=[])
        match = _JOB_KEY_RE.match(runner.get_job_key())

        self.assertEqual(match.group(1), 'no_script')
        self.assertEqual(match.group(2), getpass.getuser())

    def test_empty_no_user(self):
        self.getuser_should_fail = True
        runner = InlineMRJobRunner(conf_paths=[])
        match = _JOB_KEY_RE.match(runner.get_job_key())

        self.assertEqual(match.group(1), 'no_script')
        self.assertEqual(match.group(2), 'no_user')

    def test_auto_label(self):
        runner = MRTwoStepJob(['--no-conf']).make_runner()
        match = _JOB_KEY_RE.match(runner.get_job_key())

        self.assertEqual(match.group(1), 'mr_two_step_job')
        self.assertEqual(match.group(2), getpass.getuser())

    def test_auto_owner(self):
        os.environ['USER'] = 'mcp'
        runner = InlineMRJobRunner(conf_paths=[])
        match = _JOB_KEY_RE.match(runner.get_job_key())

        self.assertEqual(match.group(1), 'no_script')
        self.assertEqual(match.group(2), 'mcp')

    def test_auto_everything(self):
        test_start = datetime.datetime.utcnow()

        os.environ['USER'] = 'mcp'
        runner = MRTwoStepJob(['--no-conf']).make_runner()
        match = _JOB_KEY_RE.match(runner.get_job_key())

        self.assertEqual(match.group(1), 'mr_two_step_job')
        self.assertEqual(match.group(2), 'mcp')

        job_start = datetime.datetime.strptime(
            match.group(3) + match.group(4), '%Y%m%d%H%M%S')
        job_start = job_start.replace(microsecond=int(match.group(5)))
        self.assertGreaterEqual(job_start, test_start)
        self.assertLessEqual(job_start - test_start,
                             datetime.timedelta(seconds=5))

    def test_owner_and_label_switches(self):
        runner_opts = ['--no-conf', '--owner=ads', '--label=ads_chain']
        runner = MRTwoStepJob(runner_opts).make_runner()
        match = _JOB_KEY_RE.match(runner.get_job_key())

        self.assertEqual(match.group(1), 'ads_chain')
        self.assertEqual(match.group(2), 'ads')

    def test_owner_and_label_kwargs(self):
        runner = InlineMRJobRunner(conf_paths=[],
                                   owner='ads', label='ads_chain')
        match = _JOB_KEY_RE.match(runner.get_job_key())

        self.assertEqual(match.group(1), 'ads_chain')
        self.assertEqual(match.group(2), 'ads')


class CreateMrjobZipTestCase(SandboxedTestCase):

    def test_create_mrjob_zip(self):
        with no_handlers_for_logger('mrjob.runner'):
            with InlineMRJobRunner(conf_paths=[]) as runner:
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
        runner = InlineMRJobRunner()
        with no_handlers_for_logger('mrjob.runner'):
            mrjob_zip = runner._create_mrjob_zip()

        ZipFile(mrjob_zip).extractall(self.tmp_dir)

        self.assertTrue(
            compileall.compile_dir(os.path.join(self.tmp_dir, 'mrjob'),
                                   quiet=1))


class TestStreamingOutput(TestCase):

    def setUp(self):
        self.make_tmp_dir()

    def tearDown(self):
        self.rm_tmp_dir()

    def make_tmp_dir(self):
        # use a leading underscore to test behavior of underscore-ignoring
        # code that shouldn't ignore the entire output_dir
        self.tmp_dir = tempfile.mkdtemp(prefix='_streamingtest')

    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    # Test regression for #269
    def test_stream_output(self):
        a_dir_path = os.path.join(self.tmp_dir, 'a')
        b_dir_path = os.path.join(self.tmp_dir, 'b')
        l_dir_path = os.path.join(self.tmp_dir, '_logs')
        os.mkdir(a_dir_path)
        os.mkdir(b_dir_path)
        os.mkdir(l_dir_path)

        a_file_path = os.path.join(a_dir_path, 'part-00000')
        b_file_path = os.path.join(b_dir_path, 'part-00001')
        c_file_path = os.path.join(self.tmp_dir, 'part-00002')
        x_file_path = os.path.join(l_dir_path, 'log.xml')
        y_file_path = os.path.join(self.tmp_dir, '_SUCCESS')

        with open(a_file_path, 'w') as f:
            f.write('A')

        with open(b_file_path, 'w') as f:
            f.write('B')

        with open(c_file_path, 'w') as f:
            f.write('C')

        with open(x_file_path, 'w') as f:
            f.write('<XML XML XML/>')

        with open(y_file_path, 'w') as f:
            f.write('I win')

        runner = InlineMRJobRunner(conf_paths=[], output_dir=self.tmp_dir)
        self.assertEqual(sorted(runner.stream_output()),
                         [b'A', b'B', b'C'])


class TestInvokeSort(TestCase):

    def setUp(self):
        self.make_tmp_dir_and_set_up_files()
        self.save_environment()

    def tearDown(self):
        self.restore_environment()
        self.rm_tmp_dir()

    def make_tmp_dir_and_set_up_files(self):
        self.tmp_dir = tempfile.mkdtemp()

        self.a = os.path.join(self.tmp_dir, 'a')
        with open(self.a, 'w') as a:
            a.write('A\n')
            a.write('apple\n')
            a.write('alligator\n')

        self.b = os.path.join(self.tmp_dir, 'b')
        with open(self.b, 'w') as b:
            b.write('B\n')
            b.write('banana\n')
            b.write('ball\n')

        self.out = os.path.join(self.tmp_dir, 'out')

    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def save_environment(self):
        self._old_environ = os.environ.copy()

    def restore_environment(self):
        os.environ.clear()
        os.environ.update(self._old_environ)

    def find_real_sort_bin(self):
        for path in os.environ.get('PATH', '').split(os.pathsep) or ():
            for sort_path in [os.path.join(path, 'sort'),
                              os.path.join(path, 'sort.exe')]:
                if os.path.exists(sort_path):
                    return os.path.abspath(sort_path)

        raise Exception("Can't find sort binary!")

    def use_alternate_sort(self, script_contents):
        sort_bin = os.path.join(self.tmp_dir, 'sort')
        with open(sort_bin, 'w') as f:
            f.write('#!%s\n' % sys.executable)
            f.write(script_contents)

        os.chmod(sort_bin, stat.S_IREAD | stat.S_IEXEC)
        os.environ['PATH'] = self.tmp_dir

    def use_simulated_windows_sort(self):
        script_contents = """\
import os
from subprocess import check_call
import sys

if len(sys.argv) > 2:
    print >> sys.stderr, 'Input file specified two times.'
    sys.exit(1)

real_sort_bin = %r

check_call([real_sort_bin] + sys.argv[1:])
""" % (self.find_real_sort_bin())

        self.use_alternate_sort(script_contents)

    def use_bad_sort(self):
        script_contents = """\
import sys

print >> sys.stderr, 'Sorting is for chumps!'
sys.exit(13)
"""

        self.use_alternate_sort(script_contents)

    def environment_variable_checks(self, runner, environment_check_list):
        environment_vars = {}

        def check_call_se(*args, **kwargs):
            for key in kwargs['env'].keys():
                environment_vars[key] = kwargs['env'][key]

        with patch('mrjob.runner.check_call', side_effect=check_call_se):
            runner._invoke_sort([self.a], self.out)
            for key in environment_check_list:
                self.assertEqual(environment_vars.get(key, None),
                                 runner._opts['local_tmp_dir'])

    def test_no_files(self):
        runner = MRJobRunner(conf_paths=[])
        self.assertRaises(ValueError,
                          runner._invoke_sort, [], self.out)

    def test_one_file(self):
        runner = MRJobRunner(conf_paths=[])
        self.addCleanup(runner.cleanup)

        runner._invoke_sort([self.a], self.out)

        with open(self.out) as out_f:
            self.assertEqual(list(out_f),
                             ['A\n',
                              'alligator\n',
                              'apple\n'])

    def test_two_files(self):
        runner = MRJobRunner(conf_paths=[])
        self.addCleanup(runner.cleanup)

        runner._invoke_sort([self.a, self.b], self.out)

        with open(self.out) as out_f:
            self.assertEqual(list(out_f),
                             ['A\n',
                              'B\n',
                              'alligator\n',
                              'apple\n',
                              'ball\n',
                              'banana\n'])

    def test_windows_sort_on_one_file(self):
        self.use_simulated_windows_sort()
        self.test_one_file()

    def test_windows_sort_on_two_files(self):
        self.use_simulated_windows_sort()
        self.test_two_files()

    def test_bad_sort(self):
        self.use_bad_sort()

        runner = MRJobRunner(conf_paths=[])
        self.addCleanup(runner.cleanup)

        with no_handlers_for_logger():
            # sometimes we get a broken pipe error (IOError) on PyPy
            self.assertRaises((CalledProcessError, IOError),
                              runner._invoke_sort, [self.a, self.b], self.out)

    def test_environment_variables_non_windows(self):
        runner = MRJobRunner(conf_paths=[])
        self.addCleanup(runner.cleanup)

        self.environment_variable_checks(runner, ['TEMP', 'TMPDIR'])

    def test_environment_variables_windows(self):
        runner = MRJobRunner(conf_paths=[])
        self.addCleanup(runner.cleanup)

        runner._sort_is_windows_sort = True
        self.environment_variable_checks(runner, ['TMP'])


class HadoopArgsForStepTestCase(EmptyMrjobConfTestCase):

    # hadoop_extra_args is tested in tests.test_hadoop.HadoopExtraArgsTestCase

    def test_empty(self):
        job = MRWordCount()
        with job.make_runner() as runner:
            self.assertEqual(runner._hadoop_args_for_step(0), [])

    def test_cmdenv(self):
        job = MRWordCount(['--cmdenv', 'FOO=bar',
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
        job1 = MRWordCount()
        # no cmd-line argument for this because it's part of job semantics
        job1.HADOOP_INPUT_FORMAT = input_format
        with job1.make_runner() as runner1:
            self.assertEqual(runner1._hadoop_args_for_step(0),
                             ['-inputformat', input_format])

        # multi-step job: only use -inputformat on the first step
        job2 = MRTwoStepJob()
        job2.HADOOP_INPUT_FORMAT = input_format
        with job2.make_runner() as runner2:
            self.assertEqual(runner2._hadoop_args_for_step(0),
                             ['-inputformat', input_format])
            self.assertEqual(runner2._hadoop_args_for_step(1), [])

    def test_hadoop_output_format(self):
        output_format = 'org.apache.hadoop.mapred.SequenceFileOutputFormat'

        # one-step job
        job1 = MRWordCount()
        # no cmd-line argument for this because it's part of job semantics
        job1.HADOOP_OUTPUT_FORMAT = output_format
        with job1.make_runner() as runner1:
            self.assertEqual(runner1._hadoop_args_for_step(0),
                             ['-outputformat', output_format])

        # multi-step job: only use -outputformat on the last step
        job2 = MRTwoStepJob()
        job2.HADOOP_OUTPUT_FORMAT = output_format
        with job2.make_runner() as runner2:
            self.assertEqual(runner2._hadoop_args_for_step(0), [])
            self.assertEqual(runner2._hadoop_args_for_step(1),
                             ['-outputformat', output_format])

    def test_jobconf(self):
        jobconf_args = ['--jobconf', 'FOO=bar',
                        '--jobconf', 'BAZ=qux',
                        '--jobconf', 'BAX=Arnold']

        job = MRWordCount(jobconf_args)
        with job.make_runner() as runner:
            self.assertEqual(runner._hadoop_args_for_step(0),
                             ['-D', 'BAX=Arnold',
                              '-D', 'BAZ=qux',
                              '-D', 'FOO=bar',
                              ])

    def test_empty_jobconf_values(self):
        # value of None means to omit that jobconf
        job = MRWordCount()
        # no way to pass in None from the command line
        job.JOBCONF = {'foo': '', 'bar': None}

        with job.make_runner() as runner:
            self.assertEqual(runner._hadoop_args_for_step(0),
                             ['-D', 'foo='])

    def test_configuration_translation(self):
        job = MRWordCount(
            ['--jobconf', 'mapred.jobtracker.maxtasks.per.job=1'])

        with job.make_runner() as runner:
            with no_handlers_for_logger('mrjob.runner'):
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

    def test_partitioner(self):
        partitioner = 'org.apache.hadoop.mapreduce.Partitioner'
        job = MRWordCount(['--partitioner', partitioner])

        with job.make_runner() as runner:
            self.assertEqual(runner._hadoop_args_for_step(0),
                             ['-partitioner', partitioner])


class ArgsForSparkStepTestCase(SandboxedTestCase):
    # just test the structure of _args_for_spark_step()

    def setUp(self):
        self.mock_get_spark_submit_bin = self.start(patch(
            'mrjob.runner.MRJobRunner.get_spark_submit_bin',
            return_value=['<spark-submit bin>']))

        self.mock_spark_submit_args = self.start(patch(
            'mrjob.runner.MRJobRunner._spark_submit_args',
            return_value=['<spark submit args>']))

        self.mock_spark_script_path = self.start(patch(
            'mrjob.runner.MRJobRunner._spark_script_path',
            return_value='<spark script path>'))

        self.mock_spark_script_args = self.start(patch(
            'mrjob.runner.MRJobRunner._spark_script_args',
            return_value=['<spark script args>']))

    def _test_step(self, step_num):
        job = MRNullSpark()
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


class GetSparkSubmitBinTestCase(SandboxedTestCase):

    def test_default(self):
        job = MRNullSpark()
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner.get_spark_submit_bin(),
                             ['spark-submit'])

    def test_spark_submit_bin_option(self):
        job = MRNullSpark(['--spark-submit-bin', 'spork-submit -kfc'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner.get_spark_submit_bin(),
                             ['spork-submit', '-kfc'])


class SparkSubmitArgsTestCase(SandboxedTestCase):

    def setUp(self):
        super(SparkSubmitArgsTestCase, self).setUp()

        self.start(patch('mrjob.runner.MRJobRunner._python_bin',
                         return_value=['mypy']))

        # bootstrapping mrjob is tested below in SparkPyFilesTestCase
        self.start(patch('mrjob.runner.MRJobRunner._bootstrap_mrjob',
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
        job = MRNullSpark()
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                self._expected_conf_args(
                    cmdenv=dict(PYSPARK_PYTHON='mypy')))

    def test_spark_submit_arg_prefix(self):
        self.start(patch('mrjob.runner.MRJobRunner._spark_submit_arg_prefix',
                         return_value=['<arg prefix>']))

        job = MRNullSpark()
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                ['<arg prefix>'] +
                self._expected_conf_args(
                    cmdenv=dict(PYSPARK_PYTHON='mypy')))

    def test_cmdenv(self):
        job = MRNullSpark(['--cmdenv', 'FOO=bar', '--cmdenv', 'BAZ=qux'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                self._expected_conf_args(
                    cmdenv=dict(PYSPARK_PYTHON='mypy', FOO='bar', BAZ='qux')))

    def test_cmdenv_can_override_python_bin(self):
        job = MRNullSpark(['--cmdenv', 'PYSPARK_PYTHON=ourpy'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                self._expected_conf_args(
                    cmdenv=dict(PYSPARK_PYTHON='ourpy')))

    def test_jobconf(self):
        job = MRNullSpark(['--jobconf', 'spark.executor.memory=10g'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                self._expected_conf_args(
                    cmdenv=dict(PYSPARK_PYTHON='mypy'),
                    jobconf={'spark.executor.memory': '10g'}))

    def test_jobconf_uses_jobconf_for_step(self):
        job = MRNullSpark()
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
            ['--cmdenv', 'FOO=bar',
             '--jobconf', 'spark.executorEnv.FOO=baz',
             '--jobconf', 'spark.yarn.appMasterEnv.PYSPARK_PYTHON=ourpy'])
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

    def test_libjars_option(self):
        fake_libjar = self.makefile('fake_lib.jar')

        job = MRNullSpark(
            ['--libjar', fake_libjar])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                ['--jars', fake_libjar] +
                self._expected_conf_args(
                    cmdenv=dict(PYSPARK_PYTHON='mypy')))

    def test_libjar_paths_override(self):
        job = MRNullSpark()
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
        job = MRNullSpark(['--spark-arg', '--name', '--spark-arg', 'Dave'])
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
        job = MRNullSpark(['--extra-spark-arg', '-v'])
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
            ['--extra-spark-arg', '-v',
             '--spark-arg', '--name', '--spark-arg', 'Dave'])
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
            '--file', foo1_path + '#foo1',
            '--file', foo2_path + '#bar',
            '--archive', baz_path,
            '--dir', qux_path,
        ])
        job.sandbox()

        with job.make_runner() as runner:
            runner._upload_mgr = self._mock_upload_mgr()

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
            '--extra-file', qux_path,  # file upload arg
        ])
        job.sandbox()

        with job.make_runner() as runner:
            runner._upload_mgr = self._mock_upload_mgr()

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

    def _mock_upload_mgr(self):
        def mock_uri(path):
            return '<uri of %s>' % path

        m = Mock()
        m.uri = Mock(side_effect=mock_uri)

        return m

    def test_py_files(self):
        job = MRNullSpark()
        job.sandbox()

        with job.make_runner() as runner:
            runner._spark_py_files = Mock(
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
        job = MRSparkJar(['--jar-main-class', 'foo.Bar',
                          '--cmdenv', 'BAZ=qux',
                          '--jobconf', 'QUX=baz'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._spark_py_files = Mock(
                return_value=['<first py_file>', '<second py_file>']
            )

            # should handle cmdenv and --class
            # but not set PYSPARK_PYTHON or --py-file
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
        job = MRSparkScript()
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_submit_args(0),
                self._expected_conf_args(
                    cmdenv=dict(PYSPARK_PYTHON='mypy')))

    def test_streaming_step_not_allowed(self):
        job = MRTwoStepJob()
        job.sandbox()

        with job.make_runner() as runner:
            self.assertRaises(
                TypeError,
                runner._spark_submit_args, 0)


class SparkPyFilesTestCase(SandboxedTestCase):

    def test_default(self):
        job = MRNullSpark()
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner._spark_py_files(),
                             [runner._create_mrjob_zip()])

    def test_eggs(self):
        # by default, we pass py_files directly to Spark
        egg1_path = self.makefile('dragon.egg')
        egg2_path = self.makefile('horton.egg')

        job = MRNullSpark(['--py-file', egg1_path, '--py-file', egg2_path])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_py_files(),
                [egg1_path, egg2_path, runner._create_mrjob_zip()]
            )

    def test_no_bootstrap_mrjob(self):
        job = MRNullSpark(['--no-bootstrap-mrjob'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner._spark_py_files(),
                             [])

    def test_no_bootstrap_mrjob_in_setup(self):
        job = MRNullSpark([])
        job.sandbox()

        with job.make_runner() as runner:
            # this happens in runners that run on a cluster
            runner.BOOTSTRAP_MRJOB_IN_SETUP = False
            self.assertEqual(runner._spark_py_files(),
                             [])

    def test_no_hash_paths(self):
        egg_path = self.makefile('horton.egg')

        job = MRNullSpark(['--py-file', egg_path + '#mayzie.egg'])
        job.sandbox()

        self.assertRaises(ValueError, job.make_runner)


class SparkScriptPathTestCase(SandboxedTestCase):

    def setUp(self):
        super(SparkScriptPathTestCase, self).setUp()

        self.mock_interpolate_spark_script_path = self.start(patch(
            'mrjob.runner.MRJobRunner._interpolate_spark_script_path'))

    def test_spark_mr_job(self):
        job = MRNullSpark()
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

        job = MRSparkJar(['--jar', self.fake_jar])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_script_path(0),
                self.mock_interpolate_spark_script_path(
                    self.fake_jar)
            )

    def test_spark_script(self):
        self.fake_script = self.makefile('fake_script.py')

        job = MRSparkScript(['--script', self.fake_script])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_script_path(0),
                self.mock_interpolate_spark_script_path(
                    self.fake_script)
            )

    def test_streaming_step_not_okay(self):
        job = MRTwoStepJob()
        job.sandbox()

        with job.make_runner() as runner:
            self.assertRaises(
                TypeError,
                runner._spark_script_path, 0)


class SparkScriptArgsTestCase(SandboxedTestCase):

    def setUp(self):
        super(SparkScriptArgsTestCase, self).setUp()

        # don't bother with actual input/output URIs, which
        # are tested elsewhere
        def mock_interpolate_input_and_output(args, step_num):
            def interpolate(arg):
                if arg == INPUT:
                    return '<step %d input>' % step_num
                elif arg == OUTPUT:
                    return '<step %d output>' % step_num
                else:
                    return arg

            return [interpolate(arg) for arg in args]

        self.start(patch(
            'mrjob.runner.MRJobRunner._interpolate_input_and_output',
            side_effect=mock_interpolate_input_and_output))

    def test_spark_mr_job(self):
        job = MRNullSpark()
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_script_args(0),
                ['--step-num=0',
                 '--spark',
                 '<step 0 input>',
                 '<step 0 output>'])

    def test_spark_passthrough_arg(self):
        job = MRNullSpark(['--extra-spark-arg', '--verbose'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_script_args(0),
                ['--step-num=0',
                 '--spark',
                 '--extra-spark-arg',
                 '--verbose',
                 '<step 0 input>',
                 '<step 0 output>'])

    def test_spark_file_arg(self):
        foo_path = self.makefile('foo')

        job = MRNullSpark(['--extra-file', foo_path])
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
        job = MRSparkJar(['--jar-arg', 'foo', '--jar-arg', 'bar'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_script_args(0),
                ['foo', 'bar'])

    def test_spark_jar_interpolation(self):
        job = MRSparkJar(['--jar-arg', OUTPUT, '--jar-arg', INPUT])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_script_args(0),
                ['<step 0 output>', '<step 0 input>'])

    def test_spark_script(self):
        job = MRSparkScript(['--script-arg', 'foo', '--script-arg', 'bar'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_script_args(0),
                ['foo', 'bar'])

    def test_spark_script_interpolation(self):
        job = MRSparkScript(['--script-arg', OUTPUT, '--script-arg', INPUT])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._spark_script_args(0),
                ['<step 0 output>', '<step 0 input>'])

    def test_streaming_step_not_okay(self):
        job = MRTwoStepJob()
        job.sandbox()

        with job.make_runner() as runner:
            self.assertRaises(
                TypeError,
                runner._spark_script_args, 0)


class StrictProtocolsInConfTestCase(SandboxedTestCase):
    # regression tests for #1302, where command-line option's default
    # overrode configs

    STRICT_MRJOB_CONF = {'runners': {'inline': {'strict_protocols': True}}}

    LOOSE_MRJOB_CONF = {'runners': {'inline': {'strict_protocols': False}}}

    def test_default(self):
        job = MRJob()
        with job.make_runner() as runner:
            self.assertEqual(runner._opts['strict_protocols'], True)

    def test_strict_mrjob_conf(self):
        job = MRJob()
        with mrjob_conf_patcher(self.STRICT_MRJOB_CONF):
            with job.make_runner() as runner:
                self.assertEqual(runner._opts['strict_protocols'], True)

    def test_loose_mrjob_conf(self):
        job = MRJob()
        with mrjob_conf_patcher(self.LOOSE_MRJOB_CONF):
            with job.make_runner() as runner:
                self.assertEqual(runner._opts['strict_protocols'], False)


class CheckInputPathsTestCase(TestCase):

    def test_check_input_paths_enabled_by_default(self):
        job = MRWordCount()
        with job.make_runner() as runner:
            self.assertTrue(runner._opts['check_input_paths'])

    def test_check_input_paths_disabled(self):
        job = MRWordCount(['--no-check-input-paths'])
        with job.make_runner() as runner:
            self.assertFalse(runner._opts['check_input_paths'])

    def test_can_disable_check_input_paths_in_config(self):
        job = MRWordCount()
        with mrjob_conf_patcher(
                {'runners': {'inline': {'check_input_paths': False}}}):
            with job.make_runner() as runner:
                self.assertFalse(runner._opts['check_input_paths'])


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

        self.foo_tar_gz = os.path.join(self.tmp_dir, 'foo.tar.gz')
        tar_and_gzip(self.foo_dir, self.foo_tar_gz)

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
                           '--file', self.foo_sh,
                           '--file', self.foo_sh + '#bar.sh',
                           ])
        job.sandbox()

        with job.make_runner() as r:
            r.run()

            path_to_size = dict(job.parse_output_line(line)
                                for line in r.stream_output())

        self.assertEqual(path_to_size.get('./foo.sh'), self.foo_sh_size)
        self.assertEqual(path_to_size.get('./bar.sh'), self.foo_sh_size)

    def test_archive_upload(self):
        job = MROSWalkJob(['-r', 'local',
                           '--archive', self.foo_tar_gz,
                           '--archive', self.foo_tar_gz + '#foo',
                           ])
        job.sandbox()

        with job.make_runner() as r:
            with no_handlers_for_logger('mrjob.local'):
                r.run()

            path_to_size = dict(job.parse_output_line(line)
                                for line in r.stream_output())

        self.assertEqual(path_to_size.get('./foo.tar.gz/foo.py'),
                         self.foo_py_size)
        self.assertEqual(path_to_size.get('./foo/foo.py'),
                         self.foo_py_size)

    def test_dir_upload(self):
        job = MROSWalkJob(['-r', 'local',
                           '--dir', self.foo_dir,
                           '--dir', self.foo_dir + '#bar'])
        job.sandbox()

        with job.make_runner() as r:
            with no_handlers_for_logger('mrjob.local'):
                r.run()

            path_to_size = dict(job.parse_output_line(line)
                                for line in r.stream_output())

        self.assertEqual(path_to_size.get('./foo/foo.py'),
                         self.foo_py_size)
        self.assertEqual(path_to_size.get('./bar/foo.py'),
                         self.foo_py_size)

    def test_deprecated_python_archive_option(self):
        job = MROSWalkJob(
            ['-r', 'local',
             '--python-archive', self.foo_tar_gz])
        job.sandbox()

        with job.make_runner() as r:
            r.run()

            path_to_size = dict(job.parse_output_line(line)
                                for line in r.stream_output())

        # foo.py should be there, and getsize() should be patched to return
        # double the number of bytes
        self.assertEqual(path_to_size.get('./foo.tar.gz/foo.py'),
                         self.foo_py_size * 2)

    def test_deprecated_setup_cmd_option(self):
        job = MROSWalkJob(
            ['-r', 'local',
             '--setup-cmd', 'touch bar'])
        job.sandbox()

        with job.make_runner() as r:
            r.run()

            path_to_size = dict(job.parse_output_line(line)
                                for line in r.stream_output())

        self.assertIn('./bar', path_to_size)

    def test_deprecated_setup_script_option(self):
        job = MROSWalkJob(
            ['-r', 'local',
             '--setup-script', self.foo_sh])
        job.sandbox()

        with job.make_runner() as r:
            r.run()

            path_to_size = dict(job.parse_output_line(line)
                                for line in r.stream_output())

            self.assertEqual(path_to_size.get('./foo.sh'), self.foo_sh_size)
            self.assertIn('./foo.sh-made-this', path_to_size)

    def test_python_archive(self):
        job = MROSWalkJob([
            '-r', 'local',
            '--setup', 'export PYTHONPATH=%s#/:$PYTHONPATH' % self.foo_tar_gz
        ])
        job.sandbox()

        with job.make_runner() as r:
            r.run()

            path_to_size = dict(job.parse_output_line(line)
                                for line in r.stream_output())

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

            path_to_size = dict(job.parse_output_line(line)
                                for line in r.stream_output())

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

            path_to_size = dict(job.parse_output_line(line)
                                for line in r.stream_output())

        # foo.py should be there, and getsize() should be patched to return
        # double the number of bytes
        self.assertEqual(path_to_size.get('./foo.zip'),
                         self.foo_zip_size * 2)

    def test_py_file(self):
        job = MROSWalkJob([
            '-r', 'local',
            '--py-file', self.foo_zip,
        ])
        job.sandbox()

        with job.make_runner() as r:
            r.run()

            path_to_size = dict(job.parse_output_line(line)
                                for line in r.stream_output())

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

            path_to_size = dict(job.parse_output_line(line)
                                for line in r.stream_output())

        self.assertIn('./bar', path_to_size)

    def test_setup_script(self):
        job = MROSWalkJob(
            ['-r', 'local',
             '--setup', self.foo_sh + '#'])
        job.sandbox()

        with job.make_runner() as r:
            r.run()

            path_to_size = dict(job.parse_output_line(line)
                                for line in r.stream_output())

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

                path_to_size = dict(job.parse_output_line(line)
                                    for line in r.stream_output())

                self.assertEqual(path_to_size.get('./stdin.txt'), 0)
                # input gets passed through by identity mapper
                self.assertEqual(path_to_size.get(None), 'some input')

        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, self._old_alarm_handler)

    def test_wrapper_script_only_writes_to_stderr(self):
        job = MROSWalkJob([
            '-r', 'local',
            '--setup', 'echo stray output',
        ])
        job.sandbox()

        with no_handlers_for_logger('mrjob.local'):
            stderr = StringIO()
            log_to_stream('mrjob.local', stderr, debug=True)

            with job.make_runner() as r:
                r.run()

                output = b''.join(r.stream_output())

                # stray ouput should be in stderr, not the job's output
                self.assertIn('stray output', stderr.getvalue())
                self.assertNotIn(b'stray output', output)


class ClosedRunnerTestCase(EmptyMrjobConfTestCase):

    def test_job_closed_on_cleanup(self):
        job = MRWordCount()
        with job.make_runner() as runner:
            # do nothing
            self.assertFalse(runner._closed)
        self.assertTrue(runner._closed)


class InterpreterTestCase(TestCase):

    def default_python_bin(self):
        return ['python'] if PY2 else ['python3']

    def test_default(self):
        runner = MRJobRunner()
        self.assertEqual(runner._python_bin(),
                         self.default_python_bin())
        self.assertEqual(runner._interpreter(),
                         self.default_python_bin())
        self.assertEqual(runner._interpreter(steps=True),
                         [sys.executable])

    def test_python_bin(self):
        runner = MRJobRunner(python_bin=['python', '-v'])
        self.assertEqual(runner._python_bin(), ['python', '-v'])
        self.assertEqual(runner._interpreter(), ['python', '-v'])
        self.assertEqual(runner._interpreter(steps=True), [sys.executable])

    def test_steps_python_bin(self):
        runner = MRJobRunner(steps_python_bin=['python', '-v'])
        self.assertEqual(runner._python_bin(), self.default_python_bin())
        self.assertEqual(runner._interpreter(), self.default_python_bin())
        self.assertEqual(runner._interpreter(steps=True), ['python', '-v'])

    def test_task_python_bin(self):
        runner = MRJobRunner(task_python_bin=['python', '-v'])
        self.assertEqual(runner._python_bin(), self.default_python_bin())
        self.assertEqual(runner._interpreter(), ['python', '-v'])
        self.assertEqual(runner._interpreter(steps=True),
                         [sys.executable])

    def test_interpreter(self):
        runner = MRJobRunner(interpreter=['ruby'])
        self.assertEqual(runner._interpreter(), ['ruby'])
        self.assertEqual(runner._interpreter(steps=True), ['ruby'])

    def test_steps_interpreter(self):
        # including whether steps_interpreter overrides interpreter
        runner = MRJobRunner(interpreter=['ruby', '-v'],
                             steps_interpreter=['ruby'])
        self.assertEqual(runner._interpreter(), ['ruby', '-v'])
        self.assertEqual(runner._interpreter(steps=True), ['ruby'])

    def test_interpreter_overrides_python_bin(self):
        runner = MRJobRunner(interpreter=['ruby'],
                             python_bin=['python', '-v'])
        self.assertEqual(runner._interpreter(), ['ruby'])
        self.assertEqual(runner._interpreter(steps=True), ['ruby'])

    def test_interpreter_overrides_steps_python_bin(self):
        runner = MRJobRunner(interpreter=['ruby'],
                             steps_python_bin=['python', '-v'])
        self.assertEqual(runner._interpreter(), ['ruby'])
        self.assertEqual(runner._interpreter(steps=True), ['ruby'])


class BootstrapMRJobTestCase(TestCase):
    # this just tests _bootstrap_mrjob() (i.e. whether to bootstrap mrjob);
    # actual testing of bootstrapping is in test_local

    def test_default(self):
        runner = MRJobRunner(conf_paths=[])
        self.assertEqual(runner._bootstrap_mrjob(), True)

    def test_no_bootstrap_mrjob(self):
        runner = MRJobRunner(conf_paths=[], bootstrap_mrjob=False)
        self.assertEqual(runner._bootstrap_mrjob(), False)

    def test_interpreter(self):
        runner = MRJobRunner(conf_paths=[], interpreter=['ruby'])
        self.assertEqual(runner._bootstrap_mrjob(), False)

    def test_bootstrap_mrjob_overrides_interpreter(self):
        runner = MRJobRunner(
            conf_paths=[], interpreter=['ruby'], bootstrap_mrjob=True)
        self.assertEqual(runner._bootstrap_mrjob(), True)


class FSPassthroughTestCase(TestCase):

    def test_passthrough(self):
        runner = InlineMRJobRunner()

        with no_handlers_for_logger('mrjob.runner'):
            stderr = StringIO()
            log_to_stream('mrjob.runner', stderr)

            self.assertEqual(runner.ls, runner.fs.ls)
            # no special rules for underscore methods
            self.assertEqual(runner._cat_file, runner.fs._cat_file)

            self.assertIn(
                'deprecated: call InlineMRJobRunner.fs.ls() directly',
                stderr.getvalue())
            self.assertIn(
                'deprecated: call InlineMRJobRunner.fs._cat_file() directly',
                stderr.getvalue())

    def test_prefer_own_methods(self):
        # TODO: currently can't initialize HadoopRunner without setting these
        runner = HadoopJobRunner(
            hadoop_bin='hadoop',
            hadoop_home='kansas',
            hadoop_streaming_jar='streaming.jar')

        with no_handlers_for_logger('mrjob.runner'):
            stderr = StringIO()
            log_to_stream('mrjob.runner', stderr)

            self.assertEqual(runner.ls, runner.fs.ls)

            # Hadoop Runner has its own version
            self.assertNotEqual(runner.get_hadoop_version,
                                runner.fs.get_hadoop_version)

            self.assertIn(
                'deprecated: call HadoopJobRunner.fs.ls() directly',
                stderr.getvalue())
            self.assertNotIn('get_hadoop_version', stderr.getvalue())

    def test_pass_through_fields(self):
        # TODO: currently can't initialize HadoopRunner without setting these
        runner = HadoopJobRunner(
            hadoop_bin='hadoooooooooop',
            hadoop_home='kansas',
            hadoop_streaming_jar='streaming.jar')

        with no_handlers_for_logger('mrjob.runner'):
            stderr = StringIO()
            log_to_stream('mrjob.runner', stderr)

            self.assertEqual(runner._hadoop_bin, runner.fs._hadoop_bin)

            # deprecation warning is different for non-functions
            self.assertIn(
                'deprecated: access HadoopJobRunner.fs._hadoop_bin directly',
                stderr.getvalue())


class StepInputAndOutputURIsTestCase(SandboxedTestCase):

    def test_two_step_job(self):
        input1_path = self.makefile('input1')
        input2_path = self.makefile('input2')

        job = MRTwoStepJob([
            '-r', 'hadoop',
            '--hadoop-bin', 'false',  # shouldn't run; just in case
            input1_path, input2_path])
        job.sandbox()

        with job.make_runner() as runner:
            runner._add_job_files_for_upload()

            input_uris_0 = runner._step_input_uris(0)
            self.assertEqual([os.path.basename(uri) for uri in input_uris_0],
                             ['input1', 'input2'])

            output_uri_0 = runner._step_output_uri(0)
            input_uris_1 = runner._step_input_uris(1)

            self.assertEqual(input_uris_1, [output_uri_0])

            output_uri_1 = runner._step_output_uri(1)
            self.assertEqual(output_uri_1, runner._output_dir)

    def test_output_dir_and_step_output_dir(self):
        input1_path = self.makefile('input1')
        input2_path = self.makefile('input2')

        # this has three steps, which lets us test step numbering
        job = MRCountingJob([
            '-r', 'hadoop',
            '--hadoop-bin', 'false',  # shouldn't run; just in case
            '--output-dir', 'hdfs:///tmp/output',
            '--step-output-dir', 'hdfs://tmp/step-output',
            input1_path, input2_path])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner._num_steps(), 3)

            runner._add_job_files_for_upload()

            input_uris_0 = runner._step_input_uris(0)
            self.assertEqual([os.path.basename(uri) for uri in input_uris_0],
                             ['input1', 'input2'])

            output_uri_0 = runner._step_output_uri(0)
            self.assertEqual(output_uri_0, 'hdfs://tmp/step-output/0000')

            input_uris_1 = runner._step_input_uris(1)
            self.assertEqual(input_uris_1, [output_uri_0])

            output_uri_1 = runner._step_output_uri(1)
            self.assertEqual(output_uri_1, 'hdfs://tmp/step-output/0001')

            input_uris_2 = runner._step_input_uris(2)
            self.assertEqual(input_uris_2, [output_uri_1])

            output_uri_2 = runner._step_output_uri(2)
            self.assertEqual(output_uri_2, 'hdfs:///tmp/output')


class DirArchivePathTestCase(SandboxedTestCase):

    def test_dir(self):
        archive_dir = self.makedirs('archive')

        runner = InlineMRJobRunner()
        archive_path = runner._dir_archive_path(archive_dir)

        self.assertEqual(os.path.basename(archive_path), 'archive.tar.gz')

    def test_trailing_slash(self):
        archive_dir = self.makedirs('archive') + os.sep

        runner = InlineMRJobRunner()
        archive_path = runner._dir_archive_path(archive_dir)

        self.assertEqual(os.path.basename(archive_path), 'archive.tar.gz')

    def test_missing_dir(self):
        archive_path = os.path.join(self.tmp_dir, 'archive')

        runner = InlineMRJobRunner()

        self.assertRaises(OSError, runner._dir_archive_path, archive_path)

    def test_file(self):
        foo_file = self.makefile('foo')

        runner = InlineMRJobRunner()

        self.assertRaises(OSError, runner._dir_archive_path, foo_file)

    def test_uri(self):
        # we don't check whether URIs exist or are directories
        runner = InlineMRJobRunner()
        archive_path = runner._dir_archive_path('s3://bucket/stuff')

        self.assertEqual(os.path.basename(archive_path), 'stuff.tar.gz')

    def test_dirs_with_same_name(self):
        foo_archive = self.makedirs(os.path.join('foo', 'archive'))
        bar_archive = self.makedirs(os.path.join('bar', 'archive'))

        runner = InlineMRJobRunner()
        foo_archive_path = runner._dir_archive_path(foo_archive)
        bar_archive_path = runner._dir_archive_path(bar_archive)

        self.assertEqual(os.path.basename(foo_archive_path),
                         'archive.tar.gz')
        self.assertNotEqual(foo_archive_path, bar_archive_path)

    def test_same_dir_twice(self):
        archive_dir = self.makedirs('archive')

        runner = InlineMRJobRunner()
        archive_path_1 = runner._dir_archive_path(archive_dir)
        archive_path_2 = runner._dir_archive_path(archive_dir)

        self.assertEqual(os.path.basename(archive_path_1), 'archive.tar.gz')
        self.assertEqual(archive_path_1, archive_path_2)

    def test_doesnt_actually_create_archive(self):
        archive_dir = self.makedirs('archive')

        runner = InlineMRJobRunner()
        archive_path = runner._dir_archive_path(archive_dir)

        self.assertFalse(os.path.exists(archive_path))


class CreateDirArchiveTestCase(SandboxedTestCase):

    def setUp(self):
        super(CreateDirArchiveTestCase, self).setUp()

        self._to_archive = self.makedirs('archive')
        self.makefile(os.path.join('archive', 'foo'))
        self.makefile(os.path.join('archive', 'bar', 'baz'))

    def test_archive(self):
        runner = InlineMRJobRunner()

        tar_gz_path = runner._dir_archive_path(self._to_archive)
        self.assertEqual(os.path.basename(tar_gz_path), 'archive.tar.gz')

        runner._create_dir_archive(self._to_archive)

        tar_gz = tarfile.open(tar_gz_path, 'r:gz')
        try:
            self.assertEqual(sorted(tar_gz.getnames()),
                             [os.path.join('bar', 'baz'), 'foo'])
        finally:
            tar_gz.close()

    def test_only_create_archive_once(self):
        runner = InlineMRJobRunner()

        tar_gz_path = runner._dir_archive_path(self._to_archive)

        runner._create_dir_archive(self._to_archive)
        mtime_1 = os.stat(tar_gz_path).st_mtime

        sleep(1)
        runner._create_dir_archive(self._to_archive)
        mtime_2 = os.stat(tar_gz_path).st_mtime

        self.assertEqual(mtime_1, mtime_2)

    def test_nonexistent_dir(self):
        runner = InlineMRJobRunner()

        nonexistent_dir = os.path.join(self.tmp_dir, 'nonexistent')

        self.assertRaises(
            OSError, runner._create_dir_archive, nonexistent_dir)

    def test_empty_dir(self):
        runner = InlineMRJobRunner()

        empty_dir = self.makedirs('empty')

        tar_gz_path = runner._dir_archive_path(empty_dir)
        self.assertEqual(os.path.basename(tar_gz_path), 'empty.tar.gz')

        runner._create_dir_archive(empty_dir)

        tar_gz = tarfile.open(tar_gz_path, 'r:gz')

        try:
            self.assertEqual(sorted(tar_gz.getnames()), [])
        finally:
            tar_gz.close()

    def test_file(self):
        qux_path = self.makefile('qux')

        runner = InlineMRJobRunner()

        self.assertRaises(OSError, runner._create_dir_archive, qux_path)


class RemoteCreateDirArchiveTestCase(MockBotoTestCase):
    # additional test cases that archive stuff from (mock) S3

    def setUp(self):
        super(RemoteCreateDirArchiveTestCase, self).setUp()

        fs = S3Filesystem()

        fs.create_bucket('walrus')
        fs.make_s3_key('s3://walrus/archive/foo')
        fs.make_s3_key('s3://walrus/archive/bar/baz')

    def test_archive_remote_data(self):
        runner = EMRJobRunner()

        tar_gz_path = runner._dir_archive_path('s3://walrus/archive')
        self.assertEqual(os.path.basename(tar_gz_path), 'archive.tar.gz')

        runner._create_dir_archive('s3://walrus/archive')

        tar_gz = tarfile.open(tar_gz_path, 'r:gz')
        try:
            self.assertEqual(sorted(tar_gz.getnames()),
                             [os.path.join('bar', 'baz'), 'foo'])
        finally:
            tar_gz.close()


class SortValuesTestCase(SandboxedTestCase):

    def test_no_sort_values(self):
        mr_job = MRTwoStepJob()
        mr_job.sandbox()

        self.assertFalse(mr_job.sort_values())

        with mr_job.make_runner() as runner:
            self.assertEqual(runner._jobconf_for_step(0), {})
            self.assertNotIn('-partitioner', runner._hadoop_args_for_step(0))

    def test_sort_values_jobconf_version_agnostic(self):
        # this only happens in local runners
        mr_job = MRSortValues()
        mr_job.sandbox()

        self.assertTrue(mr_job.sort_values())

        with mr_job.make_runner() as runner:
            self.assertEqual(runner._jobconf_for_step(0), {
                'mapred.text.key.partitioner.options': '-k1,1',
                'mapreduce.partition.keypartitioner.options': '-k1,1',
                'stream.num.map.output.key.fields': 2,
            })

    def test_sort_values_jobconf_hadoop_1(self):
        mr_job = MRSortValues(['--hadoop-version', '1.0.0'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            self.assertEqual(runner._jobconf_for_step(0), {
                'mapred.text.key.partitioner.options': '-k1,1',
                'stream.num.map.output.key.fields': 2,
            })

    def test_sort_values_jobconf_hadoop_2(self):
        mr_job = MRSortValues(['--hadoop-version', '2.0.0'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            self.assertEqual(runner._jobconf_for_step(0), {
                'mapreduce.partition.keypartitioner.options': '-k1,1',
                'stream.num.map.output.key.fields': 2,
            })

    def test_job_can_override_jobconf(self):
        mr_job = MRSortValuesAndMore(['--hadoop-version', '2.0.0'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            self.assertEqual(
                runner._jobconf_for_step(0), {
                    'mapreduce.partition.keycomparator.options': '-k1 -k2nr',
                    'mapreduce.partition.keypartitioner.options': '-k1,1',
                    'stream.num.map.output.key.fields': 3,
                }
            )

    def test_steps_can_override_jobconf(self):
        mr_job = MRSortValuesAndMore(['--hadoop-version', '2.0.0'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            self.assertEqual(
                runner._jobconf_for_step(1), {
                    'mapreduce.job.output.key.comparator.class':
                    'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                    'mapreduce.partition.keycomparator.options': '-k1 -k2nr',
                    'mapreduce.partition.keypartitioner.options': '-k1,1',
                    'stream.num.map.output.key.fields': 3,
                }
            )

    def test_cmd_line_can_override_jobconf(self):
        mr_job = MRSortValues([
            '--hadoop-version', '2.0.0',
            '--jobconf', 'stream.num.map.output.key.fields=3',
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
        mr_job = MRSortValues()
        mr_job.sandbox()

        self.assertTrue(mr_job.sort_values())

        with mr_job.make_runner() as runner:
            hadoop_args = runner._hadoop_args_for_step(0)
            self.assertIn('-partitioner', hadoop_args)
            self.assertEqual(
                hadoop_args[hadoop_args.index('-partitioner') + 1],
                'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner')

    def test_job_can_override_partitioner(self):
        mr_job = MRSortValuesAndMore()
        mr_job.sandbox()

        self.assertTrue(mr_job.sort_values())

        with mr_job.make_runner() as runner:
            hadoop_args = runner._hadoop_args_for_step(0)
            self.assertIn('-partitioner', hadoop_args)
            self.assertEqual(
                hadoop_args[hadoop_args.index('-partitioner') + 1],
                'org.apache.hadoop.mapred.lib.HashPartitioner')

    def test_cmd_line_can_override_partitioner(self):
        # the --partitioner option is deprecated
        mr_job = MRSortValues(['--partitioner', 'FooPartitioner'])
        mr_job.sandbox()

        self.assertTrue(mr_job.sort_values())

        with mr_job.make_runner() as runner:
            hadoop_args = runner._hadoop_args_for_step(0)
            self.assertIn('-partitioner', hadoop_args)
            self.assertEqual(
                hadoop_args[hadoop_args.index('-partitioner') + 1],
                'FooPartitioner')


class RenderSubstepTestCase(SandboxedTestCase):

    def test_streaming_mapper(self):
        self._test_streaming_substep('mapper')

    def test_streaming_combiner(self):
        self._test_streaming_substep('combiner')

    def test_streaming_reducer(self):
        self._test_streaming_substep('reducer')

    def _test_streaming_substep(self, mrc):
        job = MRTwoStepJob()  # includes mapper, combiner, reducer
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._render_substep(0, mrc),
                '%s mr_two_step_job.py --step-num=0 --%s' % (PYTHON_BIN, mrc)
            )

    def test_step_1_streaming(self):
        # just make sure that --step-num isn't hardcoded
        job = MRTwoStepJob()  # includes step 1 mapper
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._render_substep(1, 'mapper'),
                '%s mr_two_step_job.py --step-num=1 --mapper' % PYTHON_BIN
            )

    def test_mapper_cmd(self):
        self._test_cmd('mapper')

    def test_combiner_cmd(self):
        self._test_cmd('combiner')

    def test_reducer_cmd(self):
        self._test_cmd('reducer')

    def _test_cmd(self, mrc):
        job = MRCmdJob([('--%s-cmd' % mrc), 'grep foo'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._render_substep(0, mrc),
                'grep foo'
            )

    def test_no_mapper(self):
        job = MRNoMapper()  # second step has no mapper
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
        job = MRNoRunner()  # only has mapper
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._render_substep(0, mrc),
                None
            )

    def test_respects_interpreter_method(self):
        # _interpreter() method is extensively tested; just
        # verify that we use it
        job = MRTwoStepJob()
        job.sandbox()

        self.start(patch('mrjob.runner.MRJobRunner._interpreter',
                         return_value=['run-my-task']))

        with job.make_runner() as runner:
            self.assertEqual(
                runner._render_substep(0, 'mapper'),
                'run-my-task mr_two_step_job.py --step-num=0 --mapper'
            )

    def test_setup_wrapper_script(self):
        # inline runner doesn't create wrapper script
        job = MRTwoStepJob(['-r', 'local', '--setup', 'true'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._create_setup_wrapper_script()

            # note that local mode uses sys.executable, not python/python3
            self.assertEqual(
                runner._render_substep(0, 'mapper'),
                'sh -ex setup-wrapper.sh %s mr_two_step_job.py'
                ' --step-num=0 --mapper' % sys.executable
            )

    def test_setup_wrapper_script_custom_sh_bin(self):
        job = MRTwoStepJob(['-r', 'local', '--setup', 'true',
                            '--sh-bin', 'sh -xv'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._create_setup_wrapper_script()

            # note that local mode uses sys.executable, not python/python3
            self.assertEqual(
                runner._render_substep(0, 'mapper'),
                'sh -xv setup-wrapper.sh %s mr_two_step_job.py'
                ' --step-num=0 --mapper' % sys.executable
            )

    def test_setup_wrapper_respects_sh_bin_method(self):
        job = MRTwoStepJob(['-r', 'local', '--setup', 'true'])
        job.sandbox()

        self.start(patch('mrjob.runner.MRJobRunner._sh_bin',
                         return_value=['bash']))

        with job.make_runner() as runner:
            runner._create_setup_wrapper_script()

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
                "sh -ex -c 'cat |"
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

        self.start(patch('mrjob.runner.MRJobRunner._sh_bin',
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

        self.start(patch('mrjob.runner.MRJobRunner._sh_pre_commands',
                         return_value=['set -v']))

        with job.make_runner() as runner:
            self.assertEqual(
                runner._render_substep(0, 'mapper'),
                "sh -ex -c 'set -v;"
                " cat | %s mr_filter_job.py --step-num=0 --mapper"
                " --mapper-filter cat'" % sys.executable)


class SetupWrapperScriptContentTestCase(SandboxedTestCase):

    def test_no_shebang(self):
        job = MRTwoStepJob()
        job.sandbox()

        with job.make_runner() as runner:
            out = runner._setup_wrapper_script_content([])

            self.assertFalse(out[0].startswith('#!'))

    def test_respects_sh_pre_commands_methd(self):
        job = MRTwoStepJob()
        job.sandbox()

        self.start(patch('mrjob.runner.MRJobRunner._sh_pre_commands',
                         return_value=['set -e', 'set -v']))

        with job.make_runner() as runner:
            out = runner._setup_wrapper_script_content([])

            self.assertEqual(out[:2], ['set -e\n', 'set -v\n'])
