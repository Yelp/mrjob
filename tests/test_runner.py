# Copyright 2009-2013 Yelp and Contributors
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


import datetime
import getpass
import os
import os.path
import shutil
import signal
import stat
from subprocess import CalledProcessError
import sys
import tarfile
import tempfile

try:
    from cStringIO import StringIO
    StringIO  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    from StringIO import StringIO

try:
    import unittest2 as unittest
    unittest  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import unittest

from mock import patch
from mrjob.inline import InlineMRJobRunner
from mrjob.local import LocalMRJobRunner
from mrjob.parse import JOB_NAME_RE
from mrjob.runner import MRJobRunner
from mrjob.util import log_to_stream
from mrjob.util import tar_and_gzip
from tests.mr_os_walk_job import MROSWalkJob
from tests.mr_two_step_job import MRTwoStepJob
from tests.mr_word_count import MRWordCount
from tests.quiet import no_handlers_for_logger
from tests.sandbox import EmptyMrjobConfTestCase
from tests.sandbox import SandboxedTestCase


class WithStatementTestCase(unittest.TestCase):

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

    def test_cleanup_scratch(self):
        self._test_cleanup_after_with_statement(['SCRATCH'], False)

    def test_cleanup_local_scratch(self):
        self._test_cleanup_after_with_statement(['LOCAL_SCRATCH'], False)

    def test_cleanup_remote_scratch(self):
        self._test_cleanup_after_with_statement(['REMOTE_SCRATCH'], True)

    def test_cleanup_none(self):
        self._test_cleanup_after_with_statement(['NONE'], True)

    def test_cleanup_error(self):
        self.assertRaises(ValueError, self._test_cleanup_after_with_statement,
                          ['NONE', 'ALL'], True)
        self.assertRaises(ValueError, self._test_cleanup_after_with_statement,
                          ['GARBAGE'], True)

    def test_double_none_okay(self):
        self._test_cleanup_after_with_statement(['NONE', 'NONE'], True)


class TestJobName(unittest.TestCase):

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
        match = JOB_NAME_RE.match(runner.get_job_name())

        self.assertEqual(match.group(1), 'no_script')
        self.assertEqual(match.group(2), getpass.getuser())

    def test_empty_no_user(self):
        self.getuser_should_fail = True
        runner = InlineMRJobRunner(conf_paths=[])
        match = JOB_NAME_RE.match(runner.get_job_name())

        self.assertEqual(match.group(1), 'no_script')
        self.assertEqual(match.group(2), 'no_user')

    def test_auto_label(self):
        runner = MRTwoStepJob(['--no-conf']).make_runner()
        match = JOB_NAME_RE.match(runner.get_job_name())

        self.assertEqual(match.group(1), 'mr_two_step_job')
        self.assertEqual(match.group(2), getpass.getuser())

    def test_auto_owner(self):
        os.environ['USER'] = 'mcp'
        runner = InlineMRJobRunner(conf_paths=[])
        match = JOB_NAME_RE.match(runner.get_job_name())

        self.assertEqual(match.group(1), 'no_script')
        self.assertEqual(match.group(2), 'mcp')

    def test_auto_everything(self):
        test_start = datetime.datetime.utcnow()

        os.environ['USER'] = 'mcp'
        runner = MRTwoStepJob(['--no-conf']).make_runner()
        match = JOB_NAME_RE.match(runner.get_job_name())

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
        match = JOB_NAME_RE.match(runner.get_job_name())

        self.assertEqual(match.group(1), 'ads_chain')
        self.assertEqual(match.group(2), 'ads')

    def test_owner_and_label_kwargs(self):
        runner = InlineMRJobRunner(conf_paths=[],
                                  owner='ads', label='ads_chain')
        match = JOB_NAME_RE.match(runner.get_job_name())

        self.assertEqual(match.group(1), 'ads_chain')
        self.assertEqual(match.group(2), 'ads')


class CreateMrjobTarGzTestCase(unittest.TestCase):

    def test_create_mrjob_tar_gz(self):
        with no_handlers_for_logger('mrjob.runner'):
            with InlineMRJobRunner(conf_paths=[]) as runner:
                mrjob_tar_gz_path = runner._create_mrjob_tar_gz()
                mrjob_tar_gz = tarfile.open(mrjob_tar_gz_path)
                contents = mrjob_tar_gz.getnames()

                for path in contents:
                    self.assertEqual(path[:6], 'mrjob/')

                self.assertIn('mrjob/job.py', contents)


class TestStreamingOutput(unittest.TestCase):

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
                         ['A', 'B', 'C'])


class TestInvokeSort(unittest.TestCase):

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
                                 runner._opts['base_tmp_dir'])

    def test_no_files(self):
        runner = MRJobRunner(conf_paths=[])
        self.assertRaises(ValueError,
                          runner._invoke_sort, [], self.out)

    def test_one_file(self):
        runner = MRJobRunner(conf_paths=[])
        self.addCleanup(runner.cleanup)

        runner._invoke_sort([self.a], self.out)

        self.assertEqual(list(open(self.out)),
                         ['A\n',
                          'alligator\n',
                          'apple\n'])

    def test_two_files(self):
        runner = MRJobRunner(conf_paths=[])
        self.addCleanup(runner.cleanup)

        runner._invoke_sort([self.a, self.b], self.out)

        self.assertEqual(list(open(self.out)),
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
            self.assertRaises(CalledProcessError,
                              runner._invoke_sort, [self.a, self.b], self.out)

    def test_environment_variables_non_windows(self):
        runner = MRJobRunner(conf_path=False)
        self.addCleanup(runner.cleanup)

        self.environment_variable_checks(runner, ['TEMP', 'TMPDIR'])

    def test_environment_variables_windows(self):
        runner = MRJobRunner(conf_path=False)
        self.addCleanup(runner.cleanup)

        runner._sort_is_windows_sort = True
        self.environment_variable_checks(runner, ['TMP'])


class HadoopArgsTestCase(EmptyMrjobConfTestCase):

    def test_empty(self):
        job = MRWordCount()
        with job.make_runner() as runner:
            self.assertEqual(runner._hadoop_args_for_step(0), [])

    def test_hadoop_extra_args(self):
        job = MRWordCount(['--hadoop-arg', '-foo'])
        with job.make_runner() as runner:
            self.assertEqual(runner._hadoop_args_for_step(0), ['-foo'])

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

        job_0_18 = MRWordCount(jobconf_args + ['--hadoop-version', '0.18'])
        with job_0_18.make_runner() as runner_0_18:
            self.assertEqual(runner_0_18._hadoop_args_for_step(0),
                             ['-jobconf', 'BAX=Arnold',
                              '-jobconf', 'BAZ=qux',
                              '-jobconf', 'FOO=bar',
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
            ['--jobconf', 'mapred.jobtracker.maxtasks.per.job=1',
             '--hadoop-version', '0.21'])

        with job.make_runner() as runner:
            with no_handlers_for_logger('mrjob.compat'):
                self.assertEqual(runner._hadoop_args_for_step(0),
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

    def test_hadoop_extra_args_comes_first(self):
        job = MRWordCount(
            ['--cmdenv', 'FOO=bar',
             '--hadoop-arg', '-libjar', '--hadoop-arg', 'qux.jar',
             '--jobconf', 'baz=qux',
             '--partitioner', 'java.lang.Object'])
        job.HADOOP_INPUT_FORMAT = 'FooInputFormat'
        job.HADOOP_OUTPUT_FORMAT = 'BarOutputFormat'

        with job.make_runner() as runner:
            hadoop_args = runner._hadoop_args_for_step(0)
            self.assertEqual(hadoop_args[:2], ['-libjar', 'qux.jar'])
            self.assertEqual(len(hadoop_args), 12)


class SetupTestCase(SandboxedTestCase):

    def setUp(self):
        super(SetupTestCase, self).setUp()

        os.mkdir(os.path.join(self.tmp_dir, 'foo'))

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
        tar_and_gzip(os.path.join(self.tmp_dir, 'foo'), self.foo_tar_gz)

        self.foo_py_size = os.path.getsize(self.foo_py)
        self.foo_sh_size = os.path.getsize(self.foo_sh)
        self.foo_tar_gz_size = os.path.getsize(self.foo_tar_gz)

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
            r.run()

            path_to_size = dict(job.parse_output_line(line)
                                for line in r.stream_output())

        self.assertEqual(path_to_size.get('./foo.tar.gz/foo.py'),
                         self.foo_py_size)
        self.assertEqual(path_to_size.get('./foo/foo.py'),
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
        job.sandbox(stdin=StringIO('some input\n'))

        # local mode doesn't currently pipe input into stdin
        # (see issue #567), so this test would hang if it failed
        def alarm_handler(*args, **kwargs):
            raise Exception('Setup script stalled on stdin')

        try:
            self._old_alarm_handler = signal.signal(
                signal.SIGALRM, alarm_handler)
            signal.alarm(2)

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
            log_to_stream('mrjob.local', stderr)

            with job.make_runner() as r:
                r.run()

                output = ''.join(r.stream_output())

                # stray ouput should be in stderr, not the job's output
                self.assertIn('stray output', stderr.getvalue())
                self.assertNotIn('stray output', output)


class ExportJobNameTestCase(EmptyMrjobConfTestCase):

    def test_export_job_name_true(self):
        job = MRWordCount(['--export-job-name'])
        with job.make_runner() as runner:
            self.assertTrue(runner._opts['export_job_name'])
            self.assertEqual(runner._opts['cmdenv']['MRJOB_JOB_NAME'],
                             runner.get_job_name())

    def test_export_job_name_default_false(self):
        job = MRWordCount()
        with job.make_runner() as runner:
            self.assertFalse(runner._opts['export_job_name'])
            self.assertEqual(runner._opts['cmdenv'], {})
