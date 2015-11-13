# Copyright 2009-2012 Yelp
# Copyright 2013 David Marin
# Copyright 2014 Shusen Liu
# Copyright 2015 Yelp
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
"""Test the hadoop job runner."""
import getpass
import os
import pty
from io import BytesIO
from subprocess import CalledProcessError
from subprocess import check_call

from mrjob.fs.hadoop import HadoopFilesystem
from mrjob.hadoop import HadoopJobRunner
from mrjob.hadoop import fully_qualify_hdfs_path
from mrjob.py2 import PY2
from mrjob.util import bash_wrap
from mrjob.util import shlex_split

from tests.mockhadoop import create_mock_hadoop_script
from tests.mockhadoop import add_mock_hadoop_output
from tests.mr_jar_and_streaming import MRJarAndStreaming
from tests.mr_just_a_jar import MRJustAJar
from tests.mr_two_step_hadoop_format_job import MRTwoStepJob
from tests.py2 import TestCase
from tests.py2 import call
from tests.py2 import patch
from tests.quiet import logger_disabled
from tests.sandbox import EmptyMrjobConfTestCase
from tests.sandbox import SandboxedTestCase

# used to match command lines
if PY2:
    PYTHON_BIN = 'python'
else:
    PYTHON_BIN = 'python3'


class TestFullyQualifyHDFSPath(TestCase):

    def test_empty(self):
        with patch('getpass.getuser') as getuser:
            getuser.return_value = 'dave'
            self.assertEqual(fully_qualify_hdfs_path(''), 'hdfs:///user/dave/')

    def test_relative_path(self):
        with patch('getpass.getuser') as getuser:
            getuser.return_value = 'dave'
            self.assertEqual(fully_qualify_hdfs_path('path/to/chocolate'),
                             'hdfs:///user/dave/path/to/chocolate')

    def test_absolute_path(self):
        self.assertEqual(fully_qualify_hdfs_path('/path/to/cheese'),
                         'hdfs:///path/to/cheese')

    def test_hdfs_uri(self):
        self.assertEqual(fully_qualify_hdfs_path('hdfs://host/path/'),
                         'hdfs://host/path/')

    def test_s3n_uri(self):
        self.assertEqual(fully_qualify_hdfs_path('s3n://bucket/oh/noes'),
                         's3n://bucket/oh/noes')

    def test_other_uri(self):
        self.assertEqual(fully_qualify_hdfs_path('foo://bar/baz'),
                         'foo://bar/baz')


class HadoopStreamingJarTestCase(SandboxedTestCase):

    def setUp(self):
        super(HadoopStreamingJarTestCase, self).setUp()

        self.mock_paths = []

        def mock_ls(path):  # don't bother to support globs
            return (p for p in sorted(self.mock_paths) if p.startswith(path))

        self.start(patch('mrjob.fs.local.LocalFilesystem.ls',
                         side_effect=mock_ls))

        os.environ.clear()

        self.runner = HadoopJobRunner()

    def test_empty_fs(self):
        self.assertEqual(self.runner._find_hadoop_streaming_jar(), None)

    def test_deprecated_hadoop_home_option(self):
        self.runner = HadoopJobRunner(hadoop_home='/ha/do/op/home-option')

        self.mock_paths.append('/ha/do/op/home-option/hadoop-streaming.jar')
        self.assertEqual(self.runner._find_hadoop_streaming_jar(),
                         '/ha/do/op/home-option/hadoop-streaming.jar')

    def test_deprecated_hadoop_home_option_beats_hadoop_prefix(self):
        os.environ['HADOOP_PREFIX'] = '/ha/do/op/prefix'
        self.mock_paths.append('/ha/do/op/prefix/hadoop-streaming.jar')

        self.test_deprecated_hadoop_home_option()

    # tests of well-known environment variables

    def test_hadoop_prefix(self):
        os.environ['HADOOP_PREFIX'] = '/ha/do/op/prefix'
        self.mock_paths.append('/ha/do/op/prefix/hadoop-streaming.jar')

        self.assertEqual(self.runner._find_hadoop_streaming_jar(),
                         '/ha/do/op/prefix/hadoop-streaming.jar')

    def test_hadoop_prefix_beats_hadoop_home(self):
        os.environ['HADOOP_HOME'] = '/ha/do/op/home'
        self.mock_paths.append('/ha/do/op/home/hadoop-streaming.jar')

        self.test_hadoop_prefix()

    def test_hadoop_home(self):
        os.environ['HADOOP_HOME'] = '/ha/do/op/home'
        self.mock_paths.append('/ha/do/op/home/hadoop-streaming.jar')

        self.assertEqual(self.runner._find_hadoop_streaming_jar(),
                         '/ha/do/op/home/hadoop-streaming.jar')

    def test_hadoop_home_beats_hadoop_install(self):
        os.environ['HADOOP_INSTALL'] = '/ha/do/op/install'
        self.mock_paths.append('/ha/do/op/install/hadoop-streaming.jar')

        self.test_hadoop_home()

    def test_hadoop_install(self):
        os.environ['HADOOP_INSTALL'] = '/ha/do/op/install'
        self.mock_paths.append('/ha/do/op/install/hadoop-streaming.jar')

        self.assertEqual(self.runner._find_hadoop_streaming_jar(),
                         '/ha/do/op/install/hadoop-streaming.jar')

    def test_hadoop_install_beats_hadoop_mapred_home(self):
        os.environ['HADOOP_MAPRED_HOME'] = '/ha/do/op/mapred-home'
        self.mock_paths.append('/ha/do/op/mapred-home/hadoop-streaming.jar')

        self.test_hadoop_install()

    def test_hadoop_mapred_home(self):
        os.environ['HADOOP_MAPRED_HOME'] = '/ha/do/op/mapred-home'
        self.mock_paths.append('/ha/do/op/mapred-home/hadoop-streaming.jar')

        self.assertEqual(self.runner._find_hadoop_streaming_jar(),
                         '/ha/do/op/mapred-home/hadoop-streaming.jar')

    def test_hadoop_mapred_home_beats_infer_from_hadoop_bin(self):
        self.runner = HadoopJobRunner(
            hadoop_bin=['/ha/do/op/bin-parent/bin/hadoop'])

        self.mock_paths.append('/ha/do/op/bin-parent/hadoop-streaming.jar')

        self.test_hadoop_mapred_home()

    # infer from hadoop_bin

    def test_infer_from_hadoop_bin_parent_dir(self):
        self.runner = HadoopJobRunner(
            hadoop_bin=['/ha/do/op/bin-parent/bin/hadoop'])

        self.mock_paths.append('/ha/do/op/bin-parent/hadoop-streaming.jar')
        self.assertEqual(self.runner._find_hadoop_streaming_jar(),
                         '/ha/do/op/bin-parent/hadoop-streaming.jar')

    def test_hadoop_bin_beats_hadoop_anything_home(self):
        os.environ['HADOOP_ANYTHING_HOME'] = '/ha/do/op/anything-home'
        self.mock_paths.append('/ha/do/op/anything-home/hadoop-streaming.jar')

        self.test_infer_from_hadoop_bin_parent_dir()

    def test_dont_infer_from_bin_hadoop(self):
        self.runner = HadoopJobRunner(hadoop_bin=['/bin/hadoop'])
        self.mock_paths.append('/hadoop-streaming.jar')

        self.assertEqual(self.runner._find_hadoop_streaming_jar(), None)

    def test_dont_infer_from_usr_bin_hadoop(self):
        self.runner = HadoopJobRunner(hadoop_bin=['/usr/bin/hadoop'])
        self.mock_paths.append('/usr/hadoop-streaming.jar')

        self.assertEqual(self.runner._find_hadoop_streaming_jar(), None)

    def test_dont_infer_from_usr_local_bin_hadoop(self):
        self.runner = HadoopJobRunner(hadoop_bin=['/usr/local/bin/hadoop'])
        self.mock_paths.append('/usr/local/hadoop-streaming.jar')

        self.assertEqual(self.runner._find_hadoop_streaming_jar(), None)

    def test_infer_from_hadoop_bin_realpath(self):
        with patch('posixpath.realpath', return_value='/ha/do/op/bin'):
            self.runner = HadoopJobRunner(hadoop_bin=['/usr/bin/hadoop'])
            self.mock_paths.append('/ha/do/op/hadoop-streaming.jar')

            self.assertEqual(self.runner._find_hadoop_streaming_jar(),
                             '/ha/do/op/hadoop-streaming.jar')

    # tests of fallback environment variables ($HADOOP_*_HOME)

    def test_hadoop_anything_home(self):
        os.environ['HADOOP_WHATEVER_HOME'] = '/ha/do/op/whatever-home'
        self.mock_paths.append('/ha/do/op/whatever-home/hadoop-streaming.jar')

        self.assertEqual(self.runner._find_hadoop_streaming_jar(),
                         '/ha/do/op/whatever-home/hadoop-streaming.jar')

        # $HADOOP_ANYTHING_HOME comes before $HADOOP_WHATEVER_HOME
        os.environ['HADOOP_ANYTHING_HOME'] = '/ha/do/op/anything-home'
        self.mock_paths.append('/ha/do/op/anything-home/hadoop-streaming.jar')

        self.assertEqual(self.runner._find_hadoop_streaming_jar(),
                         '/ha/do/op/anything-home/hadoop-streaming.jar')

    def test_hadoop_anything_home_beats_hard_coded_paths(self):
        self.mock_paths.append('/home/hadoop/contrib/hadoop-streaming.jar')
        self.mock_paths.append(
            '/usr/lib/hadoop-mapreduce/hadoop-streaming.jar')

        self.test_hadoop_anything_home()

    # hard-coded paths (for Hadoop inside EMR)

    def test_hard_coded_emr_paths(self):
        self.mock_paths.append(
            '/usr/lib/hadoop-mapreduce/hadoop-streaming.jar')
        self.assertEqual(self.runner._find_hadoop_streaming_jar(),
                         '/usr/lib/hadoop-mapreduce/hadoop-streaming.jar')

        # /home/hadoop/contrib takes precedence
        self.mock_paths.append('/home/hadoop/contrib/hadoop-streaming.jar')
        self.assertEqual(self.runner._find_hadoop_streaming_jar(),
                         '/home/hadoop/contrib/hadoop-streaming.jar')

    # invalid environment variables

    def test_other_environment_variable(self):
        os.environ['HADOOP_YARN_MRJOB_DIR'] = '/ha/do/op/yarn-mrjob-dir'
        self.mock_paths.append(
            '/ha/do/op/yarn-mrjob-dir/hadoop-streaming.jar')

        self.assertEqual(self.runner._find_hadoop_streaming_jar(), None)

    # alternate jar names and paths

    def test_subdirs(self):
        os.environ['HADOOP_PREFIX'] = '/ha/do/op'
        self.mock_paths.append('/ha/do/op/contrib/hadoop-streaming.jar')

        self.assertEqual(self.runner._find_hadoop_streaming_jar(),
                         '/ha/do/op/contrib/hadoop-streaming.jar')

    def test_hadoop_streaming_jar_name_with_version(self):
        os.environ['HADOOP_PREFIX'] = '/ha/do/op'

        self.mock_paths.append('/ha/do/op/hadoop-streaming-2.6.0-amzn-0.jar')
        self.assertEqual(self.runner._find_hadoop_streaming_jar(),
                         '/ha/do/op/hadoop-streaming-2.6.0-amzn-0.jar')

    def test_skip_hadoop_streaming_source_jar(self):
        os.environ['HADOOP_PREFIX'] = '/ha/do/op'

        # Googled it; it really is named *-sources.jar, not *-source.jar
        self.mock_paths.append(
            '/ha/do/op/hadoop-streaming-2.0.0-mr1-cdh4.3.1-sources.jar')
        self.assertEqual(self.runner._find_hadoop_streaming_jar(), None)

    # multiple matching jars in same directory

    def test_pick_shortest_name(self):
        os.environ['HADOOP_PREFIX'] = '/ha/do/op'

        self.mock_paths.append('/ha/do/op/hadoop-streaming-1.0.3.jar')
        self.mock_paths.append('/ha/do/op/hadoop-streaming.jar')

        # hadoop-streaming-1.0.3.jar comes first in alphabetical order
        self.assertEqual(sorted(self.mock_paths), self.mock_paths)

        self.assertEqual(self.runner._find_hadoop_streaming_jar(),
                         '/ha/do/op/hadoop-streaming.jar')

    def test_pick_shallowest_subpath(self):
        os.environ['HADOOP_PREFIX'] = '/ha/do/op'

        self.mock_paths.append('/ha/do/op/hadoop-streaming-1.0.3.jar')
        self.mock_paths.append('/ha/do/op/old/hadoop-streaming.jar')

        self.assertEqual(self.runner._find_hadoop_streaming_jar(),
                         '/ha/do/op/hadoop-streaming-1.0.3.jar')

    def test_fall_back_to_alphabetical_order(self):
        os.environ['HADOOP_PREFIX'] = '/ha/do/op'

        self.mock_paths.append('/ha/do/op/hadoop-streaming-a.jar')
        self.mock_paths.append('/ha/do/op/hadoop-streaming-b.jar')

        self.assertEqual(self.runner._find_hadoop_streaming_jar(),
                         '/ha/do/op/hadoop-streaming-a.jar')

    # sanity-check that directory order overrides path sort order

    def test_directory_order_overrides_path_sort_order(self):
        os.environ['HADOOP_HOME'] = '/ha/do/op/a'
        os.environ['HADOOP_PREFIX'] = '/ha/do/op/b'

        self.mock_paths.append('/ha/do/op/a/hadoop-streaming-a.jar')
        self.mock_paths.append('/ha/do/op/b/hadoop-streaming-b.jar')

        # $HADOOP_PREFIX takes precendence over $HADOOP_HOME, so sort
        # order doesn't matter
        self.assertEqual(self.runner._find_hadoop_streaming_jar(),
                         '/ha/do/op/b/hadoop-streaming-b.jar')

        # now search in parent dir (/ha/do/op) to invoke sort order
        os.environ['HADOOP_PREFIX'] = '/ha/do/op'
        self.assertEqual(self.runner._find_hadoop_streaming_jar(),
                         '/ha/do/op/a/hadoop-streaming-a.jar')


class MockHadoopTestCase(SandboxedTestCase):

    def setUp(self):
        super(MockHadoopTestCase, self).setUp()
        # setup fake hadoop home
        hadoop_home = self.makedirs('mock_hadoop_home')
        os.environ['HADOOP_HOME'] = hadoop_home
        os.environ['MOCK_HADOOP_VERSION'] = "1.2.0"

        # make fake hadoop binary
        os.mkdir(os.path.join(hadoop_home, 'bin'))
        self.hadoop_bin = os.path.join(hadoop_home, 'bin', 'hadoop')
        create_mock_hadoop_script(self.hadoop_bin)

        # make fake streaming jar
        os.makedirs(os.path.join(hadoop_home, 'contrib', 'streaming'))
        streaming_jar_path = os.path.join(
            hadoop_home, 'contrib', 'streaming', 'hadoop-0.X.Y-streaming.jar')
        open(streaming_jar_path, 'w').close()

        # set up fake HDFS
        mock_hdfs_root = self.makedirs('mock_hdfs_root')
        os.environ['MOCK_HDFS_ROOT'] = mock_hdfs_root

        # make fake output dir
        mock_output_dir = self.makedirs('mock_hadoop_output')
        os.environ['MOCK_HADOOP_OUTPUT'] = mock_output_dir

        # set up cmd log
        mock_log_path = self.makefile('mock_hadoop_logs', '')
        os.environ['MOCK_HADOOP_LOG'] = mock_log_path

        # make sure the fake hadoop binaries can find mrjob
        self.add_mrjob_to_pythonpath()


class GetHadoopVersionTestCase(MockHadoopTestCase):

    def test_get_hadoop_version(self):
        runner = HadoopJobRunner()
        self.assertEqual(runner.get_hadoop_version(), '1.2.0')

    def test_missing_hadoop_version(self):
        with patch.dict('os.environ', MOCK_HADOOP_VERSION=''):
            runner = HadoopJobRunner()
            self.assertRaises(Exception, runner.get_hadoop_version)


class HadoopJobRunnerEndToEndTestCase(MockHadoopTestCase):

    def _test_end_to_end(self, args=()):
        # read from STDIN, a local file, and a remote file
        stdin = BytesIO(b'foo\nbar\n')

        local_input_path = os.path.join(self.tmp_dir, 'input')
        with open(local_input_path, 'w') as local_input_file:
            local_input_file.write('bar\nqux\n')

        input_to_upload = os.path.join(self.tmp_dir, 'remote_input')
        with open(input_to_upload, 'w') as input_to_upload_file:
            input_to_upload_file.write('foo\n')
        remote_input_path = 'hdfs:///data/foo'
        check_call([self.hadoop_bin,
                    'fs', '-put', input_to_upload, remote_input_path])

        # doesn't matter what the intermediate output is; just has to exist.
        add_mock_hadoop_output([b''])
        add_mock_hadoop_output([b'1\t"qux"\n2\t"bar"\n',
                                b'2\t"foo"\n5\tnull\n'])

        mr_job = MRTwoStepJob(['-r', 'hadoop', '-v',
                               '--no-conf', '--hadoop-arg', '-libjar',
                               '--hadoop-arg', 'containsJars.jar'] + list(args)
                              + ['-', local_input_path, remote_input_path]
                              + ['--jobconf', 'x=y'])
        mr_job.sandbox(stdin=stdin)

        local_tmp_dir = None
        results = []

        with mr_job.make_runner() as runner:
            assert isinstance(runner, HadoopJobRunner)

            runner.run()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

            local_tmp_dir = runner._get_local_tmp_dir()
            # make sure cleanup hasn't happened yet
            assert os.path.exists(local_tmp_dir)
            assert any(runner.fs.ls(runner.get_output_dir()))

            # make sure we're writing to the correct path in HDFS
            hdfs_root = os.environ['MOCK_HDFS_ROOT']
            self.assertEqual(sorted(os.listdir(hdfs_root)), ['data', 'user'])
            home_dir = os.path.join(hdfs_root, 'user', getpass.getuser())
            self.assertEqual(os.listdir(home_dir), ['tmp'])
            self.assertEqual(os.listdir(os.path.join(home_dir, 'tmp')),
                             ['mrjob'])
            self.assertEqual(runner._opts['hadoop_extra_args'],
                             ['-libjar', 'containsJars.jar'])

            # make sure mrjob.tar.gz is was uploaded
            self.assertTrue(os.path.exists(runner._mrjob_tar_gz_path))
            self.assertIn(runner._mrjob_tar_gz_path,
                          runner._upload_mgr.path_to_uri())

            # make sure setup script exists, and mrjob.tar.gz is added
            # to PYTHONPATH in it
            self.assertTrue(os.path.exists(runner._setup_wrapper_script_path))
            self.assertIn(runner._setup_wrapper_script_path,
                          runner._upload_mgr.path_to_uri())
            mrjob_tar_gz_name = runner._working_dir_mgr.name(
                'archive', runner._mrjob_tar_gz_path)
            with open(runner._setup_wrapper_script_path) as wrapper:
                self.assertTrue(any(
                    ('export PYTHONPATH' in line and mrjob_tar_gz_name in line)
                    for line in wrapper))

        self.assertEqual(sorted(results),
                         [(1, 'qux'), (2, 'bar'), (2, 'foo'), (5, None)])

        # make sure we called hadoop the way we expected
        with open(os.environ['MOCK_HADOOP_LOG']) as mock_log:
            hadoop_cmd_args = [shlex_split(cmd) for cmd in mock_log]

        jar_cmd_args = [cmd_args for cmd_args in hadoop_cmd_args
                        if cmd_args[:1] == ['jar']]
        self.assertEqual(len(jar_cmd_args), 2)
        step_0_args, step_1_args = jar_cmd_args

        # check input/output format
        self.assertIn('-inputformat', step_0_args)
        self.assertNotIn('-outputformat', step_0_args)
        self.assertNotIn('-inputformat', step_1_args)
        self.assertIn('-outputformat', step_1_args)

        # make sure -libjar extra arg comes before -mapper
        for args in (step_0_args, step_1_args):
            self.assertIn('-libjar', args)
            self.assertIn('-mapper', args)
            self.assertLess(args.index('-libjar'), args.index('-mapper'))

        # make sure -jobconf made it through
        self.assertIn('-D', step_0_args)
        self.assertIn('x=y', step_0_args)
        self.assertIn('-D', step_1_args)
        # job overrides jobconf in step 1
        self.assertIn('x=z', step_1_args)

        # make sure cleanup happens
        assert not os.path.exists(local_tmp_dir)
        assert not any(runner.fs.ls(runner.get_output_dir()))

    def test_end_to_end(self):
        self._test_end_to_end()

    def test_end_to_end_with_explicit_hadoop_bin(self):
        self._test_end_to_end(['--hadoop-bin', self.hadoop_bin])

    def test_end_to_end_without_pty_fork(self):
        with patch.object(pty, 'fork', side_effect=OSError()):
            self._test_end_to_end()

    def test_end_to_end_with_disabled_input_path_check(self):
        self._test_end_to_end(['--no-check-input-paths'])

    def test_end_to_end_with_hadoop_2_0(self):
        with patch.dict('os.environ', MOCK_HADOOP_VERSION='2.0.0'):
            self._test_end_to_end()


class StreamingArgsTestCase(EmptyMrjobConfTestCase):

    MRJOB_CONF_CONTENTS = {'runners': {'hadoop': {
        'hadoop_home': 'kansas',
        'hadoop_streaming_jar': 'binks.jar.jar',
    }}}

    def setUp(self):
        super(StreamingArgsTestCase, self).setUp()
        self.runner = HadoopJobRunner(
            hadoop_bin='hadoop', hadoop_streaming_jar='streaming.jar',
            mr_job_script='my_job.py', stdin=BytesIO())
        self.runner._add_job_files_for_upload()

        self.start(patch.object(self.runner, '_upload_args',
                                return_value=['new_upload_args']))
        self.start(patch.object(self.runner, '_pre_0_20_upload_args',
                                return_value=['old_upload_args']))
        self.start(patch.object(self.runner, '_hadoop_args_for_step',
                                return_value=['hadoop_args_for_step']))
        self.start(patch.object(self.runner, '_hdfs_step_input_files',
                                return_value=['hdfs_step_input_files']))
        self.start(patch.object(self.runner, '_hdfs_step_output_dir',
                                return_value='hdfs_step_output_dir'))
        self.start(patch.object(HadoopFilesystem, 'get_hadoop_version',
                                return_value='1.2.0'))
        self.runner._script_path = 'my_job.py'

        self._new_basic_args = [
            'hadoop', 'jar', 'streaming.jar',
             'new_upload_args', 'hadoop_args_for_step',
             '-input', 'hdfs_step_input_files',
             '-output', 'hdfs_step_output_dir']

        self._old_basic_args = [
            'hadoop', 'jar', 'streaming.jar',
             'hadoop_args_for_step',
             '-input', 'hdfs_step_input_files',
             '-output', 'hdfs_step_output_dir',
             'old_upload_args']

    def _assert_streaming_step(self, step, args):
        self.runner._steps = [step]
        self.assertEqual(
            self.runner._args_for_streaming_step(0),
            self._new_basic_args + args)

    def _assert_streaming_step_old(self, step, args):
        HadoopFilesystem.get_hadoop_version.return_value = '0.18'
        self.runner._steps = [step]
        self.assertEqual(
            self.runner._args_for_streaming_step(0),
            self._old_basic_args + args)

    def test_basic_mapper(self):
        self._assert_streaming_step(
            {
                'type': 'streaming',
                'mapper': {
                    'type': 'script',
                },
            },
            ['-mapper',
             PYTHON_BIN + ' my_job.py --step-num=0 --mapper',
             '-jobconf',
             'mapred.reduce.tasks=0'])

    def test_basic_reducer(self):
        self._assert_streaming_step(
            {
                'type': 'streaming',
                'reducer': {
                    'type': 'script',
                },
            },
            ['-mapper',
             'cat',
             '-reducer',
             PYTHON_BIN + ' my_job.py --step-num=0 --reducer'])

    def test_pre_filters(self):
        self._assert_streaming_step(
            {
                'type': 'streaming',
                'mapper': {
                    'type': 'script',
                    'pre_filter': 'grep anything',
                },
                'combiner': {
                    'type': 'script',
                    'pre_filter': 'grep nothing',
                },
                'reducer': {
                    'type': 'script',
                    'pre_filter': 'grep something',
                },
            },
            ["-mapper",
             "bash -c 'grep anything | " + PYTHON_BIN +
             " my_job.py --step-num=0 --mapper'",
             "-combiner",
             "bash -c 'grep nothing | " + PYTHON_BIN +
             " my_job.py --step-num=0 --combiner'",
             "-reducer",
             "bash -c 'grep something | " + PYTHON_BIN +
             " my_job.py --step-num=0 --reducer'"])

    def test_combiner_018(self):
        self._assert_streaming_step_old(
            {
                'type': 'streaming',
                'mapper': {
                    'type': 'command',
                    'command': 'cat',
                },
                'combiner': {
                    'type': 'script',
                },
            },
            ["-mapper",
             "bash -c 'cat | sort | " + PYTHON_BIN +
             " my_job.py --step-num=0 --combiner'",
             '-jobconf', 'mapred.reduce.tasks=0'])

    def test_pre_filters_018(self):
        self._assert_streaming_step_old(
            {
                'type': 'streaming',
                'mapper': {
                    'type': 'script',
                    'pre_filter': 'grep anything',
                },
                'combiner': {
                    'type': 'script',
                    'pre_filter': 'grep nothing',
                },
                'reducer': {
                    'type': 'script',
                    'pre_filter': 'grep something',
                },
            },
            ['-mapper',
             "bash -c 'grep anything | " + PYTHON_BIN +
             " my_job.py --step-num=0"
             " --mapper | sort | grep nothing | " + PYTHON_BIN +
             " my_job.py --step-num=0 --combiner'",
             '-reducer',
             "bash -c 'grep something | " + PYTHON_BIN +
             " my_job.py --step-num=0 --reducer'"])

    def test_pre_filter_escaping(self):
        # ESCAPE ALL THE THINGS!!!
        self._assert_streaming_step(
            {
                'type': 'streaming',
                'mapper': {
                    'type': 'script',
                    'pre_filter': bash_wrap("grep 'anything'"),
                },
            },
            ['-mapper',
             "bash -c 'bash -c '\\''grep"
             " '\\''\\'\\'''\\''anything'\\''\\'\\'''\\'''\\'' | " +
             PYTHON_BIN +
             " my_job.py --step-num=0 --mapper'",
             '-jobconf', 'mapred.reduce.tasks=0'])


class JarStepTestCase(MockHadoopTestCase):

    def test_local_jar(self):
        fake_jar = os.path.join(self.tmp_dir, 'fake.jar')
        open(fake_jar, 'w').close()

        job = MRJustAJar(['-r', 'hadoop', '--jar', fake_jar])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

        with open(os.environ['MOCK_HADOOP_LOG']) as hadoop_log:
            hadoop_jar_lines = [line for line in hadoop_log
                                if line.startswith('jar ')]
            self.assertEqual(len(hadoop_jar_lines), 1)
            self.assertEqual(hadoop_jar_lines[0].rstrip(), 'jar ' + fake_jar)

    def test_hdfs_jar_uri(self):
        # this could change, but for now, we pass URIs straight through
        mock_hdfs_jar = os.path.join(os.environ['MOCK_HDFS_ROOT'], 'fake.jar')
        open(mock_hdfs_jar, 'w').close()

        jar_uri = 'hdfs:///fake.jar'

        job = MRJustAJar(['-r', 'hadoop', '--jar', jar_uri])
        job.sandbox()

        with job.make_runner() as runner:
            with logger_disabled('mrjob.hadoop'):
                # `hadoop jar` doesn't actually accept URIs
                self.assertRaises(CalledProcessError, runner.run)

        with open(os.environ['MOCK_HADOOP_LOG']) as hadoop_log:
            hadoop_jar_lines = [
                line for line in hadoop_log if line.startswith('jar ')]
            self.assertEqual(len(hadoop_jar_lines), 1)
            self.assertEqual(hadoop_jar_lines[0].rstrip(), 'jar ' + jar_uri)

    def test_input_output_interpolation(self):
        fake_jar = os.path.join(self.tmp_dir, 'fake.jar')
        open(fake_jar, 'w').close()
        input1 = os.path.join(self.tmp_dir, 'input1')
        open(input1, 'w').close()
        input2 = os.path.join(self.tmp_dir, 'input2')
        open(input2, 'w').close()

        job = MRJarAndStreaming(
            ['-r', 'hadoop', '--jar', fake_jar, input1, input2])
        job.sandbox()

        add_mock_hadoop_output([b''])  # need this for streaming step

        with job.make_runner() as runner:
            runner.run()

            with open(os.environ['MOCK_HADOOP_LOG']) as hadoop_log:
                hadoop_jar_lines = [
                    line for line in hadoop_log if line.startswith('jar ')]

                self.assertEqual(len(hadoop_jar_lines), 2)
                jar_args = hadoop_jar_lines[0].rstrip().split()
                streaming_args = hadoop_jar_lines[1].rstrip().split()

                self.assertEqual(len(jar_args), 5)
                self.assertEqual(jar_args[0], 'jar')
                self.assertEqual(jar_args[1], fake_jar)
                self.assertEqual(jar_args[2], 'stuff')

                # check input is interpolated
                input_arg = ','.join(
                    runner._upload_mgr.uri(path) for path in (input1, input2))
                self.assertEqual(jar_args[3], input_arg)

                # check output of jar is input of next step
                jar_output_arg = jar_args[4]
                streaming_input_arg = streaming_args[
                    streaming_args.index('-input') + 1]
                self.assertEqual(jar_output_arg, streaming_input_arg)


class SetupLineEncodingTestCase(MockHadoopTestCase):

    def test_setup_wrapper_script_uses_local_line_endings(self):
        job = MRTwoStepJob(['-r', 'hadoop', '--setup', 'true'])
        job.sandbox(stdin=BytesIO(b''))

        add_mock_hadoop_output([b''])
        add_mock_hadoop_output([b''])

        # tests #1071. Unfortunately, we mostly run these tests on machines
        # that use unix line endings anyway. So monitor open() instead
        with patch(
                'mrjob.runner.open', create=True, side_effect=open) as m_open:
            with logger_disabled('mrjob.hadoop'):
                with job.make_runner() as runner:
                    runner.run()

                    self.assertIn(
                        call(runner._setup_wrapper_script_path, 'wb'),
                        m_open.mock_calls)
