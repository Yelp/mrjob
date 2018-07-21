# Copyright 2009-2012 Yelp
# Copyright 2013 David Marin
# Copyright 2014 Shusen Liu
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
"""Test the hadoop job runner."""
import getpass
import os
import os.path
from io import BytesIO
from subprocess import check_call
from subprocess import PIPE
from unittest import TestCase

import mrjob.step
from mrjob.conf import combine_dicts
from mrjob.fs.hadoop import HadoopFilesystem
from mrjob.hadoop import HadoopJobRunner
from mrjob.hadoop import fully_qualify_hdfs_path
from mrjob.util import bash_wrap
from mrjob.util import which

from tests.mockhadoop import add_mock_hadoop_counters
from tests.mockhadoop import add_mock_hadoop_output
from tests.mockhadoop import create_mock_hadoop_script
from tests.mockhadoop import get_mock_hadoop_cmd_args
from tests.mockhadoop import get_mock_hdfs_root
from tests.mr_jar_and_streaming import MRJarAndStreaming
from tests.mr_just_a_jar import MRJustAJar
from tests.mr_null_spark import MRNullSpark
from tests.mr_spark_jar import MRSparkJar
from tests.mr_spark_script import MRSparkScript
from tests.mr_streaming_and_spark import MRStreamingAndSpark
from tests.mr_two_step_hadoop_format_job import MRTwoStepJob
from tests.mr_word_count import MRWordCount
from tests.py2 import Mock
from tests.py2 import call
from tests.py2 import patch
from tests.quiet import logger_disabled
from tests.sandbox import EmptyMrjobConfTestCase
from tests.sandbox import SandboxedTestCase
from tests.test_runner import PYTHON_BIN

# pty isn't available on Windows
try:
    import pty
    pty
except ImportError:
    pty = Mock()


class MockHadoopTestCase(SandboxedTestCase):

    def setUp(self):
        super(MockHadoopTestCase, self).setUp()
        # setup fake hadoop home
        hadoop_home = self.makedirs('mock_hadoop_home')
        os.environ['HADOOP_HOME'] = hadoop_home
        os.environ['MOCK_HADOOP_VERSION'] = "1.2.0"
        os.environ['MOCK_HADOOP_TMP'] = self.makedirs('mock_hadoop_tmp')

        # make fake hadoop binary
        os.mkdir(os.path.join(hadoop_home, 'bin'))
        self.hadoop_bin = os.path.join(hadoop_home, 'bin', 'hadoop')
        create_mock_hadoop_script(self.hadoop_bin)

        # make fake streaming jar
        os.makedirs(os.path.join(hadoop_home, 'contrib', 'streaming'))
        streaming_jar_path = os.path.join(
            hadoop_home, 'contrib', 'streaming', 'hadoop-0.X.Y-streaming.jar')
        open(streaming_jar_path, 'w').close()

        # make sure the fake hadoop binaries can find mrjob
        self.add_mrjob_to_pythonpath()


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


class RunnerFullyQualifiesOutputPathsTestCase(MockHadoopTestCase):

    def test_output_dir(self):
        runner = HadoopJobRunner(output_dir='/path/to/output')

        self.assertEqual(runner._output_dir, 'hdfs:///path/to/output')

    def test_step_output_dir(self):
        runner = HadoopJobRunner(output_dir='/path/to/step-output')

        self.assertEqual(runner._output_dir, 'hdfs:///path/to/step-output')


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


class HadoopLogDirsTestCase(SandboxedTestCase):

    def setUp(self):
        super(HadoopLogDirsTestCase, self).setUp()

        os.environ.clear()

        self.mock_hadoop_version = '2.7.0'
        # the result of _hadoop_dir(). This handles non-log-specific
        # environment variables, such as $HADOOP_PREFIX, and also guesses
        # based on the path of the Hadoop binary
        self.mock_hadoop_dirs = []

        def mock_get_hadoop_version():
            return self.mock_hadoop_version

        def mock_hadoop_dirs_method():
            return (d for d in self.mock_hadoop_dirs)

        self.start(patch('mrjob.hadoop.HadoopJobRunner.get_hadoop_version',
                         side_effect=mock_get_hadoop_version))
        self.start(patch('mrjob.hadoop.HadoopJobRunner._hadoop_dirs',
                         side_effect=mock_hadoop_dirs_method))

        self.runner = HadoopJobRunner()

    def test_empty(self):
        self.assertEqual(list(self.runner._hadoop_log_dirs()),
                         ['hdfs:///tmp/hadoop-yarn/staging',
                          '/var/log/hadoop-yarn',
                          '/mnt/var/log/hadoop-yarn',
                          '/var/log/hadoop',
                          '/mnt/var/log/hadoop'])

    def test_precedence(self):
        os.environ['HADOOP_LOG_DIR'] = '/path/to/hadoop-log-dir'
        os.environ['YARN_LOG_DIR'] = '/path/to/yarn-log-dir'
        self.mock_hadoop_dirs = ['/path/to/hadoop-prefix',
                                 '/path/to/hadoop-home']

        self.assertEqual(
            list(self.runner._hadoop_log_dirs(output_dir='hdfs:///output/')),
            ['/path/to/hadoop-log-dir',
             '/path/to/yarn-log-dir',
             'hdfs:///tmp/hadoop-yarn/staging',
             'hdfs:///output/_logs',
             '/path/to/hadoop-prefix/logs',
             '/path/to/hadoop-home/logs',
             '/var/log/hadoop-yarn',
             '/mnt/var/log/hadoop-yarn',
             '/var/log/hadoop',
             '/mnt/var/log/hadoop'])

    def test_hadoop_log_dirs_opt(self):
        self.runner = HadoopJobRunner(hadoop_log_dirs=['/logs1', '/logs2'])

        os.environ['HADOOP_LOG_DIR'] = '/path/to/hadoop-log-dir'

        # setting hadoop_log_dirs short-circuits automatic discovery of logs
        self.assertEqual(
            list(self.runner._hadoop_log_dirs()),
            ['/logs1', '/logs2'])

    def test_need_yarn_for_yarn_log_dir_and_hdfs_log_dir(self):
        os.environ['YARN_LOG_DIR'] = '/path/to/yarn-log-dir'

        self.mock_hadoop_version = '2.0.0'
        self.assertEqual(list(self.runner._hadoop_log_dirs()),
                         ['/path/to/yarn-log-dir',
                          'hdfs:///tmp/hadoop-yarn/staging',
                          '/var/log/hadoop-yarn',
                          '/mnt/var/log/hadoop-yarn',
                          '/var/log/hadoop',
                          '/mnt/var/log/hadoop'])

        self.mock_hadoop_version = '1.0.3'
        self.assertEqual(list(self.runner._hadoop_log_dirs()),
                         ['/var/log/hadoop',
                          '/mnt/var/log/hadoop'])


class StreamingLogDirsTestCase(SandboxedTestCase):
    # tests for the _stream_*_log_dirs() methods, mocking out
    # _hadoop_log_dirs(), which is tested above

    def setUp(self):
        super(StreamingLogDirsTestCase, self).setUp()

        self.log = self.start(patch('mrjob.hadoop.log'))

        self.runner = HadoopJobRunner()
        self.runner._hadoop_log_dirs = Mock(return_value=[])
        self.runner.fs.exists = Mock(return_value=True)

        self.log.reset_mock()  # ignore logging from HadoopJobRunner init


class StreamHistoryLogDirsTestCase(StreamingLogDirsTestCase):

    def test_empty(self):
        results = self.runner._stream_history_log_dirs()

        self.assertFalse(self.log.info.called)

        self.assertRaises(StopIteration, next, results)

    def test_basic(self):
        self.runner._hadoop_log_dirs.return_value = [
            '/mnt/var/logs/hadoop', 'hdfs:///logs']

        results = self.runner._stream_history_log_dirs()

        self.assertFalse(self.log.info.called)

        self.assertEqual(next(results), ['/mnt/var/logs/hadoop'])

        self.assertEqual(self.log.info.call_count, 1)
        self.assertIn('/mnt/var/logs/hadoop', self.log.info.call_args[0][0])

        self.assertEqual(next(results), ['hdfs:///logs'])

        self.assertEqual(self.log.info.call_count, 2)
        self.assertIn('hdfs:///logs', self.log.info.call_args[0][0])

        self.assertRaises(StopIteration, next, results)

    def test_output_dir(self):
        output_dir = 'hdfs:///path/to/output'
        self.runner._hadoop_log_dirs.return_value = [output_dir]

        results = self.runner._stream_history_log_dirs(output_dir=output_dir)

        self.assertEqual(next(results), [output_dir])

        self.runner._hadoop_log_dirs.assert_called_with(output_dir=output_dir)

        self.assertRaises(StopIteration, next, results)

    def test_fs_exists(self):
        self.runner._hadoop_log_dirs.return_value = [
            '/mnt/var/logs/hadoop', 'hdfs:///logs']
        self.runner.fs.exists.return_value = False

        results = self.runner._stream_history_log_dirs()

        self.assertRaises(StopIteration, next, results)

    def test_io_error_from_fs_exists(self):
        self.runner._hadoop_log_dirs.return_value = [
            'hdfs:///tmp/output/_logs',
        ]

        self.runner.fs.exists.side_effect = IOError

        results = self.runner._stream_history_log_dirs()

        self.assertRaises(StopIteration, next, results)


class StreamTaskLogDirsTestCase(StreamingLogDirsTestCase):

    def test_empty(self):
        results = self.runner._stream_task_log_dirs()

        self.assertFalse(self.log.info.called)

        self.assertRaises(StopIteration, next, results)

    def test_basic(self):
        self.runner._hadoop_log_dirs.return_value = [
            '/mnt/var/logs/hadoop', 'hdfs:///logs']

        results = self.runner._stream_task_log_dirs()

        self.assertFalse(self.log.info.called)

        self.assertEqual(next(results), ['/mnt/var/logs/hadoop/userlogs'])

        self.assertEqual(self.log.info.call_count, 1)
        self.assertIn('/mnt/var/logs/hadoop', self.log.info.call_args[0][0])

        self.assertEqual(next(results), ['hdfs:///logs/userlogs'])

        self.assertEqual(self.log.info.call_count, 2)
        self.assertIn('hdfs:///logs/userlogs', self.log.info.call_args[0][0])

        self.assertRaises(StopIteration, next, results)

    def test_output_dir(self):
        output_dir = 'hdfs:///path/to/output'
        self.runner._hadoop_log_dirs.return_value = [output_dir]

        results = self.runner._stream_task_log_dirs(output_dir=output_dir)

        self.assertEqual(next(results), [output_dir + '/userlogs'])

        self.runner._hadoop_log_dirs.assert_called_with(output_dir=output_dir)

        self.assertRaises(StopIteration, next, results)

    def test_application_id(self):
        self.runner._hadoop_log_dirs.return_value = ['hdfs:///logs']

        results = self.runner._stream_task_log_dirs(application_id='app_1')

        self.assertEqual(next(results), ['hdfs:///logs/userlogs/app_1'])

    def test_fs_exists(self):
        self.runner._hadoop_log_dirs.return_value = [
            '/mnt/var/logs/hadoop', 'hdfs:///logs']
        self.runner.fs.exists.return_value = False

        results = self.runner._stream_task_log_dirs()

        self.assertRaises(StopIteration, next, results)

    def test_io_error_from_fs_exists(self):
        self.runner._hadoop_log_dirs.return_value = [
            'hdfs:///tmp/output/_logs',
        ]

        self.runner.fs.exists.side_effect = IOError

        results = self.runner._stream_task_log_dirs()

        self.assertRaises(StopIteration, next, results)


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

        # add counters
        add_mock_hadoop_counters({'foo': {'bar': 23}})
        add_mock_hadoop_counters({'baz': {'qux': 42}})

        # doesn't matter what the intermediate output is; just has to exist.
        add_mock_hadoop_output([b''])
        add_mock_hadoop_output([b'1\t"qux"\n2\t"bar"\n',
                                b'2\t"foo"\n5\tnull\n'])

        # -libjar is now a supported feature. Maybe -verbose?

        mr_job = MRTwoStepJob(['-r', 'hadoop', '-v',
                               '--no-conf', '--libjar', 'containsJars.jar',
                               '--hadoop-arg', '-verbose'] + list(args)
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
            hdfs_root = get_mock_hdfs_root()
            self.assertEqual(sorted(os.listdir(hdfs_root)), ['data', 'user'])
            home_dir = os.path.join(hdfs_root, 'user', getpass.getuser())
            self.assertEqual(os.listdir(home_dir), ['tmp'])
            self.assertEqual(os.listdir(os.path.join(home_dir, 'tmp')),
                             ['mrjob'])
            self.assertEqual(runner._opts['hadoop_extra_args'],
                             ['-verbose'])

            # make sure mrjob.zip was uploaded
            self.assertTrue(os.path.exists(runner._mrjob_zip_path))
            self.assertIn(runner._mrjob_zip_path,
                          runner._upload_mgr.path_to_uri())

            # make sure setup script exists, and that it adds mrjob.zip
            # to PYTHONPATH
            self.assertTrue(os.path.exists(runner._setup_wrapper_script_path))
            self.assertIn(runner._setup_wrapper_script_path,
                          runner._upload_mgr.path_to_uri())
            mrjob_zip_name = runner._working_dir_mgr.name(
                'file', runner._mrjob_zip_path)
            with open(runner._setup_wrapper_script_path) as wrapper:
                self.assertTrue(any(
                    ('export PYTHONPATH' in line and mrjob_zip_name in line)
                    for line in wrapper))

        self.assertEqual(runner.counters(),
                         [{'foo': {'bar': 23}},
                          {'baz': {'qux': 42}}])

        self.assertEqual(sorted(results),
                         [(1, 'qux'), (2, 'bar'), (2, 'foo'), (5, None)])

        # make sure we called hadoop the way we expected
        hadoop_cmd_args = get_mock_hadoop_cmd_args()

        jar_cmd_args = [cmd_args for cmd_args in hadoop_cmd_args
                        if cmd_args[:1] == ['jar']]
        self.assertEqual(len(jar_cmd_args), 2)
        step_0_args, step_1_args = jar_cmd_args

        # check input/output format
        self.assertIn('-inputformat', step_0_args)
        self.assertNotIn('-outputformat', step_0_args)
        self.assertNotIn('-inputformat', step_1_args)
        self.assertIn('-outputformat', step_1_args)

        # make sure extra arg (-verbose) comes before mapper
        for args in (step_0_args, step_1_args):
            self.assertIn('-verbose', args)
            self.assertIn('-mapper', args)
            self.assertLess(args.index('-verbose'), args.index('-mapper'))

        # make sure -libjar is set and comes before mapper
        for args in (step_0_args, step_1_args):
            self.assertIn('-libjars', args)
            self.assertIn('containsJars.jar', args)
            self.assertIn('-mapper', args)
            self.assertLess(args.index('-libjars'), args.index('-mapper'))

        # make sure -D (jobconf) made it through
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

    BASIC_HADOOP_ARGS = [
        'hadoop',
        'jar', '<streaming jar>',
        '<upload args>',
    ]

    BASIC_JOB_ARGS = [
        '<hadoop args for step>',
        '-input', '<hdfs step input files>',
        '-output', '<hdfs step output dir>',
    ]

    def setUp(self):
        super(StreamingArgsTestCase, self).setUp()
        self.runner = HadoopJobRunner(
            hadoop_bin='hadoop', hadoop_streaming_jar='<streaming jar>',
            mr_job_script='my_job.py', stdin=BytesIO())
        self.runner._add_job_files_for_upload()

        self.start(patch.object(self.runner, '_upload_args',
                                return_value=['<upload args>']))
        self.start(patch.object(self.runner, '_hadoop_args_for_step',
                                return_value=['<hadoop args for step>']))
        self.start(patch.object(self.runner, '_step_input_uris',
                                return_value=['<hdfs step input files>']))
        self.start(patch.object(self.runner, '_step_output_uri',
                                return_value='<hdfs step output dir>'))
        self.start(patch.object(HadoopFilesystem, 'get_hadoop_version',
                                return_value='2.7.1'))
        self.runner._script_path = 'my_job.py'

    def _assert_streaming_step(self, step, args):
        self.runner._steps = [step]
        self.assertEqual(
            self.runner._args_for_streaming_step(0),
            self._new_basic_args + args)

    def test_basic_mapper(self):
        self.runner._steps = [
            {
                'type': 'streaming',
                'mapper': {
                    'type': 'script',
                },
            },
        ]

        self.assertEqual(
            self.runner._args_for_streaming_step(0),
            (self.BASIC_HADOOP_ARGS +
             ['-D', 'mapreduce.job.reduces=0'] +
             self.BASIC_JOB_ARGS + [
                 '-mapper',
                 PYTHON_BIN + ' my_job.py --step-num=0 --mapper']))

    def test_basic_mapper_pre_yarn(self):
        # use a different jobconf (-D) on pre-YARN
        self.start(patch.object(HadoopFilesystem, 'get_hadoop_version',
                                return_value='1.0.3'))

        self.runner._steps = [
            {
                'type': 'streaming',
                'mapper': {
                    'type': 'script',
                },
            },
        ]

        self.assertEqual(
            self.runner._args_for_streaming_step(0),
            (self.BASIC_HADOOP_ARGS +
             ['-D', 'mapred.reduce.tasks=0'] +
             self.BASIC_JOB_ARGS + [
                 '-mapper',
                 PYTHON_BIN + ' my_job.py --step-num=0 --mapper']))

    def test_basic_reducer(self):
        self.runner._steps = [
            {
                'type': 'streaming',
                'reducer': {
                    'type': 'script',
                },
            },
        ]

        self.assertEqual(
            self.runner._args_for_streaming_step(0),
            (self.BASIC_HADOOP_ARGS + self.BASIC_JOB_ARGS + [
                '-mapper',
                'cat',
                '-reducer',
                PYTHON_BIN + ' my_job.py --step-num=0 --reducer']))

    def test_pre_filters(self):
        self.runner._steps = [
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
        ]

        self.assertEqual(
            self.runner._args_for_streaming_step(0),
            (self.BASIC_HADOOP_ARGS + self.BASIC_JOB_ARGS + [
             '-mapper',
             "sh -ex -c 'grep anything | " + PYTHON_BIN +
             " my_job.py --step-num=0 --mapper'",
             '-combiner',
             "sh -ex -c 'grep nothing | " + PYTHON_BIN +
             " my_job.py --step-num=0 --combiner'",
             '-reducer',
             "sh -ex -c 'grep something | " + PYTHON_BIN +
             " my_job.py --step-num=0 --reducer'"]))

    def test_pre_filter_escaping(self):
        # ESCAPE ALL THE THINGS!!!
        self.runner._steps = [
            {
                'type': 'streaming',
                'mapper': {
                    'type': 'script',
                    'pre_filter': bash_wrap("grep 'anything'"),
                },
            },
        ]

        self.assertEqual(
            self.runner._args_for_streaming_step(0),
            (self.BASIC_HADOOP_ARGS +
             ['-D', 'mapreduce.job.reduces=0'] +
             self.BASIC_JOB_ARGS + [
                 '-mapper',
                 "sh -ex -c 'bash -c '\\''grep"
                 " '\\''\\'\\'''\\''anything'\\''\\'\\'''\\'''\\'' | " +
                 PYTHON_BIN +
                 " my_job.py --step-num=0 --mapper'"]))


class ArgsForJarStepTestCase(MockHadoopTestCase):

    def test_local_jar(self):
        fake_jar = self.makefile('fake.jar')

        job = MRJustAJar(['-r', 'hadoop', '--jar', fake_jar])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._args_for_jar_step(0),
                runner.get_hadoop_bin() +
                ['jar', fake_jar])

    def test_hdfs_jar_uri(self):
        # this could change, but for now, we pass URIs straight through
        mock_hdfs_jar = os.path.join(get_mock_hdfs_root(), 'fake.jar')
        open(mock_hdfs_jar, 'w').close()

        jar_uri = 'hdfs:///fake.jar'

        job = MRJustAJar(['-r', 'hadoop', '--jar', jar_uri])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._args_for_jar_step(0),
                runner.get_hadoop_bin() +
                ['jar', jar_uri])

    def test_libjars(self):
        fake_jar = self.makefile('fake.jar')
        fake_libjar = self.makefile('fake_lib.jar')

        job = MRJustAJar(
            ['-r', 'hadoop', '--jar', fake_jar, '--libjar', fake_libjar])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._args_for_jar_step(0),
                runner.get_hadoop_bin() +
                ['-libjars', fake_libjar, 'jar', fake_jar])

    def test_jobconf(self):
        fake_jar = self.makefile('fake.jar')

        job = MRJustAJar(
            ['-r', 'hadoop', '--jobconf', 'foo=bar', '--jar', fake_jar])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._args_for_jar_step(0),
                runner.get_hadoop_bin() +
                ['-D', 'foo=bar', 'jar', fake_jar])

    def test_input_output_interpolation(self):
        # TODO: rewrite this to just check the step args (see #1482)
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

            hadoop_cmd_args = get_mock_hadoop_cmd_args()

            hadoop_jar_cmd_args = [args for args in hadoop_cmd_args if
                                   args and args[0] == 'jar']

            self.assertEqual(len(hadoop_jar_cmd_args), 2)
            jar_args, streaming_args = hadoop_jar_cmd_args

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


class SparkStepArgsTestCase(SandboxedTestCase):

    MRJOB_CONF_CONTENTS = dict(runners=dict(hadoop=dict(
        spark_submit_bin='spark-submit')))

    def setUp(self):
        super(SparkStepArgsTestCase, self).setUp()

        # _spark_submit_args() is tested elsewhere
        self.start(patch(
            'mrjob.runner.MRJobRunner._spark_submit_args',
            return_value=['<spark submit args>']))

    def test_spark_step(self):
        job = MRNullSpark([
            '-r', 'hadoop',
        ])
        job.sandbox()

        with job.make_runner() as runner:
            runner._add_job_files_for_upload()

            self.assertEqual(runner._args_for_step(0), [
                'spark-submit',
                '<spark submit args>',
                runner._script_path,
                '--step-num=0',
                '--spark',
                ','.join(runner._step_input_uris(0)),
                runner._step_output_uri(0)
            ])

    def test_spark_streaming_step(self):
        job = MRSparkScript([
            '-r', 'hadoop',
            '--script', '/path/to/spark_script.py',
            '--script-arg', 'foo',
            '--script-arg', mrjob.step.OUTPUT,
            '--script-arg', mrjob.step.INPUT,
        ])
        job.sandbox()

        with job.make_runner() as runner:
            runner._add_job_files_for_upload()

            self.assertEqual(runner._args_for_step(0), [
                'spark-submit',
                '<spark submit args>',
                '/path/to/spark_script.py',
                'foo',
                runner._step_output_uri(0),
                ','.join(runner._step_input_uris(0)),
            ])


class EnvForStepTestCase(MockHadoopTestCase):

    def setUp(self):
        super(EnvForStepTestCase, self).setUp()
        os.environ.clear()  # for less noisy test failures

    def test_streaming_step(self):
        job = MRTwoStepJob(['-r', 'hadoop', '--cmdenv', 'FOO=bar'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._env_for_step(0),
                dict(os.environ)
            )

    def test_jar_step(self):
        job = MRJustAJar(['-r', 'hadoop', '--cmdenv', 'FOO=bar'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._env_for_step(0),
                dict(os.environ)
            )

    def test_spark_step(self):
        job = MRNullSpark(['-r', 'hadoop', '--cmdenv', 'FOO=bar'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._env_for_step(0),
                combine_dicts(os.environ,
                              dict(FOO='bar', PYSPARK_PYTHON=PYTHON_BIN))
            )

    def test_spark_jar_step(self):
        job = MRSparkJar(['-r', 'hadoop', '--cmdenv', 'FOO=bar'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._env_for_step(0),
                combine_dicts(os.environ, dict(FOO='bar'))
            )

    def test_spark_script_step(self):
        job = MRSparkScript(['-r', 'hadoop', '--cmdenv', 'FOO=bar'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(
                runner._env_for_step(0),
                combine_dicts(os.environ,
                              dict(FOO='bar', PYSPARK_PYTHON=PYTHON_BIN))
            )


class RunJobInHadoopUsesEnvTestCase(MockHadoopTestCase):

    def setUp(self):
        super(RunJobInHadoopUsesEnvTestCase, self).setUp()

        self.mock_get_steps = self.start(patch(
            'mrjob.hadoop.HadoopJobRunner._get_steps',
            return_value=[dict(type='spark')]))

        self.mock_args_for_step = self.start(patch(
            'mrjob.hadoop.HadoopJobRunner._args_for_step',
            return_value=['args', 'for', 'step']))

        self.mock_env_for_step = self.start(patch(
            'mrjob.hadoop.HadoopJobRunner._env_for_step',
            return_value=dict(FOO='bar', BAZ='qux')))

        self.mock_pty_fork = self.start(patch.object(
            pty, 'fork', return_value=(0, None)))

        # end test once we invoke Popen or execvpe()
        self.mock_Popen = self.start(patch(
            'mrjob.hadoop.Popen', side_effect=StopIteration))

        self.mock_execvpe = self.start(patch(
            'os.execvpe', side_effect=StopIteration))

    def test_with_pty(self):
        job = MRNullSpark(['-r', 'hadoop'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertRaises(StopIteration, runner._run_job_in_hadoop)

            self.mock_execvpe.assert_called_once_with(
                'args', ['args', 'for', 'step'], dict(FOO='bar', BAZ='qux'))

            self.assertFalse(self.mock_Popen.called)

    def test_without_pty(self):
        self.mock_pty_fork.side_effect = OSError

        job = MRNullSpark(['-r', 'hadoop'])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertRaises(StopIteration, runner._run_job_in_hadoop)

            self.mock_Popen.assert_called_once_with(
                ['args', 'for', 'step'],
                stdout=PIPE, stderr=PIPE, env=dict(FOO='bar', BAZ='qux'))

            self.assertFalse(self.mock_execvpe.called)


class SparkPyFilesTestCase(MockHadoopTestCase):

    def test_eggs(self):
        egg1_path = self.makefile('dragon.egg')
        egg2_path = self.makefile('horton.egg')

        job = MRNullSpark([
            '-r', 'hadoop',
            '--py-file', egg1_path, '--py-file', egg2_path])
        job.sandbox()

        with job.make_runner() as runner:
            runner._add_job_files_for_upload()

            self.assertEqual(
                runner._spark_py_files(),
                [egg1_path, egg2_path, runner._create_mrjob_zip()]
            )

            # the py_files get uploaded anyway since they appear in
            # _upload_dir_mgr.
            self.assertIn(egg1_path, runner._upload_mgr.path_to_uri())
            self.assertIn(egg2_path, runner._upload_mgr.path_to_uri())


class SetupLineEncodingTestCase(MockHadoopTestCase):

    def test_setup_wrapper_script_uses_local_line_endings(self):
        job = MRTwoStepJob(['-r', 'hadoop', '--setup', 'true'])
        job.sandbox()

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


class PickErrorTestCase(MockHadoopTestCase):

    # integration tests for _pick_error()

    def setUp(self):
        super(PickErrorTestCase, self).setUp()

        os.environ['MOCK_HADOOP_VERSION'] = '2.7.0'

        self.runner = HadoopJobRunner()

    def test_empty(self):
        self.assertEqual(self.runner._pick_error({}, 'streaming'), None)

    def test_yarn_python_exception(self):
        APPLICATION_ID = 'application_1450486922681_0004'
        CONTAINER_ID = 'container_1450486922681_0005_01_000003'
        JOB_ID = 'job_1450486922681_0004'

        log_subdir = os.path.join(
            os.environ['HADOOP_HOME'], 'logs',
            'userlogs', APPLICATION_ID, CONTAINER_ID)

        os.makedirs(log_subdir)

        syslog_path = os.path.join(log_subdir, 'syslog')
        with open(syslog_path, 'w') as syslog:
            syslog.write(
                '2015-12-21 14:06:17,707 INFO [main]'
                ' org.apache.hadoop.mapred.MapTask: Processing split:'
                ' hdfs://e4270474c8ee:9000/user/root/tmp/mrjob'
                '/mr_boom.root.20151221.190511.059097/files'
                '/bootstrap.sh:0+335\n')
            syslog.write(
                '2015-12-21 14:06:18,538 WARN [main]'
                ' org.apache.hadoop.mapred.YarnChild: Exception running child'
                ' : java.lang.RuntimeException:'
                ' PipeMapRed.waitOutputThreads(): subprocess failed with'
                ' code 1\n')
            syslog.write(
                '        at org.apache.hadoop.streaming.PipeMapRed'
                '.waitOutputThreads(PipeMapRed.java:322)\n')

        stderr_path = os.path.join(log_subdir, 'stderr')
        with open(stderr_path, 'w') as stderr:
            stderr.write('+ python mr_boom.py --mapper')
            stderr.write('Traceback (most recent call last):\n')
            stderr.write('  File "mr_boom.py", line 10, in <module>\n')
            stderr.write('    MRBoom.run()\n')
            stderr.write('Exception: BOOM\n')

        error = self.runner._pick_error(
            dict(step=dict(application_id=APPLICATION_ID, job_id=JOB_ID)),
            'streaming',
        )

        self.assertIsNotNone(error)

        self.assertEqual(error['hadoop_error']['path'], syslog_path)
        self.assertEqual(error['task_error']['path'], stderr_path)


class HadoopExtraArgsTestCase(MockHadoopTestCase):

    # moved from tests.test_runner.HadoopArgsForStepTestCase because
    # hadoop_extra_args isn't defined in the base runner

    RUNNER = 'hadoop'

    def test_hadoop_extra_args(self):
        # hadoop_extra_args doesn't exist in default runner
        job = MRWordCount(['-r', self.RUNNER, '--hadoop-arg', '-foo'])
        with job.make_runner() as runner:
            self.assertEqual(runner._hadoop_args_for_step(0), ['-foo'])

    def test_hadoop_extra_args_comes_after_jobconf(self):
        job = MRWordCount(
            ['-r', self.RUNNER,
             '--cmdenv', 'FOO=bar',
             '--hadoop-arg', '-libjar', '--hadoop-arg', 'qux.jar',
             '--jobconf', 'baz=qux',
             '--partitioner', 'java.lang.Object'])
        job.HADOOP_INPUT_FORMAT = 'FooInputFormat'
        job.HADOOP_OUTPUT_FORMAT = 'BarOutputFormat'

        with job.make_runner() as runner:
            hadoop_args = runner._hadoop_args_for_step(0)
            self.assertEqual(
                hadoop_args[:4],
                ['-D', 'baz=qux', '-libjar', 'qux.jar'])
            self.assertEqual(len(hadoop_args), 12)


class LibjarsTestCase(MockHadoopTestCase):

    def test_empty(self):
        job = MRWordCount(['-r', 'hadoop'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._add_job_files_for_upload()
            args = runner._args_for_streaming_step(0)

            self.assertNotIn('-libjars', args)

    def test_one_jar(self):
        job = MRWordCount([
            '-r', 'hadoop',
            '--libjar', '/path/to/a.jar',
        ])
        job.sandbox()

        with job.make_runner() as runner:
            runner._add_job_files_for_upload()
            args = runner._args_for_streaming_step(0)

            self.assertIn('-libjars', args)
            self.assertIn('/path/to/a.jar', args)

    def test_two_jars(self):
        job = MRWordCount([
            '-r', 'hadoop',
            '--libjar', '/path/to/a.jar',
            '--libjar', '/path/to/b.jar',
        ])
        job.sandbox()

        with job.make_runner() as runner:
            runner._add_job_files_for_upload()
            args = runner._args_for_streaming_step(0)

            self.assertIn('-libjars', args)
            self.assertIn('/path/to/a.jar,/path/to/b.jar', args)


class FindSparkSubmitBinTestCase(SandboxedTestCase):

    def setUp(self):
        super(FindSparkSubmitBinTestCase, self).setUp()

        # track calls to which()
        self.which = self.start(patch('mrjob.hadoop.which', wraps=which))

        # keep which() from searching in /bin, etc.
        os.environ['PATH'] = self.tmp_dir

        # create basic runner (okay to overwrite)
        self.runner = HadoopJobRunner()

    def test_do_nothing_on_init(self):
        self.assertFalse(self.which.called)

    def test_option_short_circuits_search(self):
        self.runner = HadoopJobRunner(
            spark_submit_bin=['/path/to/spark-submit', '-v'])

        self.assertEqual(self.runner.get_spark_submit_bin(),
                         ['/path/to/spark-submit', '-v'])

        self.assertFalse(self.which.called)

    def test_fallback_and_hard_coded_dirs(self):
        # don't get caught by real spark install
        self.which.return_value = None

        os.environ['SPARK_HOME'] = '/spark/home'

        self.assertEqual(self.runner.get_spark_submit_bin(), ['spark-submit'])

        which_paths = [
            kwargs.get('path') for args, kwargs in self.which.call_args_list]

        self.assertEqual(which_paths, [
            '/spark/home/bin',
            None,
            '/usr/lib/spark/bin',
            '/usr/local/spark/bin',
            '/usr/local/lib/spark/bin',
        ])

    def test_find_in_spark_home(self):
        spark_submit_bin = self.makefile(
            os.path.join(self.tmp_dir, 'spark', 'bin', 'spark-submit'),
            executable=True)

        os.environ['SPARK_HOME'] = os.path.join(self.tmp_dir, 'spark')

        self.runner.get_spark_submit_bin()

        self.assertEqual(self.runner.get_spark_submit_bin(),
                         [spark_submit_bin])

        self.assertEqual(self.which.call_count, 1)

    def test_find_in_path(self):
        spark_submit_bin = self.makefile(
            os.path.join(self.tmp_dir, 'bin', 'spark-submit'),
            executable=True)

        os.environ['PATH'] = os.path.join(self.tmp_dir, 'bin')

        # don't get caught by real $SPARK_HOME
        if 'SPARK_HOME' in os.environ:
            del os.environ['SPARK_HOME']

        self.assertEqual(self.runner.get_spark_submit_bin(),
                         [spark_submit_bin])


class FindBinariesAndJARsTestCase(SandboxedTestCase):

    def setUp(self):
        super(FindBinariesAndJARsTestCase, self).setUp()

        self.get_hadoop_version = self.start(patch(
            'mrjob.hadoop.HadoopJobRunner.get_hadoop_version'))

        self.get_hadoop_streaming_jar = self.start(patch(
            'mrjob.hadoop.HadoopJobRunner.get_hadoop_streaming_jar'))

        self.get_spark_submit_bin = self.start(patch(
            'mrjob.hadoop.HadoopJobRunner.get_spark_submit_bin'))

    def test_always_call_get_hadoop_version(self):
        runner = HadoopJobRunner()

        runner._find_binaries_and_jars()

        self.assertTrue(self.get_hadoop_version.called)
        self.assertFalse(self.get_hadoop_streaming_jar.called)
        self.assertFalse(self.get_spark_submit_bin.called)

    def test_streaming_steps(self):
        job = MRTwoStepJob(['-r', 'hadoop'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._find_binaries_and_jars()

            self.assertTrue(self.get_hadoop_version.called)
            self.assertTrue(self.get_hadoop_streaming_jar.called)
            self.assertFalse(self.get_spark_submit_bin.called)

    def test_spark_steps(self):
        job = MRNullSpark(['-r', 'hadoop'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._find_binaries_and_jars()

            self.assertTrue(self.get_hadoop_version.called)
            self.assertFalse(self.get_hadoop_streaming_jar.called)
            self.assertTrue(self.get_spark_submit_bin.called)

    def test_streaming_and_spark(self):
        job = MRStreamingAndSpark(['-r', 'hadoop'])
        job.sandbox()

        with job.make_runner() as runner:
            runner._find_binaries_and_jars()

            self.assertTrue(self.get_hadoop_version.called)
            self.assertTrue(self.get_hadoop_streaming_jar.called)
            self.assertTrue(self.get_spark_submit_bin.called)


class SparkSubmitArgPrefixTestCase(MockHadoopTestCase):

    def test_default(self):
        runner = HadoopJobRunner()

        self.assertEqual(
            runner._spark_submit_arg_prefix(),
            ['--master', 'yarn'])

    def test_spark_master(self):
        runner = HadoopJobRunner(spark_master='local')

        self.assertEqual(
            runner._spark_submit_arg_prefix(),
            ['--master', 'local'])


class WarnAboutSparkArchivesTestCase(MockHadoopTestCase):

    def setUp(self):
        super(WarnAboutSparkArchivesTestCase, self).setUp()

        self.log = self.start(patch('mrjob.hadoop.log'))

    def test_warning(self):
        fake_archive = self.makefile('fake.tar.gz')

        job = MRNullSpark(['-r', 'hadoop',
                           '--spark-master', 'local',
                           '--archive', fake_archive])
        job.sandbox()

        with job.make_runner() as runner:
            runner._warn_about_spark_archives(runner._get_step(0))

        self.assertTrue(self.log.warning.called)

    def test_requires_spark(self):
        fake_archive = self.makefile('fake.tar.gz')

        job = MRTwoStepJob(['-r', 'hadoop',
                            '--spark-master', 'local',
                            '--archive', fake_archive])
        job.sandbox()

        with job.make_runner() as runner:
            runner._warn_about_spark_archives(runner._get_step(0))

        self.assertFalse(self.log.warning.called)

    def test_requires_non_yarn_spark_master(self):
        fake_archive = self.makefile('fake.tar.gz')

        job = MRNullSpark(['-r', 'hadoop',
                           '--spark-master', 'yarn',
                           '--archive', fake_archive])
        job.sandbox()

        with job.make_runner() as runner:
            runner._warn_about_spark_archives(runner._get_step(0))

        self.assertFalse(self.log.warning.called)

    def test_requires_archives(self):
        fake_file = self.makefile('fake.txt')

        job = MRNullSpark(['-r', 'hadoop',
                           '--spark-master', 'local',
                           '--file', fake_file])
        job.sandbox()

        with job.make_runner() as runner:
            runner._warn_about_spark_archives(runner._get_step(0))

        self.assertFalse(self.log.warning.called)
