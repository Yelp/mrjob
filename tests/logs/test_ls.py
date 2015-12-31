# -*- encoding: utf-8 -*-
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
from tests.py2 import TestCase

from mrjob.logs.ls import _JOB_LOG_PATH_RE
from mrjob.logs.ls import _PRE_YARN_TASK_SYSLOG_RE
from mrjob.logs.ls import _TASK_LOG_PATH_RE
from mrjob.logs.ls import _YARN_TASK_SYSLOG_RE
from mrjob.logs.ls import _stderr_for_syslog
from mrjob.logs.ls import _ls_logs
from mrjob.logs.ls import _ls_pre_yarn_task_syslogs
from mrjob.logs.ls import _ls_yarn_task_syslogs
from mrjob.py2 import StringIO
from mrjob.util import log_to_stream

from tests.py2 import Mock
from tests.py2 import patch
from tests.quiet import no_handlers_for_logger
from tests.sandbox import PatcherTestCase


class LogRegexTestCase(TestCase):

    def test_job_log_path_re_on_2_x_ami(self):
        uri = 'ssh://ec2-52-88-7-250.us-west-2.compute.amazonaws.com/mnt/var/log/hadoop/history/done/version-1/ip-172-31-29-201.us-west-2.compute.internal_1441062912502_/2015/08/31/000000/job_201508312315_0011_1441062985499_hadoop_streamjob1474198573915234945.jar'  # noqa

        m = _JOB_LOG_PATH_RE.match(uri)

        self.assertTrue(m)
        self.assertEqual(m.group('timestamp'), '201508312315')
        self.assertEqual(m.group('step_num'), '0011')
        self.assertEqual(m.group('user'), 'hadoop')

    def test_job_log_path_re_on_3_x_ami(self):
        uri = 'ssh://ec2-52-24-131-73.us-west-2.compute.amazonaws.com/mnt/var/log/hadoop/history/2015/08/31/000000/job_1441057410014_0011-1441057493406-hadoop-streamjob6928722756977481487.jar-1441057604210-2-1-SUCCEEDED-default-1441057523674.jhist'  # noqa

        m = _JOB_LOG_PATH_RE.match(uri)

        self.assertTrue(m)
        self.assertEqual(m.group('timestamp'), '1441057410014')
        self.assertEqual(m.group('step_num'), '0011')
        self.assertEqual(m.group('user'), 'hadoop')

    def test_task_re_on_2_x_ami(self):
        uri = 's3://mrjob-35cdec11663cb1cb/tmp/logs/j-3J3Y9EBUUBRFW/task-attempts/job_201508312315_0002/attempt_201508312315_0002_m_000000_0/syslog'  # noqa

        m = _TASK_LOG_PATH_RE.match(uri)

        self.assertTrue(m)
        self.assertEqual(m.group('timestamp'), '201508312315')
        self.assertEqual(m.group('step_num'), '0002')
        self.assertEqual(m.group('task_type'), 'm')
        self.assertEqual(m.group('yarn_attempt_num'), None)
        self.assertEqual(m.group('task_num'), '000000')
        self.assertEqual(m.group('attempt_num'), '0')
        self.assertEqual(m.group('stream'), 'syslog')

    def test_task_re_on_3_x_ami(self):
        uri = 's3://mrjob-35cdec11663cb1cb/tmp/logs/j-21QKHYM5WJJHS/task-attempts/application_1441057410014_0001/container_1441057410014_0001_01_000004/stderr.gz'  # noqa

        m = _TASK_LOG_PATH_RE.match(uri)

        self.assertTrue(m)
        self.assertEqual(m.group('timestamp'), '1441057410014')
        self.assertEqual(m.group('step_num'), '0001')
        self.assertEqual(m.group('task_type'), None)
        self.assertEqual(m.group('yarn_attempt_num'), '01')
        self.assertEqual(m.group('task_num'), '000004')
        self.assertEqual(m.group('attempt_num'), None)
        self.assertEqual(m.group('stream'), 'stderr')

    def test_yarn_task_syslog_re(self):
        uri = '/usr/local/hadoop-2.7.0/logs/userlogs/application_1450486922681_0005/container_1450486922681_0005_01_000003/syslog'  # noqa

        m = _YARN_TASK_SYSLOG_RE.match(uri)

        self.assertTrue(m)
        self.assertEqual(m.group('prefix'),
                         '/usr/local/hadoop-2.7.0/logs/userlogs/')
        self.assertEqual(m.group('application_id'),
                         'application_1450486922681_0005')
        self.assertEqual(m.group('container_id'),
                         'container_1450486922681_0005_01_000003')
        self.assertEqual(m.group('suffix'),
                         None)

    def test_yarn_task_syslog_re_on_gz(self):
        uri = '/usr/local/hadoop-2.7.0/logs/userlogs/application_1450486922681_0005/container_1450486922681_0005_01_000003/syslog.gz'  # noqa

        m = _YARN_TASK_SYSLOG_RE.match(uri)

        self.assertTrue(m)
        self.assertEqual(m.group('prefix'),
                         '/usr/local/hadoop-2.7.0/logs/userlogs/')
        self.assertEqual(m.group('application_id'),
                         'application_1450486922681_0005')
        self.assertEqual(m.group('container_id'),
                         'container_1450486922681_0005_01_000003')
        self.assertEqual(m.group('suffix'), '.gz')

    def test_pre_yarn_task_syslog_re(self):
        uri = '/mnt/var/log/hadoop/userlogs/job_201512232143_0006/attempt_201512232143_0006_m_000000_0/syslog'  # noqa

        m = _PRE_YARN_TASK_SYSLOG_RE.match(uri)

        self.assertTrue(m)
        self.assertEqual(m.group('prefix'),
                         '/mnt/var/log/hadoop/userlogs/job_201512232143_0006/')
        self.assertEqual(m.group('timestamp'), '201512232143')
        self.assertEqual(m.group('step_num'), '0006')
        self.assertEqual(m.group('task_type'), 'm')
        self.assertEqual(m.group('task_num'), '000000')
        self.assertEqual(m.group('attempt_num'), '0')
        self.assertEqual(m.group('suffix'), None)

    def test_pre_yarn_task_syslog_re_on_gz(self):
        uri = '/mnt/var/log/hadoop/userlogs/job_201512232143_0006/attempt_201512232143_0006_m_000000_0/syslog.gz'  # noqa

        m = _PRE_YARN_TASK_SYSLOG_RE.match(uri)

        self.assertTrue(m)
        self.assertEqual(m.group('prefix'),
                         '/mnt/var/log/hadoop/userlogs/job_201512232143_0006/')
        self.assertEqual(m.group('timestamp'), '201512232143')
        self.assertEqual(m.group('step_num'), '0006')
        self.assertEqual(m.group('task_type'), 'm')
        self.assertEqual(m.group('task_num'), '000000')
        self.assertEqual(m.group('attempt_num'), '0')
        self.assertEqual(m.group('suffix'), '.gz')


class StderrForSyslogTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(_stderr_for_syslog(''), 'stderr')

    def test_no_stem(self):
        self.assertEqual(_stderr_for_syslog('/path/to/syslog'),
                         '/path/to/stderr')

    def test_gz(self):
        self.assertEqual(_stderr_for_syslog('/path/to/syslog.gz'),
                        '/path/to/stderr.gz')

    def test_doesnt_check_filename(self):
        self.assertEqual(_stderr_for_syslog('/path/to/garden'),
                         '/path/to/stderr')


class LsLogsTestCase(TestCase):

    def setUp(self):
        super(LsLogsTestCase, self).setUp()

        self.mock_fs = Mock()
        self.mock_paths = []

        def mock_fs_ls(path):
            # we just ignore path, keeping it simple
            for p in self.mock_paths:
                if isinstance(p, Exception):
                    raise p
                else:
                    yield p

        self.mock_fs.ls = Mock(side_effect=mock_fs_ls)

    def test_empty(self):
        self.assertEqual(list(_ls_logs(self.mock_fs, '/path/to/logs')),
                         [])
        self.mock_fs.ls.assert_called_once_with('/path/to/logs')

    def test_paths(self):
        self.mock_paths = [
            '/path/to/logs/oak',
            '/path/to/logs/pine',
            '/path/to/logs/redwood',
        ]
        self.assertEqual(list(_ls_logs(self.mock_fs, '/path/to/logs')),
                         self.mock_paths)

        self.mock_fs.ls.assert_called_once_with('/path/to/logs')

    def test_io_error(self):
        self.mock_paths = [
            IOError(),
        ]

        with no_handlers_for_logger('mrjob.logs.ls'):
            stderr = StringIO()
            log_to_stream('mrjob.logs.ls', stderr)

            self.assertEqual(list(_ls_logs(self.mock_fs, '/path/to/logs')), [])

            self.mock_fs.ls.assert_called_once_with('/path/to/logs')

            self.assertIn("couldn't ls() /path/to/logs", stderr.getvalue())


class LsTaskSyslogsTestCase(PatcherTestCase):

    def setUp(self):
        super(LsTaskSyslogsTestCase, self).setUp()

        self.mock_fs = 'MOCK_FS'
        self.mock_paths = []

        def mock_ls_logs(fs, log_dir):
            log_dir = log_dir.rstrip('/') + '/'
            for path in self.mock_paths:
                if path.startswith(log_dir):
                    yield path

        self.start(patch('mrjob.logs.ls._ls_logs', mock_ls_logs))


class LsYarnTaskSyslogsTestCase(LsTaskSyslogsTestCase):

    def test_no_log_dirs(self):
        self.assertEqual(_ls_yarn_task_syslogs(self.mock_fs, []), [])

    def test_filter_and_sort(self):
        self.mock_paths = [
            '/log/dir/userlogs/application_1450486922681_0004'
            '/container_1450486922681_0005_01_000003/syslog',
            '/log/dir/userlogs/application_1450486922681_0005'
            '/container_1450486922681_0005_01_000004/syslog',
            '/log/dir/userlogs/application_1450486922681_0005'
            '/container_1450486922681_0005_01_000003/syslog',
            '/log/dir/userlogs/application_1450486922681_0005'
            '/container_1450486922681_0005_01_000003/stderr',
            '/log/dir/random-crud',
        ]

        # should be sorted in reverse order by app and container ID
        self.assertEqual(
            _ls_yarn_task_syslogs(
                self.mock_fs, ['/log/dir']),
            ['/log/dir/userlogs/application_1450486922681_0005'
             '/container_1450486922681_0005_01_000004/syslog',
             '/log/dir/userlogs/application_1450486922681_0005'
             '/container_1450486922681_0005_01_000003/syslog',
             '/log/dir/userlogs/application_1450486922681_0004'
             '/container_1450486922681_0005_01_000003/syslog'])

        # test filter by application ID
        self.assertEqual(
            _ls_yarn_task_syslogs(
                self.mock_fs, ['/log/dir'],
                application_id='application_1450486922681_0004'),
            ['/log/dir/userlogs/application_1450486922681_0004'
             '/container_1450486922681_0005_01_000003/syslog'])

        # test subdir
        self.assertEqual(
            _ls_yarn_task_syslogs(
                self.mock_fs,
                ['/log/dir/userlogs/application_1450486922681_0005']),
            ['/log/dir/userlogs/application_1450486922681_0005'
             '/container_1450486922681_0005_01_000004/syslog',
             '/log/dir/userlogs/application_1450486922681_0005'
             '/container_1450486922681_0005_01_000003/syslog'])

    # reading from multiple dirs is handled by code shared with
    # _ls_pre_yarn_task_syslogs(), and thus is tested below


class LsPreYarnTaskSyslogsTestCase(LsTaskSyslogsTestCase):

    def test_no_log_dirs(self):
        self.assertEqual(_ls_pre_yarn_task_syslogs(self.mock_fs, []), [])

    def test_filter_and_sort(self):
        # on EMR, looks like attempts are grouped in subdirectories

        self.mock_paths = [
            '/userlogs/attempt_201512232143_0008_m_000001_3/syslog',
            '/userlogs/attempt_201512232143_0008_r_000000_0/syslog',
            '/userlogs/attempt_201512232143_0008_m_000003_1/syslog',
            '/userlogs/attempt_201512232143_0006_m_000000_0/syslog',
            '/userlogs/attempt_201512232143_0006_m_000000_0/stderr',
            '/userlogs/random-crud',
        ]

        # should be sorted in reverse order by app and container ID
        self.assertEqual(
            _ls_pre_yarn_task_syslogs(
                self.mock_fs, ['/userlogs']), [
                    '/userlogs/attempt_201512232143_0008_r_000000_0/syslog',
                    '/userlogs/attempt_201512232143_0008_m_000001_3/syslog',
                    '/userlogs/attempt_201512232143_0008_m_000003_1/syslog',
                    '/userlogs/attempt_201512232143_0006_m_000000_0/syslog',
                ])

        # test filter by job ID
        self.assertEqual(
            _ls_pre_yarn_task_syslogs(
                self.mock_fs, ['/userlogs'],
                job_id='job_201512232143_0006'),
            ['/userlogs/attempt_201512232143_0006_m_000000_0/syslog'])

    def test_read_logs_from_multiple_dirs(self):
        self.mock_paths = [
            'ssh://node1/logs/attempt_201512232143_0008_m_000000_0/syslog',
            'ssh://node2/logs/attempt_201512232143_0008_r_000000_0/syslog',
            'ssh://node1/etc/sys-stuff',
        ]

        self.assertEqual(
            _ls_pre_yarn_task_syslogs(
                self.mock_fs,
                ['ssh://node1/logs', 'ssh://node2/logs']),
            ['ssh://node2/logs/attempt_201512232143_0008_r_000000_0/syslog',
             'ssh://node1/logs/attempt_201512232143_0008_m_000000_0/syslog',])


    # subdirs are handled by code shared with _ls_yarn_task_syslogs(), and
    # thus are tested above
