# -*- encoding: utf-8 -*-
# Copyright 2015-2016 Yelp
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
from unittest import TestCase
import unittest

from mrjob.logs.task import _interpret_task_logs
from mrjob.logs.task import _interpret_spark_task_logs
from mrjob.logs.task import _ls_spark_task_logs
from mrjob.logs.task import _ls_task_logs
from mrjob.logs.task import _match_task_log_path
from mrjob.logs.task import _parse_task_stderr
from mrjob.logs.task import _parse_task_syslog

from tests.py2 import call
from tests.py2 import Mock
from tests.py2 import patch
from tests.sandbox import PatcherTestCase


class MatchTaskLogPathTestCase(TestCase):

    PRE_YARN_STDERR_PATH = (
        '/userlogs/attempt_201512232143_0008_m_000001_3/stderr')
    PRE_YARN_SYSLOG_PATH = (
        '/userlogs/attempt_201512232143_0008_m_000001_3/syslog')

    YARN_STDERR_PATH = ('/log/dir/userlogs/application_1450486922681_0004/'
                        'container_1450486922681_0005_01_000003/stderr')
    # stdout is only used by Spark
    YARN_STDOUT_PATH = ('/log/dir/userlogs/application_1450486922681_0004/'
                        'container_1450486922681_0005_01_000003/stdout')
    YARN_SYSLOG_PATH = ('/log/dir/userlogs/application_1450486922681_0004/'
                        'container_1450486922681_0005_01_000003/syslog')

    def test_empty(self):
        self.assertEqual(_match_task_log_path(''), None)

    def test_pre_yarn_syslog(self):
        self.assertEqual(
            _match_task_log_path(self.PRE_YARN_SYSLOG_PATH),
            dict(attempt_id='attempt_201512232143_0008_m_000001_3',
                 log_type='syslog'))

    def test_pre_yarn_syslog_gz(self):
        self.assertEqual(
            _match_task_log_path(self.PRE_YARN_SYSLOG_PATH + '.gz'),
            dict(attempt_id='attempt_201512232143_0008_m_000001_3',
                 log_type='syslog'))

    def test_pre_yarn_stderr(self):
        self.assertEqual(
            _match_task_log_path(self.PRE_YARN_STDERR_PATH),
            dict(attempt_id='attempt_201512232143_0008_m_000001_3',
                 log_type='stderr'))

    def test_pre_yarn_job_id_filter(self):
        self.assertEqual(
            _match_task_log_path(
                self.PRE_YARN_SYSLOG_PATH,
                job_id='job_201512232143_0008'),
            dict(attempt_id='attempt_201512232143_0008_m_000001_3',
                 log_type='syslog'))

        self.assertEqual(
            _match_task_log_path(
                self.PRE_YARN_SYSLOG_PATH,
                job_id='job_201512232143_0009'),
            None)

    def test_yarn_syslog(self):
        self.assertEqual(
            _match_task_log_path(self.YARN_SYSLOG_PATH),
            dict(application_id='application_1450486922681_0004',
                 container_id='container_1450486922681_0005_01_000003',
                 log_type='syslog'))

    def test_yarn_syslog_gz(self):
        self.assertEqual(
            _match_task_log_path(self.YARN_SYSLOG_PATH + '.gz'),
            dict(application_id='application_1450486922681_0004',
                 container_id='container_1450486922681_0005_01_000003',
                 log_type='syslog'))

    def test_dont_match_yarn_shuffle_syslog(self):
        self.assertEqual(
            _match_task_log_path(self.YARN_SYSLOG_PATH + '.shuffle'),
            None)

    def test_match_yarn_stderr(self):
        self.assertEqual(
            _match_task_log_path(self.YARN_STDERR_PATH + '.gz'),
            dict(application_id='application_1450486922681_0004',
                 container_id='container_1450486922681_0005_01_000003',
                 log_type='stderr'))

    def test_match_yarn_stdout(self):
        self.assertEqual(
            _match_task_log_path(self.YARN_STDOUT_PATH),
            dict(application_id='application_1450486922681_0004',
                 container_id='container_1450486922681_0005_01_000003',
                 log_type='stdout'))

    def test_yarn_application_id_filter(self):
        self.assertEqual(
            _match_task_log_path(
                self.YARN_SYSLOG_PATH,
                application_id='application_1450486922681_0004'),
            dict(application_id='application_1450486922681_0004',
                 container_id='container_1450486922681_0005_01_000003',
                 log_type='syslog'))

        self.assertEqual(
            _match_task_log_path(
                self.YARN_SYSLOG_PATH,
                application_id='application_1450486922681_0005'),
            None)


class InterpretTaskLogsTestCase(PatcherTestCase):

    maxDiff = None

    def setUp(self):
        super(InterpretTaskLogsTestCase, self).setUp()

        # instead of mocking out contents of files, just mock out
        # what _parse_task_{syslog,stderr}() should return, and have
        # _cat_log() just pass through the path
        self.mock_paths = []
        self.path_to_mock_result = {}

        self.mock_log_callback = Mock()

        self.mock_paths_catted = []

        def mock_cat_log(fs, path):
            if path in self.mock_paths:
                self.mock_paths_catted.append(path)
            return path

        # (the actual log-parsing functions take lines from the log)
        def mock_parse_task_syslog(path_from_mock_cat_log):
            # default is {}
            return self.path_to_mock_result.get(path_from_mock_cat_log, {})

        def mock_parse_task_stderr(path_from_mock_cat_log):
            # default is None
            return self.path_to_mock_result.get(path_from_mock_cat_log)

        def mock_exists(path):
            return path in self.mock_paths or path == 'MOCK_LOG_DIR'

        # need to mock ls so that _ls_task_logs() can work
        def mock_ls(log_dir):
            return self.mock_paths

        self.mock_fs = Mock()
        self.mock_fs.exists = Mock(side_effect=mock_exists)
        self.mock_fs.ls = Mock(side_effect=mock_ls)

        self.mock_cat_log = self.start(
            patch('mrjob.logs.task._cat_log', side_effect=mock_cat_log))

        self.start(patch('mrjob.logs.task._parse_task_syslog',
                         side_effect=mock_parse_task_syslog))
        self.start(patch('mrjob.logs.task._parse_task_stderr',
                         side_effect=mock_parse_task_stderr))

    def mock_path_matches(self):
        mock_log_dir_stream = [['MOCK_LOG_DIR']]  # _ls_logs() needs this
        return _ls_task_logs(self.mock_fs, mock_log_dir_stream)

    def interpret_task_logs(self, **kwargs):
        return _interpret_task_logs(
            self.mock_fs, self.mock_path_matches(),
            log_callback=self.mock_log_callback,
            **kwargs)

    def test_empty(self):
        self.assertEqual(self.interpret_task_logs(), {})

    def test_syslog_with_no_error(self):
        syslog_path = '/userlogs/attempt_201512232143_0008_m_000001_3/syslog'

        self.mock_paths = [syslog_path]

        self.assertEqual(self.interpret_task_logs(), {})

    def test_syslog_with_split_only(self):
        syslog_path = '/userlogs/attempt_201512232143_0008_m_000001_3/syslog'

        self.mock_paths = [syslog_path]

        self.path_to_mock_result = {
            syslog_path: dict(split=dict(path='best_input_file_ever'))
        }

        self.assertEqual(self.interpret_task_logs(), {})

    def test_syslog_with_error(self):
        syslog_path = '/userlogs/attempt_201512232143_0008_m_000001_3/syslog'

        self.mock_paths = [syslog_path]

        self.path_to_mock_result = {
            syslog_path: dict(hadoop_error=dict(message='BOOM')),
        }

        self.assertEqual(self.interpret_task_logs(), dict(
            errors=[
                dict(
                    attempt_id='attempt_201512232143_0008_m_000001_3',
                    hadoop_error=dict(
                        message='BOOM',
                        path=syslog_path,
                    ),
                    task_id='task_201512232143_0008_m_000001',
                ),
            ],
            partial=True,
        ))

    def test_syslog_with_error_and_split(self):
        syslog_path = '/userlogs/attempt_201512232143_0008_m_000001_3/syslog'

        self.mock_paths = [syslog_path]

        self.path_to_mock_result = {
            syslog_path: dict(hadoop_error=dict(message='BOOM'),
                              split=dict(path='best_input_file_ever')),
        }

        self.assertEqual(self.interpret_task_logs(), dict(
            errors=[
                dict(
                    attempt_id='attempt_201512232143_0008_m_000001_3',
                    hadoop_error=dict(
                        message='BOOM',
                        path=syslog_path,
                    ),
                    split=dict(path='best_input_file_ever'),
                    task_id='task_201512232143_0008_m_000001',
                ),
            ],
            partial=True,
        ))

    def test_syslog_with_corresponding_stderr(self):
        syslog_path = '/userlogs/attempt_201512232143_0008_m_000001_3/syslog'
        stderr_path = '/userlogs/attempt_201512232143_0008_m_000001_3/stderr'

        self.mock_paths = [syslog_path, stderr_path]

        self.path_to_mock_result = {
            syslog_path: dict(hadoop_error=dict(message='BOOM')),
            stderr_path: dict(message='because, exploding code')
        }

        self.assertEqual(
            self.interpret_task_logs(),
            dict(
                errors=[
                    dict(
                        attempt_id='attempt_201512232143_0008_m_000001_3',
                        hadoop_error=dict(
                            message='BOOM',
                            path=syslog_path,
                        ),
                        task_error=dict(
                            message='because, exploding code',
                            path=stderr_path,
                        ),
                        task_id='task_201512232143_0008_m_000001',
                    ),
                ],
                partial=True,
            )
        )

        self.assertEqual(
            self.mock_log_callback.call_args_list,
            [call(stderr_path), call(syslog_path)])

    def test_syslog_with_empty_corresponding_stderr(self):
        syslog_path = '/userlogs/attempt_201512232143_0008_m_000001_3/syslog'
        stderr_path = '/userlogs/attempt_201512232143_0008_m_000001_3/stderr'

        self.mock_paths = [syslog_path, stderr_path]

        self.path_to_mock_result = {
            syslog_path: dict(hadoop_error=dict(message='BOOM')),
        }

        self.assertEqual(
            self.interpret_task_logs(),
            dict(
                errors=[
                    dict(
                        attempt_id='attempt_201512232143_0008_m_000001_3',
                        hadoop_error=dict(
                            message='BOOM',
                            path=syslog_path,
                        ),
                        task_id='task_201512232143_0008_m_000001',
                    ),
                ],
                partial=True,
            )
        )

        self.assertEqual(
            self.mock_log_callback.call_args_list,
            [call(stderr_path), call(syslog_path)])

    def test_yarn_syslog_with_error(self):
        # this works the same way as the other tests, except we get
        # container_id rather than attempt_id and task_id
        syslog_path = (
            '/log/dir/userlogs/application_1450486922681_0004'
            '/container_1450486922681_0005_01_000003/syslog')
        self.mock_paths = [syslog_path]

        self.path_to_mock_result = {
            syslog_path: dict(hadoop_error=dict(message='BOOM')),
        }

        self.assertEqual(self.interpret_task_logs(), dict(
            errors=[
                dict(
                    container_id='container_1450486922681_0005_01_000003',
                    hadoop_error=dict(
                        message='BOOM',
                        path=syslog_path,
                    ),
                ),
            ],
            partial=True,
        ))

    def test_error_in_stderr_only(self):
        syslog_path = '/userlogs/attempt_201512232143_0008_m_000001_3/syslog'
        stderr_path = '/userlogs/attempt_201512232143_0008_m_000001_3/stderr'

        self.mock_paths = [syslog_path, stderr_path]

        self.path_to_mock_result = {
            stderr_path: dict(message='because, exploding code')
        }

        self.assertEqual(self.interpret_task_logs(), {})
        self.assertEqual(self.mock_paths_catted, [stderr_path, syslog_path])

    def test_stderr_without_corresponding_syslog(self):
        stderr_path = '/userlogs/attempt_201512232143_0008_m_000001_3/stderr'

        self.mock_paths = [stderr_path]

        self.path_to_mock_result = {
            stderr_path: dict(message='because, exploding code')
        }

        # don't even look at stderr if it doesn't have a matching syslog
        self.assertEqual(self.interpret_task_logs(), {})
        self.assertEqual(self.mock_paths_catted, [])

    # indirectly tests _ls_task_syslogs() and its ability to sort by
    # log type and recency
    def test_multiple_logs(self):
        stderr1_path = '/userlogs/attempt_201512232143_0008_m_000001_3/stderr'
        syslog1_path = '/userlogs/attempt_201512232143_0008_m_000001_3/syslog'
        stderr2_path = '/userlogs/attempt_201512232143_0008_m_000002_3/stderr'
        syslog2_path = '/userlogs/attempt_201512232143_0008_m_000002_3/syslog'
        stderr3_path = '/userlogs/attempt_201512232143_0008_m_000003_3/stderr'
        syslog3_path = '/userlogs/attempt_201512232143_0008_m_000003_3/syslog'
        syslog4_path = '/userlogs/attempt_201512232143_0008_m_000004_3/syslog'

        self.mock_paths = [
            stderr1_path,
            syslog1_path,
            stderr2_path,
            syslog2_path,
            stderr3_path,
            syslog3_path,
            syslog4_path,
        ]

        self.path_to_mock_result = {
            syslog1_path: dict(hadoop_error=dict(message='BOOM1')),
            syslog2_path: dict(hadoop_error=dict(message='BOOM2')),
            stderr2_path: dict(message='BoomException'),
            syslog3_path: dict(hadoop_error=dict(message='BOOM3')),
            # no errors for stderr1_path, stderr3_path, or syslog4_path
        }

        # we should read from syslog2_path first (later task number)
        self.assertEqual(self.interpret_task_logs(), dict(
            errors=[
                dict(
                    attempt_id='attempt_201512232143_0008_m_000002_3',
                    hadoop_error=dict(
                        message='BOOM2',
                        path=syslog2_path,
                    ),
                    task_error=dict(
                        message='BoomException',
                        path=stderr2_path,
                    ),
                    task_id='task_201512232143_0008_m_000002',
                ),
            ],
            partial=True,
        ))

        # skip over syslog4_path (no stderr), never get to syslog1_path
        self.assertEqual(self.mock_paths_catted, [
            stderr3_path,
            stderr2_path,
            syslog2_path,
        ])

        # try again, with partial=False
        self.mock_paths_catted = []

        # paths still get sorted by _ls_logs()
        self.assertEqual(self.interpret_task_logs(partial=False), dict(
            errors=[
                dict(
                    attempt_id='attempt_201512232143_0008_m_000002_3',
                    hadoop_error=dict(
                        message='BOOM2',
                        path=syslog2_path,
                    ),
                    task_error=dict(
                        message='BoomException',
                        path=stderr2_path,
                    ),
                    task_id='task_201512232143_0008_m_000002',
                ),
                dict(
                    attempt_id='attempt_201512232143_0008_m_000003_3',
                    hadoop_error=dict(
                        message='BOOM3',
                        path=syslog3_path,
                    ),
                    task_id='task_201512232143_0008_m_000003',
                ),
                dict(
                    attempt_id='attempt_201512232143_0008_m_000001_3',
                    hadoop_error=dict(
                        message='BOOM1',
                        path=syslog1_path,
                    ),
                    task_id='task_201512232143_0008_m_000001',
                ),
            ],
        ))

        self.assertEqual(self.mock_paths_catted, [
            stderr3_path,
            stderr2_path,
            syslog2_path,
            stderr1_path,
            syslog4_path,
            syslog3_path,
            syslog1_path,
        ])

    def test_pre_yarn_sorting(self):
        # NOTE: we currently don't have to handle errors from multiple
        # jobs at once; this is a latent feature that might become
        # useful later

        self.mock_paths = [
            '/userlogs/attempt_201512232143_0008_m_000001_3/syslog',
            '/userlogs/attempt_201512232143_0008_r_000000_0/syslog',
            '/userlogs/attempt_201512232143_0008_m_000003_1/syslog',
            '/userlogs/attempt_201512232143_0006_m_000000_0/syslog',
        ]

        # just want to see order that logs are catted
        self.assertEqual(self.interpret_task_logs(), {})

        self.assertEqual(
            self.mock_paths_catted,
            [
                '/userlogs/attempt_201512232143_0008_r_000000_0/syslog',
                '/userlogs/attempt_201512232143_0008_m_000001_3/syslog',
                '/userlogs/attempt_201512232143_0008_m_000003_1/syslog',
                '/userlogs/attempt_201512232143_0006_m_000000_0/syslog',
            ]
        )

    def test_yarn_sorting(self):
        # NOTE: we currently don't have to handle errors from multiple
        # jobs/applications at once; this is a latent feature that might
        # become useful later

        self.mock_paths = [
            '/log/dir/userlogs/application_1450486922681_0004'
            '/container_1450486922681_0005_01_000003/syslog',
            '/log/dir/userlogs/application_1450486922681_0005'
            '/container_1450486922681_0005_01_000004/syslog',
            '/log/dir/userlogs/application_1450486922681_0005'
            '/container_1450486922681_0005_01_000003/syslog',
        ]

        # just want to see order that logs are catted
        self.assertEqual(self.interpret_task_logs(), {})

        self.assertEqual(
            self.mock_paths_catted,
            [
                '/log/dir/userlogs/application_1450486922681_0005'
                '/container_1450486922681_0005_01_000004/syslog',
                '/log/dir/userlogs/application_1450486922681_0005'
                '/container_1450486922681_0005_01_000003/syslog',
                '/log/dir/userlogs/application_1450486922681_0004'
                '/container_1450486922681_0005_01_000003/syslog',
            ]
        )


class InterpretSparkTaskLogsTestCase(PatcherTestCase):

    maxDiff = None

    def setUp(self):
        super(InterpretSparkTaskLogsTestCase, self).setUp()

        # instead of mocking out contents of files, just mock out
        # what _parse_task_{syslog,stderr}() should return, and have
        # _cat_log() just pass through the path
        self.mock_paths = []
        self.path_to_mock_result = {}

        self.mock_log_callback = Mock()

        def mock_cat_log(fs, path):
            if path in self.mock_paths:
                return path

        # (the actual log-parsing functions take lines from the log)
        def mock_parse_task_syslog(path_from_mock_cat_log):
            # default is {}
            return self.path_to_mock_result.get(path_from_mock_cat_log, {})

        def mock_parse_task_stderr(path_from_mock_cat_log):
            # default is None
            return self.path_to_mock_result.get(path_from_mock_cat_log)

        def mock_exists(path):
            return path in self.mock_paths or path == 'MOCK_LOG_DIR'

        # need to mock ls so that _ls_spark_task_logs() can work
        def mock_ls(log_dir):
            return self.mock_paths

        self.mock_fs = Mock()
        self.mock_fs.exists = Mock(side_effect=mock_exists)
        self.mock_fs.ls = Mock(side_effect=mock_ls)

        self.mock_cat_log = self.start(
            patch('mrjob.logs.task._cat_log', side_effect=mock_cat_log))

        self.start(patch('mrjob.logs.task._parse_task_syslog',
                         side_effect=mock_parse_task_syslog))
        self.start(patch('mrjob.logs.task._parse_task_stderr',
                         side_effect=mock_parse_task_stderr))

    def mock_path_matches(self):
        mock_log_dir_stream = [['MOCK_LOG_DIR']]  # _ls_logs() needs this
        return _ls_spark_task_logs(self.mock_fs, mock_log_dir_stream)

    def interpret_spark_task_logs(self, **kwargs):
        return _interpret_spark_task_logs(
            self.mock_fs, self.mock_path_matches(),
            log_callback=self.mock_log_callback,
            **kwargs)

    # need to port over remaining tests
    def interpret_task_logs(self, **kwargs):
        raise unittest.SkipTest()

    def test_empty(self):
        self.assertEqual(self.interpret_spark_task_logs(), {})

    def test_stderr_with_no_error(self):
        stderr_path = ('/log/dir/userlogs/application_1450486922681_0005'
                       '/container_1450486922681_0005_01_000004/stderr')

        self.mock_paths = [stderr_path]

        self.assertEqual(self.interpret_spark_task_logs(), {})

    def test_stderr_with_split_only(self):
        stderr_path = ('/log/dir/userlogs/application_1450486922681_0005'
                       '/container_1450486922681_0005_01_000004/stderr')

        self.mock_paths = [stderr_path]

        self.path_to_mock_result = {
            stderr_path: dict(split=dict(path='best_input_file_ever'))
        }

        self.assertEqual(self.interpret_spark_task_logs(), {})

    def test_stderr_with_executor_error(self):
        stderr_path = ('/log/dir/userlogs/application_1450486922681_0005'
                       '/container_1450486922681_0005_01_000004/stderr')

        self.mock_paths = [stderr_path]

        self.path_to_mock_result = {
            stderr_path: dict(hadoop_error=dict(message='BOOM')),
        }

        self.assertEqual(self.interpret_spark_task_logs(), dict(
            errors=[
                dict(
                    container_id='container_1450486922681_0005_01_000004',
                    hadoop_error=dict(
                        message='BOOM',
                        path=stderr_path,
                    ),
                ),
            ],
            partial=True,
        ))

    def test_stderr_with_executor_error_and_split(self):
        stderr_path = ('/log/dir/userlogs/application_1450486922681_0005'
                       '/container_1450486922681_0005_01_000004/stderr')

        self.mock_paths = [stderr_path]

        self.path_to_mock_result = {
            stderr_path: dict(hadoop_error=dict(message='BOOM'),
                              split=dict(path='best_input_file_ever')),
        }

        self.assertEqual(self.interpret_spark_task_logs(), dict(
            errors=[
                dict(
                    container_id='container_1450486922681_0005_01_000004',
                    hadoop_error=dict(
                        message='BOOM',
                        path=stderr_path,
                    ),
                    split=dict(path='best_input_file_ever'),
                ),
            ],
            partial=True,
        ))

    def test_stderr_with_application_exited_and_stdout(self):
        stderr_path = ('/log/dir/userlogs/application_1450486922681_0005'
                       '/container_1450486922681_0005_01_000004/stderr')
        stdout_path = ('/log/dir/userlogs/application_1450486922681_0005'
                       '/container_1450486922681_0005_01_000004/stdout')

        self.mock_paths = [stderr_path, stdout_path]

        self.path_to_mock_result = {
            stderr_path: dict(
                check_stdout=True,
                hadoop_error=dict(message='application exited')),
            stdout_path: dict(message='because, exploding code')
        }

        self.assertEqual(
            self.interpret_spark_task_logs(),
            dict(
                errors=[
                    dict(
                        container_id='container_1450486922681_0005_01_000004',
                        hadoop_error=dict(
                            message='application exited',
                            path=stderr_path,
                        ),
                        task_error=dict(
                            message='because, exploding code',
                            path=stdout_path,
                        ),
                    ),
                ],
                partial=True,
            )
        )

        self.assertEqual(
            self.mock_log_callback.call_args_list,
            [call(stderr_path), call(stdout_path)])

    def test_stderr_with_application_exited_and_empty_stdout(self):
        stderr_path = ('/log/dir/userlogs/application_1450486922681_0005'
                       '/container_1450486922681_0005_01_000004/stderr')
        stdout_path = ('/log/dir/userlogs/application_1450486922681_0005'
                       '/container_1450486922681_0005_01_000004/stdout')

        self.mock_paths = [stderr_path, stdout_path]

        self.path_to_mock_result = {
            stderr_path: dict(
                check_stdout=True,
                hadoop_error=dict(message='application exited')),
        }

        self.assertEqual(
            self.interpret_spark_task_logs(),
            dict(
                errors=[
                    dict(
                        container_id='container_1450486922681_0005_01_000004',
                        hadoop_error=dict(
                            message='application exited',
                            path=stderr_path,
                        ),
                    ),
                ],
                partial=True,
            )
        )

        self.assertEqual(
            self.mock_log_callback.call_args_list,
            [call(stderr_path), call(stdout_path)])

    def test_stderr_with_application_exited_and_no_stdout(self):
        stderr_path = ('/log/dir/userlogs/application_1450486922681_0005'
                       '/container_1450486922681_0005_01_000004/stderr')

        self.mock_paths = [stderr_path]

        self.path_to_mock_result = {
            stderr_path: dict(
                check_stdout=True,
                hadoop_error=dict(message='application exited')),
        }

        self.assertEqual(
            self.interpret_spark_task_logs(),
            dict(
                errors=[
                    dict(
                        container_id='container_1450486922681_0005_01_000004',
                        hadoop_error=dict(
                            message='application exited',
                            path=stderr_path,
                        ),
                    ),
                ],
                partial=True,
            )
        )

        self.assertEqual(
            self.mock_log_callback.call_args_list,
            [call(stderr_path)])

    def test_error_in_stdout_only(self):
        stderr_path = ('/log/dir/userlogs/application_1450486922681_0005'
                       '/container_1450486922681_0005_01_000004/stderr')
        stdout_path = ('/log/dir/userlogs/application_1450486922681_0005'
                       '/container_1450486922681_0005_01_000004/stdout')

        self.mock_paths = [stderr_path, stdout_path]

        self.path_to_mock_result = {
            stdout_path: dict(message='because, exploding code')
        }

        self.assertEqual(
            self.interpret_spark_task_logs(),
            {})

        self.assertEqual(
            self.mock_log_callback.call_args_list,
            [call(stderr_path)])

    # indirectly tests _ls_spark_task_logs() and its ability to sort by
    # log type and recency
    def test_multiple_logs(self):
        stdout1_path = ('/log/dir/userlogs/application_1450486922681_0005'
                        '/container_1450486922681_0005_01_000001/stdout')
        stderr1_path = ('/log/dir/userlogs/application_1450486922681_0005'
                        '/container_1450486922681_0005_01_000001/stderr')
        stdout2_path = ('/log/dir/userlogs/application_1450486922681_0005'
                        '/container_1450486922681_0005_01_000002/stdout')
        stderr2_path = ('/log/dir/userlogs/application_1450486922681_0005'
                        '/container_1450486922681_0005_01_000002/stderr')
        stdout3_path = ('/log/dir/userlogs/application_1450486922681_0005'
                        '/container_1450486922681_0005_01_000003/stdout')
        stderr3_path = ('/log/dir/userlogs/application_1450486922681_0005'
                        '/container_1450486922681_0005_01_000003/stderr')
        stderr4_path = ('/log/dir/userlogs/application_1450486922681_0005'
                        '/container_1450486922681_0005_01_000004/stderr')

        self.mock_paths = [
            stdout1_path,
            stderr1_path,
            stdout2_path,
            stderr2_path,
            stdout3_path,
            stderr3_path,
            stderr4_path,
        ]

        self.path_to_mock_result = {
            stderr1_path: dict(
                hadoop_error=dict(message='BOOM1')),
            stderr2_path: dict(
                check_stdout=True,
                hadoop_error=dict(message='exited with status 2')),
            stdout2_path: dict(message='BoomException'),
            stderr4_path: dict(
                check_stdout=True,
                hadoop_error=dict(message='exited with status 4')),
            # no errors for stdout1_path, stdout3_path, or stderr4_path
        }

        # we should read from stderr4_path first (later task number)
        self.assertEqual(self.interpret_spark_task_logs(), dict(
            errors=[
                dict(
                    container_id='container_1450486922681_0005_01_000004',
                    hadoop_error=dict(
                        message='exited with status 4',
                        path=stderr4_path,
                    ),
                ),
            ],
            partial=True,
        ))

        self.assertEqual(
            self.mock_log_callback.call_args_list,
            [call(stderr4_path)])

        # try again, with partial=False
        self.mock_log_callback.reset_mock()

        # paths still get sorted by _ls_logs()
        self.assertEqual(self.interpret_spark_task_logs(partial=False), dict(
            errors=[
                dict(
                    container_id='container_1450486922681_0005_01_000004',
                    hadoop_error=dict(
                        message='exited with status 4',
                        path=stderr4_path,
                    ),
                ),
                dict(
                    container_id='container_1450486922681_0005_01_000002',
                    hadoop_error=dict(
                        message='exited with status 2',
                        path=stderr2_path,
                    ),
                    task_error=dict(
                        message='BoomException',
                        path=stdout2_path,
                    ),
                ),
                dict(
                    container_id='container_1450486922681_0005_01_000001',
                    hadoop_error=dict(
                        message='BOOM1',
                        path=stderr1_path,
                    ),
                ),
            ],
        ))

        self.assertEqual(
            self.mock_log_callback.call_args_list,
            [
                call(stderr4_path),
                call(stderr3_path),
                call(stderr2_path),
                call(stdout2_path),
                call(stderr1_path),
            ]
        )


class ParseTaskSyslogTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(_parse_task_syslog([]), {})

    def test_split(self):
        lines = [
            '2015-12-21 14:06:17,707 INFO [main]'
            ' org.apache.hadoop.mapred.MapTask: Processing split:'
            ' hdfs://e4270474c8ee:9000/user/root/tmp/mrjob'
            '/mr_boom.root.20151221.190511.059097/files/bootstrap.sh:0+335\n',
        ]

        self.assertEqual(
            _parse_task_syslog(lines),
            dict(
                split=dict(
                    path=('hdfs://e4270474c8ee:9000/user/root/tmp/mrjob'
                          '/mr_boom.root.20151221.190511.059097/files'
                          '/bootstrap.sh'),
                    start_line=0,
                    num_lines=335)))

    def test_opening_file(self):
        lines = [
            '2010-07-27 17:54:54,344 INFO'
            ' org.apache.hadoop.fs.s3native.NativeS3FileSystem (main):'
            " Opening 's3://yourbucket/logs/2010/07/23/log2-00077.gz'"
            ' for reading\n'
        ]

        self.assertEqual(
            _parse_task_syslog(lines),
            dict(
                split=dict(
                    path='s3://yourbucket/logs/2010/07/23/log2-00077.gz')))

    def test_yarn_error(self):
        lines = [
            '2015-12-21 14:06:18,538 WARN [main]'
            ' org.apache.hadoop.mapred.YarnChild: Exception running child'
            ' : java.lang.RuntimeException: PipeMapRed.waitOutputThreads():'
            ' subprocess failed with code 1\n',
            '        at org.apache.hadoop.streaming.PipeMapRed'
            '.waitOutputThreads(PipeMapRed.java:322)\n',
            '        at org.apache.hadoop.streaming.PipeMapRed'
            '.mapRedFinished(PipeMapRed.java:535)\n',
        ]

        self.assertEqual(
            _parse_task_syslog(lines),
            dict(
                hadoop_error=dict(
                    message=(
                        'Exception running child : java.lang.RuntimeException:'
                        ' PipeMapRed.waitOutputThreads():'
                        ' subprocess failed with code 1\n'
                        '        at org.apache.hadoop.streaming.PipeMapRed'
                        '.waitOutputThreads(PipeMapRed.java:322)\n'
                        '        at org.apache.hadoop.streaming.PipeMapRed'
                        '.mapRedFinished(PipeMapRed.java:535)'),
                    num_lines=3,
                    start_line=0,
                )
            ))

    def test_pre_yarn_error(self):
        lines = [
            '2015-12-30 19:21:39,980 WARN'
            ' org.apache.hadoop.mapred.Child (main): Error running child\n',
            'java.lang.RuntimeException: PipeMapRed.waitOutputThreads():'
            ' subprocess failed with code 1\n',
            '        at org.apache.hadoop.streaming.PipeMapRed'
            '.waitOutputThreads(PipeMapRed.java:372)\n',
        ]

        self.assertEqual(
            _parse_task_syslog(lines),
            dict(
                hadoop_error=dict(
                    message=(
                        'Error running child\n'
                        'java.lang.RuntimeException:'
                        ' PipeMapRed.waitOutputThreads():'
                        ' subprocess failed with code 1\n'
                        '        at org.apache.hadoop.streaming.PipeMapRed'
                        '.waitOutputThreads(PipeMapRed.java:372)'),
                    num_lines=3,
                    start_line=0,
                )))

    def test_spark_executor_exception(self):
        lines = [
            '16/11/16 22:05:00 ERROR Executor: Exception in task 0.2 in stage'
            ' 0.0 (TID 4)'
            ' org.apache.spark.api.python.PythonException: Traceback (most'
            ' recent call last):\n',
            '  File "/mnt/yarn/usercache/hadoop/appcache/application'
            '_1479325434015_0003/container_1479325434015_0003_02_000002/'
            'pyspark.zip/pyspark/worker.py", line 111, in main process()\n'
            'Exception: KABOOM\n',
            '\n',
            '        at org.apache.spark.api.python.PythonRunner$$anon$1.read'
            '(PythonRDD.scala:166)\n',
        ]

        self.assertEqual(
            _parse_task_syslog(lines),
            dict(
                hadoop_error=dict(
                    message=(
                        'Exception in task 0.2 in stage'
                        ' 0.0 (TID 4)'
                        ' org.apache.spark.api.python.PythonException:'
                        ' Traceback (most recent call last):\n'
                        '  File "/mnt/yarn/usercache/hadoop/appcache/'
                        'application_1479325434015_0003/container'
                        '_1479325434015_0003_02_000002/pyspark.zip/pyspark/'
                        'worker.py", line 111, in main process()\n'
                        'Exception: KABOOM\n'
                        '\n'
                        '        at org.apache.spark.api.python.PythonRunner'
                        '$$anon$1.read(PythonRDD.scala:166)'),
                    num_lines=4,
                    start_line=0,
                )
            ))

    def test_spark_application_failed(self):
        lines = [
            '16/11/16 22:26:22 ERROR ApplicationMaster: User application'
            ' exited with status 1\n',
            '16/11/16 22:26:22 INFO ApplicationMaster: Final app status:'
            ' FAILED, exitCode: 1, (reason: User application exited with'
            ' status 1)\n',
            '16/11/16 22:26:31 ERROR ApplicationMaster: SparkContext did not'
            ' initialize after waiting for 100000 ms. Please check earlier'
            ' log output for errors. Failing the application.\n',
        ]

        self.assertEqual(
            _parse_task_syslog(lines),
            dict(
                check_stdout=True,
                hadoop_error=dict(
                    message='User application exited with status 1',
                    num_lines=1,
                    start_line=0,
                ),
            )
        )


class ParseTaskStderrTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(_parse_task_stderr([]), None)

    def test_python_exception(self):
        lines = [
            '+ python mr_boom.py --step-num=0 --mapper\n',
            'Traceback (most recent call last):\n',
            '  File "mr_boom.py", line 10, in <module>\n',
            '    MRBoom.run()\n',
            'Exception: BOOM\n',
        ]

        self.assertEqual(
            _parse_task_stderr(lines),
            dict(
                message=''.join(lines).rstrip('\n'),
                start_line=0,
                num_lines=5,
            ))

    def test_setup_command_error(self):
        lines = [
            '+ __mrjob_PWD=/mnt/var/lib/hadoop/tmp/nm-local-dir/usercache'
            '/hadoop/appcache/application_1453488173054_0002'
            '/container_1453488173054_0002_01_000005\n',
            '+ exec\n',
            "+ python3 -c 'import fcntl; fcntl.flock(9, fcntl.LOCK_EX)\n",
            '+ export PYTHONPATH=/mnt/var/lib/hadoop/tmp/nm-local-dir'
            '/usercache/hadoop/appcache/application_1453488173054_0002'
            '/container_1453488173054_0002_01_000005/mrjob.zip:\n',
            '+ PYTHONPATH=/mnt/var/lib/hadoop/tmp/nm-local-dir/usercache'
            '/hadoop/appcache/application_1453488173054_0002'
            '/container_1453488173054_0002_01_000005/mrjob.zip:\n',
            '+ rm /\n',
            'rm: cannot remove ‘/’: Is a directory\n',
        ]

        self.assertEqual(
            _parse_task_stderr(lines),
            dict(
                message='+ rm /\nrm: cannot remove ‘/’: Is a directory',
                start_line=5,
                num_lines=2,
            ))

    def test_strip_carriage_return(self):
        lines = [
            '+ rm /\r\n',
            'rm: cannot remove ‘/’: Is a directory\r\n',
        ]

        self.assertEqual(
            _parse_task_stderr(lines),
            dict(
                message='+ rm /\nrm: cannot remove ‘/’: Is a directory',
                start_line=0,
                num_lines=2,
            ))

    def test_silent_bad_actor(self):
        lines = [
            '+ false\n',
        ]

        self.assertEqual(
            _parse_task_stderr(lines),
            dict(
                message='+ false',
                start_line=0,
                num_lines=1,
            ))

    def test_error_without_leading_plus(self):
        lines = [
            'ERROR: something is terribly, terribly wrong\n',
            'OH THE HORROR\n',
        ]

        self.assertEqual(
            _parse_task_stderr(lines),
            dict(
                message=('ERROR: something is terribly, terribly wrong\n'
                         'OH THE HORROR'),
                start_line=0,
                num_lines=2,
            )
        )

    def test_log4j_init_warnings(self):
        lines = [
            'log4j:WARN No appenders could be found for logger'
            ' (amazon.emr.metrics.MetricsSaver).\n',
            'log4j:WARN Please initialize the log4j system properly.\n',
            'log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html'
            '#noconfig for more info.\n',
        ]

        self.assertEqual(_parse_task_stderr(lines), None)

    def test_error_with_log4j_init_warnings(self):
        lines = [
            'ERROR: something is terribly, terribly wrong\n',
            'OH THE HORROR\n',
            'log4j:WARN No appenders could be found for logger'
            ' (amazon.emr.metrics.MetricsSaver).\n',
            'log4j:WARN Please initialize the log4j system properly.\n',
            'log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html'
            '#noconfig for more info.\n',
        ]

        self.assertEqual(
            _parse_task_stderr(lines),
            dict(
                message=('ERROR: something is terribly, terribly wrong\n'
                         'OH THE HORROR'),
                start_line=0,
                num_lines=2,
            )
        )

    def test_subprocess_failed_stack_trace(self):
        # real example, with fanciful error code
        lines = [
            'java.lang.RuntimeException: PipeMapRed.waitOutputThreads():'
            ' subprocess failed with code ^^vv<><>BA',
            '    at org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads'
            '(PipeMapRed.java:372)',
            '    at org.apache.hadoop.streaming.PipeMapRed.mapRedFinished'
            '(PipeMapRed.java:586)',
            # ...
        ]

        self.assertEqual(_parse_task_stderr(lines), None)

    def test_error_followed_by_subprocess_failed_stack_trace(self):
        # real example, from #1430
        lines = [
            'Traceback (most recent call last):',
            '  File "mr_boom.py", line 10, in <module>',
            '    MRBoom.run()',
            # ...
            'Exception: BOOM',
            'java.lang.RuntimeException: PipeMapRed.waitOutputThreads():'
            ' subprocess failed with code 1',
            '    at org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads'
            '(PipeMapRed.java:372)',
            '    at org.apache.hadoop.streaming.PipeMapRed.mapRedFinished'
            '(PipeMapRed.java:586)',
            # ...
        ]

        self.assertEqual(
            _parse_task_stderr(lines),
            dict(
                message=(
                    'Traceback (most recent call last):\n'
                    '  File "mr_boom.py", line 10, in <module>\n'
                    '    MRBoom.run()\n'
                    'Exception: BOOM'),
                start_line=0,
                num_lines=4,
            )
        )
