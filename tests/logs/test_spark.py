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
from tests.py2 import Mock
from tests.py2 import patch
from tests.sandbox import BasicTestCase

from mrjob.logs.spark import _interpret_spark_logs
from mrjob.logs.spark import _parse_spark_log
from mrjob.logs.task import _ls_spark_task_logs

# these are actual error messages from Spark, though the tracebacks
# in

_SINGLE_LINE_ERROR = """\
2019-08-29 21:12:43 ERROR SparkHadoopWriter:70 -\
 Task attempt_20190829211242_0004_m_000000_0 aborted."""

_SINGLE_LINE_WARNING = """\
 2019-08-29 21:12:38 WARN  NativeCodeLoader:62 - Unable to load native-hadoop\
  library for your platform... using builtin-java classes where applicable"""

_MULTI_LINE_ERROR = """\
2019-08-29 21:12:43 ERROR Utils:91 - Aborting task
org.apache.spark.api.python.PythonException: Traceback (most recent call last):
  File "/Users/davidmarin/git/venv-2.7-mrjob/lib/python2.7/site-packages\
/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 372, in main
    process()
  File "/Users/davidmarin/git/mrjob/mrjob/examples/mr_sparkaboom.py", line 28,\
 in kaboom
    raise Exception('KABOOM')
Exception: KABOOM

	at org.apache.spark.api.python.BasePythonRunner$ReaderIterator\
.handlePythonException(PythonRunner.scala:452)
	at java.lang.Thread.run(Thread.java:748)"""

_MULTI_LINE_WARNING = """\
2019-08-29 21:38:55 INFO  BlockManagerInfo:54 - Added broadcast_0_piece0 in memory on 10.0.1.15:55242 (size: 23.2 KB, free: 366.2 MB)
2019-08-29 21:38:58 WARN  TaskSetManager:66 - Lost task 1.0 in stage 0.0 (TID 1, 10.0.1.15, executor 0): org.apache.spark.SparkException: Task failed while writing rows
	at org.apache.spark.internal.io.SparkHadoopWriter$.org$apache$spark$internal$io$SparkHadoopWriter$$executeTask(SparkHadoopWriter.scala:155)
	at java.lang.Thread.run(Thread.java:748)
Caused by: org.apache.spark.api.python.PythonException: Traceback (most recent call last):
  File "/Users/davidmarin/git/venv-2.7-mrjob/lib/python2.7/site-packages/pyspark/python/lib/pyspark.zip/pyspark/worker.py", line 372, in main
    process()
  File "/Users/davidmarin/git/mrjob/mrjob/examples/mr_sparkaboom.py", line 28, in kaboom
    raise Exception('KABOOM')
Exception: KABOOM

	at org.apache.spark.api.python.BasePythonRunner$ReaderIterator.handlePythonException(PythonRunner.scala:452)
	at org.apache.spark.internal.io.SparkHadoopWriter$.org$apache$spark$internal$io$SparkHadoopWriter$$executeTask(SparkHadoopWriter.scala:139)
	... 10 more"""


_APPLICATION_ID_LINE = """\
19/09/13 22:51:14 INFO YarnClientImpl: Submitted application application_1568415025507_0001"""


class ParseSparkLogTestCase(BasicTestCase):

    def test_empty(self):
        self.assertEqual(_parse_spark_log([]), {})

    def test_single_line_error(self):
        self.assertEqual(
            _parse_spark_log(_SINGLE_LINE_ERROR.split('\n')),
            dict(errors=[
                dict(
                    spark_error=(
                        dict(
                            message=_SINGLE_LINE_ERROR[49:],
                            start_line=0,
                            num_lines=1,
                        )
                    )
                )
            ])
        )

    def test_ignore_single_line_warning(self):
        # single-line warnings can be all sorts of irrelevant things
        self.assertEqual(
            _parse_spark_log(_SINGLE_LINE_WARNING.split('\n')),
            {}
        )

    maxDiff = None

    def test_multi_line_error(self):
        self.assertEqual(
            _parse_spark_log(_MULTI_LINE_ERROR.split('\n')),
            dict(errors=[
                dict(
                    spark_error=dict(
                        message=_MULTI_LINE_ERROR[37:],
                        start_line=0,
                        num_lines=10,
                    )
                )
            ])
        )

    def test_multi_line_warning(self):
        # on the local-cluster master, Python Tracebacks are only available
        # from warnings, not errors
        self.assertEqual(
            _parse_spark_log(_MULTI_LINE_WARNING.split('\n')),
            dict(errors=[
                dict(
                    spark_error=dict(
                        message=_MULTI_LINE_WARNING[180:],
                        start_line=1,
                        num_lines=13,
                    )
                )
            ])
        )

    def test_application_id(self):
        self.assertEqual(
            _parse_spark_log(_APPLICATION_ID_LINE.split('\n')),
            dict(application_id='application_1568415025507_0001')
        )

    def test_multiple_errors(self):
        ERRORS = '\n'.join([
            _SINGLE_LINE_ERROR, _MULTI_LINE_ERROR, _MULTI_LINE_WARNING])

        self.assertEqual(
            _parse_spark_log(ERRORS.split('\n')),
            dict(errors=[
                dict(
                    spark_error=(
                        dict(
                            message=_SINGLE_LINE_ERROR[49:],
                            start_line=0,
                            num_lines=1,
                        )
                    )
                ),
                dict(
                    spark_error=dict(
                        message=_MULTI_LINE_ERROR[37:],
                        start_line=1,
                        num_lines=10,
                    )
                ),
                dict(
                    spark_error=dict(
                        message=_MULTI_LINE_WARNING[180:],
                        start_line=12,
                        num_lines=13,
                    )
                )
            ])
        )


class InterpretSparkLogsTestCase(BasicTestCase):
    # this mostly checks when partial log parsing stops

    def setUp(self):
        super(InterpretSparkLogsTestCase, self).setUp()

        # instead of mocking out contents of files, just mock out
        # what _parse_spark_log() should return, and have
        # _cat_log_lines() just pass through the path
        self.mock_paths = []
        self.path_to_mock_result = {}

        self.mock_log_callback = Mock()

        self.mock_paths_catted = []

        def mock_cat_log_lines(fs, path):
            if path in self.mock_paths:
                self.mock_paths_catted.append(path)
            return path

        # (the actual log-parsing functions take lines from the log)
        def mock_parse_spark_log(path_from_mock_cat_log_lines):
            # default is {}
            return self.path_to_mock_result.get(
                path_from_mock_cat_log_lines, {})

        def mock_exists(path):
            return path in self.mock_paths or path == 'MOCK_LOG_DIR'

        # need to mock ls so that _ls_task_logs() can work
        def mock_ls(log_dir):
            return self.mock_paths

        self.mock_fs = Mock()
        self.mock_fs.exists = Mock(side_effect=mock_exists)
        self.mock_fs.ls = Mock(side_effect=mock_ls)

        self.mock_cat_log_lines = self.start(
            patch('mrjob.logs.spark._cat_log_lines',
                  side_effect=mock_cat_log_lines))

        self.start(patch('mrjob.logs.spark._parse_spark_log',
                         side_effect=mock_parse_spark_log))

    def mock_path_matches(self):
        mock_log_dir_stream = [['MOCK_LOG_DIR']]  # _ls_logs() needs this
        return _ls_spark_task_logs(self.mock_fs, mock_log_dir_stream)

    def interpret_spark_logs(self, **kwargs):
        return _interpret_spark_logs(
            self.mock_fs, self.mock_path_matches(),
            log_callback=self.mock_log_callback,
            **kwargs)

    def test_empty(self):
        self.assertEqual(self.interpret_spark_logs(), {})

    def test_stderr_with_error(self):
        stderr_path = ('/log/dir/userlogs/application_1450486922681_0005'
                       '/container_1450486922681_0005_01_000004/stderr')

        self.mock_paths = [stderr_path]

        self.path_to_mock_result = {
            stderr_path: dict(errors=[dict(spark_error=dict(
                message='cat trace:\ntoo many cats',
                start_line=999,
                num_lines=2))]),
        }

        self.assertEqual(self.interpret_spark_logs(), dict(
            errors=[
                dict(
                    container_id='container_1450486922681_0005_01_000004',
                    spark_error=dict(
                        message='cat trace:\ntoo many cats',
                        num_lines=2,
                        path=stderr_path,
                        start_line=999,
                    ),
                ),
            ],
            partial=True,
        ))

    def test_stop_after_first_multiline_error(self):
        stderr_path1 = ('/log/dir/userlogs/application_1450486922681_0005'
                        '/container_1450486922681_0005_01_000001/stderr')
        stderr_path2 = ('/log/dir/userlogs/application_1450486922681_0005'
                        '/container_1450486922681_0005_01_000004/stderr')

        self.mock_paths = [stderr_path1, stderr_path2]

        self.path_to_mock_result = {
            stderr_path1: dict(errors=[dict(spark_error=dict(
                message='cat trace:\ntoo many cats',
                start_line=999,
                num_lines=2))]),
            stderr_path2: dict(errors=[dict(spark_error=dict(
                message='cat trace:\ntoo many cats',
                start_line=999,
                num_lines=2))]),
        }

        self.assertEqual(self.interpret_spark_logs(), dict(
            errors=[
                dict(
                    container_id='container_1450486922681_0005_01_000001',
                    spark_error=dict(
                        message='cat trace:\ntoo many cats',
                        num_lines=2,
                        path=stderr_path1,
                        start_line=999,
                    ),
                ),
            ],
            partial=True,
        ))

    def test_continue_after_single_line_error(self):
        stderr_path1 = ('/log/dir/userlogs/application_1450486922681_0005'
                        '/container_1450486922681_0005_01_000001/stderr')
        stderr_path2 = ('/log/dir/userlogs/application_1450486922681_0005'
                        '/container_1450486922681_0005_01_000004/stderr')

        self.mock_paths = [stderr_path1, stderr_path2]

        self.path_to_mock_result = {
            stderr_path1: dict(errors=[dict(spark_error=dict(
                message='cat problems',
                start_line=333,
                num_lines=1))]),
            stderr_path2: dict(errors=[dict(spark_error=dict(
                message='cat trace:\ntoo many cats',
                start_line=999,
                num_lines=2))]),
        }

        self.assertEqual(self.interpret_spark_logs(), dict(
            errors=[
                dict(
                    container_id='container_1450486922681_0005_01_000001',
                    spark_error=dict(
                        message='cat problems',
                        num_lines=1,
                        path=stderr_path1,
                        start_line=333,
                    ),
                ),
                dict(
                    container_id='container_1450486922681_0005_01_000004',
                    spark_error=dict(
                        message='cat trace:\ntoo many cats',
                        num_lines=2,
                        path=stderr_path2,
                        start_line=999,
                    ),
                ),
            ],
            partial=True,
        ))

    def test_parse_entire_file_before_stopping(self):
        stderr_path1 = ('/log/dir/userlogs/application_1450486922681_0005'
                        '/container_1450486922681_0005_01_000001/stderr')
        stderr_path2 = ('/log/dir/userlogs/application_1450486922681_0005'
                        '/container_1450486922681_0005_01_000004/stderr')

        self.mock_paths = [stderr_path1, stderr_path2]

        self.path_to_mock_result = {
            stderr_path1: dict(errors=[
                dict(spark_error=dict(
                    message='cat trace:\ntoo many cats',
                    start_line=222,
                    num_lines=2)),
                dict(spark_error=dict(
                    message='cat problems',
                    start_line=333,
                    num_lines=1)),
            ]),
            stderr_path2: dict(errors=[dict(spark_error=dict(
                message='cat trace:\ntoo many cats',
                start_line=999,
                num_lines=2))]),
        }

        self.assertEqual(self.interpret_spark_logs(), dict(
            errors=[
                dict(
                    container_id='container_1450486922681_0005_01_000001',
                    spark_error=dict(
                        message='cat trace:\ntoo many cats',
                        start_line=222,
                        num_lines=2,
                        path=stderr_path1,
                    ),
                ),
                dict(
                    container_id='container_1450486922681_0005_01_000001',
                    spark_error=dict(
                        message='cat problems',
                        start_line=333,
                        num_lines=1,
                        path=stderr_path1,
                    ),
                ),
            ],
            partial=True,
        ))

    def test_disable_partial_log_parsing(self):
        stderr_path1 = ('/log/dir/userlogs/application_1450486922681_0005'
                        '/container_1450486922681_0005_01_000001/stderr')
        stderr_path2 = ('/log/dir/userlogs/application_1450486922681_0005'
                        '/container_1450486922681_0005_01_000004/stderr')

        self.mock_paths = [stderr_path1, stderr_path2]

        cat_trace_error = dict(errors=[dict(spark_error=dict(
            message='cat trace:\ntoo many cats',
            start_line=999,
            num_lines=2))])

        self.path_to_mock_result = {
            stderr_path1: dict(errors=[dict(spark_error=dict(
                message='cat trace:\ntoo many cats',
                start_line=999,
                num_lines=2))]),
            stderr_path2: dict(errors=[dict(spark_error=dict(
                message='cat trace:\ntoo many cats',
                start_line=999,
                num_lines=2))]),
        }

        self.assertEqual(self.interpret_spark_logs(partial=False), dict(
            errors=[
                dict(
                    container_id='container_1450486922681_0005_01_000001',
                    spark_error=dict(
                        message='cat trace:\ntoo many cats',
                        num_lines=2,
                        path=stderr_path1,
                        start_line=999,
                    ),
                ),
                dict(
                    container_id='container_1450486922681_0005_01_000004',
                    spark_error=dict(
                        message='cat trace:\ntoo many cats',
                        num_lines=2,
                        path=stderr_path2,
                        start_line=999,
                    ),
                ),
            ],
        ))
