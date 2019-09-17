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
from tests.sandbox import BasicTestCase

from mrjob.logs.spark import _parse_spark_log

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
