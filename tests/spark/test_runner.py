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
"""Test the Spark runner."""
from io import BytesIO
from unittest import skipIf

try:
    import pyspark
except ImportError:
    pyspark = None

from mrjob.parse import is_uri
from mrjob.spark.runner import SparkMRJobRunner
from mrjob.examples.mr_spark_wordcount import MRSparkWordcount
from mrjob.examples.mr_spark_wordcount_script import MRSparkScriptWordcount
from mrjob.examples.mr_sparkaboom import MRSparKaboom
from mrjob.step import StepFailedException
from mrjob.util import safeeval
from mrjob.util import to_lines


from tests.mock_boto3 import MockBoto3TestCase
from tests.mock_google import MockGoogleTestCase
from tests.mockhadoop import MockHadoopTestCase
from tests.py2 import patch
from tests.sandbox import SandboxedTestCase



class MockFilesystemsTestCase(
        MockBoto3TestCase, MockGoogleTestCase, MockHadoopTestCase):
    pass


class SparkTmpDirTestCase(MockFilesystemsTestCase):

    def test_default(self):
        runner = SparkMRJobRunner()

        self.assertFalse(is_uri(runner._spark_tmp_dir))
        self.assertIsNone(runner._upload_mgr)

        self.assertEqual(runner._spark_tmp_dir[-6:], '-spark')

    def test_spark_master_local(self):
        runner = SparkMRJobRunner(spark_master='local[*]')

        self.assertFalse(is_uri(runner._spark_tmp_dir))
        self.assertIsNone(runner._upload_mgr)

    def test_spark_master_mesos(self):
        runner = SparkMRJobRunner(spark_master='mesos://host:12345')

        self.assertTrue(is_uri(runner._spark_tmp_dir))
        self.assertEqual(runner._spark_tmp_dir[:8], 'hdfs:///')

        self.assertIsNotNone(runner._upload_mgr)

    def test_spark_master_yarn(self):
        runner = SparkMRJobRunner(spark_master='yarn')

        self.assertTrue(is_uri(runner._spark_tmp_dir))
        self.assertEqual(runner._spark_tmp_dir[:8], 'hdfs:///')

        self.assertIsNotNone(runner._upload_mgr)

    def test_explicit_spark_tmp_dir_uri(self):
        runner = SparkMRJobRunner(spark_master='mesos://host:12345',
                                  spark_tmp_dir='s3://walrus/tmp')

        self.assertTrue(runner._spark_tmp_dir.startswith('s3://walrus/tmp/'))
        self.assertGreater(len(runner._spark_tmp_dir), len('s3://walrus/tmp/'))

        self.assertIsNotNone(runner._upload_mgr)

    def test_explicit_spark_tmp_dir_path(self):
        # posixpath.join() and os.path.join() are the same on UNIX
        self.start(patch('os.path.join', lambda *paths: '/./'.join(paths)))

        runner = SparkMRJobRunner(spark_tmp_dir='/path/to/tmp')

        self.assertTrue(runner._spark_tmp_dir.startswith('/path/to/tmp/./'))
        self.assertGreater(len(runner._spark_tmp_dir), len('/path/to/tmp/./'))

        self.assertIsNone(runner._upload_mgr)


@skipIf(pyspark is None, 'no pyspark module')
class SparkRunnerSparkTestCase(SandboxedTestCase):
    # these tests are slow (~30s) because they run on
    # actual Spark, in local-cluster mode

    def test_spark_mrjob(self):
        text = b'one fish\ntwo fish\nred fish\nblue fish\n'

        job = MRSparkWordcount(['-r', 'spark'])
        job.sandbox(stdin=BytesIO(text))

        counts = {}

        with job.make_runner() as runner:
            runner.run()

            for line in to_lines(runner.cat_output()):
                k, v = safeeval(line)
                counts[k] = v

        self.assertEqual(counts, dict(
            blue=1, fish=4, one=1, red=1, two=1))

    def test_spark_job_failure(self):
        job = MRSparKaboom(['-r', 'spark'])
        job.sandbox(stdin=BytesIO(b'line\n'))

        with job.make_runner() as runner:
            self.assertRaises(StepFailedException, runner.run)

    def test_spark_script_mrjob(self):
        text = b'one fish\ntwo fish\nred fish\nblue fish\n'

        job = MRSparkScriptWordcount(['-r', 'spark'])
        job.sandbox(stdin=BytesIO(text))

        counts = {}

        with job.make_runner() as runner:
            runner.run()

            for line in to_lines(runner.cat_output()):
                k, v = safeeval(line)
                counts[k] = v

        self.assertEqual(counts, dict(
            blue=1, fish=4, one=1, red=1, two=1))

    # TODO: add a Spark JAR to the repo, so we can test it
