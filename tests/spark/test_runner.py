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
from os.path import exists
from os.path import join
from unittest import skipIf

try:
    import pyspark
except ImportError:
    pyspark = None

from mrjob.examples.mr_word_freq_count import MRWordFreqCount
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
from tests.mr_null_spark import MRNullSpark
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


class SparkPyFilesTestCase(MockFilesystemsTestCase):

    # check that py_files don't get uploaded

    def setUp(self):
        super(SparkPyFilesTestCase, self).setUp()

        # don't bother actually running spark
        self.start(patch(
            'mrjob.spark.runner.SparkMRJobRunner._run_spark_submit',
            return_value=0))

    def test_dont_upload_mrjob_zip(self):
        job = MRNullSpark(['-r', 'spark', '--spark-master', 'yarn'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            self.assertTrue(exists(runner._mrjob_zip_path))

            self.assertNotIn(runner._mrjob_zip_path,
                             runner._upload_mgr.path_to_uri())

            self.assertIn(runner._mrjob_zip_path, runner._spark_submit_args(0))

    def test_eggs(self):
        egg1_path = self.makefile('dragon.egg')
        egg2_path = self.makefile('horton.egg')

        job = MRNullSpark([
            '-r', 'spark',
            '--py-files', '%s,%s' % (egg1_path, egg2_path)])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            py_files_arg = '%s,%s,%s' % (
                egg1_path, egg2_path, runner._mrjob_zip_path)
            self.assertIn(py_files_arg, runner._spark_submit_args(0))


@skipIf(pyspark is None, 'no pyspark module')
class SparkRunnerSparkStepsTestCase(MockFilesystemsTestCase):
    # these tests are slow (~20s) because they run on actual Spark

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


@skipIf(pyspark is None, 'no pyspark module')
class SparkRunnerStreamingStepsTestCase(MockFilesystemsTestCase):
    # test that the spark harness works as expected.
    #
    # this runs tests similar to those in SparkHarnessOutputComparisonTestCase
    # in tests/spark/test_mrjob_spark_harness.py.

    def test_basic_job(self):
        job = MRWordFreqCount(['-r', 'spark'])
        job.sandbox(stdin=BytesIO(
            b'one fish\ntwo fish\nred fish\nblue fish\n'))

        with job.make_runner() as runner:
            runner.run()

            output = dict(job.parse_output(runner.cat_output()))

            self.assertEqual(output, dict(blue=1, fish=4, one=1, red=1, two=1))

    def test_compression(self):
        # deliberately mix Hadoop 1 and 2 config properties
        jobconf_args = [
            '--jobconf',
            'mapred.map.output.compression.codec='\
            'org.apache.hadoop.io.compress.GzipCodec',
            '--jobconf',
            'mapreduce.output.fileoutputformat.compress=true',
        ]

        job = MRWordFreqCount(['-r', 'spark'] + jobconf_args)
        job.sandbox(stdin=BytesIO(b'fa la la la la\nla la la la\n'))

        with job.make_runner() as runner:
            runner.run()

            self.assertTrue(runner.fs.exists(
                join(runner.get_output_dir(), 'part*.gz')))

            self.assertEqual(dict(job.parse_output(runner.cat_output())),
                             dict(fa=1, la=8))




# going to need tests of uploading files once we fix #1922
