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

from mrjob.examples.mr_spark_wordcount import MRSparkWordcount
from mrjob.examples.mr_spark_wordcount_script import MRSparkScriptWordcount
from mrjob.examples.mr_sparkaboom import MRSparKaboom
from mrjob.examples.mr_word_freq_count import MRWordFreqCount
from mrjob.job import MRJob
from mrjob.parse import is_uri
from mrjob.spark.runner import SparkMRJobRunner
from mrjob.step import MRStep
from mrjob.step import StepFailedException
from mrjob.util import safeeval
from mrjob.util import to_lines

from tests.mock_boto3 import MockBoto3TestCase
from tests.mock_google import MockGoogleTestCase
from tests.mockhadoop import MockHadoopTestCase
from tests.mr_doubler import MRDoubler
from tests.mr_null_spark import MRNullSpark
from tests.mr_pass_thru_arg_test import MRPassThruArgTest
from tests.mr_sort_and_group import MRSortAndGroup
from tests.mr_streaming_and_spark import MRStreamingAndSpark
from tests.py2 import ANY
from tests.py2 import call
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
            'mapred.output.compression.codec='\
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

    def test_sort_values(self):
        job = MRSortAndGroup(['-r', 'spark'])
        job.sandbox(stdin=BytesIO(
            b'alligator\nactuary\nbowling\nartichoke\nballoon\nbaby\n'))

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(
                dict(job.parse_output(runner.cat_output())),
                dict(a=['actuary', 'alligator', 'artichoke'],
                     b=['baby', 'balloon', 'bowling']))

    def test_passthru_args(self):
        job = MRPassThruArgTest(['-r', 'spark', '--chars', '--ignore', 'to'])
        job.sandbox(stdin=BytesIO(
            b'to be or\nnot to be\nthat is the question'))

        with job.make_runner() as runner:
            runner.run()

            # should delete 'to' and count remaining chars
            self.assertEqual(
                dict(job.parse_output(runner.cat_output())),
                {' ': 7, 'a': 1, 'b': 2, 'e': 4, 'h': 2,
                 'i': 2, 'n': 2, 'o': 3, 'q': 1, 'r': 1,
                 's': 2, 't': 5, 'u': 1}
            )

    def test_mixed_job(self):
        # test a combination of streaming and spark steps
        job = MRStreamingAndSpark(['-r', 'spark'])
        job.sandbox(stdin=BytesIO(
            b'foo\nbar\n'))

        with job.make_runner() as runner:
            runner.run()

            # converts to 'null\t"foo"', 'null\t"bar"' and then counts chars
            self.assertEqual(
                sorted(to_lines(runner.cat_output())),
                [
                    b'\t 2\n',
                    b'" 4\n',
                    b'a 1\n',
                    b'b 1\n',
                    b'f 1\n',
                    b'l 4\n',
                    b'n 2\n',
                    b'o 2\n',
                    b'r 1\n',
                    b'u 2\n',
                ]
            )

    # TODO: add test of file upload args once we fix #1922


class RunnerIgnoresJobKwargsTestCase(MockFilesystemsTestCase):

    def test_ignore_format_and_sort_kwargs(self):
        # hadoop formats and SORT_VALUES are read directly from the job,
        # so the runner's constructor ignores the corresponding kwargs
        #
        # see #2022

        # same set up as test_sort_values(), above
        runner = SparkMRJobRunner(
            mr_job_script=MRSortAndGroup.mr_job_script(),
            mrjob_cls=MRSortAndGroup,
            stdin=BytesIO(
                b'alligator\nactuary\nbowling\nartichoke\nballoon\nbaby\n'),
            hadoop_input_format='TerribleInputFormat',
            hadoop_output_format='AwfulOutputFormat',
            sort_values=False)

        runner.run()

        self.assertEqual(
            dict(MRSortAndGroup().parse_output(runner.cat_output())),
            dict(a=['actuary', 'alligator', 'artichoke'],
                 b=['baby', 'balloon', 'bowling']))



class GroupStepsTestCase(MockFilesystemsTestCase):
    # test the way the runner groups multiple streaming steps together

    def setUp(self):
        super(GroupStepsTestCase, self).setUp()

        self.run_step_on_spark = self.start(patch(
            'mrjob.spark.runner.SparkMRJobRunner._run_step_on_spark'))

    def _run_job(self, job_class, *extra_args):
        job = job_class(['-r', 'spark'] + list(extra_args))
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

    def test_single_spark_step(self):
        self._run_job(MRNullSpark)

        # the first argument to _run_step_on_spark() is a "step group"
        # dict whose format we don't care about. we just want to make
        # sure the first and last step numbers are correct
        self.run_step_on_spark.assert_called_once_with(
            ANY, 0, 0)

    def test_mixed_job(self):
        self._run_job(MRStreamingAndSpark)

        self.run_step_on_spark.assert_has_calls([
            call(ANY, 0, 0),
            call(ANY, 1, 1),
        ])

    def test_five_streaming_steps(self):
        self._run_job(MRDoubler, '-n', '5')

        self.run_step_on_spark.assert_called_once_with(
            ANY, 0, 4)

    def tests_streaming_steps_with_different_jobconf(self):
        class MRDifferentJobconfJob(MRJob):

            def mapper(self, key, value):
                yield key, value

            def steps(self):
                return [
                    MRStep(mapper=self.mapper),
                    MRStep(mapper=self.mapper, jobconf=dict(foo='bar')),
                    MRStep(mapper=self.mapper, jobconf=dict(foo='bar')),
                    MRStep(mapper=self.mapper, jobconf=dict(foo='baz')),
                ]

        self._run_job(MRDifferentJobconfJob)

        # steps 1 and 2 should be grouped together
        self.run_step_on_spark.assert_has_calls([
            call(ANY, 0, 0),
            call(ANY, 1, 2),
            call(ANY, 3, 3),
        ])


# TODO: test uploading files and setting up working dir once we fix #1922
