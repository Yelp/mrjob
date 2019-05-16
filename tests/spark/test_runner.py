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
from os import listdir
from os.path import exists
from os.path import join
from unittest import skipIf

try:
    import pyspark
except ImportError:
    pyspark = None
from mrjob.examples.mr_count_lines_by_file import MRCountLinesByFile
from mrjob.examples.mr_most_used_word import MRMostUsedWord
from mrjob.examples.mr_nick_nack import MRNickNack
from mrjob.examples.mr_spark_most_used_word import MRSparkMostUsedWord
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
from tests.mr_count_lines_by_filename import MRCountLinesByFilename
from tests.mr_doubler import MRDoubler
from tests.mr_null_spark import MRNullSpark
from tests.mr_os_walk_job import MROSWalkJob
from tests.mr_pass_thru_arg_test import MRPassThruArgTest
from tests.mr_sort_and_group import MRSortAndGroup
from tests.mr_spark_os_walk import MRSparkOSWalk
from tests.mr_streaming_and_spark import MRStreamingAndSpark
from tests.mr_test_jobconf import MRTestJobConf
from tests.mr_tower_of_powers import MRTowerOfPowers
from tests.mr_two_step_job import MRTwoStepJob
from tests.py2 import ANY
from tests.py2 import call
from tests.py2 import patch
from tests.sandbox import SandboxedTestCase
from tests.sandbox import mrjob_conf_patcher
from tests.test_bin import _LOCAL_CLUSTER_MASTER


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
class SparkWorkingDirTestCase(MockFilesystemsTestCase):
    # regression tests for #1922

    def test_copy_files_with_rename_to_local_wd_mirror(self):
        # see test_upload_files_with_rename() in test_local for comparison

        fish_path = self.makefile('fish', b'salmon')
        fowl_path = self.makefile('fowl', b'goose')

        # use _LOCAL_CLUSTER_MASTER because the default master (local[*])
        # doesn't have a working directory
        job = MRSparkOSWalk(['-r', 'spark',
                             '--spark-master', _LOCAL_CLUSTER_MASTER,
                             '--file', fish_path + '#ghoti',
                             '--file', fowl_path])
        job.sandbox()

        file_sizes = {}

        with job.make_runner() as runner:
            runner.run()

            # check working dir mirror
            wd_mirror = runner._wd_mirror()
            self.assertIsNotNone(wd_mirror)
            self.assertFalse(is_uri(wd_mirror))

            self.assertTrue(exists(wd_mirror))
            # only files which needed to be renamed should be in wd_mirror
            self.assertTrue(exists(join(wd_mirror, 'ghoti')))
            self.assertFalse(exists(join(wd_mirror, 'fish')))
            self.assertFalse(exists(join(wd_mirror, 'fowl')))

            for line in to_lines(runner.cat_output()):
                path, size = safeeval(line)
                file_sizes[path] = size

        # check that files were uploaded to working dir
        self.assertIn('fowl', file_sizes)
        self.assertEqual(file_sizes['fowl'], 5)

        self.assertIn('ghoti', file_sizes)
        self.assertEqual(file_sizes['ghoti'], 6)

        # fish was uploaded as "ghoti"
        self.assertNotIn('fish', file_sizes)

    def test_copy_files_with_rename_to_remote_wd_mirror(self):
        self.add_mock_s3_data({'walrus': {'fish': b'salmon',
                                          'fowl': b'goose'}})

        foe_path = self.makefile('foe', b'giant')

        run_spark_submit = self.start(patch(
            'mrjob.bin.MRJobBinRunner._run_spark_submit',
            return_value=0))

        job = MRSparkOSWalk(['-r', 'spark',
                             '--spark-master', 'mesos://host:9999',
                             '--spark-tmp-dir', 's3://walrus/tmp',
                             '--file', 's3://walrus/fish#ghoti',
                             '--file', 's3://walrus/fowl',
                             '--file', foe_path])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

            # check working dir mirror
            wd_mirror = runner._wd_mirror()
            fs = runner.fs

            self.assertIsNotNone(wd_mirror)
            self.assertTrue(is_uri(wd_mirror))

            self.assertTrue(fs.exists(wd_mirror))
            # uploaded for rename
            self.assertTrue(fs.exists(fs.join(wd_mirror, 'ghoti')))
            # wrong name
            self.assertFalse(fs.exists(fs.join(wd_mirror, 'fish')))
            # no need to upload, already visible
            self.assertFalse(fs.exists(fs.join(wd_mirror, 'fowl')))
            # need to upload from local to remote
            self.assertTrue(fs.exists(fs.join(wd_mirror, 'foe')))

            run_spark_submit.assert_called_once_with(
                ANY, ANY, record_callback=ANY)

            spark_submit_args = run_spark_submit.call_args[0][0]
            self.assertIn('--files', spark_submit_args)
            files_arg = spark_submit_args[
                spark_submit_args.index('--files') + 1]

            self.assertEqual(
                files_arg, ','.join([
                    fs.join(wd_mirror, 'foe'),
                    's3://walrus/fowl',
                    fs.join(wd_mirror, 'ghoti'),
                    fs.join(wd_mirror, 'mr_spark_os_walk.py'),
                ]))


@skipIf(pyspark is None, 'no pyspark module')
class SparkRunnerStreamingStepsTestCase(MockFilesystemsTestCase):
    # test that the spark harness works as expected.
    #
    # this runs tests similar to those in SparkHarnessOutputComparisonTestCase
    # in tests/spark/test_harness.py.

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
            ('mapred.output.compression.codec='
             'org.apache.hadoop.io.compress.GzipCodec'),
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

    def test_hadoop_output_format(self):
        input_bytes = b'ee eye ee eye oh'

        job = MRNickNack(['-r', 'spark'])
        job.sandbox(stdin=BytesIO(input_bytes))

        with job.make_runner() as runner:
            runner.run()

            # nicknack.MultipleValueOutputFormat should put output in subdirs
            self.assertTrue(runner.fs.exists(
                runner.fs.join(runner.get_output_dir(), 'e')))

            self.assertTrue(runner.fs.exists(
                runner.fs.join(runner.get_output_dir(), 'o')))

            # check for expected output
            self.assertEqual(
                sorted(to_lines(runner.cat_output())),
                [b'"ee"\t2\n', b'"eye"\t2\n', b'"oh"\t1\n'])

    def _num_output_files(self, runner):
        return sum(
            1 for f in listdir(runner.get_output_dir())
            if f.startswith('part-'))

    def test_num_reducers(self):
        jobconf_args = [
            '--jobconf', 'mapreduce.job.reduces=1'
        ]

        job = MRWordFreqCount(['-r', 'spark'] + jobconf_args)
        job.sandbox(stdin=BytesIO(b'one two one\n two three\n'))

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(self._num_output_files(runner), 1)

    def test_max_output_files(self):
        job = MRWordFreqCount(['-r', 'spark', '--max-output-files', '1'])
        job.sandbox(stdin=BytesIO(b'one two one\n two three\n'))

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(self._num_output_files(runner), 1)

    def test_max_output_files_is_cmd_line_only(self):
        self.start(mrjob_conf_patcher(
            dict(runners=dict(spark=dict(max_output_files=1)))))

        log = self.start(patch('mrjob.runner.log'))

        job = MRWordFreqCount(['-r', 'spark'])
        job.sandbox(stdin=BytesIO(b'one two one\n two three\n'))

        with job.make_runner() as runner:
            runner.run()

            # by default there should be at least 2 output files
            self.assertNotEqual(self._num_output_files(runner), 1)

        self.assertTrue(log.warning.called)

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

    def _test_file_upload_args(self, job_class, spark_master):
        input_bytes = (b'Market Song:\n'
                       b'To market, to market, to buy a fat pig.\n'
                       b'Home again, home again, jiggety-jig')

        # deliberately collide with FILES = ['stop_words.txt']
        #
        # Make "market" a stop word too, so that "home" is most common
        stop_words_file = self.makefile(
            'stop_words.txt',
            b'again\nmarket\nto\n')

        job = job_class(['-r', 'spark',
                         '--spark-master', spark_master,
                         '--stop-words-file', stop_words_file])
        job.sandbox(stdin=BytesIO(input_bytes))

        with job.make_runner() as runner:
            runner.run()

            output = b''.join(runner.cat_output()).strip()

            self.assertEqual(output, b'"home"')

    def test_streaming_step_file_upload_args_with_working_dir(self):
        self._test_file_upload_args(MRMostUsedWord, _LOCAL_CLUSTER_MASTER)

    def test_streaming_step_file_upload_args_without_working_dir(self):
        self._test_file_upload_args(MRMostUsedWord, 'local[*]')

    def test_spark_step_file_upload_args_with_working_dir(self):
        self._test_file_upload_args(MRSparkMostUsedWord, _LOCAL_CLUSTER_MASTER)

    def test_spark_step_file_upload_args_without_working_dir(self):
        self._test_file_upload_args(MRSparkMostUsedWord, 'local[*]')

    def _test_file_upload_args_loaded_at_init(self, spark_master):
        # can we simulate a MRJob that expects to load files in its
        # constructor?

        # sanity-check that our test; if MRTowerOfPowers can initialize
        # without its --n-file, we're not testing anything
        self.assertFalse(exists('n_file'))
        self.assertRaises(IOError, MRTowerOfPowers, ['--n-file', 'n_file'])

        n_file_path = self.makefile('n_file', b'3')
        input_bytes = b'0\n1\n2\n'

        # this should work because we can see n_file_path
        job = MRTowerOfPowers(['-r', 'spark',
                               '--spark-master', spark_master,
                               '--n-file', n_file_path])
        job.sandbox(stdin=BytesIO(input_bytes))

        with job.make_runner() as runner:
            runner.run()

            output = {n for _, n in job.parse_output(runner.cat_output())}
            self.assertEqual(output,
                             {0, 1, (((2 ** 3) ** 3) ** 3)})

    def test_file_upload_args_loaded_at_init_with_working_dir(self):
        # regression test for #2044. The Spark driver can't see n_file,
        # but it doesn't need to because it'll only ever construct
        # job instances in the executor
        self._test_file_upload_args_loaded_at_init(_LOCAL_CLUSTER_MASTER)

    def test_file_upload_args_loaded_at_init_without_working_dir(self):
        # this should work even if
        # test_file_upload_args_loaded_at_init_with_working_dir()
        # doesn't. if there's no working dir, the job just gets n_file's
        # actual path
        self._test_file_upload_args_loaded_at_init('local[*]')


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


class SparkCounterSimulationTestCase(MockFilesystemsTestCase):
    # trying to keep number of tests small, since they run actual Spark jobs

    def setUp(self):
        super(MockFilesystemsTestCase, self).setUp()

        self.log = self.start(patch('mrjob.spark.runner.log'))

    def test_two_step_job(self):
        # good all-around test. MRTwoStepJob's first step logs counters, but
        # its second step does not
        job = MRTwoStepJob(['-r', 'spark'])
        job.sandbox(stdin=BytesIO(b'foo\nbar\n'))

        with job.make_runner() as runner:
            runner.run()

        counters = runner.counters()

        # should have two steps worth of counters, even though it runs as a
        # single Spark job
        self.assertEqual(len(counters), 2)

        # first step counters should be {'count': {'combiners': <int>}}
        self.assertEqual(sorted(counters[0]), ['count'])
        self.assertEqual(sorted(counters[0]['count']), ['combiners'])
        self.assertIsInstance(counters[0]['count']['combiners'], int)

        # second step counters should be empty
        self.assertEqual(counters[1], {})

        log_output = '\n'.join(c[0][0] for c in self.log.info.call_args_list)
        log_lines = log_output.split('\n')

        # should log first step counters but not second step
        self.assertIn('Counters for step 1: 1', log_lines)
        self.assertIn('\tcount', log_output)
        self.assertNotIn('Counters for step 2', log_output)

    def test_mixed_job(self):
        job = MRStreamingAndSpark(['-r', 'spark'])
        job.sandbox(stdin=BytesIO(b'foo\nbar\n'))

        with job.make_runner() as runner:
            runner.run()

        # should have empty counters for each step
        self.assertEqual(runner.counters(), [{}, {}])

        # shouldn't log empty counters
        log_output = '\n'.join(c[0][0] for c in self.log.info.call_args_list)
        self.assertNotIn('Counters', log_output)

    def test_blank_out_counters_if_not_output(self):
        self.start(patch('mrjob.bin.MRJobBinRunner._run_spark_submit',
                         return_value=2))

        job = MRTwoStepJob(['-r', 'spark'])
        job.sandbox(stdin=BytesIO(b'foo\nbar\n'))

        with job.make_runner() as runner:
            self.assertRaises(StepFailedException, runner.run)

        # should blank out counters from failed step
        self.assertEqual(runner.counters(), [{}, {}])


@skipIf(pyspark is None, 'no pyspark module')
class EmulateMapInputFileTestCase(SandboxedTestCase):

    def test_one_file(self):
        two_lines_path = self.makefile('two_lines', b'line\nother line\n')

        job = MRCountLinesByFile(['-r', 'spark',
                                  '--emulate-map-input-file', two_lines_path])

        with job.make_runner() as runner:
            runner.run()

            output = dict(job.parse_output(runner.cat_output()))

            self.assertEqual(
                output,
                {'file://' + two_lines_path: 2})

    def test_two_files(self):
        two_lines_path = self.makefile('two_lines', b'line\nother line\n')
        three_lines_path = self.makefile('three_lines', b'A\nBB\nCCC\n')

        job = MRCountLinesByFile(['-r', 'spark',
                                  '--emulate-map-input-file',
                                  two_lines_path, three_lines_path])

        with job.make_runner() as runner:
            runner.run()

            output = dict(job.parse_output(runner.cat_output()))

            self.assertEqual(
                output,
                {'file://' + two_lines_path: 2,
                 'file://' + three_lines_path: 3})

    def test_input_dir(self):
        input_dir = self.makedirs('input')

        two_lines_path = self.makefile('input/two_lines', b'line 1\nline 2\n')
        three_lines_path = self.makefile('input/three_lines', b'A\nBB\nCCC\n')

        job = MRCountLinesByFile(['-r', 'spark',
                                  '--emulate-map-input-file',
                                  two_lines_path, three_lines_path])

        with job.make_runner() as runner:
            runner.run()

            output = dict(job.parse_output(runner.cat_output()))

            self.assertEqual(
                output,
                {'file://' + two_lines_path: 2,
                 'file://' + three_lines_path: 3})

    def test_mapper_init(self):
        two_lines_path = self.makefile('two_lines', b'line\nother line\n')

        job = MRTestJobConf(['-r', 'spark',
                             '--emulate-map-input-file',
                             two_lines_path])

        with job.make_runner() as runner:
            runner.run()

            output = dict(job.parse_output(runner.cat_output()))

            self.assertEqual(
                output['mapreduce.map.input.file'],
                'file://' + two_lines_path)

    def test_empty_file(self):
        two_lines_path = self.makefile('two_lines', b'line\nother line\n')
        no_lines_path = self.makefile('no_lines', b'')

        job = MRTestJobConf(['-r', 'spark',
                             '--emulate-map-input-file',
                             two_lines_path])

        with job.make_runner() as runner:
            runner.run()

            paths = [path for jobconf, path in
                     job.parse_output(runner.cat_output())
                     if jobconf == 'mapreduce.map.input.file']

            # ideally, no_lines_path would appear too, but what we care
            # about is that we don't get a crash from trying to read
            # the "first" line of the file
            self.assertEqual(paths, ['file://' + two_lines_path])

    def test_second_mapper(self):
        # we have to run the initial mapper somewhat differently in order
        # to deal with extra info from --emulate-map-input-file (the
        # input filename). ensure that we don't accidentally do this with
        # the second mapper too

        two_lines_path = self.makefile('two_lines', b'line\nother line\n')
        three_lines_path = self.makefile('three_lines', b'A\nBB\nCCC\n')

        job = MRCountLinesByFilename(['-r', 'spark',
                                      '--emulate-map-input-file',
                                      two_lines_path, three_lines_path])

        with job.make_runner() as runner:
            runner.run()

            output = dict(job.parse_output(runner.cat_output()))

            self.assertEqual(
                output,
                {'two_lines': 2, 'three_lines': 3})


@skipIf(pyspark is None, 'no pyspark module')
class SparkSetupScriptTestCase(MockFilesystemsTestCase):

    def test_setup_command(self):
        job = MROSWalkJob(
            ['-r', 'spark',
             '--spark-master', _LOCAL_CLUSTER_MASTER,
             '--setup', 'touch bar'])
        job.sandbox()

        with job.make_runner() as r:
            r.run()

            path_to_size = dict(job.parse_output(r.cat_output()))

        self.assertIn('./bar', path_to_size)
