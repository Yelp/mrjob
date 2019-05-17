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
"""Test the Spark Harness."""
import inspect
import random
import string
import json
from io import BytesIO
from contextlib import contextmanager
from os import listdir
from os.path import abspath
from os.path import dirname
from os.path import join
from tempfile import gettempdir
from shutil import rmtree

import mrjob.spark.harness
from mrjob.examples.mr_count_lines_by_file import MRCountLinesByFile
from mrjob.examples.mr_nick_nack import MRNickNack
from mrjob.examples.mr_nick_nack_input_format import \
    MRNickNackWithHadoopInputFormat
from mrjob.examples.mr_word_freq_count import MRWordFreqCount
from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
from mrjob.spark.harness import _run_combiner
from mrjob.spark.harness import _run_reducer
from mrjob.spark.harness import _shuffle_and_sort
from mrjob.util import cmd_line
from mrjob.util import to_lines

from tests.mr_counting_job import MRCountingJob
from tests.mr_doubler import MRDoubler
from tests.mr_no_mapper import MRNoMapper
from tests.mr_pass_thru_arg_test import MRPassThruArgTest
from tests.mr_streaming_and_spark import MRStreamingAndSpark
from tests.mr_sort_and_group import MRSortAndGroup
from tests.mr_spark_harness import MRSparkHarness
from tests.mr_two_step_job import MRTwoStepJob
from tests.mr_word_freq_count_with_combiner_cmd import \
    MRWordFreqCountWithCombinerCmd
from tests.py2 import Mock
from tests.sandbox import SandboxedTestCase
from tests.sandbox import SingleSparkContextTestCase


def _rev(s):
    return ''.join(reversed(s))


class ReversedTextProtocol(TextProtocol):
    """Like TextProtocol, but stores text backwards."""

    def read(self, line):
        key, value = super(ReversedTextProtocol, self).read(line)

        return _rev(key), _rev(value)

    def write(self, key, value):
        return super(ReversedTextProtocol, self).write(
            _rev(key), _rev(value))


class MRSortAndGroupReversedText(MRSortAndGroup):

    INTERNAL_PROTOCOL = ReversedTextProtocol


class MRWordFreqCountCombinerYieldsTwo(MRWordFreqCount):

    def combiner(self, word, counts):
        yield (word, sum(counts))
        yield (word, 1000)


class MRWordFreqCountCombinerYieldsZero(MRWordFreqCount):

    def combiner(self, word, counts):
        return


class MRWordFreqCountFailingCombiner(MRWordFreqCount):

    unique_exception_str = '619dc867a1a34793b2157c258c859e78'

    def combiner(self, word, counts):
        raise Exception(self.unique_exception_str)


class MRSumValuesByWord(MRJob):
    # if combiner is run, keys with values that sum to 0
    # will be eliminated

    def mapper(self, _, line):
        word, value_str = line.split('\t', 1)
        yield word, int(value_str)

    def combiner(self, word, values):
        values_sum = sum(values)
        if values_sum != 0:
            yield word, values_sum

    def reducer(self, word, values):
        yield word, sum(values)


class MRSumValuesByWordWithCombinerPreFilter(MRSumValuesByWord):

    def combiner_pre_filter(self):
        return 'cat'


class MRSumValuesByWordWithNoCombinerJobs(MRSumValuesByWord):

    def __init__(self, *args, **kwargs):
        super(MRSumValuesByWordWithNoCombinerJobs, self).__init__(
            *args, **kwargs)

        if self.options.run_combiner:
            raise NotImplementedError("Can't init combiner jobs")


class SparkHarnessOutputComparisonBaseTestCase(
        SandboxedTestCase, SingleSparkContextTestCase):

    def _spark_harness_path(self):
        path = mrjob.spark.harness.__file__
        if path.endswith('.pyc'):
            path = path[:-1]
        return path

    def _reference_job(self, job_class, input_bytes=b'', input_paths=(),
                       runner_alias='inline', job_args=[]):

        args = ['-r', runner_alias] + list(job_args) + list(input_paths)

        reference_job = job_class(args)
        reference_job.sandbox(stdin=BytesIO(input_bytes))

        return reference_job

    def _harness_job(self, job_class, input_bytes=b'', input_paths=(),
                     runner_alias='inline', compression_codec=None,
                     job_args=None, spark_conf=None, first_step_num=None,
                     last_step_num=None, counter_output_dir=None,
                     num_reducers=None, max_output_files=None,
                     emulate_map_input_file=False):
        job_class_path = '%s.%s' % (job_class.__module__, job_class.__name__)

        harness_job_args = ['-r', runner_alias, '--job-class', job_class_path]
        if spark_conf:
            for key, value in spark_conf.items():
                harness_job_args.append('--jobconf')
                harness_job_args.append('%s=%s' % (key, value))
        if compression_codec:
            harness_job_args.append('--compression-codec')
            harness_job_args.append(compression_codec)
        if job_args:
            harness_job_args.extend(['--job-args', cmd_line(job_args)])
        if first_step_num is not None:
            harness_job_args.extend(['--first-step-num', str(first_step_num)])
        if last_step_num is not None:
            harness_job_args.extend(['--last-step-num', str(last_step_num)])
        if counter_output_dir is not None:
            harness_job_args.extend(
                ['--counter-output-dir', counter_output_dir])
        if num_reducers is not None:
            harness_job_args.extend(
                ['--num-reducers', str(num_reducers)])

        if max_output_files is not None:
            harness_job_args.extend(
                ['--max-output-files', str(max_output_files)])

        if emulate_map_input_file:
            harness_job_args.append('--emulate-map-input-file')

        harness_job_args.extend(input_paths)

        harness_job = MRSparkHarness(harness_job_args)
        harness_job.sandbox(stdin=BytesIO(input_bytes))

        return harness_job

    def _count_output_files(self, runner):
        return sum(
            1
            for f in listdir(runner.get_output_dir())
            if f.startswith('part-')
        )

    def _assert_output_matches(
            self, job_class, input_bytes=b'', input_paths=(), job_args=[],
            num_reducers=None, max_output_files=None,
            emulate_map_input_file=False):

        # run classes defined in this module in inline mode, classes
        # with their own script files in local mode. used by
        # test_skip_combiner_that_runs_cmd()
        if job_class.__module__ == __name__:
            ref_job_runner_alias = 'inline'
        else:
            ref_job_runner_alias = 'local'

        reference_job = self._reference_job(
            job_class, input_bytes=input_bytes,
            input_paths=input_paths,
            job_args=job_args,
            runner_alias=ref_job_runner_alias)

        with reference_job.make_runner() as runner:
            runner.run()

            reference_output = sorted(to_lines(runner.cat_output()))

        if emulate_map_input_file:
            # uses dataframes, which don't seem to work in inline mode:
            # java.util.ArrayList cannot be cast to org.apache.spark.sql.Column
            harness_job_runner_alias = 'local'
        else:
            harness_job_runner_alias = 'inline'

        harness_job = self._harness_job(
            job_class, input_bytes=input_bytes,
            input_paths=input_paths,
            job_args=job_args,
            max_output_files=max_output_files,
            num_reducers=num_reducers,
            emulate_map_input_file=emulate_map_input_file,
            runner_alias=harness_job_runner_alias)

        with harness_job.make_runner() as runner:
            runner.run()

            harness_output = sorted(to_lines(runner.cat_output()))

        self.assertEqual(harness_output, reference_output)


class SparkHarnessOutputComparisonTestCase(
        SparkHarnessOutputComparisonBaseTestCase):

    def test_basic_job(self):
        input_bytes = b'one fish\ntwo fish\nred fish\nblue fish\n'

        self._assert_output_matches(MRWordFreqCount, input_bytes=input_bytes)

    def test_two_step_job(self):
        input_bytes = b'foo\nbar\n'

        self._assert_output_matches(MRTwoStepJob, input_bytes=input_bytes)

    def test_mixed_job(self):
        # can we run just the streaming part of a job?
        input_bytes = b'foo\nbar\n'

        job = self._harness_job(
            MRStreamingAndSpark, input_bytes=input_bytes,
            first_step_num=0, last_step_num=0)

        with job.make_runner() as runner:
            runner.run()

            # the streaming part is just an identity mapper, but it converts
            # lines to pairs of JSON
            self.assertEqual(set(to_lines(runner.cat_output())),
                             {b'null\t"foo"\n', b'null\t"bar"\n'})

    def test_range_of_steps(self):
        # check for off-by-one errors, etc.
        input_bytes = b'"three"\t3\n"five"\t5'

        # sanity-check
        self._assert_output_matches(MRDoubler, input_bytes=input_bytes,
                                    job_args=['-n', '5'])

        # just run two of the five steps
        steps_2_and_3_job = self._harness_job(
            MRDoubler, input_bytes=input_bytes, job_args=['-n', '5'],
            first_step_num=2, last_step_num=3)

        with steps_2_and_3_job.make_runner() as runner:
            runner.run()

            # parse_output() works because internal and output protocols match
            self.assertEqual(
                dict(steps_2_and_3_job.parse_output(runner.cat_output())),
                dict(three=12, five=20),
            )

    def test_compression(self):
        compression_codec = 'org.apache.hadoop.io.compress.GzipCodec'

        input_bytes = b'fa la la la la\nla la la la\n'

        job = self._harness_job(
            MRWordFreqCount, input_bytes=input_bytes,
            compression_codec=compression_codec)

        with job.make_runner() as runner:
            runner.run()

            self.assertTrue(runner.fs.exists(
                join(runner.get_output_dir(), 'part*.gz')))

            self.assertEqual(dict(job.parse_output(runner.cat_output())),
                             dict(fa=1, la=8))

    def test_sort_values(self):
        input_bytes = (
            b'alligator\nactuary\nbowling\nartichoke\nballoon\nbaby\n')

        self._assert_output_matches(MRSortAndGroup, input_bytes=input_bytes)

    def test_sort_values_sorts_encoded_values(self):
        input_bytes = (
            b'alligator\nactuary\nbowling\nartichoke\nballoon\nbaby\n')

        job = self._harness_job(MRSortAndGroupReversedText,
                                input_bytes=input_bytes)

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(
                dict(job.parse_output(runner.cat_output())),
                dict(a=['artichoke', 'alligator', 'actuary'],
                     b=['bowling', 'balloon', 'baby']))

    def test_passthru_args(self):
        input_bytes = b'\n'.join([
            b'to be or',
            b'not to be',
            b'that is the question'])

        self._assert_output_matches(
            MRPassThruArgTest,
            input_bytes=input_bytes,
            job_args=['--chars', '--ignore', 'to'])

    def test_combiner_called(self):
        input_bytes = b'one two three one two three one two three'
        job = self._harness_job(
            MRWordFreqCountFailingCombiner, input_bytes=input_bytes)

        with job.make_runner() as runner, \
                self.assertRaises(Exception) as assert_raises_context:
            runner.run()

        exception_text = assert_raises_context.exception.__str__()
        expected_str = MRWordFreqCountFailingCombiner.unique_exception_str
        assert expected_str in exception_text

    def test_combiner_that_yields_two_values(self):
        input_bytes = b'one two three one two three one two three'

        job = self._harness_job(MRWordFreqCountCombinerYieldsTwo,
                                input_bytes=input_bytes)

        # Given that the combiner for this job yields the count and 1000 for
        # each word and the Spark harness' combiner helper stops running
        # jobs' combiners if they do not reduce the mapper's output to a single
        # value per key, we expect the combiner to run once per word, resulting
        # in an extra 1000 being added to each word's count.
        #
        # Note that if combiner_helper did not stop running the combiner in
        # this case, we would expect 2000 to be added to each word's count
        # since each word appears 3 times, resulting in 2 calls to
        # combine_pairs.
        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(
                dict(job.parse_output(runner.cat_output())),
                dict(one=1003, two=1003, three=1003))

    def test_combiner_that_yields_zero_values(self):
        input_bytes = b'a b c\na b c\na b c\na b c'

        job = self._harness_job(MRWordFreqCountCombinerYieldsZero,
                                input_bytes=input_bytes)

        with job.make_runner() as runner:
            runner.run()
            self.assertEqual(
                dict(job.parse_output(runner.cat_output())),
                dict(),
            )

    def test_combiner_that_sometimes_yields_zero_values(self):
        # another test that the combiner actually runs
        #
        # would like to test this against a reference job, but whether
        # the combiner can see and eliminate the "sad" values that sum
        # to zero depends on the reference runner's partitioning

        # note that "sad" values sum to zero
        input_bytes = b'happy\t5\nsad\t3\nhappy\t2\nsad\t-3\n'

        job = self._harness_job(MRSumValuesByWord, input_bytes=input_bytes)
        with job.make_runner() as runner:
            runner.run()
            self.assertEqual(
                dict(job.parse_output(runner.cat_output())),
                dict(happy=7),  # combiner should eliminate sad=0
            )

    def test_skip_combiner_if_runs_subprocesses(self):
        # same as above, but we have to skip combiner because of its pre-filter
        input_bytes = b'happy\t5\nsad\t3\nhappy\t2\nsad\t-3\n'

        job = self._harness_job(MRSumValuesByWordWithCombinerPreFilter,
                                input_bytes=input_bytes)
        with job.make_runner() as runner:
            runner.run()
            self.assertEqual(
                dict(job.parse_output(runner.cat_output())),
                dict(happy=7, sad=0),
            )

    def test_skip_combiner_if_cant_init_job(self):
        input_bytes = b'happy\t5\nsad\t3\nhappy\t2\nsad\t-3\n'

        job = self._harness_job(MRSumValuesByWordWithNoCombinerJobs,
                                input_bytes=input_bytes)
        with job.make_runner() as runner:
            runner.run()
            self.assertEqual(
                dict(job.parse_output(runner.cat_output())),
                dict(happy=7, sad=0),
            )

    def test_skip_combiner_that_runs_cmd(self):
        input_bytes = b'one fish\ntwo fish\nred fish\nblue fish\n'

        self._assert_output_matches(
            MRWordFreqCountWithCombinerCmd, input_bytes=input_bytes)

    @contextmanager
    def create_temp_counter_dir(self):
        random_folder_name = 'test_' + ''.join(random.choice(
            string.ascii_uppercase + string.digits) for _ in range(10))
        output_counter_dir = join(gettempdir(), random_folder_name)
        yield output_counter_dir
        rmtree(output_counter_dir)

    def test_increment_counter(self):
        input_bytes = b'one fish\ntwo fish\nred fish\nblue fish\n'
        reference_job = self._reference_job(
            MRCountingJob, input_bytes=input_bytes)

        with reference_job.make_runner() as runner:
            runner.run()
            reference_counters = runner.counters()

        with self.create_temp_counter_dir() as output_counter_dir:
            harness_job = self._harness_job(
                MRCountingJob, input_bytes=input_bytes,
                counter_output_dir='file://{}'.format(output_counter_dir)
            )
            with harness_job.make_runner() as runner:
                runner.run()

                harness_counters = json.loads(
                    self.spark_context.textFile(
                        'file://' + output_counter_dir
                    ).collect()[0])

        self.assertEqual(harness_counters, reference_counters)

    def test_job_with_no_mapper_in_second_step(self):
        # mini-regression test for not passing step_num to step.description()
        input_bytes = b'one fish\ntwo fish\nred fish\nblue fish\n'

        self._assert_output_matches(MRNoMapper, input_bytes=input_bytes)


class SparkConfigureReducerTestCase(SparkHarnessOutputComparisonBaseTestCase):

    def _assert_partition_count_different(self, cls, num_reducers):
        input_bytes = b'one fish\ntwo fish\nred fish\nblue fish\n'
        unlimited_job = self._harness_job(cls, input_bytes=input_bytes)
        with unlimited_job.make_runner() as runner:
            runner.run()
            default_partitions = self._count_output_files(runner)

        job = self._harness_job(
            cls, input_bytes=input_bytes, num_reducers=num_reducers)
        with job.make_runner() as runner:
            runner.run()
            part_files = self._count_output_files(runner)

        assert part_files != default_partitions
        assert part_files == num_reducers

    def test_reducer_cannot_set_to_zero(self):
        with self.assertRaises(ValueError):
            job = self._harness_job(MRWordFreqCount, num_reducers=0)
            with job.make_runner() as runner:
                runner.run()

    def test_reducer_less_reducer(self):
        self._assert_partition_count_different(MRWordFreqCount, num_reducers=1)

    def test_reducer_more_reducer(self):
        self._assert_partition_count_different(
            MRWordFreqCount, num_reducers=10)

    def test_combiner_less_reducer(self):
        self._assert_partition_count_different(
            MRWordFreqCountWithCombinerCmd, num_reducers=1)

    def test_combiner_more_reducer(self):
        self._assert_partition_count_different(
            MRWordFreqCountWithCombinerCmd, num_reducers=10)


class MaxOutputFilesTestCase(SparkHarnessOutputComparisonBaseTestCase):

    def test_single_output_file(self):
        input_bytes = b'one fish\ntwo fish\nred fish\nblue fish\n'

        job = self._harness_job(
            MRWordFreqCount,
            input_bytes=input_bytes,
            max_output_files=1)

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(self._count_output_files(runner), 1)

    def test_can_reduce_number_of_output_files(self):
        # use num_reducers to ratchet up number of output files

        input_bytes = b'one fish\ntwo fish\nred fish\nblue fish\n'

        job = self._harness_job(
            MRWordFreqCount,
            input_bytes=input_bytes,
            num_reducers=5,
            max_output_files=3)

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(self._count_output_files(runner), 3)

    def test_cant_increase_number_of_output_files(self):
        # use num_reducers to ratchet up number of output files

        input_bytes = b'one fish\ntwo fish\nred fish\nblue fish\n'

        job = self._harness_job(
            MRWordFreqCount,
            input_bytes=input_bytes,
            num_reducers=5,
            max_output_files=10)

        with job.make_runner() as runner:
            runner.run()

            self.assertEqual(self._count_output_files(runner), 5)

    def test_does_not_perturb_job_output(self):
        input_bytes = b'one fish\ntwo fish\nred fish\nblue fish\n'

        self._assert_output_matches(
            MRWordFreqCount, input_bytes=input_bytes, max_output_files=1)

        self._assert_output_matches(
            MRWordFreqCount, input_bytes=input_bytes,
            num_reducers=5, max_output_files=3)


class HadoopFormatsTestCase(SparkHarnessOutputComparisonBaseTestCase):

    SPARK_CONF = {
        'spark.jars': abspath(join(
            dirname(inspect.getsourcefile(MRNickNack)),
            'nicknack-1.0.1.jar')),

        # limit tests to a single executor
        'spark.executor.cores': 1,
        'spark.cores.max': 1,
    }

    def test_hadoop_output_format(self):
        input_bytes = b'ee eye ee eye oh'

        job = self._harness_job(
            MRNickNack, input_bytes=input_bytes, runner_alias='local',
            spark_conf=self.SPARK_CONF)

        with job.make_runner() as runner:
            runner.run()

            self.assertTrue(runner.fs.exists(
                join(runner.get_output_dir(), 'e')))

            self.assertTrue(runner.fs.exists(
                join(runner.get_output_dir(), 'o')))

            output_counts = dict(
                (line.strip().split(b'\t')
                 for line in to_lines(runner.cat_output())))

        expected_output_counts = {
            b'"ee"': b'2', b'"eye"': b'2', b'"oh"': b'1'}

        self.assertEqual(expected_output_counts, output_counts)

    def test_hadoop_output_format_with_compression(self):
        input_bytes = b"one two one two"
        compression_codec = 'org.apache.hadoop.io.compress.GzipCodec'

        job = self._harness_job(
            MRNickNack, input_bytes=input_bytes, runner_alias='local',
            spark_conf=self.SPARK_CONF, compression_codec=compression_codec)

        with job.make_runner() as runner:
            runner.run()

            self.assertTrue(runner.fs.exists(
                join(runner.get_output_dir(), 'o', 'part*.gz')))

            self.assertTrue(runner.fs.exists(
                join(runner.get_output_dir(), 't', 'part*.gz')))

            output_counts = dict(
                line.strip().split(b'\t')
                for line in to_lines(runner.cat_output()))

        expected_output_counts = {b'"one"': b'2', b'"two"': b'2'}

        self.assertEqual(expected_output_counts, output_counts)

    def test_hadoop_input_format(self):
        input_file_bytes = b'potato tomato'

        manifest_filename = join(self.tmp_dir, 'manifest')
        input1_filename = join(self.tmp_dir, 'input1')
        input2_filename = join(self.tmp_dir, 'input2')

        with open(input1_filename, 'wb') as input1_file:
            input1_file.write(input_file_bytes)
        with open(input2_filename, 'wb') as input2_file:
            input2_file.write(input_file_bytes)

        with open(manifest_filename, 'w') as manifest_file:
            manifest_file.writelines([
                '%s\n' % input1_filename, '%s\n' % input2_filename])

        job = self._harness_job(
            MRNickNackWithHadoopInputFormat, runner_alias='local',
            spark_conf=self.SPARK_CONF, input_paths=([manifest_filename]))

        with job.make_runner() as runner:
            runner.run()

            output_counts = dict(
                line.strip().split(b'\t')
                for line in to_lines(runner.cat_output()))

        expected_output_counts = {b'"tomato"': b'2', b'"potato"': b'2'}
        self.assertEqual(expected_output_counts, output_counts)


class EmulateMapInputFileTestCase(SparkHarnessOutputComparisonBaseTestCase):

    def test_one_file(self):
        two_lines_path = self.makefile('two_lines', b'line\nother line\n')

        self._assert_output_matches(
            MRCountLinesByFile,
            emulate_map_input_file=True,
            input_paths=[two_lines_path])


class PreservesPartitioningTestCase(SandboxedTestCase):

    # ensure that Spark doesn't repartition values once they're grouped
    # by key.
    #
    # unfortunately, it's hard to "catch" Spark re-partitioning (espeically
    # since our code doesn't give it a good reason to re-partition), so we
    # instead use mocks and check that the RDD was called with
    # preservesPartitioning=True when necessary

    def mock_rdd(self):
        """Make a mock RDD that returns itself."""
        method_names = [
            'combineByKey',
            'flatMap',
            'groupBy',
            'map',
            'mapPartitions',
            'mapValues',
        ]

        rdd = Mock(spec=method_names)

        for name in method_names:
            getattr(rdd, name).return_value = rdd

        return rdd

    def test_run_combiner_with_sort_values(self):
        self._test_run_combiner(sort_values=True)

    def test_run_combiner_with_num_reducers(self):
        self._test_run_combiner(num_reducers=1)

    def test_run_combiner_without_sort_values_and_num_reducers(self):
        self._test_run_combiner()

    def _test_run_combiner(self, sort_values=False, num_reducers=None):
        rdd = self.mock_rdd()

        combiner_job = Mock()
        combiner_job.pick_protocols.return_value = (Mock(), Mock())

        final_rdd = _run_combiner(
            combiner_job, rdd,
            sort_values=sort_values, num_reducers=num_reducers)
        self.assertEqual(final_rdd, rdd)  # mock RDD's methods return it

        # check that we preserve partitions after calling combineByKey()
        #
        # Python 3.4 and 3.5's mock modules have slightly different ways
        # of tracking function calls. to work around this, we avoid calling
        # assert_called() and just inspect `method_calls` directly
        called_combineByKey = False
        for name, args, kwargs in rdd.method_calls:
            if called_combineByKey:
                # mapValues() doesn't have to use preservesPartitioning
                # because it's just encoding the list of all values for a key
                if name == 'mapValues':
                    f = args[0]
                    self._assert_maps_list_to_list_of_same_size(f)
                else:
                    self.assertEqual(kwargs.get('preservesPartitioning'), True)
            elif name == 'combineByKey':
                called_combineByKey = True

        # check that combineByKey() was actually called
        self.assertTrue(called_combineByKey)

    def _assert_maps_list_to_list_of_same_size(self, f):
        # used by _test_run_combiner() to ensure that our call to
        # mapValues() doesn't split keys between partitions
        f_of_values = f([('k', 'v1'), ('k', 'v2')])
        self.assertEqual(type(f_of_values), list)
        self.assertEqual(len(f_of_values), 2)
        self.assertRaises(TypeError, f, 123)  # iterables only, fools

    def test_shuffle_and_sort_with_sort_values(self):
        self._test_shuffle_and_sort(sort_values=True)

    def test_shuffle_and_sort_with_num_reducers(self):
        self._test_shuffle_and_sort(num_reducers=1)

    def test_shuffle_and_sort_without_sort_values_and_num_reducers(self):
        self._test_shuffle_and_sort()

    def _test_shuffle_and_sort(self, sort_values=False, num_reducers=None):
        rdd = self.mock_rdd()

        final_rdd = _shuffle_and_sort(
            rdd, sort_values=sort_values, num_reducers=num_reducers)
        self.assertEqual(final_rdd, rdd)  # mock RDD's methods return it

        # check that we always preserve partitioning after groupBy()
        called_groupBy = False
        for name, args, kwargs in rdd.method_calls:
            if called_groupBy:
                if '.' in name:
                    continue  # Python 3.4/3.5 tracks groupBy.assert_called()
                self.assertEqual(kwargs.get('preservesPartitioning'), True)
            elif name == 'groupBy':
                called_groupBy = True

        # check that groupBy() was actually called
        self.assertTrue(called_groupBy)

    def test_run_reducer_with_num_reducers(self):
        self._test_run_reducer(num_reducers=1)

    def test_run_reducer_without_num_reducers(self):
        self._test_run_reducer()

    def _test_run_reducer(self, num_reducers=None):
        rdd = self.mock_rdd()

        def make_mock_mrc_job(mrc, step_num):
            job = Mock()
            job.pick_protocols.return_value = (Mock(), Mock())

            return job

        final_rdd = _run_reducer(
            make_mock_mrc_job, 0, rdd, num_reducers=num_reducers)
        self.assertEqual(final_rdd, rdd)  # mock RDD's methods return it

        called_mapPartitions = False

        before_mapPartitions = True
        for name, args, kwargs in rdd.method_calls:
            if name == 'mapPartitions':
                called_mapPartitions = True
                before_mapPartitions = False

            # We want to make sure we keep the original partition before
            # reaching mapPartitions()
            #
            # (currently there aren't any method calls before mapPartitions(),
            # but there were when this test was created)
            if before_mapPartitions:
                self.assertEqual(kwargs.get('preservesPartitioning'), True)

            # Once we've run mapPartitions(), we only need to preserve
            # partitions if the user explicitly requested a certain number
            else:
                self.assertEqual(
                    kwargs.get('preservesPartitioning'), bool(num_reducers))

        # sanity-check that mapPartitions() was actually called
        self.assertTrue(called_mapPartitions)
