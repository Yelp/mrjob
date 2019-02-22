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
import uuid
from io import BytesIO
from os.path import join

from mrjob.examples.mr_word_freq_count import MRWordFreqCount
from mrjob.job import MRJob
from mrjob.local import LocalMRJobRunner
from mrjob.protocol import TextProtocol
from mrjob.spark import mrjob_spark_harness
from mrjob.spark.mr_spark_harness import MRSparkHarness
from mrjob.spark.mrjob_spark_harness import _run_combiner
from mrjob.spark.mrjob_spark_harness import _run_reducer
from mrjob.spark.mrjob_spark_harness import _shuffle_and_sort
from mrjob.step import INPUT
from mrjob.step import OUTPUT
from mrjob.util import cmd_line
from mrjob.util import to_lines

from tests.mr_doubler import MRDoubler
from tests.mr_streaming_and_spark import MRStreamingAndSpark
from tests.mr_sort_and_group import MRSortAndGroup
from tests.mr_two_step_job import MRTwoStepJob
from tests.mr_word_freq_count_with_combiner_cmd import \
     MRWordFreqCountWithCombinerCmd
from tests.py2 import Mock
from tests.py2 import call
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

    def mapper(self, _, line):
        word, value_str = line.split('\t', 1)
        yield word, int(value_str)

    def combiner(self, word, values):
        values_sum = sum(values)
        if values_sum != 0:
            yield word, values_sum

    reducer = combiner


class MRPassThruArgTest(MRJob):

    def configure_args(self):
        super(MRPassThruArgTest, self).configure_args()
        self.add_passthru_arg('--chars', action='store_true')
        self.add_passthru_arg('--ignore')

    def mapper(self, _, value):
        if self.options.ignore:
            value = value.replace(self.options.ignore, '')
        if self.options.chars:
            for c in value:
                yield c, 1
        else:
            for w in value.split(' '):
                yield w, 1

    def reducer(self, key, values):
        yield key, sum(values)


class SparkHarnessOutputComparisonTestCase(
        SandboxedTestCase, SingleSparkContextTestCase):

    def _spark_harness_path(self):
        path = mrjob_spark_harness.__file__
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
                     job_args=None, start_step=None, end_step=None):
        job_class_path = '%s.%s' % (job_class.__module__, job_class.__name__)

        harness_job_args = ['-r', runner_alias, '--job-class', job_class_path]
        if compression_codec:
            harness_job_args.append('--compression-codec')
            harness_job_args.append(compression_codec)
        if job_args:
            harness_job_args.extend(['--job-args', cmd_line(job_args)])
        if start_step:
            harness_job_args.extend(['--start-step', str(start_step)])
        if end_step:
            harness_job_args.extend(['--end-step', str(end_step)])

        harness_job_args.extend(input_paths)

        harness_job = MRSparkHarness(harness_job_args)
        harness_job.sandbox(stdin=BytesIO(input_bytes))

        return harness_job

    def _assert_output_matches(
            self, job_class, input_bytes=b'', input_paths=(), job_args=[]):

        # run classes defined in this module in inline mode, classes
        # with their own script files in local mode. used by
        # test_skip_combiner_that_runs_cmd()
        if job_class.__module__ == __name__:
            runner_alias = 'inline'
        else:
            runner_alias = 'local'

        reference_job = self._reference_job(
            job_class, input_bytes=input_bytes,
            input_paths=input_paths,
            job_args=job_args,
            runner_alias=runner_alias)

        with reference_job.make_runner() as runner:
            runner.run()

            reference_output = sorted(to_lines(runner.cat_output()))

        harness_job = self._harness_job(
            job_class, input_bytes=input_bytes,
            input_paths=input_paths,
            job_args=job_args)

        with harness_job.make_runner() as runner:
            runner.run()

            harness_output = sorted(to_lines(runner.cat_output()))

        self.assertEqual(harness_output, reference_output)

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
            start_step=0, end_step=1)

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
            start_step=2, end_step=4)

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
        # each word and the mrjob_spark_harness combiner_helper stops running
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
        # a more plausible test of a combiner that sometimes doesn't yield a
        # value that we can compare to the reference job
        input_bytes = b'\n'.join([
            b'happy\t5',
            b'sad\t3',
            b'happy\t2',
            b'sad\t-3',
        ])

        self._assert_output_matches(MRSumValuesByWord, input_bytes=input_bytes)

    def test_skip_combiner_that_runs_cmd(self):
        input_bytes = b'one fish\ntwo fish\nred fish\nblue fish\n'

        self._assert_output_matches(
            MRWordFreqCountWithCombinerCmd, input_bytes=input_bytes)


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

    def test_run_combiner_without_sort_values(self):
        self._test_run_combiner(sort_values=False)

    def _test_run_combiner(self, sort_values):
        rdd = self.mock_rdd()

        combiner_job = Mock()
        combiner_job.pick_protocols.return_value = (Mock(), Mock())

        final_rdd = _run_combiner(combiner_job, rdd)
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
        self.assertRaises(TypeError, 123)

    def test_shuffle_and_sort_with_sort_values(self):
        self._test_shuffle_and_sort(sort_values=True)

    def test_shuffle_and_sort_without_sort_values(self):
        self._test_shuffle_and_sort(sort_values=False)

    def _test_shuffle_and_sort(self, sort_values):
        rdd = self.mock_rdd()

        final_rdd = _shuffle_and_sort(rdd)
        self.assertEqual(final_rdd, rdd)  # mock RDD's methods return it

        # check that we always preserve partitioning after groupBy()
        called_groupBy = False
        for name, args, kwargs in rdd.method_calls:
            if called_groupBy:
                if '.' in name:
                    continue  # Python 3.4/3.5 tracks groupBy.assert_called()

                if not kwargs.get('preservesPartitioning'):
                    import pdb; pdb.set_trace()
                self.assertEqual(kwargs.get('preservesPartitioning'), True)
            elif name == 'groupBy':
                called_groupBy = True

        # check that groupBy() was actually called
        self.assertTrue(called_groupBy)

    def test_run_reducer(self):
        rdd = self.mock_rdd()

        reducer_job = Mock()
        reducer_job.pick_protocols.return_value = (Mock(), Mock())

        final_rdd = _run_reducer(reducer_job, rdd)
        self.assertEqual(final_rdd, rdd)  # mock RDD's methods return it

        called_mapPartitions = False
        for name, args, kwargs in rdd.method_calls:
            if name == 'mapPartitions':
                called_mapPartitions = True
                break  # nothing else to check
            else:
                self.assertEqual(kwargs.get('preservesPartitioning'), True)

        # sanity-check that mapPartitions() was actually called
        self.assertTrue(called_mapPartitions)
