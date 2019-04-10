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
"""A Spark script that can run a MRJob without Hadoop."""
import sys
import json
from argparse import ArgumentParser
from collections import defaultdict
from importlib import import_module

from mrjob.util import shlex_split
from pyspark.accumulators import AccumulatorParam


# tuples of (args, kwargs) for ArgumentParser.add_argument()
#
# TODO: this is shared code with mr_spark_harness.py, which started out
# in this directory but has since been moved to tests/. Totally fine to
# inline this stuff and reduplicate it in mr_spark_harness.py
_PASSTHRU_OPTIONS = [
    (['--job-args'], dict(
        default=None,
        dest='job_args',
        help=('The arguments pass to the MRJob. Please quote all passthru args'
              ' so that they are in the same string'),
    )),
    (['--first-step-num'], dict(
        default=None,
        dest='first_step_num',
        type=int,
        help=("(0-indexed) first step in range of steps to run")
    )),
    (['--last-step-num'], dict(
        default=None,
        dest='last_step_num',
        type=int,
        help=("(0-indexed) last step in range of steps to run")
    )),
    (['--compression-codec'], dict(
        default=None,
        dest='compression_codec',
        help=('Java class path of a codec to use to compress output.'),
    )),
    (['--counter-output-dir'], dict(
        default=None,
        dest='counter_output_dir',
        help=(
            'An empty directory to write counter output to. '
            'Can be a path or URI.')
    )),
    (['--num-reducers'], dict(
        default=None,
        dest='num_reducers',
        type=int,
        help=('Set number of reducers (and thus number of output files)')
    )),
    # switches to deal with jobs that can't be instantiated in the Spark
    # driver (e.g. because of problems with file upload args). See #2044
    (['--steps-desc'], dict(
        default=None,
        dest='steps_desc',
        help=("Description of job's steps, in JSON format (otherwise,"
              " we'll instantiate a job and ask it"),
    )),
    (['--hadoop-input-format'], dict(
        default=None,
        dest='hadoop_input_format',
        help=("Hadoop input format class. Set to '' to indicate no"
              " format (otherwise we'll instantiate a job and ask it"),
    )),
    (['--hadoop-output-format'], dict(
        default=None,
        dest='hadoop_output_format',
        help=("Hadoop output format class. Set to '' to indicate no"
              " format (otherwise we'll instantiate a job and ask it"),
    )),
]


class CounterAccumulator(AccumulatorParam):

    def zero(self, value):
        return value

    def addInPlace(self, value1, value2):
        for group in value2:
            for key in value2[group]:
                if key not in value1[group]:
                    value1[group][key] = value2[group][key]
                else:
                    value1[group][key] += value2[group][key]
        return value1


def main(cmd_line_args=None):
    if cmd_line_args is None:
        cmd_line_args = sys.argv[1:]

    parser = _make_arg_parser()
    args = parser.parse_args(cmd_line_args)

    if args.num_reducers is not None and args.num_reducers <= 0:
        raise ValueError(
            'You can only configure num_reducers to positive number.')

    # get job_class
    job_module_name, job_class_name = args.job_class.rsplit('.', 1)
    job_module = import_module(job_module_name)
    job_class = getattr(job_module, job_class_name)

    # load initial data
    from pyspark import SparkContext

    if args.job_args:
        job_args = shlex_split(args.job_args)
    else:
        job_args = []

    # determine hadoop_*_format, steps
    # try to avoid instantiating a job in the driver; see #2044
    job = None

    if args.hadoop_input_format is None:
        job = job or job_class(job_args)
        hadoop_input_format = job.hadoop_input_format()
    else:
        hadoop_input_format = args.hadoop_input_format or None

    if args.hadoop_output_format is None:
        job = job or job_class(job_args)
        hadoop_output_format = job.hadoop_output_format()
    else:
        hadoop_output_format = args.hadoop_output_format or None

    if args.steps_desc is None:
        job = job or job_class(job_args)
        steps = [step.description(step_num)
                 for step_num, step in enumerate(job.steps())]
    else:
        steps = json.loads(args.steps_desc)

    # pick steps
    start = args.first_step_num or 0
    end = None if args.last_step_num is None else args.last_step_num + 1
    steps_to_run = list(enumerate(steps))[start:end]

    sc = SparkContext()

    # keep track of one set of counters per job step
    counter_accumulators = [
        sc.accumulator(defaultdict(dict), CounterAccumulator())
        for _ in steps_to_run
    ]

    def make_increment_counter(step_num):
        counter_accumulator = counter_accumulators[step_num - start]

        def increment_counter(group, name, amount=1):
            counter_accumulator.add({group: {name:  amount}})

        return increment_counter

    def make_mrc_job(mrc, step_num):
        j = job_class(job_args + [
            '--%s' % mrc, '--step-num=%d' % step_num
        ])

        # patch increment_counter() to update the accumulator for this step
        j.increment_counter = make_increment_counter(step_num)

        return j

    try:
        if hadoop_input_format:
            rdd = sc.hadoopFile(
                args.input_path,
                inputFormatClass=job.hadoop_input_format(),
                keyClass='org.apache.hadoop.io.Text',
                valueClass='org.apache.hadoop.io.Text')

            # hadoopFile loads each line as a key-value pair in which the
            # contents of the line are the key and the value is an empty
            # string. Convert to an rdd of just lines, encoded as bytes.
            rdd = rdd.map(lambda kv: kv[0].encode('utf-8'))
        else:
            rdd = sc.textFile(args.input_path, use_unicode=False)

        # run steps
        for step_num, step in steps_to_run:
            rdd = _run_step(step, step_num, rdd,
                            make_mrc_job, args.num_reducers)

        # max_output_files: limit number of partitions
        if args.max_output_files:
            rdd = rdd.coalesce(args.max_output_files)

        # write the results
        if hadoop_output_format:
            # saveAsHadoopFile takes an rdd of key-value pairs, so convert to
            # that format
            rdd = rdd.map(lambda line: tuple(
                x.decode('utf-8') for x in line.split(b'\t', 1)))
            rdd.saveAsHadoopFile(
                args.output_path,
                outputFormatClass=job.hadoop_output_format(),
                compressionCodecClass=args.compression_codec)
        else:
            rdd.saveAsTextFile(
                args.output_path, compressionCodecClass=args.compression_codec)
    finally:
        if args.counter_output_dir is not None:
            counters = [ca.value for ca in counter_accumulators]

            sc.parallelize(
                [json.dumps(counters)],
                numSlices=1
            ).saveAsTextFile(
                args.counter_output_dir
            )


def _run_step(step, step_num, rdd, make_mrc_job, num_reducers=None):
    """Run the given step on the RDD and return the transformed RDD."""
    _check_step(step, step_num)

    # create a separate job instance for each substep. This contains
    # *step_num* (in ``job.options.step_num``) and ensures that
    # ``job.is_task()`` is set to true
    mapper_job, reducer_job, combiner_job = (
        make_mrc_job(mrc, step_num) if step.get(mrc) else None
        for mrc in ('mapper', 'reducer', 'combiner')
    )

    # if combiner runs subprocesses, skip it. (:py:func:`_check_step`
    # already screens out mappers and reducers that do, but combiners
    # are optional)
    try:
        _check_substep(step, step_num, 'combiner')
    except NotImplementedError:
        combiner_job = None

    # is SORT_VALUES enabled?
    sort_values = reducer_job.sort_values() if reducer_job else False

    if step.get('mapper'):
        rdd = _run_mapper(make_mrc_job, step_num, rdd)

    if combiner_job:
        # _run_combiner() includes shuffle-and-sort
        rdd = _run_combiner(
            combiner_job, rdd,
            sort_values=sort_values,
            num_reducers=num_reducers)
    elif reducer_job:
        rdd = _shuffle_and_sort(
            rdd, sort_values=sort_values, num_reducers=num_reducers)

    if reducer_job:
        rdd = _run_reducer(reducer_job, rdd, num_reducers=num_reducers)

    return rdd


def _run_mapper(make_mrc_job, step_num, rdd):
    """Run our job's mapper.

    :param mapper_job: an instance of our job, instantiated to be the mapper
                       for the step we wish to run
    :param rdd: an RDD containing lines representing encoded key-value pairs
    :return: an RDD containing lines representing encoded key-value pairs
    """
    # initialize job class inside mapPartitions(). this deals with jobs that
    # can't be initialized in the Spark driver (see #2044)
    def map_lines(lines):
        job = make_mrc_job('mapper', step_num)

        m_read, m_write = job.pick_protocols(step_num, 'mapper')

        def read_pairs():
            # decode lines into key-value pairs
            #
            # line -> (k, v)
            for line in lines:
                yield m_read(line)

        for k, v in job.map_pairs(read_pairs(), step_num):
            # encode key-value pairs back into lines
            #
            # (k, v) -> line
            out_line = m_write(k, v)
            yield out_line

    return rdd.mapPartitions(map_lines)


def _run_combiner(combiner_job, rdd, sort_values=False, num_reducers=None):
    """Run our job's combiner, and group lines with the same key together.

    :param combiner_job: an instance of our job, instantiated to be the mapper
                         for the step we wish to run
    :param rdd: an RDD containing lines representing encoded key-value pairs
    :param sort_values: if true, ensure all lines corresponding to a given key
                        are sorted (by their encoded value)
    :param num_reducers: limit the number of paratitions of output rdd, which
                         is similar to mrjob's limit on number of reducers.
    :return: an RDD containing "reducer ready" lines representing encoded
             key-value pairs, that is, where all lines with the same key are
             adjacent and in the same partition
    """
    step_num = combiner_job.options.step_num

    c_read, c_write = combiner_job.pick_protocols(step_num, 'combiner')

    # decode lines into key-value pairs
    #
    # line -> (k, v)
    rdd = rdd.map(c_read)

    # The common case for MRJob combiners is to yield a single key-value pair
    # (for example ``(key, sum(values))``. If the combiner does something
    # else, just build a list of values so we don't end up running multiple
    # values through the MRJob's combiner multiple times.
    def combiner_helper(pairs1, pairs2):
        if len(pairs1) == len(pairs2) == 1:
            return list(
                combiner_job.combine_pairs(pairs1 + pairs2, step_num),
            )
        else:
            pairs1.extend(pairs2)
            return pairs1

    # include key in "value", so MRJob combiner can see it
    #
    # (k, v) -> (k, (k, v))
    rdd = rdd.map(lambda k_v: (k_v[0], k_v))

    # :py:meth:`pyspark.RDD.combineByKey()`, where the magic happens.
    #
    # (k, (k, v)), ... -> (k, ([(k, v1), (k, v2), ...]))
    #
    # Our "values" are key-value pairs, and our "combined values" are lists of
    # key-value pairs (single-item lists in the common case).
    #
    # note that unlike Hadoop combiners, combineByKey() sees *all* the
    # key-value pairs, essentially doing a shuffle-and-sort for free.
    rdd = rdd.combineByKey(
        createCombiner=lambda k_v: [k_v],
        mergeValue=lambda k_v_list, k_v: combiner_helper(k_v_list, [k_v]),
        mergeCombiners=combiner_helper,
        numPartitions=num_reducers
    )

    # encode lists of key-value pairs into lists of lines
    #
    # (k, ([(k, v1), (k, v2), ...])) -> (k, [line1, line2, ...])
    rdd = rdd.mapValues(
        lambda pairs: [c_write(*pair) for pair in pairs])

    # free the lines!
    #
    # (k, [line1, line2, ...]) -> line1, line2, ...
    rdd = _discard_key_and_flatten_values(rdd, sort_values=sort_values)

    return rdd


def _shuffle_and_sort(rdd, sort_values=False, num_reducers=None):
    """Simulate Hadoop's shuffle-and-sort step, so that data will be in the
    format the reducer expects.

    :param rdd: an RDD containing lines representing encoded key-value pairs,
                where the encoded key comes first and is followed by a TAB
                character (the encoded key may not contain TAB).
    :param sort_values: if true, ensure all lines corresponding to a given key
                        are sorted (by their encoded value)
    :param num_reducers: limit the number of paratitions of output rdd, which
                         is similar to mrjob's limit on number of reducers.
    :return: an RDD containing "reducer ready" lines representing encoded
             key-value pairs, that is, where all lines with the same key are
             adjacent and in the same partition
    """
    rdd = rdd.groupBy(
        lambda line: line.split(b'\t')[0], numPartitions=num_reducers)
    rdd = _discard_key_and_flatten_values(rdd, sort_values=sort_values)

    return rdd


def _run_reducer(reducer_job, rdd, num_reducers=None):
    """Run our job's combiner, and group lines with the same key together.

    :param reducer_job: an instance of our job, instantiated to be the mapper
                        for the step we wish to run
    :param rdd: an RDD containing "reducer ready" lines representing encoded
                key-value pairs, that is, where all lines with the same key are
                adjacent and in the same partition
    :param num_reducers: limit the number of paratitions of output rdd, which
                         is similar to mrjob's limit on number of reducers.
    :return: an RDD containing encoded key-value pairs
    """
    step_num = reducer_job.options.step_num
    should_preserve_final_partitions = bool(num_reducers)

    r_read, r_write = reducer_job.pick_protocols(step_num, 'reducer')

    # decode lines into key-value pairs (keeping partitions the same)
    #
    # line -> (k, v)
    rdd = rdd.map(r_read, preservesPartitioning=True)

    # run each partition through reducer_pairs(). because we carefully
    # preserved partitioning, all values belonging to the same key will
    # be fed to the same call of reducer_pairs()
    #
    # (k, v), ... -> (k, v), ...
    rdd = rdd.mapPartitions(
        lambda pairs: reducer_job.reduce_pairs(pairs, step_num),
        preservesPartitioning=should_preserve_final_partitions)

    # encode key-value pairs back into lines
    #
    # (k, v) -> line
    rdd = rdd.map(
        lambda k_v: r_write(*k_v),
        preservesPartitioning=should_preserve_final_partitions)

    return rdd


def _discard_key_and_flatten_values(rdd, sort_values=False):
    """Helper function for :py:func:`_run_combiner` and
    :py:func:`_shuffle_and_sort`.

    Given an RDD containing (key, [line1, line2, ...]), discard *key*
    and return an RDD containing line1, line2, ...

    Guarantees that lines in the same list will end up in the same partition.

    If *sort_values* is true, sort each list of lines before flattening it.
    """
    if sort_values:
        map_f = lambda key_and_lines: sorted(key_and_lines[1])
    else:
        map_f = lambda key_and_lines: key_and_lines[1]

    return rdd.flatMap(map_f, preservesPartitioning=True)


def _check_step(step, step_num):
    """Check that the given step description is for a MRStep
    with no input manifest"""
    if step.get('type') != 'streaming':
        raise ValueError(
            'step %d has unexpected type: %r' % (
                step_num, step.get('type')))

    if step.get('input_manifest'):
        raise NotImplementedError(
            'step %d uses an input manifest, which is unsupported')

    for mrc in ('mapper', 'reducer'):
        _check_substep(step, step_num, mrc)


def _check_substep(step, step_num, mrc):
    """Raise :py:class:`NotImplementedError` if the given substep
    (e.g. ``'mapper'``) runs subprocesses."""
    substep = step.get(mrc)
    if not substep:
        return

    if substep.get('type') != 'script':
        raise NotImplementedError(
            "step %d's %s has unexpected type: %r" % (
                step_num, mrc, substep.get('type')))

    if substep.get('pre_filter'):
        raise NotImplementedError(
            "step %d's %s has pre-filter, which is unsupported" % (
                step_num, mrc))


def _make_arg_parser():
    parser = ArgumentParser()

    parser.add_argument(
        dest='job_class',
        help=('dot-separated module and name of MRJob class. For example:'
              ' mrjob.examples.mr_wc.MRWordCountUtility'))

    parser.add_argument(
        dest='input_path',
        help=('Where to read input from. Can be a path or a URI, or several of'
              ' these joined by commas'))

    parser.add_argument(
        dest='output_path',
        help=('An empty directory to write output to. Can be a path or URI.'))

    # can't put this in _PASSTHRU_OPTIONS because it's also a runner opt
    parser.add_argument(
        '--max-output-files',
        dest='max_output_files',
        type=int,
        help='Directly limit number of output files, using coalesce()',
    )

    for args, kwargs in _PASSTHRU_OPTIONS:
        parser.add_argument(*args, **kwargs)

    return parser


if __name__ == '__main__':
    main()
