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
        dest='counter_output_path',
        help=(
            'An empty directory to write counter output to. '
            'Can be a path or URI.')
    )),
]


class CounterAccumulator(AccumulatorParam):

    def zero(self, value):
        return value

    def addInPlace(self, value1, value2):
        for key in value2:
            value1[key] += value2[key]
        return value1

def print_counter_status(counter, output_stream):
    prev_group = None
    for (group, name), val in counter.value.items():
        if group != prev_group:
            output_stream.write(group + ':\n')
            prev_group = group
        output_stream.write('\t{}: {}\n'.format(name, val))


def main(cmd_line_args=None):
    if cmd_line_args is None:
        cmd_line_args = sys.argv[1:]

    parser = _make_arg_parser()
    args = parser.parse_args(cmd_line_args)

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

    sc = SparkContext()
    global counter
    counter = sc.accumulator(
        defaultdict(int),
        CounterAccumulator()
    )
    def increment_counter(group, name, amount=1):
        global counter
        counter += {(group, name):  amount}

    def make_job(*args):
        j = job_class(job_args + list(args))
        j.sandbox()  # so Spark doesn't try to serialize stdin
        j.increment_counter = increment_counter
        return j

    # get job steps. don't pass --steps, which is deprecated
    steps = make_job().steps()

    # pick steps
    start = args.first_step_num
    end = None if args.last_step_num is None else args.last_step_num + 1
    steps_to_run = list(enumerate(steps))[start:end]

    try:
        rdd = sc.textFile(args.input_path, use_unicode=False)

        # run steps
        for step_num, step in steps_to_run:
            rdd = _run_step(step, step_num, rdd, make_job)

        # write the results
        rdd.saveAsTextFile(
            args.output_path, compressionCodecClass=args.compression_codec)
    finally:
        # Output result of counters
        print_counter_status(counter, sys.stderr)
        if args.counter_output_path is not None:
            sc.parallelize(
                [json.dumps(counter.value)]
            ).saveAsTextFile(
                args.counter_output_path
            )


def _run_step(step, step_num, rdd, make_job):
    """Run the given step on the RDD and return the transformed RDD."""
    step_desc = step.description(step_num)
    _check_step(step_desc, step_num)

    # create a separate job instance for each substep. This contains
    # *step_num* (in ``job.options.step_num``) and ensures that
    # ``job.is_task()`` is set to true
    mapper_job, reducer_job, combiner_job = (
        make_job('--%s' % mrc, '--step-num=%d' % step_num)
        if step_desc.get(mrc) else None
        for mrc in ('mapper', 'reducer', 'combiner')
    )

    # if combiner runs subprocesses, skip it. (:py:func:`_check_step`
    # already screens out mappers and reducers that do, but combiners
    # are optional)
    try:
        _check_substep(step_desc, step_num, 'combiner')
    except NotImplementedError:
        combiner_job = None

    # is SORT_VALUES enabled?
    sort_values = reducer_job.sort_values() if reducer_job else False

    if mapper_job:
        rdd = _run_mapper(mapper_job, rdd)

    if combiner_job:
        # _run_combiner() includes shuffle-and-sort
        rdd = _run_combiner(combiner_job, rdd, sort_values=sort_values)
    elif reducer_job:
        rdd = _shuffle_and_sort(rdd, sort_values=sort_values)

    if reducer_job:
        rdd = _run_reducer(reducer_job, rdd)

    return rdd


def _run_mapper(mapper_job, rdd):
    """Run our job's mapper.

    :param mapper_job: an instance of our job, instantiated to be the mapper
                       for the step we wish to run
    :param rdd: an RDD containing lines representing encoded key-value pairs
    :return: an RDD containing lines representing encoded key-value pairs
    """
    step_num = mapper_job.options.step_num

    m_read, m_write = mapper_job.pick_protocols(step_num, 'mapper')

    # decode lines into key-value pairs
    #
    # line -> (k, v)
    rdd = rdd.map(m_read)

    # run each partition through map_pairs()
    #
    # (k, v), ... -> (k, v), ...
    rdd = rdd.mapPartitions(
        lambda pairs: mapper_job.map_pairs(pairs, step_num))

    # encode key-value pairs back into lines
    #
    # (k, v) -> line
    rdd = rdd.map(lambda k_v: m_write(*k_v))

    return rdd


def _run_combiner(combiner_job, rdd, sort_values=False):
    """Run our job's combiner, and group lines with the same key together.

    :param combiner_job: an instance of our job, instantiated to be the mapper
                         for the step we wish to run
    :param rdd: an RDD containing lines representing encoded key-value pairs
    :sort_values: if true, ensure all lines corresponding to a given key
                  are sorted (by their encoded value)
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


def _shuffle_and_sort(rdd, sort_values=False):
    """Simulate Hadoop's shuffle-and-sort step, so that data will be in the
    format the reducer expects.

    :param rdd: an RDD containing lines representing encoded key-value pairs,
                where the encoded key comes first and is followed by a TAB
                character (the encoded key may not contain TAB).
    :sort_values: if true, ensure all lines corresponding to a given key
                  are sorted (by their encoded value)
    :return: an RDD containing "reducer ready" lines representing encoded
             key-value pairs, that is, where all lines with the same key are
             adjacent and in the same partition
    """
    rdd = rdd.groupBy(lambda line: line.split(b'\t')[0])
    rdd = _discard_key_and_flatten_values(rdd, sort_values=sort_values)

    return rdd


def _run_reducer(reducer_job, rdd):
    """Run our job's combiner, and group lines with the same key together.

    :param reducer_job: an instance of our job, instantiated to be the mapper
                        for the step we wish to run
    :param rdd: an RDD containing "reducer ready" lines representing encoded
                key-value pairs, that is, where all lines with the same key are
                adjacent and in the same partition
    :return: an RDD containing encoded key-value pairs
    """
    step_num = reducer_job.options.step_num

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
        lambda pairs: reducer_job.reduce_pairs(pairs, step_num))

    # encode key-value pairs back into lines
    #
    # (k, v) -> line
    rdd = rdd.map(lambda k_v: r_write(*k_v))

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


def _check_step(step_desc, step_num):
    """Check that the given step description is for a MRStep
    with no input manifest"""
    if step_desc.get('type') != 'streaming':
        raise ValueError(
            'step %d has unexpected type: %r' % (
                step_num, step_desc.get('type')))

    if step_desc.get('input_manifest'):
        raise NotImplementedError(
            'step %d uses an input manifest, which is unsupported')

    for mrc in ('mapper', 'reducer'):
        _check_substep(step_desc, step_num, mrc)


def _check_substep(step_desc, step_num, mrc):
    """Raise :py:class:`NotImplementedError` if the given substep
    (e.g. ``'mapper'``) runs subprocesses."""
    substep_desc = step_desc.get(mrc)
    if not substep_desc:
        return

    if substep_desc.get('type') != 'script':
        raise NotImplementedError(
            "step %d's %s has unexpected type: %r" % (
                step_num, mrc, substep_desc.get('type')))

    if substep_desc.get('pre_filter'):
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

    for args, kwargs in _PASSTHRU_OPTIONS:
        parser.add_argument(*args, **kwargs)

    return parser


if __name__ == '__main__':
    main()
