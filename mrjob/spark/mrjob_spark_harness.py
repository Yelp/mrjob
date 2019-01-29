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
from argparse import REMAINDER
from importlib import import_module

from mrjob.util import shlex_split

def main(cmd_line_args=None):
    if cmd_line_args is None:
        cmd_line_args = sys.argv[1:]

    parser = _make_arg_parser()
    args = parser.parse_args(cmd_line_args)

    # get job_class
    job_module_name, job_class_name = args.job_class.rsplit('.', 1)
    job_module = import_module(job_module_name)
    job_class = getattr(job_module, job_class_name)

    job_args = shlex_split(args.passthru_args)

    def make_job(*args):
        j = job_class(job_args + list(args))
        j.sandbox()  # so Spark doesn't try to serialize stdin
        return j

    # load initial data
    from pyspark import SparkContext
    sc = SparkContext()
    rdd = sc.textFile(args.input_path, use_unicode=False)

    # get job steps. don't pass --steps, which is deprecated
    steps = make_job().steps()

    # process steps
    for step_num, step in enumerate(steps):
        rdd = _run_step(step, step_num, rdd, make_job)

    # write the results
    rdd.saveAsTextFile(
        args.output_path, compressionCodecClass=args.compression_codec)


def _run_step(step, step_num, rdd, make_job):
    """Run the given step on the RDD and return the transformed RDD."""
    step_desc = step.description(step_num)
    _check_step(step_desc, step_num)

    if step_desc.get('mapper'):
        # creating a separate job instance to ensure that initialization
        # happens correctly (e.g. mapper_job.is_task() should be True).
        # probably doesn't actually matter that we pass --step-num
        mapper_job = make_job('--mapper', '--step-num=%d' % step_num)
        m_read, m_write = mapper_job.pick_protocols(step_num, 'mapper')

        # run the mapper
        rdd = rdd.map(m_read)
        rdd = rdd.mapPartitions(
            lambda pairs: mapper_job.map_pairs(pairs, step_num))
        rdd = rdd.map(lambda k_v: m_write(*k_v))

    if step_desc.get('reducer'):
        reducer_job = make_job('--reducer', '--step-num=%d' % step_num)
        r_read, r_write = reducer_job.pick_protocols(step_num, 'reducer')

        # simulate shuffle in Hadoop Streaming
        rdd = rdd.groupBy(lambda line: line.split(b'\t')[0])

        if reducer_job.sort_values():
            rdd = rdd.flatMap(lambda key_and_lines: sorted(key_and_lines[1]))
        else:
            rdd = rdd.flatMap(lambda key_and_lines: key_and_lines[1])

        # run the reducer
        rdd = rdd.map(r_read)
        rdd = rdd.mapPartitions(
            lambda pairs: reducer_job.reduce_pairs(pairs, step_num))
        rdd = rdd.map(lambda k_v: r_write(*k_v))

    return rdd


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
        # bad combiners won't cause an error because they're optional
        substep_desc = step_desc.get(mrc)
        if not substep_desc:
            continue

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

    parser.add_argument(
        '--compression-codec',
        dest='compression_codec',
        help=('Java class path of a codec to use to compress output.'))

    parser.add_argument(
        '--passthru-args',
        default='',
        help=('The arguments pass to the MRJob. Please quote all passthru args'
              ' so that they are in the same string')
    )
    return parser


if __name__ == '__main__':
    main()
