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
from importlib import import_module

from pyspark import SparkContext


def main(cmd_line_args=None):
    if cmd_line_args is None:
        cmd_line_args = sys.argv[1:]

    parser = _make_arg_parser()
    args = parser.parse_args(cmd_line_args)

    # get job_class
    job_module_name, job_class_name = args.job_class.rsplit('.', 1)
    job_module = import_module(job_module_name)
    job_class = getattr(job_module, job_class_name)

    # eventually will want to set this for passthrough args, etc.
    job_args = []

    def job(*args):
        j = job_class(job_args + list(args))
        j.sandbox()  # so Spark doesn't try to serialize stdin
        return j

    # load initial data
    sc = SparkContext()
    rdd = sc.textFile(args.input_path, use_unicode=False)

    # get job steps. don't pass --steps, which is deprecated
    steps = job().steps()

    # process steps
    for step_num, step in enumerate(steps):
        step_desc = step.description()
        _check_step(step_desc, step_num)

        if step_desc.get('mapper'):
            # creating a separate job instance to ensure that initialization
            # happens correctly (e.g. mapper_job.is_task() should be true)
            mapper_job = job('--mapper', '--step-num=%d' % step_num)
            m_read, m_write = mapper_job.pick_protocols(step_num, 'mapper')

            rdd = rdd.map(lambda line: m_read(line.rstrip(b'\r\n')))
            rdd = rdd.mapPartitions(
                lambda pairs: mapper_job.map_pairs(pairs, step_num=step_num))
            rdd = rdd.map(lambda k_v: m_write(*k_v) + b'\n')

        if step_desc.get('reducer'):
            reducer_job = job('--reducer', '--step-num=%d' % step_num)
            r_read, r_write = reducer_job.pick_protocols(step_num, 'reducer')

            # simulate shuffle in Hadoop Streaming
            rdd = rdd.groupBy(lambda line: line.split(b'\t')[0])
            rdd = rdd.flatMap(
                lambda key_and_lines: (r_read(line.rstrip(b'\r\n'))
                                       for line in key_and_lines[1]),
                preservesPartitioning=True)
            # run the reducer
            rdd = rdd.mapPartitions(
                lambda pairs: reducer_job.map_pairs(pairs, step_num=step_num))
            rdd = rdd.map(lambda k_v: r_write(*k_v) + b'\n')

    # write the results
    rdd.saveAsTextFile(args.output_path)


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

    return parser


if __name__ == '__main__':
    main()
