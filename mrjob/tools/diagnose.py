# Copyright 2017 Yelp
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
"""Print probable cause of error for a failed step."""
from argparse import ArgumentParser
from logging import getLogger

from mrjob.emr import EMRJobRunner
from mrjob.job import MRJob
from mrjob.logs.errors import _format_error
from mrjob.options import _add_basic_args
from mrjob.options import _add_runner_args
from mrjob.options import _alphabetize_actions
from mrjob.options import _filter_by_role

log = getLogger(__name__)


def main(cl_args=None):
    arg_parser = _make_arg_parser()
    options = arg_parser.parse_args(cl_args)

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)

    runner_kwargs = {k:v for k, v in options.__dict__.items()
                     if k not in ('quiet', 'verbose', 'step_id')}

    runner = EMRJobRunner(**runner_kwargs)
    emr_client = runner.make_emr_client()

    if options.step_id:
        step = get_step(emr_client, options.cluster_id, options.step_id)
    else:
        step = get_last_failed_step(emr_client, options.cluster_id)

    log_interpretation = dict(step_id=step['Id'])

    # TODO: infer step type
    error = runner._pick_error(log_interpretation, step_type='streaming')

    if error:
        log.error('Probable cause of failure:\n\n%s\n\n' %
                              _format_error(error))
    else:
        log.warning('No error detected')


def get_step(emr_client, cluster_id, step_id):
    steps = emr_client.list_steps(
        ClusterId=cluster_id, StepIDs=[step_id])['Steps']

    if not steps:
        raise ValueError('Step %s not found in cluster %s' %
                         step_id, cluster_id)

    step = steps[0]
    state = step['Status']['State']
    if state != 'FAILED':
        log.warning('step %s is %s, not FAILED' % (step['Id'], state))

    return step


def get_last_failed_step(emr_client, cluster_id):
    steps = emr_client.list_steps(
        ClusterId=cluster_id, StepStates=['FAILED'])['Steps']

    if not steps:
        raise ValueError('Cluster %s has no FAILED steps' % cluster_id)

    return steps[0]


def _make_arg_parser():
    usage = '%(prog)s [opts] [--step-id STEP_ID] CLUSTER_ID'
    description = (
        'Get probable cause of failure for step on CLUSTER_ID.'
        ' By default we look at the last failed step')
    arg_parser = ArgumentParser(usage=usage, description=description)

    _add_basic_args(arg_parser)
    _add_runner_args(
        arg_parser,
        _filter_by_role(EMRJobRunner.OPT_NAMES, 'connect'))

    arg_parser.add_argument(
        dest='cluster_id',
        help='ID of cluster with failed step')
    arg_parser.add_argument(
        '--step-id', dest='step_id',
        help='ID of a particular failed step to diagnose')

    _alphabetize_actions(arg_parser)

    return arg_parser



if __name__ == '__main__':
    main()
