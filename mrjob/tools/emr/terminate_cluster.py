# Copyright 2009-2012 Yelp
# Copyright 2013 David Marin and Steve Johnson
# Copyright 2015-2016 Yelp
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
"""Terminate an existing EMR cluster.

Usage::

    mrjob terminate-cluster [options] j-CLUSTERID

Terminate an existing EMR cluster.

Options::

  -h, --help            show this help message and exit
  -c CONF_PATHS, --conf-path=CONF_PATHS
                        Path to alternate mrjob.conf file to read from
  --no-conf             Don't load mrjob.conf even if it's available
  --emr-endpoint=EMR_ENDPOINT
                        Force mrjob to connect to EMR on this endpoint (e.g.
                        us-west-1.elasticmapreduce.amazonaws.com). Default is
                        to infer this from region.
  -q, --quiet           Don't print anything to stderr
  --region=REGION       GCE/AWS region to run Dataproc/EMR jobs in.
  --aws-region=REGION   Deprecated alias for --region
  --s3-endpoint=S3_ENDPOINT
                        Force mrjob to connect to S3 on this endpoint (e.g. s3
                        -us-west-1.amazonaws.com). You usually shouldn't set
                        this; by default mrjob will choose the correct
                        endpoint for each S3 bucket based on its location.
  -t, --test            Don't actually delete any files; just log that we
                        would
  -v, --verbose         print more messages to stderr
"""
import logging
from optparse import OptionParser

from mrjob.emr import EMRJobRunner
from mrjob.job import MRJob
from mrjob.options import _add_basic_options
from mrjob.options import _add_runner_options
from mrjob.options import _alphabetize_options
from mrjob.options import _pick_runner_opts

log = logging.getLogger(__name__)


def main(cl_args=None):
    # parser command-line args
    option_parser = _make_option_parser()
    options, args = option_parser.parse_args(cl_args)

    if len(args) != 1:
        option_parser.error('This tool takes exactly one argument.')
    cluster_id = args[0]

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)

    # create the persistent job
    runner = EMRJobRunner(**_runner_kwargs(options))
    log.debug('Terminating cluster %s' % cluster_id)
    runner.make_emr_conn().terminate_jobflow(cluster_id)
    log.info('Terminated cluster %s' % cluster_id)


def _make_option_parser():
    usage = '%prog [options] cluster-id'
    description = 'Terminate an existing EMR cluster.'

    option_parser = OptionParser(usage=usage, description=description)

    option_parser.add_option(
        '-t', '--test', dest='test', default=False,
        action='store_true',
        help="Don't actually delete any files; just log that we would")

    _add_basic_options(option_parser)
    _add_runner_options(
        option_parser,
        _pick_runner_opts('emr', 'connect'))

    _alphabetize_options(option_parser)
    return option_parser


def _runner_kwargs(options):
    kwargs = options.__dict__.copy()
    for unused_arg in ('quiet', 'verbose', 'test'):
        del kwargs[unused_arg]

    return kwargs


if __name__ == '__main__':
    main()
