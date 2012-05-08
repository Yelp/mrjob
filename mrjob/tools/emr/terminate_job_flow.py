# Copyright 2009-2010 Yelp
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
"""Terminate an existing EMR job flow.

Usage::

    python -m mrjob.tools.emr.terminate_job_flow [options] j-JOBFLOWID

Terminate an existing EMR job flow.

Options::

  -h, --help            show this help message and exit
  -v, --verbose         print more messages to stderr
  -q, --quiet           don't print anything
  -c CONF_PATH, --conf-path=CONF_PATH
                        Path to alternate mrjob.conf file to read from
  --no-conf             Don't load mrjob.conf even if it's available

"""
from __future__ import with_statement

import logging
from optparse import OptionParser

from mrjob.emr import EMRJobRunner
from mrjob.job import MRJob

log = logging.getLogger('mrjob.tools.emr.terminate_job_flow')


def main():
    # parser command-line args
    option_parser = make_option_parser()
    options, args = option_parser.parse_args()

    if len(args) != 1:
        option_parser.error('This tool takes exactly one argument.')
    emr_job_flow_id = args[0]

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)

    # create the persistent job
    runner = EMRJobRunner(conf_path=options.conf_path)
    log.debug('Terminating job flow %s' % emr_job_flow_id)
    runner.make_emr_conn().terminate_jobflow(emr_job_flow_id)
    log.info('Terminated job flow %s' % emr_job_flow_id)


def make_option_parser():
    usage = '%prog [options] jobflowid'
    description = 'Terminate an existing EMR job flow.'
    option_parser = OptionParser(usage=usage, description=description)
    option_parser.add_option(
        '-v', '--verbose', dest='verbose', default=False,
        action='store_true',
        help='print more messages to stderr')
    option_parser.add_option(
        '-q', '--quiet', dest='quiet', default=False,
        action='store_true',
        help="don't print anything")
    option_parser.add_option(
        '-c', '--conf-path', dest='conf_path', default=None,
        help='Path to alternate mrjob.conf file to read from')
    option_parser.add_option(
        '--no-conf', dest='conf_path', action='store_false',
        help="Don't load mrjob.conf even if it's available")

    return option_parser


if __name__ == '__main__':
    main()
