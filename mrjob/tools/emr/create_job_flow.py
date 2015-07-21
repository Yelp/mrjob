# Copyright 2009-2013 Yelp and Contributors
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
"""Create a persistent EMR job flow, using bootstrap scripts and other
configs from :py:mod:`mrjob.conf`, and print the job flow ID to stdout.

Usage::

    mrjob create-job-flow
    python -m mrjob.tools.emr.create_job_flow

**WARNING**: do not run this without having
:py:mod:`mrjob.tools.emr.terminate_idle_job_flows` in your crontab; job flows
left idle can quickly become expensive!
"""
from optparse import OptionParser
from optparse import OptionGroup

from mrjob.emr import EMRJobRunner
from mrjob.job import MRJob
from mrjob.options import add_basic_opts
from mrjob.options import add_emr_connect_opts
from mrjob.options import add_emr_launch_opts
from mrjob.util import scrape_options_into_new_groups


def main(args=None):
    """Run the create_job_flow tool with arguments from ``sys.argv`` and
    printing to ``sys.stdout``."""
    runner = EMRJobRunner(**runner_kwargs(args))
    emr_job_flow_id = runner.make_persistent_job_flow()
    print emr_job_flow_id


def runner_kwargs(cl_args=None):
    """Parse command line arguments into arguments for
    :py:class:`EMRJobRunner`
    """
    # parser command-line args
    option_parser = make_option_parser()
    options, args = option_parser.parse_args(cl_args)

    if args:
        option_parser.error('takes no arguments')

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)

    # create the persistent job
    kwargs = options.__dict__.copy()
    del kwargs['quiet']
    del kwargs['verbose']
    return kwargs


def make_option_parser():
    usage = '%prog [options]'
    description = (
        'Create a persistent EMR job flow to run jobs in. WARNING: do not run'
        ' this without mrjob.tools.emr.terminate_idle_job_flows in your'
        ' crontab; job flows left idle can quickly become expensive!')
    option_parser = OptionParser(usage=usage, description=description)

    add_basic_opts(option_parser)
    # these aren't nicely broken down, just scrape specific options
    scrape_options_into_new_groups(MRJob().all_option_groups(), {
        option_parser: (
            'bootstrap_mrjob',
            'label',
            'owner',
        ),
    })
    add_emr_connect_opts(option_parser)
    add_emr_launch_opts(option_parser)

    return option_parser


if __name__ == '__main__':
    main()
