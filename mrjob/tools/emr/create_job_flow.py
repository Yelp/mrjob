# Copyright 2009-2011 Yelp
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

    python -m mrjob.tools.emr.create_job_flow

**WARNING**: do not run this without having
:py:mod:`mrjob.tools.emr.terminate.idle_job_flows` in your crontab; job flows
left idle can quickly become expensive!
"""
from __future__ import with_statement

from optparse import OptionParser
from optparse import OptionGroup

from mrjob.emr import EMRJobRunner
from mrjob.job import MRJob
from mrjob.util import scrape_options_into_new_groups
from mrjob.util import log_to_stream


def main():
    # parser command-line args
    option_parser = make_option_parser()
    options, args = option_parser.parse_args()

    if args:
        option_parser.error('takes no arguments')

    # set up logging
    if not options.quiet:
        log_to_stream(name='mrjob', debug=options.verbose)

    # create the persistent job
    runner_kwargs = options.__dict__.copy()
    del runner_kwargs['quiet']
    del runner_kwargs['verbose']

    runner = EMRJobRunner(**runner_kwargs)
    emr_job_flow_id = runner.make_persistent_job_flow()
    print emr_job_flow_id


def make_option_parser():
    usage = '%prog [options]'
    description = (
        'Create a persistent EMR job flow to run jobs in. WARNING: do not run'
        ' this without mrjob.tools.emr.terminate.idle_job_flows in your'
        ' crontab; job flows left idle can quickly become expensive!')
    option_parser = OptionParser(usage=usage, description=description)

    def make_option_group(halp):
        g = OptionGroup(option_parser, halp)
        option_parser.add_option_group(g)
        return g

    runner_group = make_option_group('Running the entire job')
    hadoop_emr_opt_group = make_option_group(
        'Running on Hadoop or EMR (these apply when you set -r hadoop or -r'
        ' emr)')
    emr_opt_group = make_option_group(
        'Running on Amazon Elastic MapReduce (these apply when you set -r'
        ' emr)')

    assignments = {
        runner_group: (
            'bootstrap_mrjob',
            'conf_path',
            'quiet',
            'verbose'
        ),
        hadoop_emr_opt_group: (
            'label',
            'owner',
        ),
        emr_opt_group: (
            'additional_emr_info',
            'aws_availability_zone',
            'aws_region',
            'bootstrap_actions',
            'bootstrap_cmds',
            'bootstrap_files',
            'bootstrap_python_packages',
            'ec2_instance_type',
            'ec2_key_pair',
            'ec2_master_instance_type',
            'ec2_slave_instance_type',
            'emr_endpoint',
            'emr_job_flow_pool_name',
            'enable_emr_debugging',
            'hadoop_version',
            'num_ec2_instances',
            'pool_emr_job_flows',
            's3_endpoint',
            's3_log_uri',
            's3_scratch_uri',
            's3_sync_wait_time',
        ),
    }

    # Scrape options from MRJob and index them by dest
    mr_job = MRJob()
    job_option_groups = mr_job.all_option_groups()
    scrape_options_into_new_groups(job_option_groups, assignments)
    return option_parser


if __name__ == '__main__':
    main()
