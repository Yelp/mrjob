# Copyright 2009-2011 Yelp and Contributors
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
"""Inspect available job flow pools or identify job flows suitable for
running a job with the specified options.

Usage::

    python -m mrjob.tools.emr.job_flow_pool
"""
from __future__ import with_statement

from optparse import OptionParser
from optparse import OptionGroup

from mrjob.emr import EMRJobRunner
from mrjob.emr import est_time_to_hour
from mrjob.job import MRJob
from mrjob.util import scrape_options_into_new_groups
from mrjob.util import log_to_stream


def get_pools(emr_conn):
    pools = {}
    for job_flow in emr_conn.describe_jobflows():
        if job_flow.state in ('TERMINATED', 'FAILED', 'COMPLETED',
                              'SHUTTING_DOWN'):
            continue
        if not job_flow.bootstrapactions:
            continue
        args = [arg.value for arg in job_flow.bootstrapactions[-1].args]
        if len(args) != 2:
            continue
        pools.setdefault(args[1], list()).append(job_flow)

    return pools


def pprint_job_flow(jf):
    """Print a job flow to stdout in this form::

        job.flow.name
        j-JOB_FLOW_ID: 2 instances (master=m1.small, slaves=m1.small, 20 \
minutes to the hour)
    """
    instance_count = int(jf.instancecount)

    nosep_segments = [
        '%d instance' % instance_count,
    ]
    if instance_count > 1:
        nosep_segments.append('s')

    comma_segments = [
        'master=%s' % jf.masterinstancetype,
    ]

    if instance_count > 1:
        comma_segments.append('slaves=%s' % jf.slaveinstancetype)

    comma_segments.append('%0.0f minutes to the hour' % est_time_to_hour(jf))

    nosep_segments += [
        ' (',
        ', '.join(comma_segments),
        ')',
    ]

    print '%s: %s' % (jf.jobflowid, jf.name)
    print ''.join(nosep_segments)
    print jf.state
    print


def pprint_pools(runner):
    pools = get_pools(runner.make_emr_conn())
    for pool_name, job_flows in pools.iteritems():
        print '-' * len(pool_name)
        print pool_name
        print '-' * len(pool_name)
        for job_flow in job_flows:
            pprint_job_flow(job_flow)


def terminate(runner, pool_name):
    emr_conn = runner.make_emr_conn()
    pools = get_pools(emr_conn)
    try:
        for job_flow in pools[pool_name]:
            emr_conn.terminate_jobflow(job_flow.jobflowid)
            print 'terminated %s' % job_flow.jobflowid
    except KeyError:
        print 'No job flows match pool name "%s"' % pool_name


def main():
    usage = '%prog [options]'
    description = (
        'Inspect available job flow pools or identify job flows suitable for'
        ' running a job with the specified options.')
    option_parser = OptionParser(usage=usage, description=description)

    import boto.emr.connection
    boto.emr.connection.JobFlow.Fields.add('HadoopVersion')

    def make_option_group(halp):
        g = OptionGroup(option_parser, halp)
        option_parser.add_option_group(g)
        return g

    ec2_opt_group = make_option_group('EC2 instance configuration')
    hadoop_opt_group = make_option_group('Hadoop configuration')
    job_opt_group = make_option_group('Job flow configuration')

    assignments = {
        option_parser: (
            'conf_path',
            'emr_job_flow_pool_name',
            'quiet',
            'verbose',
        ),
        ec2_opt_group: (
            'aws_availability_zone',
            'ec2_instance_type',
            'ec2_key_pair',
            'ec2_key_pair_file',
            'ec2_master_instance_type',
            'ec2_slave_instance_type',
            'emr_endpoint',
            'num_ec2_instances',
        ),
        hadoop_opt_group: (
            'hadoop_version',
            'label',
            'owner',
        ),
        job_opt_group: (
            'bootstrap_actions',
            'bootstrap_cmds',
            'bootstrap_files',
            'bootstrap_mrjob',
            'bootstrap_python_packages',
        ),
    }

    option_parser.add_option('-a', '--all', action='store_true',
                             default=False, dest='list_all',
                             help=('List all available job flows without'
                                   ' filtering by configuration'))
    option_parser.add_option('-f', '--find', action='store_true',
                             default=False, dest='find',
                             help=('Find a job flow matching the pool name,'
                                   ' bootstrap configuration, and instance'
                                   ' number/type as specified on the command'
                                   ' line and in the configuration files'))
    option_parser.add_option('-t', '--terminate', action='store',
                             default=None, dest='terminate',
                             metavar='JOB_FLOW_ID',
                             help=('Terminate all job flows in the given pool'
                                   ' (defaults to pool "default")'))

    # Scrape options from MRJob and index them by dest
    mr_job = MRJob()
    scrape_options_into_new_groups(mr_job.all_option_groups(), assignments)
    options, args = option_parser.parse_args()

    log_to_stream(name='mrjob', debug=options.verbose)

    runner_kwargs = options.__dict__.copy()
    for non_runner_kwarg in ('quiet', 'verbose', 'list_all', 'find',
                             'terminate'):
        del runner_kwargs[non_runner_kwarg]

    runner = EMRJobRunner(**runner_kwargs)

    if options.list_all:
        pprint_pools(runner)

    if options.find:
        sorted_job_flows = runner.usable_job_flows()

        if sorted_job_flows:
            jf = sorted_job_flows[-1]
            print 'You should use this one:'
            pprint_job_flow(jf)
        else:
            print 'No idle job flows match criteria'

    if options.terminate:
        terminate(runner, options.terminate)


if __name__ == '__main__':
    main()
