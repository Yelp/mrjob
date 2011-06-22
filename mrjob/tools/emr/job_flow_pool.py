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
from __future__ import with_statement

from optparse import OptionParser, OptionGroup

from mrjob.emr import EMRJobRunner, est_time_to_hour
from mrjob.job import MRJob
from mrjob.util import scrape_options_into_new_groups, log_to_stream

def pprint_job_flow(jf):
    """Print a job flow to stdout in this form:
    job.flow.name
    j-JOB_FLOW_ID: 2 instances (master=m1.small, slaves=m1.small, 20 minutes to the hour)
    """
    instance_count = int(jf.instancecount)

    nosep_segments = [
        '%s: %d instance' % (jf.jobflowid, instance_count),
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

    print jf.name
    print ''.join(nosep_segments)
    print


if __name__ == '__main__':
    usage = '%prog [options]'
    description = 'Identify an exising job flow to run jobs in or create a new one if none are available. WARNING: do not run this without mrjob.tools.emr.terminate.idle_job_flows in your crontab; job flows left idle can quickly become expensive!'
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
        option_parser: ('conf_path', 'quiet', 'verbose'),
        ec2_opt_group: ('ec2_instance_type', 'ec2_master_instance_type',
                        'ec2_slave_instance_type', 'num_ec2_instances',
                        'aws_availability_zone', 
                        'ec2_key_pair', 'ec2_key_pair_file',
                        'emr_endpoint',),
        hadoop_opt_group: ('hadoop_version', 'label', 'owner'),
        job_opt_group: ('bootstrap_mrjob', 'bootstrap_cmds',
                        'bootstrap_files', 'bootstrap_python_packages',),
    }

    # Scrape options from MRJob and index them by dest
    mr_job = MRJob()
    scrape_options_into_new_groups(mr_job.all_option_groups(), assignments)
    options, args = option_parser.parse_args()

    log_to_stream(name='mrjob', debug=options.verbose)

    runner_kwargs = options.__dict__
    del runner_kwargs['quiet']
    del runner_kwargs['verbose']

    runner = EMRJobRunner(**options.__dict__)
    sorted_tagged_job_flows = runner.usable_job_flows()

    if sorted_tagged_job_flows:
        time_to_hour, jf = sorted_tagged_job_flows[-1]
        print 'You should use this one:'
        pprint_job_flow(jf)
    else:
        print 'No idle job flows match criteria'
