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
"""Create a persistent EMR job flow, using bootstrap scripts and other
configs from :py:mod:`mrjob.conf`, and print the job flow ID to stdout.

Usage::

    python -m mrjob.tools.emr.create_job_flow

**WARNING**: do not run this without having :py:mod:`mrjob.tools.emr.terminate.idle_job_flows` in your crontab; job flows left idle can quickly become expensive!
"""
from __future__ import with_statement

from optparse import OptionParser

from mrjob.emr import EMRJobRunner
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
    runner_kwargs = {
        'conf_path': options.conf_path,
        'ec2_instance_type': options.ec2_instance_type,
        'ec2_master_instance_type': options.ec2_master_instance_type,
        'ec2_slave_instance_type': options.ec2_slave_instance_type,
        'label': options.label,
        'num_ec2_instances': options.num_ec2_instances,
        'owner': options.owner,
    }
    runner = EMRJobRunner(**runner_kwargs)
    emr_job_flow_id = runner.make_persistent_job_flow()
    print emr_job_flow_id

def make_option_parser():
    usage = '%prog [options]'
    description = 'Create a persistent EMR job flow to run jobs in. WARNING: do not run this without mrjob.tools.emr.terminate.idle_job_flows in your crontab; job flows left idle can quickly become expensive!'
    option_parser = OptionParser(usage=usage, description=description)
    option_parser.add_option(
        '-v', '--verbose', dest='verbose', default=False, action='store_true',
        help='print more messages to stderr')
    option_parser.add_option(
        '-q', '--quiet', dest='quiet', default=False, action='store_true',
        help='just print job flow ID to stdout')
    option_parser.add_option(
        '-c', '--conf-path', dest='conf_path', default=None,
        help='Path to alternate mrjob.conf file to read from')
    option_parser.add_option(
        '--no-conf', dest='conf_path', action='store_false',
        help="Don't load mrjob.conf even if it's available")
    option_parser.add_option(
        '--ec2-instance-type', dest='ec2_instance_type', default=None,
        help='Type of EC2 instance(s) to launch (e.g. m1.small, c1.xlarge, m2.xlarge). See http://aws.amazon.com/ec2/instance-types/ for the full list.')
    option_parser.add_option(
        '--ec2-master-instance-type', dest='ec2_master_instance_type', default=None,
        help='Type of EC2 instance for master node only')
    option_parser.add_option(
        '--ec2-slave-instance-type', dest='ec2_slave_instance_type', default=None,
        help='Type of EC2 instance for slave nodes only')
    option_parser.add_option(
        '--num-ec2-instances', dest='num_ec2_instances', default=None,
        type='int',
        help='Number of EC2 instances to launch')
    option_parser.add_option(
        '-l', '--label', dest='label',
        default='create_job_flow',
        help='Optional label for this job flow; useful for auditing. default: %default')
    option_parser.add_option(
        '-o', '--owner', dest='owner',
        default=None,
        help='Optional owner for this job; useful for auditing. Default is your username.')

    return option_parser

if __name__ == '__main__':
    main()
