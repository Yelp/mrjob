"""Create a persistent EMR job flow, using bootstrap scripts and other
configs from :py:mod:`mrjob.conf`, and print the job flow ID to stdout.

Usage::

    python -m mrjob.emr.tools.create_job_flow

**WARNING**: do not run this without having :py:mod:`mrjob.emr.tools.terminate.idle_job_flows` in your crontab; job flows left idle can quickly become expensive!
"""


from optparse import OptionParser
import sys

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
        'num_ec2_instances': options.num_ec2_instances,
    }
    runner = EMRJobRunner(**runner_kwargs)
    emr_job_flow_id = runner.make_persistent_job_flow()
    print(emr_job_flow_id)

def make_option_parser():
    usage = '%prog [options]'
    description = 'Create a persistent EMR job flow to run jobs in. WARNING: do not run this without mrjob.emr.tools.terminate.idle_job_flows in your crontab; job flows left idle can quickly become expensive!'
    option_parser = OptionParser(usage=usage, description=description)
    option_parser.add_option(
            '-v', '--verbose', dest='verbose', default=False,
            action='store_true',
            help='print more messages to stderr')
    option_parser.add_option(
            '-q', '--quiet', dest='quiet', default=False,
            action='store_true',
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

    return option_parser

if __name__ == '__main__':
    main()
