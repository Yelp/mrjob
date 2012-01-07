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
"""Run a command on the master and all slaves. Store stdout and stderr for
results in OUTPUT_DIR.

Usage::

    python -m mrjob.tools.emr.mrboss JOB_FLOW_ID [options] "command string"

Options::

  -c CONF_PATH, --conf-path=CONF_PATH
  --ec2-key-pair-file=EC2_KEY_PAIR_FILE
                        Path to file containing SSH key for EMR
  -h, --help            show this help message and exit
  --no-conf             Don't load mrjob.conf even if it's available
  -o, --output-dir      Specify an output directory (default: JOB_FLOW_ID)
  -q, --quiet           Don't print anything to stderr
  -v, --verbose         print more messages to stderr
"""
from __future__ import with_statement

from optparse import OptionParser
import os
import shlex
import sys

from mrjob.emr import EMRJobRunner
from mrjob.job import MRJob
from mrjob.ssh import ssh_run_with_recursion
from mrjob.util import scrape_options_into_new_groups


def main():
    usage = 'usage: %prog JOB_FLOW_ID OUTPUT_DIR [options] "command string"'
    description = ('Run a command on the master and all slaves of an EMR job'
                   ' flow. Store stdout and stderr for results in OUTPUT_DIR.')

    option_parser = OptionParser(usage=usage, description=description)

    assignments = {
        option_parser: ('conf_path', 'quiet', 'verbose',
                        'ec2_key_pair_file')
    }

    option_parser.add_option('-o', '--output-dir', dest='output_dir',
                             default=None,
                             help="Specify an output directory (default:"
                             " JOB_FLOW_ID)")

    mr_job = MRJob()
    scrape_options_into_new_groups(mr_job.all_option_groups(), assignments)

    options, args = option_parser.parse_args()

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)

    runner_kwargs = options.__dict__.copy()
    for unused_arg in ('output_dir', 'quiet', 'verbose'):
        del runner_kwargs[unused_arg]

    if len(args) < 2:
        option_parser.print_help()
        sys.exit(1)

    job_flow_id, cmd_string = args[:2]
    cmd_args = shlex.split(cmd_string)

    output_dir = os.path.abspath(options.output_dir or job_flow_id)

    with EMRJobRunner(emr_job_flow_id=job_flow_id, **runner_kwargs) as runner:
        runner._enable_slave_ssh_access()
        run_on_all_nodes(runner, output_dir, cmd_args)


def run_on_all_nodes(runner, output_dir, cmd_args, print_stderr=True):
    """Given an :py:class:`EMRJobRunner`, run the command specified by
    *cmd_args* on all nodes in the job flow and save the stdout and stderr of
    each run to subdirectories of *output_dir*.

    You should probably have run :py:meth:`_enable_slave_ssh_access()` on the
    runner before calling this function.
    """

    master_addr = runner._address_of_master()
    addresses = [master_addr]
    if runner._opts['num_ec2_instances'] > 1:
        addresses += ['%s!%s' % (master_addr, slave_addr)
                      for slave_addr in runner._addresses_of_slaves()]

    for addr in addresses:
        stdout, stderr = ssh_run_with_recursion(
            runner._opts['ssh_bin'],
            addr,
            runner._opts['ec2_key_pair_file'],
            runner._ssh_key_name,
            cmd_args,
        )

        if print_stderr:
            print '---'
            print 'Command completed on %s.' % addr
            print stderr,

        if '!' in addr:
            base_dir = os.path.join(output_dir, 'slave ' + addr.split('!')[1])
        else:
            base_dir = os.path.join(output_dir, 'master')

        if not os.path.exists(base_dir):
            os.makedirs(base_dir)

        with open(os.path.join(base_dir, 'stdout'), 'w') as f:
            f.write(stdout)

        with open(os.path.join(base_dir, 'stderr'), 'w') as f:
            f.write(stderr)


if __name__ == '__main__':
    main()
