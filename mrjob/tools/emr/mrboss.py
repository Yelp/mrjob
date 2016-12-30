# Copyright 2009-2012 Yelp
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
"""Run a command on every node of a cluster. Store stdout and stderr for
results in OUTPUT_DIR.

Usage::

    mrjob boss CLUSTER_ID [options] "command string"

Options::

  -h, --help            show this help message and exit
  -c CONF_PATHS, --conf-path=CONF_PATHS
                        Path to alternate mrjob.conf file to read from
  --no-conf             Don't load mrjob.conf even if it's available
  --ec2-key-pair-file=EC2_KEY_PAIR_FILE
                        Path to file containing SSH key for EMR
  --emr-endpoint=EMR_ENDPOINT
                        Force mrjob to connect to EMR on this endpoint (e.g.
                        us-west-1.elasticmapreduce.amazonaws.com). Default is
                        to infer this from region.
  -o OUTPUT_DIR, --output-dir=OUTPUT_DIR
                        Specify an output directory (default: CLUSTER_ID)
  -q, --quiet           Don't print anything to stderr
  --region=REGION       GCE/AWS region to run Dataproc/EMR jobs in.
  --aws-region=REGION   Deprecated alias for --region
  --s3-endpoint=S3_ENDPOINT
                        Force mrjob to connect to S3 on this endpoint (e.g. s3
                        -us-west-1.amazonaws.com). You usually shouldn't set
                        this; by default mrjob will choose the correct
                        endpoint for each S3 bucket based on its location.
  --ssh-bin=SSH_BIN     Name/path of ssh binary. Arguments are allowed (e.g.
                        --ssh-bin 'ssh -v')
  -v, --verbose         print more messages to stderr
"""
from __future__ import print_function

from optparse import OptionParser
import os
import sys

from mrjob.emr import EMRJobRunner
from mrjob.job import MRJob
from mrjob.options import _add_basic_options
from mrjob.options import _add_runner_options
from mrjob.options import _alphabetize_options
from mrjob.options import _pick_runner_opts
from mrjob.py2 import to_string
from mrjob.ssh import _ssh_copy_key
from mrjob.ssh import _ssh_run_with_recursion
from mrjob.util import random_identifier
from mrjob.util import shlex_split


def main(cl_args=None):
    usage = 'usage: %prog CLUSTER_ID [options] "command string"'
    description = ('Run a command on the master and all slaves of an EMR'
                   ' cluster. Store stdout/stderr for results in OUTPUT_DIR.')

    option_parser = OptionParser(usage=usage, description=description)
    option_parser.add_option('-o', '--output-dir', dest='output_dir',
                             default=None,
                             help="Specify an output directory (default:"
                             " CLUSTER_ID)")
    _add_basic_options(option_parser)
    _add_runner_options(
        option_parser,
        _pick_runner_opts('emr', 'connect') | set(
            ['ssh_bin', 'ec2_key_pair_file'])
    )

    _alphabetize_options(option_parser)

    options, args = option_parser.parse_args(cl_args)

    MRJob.set_up_logging(quiet=options.quiet, verbose=options.verbose)

    runner_kwargs = options.__dict__.copy()
    for unused_arg in ('output_dir', 'quiet', 'verbose'):
        del runner_kwargs[unused_arg]

    if len(args) < 2:
        option_parser.print_help()
        sys.exit(1)

    cluster_id, cmd_string = args[:2]
    cmd_args = shlex_split(cmd_string)

    output_dir = os.path.abspath(options.output_dir or cluster_id)

    with EMRJobRunner(cluster_id=cluster_id, **runner_kwargs) as runner:
        _run_on_all_nodes(runner, output_dir, cmd_args)


def _run_on_all_nodes(runner, output_dir, cmd_args, print_stderr=True):
    """Given an :py:class:`EMRJobRunner`, run the command specified by
    *cmd_args* on all nodes in the cluster and save the stdout and stderr of
    each run to subdirectories of *output_dir*.

    You should probably have run :py:meth:`_enable_slave_ssh_access()` on the
    runner before calling this function.
    """
    master_addr = runner._address_of_master()
    addresses = [master_addr]

    ssh_bin = runner._opts['ssh_bin']
    ec2_key_pair_file = runner._opts['ec2_key_pair_file']

    keyfile = None
    slave_addrs = runner._ssh_worker_hosts()

    if slave_addrs:
        addresses += ['%s!%s' % (master_addr, slave_addr)
                      for slave_addr in slave_addrs]
        # copying key file like a boss (name of keyfile doesn't really matter)
        keyfile = 'mrboss-%s.pem' % random_identifier()
        _ssh_copy_key(ssh_bin, master_addr, ec2_key_pair_file, keyfile)

    for addr in addresses:

        stdout, stderr = _ssh_run_with_recursion(
            ssh_bin,
            addr,
            ec2_key_pair_file,
            keyfile,
            cmd_args,
        )

        if print_stderr:
            print('---')
            print('Command completed on %s.' % addr)
            print(to_string(stderr), end=' ')

        if '!' in addr:
            base_dir = os.path.join(output_dir, 'slave ' + addr.split('!')[1])
        else:
            base_dir = os.path.join(output_dir, 'master')

        if not os.path.exists(base_dir):
            os.makedirs(base_dir)

        with open(os.path.join(base_dir, 'stdout'), 'wb') as f:
            f.write(stdout)

        with open(os.path.join(base_dir, 'stderr'), 'wb') as f:
            f.write(stderr)


if __name__ == '__main__':
    main()
