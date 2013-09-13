# Copyright 2012 Yelp
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

from sys import argv
from sys import stderr


commands = {}

descriptions = {}

usage = """usage: mrjob {subcommand|--help}"

subcommands:"""


def error(msg=None):
    if msg:
        print >> stderr, msg

    longest_command = max(len(name) for name in commands)

    def subcommand_line(name):
        spaces = ' ' * (longest_command - len(name))
        return '  %s: %s%s' % (
            name, spaces, descriptions[name])
    print >> stderr, usage
    print >> stderr, '\n'.join(
        subcommand_line(name) for name in sorted(commands))


def command(name, description):
    def decorator(f):
        commands[name] = f
        descriptions[name] = description
        return f
    return decorator


def main(args=None):
    args = args or argv
    if not args[1:] or args[1] in ('-h', '--help'):
        error()
    elif args[1] not in commands:
        error('"%s" is not a command' % args[1])
    else:
        commands[args[1]](args[2:])


@command('run', 'Run a job')
def run(args):
    from mrjob.launch import MRJobLauncher
    MRJobLauncher(args=args, from_cl=True).run_job()


@command('audit-emr-usage', 'Audit EMR usage')
def audit_usage(args):
    from mrjob.tools.emr.audit_usage import main
    main(args)


@command('create-job-flow', 'Create an EMR job flow')
def create_jf(args):
    from mrjob.tools.emr.create_job_flow import main
    main(args)


@command('fetch-logs', 'Fetch and parse EMR logs for errors and counters')
def fetch_logs(args):
    from mrjob.tools.emr.fetch_logs import main
    main(args)


@command('report-long-jobs', 'Report EMR jobs which have been running for a'
         ' long time')
def report_long_jobs(args):
    from mrjob.tools.emr.report_long_jobs import main
    main(args)


@command('s3-tmpwatch', 'Delete S3 keys older than a specified time')
def s3_tmpwatch(args):
    from mrjob.tools.emr.s3_tmpwatch import main
    main(args)


@command('terminate-idle-job-flows', 'Terminate idle EMR job flows')
def terminate_idle_jfs(args):
    from mrjob.tools.emr.terminate_idle_job_flows import main
    main(args)


@command('terminate-job-flow', 'Terminate a single EMR job flow')
def terminate_jf(args):
    from mrjob.tools.emr.terminate_job_flow import main
    main(args)


if __name__ == '__main__':
    main()
