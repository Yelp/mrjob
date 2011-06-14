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
from __future__ import with_statement

import sys

from mrjob.logfetch import LogFetchException
from mrjob.logfetch.s3 import S3LogFetcher
from mrjob.logfetch.ssh import SSHLogFetcher
from mrjob.emr import EMRJobRunner


# Log file locations are like so:
#    S3 location             Local location
#    /daemons                / (root)
#    /jobs                   /history
#    /node                   <not present>
#    /steps                  /steps
#    /task-attempts          /userlogs


def ssh_fetcher(jobflow_id):
    dummy_runner = EMRJobRunner()
    emr_conn = dummy_runner.make_emr_conn()
    ec2_key_pair_file = dummy_runner._opts['ec2_key_pair_file']
    return SSHLogFetcher(emr_conn, jobflow_id,
                         ec2_key_pair_file=ec2_key_pair_file)


def s3_fetcher(jobflow_id):
    dummy_runner = EMRJobRunner()
    emr_conn = dummy_runner.make_emr_conn()

    jobflow = emr_conn.describe_jobflow(jobflow_id)
    log_uri = getattr(jobflow, 'loguri', '')
    tweaked_log_uri = log_uri.replace('s3n://', 's3://')
    root_path = '%s%s/' % (tweaked_log_uri, jobflow_id)

    s3_conn = dummy_runner.make_s3_conn()
    return S3LogFetcher(s3_conn, root_path)


def cat_files(fetcher, paths):
    for remote_path in paths:
        print '=== %s ===' % remote_path
        local_path = fetcher.get(remote_path)
        # Sometimes get() will return None
        if local_path:
            with open(local_path, 'r') as f:
                pass
                # print f.read()


def fetchlogs(jobflow_id, path):
    try:
        fetcher = ssh_fetcher(jobflow_id)
        task_attempts, steps, jobs = fetcher.list_logs()
        print 'Task attempts:'
        cat_files(fetcher, task_attempts)
        print
        print 'Steps:'
        cat_files(fetcher, steps)
        print
        print 'Jobs:'
        cat_files(fetcher, jobs)

    except LogFetchException:
        fetcher = s3_fetcher(jobflow_id)
        task_attempts, step_logs, job_logs = fetcher.list_logs()
        print 'Task attempts:'
        cat_files(fetcher, task_attempts)
        print
        print 'Steps:'
        cat_files(fetcher, steps)
        print
        print 'Jobs:'
        cat_files(fetcher, jobs)


def main():
    if len(sys.argv) < 3:
        sys.argv.append('')
    fetchlogs(sys.argv[1], sys.argv[2])


if __name__ == '__main__':
    main()
