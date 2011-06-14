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


def fetch_ssh(jobflow_id, path):
    dummy_runner = EMRJobRunner()
    emr_conn = dummy_runner.make_emr_conn()
    keypath_file = dummy_runner._opts['ec2_key_pair_file']
    fetcher = SSHLogFetcher(emr_conn, jobflow_id,
                            keyfile_path=keypath_file)
    return fetcher.ls(path)


def fetch_s3(jobflow_id, path):
    dummy_runner = EMRJobRunner()
    emr_conn = dummy_runner.make_emr_conn()

    jobflow = emr_conn.describe_jobflow(jobflow_id)
    log_uri = getattr(jobflow, 'loguri', '')
    tweaked_log_uri = log_uri.replace('s3n://', 's3://')
    root_path = '%s%s/' % (tweaked_log_uri, jobflow_id)

    s3_conn = dummy_runner.make_s3_conn()
    fetcher = S3LogFetcher(s3_conn, root_path)
    return fetcher.ls(path)


def fetchlogs(jobflow_id, path):
    try:
        print '\n'.join(fetch_ssh(jobflow_id, path))
    except LogFetchException:
        print '\n'.join(fetch_s3(jobflow_id, path))


def main():
    if len(sys.argv) < 3:
        sys.argv.append('')
    fetchlogs(sys.argv[1], sys.argv[2])


if __name__ == '__main__':
    main()
