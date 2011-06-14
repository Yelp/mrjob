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

import logging
import subprocess

from mrjob.logfetch import LogFetcher, LogFetchException


log = logging.getLogger('mrjob.fetch_ssh')


class SSHLogFetcher(LogFetcher):

    def __init__(self, emr_conn, jobflow_id, 
                 ec2_key_pair_file='/nail/etc/EMR.pem.dev',
                 local_temp_dir='/tmp'):
        super(SSHLogFetcher, self).__init__(local_temp_dir=local_temp_dir)
        self.emr_conn = emr_conn
        self.jobflow_id = jobflow_id
        self.ec2_key_pair_file = ec2_key_pair_file
        self.root_path = '/mnt/var/log/hadoop/'
        self.address = None

    def _address_of_master(self):
        # cache address of master to avoid redundant calls to describe_jobflow
        if self.address:
            return self.address

        jobflow = self.emr_conn.describe_jobflow(self.jobflow_id)
        if jobflow.state not in ('WAITING', 'RUNNING'):
            raise LogFetchException('Cannot ssh to master; job flow is not waiting or running')

        self.address = jobflow.masterpublicdnsname
        return self.address

    def ls(self, path='*'):
        args = [
            'ssh', '-q',
            '-i', self.ec2_key_pair_file,
            'hadoop@%s' % self._address_of_master(),
            'find', self.root_path,
        ]
        print ' '.join(args)
        p = subprocess.Popen(args, 
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        output, err = p.communicate()
        if err:
            log.error(str(err))
            raise LogFetchException('ssh error: %s' % str(err))
        else:
            return output.split('\n')

    def get(self, path):
        return None

if __name__ == '__main__':
    emr_conn = EMRJobRunner().make_emr_conn()
    addr = address_of_master(emr_conn, sys.argv[1])
    print '\n'.join(ls(addr, '/*'))
