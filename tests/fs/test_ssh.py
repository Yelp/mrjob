# Copyright 2009-2012 Yelp
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

import os

from mrjob.fs.multi import MultiFilesystem
from mrjob.fs.ssh import SSHFilesystem
from mrjob import ssh

from tests.fs import MockSubprocessTestCase
from tests.mockssh import main as mock_ssh_main


class SSHFSTestCase(MockSubprocessTestCase):

    def setUp(self):
        super(SSHFSTestCase, self).setUp()
        self.ec2_key_pair_file = self.makefile('key.pem', 'i am an ssh key')
        self.ssh_key_name = 'key_name'
        # wrap SSHFilesystem so it gets cat()
        self.fs = MultiFilesystem(
            SSHFilesystem(['ssh'], self.ec2_key_pair_file, self.ssh_key_name))
        self.set_up_mock_ssh()
        self.mock_popen(ssh, mock_ssh_main, self.env)

    def set_up_mock_ssh(self):
        self.master_ssh_root = self.makedirs('master_ssh_root')
        self.env = dict(
            MOCK_SSH_VERIFY_KEY_FILE='true',
            MOCK_SSH_ROOTS='testmaster=%s' % self.master_ssh_root,
        )
        self.ssh_slave_roots = []

    def add_slave(self):
        slave_num = len(self.slave_ssh_roots)
        new_dir = self.makedirs('slave_%d_ssh_root' % slave_num)
        self.slave_ssh_roots_append(new_dir)
        self.env['MOCK_SSH_ROOTS'] += (':testmaster!testslave%d=%s'
                                         % (slave_num, new_dir))

    def test_ls_empty(self):
        self.assertEqual(list(self.fs.ls('ssh://testmaster/')), [])

    def test_ls_basic(self):
        self.make_hdfs_file('f', 'contents')
        self.assertEqual(list(self.fs.ls('hdfs:///')), ['hdfs:///f'])

    def test_ls_basic_2(self):
        self.make_hdfs_file('f', 'contents')
        self.make_hdfs_file('f2', 'contents')
        self.assertEqual(list(self.fs.ls('hdfs:///')), ['hdfs:///f',
                                                        'hdfs:///f2'])

    def test_ls_recurse(self):
        self.make_hdfs_file('f', 'contents')
        self.make_hdfs_file('d/f2', 'contents')
        self.assertEqual(list(self.fs.ls('hdfs:///')),
                         ['hdfs:///f', 'hdfs:///d/f2'])

    def test_cat_uncompressed(self):
        # mockhadoop doesn't support compressed files, so we won't test for it.
        # this is only a sanity check anyway.
        self.makefile(os.path.join('mock_hdfs_root', 'data', 'foo'), 'foo\nfoo\n')
        remote_path = self.fs.path_join('hdfs:///data', 'foo')

        self.assertEqual(list(self.fs.cat(remote_path)), ['foo\n', 'foo\n'])

    def test_du(self):
        root = self.hadoop_env['MOCK_HDFS_ROOT']
        self.makefile(os.path.join('mock_hdfs_root', 'data1'), 'abcd')
        remote_data_1 = 'hdfs:///data1'

        remote_dir = self.makedirs('mock_hdfs_root/more')

        self.makefile(os.path.join('mock_hdfs_root', 'more', 'data2'), 'defg')
        remote_data_2 = 'hdfs:///more/data2'

        self.makefile(os.path.join('mock_hdfs_root', 'more', 'data3'), 'hijk')
        remote_data_3 = 'hdfs:///more/data3'

        self.assertEqual(self.fs.du('hdfs:///'), 12)
        self.assertEqual(self.fs.du('hdfs:///data1'), 4)
        self.assertEqual(self.fs.du('hdfs:///more'), 8)
        self.assertEqual(self.fs.du('hdfs:///more/*'), 8)
        self.assertEqual(self.fs.du('hdfs:///more/data2'), 4)
        self.assertEqual(self.fs.du('hdfs:///more/data3'), 4)

    def test_mkdir(self):
        self.fs.mkdir('hdfs:///d')
        local_path = os.path.join(self.root, 'mock_hdfs_root', 'd')
        self.assertEqual(os.path.isdir(local_path), True)

    def test_rm(self):
        local_path = self.make_hdfs_file('f', 'contents')
        self.assertEqual(os.path.exists(local_path), True)
        self.fs.rm('hdfs:///f')
        self.assertEqual(os.path.exists(local_path), False)

    def test_touchz(self):
        # mockhadoop doesn't implement this.
        pass
