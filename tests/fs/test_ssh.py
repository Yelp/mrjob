# Copyright 2009-2013 Yelp and Contributors
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
import bz2
import os

from mrjob.fs.ssh import SSHFilesystem
from mrjob import ssh

from tests.compress import gzip_compress
from tests.fs import MockSubprocessTestCase
from tests.mockssh import main as mock_ssh_main


class SSHFSTestCase(MockSubprocessTestCase):

    def setUp(self):
        super(SSHFSTestCase, self).setUp()
        self.ec2_key_pair_file = self.makefile('key.pem', 'i am an ssh key')
        self.ssh_key_name = 'key_name.pem'
        self.fs = SSHFilesystem(['ssh'], self.ec2_key_pair_file,
                                self.ssh_key_name)
        self.set_up_mock_ssh()
        self.mock_popen(ssh, mock_ssh_main, self.env)

    def set_up_mock_ssh(self):
        self.master_ssh_root = self.makedirs('testmaster')
        self.env = dict(
            MOCK_SSH_VERIFY_KEY_FILE='true',
            MOCK_SSH_ROOTS='testmaster=%s' % self.master_ssh_root,
        )
        self.ssh_slave_roots = []

    def add_slave(self):
        slave_num = len(self.ssh_slave_roots) + 1
        new_dir = self.makedirs('testslave%d' % slave_num)
        self.ssh_slave_roots.append(new_dir)
        self.env['MOCK_SSH_ROOTS'] += (':testmaster!testslave%d=%s'
                                       % (slave_num, new_dir))

    def make_master_file(self, path, contents):
        return self.makefile(os.path.join(self.master_ssh_root, path),
                             contents)

    def make_slave_file(self, slave_num, path, contents):
        return self.makefile(os.path.join('testslave%d' % slave_num, path),
                             contents)

    def test_ls_empty(self):
        self.assertEqual(list(self.fs.ls('ssh://testmaster/')), [])

    def test_ls_basic(self):
        self.make_master_file('f', 'contents')
        self.assertEqual(list(self.fs.ls('ssh://testmaster/')),
                         ['ssh://testmaster/f'])

    def test_ls_basic_2(self):
        self.make_master_file('f', 'contents')
        self.make_master_file('f2', 'contents')
        self.assertItemsEqual(list(self.fs.ls('ssh://testmaster/')),
                              ['ssh://testmaster/f', 'ssh://testmaster/f2'])

    def test_ls_recurse(self):
        self.make_master_file('f', 'contents')
        self.make_master_file('d/f2', 'contents')
        self.assertItemsEqual(list(self.fs.ls('ssh://testmaster/')),
                              ['ssh://testmaster/f', 'ssh://testmaster/d/f2'])

    def test_cat_uncompressed(self):
        self.make_master_file(os.path.join('data', 'foo'), 'foo\nfoo\n')
        remote_path = self.fs.path_join('ssh://testmaster/data', 'foo')

        self.assertEqual(list(self.fs._cat_file(remote_path)),
                         ['foo\n', 'foo\n'])

    def test_cat_bz2(self):
        self.make_master_file(os.path.join('data', 'foo.bz2'),
                              bz2.compress('foo\n' * 1000))
        remote_path = self.fs.path_join('ssh://testmaster/data', 'foo.bz2')

        self.assertEqual(list(self.fs._cat_file(remote_path)),
                         ['foo\n'] * 1000)

    def test_cat_gz(self):
        self.make_master_file(os.path.join('data', 'foo.gz'),
                              gzip_compress('foo\n' * 10000))
        remote_path = self.fs.path_join('ssh://testmaster/data', 'foo.gz')

        self.assertEqual(list(self.fs._cat_file(remote_path)),
                         ['foo\n'] * 10000)

    def test_slave_cat(self):
        self.add_slave()
        self.make_slave_file(1, 'f', 'foo\nfoo\n')
        remote_path = 'ssh://testmaster!testslave1/f'

        # it is not SSHFilesystem's responsibility to copy the key.
        self.assertRaises(IOError, self.fs._cat_file, remote_path)

        self.make_master_file(self.ssh_key_name, 'key')
        self.assertEqual(list(self.fs._cat_file(remote_path)),
                         ['foo\n', 'foo\n'])

    def test_slave_ls(self):
        self.add_slave()
        self.make_slave_file(1, 'f', 'foo\nfoo\n')
        remote_path = 'ssh://testmaster!testslave1/'

        self.assertRaises(IOError, list, self.fs.ls(remote_path))

        # it is not SSHFilesystem's responsibility to copy the key.
        self.make_master_file(self.ssh_key_name, 'key')
        self.assertEqual(list(self.fs.ls(remote_path)),
                         ['ssh://testmaster!testslave1/f'])

    def test_du(self):
        self.make_master_file('f', 'contents')
        # not implemented
        self.assertRaises(IOError, self.fs.du, 'ssh://testmaster/f')

    def test_mkdir(self):
        # not implemented
        self.assertRaises(IOError, self.fs.mkdir, 'ssh://testmaster/d')

    def test_path_exists_no(self):
        path = 'ssh://testmaster/f'
        self.assertEqual(self.fs.path_exists(path), False)

    def test_path_exists_yes(self):
        self.make_master_file('f', 'contents')
        path = 'ssh://testmaster/f'
        self.assertEqual(self.fs.path_exists(path), True)

    def test_rm(self):
        self.make_master_file('f', 'contents')
        # not implemented
        self.assertRaises(IOError, self.fs.rm, 'ssh://testmaster/f')

    def test_touchz(self):
        # not implemented
        self.assertRaises(IOError, self.fs.touchz, 'ssh://testmaster/d')

    def test_md5sum(self):
        # not implemented
        self.assertRaises(IOError, self.fs.md5sum, 'ssh://testmaster/d')
