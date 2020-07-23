# Copyright 2009-2016 Yelp and Contributors
# Copyright 2017 Yelp
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
import os.path
import shutil

import mrjob.fs.ssh
from mrjob.fs.ssh import SSHFilesystem

from tests.compress import gzip_compress
from tests.fs import MockSubprocessTestCase
from tests.mockssh import main as mock_ssh_main


class SSHFSTestCase(MockSubprocessTestCase):

    def setUp(self):
        super(SSHFSTestCase, self).setUp()
        self.ec2_key_pair_file = self.makefile('key.pem', 'i am an ssh key')
        self.fs = SSHFilesystem(['ssh'], self.ec2_key_pair_file)
        self.set_up_mock_ssh()
        self.mock_popen(mrjob.fs.ssh, mock_ssh_main, self.env)

    def set_up_mock_ssh(self):
        self.main_ssh_root = self.makedirs('testmain')
        self.env = dict(
            MOCK_SSH_VERIFY_KEY_FILE='true',
            MOCK_SSH_ROOTS='testmain=%s' % self.main_ssh_root,
        )
        self.ssh_worker_roots = []

        self.addCleanup(self.teardown_ssh, self.main_ssh_root)

    def teardown_ssh(self, main_ssh_root):
        shutil.rmtree(main_ssh_root)
        for path in self.ssh_worker_roots:
            shutil.rmtree(path)

    def add_worker(self):
        worker_num = len(self.ssh_worker_roots) + 1
        new_dir = self.makedirs('testworker%d' % worker_num)
        self.ssh_worker_roots.append(new_dir)
        self.env['MOCK_SSH_ROOTS'] += (':testmain!testworker%d=%s'
                                       % (worker_num, new_dir))

    def require_sudo(self):
        self.env['MOCK_SSH_REQUIRES_SUDO'] = '1'

    def make_main_file(self, path, contents):
        return self.makefile(os.path.join(self.main_ssh_root, path),
                             contents)

    def make_worker_file(self, worker_num, path, contents):
        return self.makefile(os.path.join('testworker%d' % worker_num, path),
                             contents)

    def test_ls_empty(self):
        self.assertEqual(list(self.fs.ls('ssh://testmain/')), [])

    def test_ls_basic(self):
        self.make_main_file('f', 'contents')
        self.assertEqual(list(self.fs.ls('ssh://testmain/')),
                         ['ssh://testmain/f'])

    def test_ls_basic_2(self):
        self.make_main_file('f', 'contents')
        self.make_main_file('f2', 'contents')
        self.assertEqual(sorted(self.fs.ls('ssh://testmain/')),
                         ['ssh://testmain/f', 'ssh://testmain/f2'])

    def test_ls_recurse(self):
        self.make_main_file('f', 'contents')
        self.make_main_file('d/f2', 'contents')
        self.assertEqual(sorted(self.fs.ls('ssh://testmain/')),
                         ['ssh://testmain/d/f2', 'ssh://testmain/f'])

    def test_ls_without_required_sudo(self):
        self.make_main_file('f', 'contents')
        self.require_sudo()

        self.assertRaises(IOError, list, self.fs.ls('ssh://testmain/'))

    def test_ls_with_required_sudo(self):
        self.make_main_file('f', 'contents')
        self.require_sudo()

        self.fs.use_sudo_over_ssh()

        self.assertEqual(list(self.fs.ls('ssh://testmain/')),
                         ['ssh://testmain/f'])

    def test_worker_ls(self):
        self.add_worker()
        self.make_worker_file(1, 'f', 'foo\nfoo\n')
        remote_path = 'ssh://testmain!testworker1/'

        self.assertEqual(list(self.fs.ls(remote_path)),
                         ['ssh://testmain!testworker1/f'])

    def test_worker_ls_without_required_sudo(self):
        self.add_worker()
        self.make_worker_file(1, 'f', 'foo\nfoo\n')
        remote_path = 'ssh://testmain!testworker1/'

        self.require_sudo()

        self.assertRaises(IOError, list, self.fs.ls(remote_path))

    def test_worker_ls_with_required_sudo(self):
        self.add_worker()
        self.make_worker_file(1, 'f', 'foo\nfoo\n')
        remote_path = 'ssh://testmain!testworker1/'

        self.require_sudo()

        self.fs.use_sudo_over_ssh()

        self.assertEqual(list(self.fs.ls(remote_path)),
                         ['ssh://testmain!testworker1/f'])

    def test_cat_uncompressed(self):
        self.make_main_file(os.path.join('data', 'foo'), 'foo\nfoo\n')
        remote_path = self.fs.join('ssh://testmain/data', 'foo')

        self.assertEqual(b''.join(self.fs._cat_file(remote_path)),
                         b'foo\nfoo\n')

    def test_cat_bz2(self):
        self.make_main_file(os.path.join('data', 'foo.bz2'),
                              bz2.compress(b'foo\n' * 1000))
        remote_path = self.fs.join('ssh://testmain/data', 'foo.bz2')

        self.assertEqual(b''.join(self.fs._cat_file(remote_path)),
                         b'foo\n' * 1000)

    def test_cat_gz(self):
        self.make_main_file(os.path.join('data', 'foo.gz'),
                              gzip_compress(b'foo\n' * 10000))
        remote_path = self.fs.join('ssh://testmain/data', 'foo.gz')

        self.assertEqual(b''.join(self.fs._cat_file(remote_path)),
                         b'foo\n' * 10000)

    def test_cat_without_required_sudo(self):
        self.make_main_file(os.path.join('data', 'foo'), 'foo\nfoo\n')
        remote_path = self.fs.join('ssh://testmain/data', 'foo')

        self.require_sudo()

        self.assertRaises(IOError, list, self.fs._cat_file(remote_path))

    def test_cat_with_required_sudo(self):
        self.make_main_file(os.path.join('data', 'foo'), 'foo\nfoo\n')
        remote_path = self.fs.join('ssh://testmain/data', 'foo')

        self.require_sudo()

        self.fs.use_sudo_over_ssh()

        self.assertEqual(b''.join(self.fs._cat_file(remote_path)),
                         b'foo\nfoo\n')

    def test_worker_cat(self):
        self.add_worker()
        self.make_worker_file(1, 'f', 'foo\nfoo\n')
        remote_path = 'ssh://testmain!testworker1/f'

        self.assertEqual(b''.join(self.fs._cat_file(remote_path)),
                         b'foo\nfoo\n')

    def test_worker_cat_without_required_sudo(self):
        self.add_worker()
        self.make_worker_file(1, 'f', 'foo\nfoo\n')
        remote_path = 'ssh://testmain!testworker1/f'
        self.require_sudo()

        self.assertRaises(IOError, list, self.fs._cat_file(remote_path))

    def test_worker_cat_with_required_sudo(self):
        self.add_worker()
        self.make_worker_file(1, 'f', 'foo\nfoo\n')
        remote_path = 'ssh://testmain!testworker1/f'
        self.require_sudo()

        self.fs.use_sudo_over_ssh()

        self.assertEqual(b''.join(self.fs._cat_file(remote_path)),
                         b'foo\nfoo\n')

    def test_du(self):
        self.make_main_file('f', 'contents')
        # not implemented
        self.assertRaises(IOError, self.fs.du, 'ssh://testmain/f')

    def test_mkdir(self):
        # not implemented
        self.assertRaises(IOError, self.fs.mkdir, 'ssh://testmain/d')

    def test_exists_no(self):
        path = 'ssh://testmain/f'
        self.assertEqual(self.fs.exists(path), False)

    def test_exists_yes(self):
        self.make_main_file('f', 'contents')
        path = 'ssh://testmain/f'
        self.assertEqual(self.fs.exists(path), True)

    def test_exists_without_required_sudo(self):
        # apparently we just swallow IOErrors over SSH? See #1388
        self.make_main_file('f', 'contents')
        path = 'ssh://testmain/f'

        self.require_sudo()

        self.assertEqual(self.fs.exists(path), False)

    def test_exists_with_required_sudo(self):
        self.make_main_file('f', 'contents')
        path = 'ssh://testmain/f'

        self.require_sudo()

        self.fs.use_sudo_over_ssh()

        self.assertEqual(self.fs.exists(path), True)

    def test_rm(self):
        self.make_main_file('f', 'contents')
        # not implemented
        self.assertRaises(IOError, self.fs.rm, 'ssh://testmain/f')

    def test_touchz(self):
        # not implemented
        self.assertRaises(IOError, self.fs.touchz, 'ssh://testmain/d')

    def test_md5sum(self):
        # not implemented
        self.assertRaises(IOError, self.fs.md5sum, 'ssh://testmain/d')
