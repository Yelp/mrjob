# Copyright 2011-2012 Yelp
# Copyright 2014 Yelp and Contributors
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
"""Test the mrboss tool"""
import os
import shutil
import tempfile

from mrjob.emr import EMRJobRunner
from mrjob.tools.emr.mrboss import _run_on_all_nodes
from tests.mockssh import mock_ssh_file
from tests.mockboto import MockBotoTestCase
from tests.py2 import patch
from tests.test_emr import BUCKET_URI
from tests.test_emr import LOG_DIR


class MRBossTestCase(MockBotoTestCase):

    def setUp(self):
        super(MRBossTestCase, self).setUp()

        self.ssh_worker_hosts = self.start(patch(
            'mrjob.emr.EMRJobRunner._ssh_worker_hosts',
            return_value=[]))

        self.make_runner()

    def tearDown(self):
        self.cleanup_runner()
        super(MRBossTestCase, self).tearDown()

    def make_runner(self):
        self.runner = EMRJobRunner(conf_paths=[])
        self.add_mock_s3_data({'walrus': {}})
        self.runner = EMRJobRunner(cloud_fs_sync_secs=0,
                                   cloud_tmp_dir='s3://walrus/tmp',
                                   conf_paths=[])
        self.runner._s3_log_dir_uri = BUCKET_URI + LOG_DIR
        self.prepare_runner_for_ssh(self.runner)
        self.output_dir = tempfile.mkdtemp(prefix='mrboss_wd')

    def cleanup_runner(self):
        """This method assumes ``prepare_runner_for_ssh()`` was called. That
        method isn't a "proper" setup method because it requires different
        arguments for different tests.
        """
        shutil.rmtree(self.output_dir)
        self.runner.cleanup()

    def test_one_node(self):
        mock_ssh_file('testmaster', 'some_file', b'file contents')

        _run_on_all_nodes(self.runner, self.output_dir, ['cat', 'some_file'],
                          print_stderr=False)

        with open(os.path.join(self.output_dir, 'master', 'stdout'), 'r') as f:
            self.assertEqual(f.read().rstrip(), 'file contents')

        self.assertEqual(os.listdir(self.output_dir), ['master'])

    def test_two_nodes(self):
        self.add_slave()
        self.ssh_worker_hosts.return_value = ['testslave0']

        self.runner._opts['num_ec2_instances'] = 2

        mock_ssh_file('testmaster', 'some_file', b'file contents 1')
        mock_ssh_file('testmaster!testslave0', 'some_file', b'file contents 2')

        self.runner.fs  # force initialization of _ssh_fs

        _run_on_all_nodes(self.runner, self.output_dir, ['cat', 'some_file'],
                          print_stderr=False)

        with open(os.path.join(self.output_dir, 'master', 'stdout'), 'r') as f:
            self.assertEqual(f.read().rstrip(), 'file contents 1')

        with open(os.path.join(self.output_dir, 'slave testslave0', 'stdout'),
                  'r') as f:
            self.assertEqual(f.read().strip(), 'file contents 2')

        self.assertEqual(sorted(os.listdir(self.output_dir)),
                         ['master', 'slave testslave0'])
