
# Copyright 2011 Yelp
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

from __future__ import with_statement

import os
import shutil
import tempfile

from testify import assert_equal
from testify import setup
from testify import teardown

from mrjob.emr import EMRJobRunner
from mrjob.tools.emr.mrboss import run_on_all_nodes
from tests.emr_test import MockEMRAndS3TestCase
from tests.emr_test import BUCKET_URI
from tests.emr_test import LOG_DIR
from tests.mockssh import mock_ssh_dir
from tests.mockssh import mock_ssh_file


class MRBossTestCase(MockEMRAndS3TestCase):

    @setup
    def make_runner(self):
        self.runner = EMRJobRunner(conf_path=False)
        self.add_mock_s3_data({'walrus': {}})
        self.runner = EMRJobRunner(s3_sync_wait_time=0,
                                   s3_scratch_uri='s3://walrus/tmp',
                                   conf_path=False)
        self.runner._s3_job_log_uri = BUCKET_URI + LOG_DIR
        self.prepare_runner_for_ssh(self.runner)
        self.runner._enable_slave_ssh_access()
        self.output_dir = tempfile.mkdtemp(prefix='mrboss_wd')

    @teardown
    def cleanup_runner(self):
        """This method assumes ``prepare_runner_for_ssh()`` was called. That
        method isn't a "proper" setup method because it requires different
        arguments for different tests.
        """
        shutil.rmtree(self.output_dir)
        self.runner.cleanup()
        self.teardown_ssh()

    def test_one_node(self):
        mock_ssh_file('testmaster', 'some_file', 'file contents')

        run_on_all_nodes(self.runner, self.output_dir, ['cat', 'some_file'],
                         print_stderr=False)

        with open(os.path.join(self.output_dir, 'master', 'stdout'), 'r') as f:
            assert_equal(f.read(), 'file contents\n')

        assert_equal(os.listdir(self.output_dir), ['master'])

    def test_two_nodes(self):
        self.add_slave()
        self.runner._opts['num_ec2_instances'] = 2

        mock_ssh_file('testmaster', 'some_file', 'file contents 1')
        mock_ssh_file('testmaster!testslave0', 'some_file', 'file contents 2')

        run_on_all_nodes(self.runner, self.output_dir, ['cat', 'some_file'],
                         print_stderr=False)

        with open(os.path.join(self.output_dir, 'master', 'stdout'), 'r') as f:
            assert_equal(f.read(), 'file contents 1\n')

        with open(os.path.join(self.output_dir, 'slave testslave0', 'stdout'),
                  'r') as f:
            assert_equal(f.read(), 'file contents 2\n')

        assert_equal(sorted(os.listdir(self.output_dir)),
                     ['master', 'slave testslave0'])
