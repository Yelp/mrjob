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
"""Unit testing of MRJobRunner"""
from __future__ import with_statement

from cStringIO import StringIO
import os
from testify import TestCase, assert_equal, assert_not_equal, assert_raises, setup, teardown
import tempfile

from mrjob.conf import dump_mrjob_conf
from mrjob.local import LocalMRJobRunner
from mrjob.tests.mr_two_step_job import MRTwoStepJob

class WithStatementTestCase(TestCase):

    def test_cleanup_after_with_statement(self):
        local_tmp_dir = None

        with LocalMRJobRunner() as runner:
            local_tmp_dir = runner._get_local_tmp_dir()
            assert os.path.exists(local_tmp_dir)

        assert not os.path.exists(local_tmp_dir)

class TestExtraKwargs(TestCase):

    @setup
    def make_mrjob_conf(self):
        _, self.mrjob_conf_path = tempfile.mkstemp(prefix='mrjob.conf.')
        # include one fake kwarg, and one real one
        conf = {'runners': {'local': {'qux': 'quux',
                                      'setup_cmds': ['echo foo']}}}
        with open(self.mrjob_conf_path, 'w') as conf_file:
            self.mrjob_conf = dump_mrjob_conf(conf, conf_file)

    @teardown
    def delete_mrjob_conf(self):
        os.unlink(self.mrjob_conf_path)
    
    def test_extra_kwargs_in_mrjob_conf_okay(self):
        with LocalMRJobRunner(conf_path=self.mrjob_conf_path) as runner:
            assert_equal(runner._opts['setup_cmds'], ['echo foo'])

    def test_extra_kwargs_passed_in_directly_not_okay(self):
        assert_raises(TypeError,
                      LocalMRJobRunner, qux='quux', setup_cmds=['echo foo'])
