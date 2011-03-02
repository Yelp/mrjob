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

from StringIO import StringIO
import datetime
import getpass
import os
import tarfile
from testify import TestCase, assert_equal, assert_in, assert_not_equal, assert_gte, assert_lte, assert_not_in, assert_raises, setup, teardown
import tempfile

from mrjob.conf import dump_mrjob_conf
from mrjob.local import LocalMRJobRunner
from mrjob.parse import JOB_NAME_RE
from tests.mr_two_step_job import MRTwoStepJob

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
            assert_not_in('qux', runner._opts)

    def test_extra_kwargs_passed_in_directly_okay(self):
        with LocalMRJobRunner(
            conf_path=False, base_tmp_dir='/var/tmp', foo='bar') as runner:
            assert_equal(runner._opts['base_tmp_dir'], '/var/tmp')
            assert_not_in('bar', runner._opts)

class TestDeprecatedKwargs(TestCase):

    def test_job_name_prefix_is_now_label(self):
        old_way = LocalMRJobRunner(conf_path=False, job_name_prefix='ads_chain')
        old_opts = old_way.get_opts()

        new_way = LocalMRJobRunner(conf_path=False, label='ads_chain')
        new_opts = new_way.get_opts()

        assert_equal(old_opts, new_opts)
        assert_equal(old_opts['label'], 'ads_chain')
        assert_not_in('job_name_prefix', old_opts)

class TestJobName(TestCase):

    @setup
    def blank_out_environment(self):
        self._old_environ = os.environ.copy()
        # don't do os.environ = {}! This won't actually set environment
        # variables; it just monkey-patches os.environ
        os.environ.clear()

    @teardown
    def restore_environment(self):
        os.environ.clear()
        os.environ.update(self._old_environ)

    @setup
    def monkey_patch_getuser(self):
        self._real_getuser = getpass.getuser
        self.getuser_should_fail = False

        def fake_getuser():
            if self.getuser_should_fail:
                raise Exception('fake getuser() was instructed to fail')
            else:
                return self._real_getuser()

        getpass.getuser = fake_getuser

    @teardown
    def restore_getuser(self):
        getpass.getuser = self._real_getuser

    def test_empty(self):
        runner = LocalMRJobRunner(conf_path=False)
        match = JOB_NAME_RE.match(runner.get_job_name())

        assert_equal(match.group(1), 'no_script')
        assert_equal(match.group(2), getpass.getuser())

    def test_empty_no_user(self):
        self.getuser_should_fail = True
        runner = LocalMRJobRunner(conf_path=False)
        match = JOB_NAME_RE.match(runner.get_job_name())

        assert_equal(match.group(1), 'no_script')
        assert_equal(match.group(2), 'no_user')

    def test_auto_label(self):
        runner = MRTwoStepJob(['--no-conf']).make_runner()
        match = JOB_NAME_RE.match(runner.get_job_name())

        assert_equal(match.group(1), 'mr_two_step_job')
        assert_equal(match.group(2), getpass.getuser())

    def test_auto_owner(self):
        os.environ['USER'] = 'mcp'
        runner = LocalMRJobRunner(conf_path=False)
        match = JOB_NAME_RE.match(runner.get_job_name())

        assert_equal(match.group(1), 'no_script')
        assert_equal(match.group(2), 'mcp')

    def test_auto_everything(self):
        test_start = datetime.datetime.utcnow()

        os.environ['USER'] = 'mcp'
        runner = MRTwoStepJob(['--no-conf']).make_runner()
        match = JOB_NAME_RE.match(runner.get_job_name())

        assert_equal(match.group(1), 'mr_two_step_job')
        assert_equal(match.group(2), 'mcp')

        job_start = datetime.datetime.strptime(
            match.group(3) + match.group(4), '%Y%m%d%H%M%S')
        job_start = job_start.replace(microsecond=int(match.group(5)))
        assert_gte(job_start, test_start)
        assert_lte(job_start - test_start, datetime.timedelta(seconds=5))

    def test_owner_and_label_switches(self):
        runner_opts = ['--no-conf', '--owner=ads', '--label=ads_chain']
        runner = MRTwoStepJob(runner_opts).make_runner()
        match = JOB_NAME_RE.match(runner.get_job_name())

        assert_equal(match.group(1), 'ads_chain')
        assert_equal(match.group(2), 'ads')

    def test_owner_and_label_kwargs(self):
        runner = LocalMRJobRunner(conf_path=False,
                                  owner='ads', label='ads_chain')
        match = JOB_NAME_RE.match(runner.get_job_name())

        assert_equal(match.group(1), 'ads_chain')
        assert_equal(match.group(2), 'ads')


class CreateMrjobTarGzTestCase(TestCase):

    def test_create_mrjob_tar_gz(self):
        with LocalMRJobRunner(conf_path=False) as runner:
            mrjob_tar_gz_path = runner._create_mrjob_tar_gz()
            mrjob_tar_gz = tarfile.open(mrjob_tar_gz_path)
            contents = mrjob_tar_gz.getnames()

            for path in contents:
                assert_equal(path[:6], 'mrjob/')

            assert_in('mrjob/job.py', contents)
