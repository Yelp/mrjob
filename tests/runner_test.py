# Copyright 2009-2011 Yelp
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
"""Test the runner base class MRJobRunner"""

from __future__ import with_statement

from StringIO import StringIO
import bz2
import datetime
import getpass
import gzip
import os
import shutil
import tarfile
from testify import TestCase
from testify import assert_equal
from testify import assert_in
from testify import assert_gte
from testify import assert_lte
from testify import assert_not_in
from testify import assert_raises
from testify import setup
from testify import teardown
import tempfile

from mrjob.conf import dump_mrjob_conf
from mrjob.local import LocalMRJobRunner
from mrjob.parse import JOB_NAME_RE
from mrjob.runner import CLEANUP_DEFAULT
from mrjob.util import log_to_stream
from tests.mr_two_step_job import MRTwoStepJob
from tests.quiet import logger_disabled
from tests.quiet import no_handlers_for_logger


class WithStatementTestCase(TestCase):

    @setup
    def setup_ivars(self):
        self.local_tmp_dir = None

    @teardown
    def delete_tmpdir(self):
        if self.local_tmp_dir:
            shutil.rmtree(self.local_tmp_dir)
            self.local_tmp_dir = None

    def _test_cleanup_after_with_statement(self, mode, should_exist):
        with LocalMRJobRunner(cleanup=mode) as runner:
            self.local_tmp_dir = runner._get_local_tmp_dir()
            assert os.path.exists(self.local_tmp_dir)

        assert_equal(os.path.exists(self.local_tmp_dir), should_exist)
        if not should_exist:
            self.local_tmp_dir = None

    def test_cleanup_all(self):
        self._test_cleanup_after_with_statement(['ALL'], False)

    def test_cleanup_scratch(self):
        self._test_cleanup_after_with_statement(['SCRATCH'], False)

    def test_cleanup_local_scratch(self):
        self._test_cleanup_after_with_statement(['LOCAL_SCRATCH'], False)

    def test_cleanup_remote_scratch(self):
        self._test_cleanup_after_with_statement(['REMOTE_SCRATCH'], True)

    def test_cleanup_none(self):
        self._test_cleanup_after_with_statement(['NONE'], True)

    def test_cleanup_error(self):
        assert_raises(ValueError, self._test_cleanup_after_with_statement,
                      ['NONE', 'ALL'], True)
        assert_raises(ValueError, self._test_cleanup_after_with_statement,
                      ['GARBAGE'], True)

    def test_double_none_okay(self):
        self._test_cleanup_after_with_statement(['NONE', 'NONE'], True)

    def test_cleanup_deprecated(self):
        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob', stderr)
            with LocalMRJobRunner(cleanup=CLEANUP_DEFAULT) as runner:
                self.local_tmp_dir = runner._get_local_tmp_dir()
                assert os.path.exists(self.local_tmp_dir)

            assert_equal(os.path.exists(self.local_tmp_dir), False)
            self.local_tmp_dir = None
            assert_in('deprecated', stderr.getvalue())

    def test_cleanup_not_supported(self):
        assert_raises(ValueError,
                      LocalMRJobRunner, cleanup_on_failure=CLEANUP_DEFAULT)


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
        with logger_disabled('mrjob.runner'):
            with LocalMRJobRunner(conf_path=self.mrjob_conf_path) as runner:
                assert_equal(runner._opts['setup_cmds'], ['echo foo'])
                assert_not_in('qux', runner._opts)

    def test_extra_kwargs_passed_in_directly_okay(self):
        with logger_disabled('mrjob.runner'):
            with LocalMRJobRunner(
                conf_path=False, base_tmp_dir='/var/tmp', foo='bar') as runner:
                assert_equal(runner._opts['base_tmp_dir'], '/var/tmp')
                assert_not_in('bar', runner._opts)


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


class TestHadoopConfArgs(TestCase):

    def test_empty(self):
        runner = LocalMRJobRunner(conf_path=False)
        assert_equal(runner._hadoop_conf_args(0, 1), [])

    def test_hadoop_extra_args(self):
        extra_args = ['-foo', 'bar']
        runner = LocalMRJobRunner(conf_path=False,
                                  hadoop_extra_args=extra_args)
        assert_equal(runner._hadoop_conf_args(0, 1), extra_args)

    def test_cmdenv(self):
        cmdenv = {'FOO': 'bar', 'BAZ': 'qux', 'BAX': 'Arnold'}
        runner = LocalMRJobRunner(conf_path=False, cmdenv=cmdenv)
        assert_equal(runner._hadoop_conf_args(0, 1),
                     ['-cmdenv', 'BAX=Arnold',
                      '-cmdenv', 'BAZ=qux',
                      '-cmdenv', 'FOO=bar',
                      ])

    def test_hadoop_input_format(self):
        format = 'org.apache.hadoop.mapred.SequenceFileInputFormat'
        runner = LocalMRJobRunner(conf_path=False, hadoop_input_format=format)
        assert_equal(runner._hadoop_conf_args(0, 1),
                     ['-inputformat', format])
        # test multi-step job
        assert_equal(runner._hadoop_conf_args(0, 2),
                     ['-inputformat', format])
        assert_equal(runner._hadoop_conf_args(1, 2), [])

    def test_hadoop_output_format(self):
        format = 'org.apache.hadoop.mapred.SequenceFileOutputFormat'
        runner = LocalMRJobRunner(conf_path=False, hadoop_output_format=format)
        assert_equal(runner._hadoop_conf_args(0, 1),
                     ['-outputformat', format])
        # test multi-step job
        assert_equal(runner._hadoop_conf_args(0, 2), [])
        assert_equal(runner._hadoop_conf_args(1, 2),
                     ['-outputformat', format])

    def test_jobconf(self):
        jobconf = {'FOO': 'bar', 'BAZ': 'qux', 'BAX': 'Arnold'}
        runner = LocalMRJobRunner(conf_path=False, jobconf=jobconf)
        assert_equal(runner._hadoop_conf_args(0, 1),
                     ['-D', 'BAX=Arnold',
                      '-D', 'BAZ=qux',
                      '-D', 'FOO=bar',
                      ])
        runner = LocalMRJobRunner(conf_path=False, jobconf=jobconf,
                                  hadoop_version='0.18')
        assert_equal(runner._hadoop_conf_args(0, 1),
                     ['-jobconf', 'BAX=Arnold',
                      '-jobconf', 'BAZ=qux',
                      '-jobconf', 'FOO=bar',
                      ])

    def test_partitioner(self):
        partitioner = 'org.apache.hadoop.mapreduce.Partitioner'

        runner = LocalMRJobRunner(conf_path=False, partitioner=partitioner)
        assert_equal(runner._hadoop_conf_args(0, 1),
                     ['-partitioner', partitioner])

    def test_hadoop_extra_args_comes_first(self):
        runner = LocalMRJobRunner(
            cmdenv={'FOO': 'bar'},
            conf_path=False,
            hadoop_extra_args=['-libjar', 'qux.jar'],
            hadoop_input_format='FooInputFormat',
            hadoop_output_format='BarOutputFormat',
            jobconf={'baz': 'quz'},
            partitioner='java.lang.Object',
        )
        # hadoop_extra_args should come first
        conf_args = runner._hadoop_conf_args(0, 1)
        assert_equal(conf_args[:2], ['-libjar', 'qux.jar'])
        assert_equal(len(conf_args), 12)


class TestCat(TestCase):

    @setup
    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    @teardown
    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_cat_uncompressed(self):
        input_path = os.path.join(self.tmp_dir, 'input')
        with open(input_path, 'w') as input_file:
            input_file.write('bar\nfoo\n')

        with LocalMRJobRunner() as runner:
            output = []
            for line in runner.cat(input_path):
                output.append(line)

        assert_equal(output, ['bar\n', 'foo\n'])

    def test_cat_compressed(self):
        input_gz_path = os.path.join(self.tmp_dir, 'input.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'w')
        input_gz.write('foo\nbar\n')
        input_gz.close()

        with LocalMRJobRunner() as runner:
            output = []
            for line in runner.cat(input_gz_path):
                output.append(line)

        assert_equal(output, ['foo\n', 'bar\n'])

        input_bz2_path = os.path.join(self.tmp_dir, 'input.bz2')
        input_bz2 = bz2.BZ2File(input_bz2_path, 'w')
        input_bz2.write('bar\nbar\nfoo\n')
        input_bz2.close()

        with LocalMRJobRunner() as runner:
            output = []
            for line in runner.cat(input_bz2_path):
                output.append(line)

        assert_equal(output, ['bar\n', 'bar\n', 'foo\n'])
