# Copyright 2009-2017 Yelp
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
import compileall
import datetime
import getpass
import os
import os.path
import shutil
import tarfile
from time import sleep
from unittest import TestCase
from unittest import skipIf
from zipfile import ZipFile

import mrjob.conf
from mrjob.conf import ClearedValue
from mrjob.conf import dump_mrjob_conf
from mrjob.emr import EMRJobRunner
from mrjob.inline import InlineMRJobRunner
from mrjob.py2 import StringIO
from mrjob.tools.emr.audit_usage import _JOB_KEY_RE
from mrjob.util import log_to_stream
from mrjob.util import to_lines

from tests.mock_boto3 import MockBoto3TestCase
from tests.mr_counting_job import MRCountingJob
from tests.mr_two_step_job import MRTwoStepJob
from tests.mr_word_count import MRWordCount
from tests.py2 import patch
from tests.quiet import logger_disabled
from tests.quiet import no_handlers_for_logger
from tests.sandbox import EmptyMrjobConfTestCase
from tests.sandbox import SandboxedTestCase
from tests.sandbox import mrjob_conf_patcher


class WithStatementTestCase(TestCase):

    def setUp(self):
        self.local_tmp_dir = None

    def tearDown(self):
        if self.local_tmp_dir:
            shutil.rmtree(self.local_tmp_dir)
            self.local_tmp_dir = None

    def _test_cleanup_after_with_statement(self, mode, should_exist):
        with InlineMRJobRunner(cleanup=mode, conf_paths=[]) as runner:
            self.local_tmp_dir = runner._get_local_tmp_dir()
            self.assertTrue(os.path.exists(self.local_tmp_dir))

        self.assertEqual(os.path.exists(self.local_tmp_dir), should_exist)
        if not should_exist:
            self.local_tmp_dir = None

    def test_cleanup_all(self):
        self._test_cleanup_after_with_statement(['ALL'], False)

    def test_cleanup_tmp(self):
        self._test_cleanup_after_with_statement(['TMP'], False)

    def test_cleanup_local_tmp(self):
        self._test_cleanup_after_with_statement(['LOCAL_TMP'], False)

    def test_cleanup_cloud_tmp(self):
        self._test_cleanup_after_with_statement(['CLOUD_TMP'], True)

    def test_cleanup_none(self):
        self._test_cleanup_after_with_statement(['NONE'], True)

    def test_cleanup_error(self):
        self.assertRaises(ValueError, self._test_cleanup_after_with_statement,
                          ['NONE', 'ALL'], True)
        self.assertRaises(ValueError, self._test_cleanup_after_with_statement,
                          ['GARBAGE'], True)

    def test_double_none_okay(self):
        self._test_cleanup_after_with_statement(['NONE', 'NONE'], True)


class TestJobName(TestCase):

    def setUp(self):
        self.blank_out_environment()
        self.monkey_patch_getuser()

    def tearDown(self):
        self.restore_getuser()
        self.restore_environment()

    def blank_out_environment(self):
        self._old_environ = os.environ.copy()
        # don't do os.environ = {}! This won't actually set environment
        # variables; it just monkey-patches os.environ
        os.environ.clear()

    def restore_environment(self):
        os.environ.clear()
        os.environ.update(self._old_environ)

    def monkey_patch_getuser(self):
        self._real_getuser = getpass.getuser
        self.getuser_should_fail = False

        def fake_getuser():
            if self.getuser_should_fail:
                raise Exception('fake getuser() was instructed to fail')
            else:
                return self._real_getuser()

        getpass.getuser = fake_getuser

    def restore_getuser(self):
        getpass.getuser = self._real_getuser

    def test_empty(self):
        runner = InlineMRJobRunner(conf_paths=[])
        match = _JOB_KEY_RE.match(runner.get_job_key())

        self.assertEqual(match.group(1), 'no_script')
        self.assertEqual(match.group(2), getpass.getuser())

    def test_empty_no_user(self):
        self.getuser_should_fail = True
        runner = InlineMRJobRunner(conf_paths=[])
        match = _JOB_KEY_RE.match(runner.get_job_key())

        self.assertEqual(match.group(1), 'no_script')
        self.assertEqual(match.group(2), 'no_user')

    def test_auto_label(self):
        runner = MRTwoStepJob(['--no-conf']).make_runner()
        match = _JOB_KEY_RE.match(runner.get_job_key())

        self.assertEqual(match.group(1), 'mr_two_step_job')
        self.assertEqual(match.group(2), getpass.getuser())

    def test_auto_owner(self):
        os.environ['USER'] = 'mcp'
        runner = InlineMRJobRunner(conf_paths=[])
        match = _JOB_KEY_RE.match(runner.get_job_key())

        self.assertEqual(match.group(1), 'no_script')
        self.assertEqual(match.group(2), 'mcp')

    def test_auto_everything(self):
        test_start = datetime.datetime.utcnow()

        os.environ['USER'] = 'mcp'
        runner = MRTwoStepJob(['--no-conf']).make_runner()
        match = _JOB_KEY_RE.match(runner.get_job_key())

        self.assertEqual(match.group(1), 'mr_two_step_job')
        self.assertEqual(match.group(2), 'mcp')

        job_start = datetime.datetime.strptime(
            match.group(3) + match.group(4), '%Y%m%d%H%M%S')
        job_start = job_start.replace(microsecond=int(match.group(5)))
        self.assertGreaterEqual(job_start, test_start)
        self.assertLessEqual(job_start - test_start,
                             datetime.timedelta(seconds=5))

    def test_owner_and_label_switches(self):
        runner_opts = ['--no-conf', '--owner=ads', '--label=ads_chain']
        runner = MRTwoStepJob(runner_opts).make_runner()
        match = _JOB_KEY_RE.match(runner.get_job_key())

        self.assertEqual(match.group(1), 'ads_chain')
        self.assertEqual(match.group(2), 'ads')

    def test_owner_and_label_kwargs(self):
        runner = InlineMRJobRunner(conf_paths=[],
                                   owner='ads', label='ads_chain')
        match = _JOB_KEY_RE.match(runner.get_job_key())

        self.assertEqual(match.group(1), 'ads_chain')
        self.assertEqual(match.group(2), 'ads')


class CreateMrjobZipTestCase(SandboxedTestCase):

    def test_create_mrjob_zip(self):
        with no_handlers_for_logger('mrjob.runner'):
            with InlineMRJobRunner(conf_paths=[]) as runner:
                mrjob_zip_path = runner._create_mrjob_zip()
                mrjob_zip = ZipFile(mrjob_zip_path)
                contents = mrjob_zip.namelist()

                for path in contents:
                    self.assertEqual(path[:6], 'mrjob/')

                self.assertIn('mrjob/job.py', contents)
                for filename in contents:
                    self.assertFalse(filename.endswith('.pyc'),
                                     msg="%s ends with '.pyc'" % filename)

    def test_mrjob_zip_compiles(self):
        runner = InlineMRJobRunner()
        with no_handlers_for_logger('mrjob.runner'):
            mrjob_zip = runner._create_mrjob_zip()

        ZipFile(mrjob_zip).extractall(self.tmp_dir)

        self.assertTrue(
            compileall.compile_dir(os.path.join(self.tmp_dir, 'mrjob'),
                                   quiet=1))


class TestCatOutput(SandboxedTestCase):

    # Test regression for #269
    def test_cat_output(self):
        a_dir_path = os.path.join(self.tmp_dir, 'a')
        b_dir_path = os.path.join(self.tmp_dir, 'b')
        l_dir_path = os.path.join(self.tmp_dir, '_logs')
        os.mkdir(a_dir_path)
        os.mkdir(b_dir_path)
        os.mkdir(l_dir_path)

        a_file_path = os.path.join(a_dir_path, 'part-00000')
        b_file_path = os.path.join(b_dir_path, 'part-00001')
        c_file_path = os.path.join(self.tmp_dir, 'part-00002')
        x_file_path = os.path.join(l_dir_path, 'log.xml')
        y_file_path = os.path.join(self.tmp_dir, '_SUCCESS')

        with open(a_file_path, 'w') as f:
            f.write('A')

        with open(b_file_path, 'w') as f:
            f.write('B')

        with open(c_file_path, 'w') as f:
            f.write('C')

        with open(x_file_path, 'w') as f:
            f.write('<XML XML XML/>')

        with open(y_file_path, 'w') as f:
            f.write('I win')

        runner = InlineMRJobRunner(conf_paths=[], output_dir=self.tmp_dir)
        self.assertEqual(sorted(to_lines(runner.cat_output())),
                         [b'A', b'B', b'C'])

    def test_deprecated_stream_output(self):
        self.makefile('part-00000', contents=b'1\n2')
        self.makefile('part-00001', contents=b'3\n4\n')

        runner = InlineMRJobRunner(conf_paths=[], output_dir=self.tmp_dir)

        log = self.start(patch('mrjob.runner.log'))

        # should group output into lines, but not join across files
        self.assertEqual(sorted(runner.stream_output()),
                         [b'1\n', b'2', b'3\n', b'4\n'])

        # should issue deprecation warning
        self.assertEqual(log.warning.call_count, 1)


class CheckInputPathsTestCase(SandboxedTestCase):

    def test_existing_input_paths(self):
        data = self.makefile('data', contents=b'stuff')

        job = MRWordCount([data])
        with job.make_runner() as runner:
            runner.run()

    def test_nonexistent_input_paths(self):
        missing_data = os.path.join(self.tmp_dir, 'data')

        job = MRWordCount([missing_data])
        with job.make_runner() as runner:
            self.assertRaises(IOError, runner.run)

    def test_disable_check_input_paths(self):
        missing_data = os.path.join(self.tmp_dir, 'data')

        job = MRWordCount(['--no-check-input-paths', missing_data])

        self.start(patch('mrjob.inline.InlineMRJobRunner._run',
                   side_effect=StopIteration))

        with job.make_runner() as runner:
            self.assertRaises(StopIteration, runner.run)

    def test_stdin_is_fine(self):
        job = MRWordCount()
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

    def test_dash_for_stdin(self):
        job = MRWordCount(['-'])
        job.sandbox()

        with job.make_runner() as runner:
            runner.run()

    def test_check_input_paths_enabled_by_default(self):
        job = MRWordCount()
        with job.make_runner() as runner:
            self.assertTrue(runner._opts['check_input_paths'])

    def test_check_input_paths_disabled(self):
        job = MRWordCount(['--no-check-input-paths'])
        with job.make_runner() as runner:
            self.assertFalse(runner._opts['check_input_paths'])

    def test_can_disable_check_input_paths_in_config(self):
        job = MRWordCount()
        with mrjob_conf_patcher(
                {'runners': {'inline': {'check_input_paths': False}}}):
            with job.make_runner() as runner:
                self.assertFalse(runner._opts['check_input_paths'])


class ClosedRunnerTestCase(EmptyMrjobConfTestCase):

    def test_job_closed_on_cleanup(self):
        job = MRWordCount()
        with job.make_runner() as runner:
            # do nothing
            self.assertFalse(runner._closed)
        self.assertTrue(runner._closed)


class StepInputAndOutputURIsTestCase(SandboxedTestCase):

    def test_two_step_job(self):
        input1_path = self.makefile('input1')
        input2_path = self.makefile('input2')

        job = MRTwoStepJob([
            '-r', 'hadoop',
            '--hadoop-bin', 'false',  # shouldn't run; just in case
            input1_path, input2_path])
        job.sandbox()

        with job.make_runner() as runner:
            runner._add_job_files_for_upload()

            input_uris_0 = runner._step_input_uris(0)
            self.assertEqual([os.path.basename(uri) for uri in input_uris_0],
                             ['input1', 'input2'])

            output_uri_0 = runner._step_output_uri(0)
            input_uris_1 = runner._step_input_uris(1)

            self.assertEqual(input_uris_1, [output_uri_0])

            output_uri_1 = runner._step_output_uri(1)
            self.assertEqual(output_uri_1, runner._output_dir)

    def test_output_dir_and_step_output_dir(self):
        input1_path = self.makefile('input1')
        input2_path = self.makefile('input2')

        # this has three steps, which lets us test step numbering
        job = MRCountingJob([
            '-r', 'hadoop',
            '--hadoop-bin', 'false',  # shouldn't run; just in case
            '--output-dir', 'hdfs:///tmp/output',
            '--step-output-dir', 'hdfs://tmp/step-output',
            input1_path, input2_path])
        job.sandbox()

        with job.make_runner() as runner:
            self.assertEqual(runner._num_steps(), 3)

            runner._add_job_files_for_upload()

            input_uris_0 = runner._step_input_uris(0)
            self.assertEqual([os.path.basename(uri) for uri in input_uris_0],
                             ['input1', 'input2'])

            output_uri_0 = runner._step_output_uri(0)
            self.assertEqual(output_uri_0, 'hdfs://tmp/step-output/0000')

            input_uris_1 = runner._step_input_uris(1)
            self.assertEqual(input_uris_1, [output_uri_0])

            output_uri_1 = runner._step_output_uri(1)
            self.assertEqual(output_uri_1, 'hdfs://tmp/step-output/0001')

            input_uris_2 = runner._step_input_uris(2)
            self.assertEqual(input_uris_2, [output_uri_1])

            output_uri_2 = runner._step_output_uri(2)
            self.assertEqual(output_uri_2, 'hdfs:///tmp/output')


class DirArchivePathTestCase(SandboxedTestCase):

    def test_dir(self):
        archive_dir = self.makedirs('archive')

        runner = InlineMRJobRunner()
        archive_path = runner._dir_archive_path(archive_dir)

        self.assertEqual(os.path.basename(archive_path), 'archive.tar.gz')

    def test_trailing_slash(self):
        archive_dir = self.makedirs('archive') + os.sep

        runner = InlineMRJobRunner()
        archive_path = runner._dir_archive_path(archive_dir)

        self.assertEqual(os.path.basename(archive_path), 'archive.tar.gz')

    def test_missing_dir(self):
        archive_path = os.path.join(self.tmp_dir, 'archive')

        runner = InlineMRJobRunner()

        self.assertRaises(OSError, runner._dir_archive_path, archive_path)

    def test_file(self):
        foo_file = self.makefile('foo')

        runner = InlineMRJobRunner()

        self.assertRaises(OSError, runner._dir_archive_path, foo_file)

    def test_uri(self):
        # we don't check whether URIs exist or are directories
        runner = InlineMRJobRunner()
        archive_path = runner._dir_archive_path('s3://bucket/stuff')

        self.assertEqual(os.path.basename(archive_path), 'stuff.tar.gz')

    def test_dirs_with_same_name(self):
        foo_archive = self.makedirs(os.path.join('foo', 'archive'))
        bar_archive = self.makedirs(os.path.join('bar', 'archive'))

        runner = InlineMRJobRunner()
        foo_archive_path = runner._dir_archive_path(foo_archive)
        bar_archive_path = runner._dir_archive_path(bar_archive)

        self.assertEqual(os.path.basename(foo_archive_path),
                         'archive.tar.gz')
        self.assertNotEqual(foo_archive_path, bar_archive_path)

    def test_same_dir_twice(self):
        archive_dir = self.makedirs('archive')

        runner = InlineMRJobRunner()
        archive_path_1 = runner._dir_archive_path(archive_dir)
        archive_path_2 = runner._dir_archive_path(archive_dir)

        self.assertEqual(os.path.basename(archive_path_1), 'archive.tar.gz')
        self.assertEqual(archive_path_1, archive_path_2)

    def test_doesnt_actually_create_archive(self):
        archive_dir = self.makedirs('archive')

        runner = InlineMRJobRunner()
        archive_path = runner._dir_archive_path(archive_dir)

        self.assertFalse(os.path.exists(archive_path))


class CreateDirArchiveTestCase(SandboxedTestCase):

    def setUp(self):
        super(CreateDirArchiveTestCase, self).setUp()

        self._to_archive = self.makedirs('archive')
        self.makefile(os.path.join('archive', 'foo'))
        self.makefile(os.path.join('archive', 'bar', 'baz'))

    def test_archive(self):
        runner = InlineMRJobRunner()

        tar_gz_path = runner._dir_archive_path(self._to_archive)
        self.assertEqual(os.path.basename(tar_gz_path), 'archive.tar.gz')

        runner._create_dir_archive(self._to_archive)

        tar_gz = tarfile.open(tar_gz_path, 'r:gz')
        try:
            self.assertEqual(sorted(tar_gz.getnames()),
                             [os.path.join('bar', 'baz'), 'foo'])
        finally:
            tar_gz.close()

    def test_only_create_archive_once(self):
        runner = InlineMRJobRunner()

        tar_gz_path = runner._dir_archive_path(self._to_archive)

        runner._create_dir_archive(self._to_archive)
        mtime_1 = os.stat(tar_gz_path).st_mtime

        sleep(1)
        runner._create_dir_archive(self._to_archive)
        mtime_2 = os.stat(tar_gz_path).st_mtime

        self.assertEqual(mtime_1, mtime_2)

    def test_nonexistent_dir(self):
        runner = InlineMRJobRunner()

        nonexistent_dir = os.path.join(self.tmp_dir, 'nonexistent')

        self.assertRaises(
            OSError, runner._create_dir_archive, nonexistent_dir)

    def test_empty_dir(self):
        runner = InlineMRJobRunner()

        empty_dir = self.makedirs('empty')

        tar_gz_path = runner._dir_archive_path(empty_dir)
        self.assertEqual(os.path.basename(tar_gz_path), 'empty.tar.gz')

        runner._create_dir_archive(empty_dir)

        with tarfile.open(tar_gz_path, 'r:gz') as tar_gz:
            self.assertEqual(sorted(tar_gz.getnames()), [])

    def test_file(self):
        qux_path = self.makefile('qux')

        runner = InlineMRJobRunner()

        self.assertRaises(OSError, runner._create_dir_archive, qux_path)


class RemoteCreateDirArchiveTestCase(MockBoto3TestCase):
    # additional test cases that archive stuff from (mock) S3

    def setUp(self):
        super(RemoteCreateDirArchiveTestCase, self).setUp()

        self.add_mock_s3_data(
            {'walrus': {'archive/foo': b'foo',
                        'archive/bar/baz': b'baz'}})

    def test_archive_remote_data(self):
        runner = EMRJobRunner()

        tar_gz_path = runner._dir_archive_path('s3://walrus/archive')
        self.assertEqual(os.path.basename(tar_gz_path), 'archive.tar.gz')

        runner._create_dir_archive('s3://walrus/archive')

        tar_gz = tarfile.open(tar_gz_path, 'r:gz')
        try:
            self.assertEqual(sorted(tar_gz.getnames()),
                             [os.path.join('bar', 'baz'), 'foo'])
        finally:
            tar_gz.close()


class ConfigFilesTestCase(SandboxedTestCase):

    MRJOB_CONF_CONTENTS = None  # don't patch load_opts_from_mrjob_confsq

    def save_conf(self, name, conf):
        conf_path = os.path.join(self.tmp_dir, name)
        with open(conf_path, 'w') as f:
            dump_mrjob_conf(conf, f)
        return conf_path

    def opts_for_conf(self, name, conf):
        conf_path = self.save_conf(name, conf)
        runner = InlineMRJobRunner(conf_paths=[conf_path])
        return runner._opts


class MultipleConfigFilesValuesTestCase(ConfigFilesTestCase):

    BASIC_CONF = {
        'runners': {
            'inline': {
                'cmdenv': {
                    'A_PATH': 'A',
                    'SOMETHING': 'X',
                },
                'hadoop_streaming_jar': 'monkey.jar',
                'jobconf': {
                    'lorax_speaks_for': 'trees',
                },
                'label': 'organic',
                'local_tmp_dir': '/tmp',
                'py_files': ['/mylib.zip'],
                'setup': [
                    'thing1',
                ],
            }
        }
    }

    def larger_conf(self):
        return {
            'include': os.path.join(self.tmp_dir, 'mrjob.conf'),
            'runners': {
                'inline': {
                    'bootstrap_mrjob': False,
                    'cmdenv': {
                        'A_PATH': 'B',
                        'SOMETHING': 'Y',
                        'SOMETHING_ELSE': 'Z',
                    },
                    'hadoop_streaming_jar': 'banana.jar',
                    'jobconf': {
                        'lorax_speaks_for': 'mazda',
                        'dr_seuss_is': 'awesome',
                    },
                    'label': 'usda_organic',
                    'local_tmp_dir': '/var/tmp',
                    'py_files': ['/yourlib.zip'],
                    'setup': [
                        'thing2',
                    ],
                }
            }
        }

    def setUp(self):
        super(MultipleConfigFilesValuesTestCase, self).setUp()
        self.opts_1 = self.opts_for_conf('mrjob.conf',
                                         self.BASIC_CONF)
        self.opts_2 = self.opts_for_conf('mrjob.larger.conf',
                                         self.larger_conf())

    def test_combine_dicts(self):
        self.assertEqual(self.opts_1['jobconf'], {
            'lorax_speaks_for': 'trees',
        })
        self.assertEqual(self.opts_2['jobconf'], {
            'lorax_speaks_for': 'mazda',
            'dr_seuss_is': 'awesome',
        })

    def test_combine_envs(self):
        self.assertEqual(self.opts_1['cmdenv'], {
            'A_PATH': 'A',
            'SOMETHING': 'X',
        })
        self.assertEqual(self.opts_2['cmdenv'], {
            'A_PATH': 'B:A',
            'SOMETHING': 'Y',
            'SOMETHING_ELSE': 'Z',
        })

    def test_combine_lists(self):
        self.assertEqual(self.opts_1['setup'], ['thing1'])
        self.assertEqual(self.opts_2['setup'],
                         ['thing1', 'thing2'])

    def test_combine_paths(self):
        self.assertEqual(self.opts_1['local_tmp_dir'], '/tmp')
        self.assertEqual(self.opts_2['local_tmp_dir'], '/var/tmp')

    def test_combine_path_lists(self):
        self.assertEqual(self.opts_1['py_files'], ['/mylib.zip'])
        self.assertEqual(self.opts_2['py_files'],
                         ['/mylib.zip', '/yourlib.zip'])

    def test_combine_values(self):
        self.assertEqual(self.opts_1['label'], 'organic')
        self.assertEqual(self.opts_2['label'], 'usda_organic')


class MultipleConfigFilesMachineryTestCase(ConfigFilesTestCase):

    def test_empty_runner_error(self):
        conf = dict(runner=dict(local=dict(local_tmp_dir='/tmp')))
        path = self.save_conf('basic', conf)

        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.conf', stderr)
            InlineMRJobRunner(conf_paths=[path])
            self.assertEqual(
                "No configs specified for inline runner\n",
                stderr.getvalue())

    def test_conf_contain_only_include_file(self):
        """If a config file only include other configuration files
        no warnings are thrown as long as the included files are
        not empty.
        """

        # dummy configuration for include file 1
        conf = {
            'runners': {
                'inline': {
                    'local_tmp_dir': "include_file1_local_tmp_dir"
                }
            }
        }

        include_file_1 = self.save_conf('include_file_1', conf)

        # dummy configuration for include file 2
        conf = {
            'runners': {
                'inline': {
                    'local_tmp_dir': "include_file2_local_tmp_dir"
                }
            }
        }

        include_file_2 = self.save_conf('include_file_2', conf)

        # test configuration
        conf = {
            'include': [include_file_1, include_file_2]
        }
        path = self.save_conf('twoincludefiles', conf)

        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.conf', stderr)
            InlineMRJobRunner(conf_paths=[path])
            self.assertEqual(
                "",
                stderr.getvalue())


class MultipleMultipleConfigFilesTestCase(ConfigFilesTestCase):

    BASE_CONFIG_LEFT = {
        'runners': {
            'inline': {
                'jobconf': dict(from_left=1, from_both=1),
                'label': 'i_dont_like_to_be_labelled',
            }
        }
    }

    BASE_CONFIG_RIGHT = {
        'runners': {
            'inline': {
                'jobconf': dict(from_right=2, from_both=2),
                'owner': 'ownership_is_against_my_principles'
            }
        }
    }

    def test_mrjob_has_multiple_inheritance_next_lets_add_generics(self):
        path_left = self.save_conf('left.conf', self.BASE_CONFIG_LEFT)
        path_right = self.save_conf('right.conf', self.BASE_CONFIG_RIGHT)
        opts_both = self.opts_for_conf('both.conf',
                                       dict(include=[path_left, path_right]))

        self.assertEqual(opts_both['jobconf'],
                         dict(from_left=1, from_both=2, from_right=2))
        self.assertEqual(opts_both['label'],
                         'i_dont_like_to_be_labelled')
        self.assertEqual(opts_both['owner'],
                         'ownership_is_against_my_principles')

    def test_multiple_configs_via_runner_args(self):
        path_left = self.save_conf('left.conf', self.BASE_CONFIG_LEFT)
        path_right = self.save_conf('right.conf', self.BASE_CONFIG_RIGHT)

        runner = InlineMRJobRunner(conf_paths=[path_left, path_right])

        self.assertEqual(runner._opts['jobconf'],
                         dict(from_left=1, from_both=2, from_right=2))


@skipIf(mrjob.conf.yaml is None, 'no yaml module')
class ClearTagTestCase(ConfigFilesTestCase):

    BASE_CONF = {
        'runners': {
            'inline': {
                'cmdenv': {
                    'PATH': '/some/nice/dir',
                },
                'jobconf': {
                    'some.property': 'something',
                },
                'setup': ['do something'],
            }
        }
    }

    def setUp(self):
        super(ClearTagTestCase, self).setUp()

        self.base_conf_path = self.save_conf('base.conf', self.BASE_CONF)
        runner = InlineMRJobRunner(conf_paths=[self.base_conf_path])
        self.base_opts = runner._opts

    def test_clear_cmdenv_path(self):
        opts = self.opts_for_conf('extend.conf', {
            'include': self.base_conf_path,
            'runners': {
                'inline': {
                    'cmdenv': {
                        'PATH': ClearedValue('/some/even/better/dir')
                    }
                }
            }
        })

        self.assertEqual(opts['cmdenv'], {'PATH': '/some/even/better/dir'})
        self.assertEqual(opts['jobconf'], self.base_opts['jobconf'])
        self.assertEqual(opts['setup'], self.base_opts['setup'])

    def test_clear_cmdenv(self):
        opts = self.opts_for_conf('extend.conf', {
            'include': self.base_conf_path,
            'runners': {
                'inline': {
                    'cmdenv': ClearedValue({
                        'USER': 'dave'
                    })
                }
            }
        })

        self.assertEqual(opts['cmdenv'], {'USER': 'dave'})
        self.assertEqual(opts['jobconf'], self.base_opts['jobconf'])
        self.assertEqual(opts['setup'], self.base_opts['setup'])

    def test_clear_jobconf(self):
        opts = self.opts_for_conf('extend.conf', {
            'include': self.base_conf_path,
            'runners': {
                'inline': {
                    'jobconf': ClearedValue(None)
                }
            }
        })

        self.assertEqual(opts['cmdenv'], self.base_opts['cmdenv'])
        self.assertEqual(opts['jobconf'], {})
        self.assertEqual(opts['setup'], self.base_opts['setup'])

    def test_clear_setup(self):
        opts = self.opts_for_conf('extend.conf', {
            'include': self.base_conf_path,
            'runners': {
                'inline': {
                    'setup': ClearedValue(['instead do this'])
                }
            }
        })

        self.assertEqual(opts['cmdenv'], self.base_opts['cmdenv'])
        self.assertEqual(opts['jobconf'], self.base_opts['jobconf'])
        self.assertEqual(opts['setup'], ['instead do this'])


class TestExtraKwargs(ConfigFilesTestCase):

    CONFIG = {'runners': {'inline': {
        'qux': 'quux',
        'setup': ['echo foo']}}}

    def setUp(self):
        super(TestExtraKwargs, self).setUp()
        self.path = self.save_conf('config', self.CONFIG)

    def test_extra_kwargs_in_mrjob_conf_okay(self):
        with logger_disabled('mrjob.runner'):
            runner = InlineMRJobRunner(conf_paths=[self.path])
            self.assertEqual(runner._opts['setup'], ['echo foo'])
            self.assertNotIn('qux', runner._opts)

    def test_extra_kwargs_passed_in_directly_okay(self):
        with logger_disabled('mrjob.runner'):
            runner = InlineMRJobRunner(
                foo='bar',
                local_tmp_dir='/var/tmp',
                conf_paths=[],
            )

            self.assertEqual(runner._opts['local_tmp_dir'], '/var/tmp')
            self.assertNotIn('bar', runner._opts)


class OptDebugPrintoutTestCase(ConfigFilesTestCase):

    def test_option_debug_printout(self):
        stderr = StringIO()

        with no_handlers_for_logger():
            log_to_stream('mrjob.runner', stderr, debug=True)

            InlineMRJobRunner(owner='dave')

        self.assertIn("'owner'", stderr.getvalue())
        self.assertIn("'dave'", stderr.getvalue())


class DeprecatedFileUploadArgsTestCase(SandboxedTestCase):

    def setUp(self):
        super(DeprecatedFileUploadArgsTestCase, self).setUp()

        self.log = self.start(patch('mrjob.runner.log'))

    def test_deprecated_file_upload_args(self):
        old_runner = InlineMRJobRunner(
            file_upload_args=[('--foo-config', '/tmp/.fooconf#dot-fooconf')])

        new_runner = InlineMRJobRunner(
            extra_args=['--foo-config', dict(
                path='/tmp/.fooconf', name='dot-fooconf', type='file')])

        self.assertEqual(old_runner._extra_args, new_runner._extra_args)
        self.assertEqual(old_runner._spark_files, new_runner._spark_files)
        self.assertEqual(old_runner._working_dir_mgr._name_to_typed_path,
                         new_runner._working_dir_mgr._name_to_typed_path)

        self.assertTrue(self.log.warning.called)
