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
"""Test the runner base class MRJobRunner"""

from __future__ import with_statement

from StringIO import StringIO
import bz2
import datetime
import getpass
import gzip
import os
import shutil
import stat
from subprocess import CalledProcessError
import sys
import tarfile
import tempfile
try:
    import unittest2 as unittest
    unittest  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import unittest
from mock import patch
from mrjob.conf import dump_mrjob_conf
from mrjob.inline import InlineMRJobRunner
from mrjob.parse import JOB_NAME_RE
from mrjob.runner import CLEANUP_DEFAULT
from mrjob.runner import MRJobRunner
from mrjob.util import log_to_stream
from tests.mr_two_step_job import MRTwoStepJob
from tests.quiet import logger_disabled
from tests.quiet import no_handlers_for_logger


class WithStatementTestCase(unittest.TestCase):

    def setUp(self):
        self.setup_ivars()

    def tearDown(self):
        self.delete_tmpdir()

    def setup_ivars(self):
        self.local_tmp_dir = None

    def delete_tmpdir(self):
        if self.local_tmp_dir:
            shutil.rmtree(self.local_tmp_dir)
            self.local_tmp_dir = None

    def _test_cleanup_after_with_statement(self, mode, should_exist):
        with InlineMRJobRunner(cleanup=mode, conf_path=False) as runner:
            self.local_tmp_dir = runner._get_local_tmp_dir()
            assert os.path.exists(self.local_tmp_dir)

        self.assertEqual(os.path.exists(self.local_tmp_dir), should_exist)
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
        self.assertRaises(ValueError, self._test_cleanup_after_with_statement,
                          ['NONE', 'ALL'], True)
        self.assertRaises(ValueError, self._test_cleanup_after_with_statement,
                          ['GARBAGE'], True)

    def test_double_none_okay(self):
        self._test_cleanup_after_with_statement(['NONE', 'NONE'], True)

    def test_cleanup_deprecated(self):
        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob', stderr)
            with InlineMRJobRunner(
                cleanup=CLEANUP_DEFAULT, conf_path=False) as runner:

                self.local_tmp_dir = runner._get_local_tmp_dir()
                assert os.path.exists(self.local_tmp_dir)

            self.assertEqual(os.path.exists(self.local_tmp_dir), False)
            self.local_tmp_dir = None
            self.assertIn('deprecated', stderr.getvalue())

    def test_cleanup_not_supported(self):
        self.assertRaises(
            ValueError,
            InlineMRJobRunner,
            cleanup_on_failure=CLEANUP_DEFAULT, conf_path=False)


class TestExtraKwargs(unittest.TestCase):

    def setUp(self):
        self.make_mrjob_conf()

    def tearDown(self):
        self.delete_mrjob_conf()

    def make_mrjob_conf(self):
        _, self.mrjob_conf_path = tempfile.mkstemp(prefix='mrjob.conf.')
        # include one fake kwarg, and one real one
        conf = {'runners': {'inline': {'qux': 'quux',
                                       'setup_cmds': ['echo foo']}}}
        with open(self.mrjob_conf_path, 'w') as conf_file:
            self.mrjob_conf = dump_mrjob_conf(conf, conf_file)

    def delete_mrjob_conf(self):
        os.unlink(self.mrjob_conf_path)

    def test_extra_kwargs_in_mrjob_conf_okay(self):
        with logger_disabled('mrjob.runner'):
            with InlineMRJobRunner(conf_path=self.mrjob_conf_path) as runner:
                self.assertEqual(runner._opts['setup_cmds'], ['echo foo'])
                self.assertNotIn('qux', runner._opts)

    def test_extra_kwargs_passed_in_directly_okay(self):
        with logger_disabled('mrjob.runner'):
            with InlineMRJobRunner(
                conf_path=False, base_tmp_dir='/var/tmp', foo='bar') as runner:
                self.assertEqual(runner._opts['base_tmp_dir'], '/var/tmp')
                self.assertNotIn('bar', runner._opts)


class TestJobName(unittest.TestCase):

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
        runner = InlineMRJobRunner(conf_path=False)
        match = JOB_NAME_RE.match(runner.get_job_name())

        self.assertEqual(match.group(1), 'no_script')
        self.assertEqual(match.group(2), getpass.getuser())

    def test_empty_no_user(self):
        self.getuser_should_fail = True
        runner = InlineMRJobRunner(conf_path=False)
        match = JOB_NAME_RE.match(runner.get_job_name())

        self.assertEqual(match.group(1), 'no_script')
        self.assertEqual(match.group(2), 'no_user')

    def test_auto_label(self):
        runner = MRTwoStepJob(['--no-conf']).make_runner()
        match = JOB_NAME_RE.match(runner.get_job_name())

        self.assertEqual(match.group(1), 'mr_two_step_job')
        self.assertEqual(match.group(2), getpass.getuser())

    def test_auto_owner(self):
        os.environ['USER'] = 'mcp'
        runner = InlineMRJobRunner(conf_path=False)
        match = JOB_NAME_RE.match(runner.get_job_name())

        self.assertEqual(match.group(1), 'no_script')
        self.assertEqual(match.group(2), 'mcp')

    def test_auto_everything(self):
        test_start = datetime.datetime.utcnow()

        os.environ['USER'] = 'mcp'
        runner = MRTwoStepJob(['--no-conf']).make_runner()
        match = JOB_NAME_RE.match(runner.get_job_name())

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
        match = JOB_NAME_RE.match(runner.get_job_name())

        self.assertEqual(match.group(1), 'ads_chain')
        self.assertEqual(match.group(2), 'ads')

    def test_owner_and_label_kwargs(self):
        runner = InlineMRJobRunner(conf_path=False,
                                  owner='ads', label='ads_chain')
        match = JOB_NAME_RE.match(runner.get_job_name())

        self.assertEqual(match.group(1), 'ads_chain')
        self.assertEqual(match.group(2), 'ads')


class CreateMrjobTarGzTestCase(unittest.TestCase):

    def test_create_mrjob_tar_gz(self):
        with InlineMRJobRunner(conf_path=False) as runner:
            mrjob_tar_gz_path = runner._create_mrjob_tar_gz()
            mrjob_tar_gz = tarfile.open(mrjob_tar_gz_path)
            contents = mrjob_tar_gz.getnames()

            for path in contents:
                self.assertEqual(path[:6], 'mrjob/')

            self.assertIn('mrjob/job.py', contents)


class TestFilesystem(unittest.TestCase):

    def setUp(self):
        self.make_tmp_dir()

    def tearDown(self):
        self.rm_tmp_dir()

    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_cat_uncompressed(self):
        input_path = os.path.join(self.tmp_dir, 'input')
        with open(input_path, 'w') as input_file:
            input_file.write('bar\nfoo\n')

        with InlineMRJobRunner(conf_path=False) as runner:
            output = []
            for line in runner.cat(input_path):
                output.append(line)

        self.assertEqual(output, ['bar\n', 'foo\n'])

    def test_cat_compressed(self):
        input_gz_path = os.path.join(self.tmp_dir, 'input.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'w')
        input_gz.write('foo\nbar\n')
        input_gz.close()

        with InlineMRJobRunner(conf_path=False) as runner:
            output = []
            for line in runner.cat(input_gz_path):
                output.append(line)

        self.assertEqual(output, ['foo\n', 'bar\n'])

        input_bz2_path = os.path.join(self.tmp_dir, 'input.bz2')
        input_bz2 = bz2.BZ2File(input_bz2_path, 'w')
        input_bz2.write('bar\nbar\nfoo\n')
        input_bz2.close()

        with InlineMRJobRunner(conf_path=False) as runner:
            output = []
            for line in runner.cat(input_bz2_path):
                output.append(line)

        self.assertEqual(output, ['bar\n', 'bar\n', 'foo\n'])

    def test_du(self):
        data_path_1 = os.path.join(self.tmp_dir, 'data1')
        with open(data_path_1, 'w') as f:
            f.write("abcd")

        data_dir = os.path.join(self.tmp_dir, 'more')
        os.mkdir(data_dir)

        data_path_2 = os.path.join(data_dir, 'data2')
        with open(data_path_2, 'w') as f:
            f.write("defg")

        self.assertEqual(InlineMRJobRunner(conf_path=False).du(self.tmp_dir), 8)
        self.assertEqual(InlineMRJobRunner(conf_path=False).du(data_path_1), 4)
        self.assertEqual(InlineMRJobRunner(conf_path=False).du(data_path_2), 4)


class TestStreamingOutput(unittest.TestCase):

    def setUp(self):
        self.make_tmp_dir()

    def tearDown(self):
        self.rm_tmp_dir()

    def make_tmp_dir(self):
        # use a leading underscore to test behavior of underscore-ignoring
        # code that shouldn't ignore the entire output_dir
        self.tmp_dir = tempfile.mkdtemp(prefix='_streamingtest')

    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    # Test regression for #269
    def test_stream_output(self):
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

        runner = InlineMRJobRunner(conf_path=False)
        runner._output_dir = self.tmp_dir
        self.assertEqual(sorted(runner.stream_output()),
                         ['A', 'B', 'C'])


class TestInvokeSort(unittest.TestCase):

    def setUp(self):
        self.make_tmp_dir_and_set_up_files()
        self.save_environment()

    def tearDown(self):
        self.restore_environment()
        self.rm_tmp_dir()

    def make_tmp_dir_and_set_up_files(self):
        self.tmp_dir = tempfile.mkdtemp()

        self.a = os.path.join(self.tmp_dir, 'a')
        with open(self.a, 'w') as a:
            a.write('A\n')
            a.write('apple\n')
            a.write('alligator\n')

        self.b = os.path.join(self.tmp_dir, 'b')
        with open(self.b, 'w') as b:
            b.write('B\n')
            b.write('banana\n')
            b.write('ball\n')

        self.out = os.path.join(self.tmp_dir, 'out')

    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def save_environment(self):
        self._old_environ = os.environ.copy()

    def restore_environment(self):
        os.environ.clear()
        os.environ.update(self._old_environ)

    def find_real_sort_bin(self):
        for path in os.environ.get('PATH', '').split(os.pathsep) or ():
            for sort_path in [os.path.join(path, 'sort'),
                              os.path.join(path, 'sort.exe')]:
                if os.path.exists(sort_path):
                    return os.path.abspath(sort_path)

        raise Exception("Can't find sort binary!")

    def use_alternate_sort(self, script_contents):
        sort_bin = os.path.join(self.tmp_dir, 'sort')
        with open(sort_bin, 'w') as f:
            f.write('#!%s\n' % sys.executable)
            f.write(script_contents)

        os.chmod(sort_bin, stat.S_IREAD | stat.S_IEXEC)
        os.environ['PATH'] = self.tmp_dir

    def use_simulated_windows_sort(self):
        script_contents = """\
import os
from subprocess import check_call
import sys

if len(sys.argv) > 2:
    print >> sys.stderr, 'Input file specified two times.'
    sys.exit(1)

real_sort_bin = %r

check_call([real_sort_bin] + sys.argv[1:])
""" % (self.find_real_sort_bin())

        self.use_alternate_sort(script_contents)

    def use_bad_sort(self):
        script_contents = """\
import sys

print >> sys.stderr, 'Sorting is for chumps!'
sys.exit(13)
"""

        self.use_alternate_sort(script_contents)

    def environment_variable_checks(self, runner, environment_check_list):
        environment_vars = {}

        def check_call_se(*args, **kwargs):
            for key in kwargs['env'].keys():
                environment_vars[key] = kwargs['env'][key]

        with patch('mrjob.runner.check_call', side_effect=check_call_se):
            runner._invoke_sort([self.a], self.out)
            for key in environment_check_list:
                self.assertEqual(environment_vars.get(key, None),
                                 runner._opts['base_tmp_dir'])

    def test_no_files(self):
        runner = MRJobRunner(conf_path=False)
        self.assertRaises(ValueError,
                          runner._invoke_sort, [], self.out)

    def test_one_file(self):
        runner = MRJobRunner(conf_path=False)
        runner._invoke_sort([self.a], self.out)

        self.assertEqual(list(open(self.out)),
                         ['A\n',
                          'alligator\n',
                          'apple\n'])

    def test_two_files(self):
        runner = MRJobRunner(conf_path=False)
        runner._invoke_sort([self.a, self.b], self.out)

        self.assertEqual(list(open(self.out)),
                         ['A\n',
                          'B\n',
                          'alligator\n',
                          'apple\n',
                          'ball\n',
                          'banana\n'])

    def test_windows_sort_on_one_file(self):
        self.use_simulated_windows_sort()
        self.test_one_file()

    def test_windows_sort_on_two_files(self):
        self.use_simulated_windows_sort()
        self.test_two_files()

    def test_bad_sort(self):
        self.use_bad_sort()

        runner = MRJobRunner(conf_path=False)
        with no_handlers_for_logger():
            self.assertRaises(CalledProcessError,
                              runner._invoke_sort, [self.a, self.b], self.out)

    def test_environment_variables_non_windows(self):
        runner = MRJobRunner(conf_path=False)
        self.environment_variable_checks(runner, ['TEMP', 'TMPDIR'])

    def test_environment_variables_windows(self):
        runner = MRJobRunner(conf_path=False)
        runner._sort_is_windows_sort = True
        self.environment_variable_checks(runner, ['TMP'])


class ConfigFilesTestCase(unittest.TestCase):

    def setUp(self):
        super(ConfigFilesTestCase, self).setUp()
        self.make_tmp_dir()

    def tearDown(self):
        super(ConfigFilesTestCase, self).tearDown()
        self.rm_tmp_dir()

    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def save_conf(self, name, conf):
        conf_path = os.path.join(self.tmp_dir, name)
        with open(conf_path, 'w') as f:
            dump_mrjob_conf(conf, f)
        return conf_path

    def opts_for_conf(self, name, conf):
        conf_path = self.save_conf(name, conf)
        runner = InlineMRJobRunner(conf_path=conf_path)
        return runner._opts


class MultipleConfigFilesValuesTestCase(ConfigFilesTestCase):

    BASIC_CONF = {
        'runners': {
            'inline': {
                'base_tmp_dir': '/tmp',
                'cmdenv': {
                    'A_PATH': 'A',
                    'SOMETHING': 'X',
                },
                'hadoop_extra_args': [
                    'thing1',
                ],
                'hadoop_streaming_jar': 'monkey.jar',
                'jobconf': {
                    'lorax_speaks_for': 'trees',
                },
                'python_bin': 'py3k',
                'setup_scripts': ['/myscript.py'],
            }
        }
    }

    def larger_conf(self):
        return {
            'include': os.path.join(self.tmp_dir, 'mrjob.conf'),
            'runners': {
                'inline': {
                    'base_tmp_dir': '/var/tmp',
                    'bootstrap_mrjob': False,
                    'cmdenv': {
                        'A_PATH': 'B',
                        'SOMETHING': 'Y',
                        'SOMETHING_ELSE': 'Z',
                    },
                    'hadoop_extra_args': [
                        'thing2',
                    ],
                    'hadoop_streaming_jar': 'banana.jar',
                    'jobconf': {
                        'lorax_speaks_for': 'mazda',
                        'dr_seuss_is': 'awesome',
                    },
                    'python_bin': 'py4k',
                    'setup_scripts': ['/yourscript.py'],
                }
            }
        }

    def setUp(self):
        super(MultipleConfigFilesValuesTestCase, self).setUp()
        self.opts_1 = self.opts_for_conf('mrjob.conf',
                                         self.BASIC_CONF)
        self.opts_2 = self.opts_for_conf('mrjob.larger.conf',
                                         self.larger_conf())

    def test_combine_cmds(self):
        self.assertEqual(self.opts_1['python_bin'], ['py3k'])
        self.assertEqual(self.opts_2['python_bin'], ['py4k'])

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
        self.assertEqual(self.opts_1['hadoop_extra_args'], ['thing1'])
        self.assertEqual(self.opts_2['hadoop_extra_args'],
                         ['thing1', 'thing2'])

    def test_combine_paths(self):
        self.assertEqual(self.opts_1['base_tmp_dir'], '/tmp')
        self.assertEqual(self.opts_2['base_tmp_dir'], '/var/tmp')

    def test_combine_path_lists(self):
        self.assertEqual(self.opts_1['setup_scripts'], ['/myscript.py'])
        self.assertEqual(self.opts_2['setup_scripts'],
                         ['/myscript.py', '/yourscript.py'])

    def test_combine_values(self):
        self.assertEqual(self.opts_1['hadoop_streaming_jar'], 'monkey.jar')
        self.assertEqual(self.opts_2['hadoop_streaming_jar'], 'banana.jar')


class MultipleConfigFilesMachineryTestCase(ConfigFilesTestCase):

    def test_recurse(self):
        path = os.path.join(self.tmp_dir, 'LOL.conf')
        recurse_conf = dict(include=path)
        with open(path, 'w') as f:
            dump_mrjob_conf(recurse_conf, f)

        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.conf', stderr)
            InlineMRJobRunner(conf_path=path)
            self.assertIn('%s tries to recursively include %s!' % (path, path),
                          stderr.getvalue())

    def test_empty_runner_error(self):
        conf = dict(runner=dict(local=dict(base_tmp_dir='/tmp')))
        path = self.save_conf('basic', conf)

        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.conf', stderr)
            InlineMRJobRunner(conf_path=path)
            self.assertIn(
                "no configs for runner type 'inline' in %s" % path,
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
