# Copyright 2012 Yelp
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
"""Test OptionStore's functionality"""

from __future__ import with_statement

import os

from tempfile import mkdtemp
from shutil import rmtree
from StringIO import StringIO

try:
    import unittest2 as unittest
    unittest  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import unittest

try:
    import boto
    import boto.emr
    import boto.emr.connection
    import boto.exception
    boto  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    boto = None

from mrjob.conf import dump_mrjob_conf
from mrjob.runner import RunnerOptionStore
from mrjob.util import log_to_stream
from tests.quiet import logger_disabled
from tests.quiet import no_handlers_for_logger
from tests.sandbox import EmptyMrjobConfTestCase


class TempdirTestCase(unittest.TestCase):
    """Patch mrjob.conf, create a temp directory, and save the environment for
    each test
    """

    def setUp(self):
        super(TempdirTestCase, self).setUp()

        # tmp dir
        self.tmp_dir = mkdtemp()
        self.addCleanup(rmtree, self.tmp_dir)

        # environment
        self._old_environ = os.environ.copy()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._old_environ)

    def makedirs(self, path):
        abs_path = os.path.join(self.tmp_dir, path)
        if not os.path.isdir(abs_path):
            os.makedirs(abs_path)
        return abs_path

    def makefile(self, path, contents):
        self.makedirs(os.path.split(path)[0])
        abs_path = os.path.join(self.tmp_dir, path)
        with open(abs_path, 'w') as f:
            f.write(contents)
        return abs_path

    def abs_paths(self, *paths):
        return [os.path.join(self.tmp_dir, path) for path in paths]


class RunnerOptionStoreTestCase(EmptyMrjobConfTestCase):

    def _assert_interp(self, val, **kwargs):
        opts = RunnerOptionStore('inline', kwargs, [])
        self.assertEqual(opts['interpreter'], val)

    def test_interpreter_fallback(self):
        self._assert_interp(['python'])

    def test_interpreter_fallback_2(self):
        self._assert_interp(['python', '-v'], python_bin=['python', '-v'])

    def test_interpreter(self):
        self._assert_interp(['ruby'], interpreter=['ruby'])


class ConfigFilesTestCase(TempdirTestCase):

    def save_conf(self, name, conf):
        conf_path = os.path.join(self.tmp_dir, name)
        with open(conf_path, 'w') as f:
            dump_mrjob_conf(conf, f)
        return conf_path

    def opts_for_conf(self, name, conf):
        conf_path = self.save_conf(name, conf)
        return RunnerOptionStore('inline', {}, [conf_path])


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
            RunnerOptionStore('inline', {}, [path])
            self.assertIn('%s tries to recursively include %s!' % (path, path),
                          stderr.getvalue())

    def test_empty_runner_error(self):
        conf = dict(runner=dict(local=dict(base_tmp_dir='/tmp')))
        path = self.save_conf('basic', conf)

        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.runner', stderr)
            RunnerOptionStore('inline', {}, [path])
            self.assertEqual(
                "No configs specified for inline runner\n",
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
        opts = RunnerOptionStore('inline', {}, [path_left, path_right])
        self.assertEqual(opts['jobconf'],
                         dict(from_left=1, from_both=2, from_right=2))


class TestExtraKwargs(ConfigFilesTestCase):

    CONFIG = {'runners': {'inline': {
        'qux': 'quux',
        'setup_cmds': ['echo foo']}}}

    def setUp(self):
        super(TestExtraKwargs, self).setUp()
        self.path = self.save_conf('config', self.CONFIG)

    def test_extra_kwargs_in_mrjob_conf_okay(self):
        with logger_disabled('mrjob.runner'):
            opts = RunnerOptionStore('inline', {}, [self.path])
            self.assertEqual(opts['setup_cmds'], ['echo foo'])
            self.assertNotIn('qux', opts)

    def test_extra_kwargs_passed_in_directly_okay(self):
        with logger_disabled('mrjob.runner'):
            opts = RunnerOptionStore(
                'inline', {'base_tmp_dir': '/var/tmp', 'foo': 'bar'}, [])
            self.assertEqual(opts['base_tmp_dir'], '/var/tmp')
            self.assertNotIn('bar', opts)
