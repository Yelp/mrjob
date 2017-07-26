# Copyright 2012-2013 Yelp
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
"""Test OptionStore's functionality"""
import os
from tempfile import mkdtemp
from shutil import rmtree
from unittest import TestCase
from unittest import skipIf

import mrjob.conf
import mrjob.hadoop
from mrjob.conf import ClearedValue
from mrjob.conf import dump_mrjob_conf
from mrjob.py2 import StringIO
from mrjob.inline import InlineMRJobRunner
from mrjob.util import log_to_stream

from tests.quiet import logger_disabled
from tests.quiet import no_handlers_for_logger
from tests.sandbox import SandboxedTestCase


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
                'python_bin': 'py3k',
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
                    'python_bin': 'py4k',
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
