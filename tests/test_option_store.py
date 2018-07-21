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

try:
    import boto
    import boto.emr
    import boto.emr.connection
    import boto.exception
    boto  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    boto = None

import mrjob.conf
import mrjob.hadoop
from mrjob.conf import ClearedValue
from mrjob.conf import dump_mrjob_conf
from mrjob.dataproc import DataprocRunnerOptionStore
from mrjob.emr import EMRRunnerOptionStore
from mrjob.hadoop import HadoopRunnerOptionStore
from mrjob.py2 import StringIO
from mrjob.runner import RunnerOptionStore
from mrjob.sim import SimRunnerOptionStore
from mrjob.util import log_to_stream

from tests.quiet import logger_disabled
from tests.quiet import no_handlers_for_logger


class TempdirTestCase(TestCase):
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
                'setup': [
                    ['thing1'],
                ],
                'setup_scripts': ['/myscript.py'],
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
                    'setup': [
                        ['thing2'],
                    ],
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
        self.assertEqual(self.opts_1['setup'], [['thing1']])
        self.assertEqual(self.opts_2['setup'],
                         [['thing1'], ['thing2']])

    def test_combine_paths(self):
        self.assertEqual(self.opts_1['local_tmp_dir'], '/tmp')
        self.assertEqual(self.opts_2['local_tmp_dir'], '/var/tmp')

    def test_combine_path_lists(self):
        self.assertEqual(self.opts_1['setup_scripts'], ['/myscript.py'])
        self.assertEqual(self.opts_2['setup_scripts'],
                         ['/myscript.py', '/yourscript.py'])

    def test_combine_values(self):
        self.assertEqual(self.opts_1['label'], 'organic')
        self.assertEqual(self.opts_2['label'], 'usda_organic')


class MultipleConfigFilesMachineryTestCase(ConfigFilesTestCase):

    def test_empty_runner_error(self):
        conf = dict(runner=dict(local=dict(local_tmp_dir='/tmp')))
        path = self.save_conf('basic', conf)

        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.runner', stderr)
            RunnerOptionStore('inline', {}, [path])
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
            log_to_stream('mrjob.runner', stderr)
            RunnerOptionStore('inline', {}, [path])
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
        opts = RunnerOptionStore('inline', {}, [path_left, path_right])
        self.assertEqual(opts['jobconf'],
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
        self.base_opts = RunnerOptionStore('inline', {}, [self.base_conf_path])

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
                'inline', {'local_tmp_dir': '/var/tmp', 'foo': 'bar'}, [])
            self.assertEqual(opts['local_tmp_dir'], '/var/tmp')
            self.assertNotIn('bar', opts)


class DeprecatedAliasesTestCase(ConfigFilesTestCase):

    def test_runner_option_store(self):
        stderr = StringIO()
        with no_handlers_for_logger('mrjob.conf'):
            log_to_stream('mrjob.conf', stderr)
            opts = RunnerOptionStore(
                'inline', dict(base_tmp_dir='/scratch'), [])

            self.assertEqual(opts['local_tmp_dir'], '/scratch')
            self.assertNotIn('base_tmp_dir', opts)
            self.assertIn('Deprecated option base_tmp_dir has been renamed'
                          ' to local_tmp_dir', stderr.getvalue())

    def test_hadoop_runner_option_store(self):
        stderr = StringIO()
        with no_handlers_for_logger('mrjob.conf'):
            log_to_stream('mrjob.conf', stderr)

            opts = HadoopRunnerOptionStore(
                'hadoop',
                dict(base_tmp_dir='/scratch',
                     hdfs_scratch_dir='hdfs:///scratch'),
                [])

            self.assertEqual(opts['local_tmp_dir'], '/scratch')
            self.assertNotIn('base_tmp_dir', opts)
            self.assertIn('Deprecated option base_tmp_dir has been renamed'
                          ' to local_tmp_dir', stderr.getvalue())

            self.assertEqual(opts['hadoop_tmp_dir'], 'hdfs:///scratch')
            self.assertNotIn('hdfs_scratch_dir', opts)
            self.assertIn('Deprecated option hdfs_scratch_dir has been renamed'
                          ' to hadoop_tmp_dir', stderr.getvalue())

    def test_emr_runner_option_store(self):
        stderr = StringIO()
        with no_handlers_for_logger('mrjob.conf'):
            log_to_stream('mrjob.conf', stderr)

            opts = EMRRunnerOptionStore(
                'emr',
                dict(base_tmp_dir='/scratch',
                     emr_job_flow_id='j-CLUSTERID',
                     emr_job_flow_pool_name='liver',
                     pool_emr_job_flows=True,
                     s3_scratch_uri='s3://bucket/walrus'),
                [])

            self.assertEqual(opts['cluster_id'], 'j-CLUSTERID')
            self.assertNotIn('emr_job_flow_id', opts)
            self.assertIn('Deprecated option emr_job_flow_id has been renamed'
                          ' to cluster_id', stderr.getvalue())

            self.assertEqual(opts['local_tmp_dir'], '/scratch')
            self.assertNotIn('base_tmp_dir', opts)
            self.assertIn('Deprecated option base_tmp_dir has been renamed'
                          ' to local_tmp_dir', stderr.getvalue())

            self.assertEqual(opts['pool_clusters'], True)
            self.assertNotIn('pool_emr_job_flows', opts)
            self.assertIn('Deprecated option pool_emr_job_flows has been'
                          ' renamed to pool_clusters', stderr.getvalue())

            self.assertEqual(opts['pool_name'], 'liver')
            self.assertNotIn('emr_job_flow_pool_name', opts)
            self.assertIn('Deprecated option emr_job_flow_pool_name has been'
                          ' renamed to pool_name', stderr.getvalue())

            self.assertEqual(opts['cloud_tmp_dir'], 's3://bucket/walrus')
            self.assertNotIn('s3_scratch_uri', opts)
            self.assertIn('Deprecated option s3_scratch_uri has been renamed'
                          ' to cloud_tmp_dir', stderr.getvalue())

    def test_cleanup_options(self):
        stderr = StringIO()
        with no_handlers_for_logger('mrjob.runner'):
            log_to_stream('mrjob.runner', stderr)
            opts = RunnerOptionStore(
                'inline',
                dict(cleanup=['LOCAL_SCRATCH', 'REMOTE_SCRATCH'],
                     cleanup_on_failure=['JOB_FLOW', 'SCRATCH']),
                [])

            self.assertEqual(opts['cleanup'], ['LOCAL_TMP', 'CLOUD_TMP'])
            self.assertIn(
                'Deprecated cleanup option LOCAL_SCRATCH has been renamed'
                ' to LOCAL_TMP', stderr.getvalue())
            self.assertIn(
                'Deprecated cleanup option REMOTE_SCRATCH has been renamed'
                ' to CLOUD_TMP', stderr.getvalue())

            self.assertEqual(opts['cleanup_on_failure'], ['CLUSTER', 'TMP'])
            self.assertIn(
                'Deprecated cleanup_on_failure option JOB_FLOW has been'
                ' renamed to CLUSTER', stderr.getvalue())
            self.assertIn(
                'Deprecated cleanup_on_failure option SCRATCH has been renamed'
                ' to TMP', stderr.getvalue())


class OptionStoreSanityCheckTestCase(TestCase):

    # catch typos etc. in option store definition. See #1322

    def assert_option_store_is_sane(self, _class):
        self.assertLessEqual(set(_class.COMBINERS),
                             _class.ALLOWED_KEYS)
        self.assertLessEqual(set(_class.DEPRECATED_ALIASES.values()),
                             _class.ALLOWED_KEYS)

    def test_runner_option_store_is_sane(self):
        self.assert_option_store_is_sane(RunnerOptionStore)

    def test_dataproc_runner_option_store_is_sane(self):
        self.assert_option_store_is_sane(DataprocRunnerOptionStore)

    def test_emr_runner_option_store_is_sane(self):
        self.assert_option_store_is_sane(EMRRunnerOptionStore)

    def test_hadoop_runner_option_store_is_sane(self):
        self.assert_option_store_is_sane(HadoopRunnerOptionStore)

    def test_sim_runner_option_store_is_sane(self):
        self.assert_option_store_is_sane(SimRunnerOptionStore)


class OptionStoreDebugPrintoutTestCase(ConfigFilesTestCase):

    def get_debug_printout(self, opt_store_class, alias, opts):
        stderr = StringIO()

        with no_handlers_for_logger():
            log_to_stream('mrjob.runner', stderr, debug=True)

            # debug printout happens in constructor
            opt_store_class(alias, opts, [])

        return stderr.getvalue()

    def test_option_debug_printout(self):
        printout = self.get_debug_printout(
            RunnerOptionStore, 'inline', dict(owner='dave'))

        self.assertIn("'owner'", printout)
        self.assertIn("'dave'", printout)

    def test_non_obfuscated_option_on_emr(self):
        printout = self.get_debug_printout(
            EMRRunnerOptionStore, 'emr', dict(owner='dave'))

        self.assertIn("'owner'", printout)
        self.assertIn("'dave'", printout)

    def test_aws_access_key_id(self):
        printout = self.get_debug_printout(
            EMRRunnerOptionStore, 'emr', dict(
                aws_access_key_id='AKIATOPQUALITYSALESEVENT'))

        self.assertIn("'aws_access_key_id'", printout)
        self.assertIn("'...VENT'", printout)

    def test_aws_access_key_id_with_wrong_type(self):
        printout = self.get_debug_printout(
            EMRRunnerOptionStore, 'emr', dict(
                aws_access_key_id=['AKIATOPQUALITYSALESEVENT']))

        self.assertIn("'aws_access_key_id'", printout)
        self.assertNotIn('VENT', printout)
        self.assertIn("'...'", printout)

    def test_aws_secret_access_key(self):
        printout = self.get_debug_printout(
            EMRRunnerOptionStore, 'emr', dict(
                aws_secret_access_key='PASSWORD'))

        self.assertIn("'aws_secret_access_key'", printout)
        self.assertNotIn('PASSWORD', printout)
        self.assertIn("'...'", printout)

    def test_aws_session_token(self):
        printout = self.get_debug_printout(
            EMRRunnerOptionStore, 'emr', dict(
                aws_session_token='TOKEN'))

        self.assertIn("'aws_session_token'", printout)
        self.assertNotIn('TOKEN', printout)
        self.assertIn("'...'", printout)

    def test_dont_obfuscate_empty_opts(self):
        printout = self.get_debug_printout(
            EMRRunnerOptionStore, 'emr', {})

        self.assertNotIn("'...'", printout)
        self.assertIn("'aws_access_key_id'", printout)
        self.assertIn("'aws_secret_access_key'", printout)
        self.assertIn("'aws_session_token'", printout)
