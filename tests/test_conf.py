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

"""Test configuration parsing and option combining"""

from __future__ import with_statement

import logging
import os
import shutil
from StringIO import StringIO
import tempfile

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import mrjob.conf
from mrjob.conf import combine_cmd_lists
from mrjob.conf import combine_cmds
from mrjob.conf import combine_dicts
from mrjob.conf import combine_envs
from mrjob.conf import combine_lists
from mrjob.conf import combine_local_envs
from mrjob.conf import combine_opts
from mrjob.conf import combine_path_lists
from mrjob.conf import combine_paths
from mrjob.conf import combine_values
from mrjob.conf import dump_mrjob_conf
from mrjob.conf import expand_path
from mrjob.conf import find_mrjob_conf
from mrjob.conf import load_mrjob_conf
from mrjob.conf import load_opts_from_mrjob_conf
from tests.quiet import logger_disabled
from tests.quiet import no_handlers_for_logger


class MRJobConfTestCase(unittest.TestCase):

    def setUp(self):
        self.patch_os_path_exists()
        self.setup_tmp_dir()
        self.blank_out_environment()

    def tearDown(self):
        self.restore_environment()
        self.rm_tmp_dir()
        self.unpatch_os_path_exists()

    def patch_os_path_exists(self):
        self._existing_paths = None
        self._real_os_path_exists = os.path.exists

        def os_path_exists_wrapper(path):
            if self._existing_paths is None:
                return self._real_os_path_exists(path)
            else:
                return path in self._existing_paths

        os.path.exists = os_path_exists_wrapper

    def unpatch_os_path_exists(self):
        os.path.exists = self._real_os_path_exists

    def setup_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def blank_out_environment(self):
        self._old_environ = os.environ.copy()
        # don't do os.environ = {}! This won't actually set environment
        # variables; it just monkey-patches os.environ
        os.environ.clear()

    def restore_environment(self):
        os.environ.clear()
        os.environ.update(self._old_environ)


class MRJobBasicConfTestCase(MRJobConfTestCase):

    def test_no_mrjob_conf(self):
        self._existing_paths = []
        self.assertEqual(find_mrjob_conf(), None)

    def test_etc_mrjob_conf(self):
        self._existing_paths = ['/etc/mrjob.conf']
        self.assertEqual(find_mrjob_conf(), '/etc/mrjob.conf')

    def test_mrjob_in_home_dir(self):
        os.environ['HOME'] = self.tmp_dir
        dot_mrjob_path = os.path.join(self.tmp_dir, '.mrjob.conf')
        open(dot_mrjob_path, 'w').close()
        self.assertEqual(find_mrjob_conf(), dot_mrjob_path)

    def test_mrjob_conf_in_home_dir(self):
        # ~/mrjob.conf is not a place we look (should be ~/.mrjob)
        os.environ['HOME'] = self.tmp_dir
        mrjob_conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')
        self._existing_paths = [mrjob_conf_path]
        self.assertEqual(find_mrjob_conf(), None)

    def test_precedence(self):
        os.environ['HOME'] = '/home/foo'
        os.environ['PYTHONPATH'] = '/py1:/py2'
        self._existing_paths = set()

        self.assertEqual(find_mrjob_conf(), None)

        self._existing_paths.add('/etc/mrjob.conf')
        self.assertEqual(find_mrjob_conf(), '/etc/mrjob.conf')

        self._existing_paths.add('/home/foo/.mrjob.conf')
        self.assertEqual(find_mrjob_conf(), '/home/foo/.mrjob.conf')

        mrjob_conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')
        open(mrjob_conf_path, 'w').close()
        os.environ['MRJOB_CONF'] = mrjob_conf_path
        self._existing_paths.add(mrjob_conf_path)
        self.assertEqual(find_mrjob_conf(), mrjob_conf_path)

    def test_load_and_load_opts_use_find_mrjob_conf(self):
        os.environ['HOME'] = self.tmp_dir

        dot_mrjob_path = os.path.join(self.tmp_dir, '.mrjob.conf')
        with open(dot_mrjob_path, 'w') as f:
            f.write('{"runners": {"foo": {"bar": "baz"}}}')

        self.assertEqual(load_mrjob_conf(),
                         {'runners': {'foo': {'bar': 'baz'}}})
        self.assertEqual(load_opts_from_mrjob_conf('foo'), {'bar': 'baz'})

    def test_load_mrjob_conf_and_load_opts(self):
        conf_path = os.path.join(self.tmp_dir, 'mrjob.conf.2')
        with open(conf_path, 'w') as f:
            f.write('{"runners": {"foo": {"qux": "quux"}}}')

        self.assertEqual(load_mrjob_conf(conf_path=conf_path),
                         {'runners': {'foo': {'qux': 'quux'}}})
        self.assertEqual(load_opts_from_mrjob_conf('foo', conf_path=conf_path),
                         {'qux': 'quux'})
        # test missing options
        with logger_disabled('mrjob.conf'):
            self.assertEqual(
                load_opts_from_mrjob_conf('bar', conf_path=conf_path), {})

    def test_round_trip(self):
        conf = {'runners': {'foo': {'qux': 'quux'}}}
        conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')

        dump_mrjob_conf(conf, open(conf_path, 'w'))
        self.assertEqual(conf, load_mrjob_conf(conf_path=conf_path))


class MRJobConfDeprecatedLocationTestCase(MRJobConfTestCase):

    def _log_to_buffer(self):
        buf = StringIO()
        log = logging.getLogger('mrjob.conf')
        log.addHandler(logging.StreamHandler(buf))
        log.setLevel(logging.WARNING)
        return buf

    def test_mrjob_conf_in_python_path(self):
        os.environ['PYTHONPATH'] = self.tmp_dir
        mrjob_conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')
        open(mrjob_conf_path, 'w').close()
        self._existing_paths = [mrjob_conf_path]

        with no_handlers_for_logger():
            buf = self._log_to_buffer()
            self.assertEqual(find_mrjob_conf(), mrjob_conf_path)
            self.assertIn('This config path is deprecated', buf.getvalue())

    def test_precedence_deprecated(self):
        os.environ['HOME'] = '/home/foo'
        os.environ['PYTHONPATH'] = '/py1:/py2'
        self._existing_paths = set()

        self.assertEqual(find_mrjob_conf(), None)

        self._existing_paths.add('/etc/mrjob.conf')
        self.assertEqual(find_mrjob_conf(), '/etc/mrjob.conf')

        self._existing_paths.add('/py2/mrjob.conf')
        with no_handlers_for_logger():
            buf = self._log_to_buffer()
            self.assertEqual(find_mrjob_conf(), '/py2/mrjob.conf')
            self.assertIn('This config path is deprecated', buf.getvalue())

        self._existing_paths.add('/py1/mrjob.conf')
        with no_handlers_for_logger():
            buf = self._log_to_buffer()
            self.assertEqual(find_mrjob_conf(), '/py1/mrjob.conf')
            self.assertIn('This config path is deprecated', buf.getvalue())

        self._existing_paths.add('/home/foo/.mrjob')
        with no_handlers_for_logger():
            buf = self._log_to_buffer()
            self.assertEqual(find_mrjob_conf(), '/home/foo/.mrjob')
            self.assertIn('This config path is deprecated', buf.getvalue())

        mrjob_conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')
        open(mrjob_conf_path, 'w').close()
        os.environ['MRJOB_CONF'] = mrjob_conf_path
        self._existing_paths.add(mrjob_conf_path)
        self.assertEqual(find_mrjob_conf(), mrjob_conf_path)


class MRJobConfNoYAMLTestCase(MRJobConfTestCase):

    def setUp(self):
        super(MRJobConfNoYAMLTestCase, self).setUp()
        self.blank_out_yaml()

    def tearDown(self):
        self.restore_yaml()
        super(MRJobConfNoYAMLTestCase, self).tearDown()

    def blank_out_yaml(self):
        self._real_yaml = mrjob.conf.yaml
        mrjob.conf.yaml = None

    def restore_yaml(self):
        mrjob.conf.yaml = self._real_yaml

    def test_using_json_and_not_yaml(self):
        conf = {'runners': {'foo': {'qux': 'quux'}}}
        conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')

        dump_mrjob_conf(conf, open(conf_path, 'w'))
        contents = open(conf_path).read()

        self.assertEqual(contents.replace(' ', '').replace('\n', ''),
                         '{"runners":{"foo":{"qux":"quux"}}}')


class CombineValuesTestCase(unittest.TestCase):

    def test_empty(self):
        self.assertEqual(combine_values(), None)

    def test_picks_last_value(self):
        self.assertEqual(combine_values(1, 2, 3), 3)

    def test_all_None(self):
        self.assertEqual(combine_values(None, None, None), None)

    def test_skips_None(self):
        self.assertEqual(combine_values(None, 'one'), 'one')
        self.assertEqual(combine_values('one', None), 'one')
        self.assertEqual(combine_values(None, None, 'one', None), 'one')

    def test_falseish_values(self):
        # everything but None is a legit value
        self.assertEqual(combine_values(True, False), False)
        self.assertEqual(combine_values(1, 0), 0)
        self.assertEqual(combine_values('full', ''), '')
        self.assertEqual(combine_values([1, 2, 3], []), [])
        self.assertEqual(combine_values((1, 2, 3), ()), ())
        self.assertEqual(combine_values({'a': 'b'}, {}), {})
        self.assertEqual(combine_values(set([1]), set()), set())


class CombineDictsTestCase(unittest.TestCase):

    def test_empty(self):
        self.assertEqual(combine_dicts(), {})

    def test_later_values_take_precedence(self):
        self.assertEqual(
            combine_dicts({'TMPDIR': '/tmp', 'HOME': '/home/dave'},
                          {'TMPDIR': '/var/tmp'}),
            {'TMPDIR': '/var/tmp', 'HOME': '/home/dave'})

    def test_skip_None(self):
        self.assertEqual(combine_envs(None, {'USER': 'dave'}, None,
                                  {'TERM': 'xterm'}, None),
                     {'USER': 'dave', 'TERM': 'xterm'})

    def test_no_special_logic_for_paths(self):
        self.assertEqual(combine_dicts(
            {'PATH': '/bin:/usr/bin',
             'PYTHONPATH': '/usr/lib/python/site-packages',
             'PS1': '> '},
            {'PATH': '/home/dave/bin',
             'PYTHONPATH': '/home/dave/python',
             'CLASSPATH': '/home/dave/java',
             'PS1': '\w> '}),
            {'PATH': '/home/dave/bin',
             'PYTHONPATH': '/home/dave/python',
             'CLASSPATH': '/home/dave/java',
             'PS1': '\w> '})


class CombineCmdsTestCase(unittest.TestCase):

    def test_empty(self):
        self.assertEqual(combine_cmds(), None)

    def test_picks_last_value(self):
        self.assertEqual(combine_cmds(['sort'], ['grep'], ['cat']), ['cat'])

    def test_all_None(self):
        self.assertEqual(combine_cmds(None, None, None), None)

    def test_skips_None(self):
        self.assertEqual(combine_values(None, ['cat']), ['cat'])
        self.assertEqual(combine_values(['cat'], None), ['cat'])
        self.assertEqual(combine_values(None, None, ['cat'], None), ['cat'])

    def test_parse_string(self):
        self.assertEqual(combine_cmds('sort', 'grep', 'cat'), ['cat'])
        self.assertEqual(combine_cmds(['python'], 'python -S'),
                         ['python', '-S'])

    def test_parse_empty_string(self):
        self.assertEqual(combine_cmds(''), [])

    def test_convert_to_list(self):
        self.assertEqual(combine_cmds('sort', ('grep', '-E')), ['grep', '-E'])


class CombineCmdsListsCase(unittest.TestCase):

    def test_empty(self):
        self.assertEqual(combine_cmd_lists(), [])

    def test_concatenation(self):
        self.assertEqual(
            combine_cmd_lists(
                [['echo', 'foo']], None, (['mkdir', 'bar'], ['rmdir', 'bar'])),
            [['echo', 'foo'], ['mkdir', 'bar'], ['rmdir', 'bar']])

    def test_conversion(self):
        self.assertEqual(
            combine_cmd_lists(
                ['echo "Hello World!"'], None, [('mkdir', '/tmp/baz')]),
            [['echo', 'Hello World!'], ['mkdir', '/tmp/baz']])


class CombineEnvsTestCase(unittest.TestCase):

    def test_empty(self):
        self.assertEqual(combine_envs(), {})

    def test_later_values_take_precedence(self):
        self.assertEqual(
            combine_envs({'TMPDIR': '/tmp', 'HOME': '/home/dave'},
                         {'TMPDIR': '/var/tmp'}),
            {'TMPDIR': '/var/tmp', 'HOME': '/home/dave'})

    def test_skip_None(self):
        self.assertEqual(combine_envs(None, {'USER': 'dave'}, None,
                                      {'TERM': 'xterm'}, None),
                     {'USER': 'dave', 'TERM': 'xterm'})

    def test_paths(self):
        self.assertEqual(combine_envs(
            {'PATH': '/bin:/usr/bin',
             'PYTHONPATH': '/usr/lib/python/site-packages',
             'PS1': '> '},
            {'PATH': '/home/dave/bin',
             'PYTHONPATH': '/home/dave/python',
             'CLASSPATH': '/home/dave/java',
             'PS1': '\w> '}),
            {'PATH': '/home/dave/bin:/bin:/usr/bin',
             'PYTHONPATH': '/home/dave/python:/usr/lib/python/site-packages',
             'CLASSPATH': '/home/dave/java',
             'PS1': '\w> '})


class CombineLocalEnvsTestCase(unittest.TestCase):

    def setUp(self):
        self.set_os_pathsep()

    def tearDown(self):
        self.restore_os_pathsep()

    def set_os_pathsep(self):
        self._real_os_pathsep = os.pathsep
        os.pathsep = ';'

    def restore_os_pathsep(self):
        os.pathsep = self._real_os_pathsep

    def test_paths(self):
        self.assertEqual(combine_local_envs(
            {'PATH': '/bin:/usr/bin',
             'PYTHONPATH': '/usr/lib/python/site-packages',
             'PS1': '> '},
            {'PATH': '/home/dave/bin',
             'PYTHONPATH': '/home/dave/python',
             'CLASSPATH': '/home/dave/java',
             'PS1': '\w> '}),
            {'PATH': '/home/dave/bin;/bin:/usr/bin',
             'PYTHONPATH': '/home/dave/python;/usr/lib/python/site-packages',
             'CLASSPATH': '/home/dave/java',
             'PS1': '\w> '})


class CombineListsTestCase(unittest.TestCase):

    def test_empty(self):
        self.assertEqual(combine_lists(), [])

    def test_concatenation(self):
        self.assertEqual(combine_lists([1, 2], None, (3, 4)), [1, 2, 3, 4])


class CombineOptsTestCase(unittest.TestCase):

    def test_empty(self):
        self.assertEqual(combine_opts(combiners={}), {})

    def test_combine_opts(self):
        combiners = {
            'foo': combine_lists,
        }
        self.assertEqual(
            combine_opts(combiners,
                         {'foo': ['bar'], 'baz': ['qux']},
                         {'foo': ['baz'], 'baz': ['quux'], 'bar': 'garply'},
                         None,
                         {}),
            # "baz" doesn't use the list combiner, so ['qux'] is overwritten
            {'foo': ['bar', 'baz'], 'baz': ['quux'], 'bar': 'garply'})


class CombineAndExpandPathsTestCase(unittest.TestCase):

    def setUp(self):
        self.setup_tmp_dir()

    @classmethod
    def setUpClass(cls):
        cls.set_environment_vars()

    def tearDown(self):
        self.rm_tmp_dir()

    @classmethod
    def tearDownClass(cls):
        cls.restore_environment_vars()

    @classmethod
    def set_environment_vars(self):
        self._old_environ = os.environ.copy()
        os.environ.clear()
        os.environ.update({
            'HOME': '/home/foo',
            'USER': 'foo',
            'BAR': 'bar',
        })

    @classmethod
    def restore_environment_vars(self):
        os.environ.clear()
        os.environ.update(self._old_environ)

    def setup_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_expand_paths_empty(self):
        self.assertEqual(expand_path(None), None)
        self.assertEqual(expand_path(''), '')

    def test_expand_paths(self):
        self.assertEqual(expand_path('$BAR'), 'bar')
        self.assertEqual(expand_path('~/$BAR'), '/home/foo/bar')
        self.assertEqual(expand_path('~/$BARn'), '/home/foo/$BARn')
        self.assertEqual(expand_path('~/${BAR}n'), '/home/foo/barn')

    def test_combine_paths_empty(self):
        self.assertEqual(combine_paths(), None)

    def test_combine_paths(self):
        self.assertEqual(combine_paths('~/tmp', '/tmp/$USER', None),
                     '/tmp/foo')
        self.assertEqual(combine_paths('~/tmp', '/tmp/$USER', ''), '')

    def test_combine_path_lists_empty(self):
        self.assertEqual(combine_path_lists(), [])

    def test_combine_path_lists(self):
        self.assertEqual(
            combine_path_lists(['~/tmp'], [], ['/dev/null', '/tmp/$USER']),
            ['/home/foo/tmp', '/dev/null', '/tmp/foo'])

    def test_globbing(self):
        foo_path = os.path.join(self.tmp_dir, 'foo')
        bar_path = os.path.join(self.tmp_dir, 'bar')
        open(foo_path, 'w').close()
        open(bar_path, 'w').close()

        # make sure that paths that don't match anything on the local
        # filesystem are still preserved.
        self.assertEqual(
            combine_path_lists([os.path.join(self.tmp_dir, '*'),
                                foo_path],
                               [os.path.join(self.tmp_dir, 'q*'),
                                's3://walrus/foo'],
                               None),
            [bar_path, foo_path, foo_path,
             os.path.join(self.tmp_dir, 'q*'),
             's3://walrus/foo'])
