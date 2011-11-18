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

from testify import TestCase
from testify import assert_equal
from testify import assert_in
from testify import assert_not_in
from testify import class_setup
from testify import class_teardown
from testify import setup
from testify import teardown

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
from mrjob.util import log_to_stream
from tests.quiet import logger_disabled
from tests.quiet import no_handlers_for_logger


class MRJobConfTestCase(TestCase):

    @setup
    def patch_os_path_exists(self):
        self._existing_paths = None
        self._real_os_path_exists = os.path.exists

        def os_path_exists_wrapper(path):
            if self._existing_paths is None:
                return self._real_os_path_exists(path)
            else:
                return path in self._existing_paths

        os.path.exists = os_path_exists_wrapper

    @teardown
    def unpatch_os_path_exists(self):
        os.path.exists = self._real_os_path_exists

    @setup
    def setup_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    @teardown
    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

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


class MRJobBasicConfTestCase(MRJobConfTestCase):

    def test_no_mrjob_conf(self):
        self._existing_paths = []
        assert_equal(find_mrjob_conf(), None)

    def test_etc_mrjob_conf(self):
        self._existing_paths = ['/etc/mrjob.conf']
        assert_equal(find_mrjob_conf(), '/etc/mrjob.conf')

    def test_mrjob_in_home_dir(self):
        os.environ['HOME'] = self.tmp_dir
        dot_mrjob_path = os.path.join(self.tmp_dir, '.mrjob.conf')
        open(dot_mrjob_path, 'w').close()
        assert_equal(find_mrjob_conf(), dot_mrjob_path)

    def test_mrjob_conf_in_home_dir(self):
        # ~/mrjob.conf is not a place we look (should be ~/.mrjob)
        os.environ['HOME'] = self.tmp_dir
        mrjob_conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')
        self._existing_paths = [mrjob_conf_path]
        assert_equal(find_mrjob_conf(), None)

    def test_precedence(self):
        os.environ['HOME'] = '/home/foo'
        os.environ['PYTHONPATH'] = '/py1:/py2'
        self._existing_paths = set()

        assert_equal(find_mrjob_conf(), None)

        self._existing_paths.add('/etc/mrjob.conf')
        assert_equal(find_mrjob_conf(), '/etc/mrjob.conf')

        self._existing_paths.add('/home/foo/.mrjob.conf')
        assert_equal(find_mrjob_conf(), '/home/foo/.mrjob.conf')

        mrjob_conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')
        open(mrjob_conf_path, 'w').close()
        os.environ['MRJOB_CONF'] = mrjob_conf_path
        self._existing_paths.add(mrjob_conf_path)
        assert_equal(find_mrjob_conf(), mrjob_conf_path)

    def test_load_and_load_opts_use_find_mrjob_conf(self):
        os.environ['HOME'] = self.tmp_dir

        dot_mrjob_path = os.path.join(self.tmp_dir, '.mrjob.conf')
        with open(dot_mrjob_path, 'w') as f:
            f.write('{"runners": {"foo": {"bar": "baz"}}}')

        assert_equal(load_mrjob_conf(),
                     {'runners': {'foo': {'bar': 'baz'}}})
        assert_equal(load_opts_from_mrjob_conf('foo'), {'bar': 'baz'})

    def test_load_mrjob_conf_and_load_opts(self):
        conf_path = os.path.join(self.tmp_dir, 'mrjob.conf.2')
        with open(conf_path, 'w') as f:
            f.write('{"runners": {"foo": {"qux": "quux"}}}')

        assert_equal(load_mrjob_conf(conf_path=conf_path),
                     {'runners': {'foo': {'qux': 'quux'}}})
        assert_equal(load_opts_from_mrjob_conf('foo', conf_path=conf_path),
                     {'qux': 'quux'})
        # test missing options
        with logger_disabled('mrjob.conf'):
            assert_equal(
                load_opts_from_mrjob_conf('bar', conf_path=conf_path), {})

    def test_round_trip(self):
        conf = {'runners': {'foo': {'qux': 'quux'}}}
        conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')

        dump_mrjob_conf(conf, open(conf_path, 'w'))
        assert_equal(conf, load_mrjob_conf(conf_path=conf_path))


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
            assert_equal(find_mrjob_conf(), mrjob_conf_path)
            assert_in('This config path is deprecated', buf.getvalue())

    def test_precedence_deprecated(self):
        os.environ['HOME'] = '/home/foo'
        os.environ['PYTHONPATH'] = '/py1:/py2'
        self._existing_paths = set()

        assert_equal(find_mrjob_conf(), None)

        self._existing_paths.add('/etc/mrjob.conf')
        assert_equal(find_mrjob_conf(), '/etc/mrjob.conf')

        self._existing_paths.add('/py2/mrjob.conf')
        with no_handlers_for_logger():
            buf = self._log_to_buffer()
            assert_equal(find_mrjob_conf(), '/py2/mrjob.conf')
            assert_in('This config path is deprecated', buf.getvalue())

        self._existing_paths.add('/py1/mrjob.conf')
        with no_handlers_for_logger():
            buf = self._log_to_buffer()
            assert_equal(find_mrjob_conf(), '/py1/mrjob.conf')
            assert_in('This config path is deprecated', buf.getvalue())

        self._existing_paths.add('/home/foo/.mrjob')
        with no_handlers_for_logger():
            buf = self._log_to_buffer()
            assert_equal(find_mrjob_conf(), '/home/foo/.mrjob')
            assert_in('This config path is deprecated', buf.getvalue())

        mrjob_conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')
        open(mrjob_conf_path, 'w').close()
        os.environ['MRJOB_CONF'] = mrjob_conf_path
        self._existing_paths.add(mrjob_conf_path)
        assert_equal(find_mrjob_conf(), mrjob_conf_path)


class MRJobConfNoYAMLTestCase(MRJobConfTestCase):

    @setup
    def blank_out_yaml(self):
        self._real_yaml = mrjob.conf.yaml
        mrjob.conf.yaml = None

    @teardown
    def restore_yaml(self):
        mrjob.conf.yaml = self._real_yaml

    def test_using_json_and_not_yaml(self):
        conf = {'runners': {'foo': {'qux': 'quux'}}}
        conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')

        dump_mrjob_conf(conf, open(conf_path, 'w'))
        contents = open(conf_path).read()

        assert_equal(contents.replace(' ', '').replace('\n', ''),
                     '{"runners":{"foo":{"qux":"quux"}}}')


class CombineValuesTestCase(TestCase):

    def test_empty(self):
        assert_equal(combine_values(), None)

    def test_picks_last_value(self):
        assert_equal(combine_values(1, 2, 3), 3)

    def test_all_None(self):
        assert_equal(combine_values(None, None, None), None)

    def test_skips_None(self):
        assert_equal(combine_values(None, 'one'), 'one')
        assert_equal(combine_values('one', None), 'one')
        assert_equal(combine_values(None, None, 'one', None), 'one')

    def test_falseish_values(self):
        # everything but None is a legit value
        assert_equal(combine_values(True, False), False)
        assert_equal(combine_values(1, 0), 0)
        assert_equal(combine_values('full', ''), '')
        assert_equal(combine_values([1, 2, 3], []), [])
        assert_equal(combine_values((1, 2, 3), ()), ())
        assert_equal(combine_values({'a': 'b'}, {}), {})
        assert_equal(combine_values(set([1]), set()), set())


class CombineDictsTestCase(TestCase):

    def test_empty(self):
        assert_equal(combine_dicts(), {})

    def test_later_values_take_precedence(self):
        assert_equal(
            combine_dicts({'TMPDIR': '/tmp', 'HOME': '/home/dave'},
                         {'TMPDIR': '/var/tmp'}),
            {'TMPDIR': '/var/tmp', 'HOME': '/home/dave'})

    def test_skip_None(self):
        assert_equal(combine_envs(None, {'USER': 'dave'}, None,
                                  {'TERM': 'xterm'}, None),
                     {'USER': 'dave', 'TERM': 'xterm'})

    def test_no_special_logic_for_paths(self):
        assert_equal(combine_dicts(
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


class CombineCmdsTestCase(TestCase):

    def test_empty(self):
        assert_equal(combine_cmds(), None)

    def test_picks_last_value(self):
        assert_equal(combine_cmds(['sort'], ['grep'], ['cat']), ['cat'])

    def test_all_None(self):
        assert_equal(combine_cmds(None, None, None), None)

    def test_skips_None(self):
        assert_equal(combine_values(None, ['cat']), ['cat'])
        assert_equal(combine_values(['cat'], None), ['cat'])
        assert_equal(combine_values(None, None, ['cat'], None), ['cat'])

    def test_parse_string(self):
        assert_equal(combine_cmds('sort', 'grep', 'cat'), ['cat'])
        assert_equal(combine_cmds(['python'], 'python -S'), ['python', '-S'])

    def test_parse_empty_string(self):
        assert_equal(combine_cmds(''), [])

    def test_convert_to_list(self):
        assert_equal(combine_cmds('sort', ('grep', '-E')), ['grep', '-E'])


class CombineCmdsListsCase(TestCase):

    def test_empty(self):
        assert_equal(combine_cmd_lists(), [])

    def test_concatenation(self):
        assert_equal(
            combine_cmd_lists(
                [['echo', 'foo']], None, (['mkdir', 'bar'], ['rmdir', 'bar'])),
            [['echo', 'foo'], ['mkdir', 'bar'], ['rmdir', 'bar']])

    def test_conversion(self):
        assert_equal(
            combine_cmd_lists(
                ['echo "Hello World!"'], None, [('mkdir', '/tmp/baz')]),
            [['echo', 'Hello World!'], ['mkdir', '/tmp/baz']])


class CombineEnvsTestCase(TestCase):

    def test_empty(self):
        assert_equal(combine_envs(), {})

    def test_later_values_take_precedence(self):
        assert_equal(
            combine_envs({'TMPDIR': '/tmp', 'HOME': '/home/dave'},
                         {'TMPDIR': '/var/tmp'}),
            {'TMPDIR': '/var/tmp', 'HOME': '/home/dave'})

    def test_skip_None(self):
        assert_equal(combine_envs(None, {'USER': 'dave'}, None,
                                  {'TERM': 'xterm'}, None),
                     {'USER': 'dave', 'TERM': 'xterm'})

    def test_paths(self):
        assert_equal(combine_envs(
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


class CombineLocalEnvsTestCase(TestCase):

    @setup
    def set_os_pathsep(self):
        self._real_os_pathsep = os.pathsep
        os.pathsep = ';'

    @teardown
    def restore_os_pathsep(self):
        os.pathsep = self._real_os_pathsep

    def test_paths(self):
        assert_equal(combine_local_envs(
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


class CombineListsTestCase(TestCase):

    def test_empty(self):
        assert_equal(combine_lists(), [])

    def test_concatenation(self):
        assert_equal(combine_lists([1, 2], None, (3, 4)), [1, 2, 3, 4])


class CombineOptsTestCase(TestCase):

    def test_empty(self):
        assert_equal(combine_opts(combiners={}), {})

    def test_combine_opts(self):
        combiners = {
            'foo': combine_lists,
        }
        assert_equal(
            combine_opts(combiners,
                         {'foo': ['bar'], 'baz': ['qux']},
                         {'foo': ['baz'], 'baz': ['quux'], 'bar': 'garply'},
                         None,
                         {}),
            # "baz" doesn't use the list combiner, so ['qux'] is overwritten
            {'foo': ['bar', 'baz'], 'baz': ['quux'], 'bar': 'garply'})


class CombineAndExpandPathsTestCase(TestCase):

    @class_setup
    def set_environment_vars(self):
        self._old_environ = os.environ.copy()
        os.environ.clear()
        os.environ.update({
            'HOME': '/home/foo',
            'USER': 'foo',
            'BAR': 'bar',
        })

    @class_teardown
    def restore_environment_vars(self):
        os.environ.clear()
        os.environ.update(self._old_environ)

    @setup
    def setup_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    @teardown
    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_expand_paths_empty(self):
        assert_equal(expand_path(None), None)
        assert_equal(expand_path(''), '')

    def test_expand_paths(self):
        assert_equal(expand_path('$BAR'), 'bar')
        assert_equal(expand_path('~/$BAR'), '/home/foo/bar')
        assert_equal(expand_path('~/$BARn'), '/home/foo/$BARn')
        assert_equal(expand_path('~/${BAR}n'), '/home/foo/barn')

    def test_combine_paths_empty(self):
        assert_equal(combine_paths(), None)

    def test_combine_paths(self):
        assert_equal(combine_paths('~/tmp', '/tmp/$USER', None),
                     '/tmp/foo')
        assert_equal(combine_paths('~/tmp', '/tmp/$USER', ''), '')

    def test_combine_path_lists_empty(self):
        assert_equal(combine_path_lists(), [])

    def test_combine_path_lists(self):
        assert_equal(
            combine_path_lists(['~/tmp'], [], ['/dev/null', '/tmp/$USER']),
            ['/home/foo/tmp', '/dev/null', '/tmp/foo'])

    def test_globbing(self):
        foo_path = os.path.join(self.tmp_dir, 'foo')
        bar_path = os.path.join(self.tmp_dir, 'bar')
        open(foo_path, 'w').close()
        open(bar_path, 'w').close()

        # make sure that paths that don't match anything on the local
        # filesystem are still preserved.
        assert_equal(
            combine_path_lists([os.path.join(self.tmp_dir, '*'),
                                foo_path],
                               [os.path.join(self.tmp_dir, 'q*'),
                                's3://walrus/foo'],
                               None),
            [bar_path, foo_path, foo_path,
             os.path.join(self.tmp_dir, 'q*'),
             's3://walrus/foo'])
