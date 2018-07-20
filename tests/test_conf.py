# Copyright 2009-2012 Yelp
# Copyright 2013 David Marin
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

"""Test configuration parsing and option combining"""
import os
import os.path
from unittest import TestCase
from unittest import skipIf

import mrjob.conf
from mrjob.conf import ClearedValue
from mrjob.conf import _conf_object_at_path
from mrjob.conf import _expanded_mrjob_conf_path
from mrjob.conf import _fix_clear_tags
from mrjob.conf import _load_yaml_with_clear_tag
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
from mrjob.conf import load_opts_from_mrjob_conf
from mrjob.conf import load_opts_from_mrjob_confs
from tests.quiet import logger_disabled
from tests.quiet import no_handlers_for_logger
from tests.sandbox import SandboxedTestCase

from tests.py2 import patch


def load_mrjob_conf(conf_path=None):
    """Shortcut for automatically loading mrjob.conf from one of the predefined
    locations and returning the de-YAMLed object
    """
    conf_path = _expanded_mrjob_conf_path(conf_path)
    return _conf_object_at_path(conf_path)


class MRJobConfTestCase(SandboxedTestCase):

    MRJOB_CONF_CONTENTS = None

    def setUp(self):
        super(MRJobConfTestCase, self).setUp()

        self._existing_paths = None
        real_path_exists = os.path.exists

        def os_path_exists_stub(path):
            if self._existing_paths is None:
                return real_path_exists(path)
            else:
                return path in self._existing_paths

        self.start(patch('os.path.exists', side_effect=os_path_exists_stub))


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

        with no_handlers_for_logger('mrjob.conf'):
            self.assertEqual(load_mrjob_conf(),
                             {'runners': {'foo': {'bar': 'baz'}}})
        self.assertEqual(load_opts_from_mrjob_conf('foo')[0][1],
                         {'bar': 'baz'})

    def test_load_mrjob_conf_and_load_opts(self):
        conf_path = os.path.join(self.tmp_dir, 'mrjob.conf.2')
        with open(conf_path, 'w') as f:
            f.write('{"runners": {"foo": {"qux": "quux"}}}')

        with no_handlers_for_logger('mrjob.conf'):
            self.assertEqual(
                load_mrjob_conf(conf_path=conf_path),
                {'runners': {'foo': {'qux': 'quux'}}})
        self.assertEqual(
            load_opts_from_mrjob_conf('foo', conf_path=conf_path)[0][1],
            {'qux': 'quux'})
        # test missing options
        with logger_disabled('mrjob.conf'):
            self.assertEqual(
                load_opts_from_mrjob_conf('bar', conf_path=conf_path)[0][1],
                {})

    def test_duplicate_conf_path(self):
        conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')

        with open(conf_path, 'w') as f:
            dump_mrjob_conf({}, f)

        self.assertEqual(
            load_opts_from_mrjob_confs(
                'foo', [conf_path, conf_path]),
            [(conf_path, {})])

    def test_symlink_to_duplicate_conf_path(self):
        conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')
        with open(conf_path, 'w') as f:
            dump_mrjob_conf({}, f)

        conf_symlink_path = os.path.join(self.tmp_dir, 'mrjob.conf.symlink')
        os.symlink('mrjob.conf', conf_symlink_path)

        self.assertEqual(
            load_opts_from_mrjob_confs(
                'foo', [conf_path, conf_symlink_path]),
            [(conf_symlink_path, {})])

        self.assertEqual(
            load_opts_from_mrjob_confs(
                'foo', [conf_symlink_path, conf_path]),
            [(conf_path, {})])

    def test_recursive_include(self):
        conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')
        with open(conf_path, 'w') as f:
            dump_mrjob_conf({'include': conf_path}, f)

        self.assertEqual(
            load_opts_from_mrjob_conf('foo', conf_path),
            [(conf_path, {})])

    def test_doubly_recursive_include(self):
        conf_path_1 = os.path.join(self.tmp_dir, 'mrjob.1.conf')
        conf_path_2 = os.path.join(self.tmp_dir, 'mrjob.2.conf')

        with open(conf_path_1, 'w') as f:
            dump_mrjob_conf({'include': conf_path_2}, f)

        with open(conf_path_2, 'w') as f:
            dump_mrjob_conf({'include': conf_path_1}, f)

        self.assertEqual(
            load_opts_from_mrjob_conf('foo', conf_path_1),
            [(conf_path_2, {}), (conf_path_1, {})])

    def test_conf_path_order_beats_include(self):
        conf_path_1 = os.path.join(self.tmp_dir, 'mrjob.1.conf')
        conf_path_2 = os.path.join(self.tmp_dir, 'mrjob.2.conf')

        with open(conf_path_1, 'w') as f:
            dump_mrjob_conf({}, f)

        with open(conf_path_2, 'w') as f:
            dump_mrjob_conf({}, f)

        # shouldn't matter that conf_path_1 includes conf_path_2
        self.assertEqual(
            load_opts_from_mrjob_confs('foo', [conf_path_1, conf_path_2]),
            [(conf_path_1, {}), (conf_path_2, {})])

    def test_include_order_beats_include(self):
        conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')
        conf_path_1 = os.path.join(self.tmp_dir, 'mrjob.1.conf')
        conf_path_2 = os.path.join(self.tmp_dir, 'mrjob.2.conf')

        with open(conf_path, 'w') as f:
            dump_mrjob_conf({'include': [conf_path_1, conf_path_2]}, f)

        with open(conf_path_1, 'w') as f:
            dump_mrjob_conf({'include': [conf_path_2]}, f)

        with open(conf_path_2, 'w') as f:
            dump_mrjob_conf({}, f)

        # shouldn't matter that conf_path_1 includes conf_path_2
        self.assertEqual(
            load_opts_from_mrjob_conf('foo', conf_path),
            [(conf_path_1, {}), (conf_path_2, {}), (conf_path, {})])

    def test_nested_include(self):
        conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')
        conf_path_1 = os.path.join(self.tmp_dir, 'mrjob.1.conf')
        conf_path_2 = os.path.join(self.tmp_dir, 'mrjob.2.conf')
        conf_path_3 = os.path.join(self.tmp_dir, 'mrjob.3.conf')

        # accidentally reversed the order of nested includes when
        # trying to make precedence work; this test would catch that

        with open(conf_path, 'w') as f:
            dump_mrjob_conf({'include': conf_path_1}, f)

        with open(conf_path_1, 'w') as f:
            dump_mrjob_conf({'include': [conf_path_2, conf_path_3]}, f)

        with open(conf_path_2, 'w') as f:
            dump_mrjob_conf({}, f)

        with open(conf_path_3, 'w') as f:
            dump_mrjob_conf({}, f)

        self.assertEqual(
            load_opts_from_mrjob_conf('foo', conf_path),
            [(conf_path_2, {}),
             (conf_path_3, {}),
             (conf_path_1, {}),
             (conf_path, {})])

    def test_relative_include(self):
        base_conf_path = os.path.join(self.tmp_dir, 'mrjob.base.conf')
        real_base_conf_path = os.path.realpath(base_conf_path)

        conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')

        with open(base_conf_path, 'w') as f:
            dump_mrjob_conf({}, f)

        with open(conf_path, 'w') as f:
            dump_mrjob_conf({'include': 'mrjob.base.conf'}, f)

        self.assertEqual(
            load_opts_from_mrjob_conf('foo', conf_path),
            [(real_base_conf_path, {}), (conf_path, {})])

    def test_include_relative_to_real_path(self):
        os.mkdir(os.path.join(self.tmp_dir, 'conf'))

        base_conf_path = os.path.join(self.tmp_dir, 'conf', 'mrjob.base.conf')
        real_base_conf_path = os.path.realpath(base_conf_path)

        conf_path = os.path.join(self.tmp_dir, 'conf', 'mrjob.conf')
        conf_symlink_path = os.path.join(self.tmp_dir, 'mrjob.conf')

        with open(base_conf_path, 'w') as f:
            dump_mrjob_conf({}, f)

        with open(conf_path, 'w') as f:
            dump_mrjob_conf({'include': 'mrjob.base.conf'}, f)

        os.symlink(os.path.join('conf', 'mrjob.conf'), conf_symlink_path)

        self.assertEqual(
            load_opts_from_mrjob_conf('foo', conf_path),
            [(real_base_conf_path, {}), (conf_path, {})])

        # relative include should work from the symlink even though
        # it's not in the same directory as mrjob.base.conf
        self.assertEqual(
            load_opts_from_mrjob_conf('foo', conf_symlink_path),
            [(real_base_conf_path, {}), (conf_symlink_path, {})])

    def test_tilde_in_include(self):
        # regression test for #1308

        os.environ['HOME'] = self.tmp_dir
        base_conf_path = os.path.join(self.tmp_dir, 'mrjob.base.conf')
        conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')

        with open(base_conf_path, 'w') as f:
            dump_mrjob_conf({}, f)

        with open(conf_path, 'w') as f:
            dump_mrjob_conf({'include': '~/mrjob.base.conf'}, f)

        self.assertEqual(
            load_opts_from_mrjob_conf('foo', conf_path),
            [(base_conf_path, {}), (conf_path, {})])

    def _test_round_trip(self, conf):
        conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')

        with open(conf_path, 'w') as f:
            dump_mrjob_conf(conf, f)
        with no_handlers_for_logger('mrjob.conf'):
            self.assertEqual(conf, load_mrjob_conf(conf_path=conf_path))

    def test_round_trip(self):
        self._test_round_trip({'runners': {'foo': {'qux': 'quux'}}})

    @skipIf(mrjob.conf.yaml is None, 'no yaml module')
    def test_round_trip_with_clear_tag(self):
        self._test_round_trip(
            {'runners': {'foo': {'qux': ClearedValue('quux')}}})


class MRJobConfNoYAMLTestCase(MRJobConfTestCase):

    def setUp(self):
        super(MRJobConfNoYAMLTestCase, self).setUp()
        self.start(patch('mrjob.conf.yaml', None))

    def test_using_json_and_not_yaml(self):
        conf = {'runners': {'foo': {'qux': 'quux'}}}
        conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')

        with open(conf_path, 'w') as f:
            dump_mrjob_conf(conf, f)

        with open(conf_path) as f:
            contents = f.read()

        self.assertEqual(contents.replace(' ', '').replace('\n', ''),
                         '{"runners":{"foo":{"qux":"quux"}}}')

    def test_no_support_for_clear_tags(self):
        conf = {'runners': {'foo': {'qux': ClearedValue('quux')}}}
        conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')

        with open(conf_path, 'w') as f:
            self.assertRaises(TypeError,
                              dump_mrjob_conf, conf, f)

    def test_json_error(self):
        conf = """
            runners:
                foo:
                    qux: quux
        """
        conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')

        with open(conf_path, 'w') as f:
            f.write(conf)

        try:
            load_mrjob_conf(conf_path)
            assert False
        except ValueError as e:
            self.assertIn('If your mrjob.conf is in YAML', e.msg)


class CombineValuesTestCase(TestCase):

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


class CombineDictsTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(combine_dicts(), {})

    def test_later_values_take_precedence(self):
        self.assertEqual(
            combine_dicts({'TMPDIR': '/tmp', 'HOME': '/home/dave'},
                          {'TMPDIR': '/var/tmp'}),
            {'TMPDIR': '/var/tmp', 'HOME': '/home/dave'})

    def test_skip_None(self):
        self.assertEqual(
            combine_dicts(None, {'USER': 'dave'}, None,
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

    def test_None_value(self):
        self.assertEqual(
            combine_dicts({'USER': 'dave', 'TERM': 'xterm'}, {'USER': None}),
            {'TERM': 'xterm', 'USER': None})

    def test_cleared_value(self):
        self.assertEqual(
            combine_dicts({'USER': 'dave', 'TERM': 'xterm'},
                          {'USER': ClearedValue('caleb')}),
            {'TERM': 'xterm', 'USER': 'caleb'})

    def test_deleted_value(self):
        self.assertEqual(
            combine_dicts({'USER': 'dave', 'TERM': 'xterm'},
                          {'USER': ClearedValue(None)}),
            {'TERM': 'xterm'})

    def test_dont_accept_wrapped_dicts(self):
        self.assertRaises(AttributeError,
                          combine_dicts, ClearedValue({'USER': 'dave'}))


class CombineCmdsTestCase(TestCase):

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

    def test_unicode(self):
        self.assertEqual(combine_cmds(u'wunderbar!'), ['wunderbar!'])


class CombineEnvsTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(combine_envs(), {})

    def test_later_values_take_precedence(self):
        self.assertEqual(
            combine_envs({'TMPDIR': '/tmp', 'HOME': '/home/dave'},
                         {'TMPDIR': '/var/tmp'}),
            {'TMPDIR': '/var/tmp', 'HOME': '/home/dave'})

    def test_skip_None(self):
        self.assertEqual(
            combine_envs(None, {'USER': 'dave'}, None,
                         {'TERM': 'xterm'}, None),
            {'USER': 'dave', 'TERM': 'xterm'})

    def test_paths(self):
        self.assertEqual(
            combine_envs(
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

    def test_clear_paths(self):
        self.assertEqual(
            combine_envs(
                {'PATH': '/bin:/usr/bin',
                 'PYTHONPATH': '/usr/lib/python/site-packages',
                 'PS1': '> '},
                {'PATH': ClearedValue('/home/dave/bin'),
                 'PYTHONPATH': ClearedValue(None),
                 'CLASSPATH': '/home/dave/java',
                 'PS1': '\w> '}),
            {'PATH': '/home/dave/bin',
             'CLASSPATH': '/home/dave/java',
             'PS1': '\w> '})


class CombineLocalEnvsTestCase(TestCase):

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


class CombineListsTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(combine_lists(), [])

    def test_concatenation(self):
        self.assertEqual(combine_lists([1, 2], None, (3, 4)), [1, 2, 3, 4])

    def test_strings(self):
        self.assertEqual(combine_lists('one', None, 'two', u'three'),
                         ['one', 'two', u'three'])

    def test_dicts(self):
        self.assertEqual(combine_lists({1: 2}, None, {}),
                         [{1: 2}, {}])

    def test_scalars(self):
        self.assertEqual(combine_lists(None, False, b'\x00', 42, 3.14),
                         [False, b'\x00', 42, 3.14])

    def test_mix_lists_and_scalars(self):
        self.assertEqual(combine_lists([1, 2], 3, (4, 5), 6),
                         [1, 2, 3, 4, 5, 6])


class CombineOptsTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(combine_opts(combiners={}), {})

    def test_combine_opts(self):
        self.assertEqual(
            combine_opts(dict(foo=combine_lists),
                         {'foo': ['bar'], 'baz': ['qux']},
                         {'foo': ['baz'], 'baz': ['quux'], 'bar': 'garply'},
                         None,
                         {}),
            # "baz" doesn't use the list combiner, so ['qux'] is overwritten
            {'foo': ['bar', 'baz'], 'baz': ['quux'], 'bar': 'garply'})

    def test_cleared_opt_values(self):
        self.assertEqual(
            combine_opts(dict(foo=combine_lists),
                         {'foo': ['bar']},
                         {'foo': ClearedValue(['baz'])}),
            # ClearedValue(['baz']) overrides bar
            {'foo': ['baz']})

        self.assertEqual(
            combine_opts(dict(foo=combine_lists),
                         {'foo': ['bar']},
                         {'foo': ClearedValue(None)}),
            # not None!
            {'foo': []})

    def test_cant_clear_entire_opt_dicts(self):
        self.assertRaises(
            TypeError,
            combine_opts,
            dict(foo=combine_lists),
            {'foo': ['bar']},
            ClearedValue({'foo': ['baz']}))


class CombineAndExpandPathsTestCase(SandboxedTestCase):

    def setUp(self):
        super(CombineAndExpandPathsTestCase, self).setUp()
        os.environ.update({
            'HOME': '/home/foo',
            'USER': 'foo',
            'BAR': 'bar',
        })

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

    def test_combine_path_lists_on_strings(self):
        self.assertEqual(
            combine_path_lists('~/tmp', [], ['/dev/null', '/tmp/$USER']),
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


@skipIf(mrjob.conf.yaml is None, 'no yaml module')
class LoadYAMLWithClearTag(TestCase):

    def test_empty(self):
        self.assertEqual(_load_yaml_with_clear_tag(''),
                         None)
        self.assertEqual(_load_yaml_with_clear_tag('!clear'),
                         ClearedValue(None))

    def test_null(self):
        self.assertEqual(_load_yaml_with_clear_tag('null'),
                         None)
        self.assertEqual(_load_yaml_with_clear_tag('!clear null'),
                         ClearedValue(None))

    def test_string(self):
        self.assertEqual(_load_yaml_with_clear_tag('foo'),
                         'foo')
        self.assertEqual(_load_yaml_with_clear_tag('!clear foo'),
                         ClearedValue('foo'))

    def test_int(self):
        self.assertEqual(_load_yaml_with_clear_tag('18'),
                         18)
        self.assertEqual(_load_yaml_with_clear_tag('!clear 18'),
                         ClearedValue(18))

    def test_list(self):
        self.assertEqual(_load_yaml_with_clear_tag('- foo\n- bar'),
                         ['foo', 'bar'])
        self.assertEqual(_load_yaml_with_clear_tag('!clear\n- foo\n- bar'),
                         ClearedValue(['foo', 'bar']))
        self.assertEqual(_load_yaml_with_clear_tag('- foo\n- !clear bar'),
                         ['foo', ClearedValue('bar')])
        self.assertEqual(
            _load_yaml_with_clear_tag('!clear\n- !clear foo\n- !clear bar'),
            ClearedValue([ClearedValue('foo'), ClearedValue('bar')]))

    def test_dict(self):
        self.assertEqual(_load_yaml_with_clear_tag('foo: bar'),
                         {'foo': 'bar'})
        self.assertEqual(_load_yaml_with_clear_tag('!clear\nfoo: bar'),
                         ClearedValue({'foo': 'bar'}))
        self.assertEqual(_load_yaml_with_clear_tag('!clear foo: bar'),
                         {ClearedValue('foo'): 'bar'})
        self.assertEqual(_load_yaml_with_clear_tag('foo: !clear bar'),
                         {'foo': ClearedValue('bar')})
        self.assertEqual(
            _load_yaml_with_clear_tag('!clear\n!clear foo: !clear bar'),
            ClearedValue({ClearedValue('foo'): ClearedValue('bar')}))
        self.assertEqual(
            _load_yaml_with_clear_tag('!clear foo: bar\nfoo: baz'),
            {ClearedValue('foo'): 'bar', 'foo': 'baz'})

    def test_nesting(self):
        self.assertEqual(
            _load_yaml_with_clear_tag('foo:\n  - bar\n  - baz: qux'),
            {'foo': ['bar', {'baz': 'qux'}]})

        self.assertEqual(
            _load_yaml_with_clear_tag(
                '!clear\n  foo:\n    - bar\n    - baz: qux'),
            ClearedValue({'foo': ['bar', {'baz': 'qux'}]}))

        self.assertEqual(
            _load_yaml_with_clear_tag('!clear foo:\n  - bar\n  - baz: qux'),
            {ClearedValue('foo'): ['bar', {'baz': 'qux'}]})

        self.assertEqual(
            _load_yaml_with_clear_tag('foo: !clear\n  - bar\n  - baz: qux'),
            {'foo': ClearedValue(['bar', {'baz': 'qux'}])})

        self.assertEqual(
            _load_yaml_with_clear_tag('foo:\n  - !clear bar\n  - baz: qux'),
            {'foo': [ClearedValue('bar'), {'baz': 'qux'}]})

        self.assertEqual(
            _load_yaml_with_clear_tag(
                'foo:\n  - bar\n  - !clear\n    baz: qux'),
            {'foo': ['bar', ClearedValue({'baz': 'qux'})]})

        self.assertEqual(
            _load_yaml_with_clear_tag('foo:\n  - bar\n  - !clear baz: qux'),
            {'foo': ['bar', {ClearedValue('baz'): 'qux'}]})

        self.assertEqual(
            _load_yaml_with_clear_tag('foo:\n  - bar\n  - baz: !clear qux'),
            {'foo': ['bar', {'baz': ClearedValue('qux')}]})


class FixClearTag(TestCase):

    def test_none(self):
        self.assertEqual(_fix_clear_tags(None), None)
        self.assertEqual(_fix_clear_tags(ClearedValue(None)),
                         ClearedValue(None))

    def test_string(self):
        self.assertEqual(_fix_clear_tags('foo'), 'foo')
        self.assertEqual(_fix_clear_tags(ClearedValue('foo')),
                         ClearedValue('foo'))

    def test_int(self):
        self.assertEqual(_fix_clear_tags(18), 18)
        self.assertEqual(_fix_clear_tags(ClearedValue(18)),
                         ClearedValue(18))

    def test_list(self):
        self.assertEqual(_fix_clear_tags(['foo', 'bar']),
                         ['foo', 'bar'])

        self.assertEqual(_fix_clear_tags(ClearedValue(['foo', 'bar'])),
                         ClearedValue(['foo', 'bar']))

        self.assertEqual(_fix_clear_tags(['foo', ClearedValue('bar')]),
                         ['foo', 'bar'])

        self.assertEqual(
            _fix_clear_tags(
                ClearedValue([ClearedValue('foo'), ClearedValue('bar')])),
            ClearedValue(['foo', 'bar']))

    def test_dict(self):
        self.assertEqual(_fix_clear_tags({'foo': 'bar'}), {'foo': 'bar'})

        self.assertEqual(_fix_clear_tags(ClearedValue({'foo': 'bar'})),
                         ClearedValue({'foo': 'bar'}))

        self.assertEqual(_fix_clear_tags({ClearedValue('foo'): 'bar'}),
                         {'foo': ClearedValue('bar')})

        self.assertEqual(_fix_clear_tags({'foo': ClearedValue('bar')}),
                         {'foo': ClearedValue('bar')})

        self.assertEqual(
            _fix_clear_tags(
                ClearedValue({ClearedValue('foo'): ClearedValue('bar')})),
            ClearedValue({'foo': ClearedValue('bar')}))

        # ClearedValue('foo') key overrides 'foo' key
        self.assertEqual(
            _fix_clear_tags({ClearedValue('foo'): 'bar', 'foo': 'baz'}),
            {'foo': ClearedValue('bar')})

    def test_nesting(self):
        self.assertEqual(_fix_clear_tags({'foo': ['bar', {'baz': 'qux'}]}),
                         {'foo': ['bar', {'baz': 'qux'}]})

        self.assertEqual(
            _fix_clear_tags(
                ClearedValue({'foo': ['bar', {'baz': 'qux'}]})),
            ClearedValue({'foo': ['bar', {'baz': 'qux'}]}))

        self.assertEqual(
            _fix_clear_tags(
                {ClearedValue('foo'): ['bar', {'baz': 'qux'}]}),
            {'foo': ClearedValue(['bar', {'baz': 'qux'}])})

        self.assertEqual(
            _fix_clear_tags(
                {'foo': ClearedValue(['bar', {'baz': 'qux'}])}),
            {'foo': ClearedValue(['bar', {'baz': 'qux'}])})

        self.assertEqual(
            _fix_clear_tags(
                {'foo': [ClearedValue('bar'), {'baz': 'qux'}]}),
            {'foo': ['bar', {'baz': 'qux'}]})

        self.assertEqual(
            _fix_clear_tags(
                {'foo': ['bar', ClearedValue({'baz': 'qux'})]}),
            {'foo': ['bar', {'baz': 'qux'}]})

        self.assertEqual(
            _fix_clear_tags(
                {'foo': ['bar', {ClearedValue('baz'): 'qux'}]}),
            {'foo': ['bar', {'baz': ClearedValue('qux')}]})

        self.assertEqual(
            _fix_clear_tags(
                {'foo': ['bar', {'baz': ClearedValue('qux')}]}),
            {'foo': ['bar', {'baz': ClearedValue('qux')}]})
