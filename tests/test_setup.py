# Copyright 2012 Yelp
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
import os
from unittest import TestCase

from mrjob.setup import UploadDirManager
from mrjob.setup import WorkingDirManager
from mrjob.setup import name_uniquely
from mrjob.setup import parse_legacy_hash_path
from mrjob.setup import parse_setup_cmd

from tests.py2 import patch


class ParseSetupCmdTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(parse_setup_cmd(''), [])
        self.assertEqual(parse_setup_cmd(' '), [' '])
        self.assertRaises(TypeError, parse_setup_cmd, None)

    def test_file_hash_path_alone(self):
        self.assertEqual(
            parse_setup_cmd('foo#'),
            [{'type': 'file', 'path': 'foo', 'name': None}])
        self.assertEqual(
            parse_setup_cmd('foo#bar'),
            [{'type': 'file', 'path': 'foo', 'name': 'bar'}])
        self.assertEqual(
            parse_setup_cmd('/dir/foo#bar'),
            [{'type': 'file', 'path': '/dir/foo', 'name': 'bar'}])

    def test_archive_hash_path_alone(self):
        self.assertEqual(
            parse_setup_cmd('foo#/'),
            [{'type': 'archive', 'path': 'foo', 'name': None}, '/'])
        self.assertEqual(
            parse_setup_cmd('foo#bar/'),
            [{'type': 'archive', 'path': 'foo', 'name': 'bar'}, '/'])
        self.assertEqual(
            parse_setup_cmd('/dir/foo#bar/'),
            [{'type': 'archive', 'path': '/dir/foo', 'name': 'bar'}, '/'])

    def test_dir_hash_path_alone(self):
        self.assertEqual(
            parse_setup_cmd('foo/#'),
            [{'type': 'dir', 'path': 'foo', 'name': None}, '/'])
        self.assertEqual(
            parse_setup_cmd('foo/#/'),
            [{'type': 'dir', 'path': 'foo', 'name': None}, '/'])
        self.assertEqual(
            parse_setup_cmd('foo/#bar'),
            [{'type': 'dir', 'path': 'foo', 'name': 'bar'}, '/'])
        self.assertEqual(
            parse_setup_cmd('foo/#bar/'),
            [{'type': 'dir', 'path': 'foo', 'name': 'bar'}, '/'])
        self.assertEqual(
            parse_setup_cmd('/dir/foo/#bar'),
            [{'type': 'dir', 'path': '/dir/foo', 'name': 'bar'}, '/'])
        self.assertEqual(
            parse_setup_cmd('/dir/foo/#bar/'),
            [{'type': 'dir', 'path': '/dir/foo', 'name': 'bar'}, '/'])

    def test_no_path(self):
        self.assertEqual(parse_setup_cmd('#bar'), ['#bar'])

    def test_root_dir_only(self):
        # tarring up the entire filesystem is a terrible idea; no
        # good reason to allow this
        self.assertEqual(parse_setup_cmd('/#'), ['/#'])

    def test_no_name(self):
        self.assertEqual(
            parse_setup_cmd('foo#'),
            [{'type': 'file', 'path': 'foo', 'name': None}])
        self.assertEqual(
            parse_setup_cmd('foo#/'),
            [{'type': 'archive', 'path': 'foo', 'name': None}, '/'])

    def test_no_hash(self):
        self.assertEqual(parse_setup_cmd('foo'), ['foo'])

    def test_double_hash(self):
        self.assertEqual(parse_setup_cmd('foo#bar#baz'),
                         [{'type': 'file', 'path': 'foo#bar', 'name': 'baz'}])

    def test_name_slash_included_in_command(self):
        self.assertEqual(
            parse_setup_cmd('sudo dpkg -i my_pkgs.tar#/fooify.deb'),
            ['sudo dpkg -i ',
             {'type': 'archive', 'path': 'my_pkgs.tar', 'name': None},
             '/fooify.deb'])

    def test_file_inside_dir(self):
        self.assertEqual(
            parse_setup_cmd('sudo dpkg -i my_pkgs/#/fooify.deb'),
            ['sudo dpkg -i ',
             {'type': 'dir', 'path': 'my_pkgs', 'name': None},
             '/fooify.deb'])

    def test_named_dir(self):
        self.assertEqual(
            parse_setup_cmd('cd src/#awesome-dir'),
            ['cd ',
             {'type': 'dir', 'path': 'src', 'name': 'awesome-dir'},
             '/'])

    def test_shell_punctuation_after_name(self):
        self.assertEqual(
            parse_setup_cmd('touch foo#; cat bar#>baz; cat qux#|grep quux'),
            [
                'touch ',
                {'type': 'file', 'path': 'foo', 'name': None},
                '; cat ',
                {'type': 'file', 'path': 'bar', 'name': None},
                '>baz; cat ',
                {'type': 'file', 'path': 'qux', 'name': None},
                '|grep quux',
            ]
        )

    def test_colon_after_name(self):
        self.assertEqual(
            parse_setup_cmd('echo foo.egg#:$PYTHONPATH'),
            ['echo ',
             {'type': 'file', 'path': 'foo.egg', 'name': None},
             ':$PYTHONPATH'])

    def test_start_path_after_colon(self):
        self.assertEqual(
            parse_setup_cmd('export PYTHONPATH=$PYTHONPATH:foo.tar.gz#/'),
            ['export PYTHONPATH=$PYTHONPATH:',
             {'type': 'archive', 'path': 'foo.tar.gz', 'name': None},
             '/'])

    def test_start_path_after_equals(self):
        self.assertEqual(
            parse_setup_cmd('export PYTHONPATH=foo.egg#'),
            ['export PYTHONPATH=',
             {'type': 'file', 'path': 'foo.egg', 'name': None}])

    def test_allow_colons_in_uris(self):
        self.assertEqual(
            parse_setup_cmd('export PATH=$PATH:s3://foo/script.sh#'),
            ['export PATH=$PATH:',
             {'type': 'file', 'path': 's3://foo/script.sh', 'name': None}])

    def test_resolve_path_but_not_name(self):
        with patch.dict(os.environ, {'HOME': '/home/foo',
                                     'USER': 'foo',
                                     'BAR': 'bar'}, clear=True):
            self.assertEqual(
                parse_setup_cmd(r'. ~/tmp/$USER/\$BAR.sh#$USER.sh'),
                ['. ',
                 {'path': '/home/foo/tmp/foo/$BAR.sh',
                  'name': '$USER.sh',
                  'type': 'file'}])

    def test_dont_parse_hash_path_inside_quotes(self):
        self.assertEqual(
            parse_setup_cmd('"foo#bar"'), ['"foo#bar"'])

        self.assertEqual(
            parse_setup_cmd("'foo#bar'"), ["'foo#bar'"])

    def test_missing_closing_quotation(self):
        self.assertRaises(
            ValueError, parse_setup_cmd, '"foo')
        self.assertRaises(
            ValueError, parse_setup_cmd, 'foo#bar "baz')

    def test_missing_escaped_character(self):
        self.assertRaises(
            ValueError, parse_setup_cmd, 'foo\\')


class ParseLegacyHashPathTestCase(TestCase):

    def test_empty(self):
        self.assertRaises(ValueError, parse_legacy_hash_path, 'archive', '')
        self.assertRaises(ValueError, parse_legacy_hash_path, 'file', '')
        self.assertRaises(TypeError, parse_legacy_hash_path, 'archive', None)
        self.assertRaises(TypeError, parse_legacy_hash_path, 'file', None)

    def test_basic(self):
        self.assertEqual(parse_legacy_hash_path('file', 'foo#bar'),
                         {'type': 'file', 'path': 'foo', 'name': 'bar'})
        self.assertEqual(parse_legacy_hash_path('file', '/dir/foo#bar'),
                         {'type': 'file', 'path': '/dir/foo', 'name': 'bar'})

    def test_no_path(self):
        self.assertRaises(ValueError, parse_legacy_hash_path, 'file', '#bar')

    def test_no_name(self):
        self.assertEqual(parse_legacy_hash_path('file', 'foo'),
                         {'type': 'file', 'path': 'foo', 'name': None})
        self.assertEqual(parse_legacy_hash_path('file', 'foo#'),
                         {'type': 'file', 'path': 'foo', 'name': None})

    def test_must_name(self):
        self.assertEqual(
            parse_legacy_hash_path('file', 'foo#bar', must_name='it'),
            {'type': 'file', 'path': 'foo', 'name': 'bar'})
        # use basename if no hash
        self.assertEqual(
            parse_legacy_hash_path('file', 'foo', must_name='it'),
            {'type': 'file', 'path': 'foo', 'name': 'foo'})
        # raise error on explicit empty name
        self.assertRaises(ValueError,
                          parse_legacy_hash_path, 'file', 'foo#',
                          must_name='it')
        # raise error if no basename
        self.assertRaises(ValueError,
                          parse_legacy_hash_path, 'file', 'foo/',
                          must_name='it')

    def test_trailing_slash_in_name(self):
        self.assertRaises(
            ValueError,
            parse_legacy_hash_path, 'file', 'foo.tar.gz#bar/')
        self.assertRaises(
            ValueError,
            parse_legacy_hash_path, 'file', 'foo.tar.gz#/')
        # trailing slash is allowed for archives because that's the new
        # way of indicating archives
        self.assertEqual(
            parse_legacy_hash_path('archive', 'foo.tar.gz#bar/'),
            {'type': 'archive', 'path': 'foo.tar.gz', 'name': 'bar'})
        self.assertEqual(
            parse_legacy_hash_path('archive', 'foo.tar.gz#/'),
            {'type': 'archive', 'path': 'foo.tar.gz', 'name': None})

    def test_bad_path_type(self):
        self.assertRaises(
            ValueError,
            parse_legacy_hash_path, 'symlink', 'foo#bar')

    def test_bad_name(self):
        self.assertRaises(
            ValueError,
            parse_legacy_hash_path, 'file', 'foo#bar#baz')
        # can't place files in subdirectories
        self.assertRaises(
            ValueError,
            parse_legacy_hash_path, 'file', 'foo#bar/baz')


class NameUniquelyTestCase(TestCase):

    def test_no_empty_names(self):
        self.assertEqual(name_uniquely(''), '1')

    def test_empty_proposed_name_same_as_none(self):
        self.assertEqual(name_uniquely('foo.py', proposed_name=None), 'foo.py')
        self.assertEqual(name_uniquely('foo.py', proposed_name=''), 'foo.py')

    def test_use_basename_by_default(self):
        self.assertEqual(name_uniquely('foo/bar.py'), 'bar.py')
        self.assertEqual(name_uniquely('/'), '1')

    def test_dont_use_names_taken(self):
        self.assertEqual(name_uniquely('foo.py'), 'foo.py')
        self.assertEqual(
            name_uniquely('foo.py', names_taken=['foo.py']),
            'foo-1.py')
        self.assertEqual(
            name_uniquely('foo.py', names_taken=['foo.py', 'foo-1.py']),
            'foo-2.py')

    def test_dont_used_proposed_names_taken(self):
        self.assertEqual(
            name_uniquely('foo.py', proposed_name='bar.py'),
            'bar.py')
        self.assertEqual(
            name_uniquely('foo.py', names_taken=['bar.py'],
                          proposed_name='bar.py'),
            'bar-1.py')
        self.assertEqual(
            name_uniquely('foo.py', names_taken=['bar.py', 'bar-1.py'],
                          proposed_name='bar.py'),
            'bar-2.py')

        # doesn't matter if path is taken
        self.assertEqual(
            name_uniquely(
                'foo.py', names_taken=['foo.py'], proposed_name='bar.py'),
            'bar.py')

    def test_auto_names_preserve_full_extension(self):
        self.assertEqual(
            name_uniquely(
                'foo.tar.gz', names_taken=['foo.tar.gz']),
            'foo-1.tar.gz')

    def test_auto_names_with_no_extension(self):
        self.assertEqual(
            name_uniquely(
                'foo', names_taken=['foo']),
            'foo-1')
        self.assertEqual(
            name_uniquely(
                '', names_taken=['1']),
            '2')

    def test_initial_dot_isnt_extension(self):
        self.assertEqual(
            name_uniquely(
                '.emacs', names_taken=['.emacs']),
            '.emacs-1')  # not '-1.emacs'

        self.assertEqual(
            name_uniquely(
                '.mrjob.conf', names_taken=['.mrjob.conf']),
            '.mrjob-1.conf')  # not '-1.mrjob.conf'

    def test_unhide(self):
        self.assertEqual(
            name_uniquely('.emacs', unhide=True), 'emacs')
        self.assertEqual(
            name_uniquely('._foo', unhide=True), 'foo')
        self.assertEqual(
            name_uniquely('_.bar', unhide=True), '1.bar')
        self.assertEqual(
            name_uniquely('_', unhide=True), '1')

    def test_unhide_affects_proposed_name(self):
        # these options aren't used together in practice, but of course
        # the proposed name is the one we care about if it's given
        self.assertEqual(
            name_uniquely(
                'foo.py', proposed_name='.hidden.foo.py', unhide=True),
            'hidden.foo.py')

    def test_strip_trailing_slash(self):
        self.assertEqual(
            name_uniquely('s3://bucket/archive-dir/'), 'archive-dir')

    def test_strip_trailing_os_sep(self):
        self.assertEqual(
            name_uniquely(os.path.join('foo', 'bar', '')), 'bar')


class UploadDirManagerTestCase(TestCase):

    def test_empty(self):
        sd = UploadDirManager('hdfs:///')
        self.assertEqual(sd.path_to_uri(), {})

    def test_simple(self):
        sd = UploadDirManager('hdfs:///')
        sd.add('foo/bar.py')
        self.assertEqual(sd.path_to_uri(), {'foo/bar.py': 'hdfs:///bar.py'})

    def test_name_collision(self):
        sd = UploadDirManager('hdfs:///')
        sd.add('foo/bar.py')
        sd.add('bar.py')
        self.assertEqual(sd.path_to_uri(),
                         {'foo/bar.py': 'hdfs:///bar.py',
                          'bar.py': 'hdfs:///bar-1.py'})

    def test_add_is_idempotent(self):
        sd = UploadDirManager('hdfs:///')
        sd.add('foo/bar.py')
        self.assertEqual(sd.path_to_uri(), {'foo/bar.py': 'hdfs:///bar.py'})
        sd.add('foo/bar.py')
        self.assertEqual(sd.path_to_uri(), {'foo/bar.py': 'hdfs:///bar.py'})

    def test_uri(self):
        sd = UploadDirManager('hdfs:///')
        sd.add('foo/bar.py')
        self.assertEqual(sd.uri('foo/bar.py'), 'hdfs:///bar.py')

    def test_unknown_uri(self):
        sd = UploadDirManager('hdfs:///')
        sd.add('foo/bar.py')
        self.assertEqual(sd.path_to_uri(), {'foo/bar.py': 'hdfs:///bar.py'})
        self.assertEqual(sd.uri('hdfs://host/path/to/bar.py'),
                         'hdfs://host/path/to/bar.py')
        # checking unknown URIs doesn't add them
        self.assertEqual(sd.path_to_uri(), {'foo/bar.py': 'hdfs:///bar.py'})

    def uri_adds_trailing_slash(self):
        sd = UploadDirManager('s3://bucket/dir')
        sd.add('foo/bar.py')
        self.assertEqual(sd.uri('foo/bar.py'), 's3://bucket/dir/bar.py')
        self.assertEqual(sd.path_to_uri(),
                         {'foo/bar.py': 's3://bucket/dir/bar.py'})

    def test_unhide_files(self):
        # avoid giving names to files that Hadoop will ignore as input
        sd = UploadDirManager('hdfs:///')
        sd.add('.foo.log')
        sd.add('_bar.txt')
        self.assertEqual(sd.path_to_uri(),
                         {'.foo.log': 'hdfs:///foo.log',
                          '_bar.txt': 'hdfs:///bar.txt'})

    def test_hidden_file_name_collision(self):
        sd = UploadDirManager('hdfs:///')
        sd.add('foo/_bar.py')
        sd.add('_bar.py')
        self.assertEqual(sd.path_to_uri(),
                         {'foo/_bar.py': 'hdfs:///bar.py',
                          '_bar.py': 'hdfs:///bar-1.py'})

    def test_underscores_only(self):
        sd = UploadDirManager('hdfs:///')
        sd.add('_')
        sd.add('_.txt')

        self.assertEqual(sd.path_to_uri(),
                         {'_': 'hdfs:///1',
                          '_.txt': 'hdfs:///1.txt'})

    def test_dot_underscore(self):
        sd = UploadDirManager('hdfs:///')

        sd.add('._')
        sd.add('._.txt')
        sd.add('._foo')

        self.assertEqual(sd.path_to_uri(),
                         {'._': 'hdfs:///1',
                          '._.txt': 'hdfs:///1.txt',
                          '._foo': 'hdfs:///foo'})


class WorkingDirManagerTestCase(TestCase):

    def test_empty(self):
        wd = WorkingDirManager()
        self.assertEqual(wd.name_to_path('archive'), {})
        self.assertEqual(wd.name_to_path('file'), {})
        self.assertEqual(wd.paths(), set())

    def test_simple(self):
        wd = WorkingDirManager()
        wd.add('archive', 's3://bucket/path/to/baz.tar.gz')
        wd.add('file', 'foo/bar.py')
        self.assertEqual(wd.name_to_path('file'),
                         {'bar.py': 'foo/bar.py'})
        self.assertEqual(wd.name_to_path('archive'),
                         {'baz.tar.gz': 's3://bucket/path/to/baz.tar.gz'})
        self.assertEqual(
            wd.paths(),
            set(['foo/bar.py', 's3://bucket/path/to/baz.tar.gz']))

    def test_explicit_name_collision(self):
        wd = WorkingDirManager()
        wd.add('file', 'foo.py', name='qux.py')
        self.assertRaises(ValueError, wd.add, 'file', 'bar.py', name='qux.py')

    def test_okay_to_give_same_path_same_name(self):
        wd = WorkingDirManager()
        wd.add('file', 'foo/bar.py', name='qux.py')
        wd.add('file', 'foo/bar.py', name='qux.py')
        self.assertEqual(wd.name_to_path('file'),
                         {'qux.py': 'foo/bar.py'})

    def test_auto_names_are_different_from_assigned_names(self):
        wd = WorkingDirManager()
        wd.add('file', 'foo/bar.py', name='qux.py')
        wd.add('file', 'foo/bar.py')  # use default name bar.py
        self.assertEqual(wd.name_to_path('file'),
                         {'qux.py': 'foo/bar.py',
                          'bar.py': 'foo/bar.py'})
        self.assertEqual(wd.paths(), set(['foo/bar.py']))

    def test_cant_give_same_path_different_types(self):
        wd = WorkingDirManager()
        wd.add('archive', 'foo/bar.py', name='qux.py')
        self.assertRaises(ValueError,
                          wd.add, 'file', 'foo/bar.py', name='qux.py')

    def test_lazy_naming(self):
        wd = WorkingDirManager()
        wd.add('file', 'qux.py')  # qux.py by default
        wd.add('file', 'bar.py', name='qux.py')
        self.assertEqual(wd.name_to_path('file'),
                         {'qux.py': 'bar.py', 'qux-1.py': 'qux.py'})

    def test_eager_naming(self):
        wd = WorkingDirManager()
        wd.add('file', 'qux.py')  # qux.py by default
        self.assertEqual(wd.name('file', 'qux.py'), 'qux.py')
        # whoops, picked that name too soon!
        self.assertRaises(ValueError, wd.add, 'file', 'bar.py', name='qux.py')

    def test_bad_path_type(self):
        wd = WorkingDirManager()
        self.assertRaises(ValueError, wd.add, 'symlink', 'foo.py')
        self.assertRaises(ValueError, wd.name_to_path, 'symlink')
        self.assertRaises(ValueError, wd.name, 'symlink', 'foo.py')

    def test_cant_name_unknown_paths(self):
        wd = WorkingDirManager()
        self.assertRaises(ValueError, wd.name, 'file', 'bar.py')
        self.assertRaises(ValueError, wd.name, 'file', 'bar.py', name='qux.py')

    def test_cant_auto_name_unless_added_as_auto(self):
        wd = WorkingDirManager()
        wd.add('file', 'bar.py', name='qux.py')
        self.assertEqual(wd.name('file', 'bar.py', 'qux.py'), 'qux.py')
        self.assertRaises(ValueError,
                          wd.name, 'file', 'bar.py')

    def test_allow_hidden_files(self):
        wd = WorkingDirManager()
        wd.add('archive', '_foo.tar.gz')
        wd.add('file', '.bazrc')

        self.assertEqual(wd.name('archive', '_foo.tar.gz'), '_foo.tar.gz')
        self.assertEqual(wd.name('file', '.bazrc'), '.bazrc')
