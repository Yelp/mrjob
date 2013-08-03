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

"""Tests of all the amazing utilities in mrjob.util"""
from __future__ import with_statement

import bz2
import gzip
import optparse
import os
import shutil
from subprocess import PIPE
from subprocess import Popen
from StringIO import StringIO
import tarfile
import tempfile

try:
    import unittest2 as unittest
    unittest  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import unittest

from mrjob.util import buffer_iterator_to_line_iterator
from mrjob.util import cmd_line
from mrjob.util import extract_dir_for_tar
from mrjob.util import file_ext
from mrjob.util import parse_and_save_options
from mrjob.util import read_file
from mrjob.util import read_input
from mrjob.util import safeeval
from mrjob.util import scrape_options_into_new_groups
from mrjob.util import tar_and_gzip
from mrjob.util import unarchive


class BufferIteratorToLineIteratorTestCase(unittest.TestCase):

    def test_empty(self):
        self.assertEqual(
            list(buffer_iterator_to_line_iterator(_ for _ in ())),
            [])

    def test_buffered_lines(self):
        self.assertEqual(
            list(buffer_iterator_to_line_iterator(chunk for chunk in
                                                  ['The quick\nbrown fox\nju',
                                                   'mped over\nthe lazy\ndog',
                                                   's.\n'])),
            ['The quick\n', 'brown fox\n', 'jumped over\n', 'the lazy\n',
             'dogs.\n'])

    def test_add_trailing_newline(self):
        self.assertEqual(
            list(buffer_iterator_to_line_iterator(chunk for chunk in
                                                  ['Alouette,\ngentille',
                                                   ' Alouette.'])),
            ['Alouette,\n', 'gentille Alouette.\n'])


class CmdLineTestCase(unittest.TestCase):

    def test_cmd_line(self):
        self.assertEqual(cmd_line(['cut', '-f', 2, '-d', ' ']),
                         "cut -f 2 -d ' '")
        self.assertIn(cmd_line(['grep', '-e', "# DON'T USE$"]),
                      ("grep -e \"# DON'T USE\\$\"",
                       'grep -e \'# DON\'"\'"\'T USE$\''))


# expand_path() is tested by tests.test_conf.CombineAndExpandPathsTestCase


class FileExtTestCase(unittest.TestCase):

    def test_file_ext(self):
        self.assertEqual(file_ext('foo.zip'), '.zip')
        self.assertEqual(file_ext('foo.Z'), '.Z')
        self.assertEqual(file_ext('foo.tar.gz'), '.tar.gz')
        self.assertEqual(file_ext('README'), '')
        self.assertEqual(file_ext('README,v'), '')
        self.assertEqual(file_ext('README.txt,v'), '.txt,v')


class OptionScrapingTestCase(unittest.TestCase):

    def setUp(self):
        self.setup_options()

    def setup_options(self):
        self.original_parser = optparse.OptionParser(
            usage="don't", description='go away')
        self.original_group = optparse.OptionGroup(self.original_parser, '?')
        self.original_parser.add_option_group(self.original_group)

        self.original_parser.add_option(
            '-b', '--no-a', dest='a', action='store_false')
        self.original_parser.add_option(
            '-a', '--yes-a', dest='a', action='store_true', default=False)
        self.original_group.add_option('-x', '--xx', dest='x', action='store')
        self.original_group.add_option('-y', '--yy', dest='y', action='store')

        self.new_parser = optparse.OptionParser()
        self.new_group_1 = optparse.OptionGroup(self.new_parser, '?')
        self.new_group_2 = optparse.OptionGroup(self.new_parser, '?')
        self.new_parser.add_option_group(self.new_group_1)
        self.new_parser.add_option_group(self.new_group_2)

    def test_scrape_all(self):
        assignments = {
            self.new_parser: ('a',),
            self.new_group_1: ('x', 'y'),
        }
        old_groups = (self.original_parser, self.original_group)
        scrape_options_into_new_groups(old_groups, assignments)
        self.assertEqual(self.original_parser.option_list[1:],
                         self.new_parser.option_list[1:])
        self.assertEqual(self.original_group.option_list,
                         self.new_group_1.option_list)

    def test_scrape_different(self):
        assignments = {
            self.new_parser: ('x',),
            self.new_group_1: ('y',),
            self.new_group_2: ('a',),
        }
        old_groups = (self.original_parser, self.original_group)
        scrape_options_into_new_groups(old_groups, assignments)
        target_1 = self.original_group.option_list[:1]
        target_2 = self.original_group.option_list[1:]
        target_3 = self.original_parser.option_list[1:]
        self.assertEqual(target_1, self.new_parser.option_list[1:])
        self.assertEqual(target_2, self.new_group_1.option_list)
        self.assertEqual(target_3, self.new_group_2.option_list)
        options, args = self.new_parser.parse_args(['-x', 'happy'])
        self.assertEqual(options.x, 'happy')

    def test_parse_and_save_simple(self):
        args = ['x.py', '-b', '-a', '--no-a', '-x', 'x', '-y', 'y', '-x', 'z']
        self.assertEqual(
            dict(parse_and_save_options(self.original_parser, args)),
            {
                'a': ['-b', '-a', '--no-a'],
                'x': ['-x', 'x', '-x', 'z'],
                'y': ['-y', 'y']
            })

    def test_parse_and_save_with_dashes(self):
        args = ['x.py', '-b', '-a', '--no-a', '-x', 'x', '-y', 'y', '-x', 'z',
                '--', 'ignore', 'these', 'args']
        self.assertEqual(
            dict(parse_and_save_options(self.original_parser, args)),
            {
                'a': ['-b', '-a', '--no-a'],
                'x': ['-x', 'x', '-x', 'z'],
                'y': ['-y', 'y']
            })


class ReadInputTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.setup_tmpdir_with_beaver_data()

    @classmethod
    def tearDownClass(cls):
        cls.delete_tmpdir()

    # we're going to put the same data in every file, so we don't
    # have to worry about ordering
    BEAVER_DATA = 'Beavers mate for life.\n'

    @classmethod
    def setup_tmpdir_with_beaver_data(self):
        self.tmpdir = tempfile.mkdtemp()

        def write_beaver_data_and_close(f):
            f.write(self.BEAVER_DATA)
            f.close()

        write_beaver_data_and_close(
            open(os.path.join(self.tmpdir, 'beavers.txt'), 'w'))
        write_beaver_data_and_close(
            gzip.GzipFile(os.path.join(self.tmpdir, 'beavers.gz'), 'w'))
        write_beaver_data_and_close(
            bz2.BZ2File(os.path.join(self.tmpdir, 'beavers.bz2'), 'w'))

        os.mkdir(os.path.join(self.tmpdir, 'beavers'))
        write_beaver_data_and_close(
            open(os.path.join(self.tmpdir, 'beavers/README.txt'), 'w'))

    @classmethod
    def delete_tmpdir(self):
        shutil.rmtree(self.tmpdir)

    def test_stdin(self):
        lines = read_input('-', stdin=StringIO(self.BEAVER_DATA))
        self.assertEqual(list(lines), [self.BEAVER_DATA])

    def test_stdin_can_be_iterator(self):
        lines = read_input('-', stdin=[self.BEAVER_DATA] * 5)
        self.assertEqual(list(lines), [self.BEAVER_DATA] * 5)

    def test_normal_file(self):
        lines = read_input(os.path.join(self.tmpdir, 'beavers'))
        self.assertEqual(list(lines), [self.BEAVER_DATA])

    def test_gz_file(self):
        lines = read_input(os.path.join(self.tmpdir, 'beavers.gz'))
        self.assertEqual(list(lines), [self.BEAVER_DATA])

    def test_bz2_file(self):
        lines = read_input(os.path.join(self.tmpdir, 'beavers.bz2'))
        self.assertEqual(list(lines), [self.BEAVER_DATA])

    def test_glob(self):
        lines = read_input(os.path.join(self.tmpdir, 'beavers.*'))
        self.assertEqual(list(lines), [self.BEAVER_DATA] * 3)

    def test_dir(self):
        lines = read_input(os.path.join(self.tmpdir, 'beavers/'))
        self.assertEqual(list(lines), [self.BEAVER_DATA])

    def test_dir_recursion(self):
        lines = read_input(self.tmpdir)
        self.assertEqual(list(lines), [self.BEAVER_DATA] * 4)

    def test_glob_including_dir(self):
        lines = read_input(os.path.join(self.tmpdir, 'beavers*'))
        self.assertEqual(list(lines), [self.BEAVER_DATA] * 4)

    def test_bad_path(self):
        # read_input is a generator, so we won't get an error
        # until we try to read from it
        self.assertRaises(IOError, list,
                          read_input(os.path.join(self.tmpdir, 'lions')))

    def test_bad_glob(self):
        # read_input is a generator, so we won't get an error
        # until we try to read from it
        self.assertRaises(IOError, list,
                          read_input(os.path.join(self.tmpdir, 'lions*')))


class SafeEvalTestCase(unittest.TestCase):

    def test_simple_data_structure(self):
        # try unrepr-ing a bunch of simple data structures
        for x in True, None, 1, range(5), {'foo': False, 'bar': 2}:
            self.assertEqual(x, safeeval(repr(x)))

    def test_no_mischief(self):
        # make sure we can't do mischief
        self.assertRaises(NameError, safeeval, "open('/tmp')")

    def test_globals_and_locals(self):
        # test passing in globals, locals
        a = -0.2
        self.assertEqual(
            abs(a),
            safeeval('abs(a)', globals={'abs': abs}, locals={'a': a}))


class ArchiveTestCase(unittest.TestCase):

    def setUp(self):
        self.setup_tmp_dir()

    def tearDown(self):
        self.rm_tmp_dir()

    def setup_tmp_dir(self):
        join = os.path.join

        self.tmp_dir = tempfile.mkdtemp()

        os.mkdir(join(self.tmp_dir, 'a'))  # contains files to archive

        # create a/foo
        with open(join(self.tmp_dir, 'a', 'foo'), 'w') as foo:
            foo.write('FOO\n')

        # a/bar symlinks to a/foo
        os.symlink('foo', join(self.tmp_dir, 'a', 'bar'))

        # create a/baz; going to filter this out
        with open(join(self.tmp_dir, 'a', 'baz'), 'w') as baz:
            baz.write('BAZ\n')

        # create a/qux/quux
        os.mkdir(join(self.tmp_dir, 'a', 'qux'))
        with open(join(self.tmp_dir, 'a', 'qux', 'quux'), 'w') as quux:
            quux.write('QUUX\n')

    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def ensure_expected_results(self, added_files=[], excluded_files=[]):
        join = os.path.join

        # make sure the files we expect are there
        expected_files = ['bar', 'baz', 'foo', 'qux']
        expected_files = (set(expected_files + added_files) -
                          set(excluded_files))

        # Python <= 2.5 inserts this into tarballs by default and doesn't strip
        # it out again when unarchiving. Don't let it mess up our tests.
        no_pax = lambda paths: (x for x in paths if x != 'PaxHeader')

        self.assertEqual(
            sorted(no_pax(os.listdir(join(self.tmp_dir, 'b')))),
            sorted(expected_files))

        self.assertEqual(
            list(no_pax(os.listdir(join(self.tmp_dir, 'b', 'qux')))), ['quux'])

        # make sure their contents are intact
        with open(join(self.tmp_dir, 'b', 'foo')) as foo:
            self.assertEqual(foo.read(), 'FOO\n')

        with open(join(self.tmp_dir, 'b', 'bar')) as bar:
            self.assertEqual(bar.read(), 'FOO\n')

        with open(join(self.tmp_dir, 'b', 'qux', 'quux')) as quux:
            self.assertEqual(quux.read(), 'QUUX\n')

        # make sure symlinks are converted to files
        assert os.path.isfile(join(self.tmp_dir, 'b', 'bar'))
        assert not os.path.islink(join(self.tmp_dir, 'b', 'bar'))

    def test_tar_and_gzip(self):
        join = os.path.join

        # tar it up, and put it in subdirectory (b/)
        tar_and_gzip(dir=join(self.tmp_dir, 'a'),
                     out_path=join(self.tmp_dir, 'a.tar.gz'),
                     filter=lambda path: not path.endswith('z'),
                     prefix='b')

        # untar it into b/
        t = tarfile.open(join(self.tmp_dir, 'a.tar.gz'), 'r:gz')
        t.extractall(self.tmp_dir)
        t.close()

        self.ensure_expected_results(excluded_files=['baz'])

    def test_extract_dir_for_tar(self):
        join = os.path.join
        tar_and_gzip(dir=join(self.tmp_dir, 'a'),
                     out_path=join(self.tmp_dir, 'not_a.tar.gz'),
                     prefix='b')

        self.assertEqual(
            extract_dir_for_tar(join(self.tmp_dir, 'not_a.tar.gz')),
            'b')

    def archive_and_unarchive(self, extension, archive_template,
                              added_files=[]):
        join = os.path.join

        # archive it up
        archive_name = 'a.' + extension
        variables = dict(archive_name=join('..', archive_name),
                         files_to_archive='.')
        archive_command = [arg % variables for arg in archive_template]

        # sometime the relevant command isn't available or doesn't work;
        # if so, skip the test
        proc = Popen(archive_command, cwd=join(self.tmp_dir, 'a'),
                     stdout=PIPE, stderr=PIPE)
        proc.communicate()  # discard output
        if proc.returncode != 0:
            self.skipTest("Can't run command to create archive.")

        # unarchive it into b/
        unarchive(join(self.tmp_dir, archive_name), join(self.tmp_dir, 'b'))

        self.ensure_expected_results(added_files=added_files)

    def test_unarchive_tar(self):
        # this test requires that tar is present
        self.archive_and_unarchive(
            'tar',
            ['tar', 'chf', '%(archive_name)s', '%(files_to_archive)s'])

    def test_unarchive_tar_gz(self):
        # this test requires that tar is present and supports the "z" option
        self.archive_and_unarchive(
            'tar.gz',
            ['tar', 'czhf', '%(archive_name)s', '%(files_to_archive)s'])

    def test_unarchive_tar_bz2(self):
        # this test requires that tar is present and supports the "j" option
        self.archive_and_unarchive(
            'tar.bz2',
            ['tar', 'cjhf', '%(archive_name)s', '%(files_to_archive)s'])

    def test_unarchive_jar(self):
        # this test requires that jar is present
        self.archive_and_unarchive(
            'jar',
            ['jar', 'cf', '%(archive_name)s', '%(files_to_archive)s'],
                                   added_files=['META-INF'])

    def test_unarchive_zip(self):
        # this test requires that zip is present
        self.archive_and_unarchive('zip', ['zip', '-qr',
                                   '%(archive_name)s', '%(files_to_archive)s'])

    def test_unarchive_non_archive(self):
        join = os.path.join

        self.assertRaises(
            IOError,
            unarchive, join(self.tmp_dir, 'a', 'foo'), join(self.tmp_dir, 'b'))


class read_fileTest(unittest.TestCase):

    def setUp(self):
        self.make_tmp_dir()

    def tearDown(self):
        self.rm_tmp_dir()

    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_read_file_uncompressed(self):
        input_path = os.path.join(self.tmp_dir, 'input')
        with open(input_path, 'w') as input_file:
            input_file.write('bar\nfoo\n')

        output = []
        for line in read_file(input_path):
            output.append(line)

        self.assertEqual(output, ['bar\n', 'foo\n'])

    def test_read_file_uncompressed_stream(self):
        input_path = os.path.join(self.tmp_dir, 'input')
        with open(input_path, 'w') as input_file:
            input_file.write('bar\nfoo\n')

        output = []
        for line in read_file(input_path, fileobj=open(input_path)):
            output.append(line)

        self.assertEqual(output, ['bar\n', 'foo\n'])

    def test_read_file_compressed(self):
        input_gz_path = os.path.join(self.tmp_dir, 'input.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'w')
        input_gz.write('foo\nbar\n')
        input_gz.close()

        output = []
        for line in read_file(input_gz_path):
            output.append(line)

        self.assertEqual(output, ['foo\n', 'bar\n'])

        input_bz2_path = os.path.join(self.tmp_dir, 'input.bz2')
        input_bz2 = bz2.BZ2File(input_bz2_path, 'w')
        input_bz2.write('bar\nbar\nfoo\n')
        input_bz2.close()

        output = []
        for line in read_file(input_bz2_path):
            output.append(line)

        self.assertEqual(output, ['bar\n', 'bar\n', 'foo\n'])

    def test_cat_compressed_stream(self):
        input_gz_path = os.path.join(self.tmp_dir, 'input.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'w')
        input_gz.write('foo\nbar\n')
        input_gz.close()

        output = []
        for line in read_file(input_gz_path, fileobj=open(input_gz_path)):
            output.append(line)

        self.assertEqual(output, ['foo\n', 'bar\n'])

        input_bz2_path = os.path.join(self.tmp_dir, 'input.bz2')
        input_bz2 = bz2.BZ2File(input_bz2_path, 'w')
        input_bz2.write('bar\nbar\nfoo\n')
        input_bz2.close()

        output = []
        for line in read_file(input_bz2_path, fileobj=open(input_bz2_path)):
            output.append(line)

        self.assertEqual(output, ['bar\n', 'bar\n', 'foo\n'])
