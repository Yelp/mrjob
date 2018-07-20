# Copyright 2009-2015 Yelp and Contributors
# Copyright 2016-2017 Yelp
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
import bz2
import gzip
import optparse
import os
import shutil
import sys
import tarfile
import tempfile
from io import BytesIO
from subprocess import PIPE
from subprocess import Popen
from unittest import TestCase

from mrjob.py2 import PY2
from mrjob.py2 import StringIO
from mrjob.util import buffer_iterator_to_line_iterator
from mrjob.util import cmd_line
from mrjob.util import file_ext
from mrjob.util import log_to_stream
from mrjob.util import parse_and_save_options
from mrjob.util import random_identifier
from mrjob.util import read_file
from mrjob.util import read_input
from mrjob.util import safeeval
from mrjob.util import scrape_options_into_new_groups
from mrjob.util import tar_and_gzip
from mrjob.util import to_lines
from mrjob.util import unarchive
from mrjob.util import unique
from mrjob.util import which

from tests.py2 import patch
from tests.quiet import no_handlers_for_logger
from tests.sandbox import SandboxedTestCase
from tests.sandbox import random_seed


class ToLinesTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(
            list(to_lines(_ for _ in ())),
            [])

    def test_buffered_lines(self):
        self.assertEqual(
            list(to_lines(chunk for chunk in
                          [b'The quick\nbrown fox\nju',
                           b'mped over\nthe lazy\ndog',
                           b's.\n'])),
            [b'The quick\n', b'brown fox\n', b'jumped over\n', b'the lazy\n',
             b'dogs.\n'])

    def test_empty_chunks(self):
        self.assertEqual(
            list(to_lines(chunk for chunk in
                          [b'',
                           b'The quick\nbrown fox\nju',
                           b'', b'', b'',
                           b'mped over\nthe lazy\ndog',
                           b'',
                           b's.\n',
                           b''])),
            [b'The quick\n', b'brown fox\n', b'jumped over\n', b'the lazy\n',
             b'dogs.\n'])

    def test_no_trailing_newline(self):
        self.assertEqual(
            list(to_lines(chunk for chunk in
                          [b'Alouette,\ngentille',
                           b' Alouette.'])),
            [b'Alouette,\n', b'gentille Alouette.'])

    def test_long_lines(self):
        super_long_line = b'a' * 10000 + b'\n' + b'b' * 1000 + b'\nlast\n'
        self.assertEqual(
            list(to_lines(
                chunk for chunk in
                (super_long_line[0 + i:1024 + i]
                 for i in range(0, len(super_long_line), 1024)))),
            [b'a' * 10000 + b'\n', b'b' * 1000 + b'\n', b'last\n'])

    def test_deprecated_alias(self):
        with no_handlers_for_logger('mrjob.util'):
            stderr = StringIO()
            log_to_stream('mrjob.util', stderr)

            self.assertEqual(
                list(buffer_iterator_to_line_iterator(
                    chunk for chunk in
                    [b'The quick\nbrown fox\njumped over\nthe lazy\ndogs.\n'])
                ),
                [b'The quick\n', b'brown fox\n', b'jumped over\n',
                 b'the lazy\n', b'dogs.\n'])

            self.assertIn('has been renamed', stderr.getvalue())


class CmdLineTestCase(TestCase):

    def test_cmd_line(self):
        self.assertEqual(cmd_line(['cut', '-f', 2, '-d', ' ']),
                         "cut -f 2 -d ' '")
        self.assertIn(cmd_line(['grep', '-e', "# DON'T USE$"]),
                      ("grep -e \"# DON'T USE\\$\"",
                       'grep -e \'# DON\'"\'"\'T USE$\''))


# expand_path() is tested by tests.test_conf.CombineAndExpandPathsTestCase


class FileExtTestCase(TestCase):

    def test_file_ext(self):
        self.assertEqual(file_ext('foo.zip'), '.zip')
        self.assertEqual(file_ext('foo.Z'), '.Z')
        self.assertEqual(file_ext('foo.tar.gz'), '.tar.gz')
        self.assertEqual(file_ext('README'), '')
        self.assertEqual(file_ext('README,v'), '')
        self.assertEqual(file_ext('README.txt,v'), '.txt,v')


class OptionScrapingTestCase(TestCase):

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
        self.original_group.add_option(
            '-y', '--yy', dest='y', action='store', nargs=2)

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
        args = ['x.py', '-b', '-a', '--no-a',
                '-x', 'x', '-y', 'y', 'ynot', '-x', 'z']

        self.assertEqual(
            dict(parse_and_save_options(self.original_parser, args)),
            {
                'a': ['-b', '-a', '--no-a'],
                'x': ['-x', 'x', '-x', 'z'],
                'y': ['-y', 'y', 'ynot']
            })

    def test_parse_and_save_with_dashes(self):
        args = ['x.py', '-b', '-a', '--no-a',
                '-x', 'x', '-y', 'y', 'ynot', '-x', 'z',
                '--', 'ignore', 'these', 'args']
        self.assertEqual(
            dict(parse_and_save_options(self.original_parser, args)),
            {
                'a': ['-b', '-a', '--no-a'],
                'x': ['-x', 'x', '-x', 'z'],
                'y': ['-y', 'y', 'ynot']
            })


class ReadInputTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.setup_tmpdir_with_beaver_data()

    @classmethod
    def tearDownClass(cls):
        cls.delete_tmpdir()

    # we're going to put the same data in every file, so we don't
    # have to worry about ordering
    BEAVER_DATA = b'Beavers mate for life.\n'

    @classmethod
    def setup_tmpdir_with_beaver_data(self):
        self.tmpdir = tempfile.mkdtemp()

        def write_beaver_data_and_close(f):
            f.write(self.BEAVER_DATA)
            f.close()

        write_beaver_data_and_close(
            open(os.path.join(self.tmpdir, 'beavers.txt'), 'wb'))
        write_beaver_data_and_close(
            gzip.GzipFile(os.path.join(self.tmpdir, 'beavers.gz'), 'wb'))
        write_beaver_data_and_close(
            bz2.BZ2File(os.path.join(self.tmpdir, 'beavers.bz2'), 'wb'))

        os.mkdir(os.path.join(self.tmpdir, 'beavers'))
        write_beaver_data_and_close(
            open(os.path.join(self.tmpdir, 'beavers/README.txt'), 'wb'))

    @classmethod
    def delete_tmpdir(self):
        shutil.rmtree(self.tmpdir)

    def test_stdin(self):
        lines = read_input('-', stdin=BytesIO(self.BEAVER_DATA))
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


class SafeEvalTestCase(TestCase):

    def test_simple_data_structures(self):
        # try unrepr-ing a bunch of simple data structures
        for x in True, None, 1, [0, 1, 2, 3, 4], {'foo': False, 'bar': 2}:
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

    def test_range_type(self):
        # ranges have different reprs on Python 2 vs. Python 3, and
        # can't be checked for equality until Python 3.3+

        if PY2:
            range_type = xrange
        else:
            range_type = range

        self.assertEqual(repr(safeeval(repr(range_type(3)))),
                         repr(range_type(3)))

        if sys.version_info >= (3, 3):
            self.assertEqual(safeeval(repr(range_type(3))),
                             range_type(3))


class ArchiveTestCase(TestCase):

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

        self.assertEqual(
            sorted(os.listdir(join(self.tmp_dir, 'b'))),
            sorted(expected_files))

        self.assertEqual(
            list(os.listdir(join(self.tmp_dir, 'b', 'qux'))), ['quux'])

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
        try:
            proc = Popen(archive_command, cwd=join(self.tmp_dir, 'a'),
                         stdout=PIPE, stderr=PIPE)
        except OSError as e:
            if e.errno == 2:
                self.skipTest("No %s command" % archive_command[0])
            else:
                raise
        proc.communicate()  # discard output
        if proc.returncode != 0:
            self.skipTest("Can't run `%s` to create archive." %
                          cmd_line(archive_command))

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


class OnlyReadWrapper(object):
    """Restrict a file object to only the read() method (used by
    ReadFileTestCase)."""

    def __init__(self, fp):
        self.fp = fp

    def read(self, *args, **kwargs):
        return self.fp.read(*args, **kwargs)


class ReadFileTestCase(TestCase):

    def setUp(self):
        self.make_tmp_dir()

    def tearDown(self):
        self.rm_tmp_dir()

    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_read_uncompressed_file(self):
        input_path = os.path.join(self.tmp_dir, 'input')
        with open(input_path, 'wb') as input_file:
            input_file.write(b'bar\nfoo\n')

        output = []
        for line in read_file(input_path):
            output.append(line)

        self.assertEqual(output, [b'bar\n', b'foo\n'])

    def test_read_uncompressed_file_from_fileobj(self):
        input_path = os.path.join(self.tmp_dir, 'input')
        with open(input_path, 'wb') as input_file:
            input_file.write(b'bar\nfoo\n')

        output = []
        with open(input_path, 'rb') as f:
            for line in read_file(input_path, fileobj=f):
                output.append(line)

        self.assertEqual(output, [b'bar\n', b'foo\n'])

    def test_read_gz_file(self):
        input_gz_path = os.path.join(self.tmp_dir, 'input.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'wb')
        input_gz.write(b'foo\nbar\n')
        input_gz.close()

        output = []
        for line in read_file(input_gz_path):
            output.append(line)

        self.assertEqual(output, [b'foo\n', b'bar\n'])

    def test_read_bz2_file(self):
        input_bz2_path = os.path.join(self.tmp_dir, 'input.bz2')
        input_bz2 = bz2.BZ2File(input_bz2_path, 'wb')
        input_bz2.write(b'bar\nbar\nfoo\n')
        input_bz2.close()

        output = []
        for line in read_file(input_bz2_path):
            output.append(line)

        self.assertEqual(output, [b'bar\n', b'bar\n', b'foo\n'])

    def test_read_large_bz2_file(self):
        # catch incorrect use of bz2 library (Issue #814)

        input_bz2_path = os.path.join(self.tmp_dir, 'input.bz2')
        input_bz2 = bz2.BZ2File(input_bz2_path, 'wb')

        # can't just repeat same value, because we need the file to be
        # compressed! 50000 lines is too few to catch the bug.
        with random_seed(0):
            for _ in range(100000):
                input_bz2.write((random_identifier() + '\n').encode('ascii'))
            input_bz2.close()

        # now expect to read back the same bytes
        with random_seed(0):
            num_lines = 0
            for line in read_file(input_bz2_path):
                self.assertEqual(line,
                                 (random_identifier() + '\n').encode('ascii'))
                num_lines += 1

            self.assertEqual(num_lines, 100000)

    def test_read_gz_file_from_fileobj(self):
        input_gz_path = os.path.join(self.tmp_dir, 'input.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'wb')
        input_gz.write(b'foo\nbar\n')
        input_gz.close()

        output = []
        with open(input_gz_path, 'rb') as f:
            for line in read_file(input_gz_path, fileobj=OnlyReadWrapper(f)):
                output.append(line)

        self.assertEqual(output, [b'foo\n', b'bar\n'])

    def test_read_bz2_file_from_fileobj(self):
        input_bz2_path = os.path.join(self.tmp_dir, 'input.bz2')
        input_bz2 = bz2.BZ2File(input_bz2_path, 'wb')
        input_bz2.write(b'bar\nbar\nfoo\n')
        input_bz2.close()

        output = []
        with open(input_bz2_path, 'rb') as f:
            for line in read_file(input_bz2_path, fileobj=OnlyReadWrapper(f)):
                output.append(line)

        self.assertEqual(output, [b'bar\n', b'bar\n', b'foo\n'])


class RandomIdentifierTestCase(TestCase):

    def test_format(self):
        with random_seed(0):
            random_id = random_identifier()
        self.assertEqual(len(random_id), 16)
        self.assertFalse(set(random_id) - set('0123456789abcdef'))

    def test_no_collisions_possible_ever(self):
        # heh
        with random_seed(0):
            self.assertNotEqual(random_identifier(), random_identifier())


class UniqueTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(list(unique([])), [])

    def test_de_duplication(self):
        self.assertEqual(list(unique([1, 2, 1, 5, 1])),
                         [1, 2, 5])

    def test_preserves_order(self):
        self.assertEqual(list(unique([6, 7, 2, 0, 7, 1])),
                         [6, 7, 2, 0, 1])

    def test_mixed_types_ok(self):
        self.assertEqual(list(unique(['a', None, 33, 'a'])),
                         ['a', None, 33])


class WhichTestCase(SandboxedTestCase):

    # which() is just a passthrough to shutil.which() and
    # distutils.spawn.find_executable, so we're really just
    # testing for consistent behavior across versions

    def setUp(self):
        super(WhichTestCase, self).setUp()

        self.shekondar_path = self.makefile('shekondar', executable=True)

    def test_explicit_path(self):
        self.assertEqual(which('shekondar', path=self.tmp_dir),
                         self.shekondar_path)

    def test_path_from_environment(self):
        with patch.dict(os.environ, PATH=self.tmp_dir):
            self.assertEqual(which('shekondar'), self.shekondar_path)

    def test_not_found(self):
        self.assertEqual(which('shekondar-the-fearsome', self.tmp_dir), None)

    def test_no_path(self):
        with patch.dict(os.environ, clear=True):
            # make sure we protect find_executable() from missing $PATH
            # on Python 2.
            self.assertEqual(which('shekondar'), None)
