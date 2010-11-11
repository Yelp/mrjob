# Copyright 2009-2010 Yelp
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
import os
import shutil
import stat
from subprocess import Popen, PIPE, CalledProcessError, check_call
from StringIO import StringIO
import tarfile
import tempfile
from testify import TestCase, assert_equal, assert_in, assert_raises, class_setup, class_teardown, setup, teardown

from mrjob.util import *

class CmdLineTestCase(TestCase):

    def test_cmd_line(self):
        assert_equal(cmd_line(['cut', '-f', 2, '-d', ' ']),
                     "cut -f 2 -d ' '")
        assert_in(cmd_line(['grep', '-e', "# DON'T USE$"]),
                  ("grep -e \"# DON'T USE\\$\"",
                   'grep -e \'# DON\'"\'"\'T USE$\''))

class FileExtTestCase(TestCase):

    def test_file_ext(self):
        assert_equal(file_ext('foo.zip'), '.zip')
        assert_equal(file_ext('foo.Z'), '.Z')
        assert_equal(file_ext('foo.tar.gz'), '.tar.gz')
        assert_equal(file_ext('README'), '')
        assert_equal(file_ext('README,v'), '')
        assert_equal(file_ext('README.txt,v'), '.txt,v')


class ReadInputTestCase(TestCase):

    # we're going to put the same data in every file, so we don't
    # have to worry about ordering
    BEAVER_DATA = 'Beavers mate for life.\n'

    @class_setup
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

    @class_teardown
    def delete_tmpdir(self):
        shutil.rmtree(self.tmpdir)

    def test_stdin(self):
        lines = read_input('-', stdin=StringIO(self.BEAVER_DATA))
        assert_equal(list(lines), [self.BEAVER_DATA])

    def test_stdin_can_be_iterator(self):
        lines = read_input('-', stdin=[self.BEAVER_DATA] * 5)
        assert_equal(list(lines), [self.BEAVER_DATA] * 5)

    def test_normal_file(self):
        lines = read_input(os.path.join(self.tmpdir, 'beavers'))
        assert_equal(list(lines), [self.BEAVER_DATA])

    def test_gz_file(self):
        lines = read_input(os.path.join(self.tmpdir, 'beavers.gz'))
        assert_equal(list(lines), [self.BEAVER_DATA])

    def test_bz2_file(self):
        lines = read_input(os.path.join(self.tmpdir, 'beavers.bz2'))
        assert_equal(list(lines), [self.BEAVER_DATA])

    def test_glob(self):
        lines = read_input(os.path.join(self.tmpdir, 'beavers.*'))
        assert_equal(list(lines), [self.BEAVER_DATA] * 3)

    def test_dir(self):
        lines = read_input(os.path.join(self.tmpdir, 'beavers/'))
        assert_equal(list(lines), [self.BEAVER_DATA])

    def test_dir_recursion(self):
        lines = read_input(self.tmpdir)
        assert_equal(list(lines), [self.BEAVER_DATA] * 4)

    def test_glob_including_dir(self):
        lines = read_input(os.path.join(self.tmpdir, 'beavers*'))
        assert_equal(list(lines), [self.BEAVER_DATA] * 4)

    def test_bad_path(self):
        # read_input is a generator, so we won't get an error
        # until we try to read from it
        assert_raises(IOError, list,
                      read_input(os.path.join(self.tmpdir, 'lions')))

    def test_bad_glob(self):
        # read_input is a generator, so we won't get an error
        # until we try to read from it
        assert_raises(IOError, list,
                      read_input(os.path.join(self.tmpdir, 'lions*')))


class SafeEvalTestCase(TestCase):
    
    def test_simple_data_structure(self):
        # try unrepr-ing a bunch of simple data structures
        for x in True, None, 1, range(5), {'foo': False, 'bar': 2}:
            assert_equal(x, safeeval(repr(x)))

    def test_no_mischief(self):
        # make sure we can't do mischief
        assert_raises(NameError, safeeval, "open('/tmp')")

    def test_globals_and_locals(self):
        # test passing in globals, locals
        a = -0.2
        assert_equal(abs(a), safeeval('abs(a)', globals={'abs': abs}, locals={'a': a}))

class ArchiveTestCase(TestCase):

    @setup
    def setup_tmp_dir(self):
        join = os.path.join

        self.tmp_dir = tempfile.mkdtemp()

        os.mkdir(join(self.tmp_dir, 'a')) # contains files to archive

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

    @teardown
    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def ensure_expected_results(self, added_files=[], excluded_files=[]):
        join = os.path.join

        # make sure the files we expect are there
        expected_files = ['bar', 'baz', 'foo', 'qux']
        expected_files = set(expected_files + added_files) - set(excluded_files)
        assert_equal(sorted(os.listdir(join(self.tmp_dir, 'b'))),
                     sorted(expected_files))
        assert_equal(os.listdir(join(self.tmp_dir, 'b', 'qux')),
                     ['quux'])

        # make sure their contents are intact
        with open(join(self.tmp_dir, 'b', 'foo')) as foo:
            assert_equal(foo.read(), 'FOO\n')

        with open(join(self.tmp_dir, 'b', 'bar')) as bar:
            assert_equal(bar.read(), 'FOO\n')

        with open(join(self.tmp_dir, 'b', 'qux', 'quux')) as quux:
            assert_equal(quux.read(), 'QUUX\n')

        # make sure symlinks are converted to files
        assert os.path.isfile(join(self.tmp_dir, 'b', 'bar'))
        assert not os.path.islink(join(self.tmp_dir, 'b', 'bar'))

    def test_tar_and_gz(self):
        join = os.path.join

        # tar it up
        tar_and_gzip(dir=join(self.tmp_dir, 'a'),
                     out_path=join(self.tmp_dir, 'a.tar.gz'),
                     filter=lambda path: not path.endswith('z'))

        # untar it into b
        os.mkdir(join(self.tmp_dir, 'b'))
        t = tarfile.open(join(self.tmp_dir, 'a.tar.gz'), 'r:gz')
        t.extractall(join(self.tmp_dir, 'b'))
        t.close()

        self.ensure_expected_results(excluded_files=['baz'])

    def archive_and_unarchive(self, extension, archive_template, added_files=[]):
        join = os.path.join

        # archive it up
        archive_name = 'a.' + extension
        variables = dict(archive_name=join('..', archive_name), files_to_archive='.')
        archive_command = [arg % variables for arg in archive_template]
        check_call(archive_command, cwd=join(self.tmp_dir, 'a'))

        # unarchive it into b
        unarchive(join(self.tmp_dir, archive_name), join(self.tmp_dir, 'b'))

        self.ensure_expected_results(added_files=added_files)

    def test_unarchive_tar(self):
        # this test requires that tar is present
        self.archive_and_unarchive('tar',
                                   ['tar', 'chf', '%(archive_name)s', '%(files_to_archive)s'])

    def test_unarchive_tar_gz(self):
        # this test requires that tar is present and supports the "z" option
        self.archive_and_unarchive('tar.gz',
                                   ['tar', 'czhf', '%(archive_name)s', '%(files_to_archive)s'])

    def test_unarchive_tar_bz2(self):
        # this test requires that tar is present and supports the "j" option
        self.archive_and_unarchive('tar.bz2',
                                   ['tar', 'cjhf', '%(archive_name)s', '%(files_to_archive)s'])

    def test_unarchive_jar(self):
        # this test requires that jar is present
        self.archive_and_unarchive('jar',
                                   ['jar', 'cf', '%(archive_name)s', '%(files_to_archive)s'],
                                   added_files=['META-INF'])

    def test_unarchive_zip(self):
        # this test requires that zip is present
        self.archive_and_unarchive('zip', ['zip', '-qr',
                                   '%(archive_name)s', '%(files_to_archive)s'])

    def test_unarchive_non_archive(self):
        join = os.path.join

        assert_raises(IOError, unarchive, join(self.tmp_dir, 'a', 'foo'), join(self.tmp_dir, 'b'))
