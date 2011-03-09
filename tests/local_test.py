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

"""Tests for LocalMRJobRunner"""

from __future__ import with_statement

from StringIO import StringIO
import gzip
import mrjob
import os
import shutil
import signal
import sys
from testify import TestCase, assert_in, assert_equal, assert_not_equal, assert_not_in, setup, teardown
import tempfile

from mrjob.conf import dump_mrjob_conf
from mrjob.local import LocalMRJobRunner
from mrjob.util import cmd_line
from tests.mr_job_where_are_you import MRJobWhereAreYou
from tests.mr_two_step_job import MRTwoStepJob
from tests.mr_verbose_job import MRVerboseJob
from tests.quiet import no_handlers_for_logger


class LocalMRJobRunnerEndToEndTestCase(TestCase):

    @setup
    def make_tmp_dir_and_mrjob_conf(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.mrjob_conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')
        dump_mrjob_conf({'runners': {'local': {}}},
                        open(self.mrjob_conf_path, 'w'))

    @teardown
    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_end_to_end(self):
        # read from STDIN, a regular file, and a .gz
        stdin = StringIO('foo\nbar\n')

        input_path = os.path.join(self.tmp_dir, 'input')
        with open(input_path, 'w') as input_file:
            input_file.write('bar\nqux\n')

        input_gz_path = os.path.join(self.tmp_dir, 'input.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'w')
        input_gz.write('foo\n')
        input_gz.close()

        mr_job = MRTwoStepJob(['-c', self.mrjob_conf_path,
                               '-', input_path, input_gz_path])
        mr_job.sandbox(stdin=stdin)

        local_tmp_dir = None
        results = []

        with mr_job.make_runner() as runner:
            assert isinstance(runner, LocalMRJobRunner)
            runner.run()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

            local_tmp_dir = runner._get_local_tmp_dir()
            assert os.path.exists(local_tmp_dir)

        # make sure cleanup happens
        assert not os.path.exists(local_tmp_dir)

        assert_equal(sorted(results),
                     [(1, 'qux'), (2, 'bar'), (2, 'foo'), (5, None)])


class LocalMRJobRunnerNoSymlinksTestCase(LocalMRJobRunnerEndToEndTestCase):
    """Test systems without os.symlink (e.g. Windows). See Issue #46"""

    @setup
    def remove_os_symlink(self):
        if hasattr(os, 'symlink'):
            self._real_os_symlink = os.symlink
            del os.symlink # sorry, were you using that? :)

    @teardown
    def restore_os_symlink(self):
        if hasattr(self, '_real_os_symlink'):
            os.symlink = self._real_os_symlink


class TimeoutException(Exception):
    pass


class LargeAmountsOfStderrTestCase(TestCase):

    @setup
    def set_alarm(self):
        # if the test fails, it'll stall forever, so set an alarm
        def alarm_handler():
            raise TimeoutException('Stalled on large amounts of stderr; probably pipe buffer is full.')
        self._old_alarm_handler = signal.signal(signal.SIGALRM, alarm_handler)
        signal.alarm(10)

    @teardown
    def restore_old_alarm_handler(self):
        signal.alarm(0)
        signal.signal(signal.SIGALRM, self._old_alarm_handler)

    def test_large_amounts_of_stderr(self):
        mr_job = MRVerboseJob(['--no-conf'])
        mr_job.sandbox()

        try:
            with no_handlers_for_logger():
                mr_job.run_job()
        except TimeoutException:
            raise
        except Exception, e:
            # we expect the job to throw an exception

            # look for expected output from MRVerboseJob
            stderr = mr_job.stderr.getvalue()
            assert_in("counters: {'Foo': {'Bar': 10000}}\n", stderr)
            assert_in('status: 0\n', stderr)
            assert_in('status: 99\n', stderr)
            assert_not_in('status: 100\n', stderr)
            assert_in('STDERR: Qux\n', stderr)
            # exception should appear in exception message
            assert_in('BOOM', e.message)
        else:
            raise AssertionError()


class PythonBinTestCase(TestCase):

    def test_echo_as_python_bin(self):
        # "echo" is a pretty poor substitute for Python, but it
        # should be available on most systems
        mr_job = MRTwoStepJob(['--python-bin', 'echo', '--no-conf'])
        mr_job.sandbox()
        
        with mr_job.make_runner() as runner:
            assert isinstance(runner, LocalMRJobRunner)
            runner.run()
            output = ''.join(runner.stream_output())

        # the output should basically be the command we used to
        # run the last step, which in this case is a mapper
        assert_in('mr_two_step_job.py', output)
        assert_in('--step-num=1', output)
        assert_in('--mapper', output)
 
    def test_python_dash_v_as_python_bin(self):
        python_cmd = cmd_line([sys.executable or 'python', '-v'])
        mr_job = MRTwoStepJob(['--python-bin', python_cmd, '--no-conf'])
        mr_job.sandbox(stdin=['bar\n'])

        with no_handlers_for_logger():
            mr_job.run_job()

        # expect debugging messages in stderr
        assert_in('import mrjob', mr_job.stderr.getvalue())
        assert_in('#', mr_job.stderr.getvalue())

        # should still get expected results
        assert_equal(sorted(mr_job.parse_output()), [(1, None), (1, 'bar')])


class LocalBootstrapMrjobTestCase(TestCase):

    @setup
    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    @teardown
    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def test_loading_boostrapped_mrjob_library(self):
        # track the dir we're loading mrjob from rather than the full path
        # to deal with edge cases where we load from the .py file,
        # and the script loads from the .pyc compiled from that .py file.
        our_mrjob_dir = os.path.dirname(os.path.realpath(mrjob.__file__))

        mr_job = MRJobWhereAreYou(['--no-conf'])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            # sanity check
            assert_equal(runner.get_opts()['bootstrap_mrjob'], True)
            local_tmp_dir = os.path.realpath(runner._get_local_tmp_dir())

            runner.run()

            output = list(runner.stream_output())
            assert_equal(len(output), 1)

            # script should load mrjob from its working dir
            _, script_mrjob_dir = mr_job.parse_output_line(output[0])

            assert_not_equal(our_mrjob_dir, script_mrjob_dir)
            assert script_mrjob_dir.startswith(local_tmp_dir)

    def test_can_turn_off_bootstrap_mrjob(self):
        # track the dir we're loading mrjob from rather than the full path
        # to deal with edge cases where we load from the .py file,
        # and the script loads from the .pyc compiled from that .py file.
        our_mrjob_dir = os.path.dirname(os.path.realpath(mrjob.__file__))

        self.mrjob_conf_path = os.path.join(self.tmp_dir, 'mrjob.conf')
        dump_mrjob_conf({'runners': {'local': {'bootstrap_mrjob': False}}},
                        open(self.mrjob_conf_path, 'w'))

        mr_job = MRJobWhereAreYou(['-c', self.mrjob_conf_path])
        mr_job.sandbox()

        with mr_job.make_runner() as runner:
            # sanity check
            assert_equal(runner.get_opts()['bootstrap_mrjob'], False)
            runner.run()

            output = list(runner.stream_output())

            assert_equal(len(output), 1)

            # script should load mrjob from the same place our test does
            _, script_mrjob_dir = mr_job.parse_output_line(output[0])
            assert_equal(our_mrjob_dir, script_mrjob_dir)
