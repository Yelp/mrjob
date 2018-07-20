# -*- coding: utf-8 -*-
# Copyright 2011 Matthew Tai
# Copyright 2012 Yelp
# Copyright 2013 Yelp and Lyft
# Copyright 2014 Marc Abramowitz
# Copyright 2015-2017 Yelp
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
"""Tests for InlineMRJobRunner"""
import gzip
import os
import os.path
from io import BytesIO
from unittest import TestCase

from mrjob import conf
from mrjob.fs.base import Filesystem
from mrjob.inline import InlineMRJobRunner
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol
from mrjob.sim import _error_on_bad_paths
from mrjob.step import MRStep
from tests.mr_no_mapper import MRNoMapper
from tests.mr_test_cmdenv import MRTestCmdenv
from tests.mr_test_jobconf import MRTestJobConf
from tests.mr_test_per_step_jobconf import MRTestPerStepJobConf
from tests.mr_two_step_job import MRTwoStepJob
from tests.mr_word_count import MRWordCount
from tests.py2 import mock
from tests.py2 import patch
from tests.sandbox import EmptyMrjobConfTestCase
from tests.sandbox import SandboxedTestCase


class InlineMRJobRunnerEndToEndTestCase(SandboxedTestCase):

    def test_end_to_end(self):
        # read from STDIN, a regular file, and a .gz
        stdin = BytesIO(b'foo\nbar\n')

        input_path = os.path.join(self.tmp_dir, 'input')
        with open(input_path, 'w') as input_file:
            input_file.write('bar\nqux\n')

        input_gz_path = os.path.join(self.tmp_dir, 'input.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'wb')
        input_gz.write(b'foo\n')
        input_gz.close()

        mr_job = MRTwoStepJob(
            ['--runner', 'inline', '-', input_path, input_gz_path])
        mr_job.sandbox(stdin=stdin)

        local_tmp_dir = None
        results = []

        with mr_job.make_runner() as runner:
            assert isinstance(runner, InlineMRJobRunner)
            runner.run()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

            local_tmp_dir = runner._get_local_tmp_dir()
            assert os.path.exists(local_tmp_dir)

        # make sure cleanup happens
        assert not os.path.exists(local_tmp_dir)

        self.assertEqual(sorted(results),
                         [(1, 'qux'), (2, 'bar'), (2, 'foo'), (5, None)])

    def test_missing_input(self):
        runner = InlineMRJobRunner(input_paths=['/some/bogus/file/path'])
        self.assertRaises(Exception, runner._run)


class InlineMRJobRunnerCmdenvTest(EmptyMrjobConfTestCase):

    def test_cmdenv(self):
        import logging
        logging.basicConfig()
        # make sure previous environment is preserved
        os.environ['SOMETHING'] = 'foofoofoo'
        old_env = os.environ.copy()

        mr_job = MRTestCmdenv(['--runner', 'inline', '--cmdenv=FOO=bar'])
        mr_job.sandbox(stdin=BytesIO(b'foo\n'))

        results = []

        with mr_job.make_runner() as runner:
            assert isinstance(runner, InlineMRJobRunner)
            runner.run()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

        self.assertEqual(sorted(results),
                         [('FOO', 'bar'), ('SOMETHING', 'foofoofoo')])

        # make sure we revert back
        self.assertEqual(old_env, os.environ)


# this doesn't need to be in its own file because it'll be run inline
class MRIncrementerJob(MRJob):
    """A terribly silly way to add a positive integer to values."""

    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol

    def configure_options(self):
        super(MRIncrementerJob, self).configure_options()

        self.add_passthrough_option('--times', type='int', default=1)

    def mapper(self, _, value):
        yield None, value + 1

    def steps(self):
        return [MRStep(mapper=self.mapper)] * self.options.times


class MRCustomFileOptionJob(MRJob):
    """ A simple MRJob that uses the input file option."""

    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol

    multiplier = 1

    def configure_options(self):
        super(MRCustomFileOptionJob, self).configure_options()
        self.add_file_option('--platform_file')

    def mapper_init(self):
        with open(self.options.platform_file) as f:
            self.multiplier = int(f.read())

    def mapper(self, _, value):
        yield None, value * self.multiplier


class InlineRunnerStepsTestCase(EmptyMrjobConfTestCase):
    # make sure file options get passed to --steps in inline mode

    def test_adding_2(self):
        mr_job = MRIncrementerJob(['-r', 'inline', '--times', '2'])
        mr_job.sandbox(stdin=BytesIO(b'0\n1\n2\n'))

        self.assertEqual(len(mr_job.steps()), 2)

        with mr_job.make_runner() as runner:
            assert isinstance(runner, InlineMRJobRunner)
            self.assertEqual(runner._get_steps(), [
                {
                    'type': 'streaming',
                    'mapper': {
                        'type': 'script',
                    }
                },
                {
                    'type': 'streaming',
                    'mapper': {
                        'type': 'script',
                    }
                },
            ])

            runner.run()

            output = sorted(mr_job.parse_output_line(line)[1]
                            for line in runner.stream_output())

            self.assertEqual(output, [2, 3, 4])


class MRJobFileOptionsTestCase(SandboxedTestCase):

    def setUp(self):
        super(MRJobFileOptionsTestCase, self).setUp()

        self.input_file_path = os.path.join(self.tmp_dir, 'input_file.txt')
        with open(self.input_file_path, 'wb') as f:
            f.write(b'2\n')

    def test_with_input_file_option(self):
        mr_job = MRCustomFileOptionJob(
            ['-r', 'inline', '--platform_file', self.input_file_path])
        mr_job.sandbox(stdin=BytesIO(b'1\n'))

        with mr_job.make_runner() as runner:
            runner.run()
            output = sorted(mr_job.parse_output_line(line)[1]
                            for line in runner.stream_output())

            self.assertEqual(output, [2])


class NoMRJobConfTestCase(TestCase):

    def test_no_mrjob_confs(self):
        with patch.object(
                conf, '_expanded_mrjob_conf_path', return_value=None):

            mr_job = MRIncrementerJob(['-r', 'inline', '--times', '2'])
            mr_job.sandbox(stdin=BytesIO(b'0\n1\n2\n'))

            with mr_job.make_runner() as runner:
                runner.run()
                output = sorted(mr_job.parse_output_line(line)[1]
                                for line in runner.stream_output())
                self.assertEqual(output, [2, 3, 4])


class InlineMRJobRunnerJobConfTestCase(SandboxedTestCase):

    # this class is also used to test local mode
    RUNNER = 'inline'

    def test_input_files_and_setting_number_of_tasks(self):
        input_path = os.path.join(self.tmp_dir, 'input')
        with open(input_path, 'wb') as input_file:
            input_file.write(b'bar\nqux\nfoo\n')

        input_gz_path = os.path.join(self.tmp_dir, 'input.gz')
        input_gz = gzip.GzipFile(input_gz_path, 'wb')
        input_gz.write(b'foo\n')
        input_gz.close()

        mr_job = MRWordCount(['-r', self.RUNNER,
                              '--jobconf=mapred.map.tasks=3',
                              '--jobconf=mapred.reduce.tasks=3',
                              input_path, input_gz_path])
        mr_job.sandbox()

        results = []

        with mr_job.make_runner() as runner:
            runner.run()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append((key, value))

            self.assertEqual(runner.counters()[0]['count']['combiners'], 3)

        self.assertEqual(sorted(results),
                         [(input_path, 3), (input_gz_path, 1)])

    def _extra_expected_local_files(self, runner):
        """A list of additional local files expected, as tuples
        of (path, name). Hook for dealing with cat.py in local mode."""
        return []

    def test_jobconf_simulated_by_runner(self):
        input_path = os.path.join(self.tmp_dir, 'input')
        with open(input_path, 'wb') as input_file:
            input_file.write(b'foo\n')

        upload_path = os.path.join(self.tmp_dir, 'upload')
        with open(upload_path, 'wb') as upload_file:
            upload_file.write(b'PAYLOAD')

        # use --no-bootstrap-mrjob so we don't have to worry about
        # mrjob.tar.gz and the setup wrapper script
        self.add_mrjob_to_pythonpath()
        mr_job = MRTestJobConf(['-r', self.RUNNER,
                                '--no-bootstrap-mrjob',
                                '--jobconf=user.defined=something',
                                '--jobconf=mapred.map.tasks=1',
                                '--file', upload_path,
                               input_path])

        mr_job.sandbox()

        results = {}

        # between the single line of input and setting mapred.map.tasks to 1,
        # we should be restricted to only one task, which will give more
        # predictable results

        with mr_job.make_runner() as runner:
            script_path = runner._script_path

            runner.run()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results[key] = value

        working_dir = results['mapreduce.job.local.dir']
        self.assertEqual(working_dir,
                         os.path.join(runner._get_local_tmp_dir(),
                                      'job_local_dir', '0', 'mapper', '0'))

        self.assertEqual(results['mapreduce.job.cache.archives'], '')
        expected_cache_files = [
            script_path + '#mr_test_jobconf.py',
            upload_path + '#upload'
        ] + [
            '%s#%s' % (path, name)
            for path, name in self._extra_expected_local_files(runner)
        ]
        self.assertEqual(
            sorted(results['mapreduce.job.cache.files'].split(',')),
            sorted(expected_cache_files))
        self.assertEqual(results['mapreduce.job.cache.local.archives'], '')
        expected_local_files = [
            os.path.join(working_dir, 'mr_test_jobconf.py'),
            os.path.join(working_dir, 'upload')
        ] + [
            os.path.join(working_dir, name)
            for path, name in self._extra_expected_local_files(runner)
        ]
        self.assertEqual(
            sorted(results['mapreduce.job.cache.local.files'].split(',')),
            sorted(expected_local_files))
        self.assertEqual(results['mapreduce.job.id'], runner._job_key)

        self.assertEqual(results['mapreduce.map.input.file'], input_path)
        self.assertEqual(results['mapreduce.map.input.length'], '4')
        self.assertEqual(results['mapreduce.map.input.start'], '0')
        self.assertEqual(results['mapreduce.task.attempt.id'],
                         'attempt_%s_mapper_00000_0' % runner._job_key)
        self.assertEqual(results['mapreduce.task.id'],
                         'task_%s_mapper_00000' % runner._job_key)
        self.assertEqual(results['mapreduce.task.ismap'], 'true')
        self.assertEqual(results['mapreduce.task.output.dir'],
                         runner._output_dir)
        self.assertEqual(results['mapreduce.task.partition'], '0')
        self.assertEqual(results['user.defined'], 'something')

    def test_per_step_jobconf(self):
        mr_job = MRTestPerStepJobConf([
            '-r', self.RUNNER, '--jobconf', 'user.defined=something'])
        mr_job.sandbox()

        results = {}

        with mr_job.make_runner() as runner:
            runner.run()

            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results[tuple(key)] = value

        # user.defined gets re-defined in the second step
        self.assertEqual(results[(0, 'user.defined')], 'something')
        self.assertEqual(results[(1, 'user.defined')], 'nothing')

    def test_per_step_jobconf_can_set_number_of_tasks(self):
        mr_job = MRTestPerStepJobConf([
            '-r', self.RUNNER, '--jobconf', 'mapred.map.tasks=2',
        ])
        # need at least two items of input to get two map tasks
        mr_job.sandbox(BytesIO(b'foo\nbar\n'))

        with mr_job.make_runner() as runner:
            runner.run()

            # sanity test: --jobconf should definitely work
            self.assertEqual(runner.counters()[0]['count']['mapper_init'], 2)
            # the job sets its own mapred.map.tasks to 4 for the 2nd step
            self.assertEqual(runner.counters()[1]['count']['mapper_init'], 4)


class InlineMRJobRunnerNoMapperTestCase(SandboxedTestCase):

    RUNNER = 'inline'

    # tests #1141. Also used by local mapper

    def test_step_with_no_mapper(self):
        mr_job = MRNoMapper(['-r', self.RUNNER])

        mr_job.sandbox(stdin=BytesIO(
            b'one fish two fish\nred fish blue fish\n'))

        with mr_job.make_runner() as runner:
            runner.run()

            results = [mr_job.parse_output_line(line)
                       for line in runner.stream_output()]

            self.assertEqual(sorted(results),
                             [(1, ['blue', 'one', 'red', 'two']),
                              (4, ['fish'])])


class InlineMRJobRunnerFSTestCase(SandboxedTestCase):

    RUNNER_CLASS = InlineMRJobRunner

    def setUp(self):
        super(InlineMRJobRunnerFSTestCase, self).setUp()
        self.runner = self.RUNNER_CLASS()

    def test_can_handle_paths(self):
        self.assertEqual(
            self.runner.fs.exists(os.path.join(self.tmp_dir, 'foo')), False)

    def test_cant_handle_uris(self):
        self.assertRaises(IOError, self.runner.fs.ls, 's3://walrus/foo')


class ErrorOnBadPathsTestCase(TestCase):

    def setUp(self):
        self.fs = mock.create_autospec(Filesystem)
        self.paths = ['/one', '/two' '/three/*']

    def test_with_paths(self):
        _error_on_bad_paths(self.fs, self.paths)
        self.fs.exists.assert_called_once_with(self.paths[0])

    def test_no_paths(self):
        self.fs.exists.return_value = False
        self.assertRaises(ValueError, _error_on_bad_paths, self.fs, self.paths)
