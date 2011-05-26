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

"""Unit testing of MRJob."""

from __future__ import with_statement

from optparse import OptionError
import os
import shutil
from subprocess import Popen, PIPE
from StringIO import StringIO
import sys
import tempfile
from testify import TestCase, assert_equal, assert_not_equal, assert_gt, assert_raises, setup, teardown
import time

from mrjob.conf import combine_envs
from mrjob.job import MRJob, _IDENTITY_MAPPER, UsageError
from mrjob.local import LocalMRJobRunner
from mrjob.parse import parse_mr_job_stderr
from tests.mr_tower_of_powers import MRTowerOfPowers
from tests.mr_two_step_job import MRTwoStepJob


### Test classes ###

# These can't be invoked as a separate script, but they don't need to be

class MRBoringJob(MRJob):
    """It's a boring job, but somebody had to do it."""
    def mapper(self, key, value):
        yield(key, value)

    def reducer(self, key, values):
        yield(key, list(values))


class MRFinalBoringJob(MRBoringJob):
    def __init__(self, args=None):
        super(MRFinalBoringJob, self).__init__(args=args)
        self.num_lines = 0

    def mapper_final(self):
        yield('num_lines', self.num_lines)


class MRCustomBoringJob(MRBoringJob):

    def configure_options(self):
        super(MRCustomBoringJob, self).configure_options()

        self.add_passthrough_option(
            '--foo-size', '-F', type='int', dest='foo_size', default=5)
        self.add_passthrough_option(
            '--bar-name', type='string', dest='bar_name', default=None)
        self.add_passthrough_option(
            '--enable-baz-mode', action='store_true', dest='baz_mode',
            default=False)
        self.add_passthrough_option(
            '--disable-quuxing', action='store_false', dest='quuxing',
            default=True)
        self.add_passthrough_option(
            '--pill-type', type='choice', choices=(['red', 'blue']),
            default='blue')
        self.add_passthrough_option(
            '--planck-constant', type='float', default=6.626068e-34)
        self.add_passthrough_option(
            '--extra-special-arg', action='append', dest='extra_special_args',
            default=[])

        self.add_file_option('--foo-config', dest='foo_config', default=None)
        self.add_file_option('--accordian-file', dest='accordian_files',
                             action='append', default=[])


### Test cases ###

class MRTestCase(TestCase):
    # some basic testing for the mr() function
    def test_mr(self):

        def mapper(k, v): pass
        def mapper_final(k, v): pass
        def reducer(k, vs): pass

        # make sure it returns the format we currently expect
        assert_equal(MRJob.mr(mapper, reducer), (mapper, reducer))
        assert_equal(MRJob.mr(mapper, reducer, mapper_final=mapper_final),
                     ((mapper, mapper_final), reducer))
        assert_equal(MRJob.mr(mapper), (mapper, None))

    def test_no_mapper(self):
        def mapper_final(k, v): pass
        def reducer(k, vs): pass

        assert_equal(MRJob.mr(), (_IDENTITY_MAPPER, None))
        assert_equal(MRJob.mr(reducer=reducer), (_IDENTITY_MAPPER, reducer))
        assert_equal(MRJob.mr(reducer=reducer, mapper_final=mapper_final),
                     ((_IDENTITY_MAPPER, mapper_final), reducer))


class NoTzsetTestCase(TestCase):
    """Test systems without time.tzset() (e.g. Windows). See Issue #46."""

    @setup
    def remove_time_tzset(self):
        if hasattr(time, 'tzset'):
            self._real_time_tzset = time.tzset
            del time.tzset

    @teardown
    def restore_time_tzset(self):
        if hasattr(self, '_real_time_tzset'):
            time.tzset = self._real_time_tzset

    def test_init_does_not_require_tzset(self):
        mr_job = MRJob()


class CountersAndStatusTestCase(TestCase):

    def test_counters_and_status(self):
        mr_job = MRJob().sandbox()

        mr_job.increment_counter('Foo', 'Bar')
        mr_job.set_status('Initializing qux gradients...')
        mr_job.increment_counter('Foo', 'Bar')
        mr_job.increment_counter('Foo', 'Baz', 20)
        mr_job.set_status('Sorting metasyntactic variables...')

        parsed_stderr = parse_mr_job_stderr(mr_job.stderr.getvalue())

        assert_equal(parsed_stderr,
                     {'counters': {'Foo': {'Bar': 2, 'Baz': 20}},
                      'statuses': ['Initializing qux gradients...',
                                   'Sorting metasyntactic variables...'],
                      'other': []})

        # make sure parse_counters() works
        assert_equal(mr_job.parse_counters(), parsed_stderr['counters'])

    def test_negative_and_zero_counters(self):
        mr_job = MRJob().sandbox()

        mr_job.increment_counter('Foo', 'Bar', -1)
        mr_job.increment_counter('Foo', 'Baz')
        mr_job.increment_counter('Foo', 'Baz', -1)
        mr_job.increment_counter('Qux', 'Quux', 0)

        assert_equal(mr_job.parse_counters(),
                     {'Foo': {'Bar': -1, 'Baz': 0}, 'Qux': {'Quux': 0}})

    def test_bad_counter_amounts(self):
        mr_job = MRJob().sandbox()

        assert_raises(TypeError, mr_job.increment_counter, 'Foo', 'Bar', 'two')
        assert_raises(TypeError, mr_job.increment_counter, 'Foo', 'Bar', None)

    def test_commas_in_counters(self):
        # commas should be replaced with semicolons
        mr_job = MRJob().sandbox()

        mr_job.increment_counter('Bad items', 'a, b, c')
        mr_job.increment_counter('girl, interrupted', 'movie')

        assert_equal(mr_job.parse_counters(),
                     {'Bad items': {'a; b; c': 1},
                      'girl; interrupted': {'movie': 1}})


class ProtocolsTestCase(TestCase):
    # not putting these in their own files because we're not going to invoke
    # it as a script anyway.

    class MRBoringJob2(MRBoringJob):
        DEFAULT_INPUT_PROTOCOL = 'json'
        DEFAULT_PROTOCOL = 'pickle'
        DEFAULT_OUTPUT_PROTOCOL = 'repr'

    class MRBoringJob3(MRBoringJob):
        DEFAULT_PROTOCOL = 'repr'

    class MRTrivialJob(MRJob):
        DEFAULT_OUTPUT_PROTOCOL = 'repr'

        def mapper(self, key, value):
            yield key, value

    def test_default_protocols(self):
        mr_job = MRBoringJob()
        assert_equal(mr_job.options.input_protocol, 'raw_value')
        assert_equal(mr_job.options.protocol, 'json')
        assert_equal(mr_job.options.output_protocol, 'json')

    def test_explicit_default_protocols(self):
        mr_job2 = self.MRBoringJob2()
        assert_equal(mr_job2.options.input_protocol, 'json')
        assert_equal(mr_job2.options.protocol, 'pickle')
        assert_equal(mr_job2.options.output_protocol, 'repr')

        mr_job3 = self.MRBoringJob3()
        assert_equal(mr_job3.options.input_protocol, 'raw_value')
        assert_equal(mr_job3.options.protocol, 'repr')
        # output protocol should default to protocol
        assert_equal(mr_job3.options.output_protocol, 'repr')

    def test_setting_protocol(self):
        mr_job2 = MRBoringJob(args=[
            '--input-protocol=json', '--protocol=pickle',
            '--output-protocol=repr'])
        assert_equal(mr_job2.options.input_protocol, 'json')
        assert_equal(mr_job2.options.protocol, 'pickle')
        assert_equal(mr_job2.options.output_protocol, 'repr')

        mr_job3 = MRBoringJob(args=['--protocol=repr'])
        assert_equal(mr_job3.options.input_protocol, 'raw_value')
        assert_equal(mr_job3.options.protocol, 'repr')
        # output protocol should default to protocol
        assert_equal(mr_job3.options.output_protocol, 'repr')

    def test_overriding_explicit_default_protocols(self):
        mr_job = self.MRBoringJob2(args=['--protocol=json'])
        assert_equal(mr_job.options.input_protocol, 'json')
        assert_equal(mr_job.options.protocol, 'json')
        assert_equal(mr_job.options.output_protocol, 'repr')

    def test_mapper_raw_value_to_json(self):
        RAW_INPUT = StringIO('foo\nbar\nbaz\n')

        mr_job = MRBoringJob(['--mapper'])
        mr_job.sandbox(stdin=RAW_INPUT)
        mr_job.run_mapper()

        assert_equal(mr_job.stdout.getvalue(),
                     'null\t"foo"\n' +
                     'null\t"bar"\n' +
                     'null\t"baz"\n')

    def test_reducer_json_to_json(self):
        JSON_INPUT = StringIO('"foo"\t"bar"\n' +
                              '"foo"\t"baz"\n' +
                              '"bar"\t"qux"\n')

        mr_job = MRBoringJob(args=['--reducer'])
        mr_job.sandbox(stdin=JSON_INPUT)
        mr_job.run_reducer()

        assert_equal(mr_job.stdout.getvalue(),
                     ('"foo"\t["bar", "baz"]\n' +
                      '"bar"\t["qux"]\n'))

    def test_output_protocol_with_no_final_reducer(self):
        # if there's no reducer, the last mapper should use the
        # output protocol (in this case, repr)
        RAW_INPUT = StringIO('foo\nbar\nbaz\n')

        mr_job = self.MRTrivialJob(['--mapper'])
        mr_job.sandbox(stdin=RAW_INPUT)
        mr_job.run_mapper()

        assert_equal(mr_job.options.output_protocol, 'repr')
        assert_equal(mr_job.stdout.getvalue(),
                     ("None\t'foo'\n" +
                      "None\t'bar'\n" +
                      "None\t'baz'\n"))

    def test_undecodable_input(self):
        BAD_JSON_INPUT = StringIO('BAD\tJSON\n' +
                                  '"foo"\t"bar"\n' +
                                  '"too"\t"many"\t"tabs"\n' +
                                  '"notabs"\n')

        mr_job = MRBoringJob(args=['--reducer'])
        mr_job.sandbox(stdin=BAD_JSON_INPUT)
        mr_job.run_reducer()

        # good data should still get through
        assert_equal(mr_job.stdout.getvalue(), '"foo"\t["bar"]\n')

        # exception type varies between versions of simplejson,
        # so just make sure there were three exceptions of some sort
        counters = mr_job.parse_counters()
        assert_equal(counters.keys(), ['Undecodable input'])
        assert_equal(sum(counters['Undecodable input'].itervalues()), 3)

    def test_undecodable_input_strict(self):
        BAD_JSON_INPUT = StringIO('BAD\tJSON\n' +
                                  '"foo"\t"bar"\n' +
                                  '"too"\t"many"\t"tabs"\n' +
                                  '"notabs"\n')

        mr_job = MRBoringJob(args=['--reducer', '--strict-protocols'])
        mr_job.sandbox(stdin=BAD_JSON_INPUT)
        
        # make sure it raises an exception
        assert_raises(Exception, mr_job.run_reducer)
        
    def test_unencodable_output(self):
        UNENCODABLE_RAW_INPUT = StringIO('foo\n' +
                                         '\xaa\n' +
                                         'bar\n')

        mr_job = MRBoringJob(args=['--mapper'])
        mr_job.sandbox(stdin=UNENCODABLE_RAW_INPUT)
        mr_job.run_mapper()

        # good data should still get through
        assert_equal(mr_job.stdout.getvalue(),
                     ('null\t"foo"\n' + 'null\t"bar"\n'))

        assert_equal(mr_job.parse_counters(),
                     {'Unencodable output': {'UnicodeDecodeError': 1}})
                     
    def test_undecodable_output_strict(self):
        UNENCODABLE_RAW_INPUT = StringIO('foo\n' +
                                         '\xaa\n' +
                                         'bar\n')

        mr_job = MRBoringJob(args=['--mapper', '--strict-protocols'])
        mr_job.sandbox(stdin=UNENCODABLE_RAW_INPUT)
        
        # make sure it raises an exception
        assert_raises(Exception, mr_job.run_mapper)


class IsMapperOrReducerTestCase(TestCase):

    def test_is_mapper_or_reducer(self):
        assert_equal(MRJob().is_mapper_or_reducer(), False)
        assert_equal(MRJob(['--mapper']).is_mapper_or_reducer(), True)
        assert_equal(MRJob(['--reducer']).is_mapper_or_reducer(), True)
        assert_equal(MRJob(['--steps']).is_mapper_or_reducer(), False)


class StepsTestCase(TestCase):

    def test_auto_build_steps(self):
        mrbj = MRBoringJob()
        assert_equal(mrbj.steps(),
                     [mrbj.mr(mrbj.mapper, mrbj.reducer)])

        mrfbj = MRFinalBoringJob()
        assert_equal(mrfbj.steps(),
                     [mrfbj.mr(mrfbj.mapper, mrfbj.reducer,
                                mapper_final=mrfbj.mapper_final)])

    def test_show_steps(self):
        mr_job = MRJob(['--steps'])
        mr_job.sandbox()
        mr_job.show_steps()
        assert_equal(mr_job.stdout.getvalue(), 'M\n')

        mr_boring_job = MRBoringJob(['--steps'])
        mr_boring_job.sandbox()
        mr_boring_job.show_steps()
        assert_equal(mr_boring_job.stdout.getvalue(), 'MR\n')

        # final mappers don't show up in the step description
        mr_final_boring_job = MRFinalBoringJob(['--steps'])
        mr_final_boring_job.sandbox()
        mr_final_boring_job.show_steps()
        assert_equal(mr_final_boring_job.stdout.getvalue(), 'MR\n')

        mr_two_step_job = MRTwoStepJob(['--steps'])
        mr_two_step_job.sandbox()
        mr_two_step_job.show_steps()
        assert_equal(mr_two_step_job.stdout.getvalue(), 'MR M\n')


class StepNumTestCase(TestCase):

    def test_two_step_job_end_to_end(self):
        # represent input as a list so we can reuse it
        # also, leave off newline (MRJobRunner should fix it)
        mapper0_input_lines = ['foo', 'bar']

        def test_mapper0(mr_job, input_lines):
            mr_job.sandbox(input_lines)
            mr_job.run_mapper(0)
            assert_equal(mr_job.parse_output(),
                         [(None, 'foo'), ('foo', None),
                          (None, 'bar'), ('bar', None),])

        mapper0 = MRTwoStepJob()
        test_mapper0(mapper0, mapper0_input_lines)

        # --step-num=0 shouldn't actually be necessary
        mapper0_no_step_num = MRTwoStepJob(['--mapper'])
        test_mapper0(mapper0_no_step_num, mapper0_input_lines)

        # sort output of mapper0
        mapper0_output_input_lines = StringIO(mapper0.stdout.getvalue())
        reducer0_input_lines = sorted(mapper0_output_input_lines,
                               key=lambda line: line.split('\t'))

        def test_reducer0(mr_job, input_lines):
            mr_job.sandbox(input_lines)
            mr_job.run_reducer(0)
            assert_equal(mr_job.parse_output(),
                         [('bar', 1), ('foo', 1), (None, 2),])

        reducer0 = MRTwoStepJob()
        test_reducer0(reducer0, reducer0_input_lines)

        # --step-num=0 shouldn't actually be necessary
        reducer0_no_step_num = MRTwoStepJob(['--reducer'])
        test_reducer0(reducer0_no_step_num, reducer0_input_lines)

        # mapper can use reducer0's output as-is
        mapper1_input_lines = StringIO(reducer0.stdout.getvalue())

        def test_mapper1(mr_job, input_lines):
            mr_job.sandbox(input_lines)
            mr_job.run_mapper(1)
            assert_equal(mr_job.parse_output(),
                         [(1, 'bar'), (1, 'foo'), (2, None),])

        mapper1 = MRTwoStepJob()
        test_mapper1(mapper1, mapper1_input_lines)

    def test_nonexistent_steps(self):
        mr_job = MRTwoStepJob()
        mr_job.sandbox()
        assert_raises(ValueError, mr_job.run_reducer, 1)
        assert_raises(ValueError, mr_job.run_mapper, 2)
        assert_raises(ValueError, mr_job.run_reducer, -1)


class CommandLineArgsTest(TestCase):

    def test_shouldnt_exit_when_invoked_as_object(self):
        assert_raises(ValueError, MRJob, args=['--quux', 'baz'])

    def test_should_exit_when_invoked_as_script(self):
        args = [sys.executable, MRJob.mr_job_script(), '--quux', 'baz']
        # add . to PYTHONPATH (in case mrjob isn't actually installed)
        env = combine_envs(os.environ,
                           {'PYTHONPATH': os.path.abspath('.')})
        proc = Popen(args, stderr=PIPE, stdout=PIPE, env=env)
        proc.communicate()
        assert_equal(proc.returncode, 2)

    def test_custom_key_value_option_parsing(self):
        # simple example
        mr_job = MRBoringJob(['--cmdenv', 'FOO=bar'])
        assert_equal(mr_job.options.cmdenv, {'FOO': 'bar'})

        # trickier example
        mr_job = MRBoringJob(
            ['--cmdenv', 'FOO=bar',
             '--cmdenv', 'FOO=baz',
             '--cmdenv', 'BAZ=qux=quux'])
        assert_equal(mr_job.options.cmdenv,
                     {'FOO': 'baz', 'BAZ': 'qux=quux'})

        # must have KEY=VALUE
        assert_raises(ValueError, MRBoringJob, ['--cmdenv', 'FOO'])

    def test_passthrough_options_defaults(self):
        mr_job = MRCustomBoringJob()

        assert_equal(mr_job.options.input_protocol, 'raw_value')
        assert_equal(mr_job.options.protocol, 'json')
        assert_equal(mr_job.options.output_protocol, 'json')
        assert_equal(mr_job.options.foo_size, 5)
        assert_equal(mr_job.options.bar_name, None)
        assert_equal(mr_job.options.baz_mode, False)
        assert_equal(mr_job.options.quuxing, True)
        assert_equal(mr_job.options.pill_type, 'blue')
        assert_equal(mr_job.options.planck_constant, 6.626068e-34)
        assert_equal(mr_job.options.extra_special_args, [])
        # should include all --protocol options
        # should include default value of --num-items
        # should use long option names (--protocol, not -p)
        # shouldn't include --limit because it's None
        # items should be in the order they were instantiated
        assert_equal(mr_job.generate_passthrough_arguments(),
                     ['--protocol', 'json',
                      '--output-protocol', 'json',
                      '--input-protocol', 'raw_value',
                      '--foo-size', '5',
                      '--pill-type', 'blue',
                      '--planck-constant', '6.626068e-34'])

    def test_explicit_passthrough_options(self):
        mr_job = MRCustomBoringJob(args=[
            '-p', 'repr', # short name for --protocol
            '--foo-size=9',
            '--bar-name', 'Alembic',
            '--enable-baz-mode', '--disable-quuxing',
            '--pill-type', 'red',
            '--planck-constant', '1',
            '--planck-constant', '42',
            '--extra-special-arg', 'you',
            '--extra-special-arg', 'me',
            '--strict-protocols',
            ])

        assert_equal(mr_job.options.input_protocol, 'raw_value')
        assert_equal(mr_job.options.protocol, 'repr')
        assert_equal(mr_job.options.output_protocol, 'repr')
        assert_equal(mr_job.options.foo_size, 9)
        assert_equal(mr_job.options.bar_name, 'Alembic')
        assert_equal(mr_job.options.baz_mode, True)
        assert_equal(mr_job.options.quuxing, False)
        assert_equal(mr_job.options.pill_type, 'red')
        assert_equal(mr_job.options.planck_constant, 42)
        assert_equal(mr_job.options.extra_special_args, ['you', 'me'])
        assert_equal(mr_job.options.strict_protocols, True)
        assert_equal(mr_job.generate_passthrough_arguments(),
                     ['--protocol', 'repr',
                      '--output-protocol', 'repr',
                      '--input-protocol', 'raw_value',
                      '--strict-protocols',
                      '--foo-size', '9',
                      '--bar-name', 'Alembic',
                      '--enable-baz-mode',
                      '--disable-quuxing',
                      '--pill-type', 'red',
                      '--planck-constant', '42.0',
                      '--extra-special-arg', 'you',
                      '--extra-special-arg', 'me',
                      ])

    def test_bad_custom_options(self):
        assert_raises(ValueError, MRCustomBoringJob, ['--planck-constant', 'c'])
        assert_raises(ValueError, MRCustomBoringJob, ['--pill-type=green'])

    def test_bad_option_types(self):
        mr_job = MRJob()
        assert_raises(
            OptionError, mr_job.add_passthrough_option,
            '--stop-words', dest='stop_words', type='set', default=None)
        assert_raises(
            OptionError, mr_job.add_passthrough_option,
            '--leave-a-msg', dest='leave_a_msg', action='callback',
            default=None)

    def test_default_file_options(self):
        mr_job = MRCustomBoringJob()
        assert_equal(mr_job.options.foo_config, None)
        assert_equal(mr_job.options.accordian_files, [])
        assert_equal(mr_job.generate_file_upload_args(), [])

    def test_explicit_file_options(self):
        mr_job = MRCustomBoringJob(args=[
            '--foo-config', '/tmp/.fooconf',
            '--foo-config', '/etc/fooconf',
            '--accordian-file', 'WeirdAl.mp3',
            '--accordian-file', '/home/dave/JohnLinnell.ogg'])
        assert_equal(mr_job.options.foo_config, '/etc/fooconf')
        assert_equal(mr_job.options.accordian_files, [
            'WeirdAl.mp3', '/home/dave/JohnLinnell.ogg'])
        assert_equal(mr_job.generate_file_upload_args(), [
            ('--foo-config', '/etc/fooconf'),
            ('--accordian-file', 'WeirdAl.mp3'),
            ('--accordian-file', '/home/dave/JohnLinnell.ogg')])


class FileOptionsTestCase(TestCase):
    # make sure custom file options work with --steps (Issue #45)

    @setup
    def make_tmp_dir(self):
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

    def test_end_to_end(self):
        n_file_path = os.path.join(self.tmp_dir, 'n_file')

        with open(n_file_path, 'w') as f:
            f.write('3')

        os.environ['LOCAL_N_FILE_PATH'] = n_file_path

        stdin = ['0\n', '1\n', '2\n']

        mr_job = MRTowerOfPowers(['--no-conf', '-v', '--cleanup=NONE', '--n-file', n_file_path])
        assert_equal(len(mr_job.steps()), 3)

        mr_job.sandbox(stdin=stdin)

        with mr_job.make_runner() as runner:
            assert isinstance(runner, LocalMRJobRunner)
            # make sure our file gets "uploaded"
            assert [fd for fd in runner._files if fd['path'] == n_file_path]

            runner.run()
            output = set()
            for line in runner.stream_output():
                _, value = mr_job.parse_output_line(line)
                output.add(value)

        assert_equal(set(output), set([0, 1, ((2**3)**3)**3]))


class RunJobTestCase(TestCase):
    # test invoking a job as a script

    @setup
    def make_tmp_dir(self):
        self.tmp_dir = tempfile.mkdtemp()

    @teardown
    def rm_tmp_dir(self):
        shutil.rmtree(self.tmp_dir)

    def run_job(self, args=()):
        args = ([sys.executable, MRTwoStepJob.mr_job_script()] +
                list(args) + ['--no-conf'])
        # add . to PYTHONPATH (in case mrjob isn't actually installed)
        env = combine_envs(os.environ,
                           {'PYTHONPATH': os.path.abspath('.')})
        proc = Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE, env=env)
        stdout, stderr = proc.communicate(input='foo\nbar\nbar\n')
        return stdout, stderr, proc.returncode

    def test_quiet(self):
        stdout, stderr, returncode = self.run_job(['-q'])
        assert_equal(stdout, '2\t"bar"\n1\t"foo"\n3\tnull\n')
        assert_equal(stderr, '')
        assert_equal(returncode, 0)

    def test_verbose(self):
        stdout, stderr, returncode = self.run_job()
        assert_equal(stdout, '2\t"bar"\n1\t"foo"\n3\tnull\n')
        assert_not_equal(stderr, '')
        assert_equal(returncode, 0)
        normal_stderr = stderr

        stdout, stderr, returncode = self.run_job(['-v'])
        assert_equal(stdout, '2\t"bar"\n1\t"foo"\n3\tnull\n')
        assert_not_equal(stderr, '')
        assert_equal(returncode, 0)
        assert_gt(len(stderr), len(normal_stderr))
        
    def test_no_output(self):
        assert_equal(os.listdir(self.tmp_dir), []) # sanity check
        
        args = ['--no-output', '--output-dir', self.tmp_dir]        
        stdout, stderr, returncode = self.run_job(args)
        assert_equal(stdout, '')
        assert_not_equal(stderr, '')
        assert_equal(returncode, 0)

        # make sure the correct output is in the temp dir
        assert_not_equal(os.listdir(self.tmp_dir), [])
        output_lines = []
        for dirpath, _, filenames in os.walk(self.tmp_dir):
            for filename in filenames:
                with open(os.path.join(dirpath, filename)) as output_f:
                    output_lines.extend(output_f)

        assert_equal(sorted(output_lines),
                     ['1\t"foo"\n', '2\t"bar"\n', '3\tnull\n'])


class TestBadMainCatch(TestCase):
    """Ensure that the user cannot do anything but just call MRYourJob.run() from __main__"""

    def test_bad_main_catch(self):
        sys.argv.append('--mapper')
        assert_raises(UsageError, MRBoringJob().make_runner)
        sys.argv = sys.argv[:-1]
