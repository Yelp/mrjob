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

"""Unit testing of MRJob."""
from StringIO import StringIO
from optparse import OptionError
from subprocess import Popen, PIPE
from testify import TestCase, assert_equal, assert_raises

from mrjob.job import MRJob, _IDENTITY_MAPPER
from mrjob.parse import parse_mr_job_stderr
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

        mr_job = MRBoringJob(args=['--mapper'])
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

        assert_equal(mr_job.parse_counters(),
                     {'Undecodable input': {'ValueError': 3}})
        
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
        args = ['python', MRJob.mr_job_script(), '--quux', 'baz']
        proc = Popen(args, stderr=PIPE, stdout=PIPE)
        stderr, stdout = proc.communicate()
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
        assert_equal(mr_job.generate_passthrough_arguments(),
                     ['--protocol', 'repr',
                      '--output-protocol', 'repr',
                      '--input-protocol', 'raw_value',
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

# we don't test run_job() or *_job_runner_kwargs() here; those should be tested
# by the tests of the various MRJobRunner classes
