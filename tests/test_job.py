# -*- encoding: utf-8 -*-
# Copyright 2009-2013 Yelp and Contributors
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

import os
from subprocess import Popen
from subprocess import PIPE
from StringIO import StringIO
import sys
import time

try:
    import unittest2 as unittest
    unittest  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import unittest

from mrjob.conf import combine_envs
from mrjob.job import MRJob
from mrjob.job import UsageError
from mrjob.parse import parse_mr_job_stderr
from mrjob.protocol import JSONProtocol
from mrjob.protocol import JSONValueProtocol
from mrjob.protocol import PickleProtocol
from mrjob.protocol import RawValueProtocol
from mrjob.protocol import ReprProtocol
from mrjob.step import JarStep
from mrjob.step import MRJobStep
from mrjob.util import log_to_stream
from tests.mr_hadoop_format_job import MRHadoopFormatJob
from tests.mr_tower_of_powers import MRTowerOfPowers
from tests.mr_two_step_job import MRTwoStepJob
from tests.quiet import logger_disabled
from tests.quiet import no_handlers_for_logger
from tests.sandbox import EmptyMrjobConfTestCase
from tests.sandbox import SandboxedTestCase


# These can't be invoked as a separate script, but they don't need to be

class MRBoringJob(MRJob):
    """It's a boring job, but somebody had to do it."""
    def mapper(self, key, value):
        yield(key, value)

    def reducer(self, key, values):
        yield(key, list(values))


class MRInitJob(MRJob):

    def __init__(self, *args, **kwargs):
        super(MRInitJob, self).__init__(*args, **kwargs)
        self.sum_amount = 0
        self.multiplier = 0
        self.combiner_multipler = 1

    def mapper_init(self):
        self.sum_amount += 10

    def mapper(self, key, value):
        yield(None, self.sum_amount)

    def reducer_init(self):
        self.multiplier += 10

    def reducer(self, key, values):
        yield(None, sum(values) * self.multiplier)

    def combiner_init(self):
        self.combiner_multiplier = 2

    def combiner(self, key, values):
        yield(None, sum(values) * self.combiner_multiplier)


### Test cases ###


class MRInitTestCase(EmptyMrjobConfTestCase):

    def test_mapper(self):
        j = MRInitJob()
        j.mapper_init()
        self.assertEqual(j.mapper(None, None).next(), (None, j.sum_amount))

    def test_init_funcs(self):
        num_inputs = 2
        stdin = StringIO("x\n" * num_inputs)
        mr_job = MRInitJob(['-r', 'inline', '-'])
        mr_job.sandbox(stdin=stdin)

        results = []
        with mr_job.make_runner() as runner:
            runner.run()
            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append(value)
        # these numbers should match if mapper_init, reducer_init, and
        # combiner_init were called as expected
        self.assertEqual(results[0], num_inputs * 10 * 10 * 2)


class NoTzsetTestCase(unittest.TestCase):

    def setUp(self):
        self.remove_time_tzset()

    def tearDown(self):
        self.restore_time_tzset()
    """Test systems without time.tzset() (e.g. Windows). See Issue #46."""

    def remove_time_tzset(self):
        if hasattr(time, 'tzset'):
            self._real_time_tzset = time.tzset
            del time.tzset

    def restore_time_tzset(self):
        if hasattr(self, '_real_time_tzset'):
            time.tzset = self._real_time_tzset

    def test_init_does_not_require_tzset(self):
        MRJob()


class CountersAndStatusTestCase(unittest.TestCase):

    def test_counters_and_status(self):
        mr_job = MRJob().sandbox()

        mr_job.increment_counter('Foo', 'Bar')
        mr_job.set_status('Initializing qux gradients...')
        mr_job.increment_counter('Foo', 'Bar')
        mr_job.increment_counter('Foo', 'Baz', 20)
        mr_job.set_status('Sorting metasyntactic variables...')

        parsed_stderr = parse_mr_job_stderr(mr_job.stderr.getvalue())

        self.assertEqual(parsed_stderr,
                         {'counters': {'Foo': {'Bar': 2, 'Baz': 20}},
                          'statuses': ['Initializing qux gradients...',
                                       'Sorting metasyntactic variables...'],
                          'other': []})

        # make sure parse_counters() works
        self.assertEqual(mr_job.parse_counters(), parsed_stderr['counters'])

    def test_unicode_set_status(self):
        mr_job = MRJob().sandbox()
        # shouldn't raise an exception
        mr_job.set_status(u'💩')

    def test_unicode_counter(self):
        mr_job = MRJob().sandbox()
        # shouldn't raise an exception
        mr_job.increment_counter(u'💩', 'x', 1)

    def test_negative_and_zero_counters(self):
        mr_job = MRJob().sandbox()

        mr_job.increment_counter('Foo', 'Bar', -1)
        mr_job.increment_counter('Foo', 'Baz')
        mr_job.increment_counter('Foo', 'Baz', -1)
        mr_job.increment_counter('Qux', 'Quux', 0)

        self.assertEqual(mr_job.parse_counters(),
                         {'Foo': {'Bar': -1, 'Baz': 0}, 'Qux': {'Quux': 0}})

    def test_bad_counter_amounts(self):
        mr_job = MRJob().sandbox()

        self.assertRaises(TypeError,
                          mr_job.increment_counter, 'Foo', 'Bar', 'two')
        self.assertRaises(TypeError,
                          mr_job.increment_counter, 'Foo', 'Bar', None)

    def test_commas_in_counters(self):
        # commas should be replaced with semicolons
        mr_job = MRJob().sandbox()

        mr_job.increment_counter('Bad items', 'a, b, c')
        mr_job.increment_counter('girl, interrupted', 'movie')

        self.assertEqual(mr_job.parse_counters(),
                         {'Bad items': {'a; b; c': 1},
                          'girl; interrupted': {'movie': 1}})


class ProtocolsTestCase(unittest.TestCase):
    # not putting these in their own files because we're not going to invoke
    # it as a script anyway.

    class MRBoringJob2(MRBoringJob):
        INPUT_PROTOCOL = JSONProtocol
        INTERNAL_PROTOCOL = PickleProtocol
        OUTPUT_PROTOCOL = ReprProtocol

    class MRBoringJob3(MRBoringJob):

        def internal_protocol(self):
            return ReprProtocol()

    class MRBoringJob4(MRBoringJob):
        INTERNAL_PROTOCOL = ReprProtocol

    class MRTrivialJob(MRJob):
        OUTPUT_PROTOCOL = ReprProtocol

        def mapper(self, key, value):
            yield key, value

    def assertMethodsEqual(self, fs, gs):
        # we're going to use this to match bound against unbound methods
        self.assertEqual([f.im_func for f in fs],
                         [g.im_func for g in gs])

    def test_default_protocols(self):
        mr_job = MRBoringJob()
        self.assertMethodsEqual(mr_job.pick_protocols(0, 'mapper'),
                                (RawValueProtocol.read, JSONProtocol.write))
        self.assertMethodsEqual(mr_job.pick_protocols(0, 'reducer'),
                               (JSONProtocol.read, JSONProtocol.write))

    def test_explicit_default_protocols(self):
        mr_job2 = self.MRBoringJob2().sandbox()
        self.assertMethodsEqual(mr_job2.pick_protocols(0, 'mapper'),
                                (JSONProtocol.read, PickleProtocol.write))
        self.assertMethodsEqual(mr_job2.pick_protocols(0, 'reducer'),
                                (PickleProtocol.read, ReprProtocol.write))

        mr_job3 = self.MRBoringJob3()
        self.assertMethodsEqual(mr_job3.pick_protocols(0, 'mapper'),
                                (RawValueProtocol.read, ReprProtocol.write))
        # output protocol should default to JSON
        self.assertMethodsEqual(mr_job3.pick_protocols(0, 'reducer'),
                                (ReprProtocol.read, JSONProtocol.write))

        mr_job4 = self.MRBoringJob4()
        self.assertMethodsEqual(mr_job4.pick_protocols(0, 'mapper'),
                                (RawValueProtocol.read, ReprProtocol.write))
        # output protocol should default to JSON
        self.assertMethodsEqual(mr_job4.pick_protocols(0, 'reducer'),
                                (ReprProtocol.read, JSONProtocol.write))

    def test_mapper_raw_value_to_json(self):
        RAW_INPUT = StringIO('foo\nbar\nbaz\n')

        mr_job = MRBoringJob(['--mapper'])
        mr_job.sandbox(stdin=RAW_INPUT)
        mr_job.run_mapper()

        self.assertEqual(mr_job.stdout.getvalue(),
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

        self.assertEqual(mr_job.stdout.getvalue(),
                         ('"foo"\t["bar", "baz"]\n' +
                          '"bar"\t["qux"]\n'))

    def test_output_protocol_with_no_final_reducer(self):
        # if there's no reducer, the last mapper should use the
        # output protocol (in this case, repr)
        RAW_INPUT = StringIO('foo\nbar\nbaz\n')

        mr_job = self.MRTrivialJob(['--mapper'])
        mr_job.sandbox(stdin=RAW_INPUT)
        mr_job.run_mapper()

        self.assertEqual(mr_job.stdout.getvalue(),
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
        self.assertEqual(mr_job.stdout.getvalue(), '"foo"\t["bar"]\n')

        # exception type varies between versions of simplejson,
        # so just make sure there were three exceptions of some sort
        counters = mr_job.parse_counters()
        self.assertEqual(counters.keys(), ['Undecodable input'])
        self.assertEqual(sum(counters['Undecodable input'].itervalues()), 3)

    def test_undecodable_input_strict(self):
        BAD_JSON_INPUT = StringIO('BAD\tJSON\n' +
                                  '"foo"\t"bar"\n' +
                                  '"too"\t"many"\t"tabs"\n' +
                                  '"notabs"\n')

        mr_job = MRBoringJob(args=['--reducer', '--strict-protocols'])
        mr_job.sandbox(stdin=BAD_JSON_INPUT)

        # make sure it raises an exception
        self.assertRaises(Exception, mr_job.run_reducer)

    def test_unencodable_output(self):
        UNENCODABLE_RAW_INPUT = StringIO('foo\n' +
                                         '\xaa\n' +
                                         'bar\n')

        mr_job = MRBoringJob(args=['--mapper'])
        mr_job.sandbox(stdin=UNENCODABLE_RAW_INPUT)
        mr_job.run_mapper()

        # good data should still get through
        self.assertEqual(mr_job.stdout.getvalue(),
                         ('null\t"foo"\n' + 'null\t"bar"\n'))

        self.assertEqual(mr_job.parse_counters(),
                         {'Unencodable output': {'UnicodeDecodeError': 1}})

    def test_undecodable_output_strict(self):
        UNENCODABLE_RAW_INPUT = StringIO('foo\n' +
                                         '\xaa\n' +
                                         'bar\n')

        mr_job = MRBoringJob(args=['--mapper', '--strict-protocols'])
        mr_job.sandbox(stdin=UNENCODABLE_RAW_INPUT)

        # make sure it raises an exception
        self.assertRaises(Exception, mr_job.run_mapper)


class PickProtocolsTestCase(unittest.TestCase):

    def _yield_none(self, *args, **kwargs):
        yield None

    def _make_job(self, steps_desc, strict_protocols=False):

        class CustomJob(MRJob):

            INPUT_PROTOCOL = PickleProtocol
            INTERNAL_PROTOCOL = JSONProtocol
            OUTPUT_PROTOCOL = JSONValueProtocol

            def _steps_desc(self):
                return steps_desc

        args = ['--no-conf']

        # tests that only use script steps should use strict_protocols so bad
        # internal behavior causes exceptions
        if strict_protocols:
            args.append('--strict-protocols')

        return CustomJob(args)

    def _assert_script_protocols(self, steps_desc, expected_protocols,
                                 strict_protocols=False):
        """Given a list of (read_protocol_class, write_protocol_class) tuples
        for *each substep*, assert that the given _steps_desc() output for each
        substep matches the protocols in order
        """
        j = self._make_job(steps_desc, strict_protocols)
        for i, step in enumerate(steps_desc):
            if step['type'] == 'jar':
                expect_read, expect_write = expected_protocols.pop(0)
                # step_type for a non-script step is undefined, and in general
                # these values should just be RawValueProtocol instances, but
                # we'll leave those checks to the actual tests.
                actual_read, actual_write = j._pick_protocol_instances(i, '?')
                self.assertIsInstance(actual_read, expect_read)
                self.assertIsInstance(actual_write, expect_write)
            else:
                for substep_key in ('mapper', 'combiner', 'reducer'):
                    if substep_key in step:
                        expect_read, expect_write = expected_protocols.pop(0)
                        actual_read, actual_write = j._pick_protocol_instances(
                            i, substep_key)
                        self.assertIsInstance(actual_read, expect_read)
                        self.assertIsInstance(actual_write, expect_write)

    def _streaming_step(self, n, *args, **kwargs):
        return MRJobStep(*args, **kwargs).description(n)

    def _jar_step(self, n, *args, **kwargs):
        return JarStep(*args, **kwargs).description(n)

    def test_single_mapper(self):
        self._assert_script_protocols(
            [self._streaming_step(0, mapper=self._yield_none)],
            [(PickleProtocol, JSONValueProtocol)],
            strict_protocols=True)

    def test_single_reducer(self):
        # MRJobStep transparently adds mapper
        self._assert_script_protocols(
            [self._streaming_step(0, reducer=self._yield_none)],
            [(PickleProtocol, JSONProtocol),
             (JSONProtocol, JSONValueProtocol)],
            strict_protocols=True)

    def test_mapper_combiner(self):
        self._assert_script_protocols(
            [self._streaming_step(
                0, mapper=self._yield_none, combiner=self._yield_none)],
            [(PickleProtocol, JSONValueProtocol),
             (JSONValueProtocol, JSONValueProtocol)],
            strict_protocols=True)

    def test_mapper_combiner_reducer(self):
        self._assert_script_protocols(
            [self._streaming_step(
                0, mapper=self._yield_none, combiner=self._yield_none,
                reducer=self._yield_none)],
            [(PickleProtocol, JSONProtocol),
             (JSONProtocol, JSONProtocol),
             (JSONProtocol, JSONValueProtocol)],
            strict_protocols=True)

    def test_begin_jar_step(self):
        self._assert_script_protocols(
            [self._jar_step(0, 'blah', 'binks_jar.jar'),
             self._streaming_step(
                 1, mapper=self._yield_none, combiner=self._yield_none,
                 reducer=self._yield_none)],
            [(RawValueProtocol, RawValueProtocol),
             (PickleProtocol, JSONProtocol),
             (JSONProtocol, JSONProtocol),
             (JSONProtocol, JSONValueProtocol)])

    def test_end_jar_step(self):
        self._assert_script_protocols(
            [self._streaming_step(
                0, mapper=self._yield_none, combiner=self._yield_none,
                reducer=self._yield_none),
             self._jar_step(1, 'blah', 'binks_jar.jar')],
            [(PickleProtocol, JSONProtocol),
             (JSONProtocol, JSONProtocol),
             (JSONProtocol, JSONValueProtocol),
             (RawValueProtocol, RawValueProtocol)])

    def test_middle_jar_step(self):
        self._assert_script_protocols(
            [self._streaming_step(
                0, mapper=self._yield_none, combiner=self._yield_none),
             self._jar_step(1, 'blah', 'binks_jar.jar'),
             self._streaming_step(2, reducer=self._yield_none)],
            [(PickleProtocol, JSONProtocol),
             (JSONProtocol, JSONProtocol),
             (RawValueProtocol, RawValueProtocol),
             (JSONProtocol, JSONValueProtocol)])

    def test_single_mapper_cmd(self):
        self._assert_script_protocols(
            [self._streaming_step(0, mapper_cmd='cat')],
            [(RawValueProtocol, RawValueProtocol)])

    def test_single_mapper_cmd_with_script_combiner(self):
        self._assert_script_protocols(
            [self._streaming_step(
                0, mapper_cmd='cat', combiner=self._yield_none)],
            [(RawValueProtocol, RawValueProtocol),
             (RawValueProtocol, RawValueProtocol)])

    def test_single_mapper_cmd_with_script_reducer(self):
        # reducer is only script step so it uses INPUT_PROTOCOL and
        # OUTPUT_PROTOCOL
        self._assert_script_protocols(
            [self._streaming_step(
                0, mapper_cmd='cat', reducer=self._yield_none)],
            [(RawValueProtocol, RawValueProtocol),
             (PickleProtocol, JSONValueProtocol)])

    def test_multistep(self):
        # reducer is only script step so it uses INPUT_PROTOCOL and
        # OUTPUT_PROTOCOL
        self._assert_script_protocols(
            [self._streaming_step(
                0, mapper_cmd='cat', reducer=self._yield_none),
             self._jar_step(1, 'blah', 'binks_jar.jar'),
             self._streaming_step(2, mapper=self._yield_none)],
            [(RawValueProtocol, RawValueProtocol),
             (PickleProtocol, JSONProtocol),
             (RawValueProtocol, RawValueProtocol),
             (JSONProtocol, JSONValueProtocol)])


class JobConfTestCase(unittest.TestCase):

    class MRJobConfJob(MRJob):
        JOBCONF = {'mapred.foo': 'garply',
                   'mapred.bar.bar.baz': 'foo'}

    class MRJobConfMethodJob(MRJob):
        def jobconf(self):
            return {'mapred.baz': 'bar'}

    class MRBoolJobConfJob(MRJob):
        JOBCONF = {'true_value': True,
                   'false_value': False}

    class MRHadoopVersionJobConfJob1(MRJob):
        JOBCONF = {'hadoop_version': 1.0}

    class MRHadoopVersionJobConfJob2(MRJob):
        JOBCONF = {'hadoop_version': 0.18}

    class MRHadoopVersionJobConfJob3(MRJob):
        JOBCONF = {'hadoop_version': 0.20}

    def test_empty(self):
        mr_job = MRJob()

        self.assertEqual(mr_job.job_runner_kwargs()['jobconf'], {})

    def test_cmd_line_options(self):
        mr_job = MRJob([
            '--jobconf', 'mapred.foo=bar',
            '--jobconf', 'mapred.foo=baz',
            '--jobconf', 'mapred.qux=quux',
        ])

        self.assertEqual(mr_job.job_runner_kwargs()['jobconf'],
                         {'mapred.foo': 'baz',  # second option takes priority
                          'mapred.qux': 'quux'})

    def test_bool_options(self):
        mr_job = self.MRBoolJobConfJob()
        self.assertEqual(mr_job.jobconf()['true_value'], 'true')
        self.assertEqual(mr_job.jobconf()['false_value'], 'false')

    def assert_hadoop_version(self, JobClass, version_string):
        mr_job = JobClass()
        mock_log = StringIO()
        with no_handlers_for_logger('mrjob.job'):
            log_to_stream('mrjob.job', mock_log)
            self.assertEqual(mr_job.jobconf()['hadoop_version'],
                             version_string)
            self.assertIn('should be a string', mock_log.getvalue())

    def test_float_options(self):
        self.assert_hadoop_version(self.MRHadoopVersionJobConfJob1, '1.0')

    def test_float_options_2(self):
        self.assert_hadoop_version(self.MRHadoopVersionJobConfJob2, '0.18')

    def test_float_options_3(self):
        self.assert_hadoop_version(self.MRHadoopVersionJobConfJob3, '0.20')

    def test_jobconf_attr(self):
        mr_job = self.MRJobConfJob()

        self.assertEqual(mr_job.job_runner_kwargs()['jobconf'],
                         {'mapred.foo': 'garply',
                          'mapred.bar.bar.baz': 'foo'})

    def test_jobconf_attr_and_cmd_line_options(self):
        mr_job = self.MRJobConfJob([
            '--jobconf', 'mapred.foo=bar',
            '--jobconf', 'mapred.foo=baz',
            '--jobconf', 'mapred.qux=quux',
        ])

        self.assertEqual(mr_job.job_runner_kwargs()['jobconf'],
                         {'mapred.bar.bar.baz': 'foo',
                          'mapred.foo': 'baz',  # command line takes priority
                          'mapred.qux': 'quux'})

    def test_redefined_jobconf_method(self):
        mr_job = self.MRJobConfMethodJob()

        self.assertEqual(mr_job.job_runner_kwargs()['jobconf'],
                         {'mapred.baz': 'bar'})

    def test_redefined_jobconf_method_overrides_cmd_line(self):
        mr_job = self.MRJobConfMethodJob([
            '--jobconf', 'mapred.foo=bar',
            '--jobconf', 'mapred.baz=foo',
        ])

        # --jobconf is ignored because that's the way we defined jobconf()
        self.assertEqual(mr_job.job_runner_kwargs()['jobconf'],
                         {'mapred.baz': 'bar'})


class MRSortValuesJob(MRJob):
    SORT_VALUES = True


class MRSortValuesAndMoreJob(MRSortValuesJob):
    PARTITIONER = 'org.apache.hadoop.mapred.lib.HashPartitioner'

    JOBCONF = {
        'stream.num.map.output.key.fields': 3,
        'mapred.output.key.comparator.class':
            'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
        'mapred.text.key.comparator.options': '-k1 -k2nr',
    }


class SortValuesTestCase(unittest.TestCase):

    def test_sort_values_sets_partitioner(self):
        mr_job = MRSortValuesJob()

        self.assertEqual(
            mr_job.partitioner(),
            'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner')

    def test_sort_values_sets_jobconf(self):
        mr_job = MRSortValuesJob()

        self.assertEqual(
            mr_job.jobconf(),
            {'stream.num.map.output.key.fields': 2,
             'mapred.text.key.partitioner.options': '-k1,1',
             'mapred.output.key.comparator.class': None,
             'mapred.text.key.comparator.options': None})

    def test_can_override_sort_values_from_job(self):
        mr_job = MRSortValuesAndMoreJob()

        self.assertEqual(
            mr_job.partitioner(),
            'org.apache.hadoop.mapred.lib.HashPartitioner')

        self.assertEqual(
            mr_job.jobconf(),
            {'stream.num.map.output.key.fields': 3,
             'mapred.text.key.partitioner.options': '-k1,1',
             'mapred.output.key.comparator.class':
                'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
             'mapred.text.key.comparator.options': '-k1 -k2nr'})

    def test_can_override_sort_values_from_cmd_line(self):
        mr_job = MRSortValuesJob(
            ['--partitioner', 'org.pants.FancyPantsPartitioner',
             '--jobconf', 'stream.num.map.output.key.fields=lots'])

        self.assertEqual(
            mr_job.partitioner(),
            'org.pants.FancyPantsPartitioner')

        self.assertEqual(
            mr_job.jobconf(),
            {'stream.num.map.output.key.fields': 'lots',
             'mapred.text.key.partitioner.options': '-k1,1',
             'mapred.output.key.comparator.class': None,
             'mapred.text.key.comparator.options': None})


class SortValuesRunnerTestCase(SandboxedTestCase):

    MRJOB_CONF_CONTENTS = {'runners': {'local': {'jobconf': {
        'mapred.text.key.partitioner.options': '-k1,1',
        'mapred.output.key.comparator.class': 'egypt.god.Anubis',
        'foo': 'bar',
    }}}}

    def test_cant_override_sort_values_from_mrjob_conf(self):
        runner = MRSortValuesJob(['-r', 'local']).make_runner()

        self.assertEqual(
            runner._hadoop_conf_args({}, 0, 1),
            # foo=bar is included, but the other options from mrjob.conf are
            # blanked out so as not to mess up SORT_VALUES
            ['-D', 'foo=bar',
             '-D', 'mapred.text.key.partitioner.options=-k1,1',
             '-D', 'stream.num.map.output.key.fields=2',
             '-partitioner',
                'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner'])


class HadoopFormatTestCase(unittest.TestCase):

    # MRHadoopFormatJob is imported above

    class MRHadoopFormatMethodJob(MRJob):

        def hadoop_input_format(self):
            return 'mapred.ReasonableInputFormat'

        def hadoop_output_format(self):
            # not a real Java class, thank god :)
            return 'mapred.EbcdicDb2EnterpriseXmlOutputFormat'

    def test_empty(self):
        mr_job = MRJob()

        self.assertEqual(mr_job.job_runner_kwargs()['hadoop_input_format'],
                         None)
        self.assertEqual(mr_job.job_runner_kwargs()['hadoop_output_format'],
                         None)

    def test_hadoop_format_attributes(self):
        mr_job = MRHadoopFormatJob()

        self.assertEqual(mr_job.job_runner_kwargs()['hadoop_input_format'],
                         'mapred.FooInputFormat')
        self.assertEqual(mr_job.job_runner_kwargs()['hadoop_output_format'],
                         'mapred.BarOutputFormat')

    def test_hadoop_format_methods(self):
        mr_job = self.MRHadoopFormatMethodJob()

        self.assertEqual(mr_job.job_runner_kwargs()['hadoop_input_format'],
                         'mapred.ReasonableInputFormat')
        self.assertEqual(mr_job.job_runner_kwargs()['hadoop_output_format'],
                         'mapred.EbcdicDb2EnterpriseXmlOutputFormat')


class PartitionerTestCase(unittest.TestCase):

    class MRPartitionerJob(MRJob):
        PARTITIONER = 'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner'

    def test_empty(self):
        mr_job = MRJob()

        self.assertEqual(mr_job.job_runner_kwargs()['partitioner'], None)

    def test_cmd_line_options(self):
        mr_job = MRJob([
            '--partitioner', 'java.lang.Object',
            '--partitioner', 'org.apache.hadoop.mapreduce.Partitioner'
        ])

        # second option takes priority
        self.assertEqual(mr_job.job_runner_kwargs()['partitioner'],
                         'org.apache.hadoop.mapreduce.Partitioner')

    def test_partitioner_attr(self):
        mr_job = self.MRPartitionerJob()

        self.assertEqual(
            mr_job.job_runner_kwargs()['partitioner'],
            'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner')

    def test_partitioner_attr_and_cmd_line_options(self):
        mr_job = self.MRPartitionerJob([
            '--partitioner', 'java.lang.Object',
            '--partitioner', 'org.apache.hadoop.mapreduce.Partitioner'
        ])

        # command line takes priority
        self.assertEqual(mr_job.job_runner_kwargs()['partitioner'],
                         'org.apache.hadoop.mapreduce.Partitioner')


class IsMapperOrReducerTestCase(unittest.TestCase):

    def test_is_mapper_or_reducer(self):
        self.assertEqual(MRJob().is_mapper_or_reducer(), False)
        self.assertEqual(MRJob(['--mapper']).is_mapper_or_reducer(), True)
        self.assertEqual(MRJob(['--reducer']).is_mapper_or_reducer(), True)
        self.assertEqual(MRJob(['--combiner']).is_mapper_or_reducer(), True)
        self.assertEqual(MRJob(['--steps']).is_mapper_or_reducer(), False)


class StepNumTestCase(unittest.TestCase):

    def test_two_step_job_end_to_end(self):
        # represent input as a list so we can reuse it
        # also, leave off newline (MRJobRunner should fix it)
        mapper0_input_lines = ['foo', 'bar']

        def test_mapper0(mr_job, input_lines):
            mr_job.sandbox(input_lines)
            mr_job.run_mapper(0)
            self.assertEqual(mr_job.parse_output(),
                             [(None, 'foo'), ('foo', None),
                              (None, 'bar'), ('bar', None)])

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
            self.assertEqual(mr_job.parse_output(),
                             [('bar', 1), ('foo', 1), (None, 2)])

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
            self.assertEqual(mr_job.parse_output(),
                             [(1, 'bar'), (1, 'foo'), (2, None)])

        mapper1 = MRTwoStepJob()
        test_mapper1(mapper1, mapper1_input_lines)

    def test_nonexistent_steps(self):
        mr_job = MRTwoStepJob()
        mr_job.sandbox()
        self.assertRaises(ValueError, mr_job.run_reducer, 1)
        self.assertRaises(ValueError, mr_job.run_mapper, 2)
        self.assertRaises(ValueError, mr_job.run_reducer, -1)


class FileOptionsTestCase(SandboxedTestCase):

    def test_end_to_end(self):
        n_file_path = os.path.join(self.tmp_dir, 'n_file')

        with open(n_file_path, 'w') as f:
            f.write('3')

        os.environ['LOCAL_N_FILE_PATH'] = n_file_path

        stdin = ['0\n', '1\n', '2\n']

        # use local runner so that the file is actually sent somewhere
        mr_job = MRTowerOfPowers(
            ['-v', '--cleanup=NONE', '--n-file', n_file_path,
             '--runner=local'])
        self.assertEqual(len(mr_job.steps()), 3)

        mr_job.sandbox(stdin=stdin)

        with logger_disabled('mrjob.local'):
            with mr_job.make_runner() as runner:
                # make sure our file gets placed in the working dir
                self.assertIn(n_file_path, runner._working_dir_mgr.paths())

                runner.run()
                output = set()
                for line in runner.stream_output():
                    _, value = mr_job.parse_output_line(line)
                    output.add(value)

        self.assertEqual(set(output), set([0, 1, ((2 ** 3) ** 3) ** 3]))


class ParseOutputTestCase(unittest.TestCase):
    # test parse_output() method

    def test_default(self):
        # test parsing JSON
        mr_job = MRJob()
        output = '0\t1\n"a"\t"b"\n'
        mr_job.stdout = StringIO(output)
        self.assertEqual(mr_job.parse_output(), [(0, 1), ('a', 'b')])

        # verify that stdout is not cleared
        self.assertEqual(mr_job.stdout.getvalue(), output)

    def test_protocol_instance(self):
        # see if we can use the repr protocol
        mr_job = MRJob()
        output = "0\t1\n['a', 'b']\tset(['c', 'd'])\n"
        mr_job.stdout = StringIO(output)
        self.assertEqual(mr_job.parse_output(ReprProtocol()),
                         [(0, 1), (['a', 'b'], set(['c', 'd']))])

        # verify that stdout is not cleared
        self.assertEqual(mr_job.stdout.getvalue(), output)


class RunJobTestCase(SandboxedTestCase):

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
        self.assertEqual(sorted(StringIO(stdout)), ['1\t"foo"\n',
                                                    '2\t"bar"\n',
                                                    '3\tnull\n'])
        self.assertEqual(stderr, '')
        self.assertEqual(returncode, 0)

    def test_verbose(self):
        stdout, stderr, returncode = self.run_job()
        self.assertEqual(sorted(StringIO(stdout)), ['1\t"foo"\n',
                                                    '2\t"bar"\n',
                                                    '3\tnull\n'])
        self.assertNotEqual(stderr, '')
        self.assertEqual(returncode, 0)
        normal_stderr = stderr

        stdout, stderr, returncode = self.run_job(['-v'])
        self.assertEqual(sorted(StringIO(stdout)), ['1\t"foo"\n',
                                                    '2\t"bar"\n',
                                                    '3\tnull\n'])
        self.assertNotEqual(stderr, '')
        self.assertEqual(returncode, 0)
        self.assertGreater(len(stderr), len(normal_stderr))

    def test_no_output(self):
        self.assertEqual(os.listdir(self.tmp_dir), [])  # sanity check

        args = ['--no-output', '--output-dir', self.tmp_dir]
        stdout, stderr, returncode = self.run_job(args)
        self.assertEqual(stdout, '')
        self.assertNotEqual(stderr, '')
        self.assertEqual(returncode, 0)

        # make sure the correct output is in the temp dir
        self.assertNotEqual(os.listdir(self.tmp_dir), [])
        output_lines = []
        for dirpath, _, filenames in os.walk(self.tmp_dir):
            for filename in filenames:
                with open(os.path.join(dirpath, filename)) as output_f:
                    output_lines.extend(output_f)

        self.assertEqual(sorted(output_lines),
                         ['1\t"foo"\n', '2\t"bar"\n', '3\tnull\n'])


class BadMainTestCase(unittest.TestCase):
    """Ensure that the user cannot do anything but just call MRYourJob.run()
    from __main__()"""

    def test_bad_main_catch(self):
        sys.argv.append('--mapper')
        self.assertRaises(UsageError, MRBoringJob().make_runner)
        sys.argv = sys.argv[:-1]


class ProtocolTypeTestCase(unittest.TestCase):

    class StrangeJob(MRJob):

        def INPUT_PROTOCOL(self):
            return JSONProtocol()

        def INTERNAL_PROTOCOL(self):
            return JSONProtocol()

        def OUTPUT_PROTOCOL(self):
            return JSONProtocol()

    def test_attrs_should_be_classes(self):
        with no_handlers_for_logger('mrjob.job'):
            stderr = StringIO()
            log_to_stream('mrjob.job', stderr)
            job = self.StrangeJob()
            self.assertIsInstance(job.input_protocol(), JSONProtocol)
            self.assertIsInstance(job.internal_protocol(), JSONProtocol)
            self.assertIsInstance(job.output_protocol(), JSONProtocol)
            logs = stderr.getvalue()
            self.assertIn('INPUT_PROTOCOL should be a class', logs)
            self.assertIn('INTERNAL_PROTOCOL should be a class', logs)
            self.assertIn('OUTPUT_PROTOCOL should be a class', logs)


class StepsTestCase(unittest.TestCase):

    class SteppyJob(MRJob):

        def _yield_none(self, *args, **kwargs):
            yield None

        def steps(self):
            return [
                self.mr(mapper_init=self._yield_none, mapper_pre_filter='cat',
                        reducer_cmd='wc -l'),
                self.jar(name='oh my jar', jar='s3://bookat/binks_jar.jar')]

    class SingleSteppyCommandJob(MRJob):

        def mapper_cmd(self):
            return 'cat'

        def combiner_cmd(self):
            return 'cat'

        def reducer_cmd(self):
            return 'wc -l'

    def test_steps(self):
        j = self.SteppyJob(['--no-conf'])
        self.assertEqual(
            j.steps()[0],
            MRJobStep(
                mapper_init=j._yield_none,
                mapper_pre_filter='cat',
                reducer_cmd='wc -l'))
        self.assertEqual(
            j.steps()[1], JarStep('oh my jar', 's3://bookat/binks_jar.jar'))

    def test_cmd_steps(self):
        j = self.SingleSteppyCommandJob(['--no-conf'])
        self.assertEqual(
            j._steps_desc(),
            [{
                'type': 'streaming',
                'mapper': {'type': 'command', 'command': 'cat'},
                'combiner': {'type': 'command', 'command': 'cat'},
                'reducer': {'type': 'command', 'command': 'wc -l'}}])
