# -*- encoding: utf-8 -*-
# Copyright 2009-2013 Yelp and Contributors
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
"""Unit testing of MRJob."""
import os
import os.path
import sys
import time
from io import BytesIO
from subprocess import Popen
from subprocess import PIPE
from unittest import TestCase

from mrjob.conf import combine_envs
from mrjob.job import MRJob
from mrjob.job import UsageError
from mrjob.job import _im_func
from mrjob.parse import parse_mr_job_stderr
from mrjob.protocol import JSONProtocol
from mrjob.protocol import JSONValueProtocol
from mrjob.protocol import PickleProtocol
from mrjob.protocol import RawValueProtocol
from mrjob.protocol import ReprProtocol
from mrjob.protocol import ReprValueProtocol
from mrjob.protocol import StandardJSONProtocol
from mrjob.py2 import StringIO
from mrjob.step import _IDENTITY_MAPPER
from mrjob.step import _IDENTITY_REDUCER
from mrjob.step import JarStep
from mrjob.step import MRStep
from mrjob.step import SparkStep
from mrjob.util import log_to_stream

from tests.mr_hadoop_format_job import MRHadoopFormatJob
from tests.mr_cmd_job import MRCmdJob
from tests.mr_sort_values import MRSortValues
from tests.mr_tower_of_powers import MRTowerOfPowers
from tests.mr_two_step_job import MRTwoStepJob
from tests.py2 import Mock
from tests.py2 import MagicMock
from tests.py2 import patch
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
        self.assertEqual(next(j.mapper(None, None)), (None, j.sum_amount))

    def test_init_funcs(self):
        num_inputs = 2
        stdin = BytesIO(b"x\n" * num_inputs)
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


class NoTzsetTestCase(TestCase):

    def setUp(self):
        self.remove_time_tzset()

    def tearDown(self):
        """Test systems without time.tzset() (e.g. Windows). See Issue #46."""
        self.restore_time_tzset()

    def remove_time_tzset(self):
        if hasattr(time, 'tzset'):
            self._real_time_tzset = time.tzset
            del time.tzset

    def restore_time_tzset(self):
        if hasattr(self, '_real_time_tzset'):
            time.tzset = self._real_time_tzset

    def test_init_does_not_require_tzset(self):
        MRJob()


class CountersAndStatusTestCase(TestCase):

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

        parsed_stderr = parse_mr_job_stderr(mr_job.stderr.getvalue())
        self.assertEqual(parsed_stderr['counters'],
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

        parsed_stderr = parse_mr_job_stderr(mr_job.stderr.getvalue())
        self.assertEqual(parsed_stderr['counters'],
                         {'Bad items': {'a; b; c': 1},
                          'girl; interrupted': {'movie': 1}})


class ProtocolsTestCase(TestCase):
    # not putting these in their own files because we're not going to invoke
    # it as a script anyway.

    class MRBoringJob2(MRBoringJob):
        INPUT_PROTOCOL = StandardJSONProtocol
        INTERNAL_PROTOCOL = PickleProtocol
        OUTPUT_PROTOCOL = ReprProtocol

    class MRBoringJob3(MRBoringJob):

        def internal_protocol(self):
            return ReprProtocol()

    class MRBoringJob4(MRBoringJob):
        INTERNAL_PROTOCOL = ReprProtocol

    class MRTrivialJob(MRJob):
        OUTPUT_PROTOCOL = RawValueProtocol

        def mapper(self, key, value):
            yield key, value

    def assertMethodsEqual(self, fs, gs):
        # we're going to use this to match bound against unbound methods
        self.assertEqual([_im_func(f) for f in fs],
                         [_im_func(g) for g in gs])

    def test_default_protocols(self):
        mr_job = MRBoringJob()

        self.assertMethodsEqual(
            mr_job.pick_protocols(0, 'mapper'),
            (RawValueProtocol.read, JSONProtocol.write))

        self.assertMethodsEqual(
            mr_job.pick_protocols(0, 'reducer'),
            (StandardJSONProtocol.read, JSONProtocol.write))

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
        RAW_INPUT = BytesIO(b'foo\nbar\nbaz\n')

        mr_job = MRBoringJob(['--mapper'])
        mr_job.sandbox(stdin=RAW_INPUT)
        mr_job.run_mapper()

        self.assertEqual(mr_job.stdout.getvalue(),
                         b'null\t"foo"\n' +
                         b'null\t"bar"\n' +
                         b'null\t"baz"\n')

    def test_reducer_json_to_json(self):
        JSON_INPUT = BytesIO(b'"foo"\t"bar"\n' +
                             b'"foo"\t"baz"\n' +
                             b'"bar"\t"qux"\n')

        mr_job = MRBoringJob(args=['--reducer'])
        mr_job.sandbox(stdin=JSON_INPUT)
        mr_job.run_reducer()

        # ujson doesn't add whitespace to JSON
        self.assertEqual(mr_job.stdout.getvalue().replace(b' ', b''),
                         (b'"foo"\t["bar","baz"]\n' +
                          b'"bar"\t["qux"]\n'))

    def test_output_protocol_with_no_final_reducer(self):
        # if there's no reducer, the last mapper should use the
        # output protocol (in this case, repr)
        RAW_INPUT = BytesIO(b'foo\nbar\nbaz\n')

        mr_job = self.MRTrivialJob(['--mapper'])
        mr_job.sandbox(stdin=RAW_INPUT)
        mr_job.run_mapper()

        self.assertEqual(mr_job.stdout.getvalue(),
                         RAW_INPUT.getvalue())


class StrictProtocolsTestCase(EmptyMrjobConfTestCase):

    class MRBoringReprAndJSONJob(MRBoringJob):
        # allowing reading in bytes that can't be JSON-encoded
        INPUT_PROTOCOL = ReprValueProtocol
        INTERNAL_PROTOCOL = StandardJSONProtocol
        OUTPUT_PROTOCOL = StandardJSONProtocol

    class MRBoringJSONJob(MRJob):
        INPUT_PROTOCOL = StandardJSONProtocol
        INTERNAL_PROTOCOL = StandardJSONProtocol
        OUTPUT_PROTOCOL = StandardJSONProtocol

        def reducer(self, key, values):
            yield(key, list(values))

    BAD_JSON_INPUT = (b'BAD\tJSON\n' +
                      b'"foo"\t"bar"\n' +
                      b'"too"\t"many"\t"tabs"\n' +
                      b'"notabs"\n')

    UNENCODABLE_REPR_INPUT = (b"'foo'\n" +
                              b'set()\n' +
                              b"'bar'\n")

    def assertJobHandlesUndecodableInput(self, job_args=()):
        job = self.MRBoringJSONJob(job_args)
        job.sandbox(stdin=BytesIO(self.BAD_JSON_INPUT))

        with job.make_runner() as r:
            r.run()

            # good data should still get through
            self.assertEqual(b''.join(r.stream_output()), b'"foo"\t["bar"]\n')

            # exception type varies between JSON implementations,
            # so just make sure there were three exceptions of some sort
            counters = r.counters()[0]
            self.assertEqual(sorted(counters), ['Undecodable input'])
            self.assertEqual(
                sum(counters['Undecodable input'].values()), 3)

    def assertJobRaisesExceptionOnUndecodableInput(self, job_args=()):
        job = self.MRBoringJSONJob(job_args)
        job.sandbox(stdin=BytesIO(self.BAD_JSON_INPUT))

        with job.make_runner() as r:
            self.assertRaises(Exception, r.run)

    def assertJobHandlesUnencodableOutput(self, job_args=()):
        job = self.MRBoringReprAndJSONJob(job_args)
        job.sandbox(stdin=BytesIO(self.UNENCODABLE_REPR_INPUT))

        with job.make_runner() as r:
            r.run()

            # good data should still get through
            self.assertEqual(b''.join(r.stream_output()),
                             b'null\t["bar", "foo"]\n')

            counters = r.counters()[0]

            # there should be one Unencodable output error. Exception
            # type may vary by json implementation
            self.assertEqual(
                list(counters), ['Unencodable output'])
            self.assertEqual(
                list(counters['Unencodable output'].values()), [1])

    def assertJobRaisesExceptionOnUnencodableOutput(self, job_args=()):
        job = self.MRBoringReprAndJSONJob(job_args)
        job.sandbox(stdin=BytesIO(self.UNENCODABLE_REPR_INPUT))

        with job.make_runner() as r:
            self.assertRaises(Exception, r.run)

    def test_undecodable_input(self):
        self.assertJobRaisesExceptionOnUndecodableInput()

    def test_undecodable_input_strict_protocols(self):
        self.assertJobRaisesExceptionOnUndecodableInput(
            ['--strict-protocols'])

    def test_undecodable_input_no_strict_protocols(self):
        self.assertJobHandlesUndecodableInput(
            ['--no-strict-protocols'])

    def test_unencodable_output(self):
        self.assertJobRaisesExceptionOnUnencodableOutput()

    def test_unencodable_output_strict(self):
        self.assertJobRaisesExceptionOnUnencodableOutput(
            ['--strict-protocols'])

    def test_unencodable_output_no_strict_protocols(self):
        self.assertJobHandlesUnencodableOutput(
            ['--no-strict-protocols'])


class PickProtocolsTestCase(TestCase):

    def _yield_none(self, *args, **kwargs):
        yield None

    def _make_job(self, steps):

        class CustomJob(MRJob):

            INPUT_PROTOCOL = PickleProtocol
            INTERNAL_PROTOCOL = JSONProtocol
            OUTPUT_PROTOCOL = JSONValueProtocol

            def steps(self):
                return steps

        args = ['--no-conf']

        return CustomJob(args)

    def _assert_script_protocols(self, steps, expected_protocols):
        """Given a list of (read_protocol_class, write_protocol_class) tuples
        for *each substep*, assert that the given _steps_desc() output for each
        substep matches the protocols in order
        """
        j = self._make_job(steps)
        for i, step in enumerate(steps):
            expected_step = expected_protocols[i]
            step_desc = step.description(i)

            if step_desc['type'] == 'jar':
                # step_type for a non-script step is undefined
                self.assertIsNone(expected_step)
            else:
                for substep_key in ('mapper', 'combiner', 'reducer'):
                    if substep_key in step_desc:
                        self.assertIn(substep_key, expected_step)
                        expected_substep = expected_step[substep_key]

                        try:
                            actual_read, actual_write = (
                                j._pick_protocol_instances(i, substep_key))
                        except ValueError:
                            self.assertIsNone(expected_substep)
                        else:
                            expected_read, expected_write = expected_substep
                            self.assertIsInstance(actual_read, expected_read)
                            self.assertIsInstance(actual_write, expected_write)
                    else:
                        self.assertNotIn(substep_key, expected_step)

    def test_single_mapper(self):
        self._assert_script_protocols(
            [MRStep(mapper=self._yield_none)],
            [dict(mapper=(PickleProtocol, JSONValueProtocol))])

    def test_single_reducer(self):
        # MRStep transparently adds mapper
        self._assert_script_protocols(
            [MRStep(reducer=self._yield_none)],
            [dict(mapper=(PickleProtocol, JSONProtocol),
                  reducer=(JSONProtocol, JSONValueProtocol))])

    def test_mapper_combiner(self):
        self._assert_script_protocols(
            [MRStep(mapper=self._yield_none,
                    combiner=self._yield_none)],
            [dict(mapper=(PickleProtocol, JSONValueProtocol),
                  combiner=(JSONValueProtocol, JSONValueProtocol))])

    def test_mapper_combiner_reducer(self):
        self._assert_script_protocols(
            [MRStep(
                mapper=self._yield_none,
                combiner=self._yield_none,
                reducer=self._yield_none)],
            [dict(mapper=(PickleProtocol, JSONProtocol),
                  combiner=(JSONProtocol, JSONProtocol),
                  reducer=(JSONProtocol, JSONValueProtocol))])

    def test_begin_jar_step(self):
        self._assert_script_protocols(
            [JarStep(jar='binks_jar.jar'),
             MRStep(
                 mapper=self._yield_none,
                 combiner=self._yield_none,
                 reducer=self._yield_none)],
            [None,
             dict(mapper=(PickleProtocol, JSONProtocol),
                  combiner=(JSONProtocol, JSONProtocol),
                  reducer=(JSONProtocol, JSONValueProtocol))])

    def test_end_jar_step(self):
        self._assert_script_protocols(
            [MRStep(
                mapper=self._yield_none,
                combiner=self._yield_none,
                reducer=self._yield_none),
             JarStep(jar='binks_jar.jar')],
            [dict(mapper=(PickleProtocol, JSONProtocol),
                  combiner=(JSONProtocol, JSONProtocol),
                  reducer=(JSONProtocol, JSONValueProtocol)),
             None])

    def test_middle_jar_step(self):
        self._assert_script_protocols(
            [MRStep(
                mapper=self._yield_none,
                combiner=self._yield_none),
             JarStep(jar='binks_jar.jar'),
             MRStep(reducer=self._yield_none)],
            [dict(mapper=(PickleProtocol, JSONProtocol),
                  combiner=(JSONProtocol, JSONProtocol)),
             None,
             dict(reducer=(JSONProtocol, JSONValueProtocol))])

    def test_single_mapper_cmd(self):
        self._assert_script_protocols(
            [MRStep(mapper_cmd='cat')],
            [dict(mapper=None)])

    def test_single_mapper_cmd_with_script_combiner(self):
        self._assert_script_protocols(
            [MRStep(
                mapper_cmd='cat',
                combiner=self._yield_none)],
            [dict(mapper=None,
                  combiner=(RawValueProtocol, RawValueProtocol))])

    def test_single_mapper_cmd_with_script_reducer(self):
        # reducer is only script step so it uses INPUT_PROTOCOL and
        # OUTPUT_PROTOCOL
        self._assert_script_protocols(
            [MRStep(
                mapper_cmd='cat',
                reducer=self._yield_none)],
            [dict(mapper=None,
                  reducer=(PickleProtocol, JSONValueProtocol))])

    def test_multistep(self):
        # reducer is only script step so it uses INPUT_PROTOCOL and
        # OUTPUT_PROTOCOL
        self._assert_script_protocols(
            [MRStep(mapper_cmd='cat',
                    reducer=self._yield_none),
             JarStep(jar='binks_jar.jar'),
             MRStep(mapper=self._yield_none)],
            [dict(mapper=None,
                  reducer=(PickleProtocol, JSONProtocol)),
             None,
             dict(mapper=(JSONProtocol, JSONValueProtocol))])


class JobConfTestCase(TestCase):

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
        self.assert_hadoop_version(self.MRHadoopVersionJobConfJob2, '0.20')

    def test_jobconf_method(self):
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


class LibjarsTestCase(TestCase):

    def test_default(self):
        job = MRJob()

        self.assertEqual(job.job_runner_kwargs()['libjars'], [])

    def test_libjar_option(self):
        job = MRJob(['--libjar', 'honey.jar'])

        self.assertEqual(job.job_runner_kwargs()['libjars'], ['honey.jar'])

    def test_libjars_attr(self):
        with patch.object(MRJob, 'LIBJARS', ['/left/dora.jar']):
            job = MRJob()

            self.assertEqual(job.job_runner_kwargs()['libjars'],
                             ['/left/dora.jar'])

    def test_libjars_attr_plus_option(self):
        with patch.object(MRJob, 'LIBJARS', ['/left/dora.jar']):
            job = MRJob(['--libjar', 'honey.jar'])

            self.assertEqual(job.job_runner_kwargs()['libjars'],
                             ['/left/dora.jar', 'honey.jar'])

    def test_libjars_attr_relative_path(self):
        job_dir = os.path.dirname(MRJob.mr_job_script())

        with patch.object(MRJob, 'LIBJARS', ['cookie.jar', '/left/dora.jar']):
            job = MRJob()

            self.assertEqual(
                job.job_runner_kwargs()['libjars'],
                [os.path.join(job_dir, 'cookie.jar'), '/left/dora.jar'])

    def test_libjars_environment_variables(self):
        job_dir = os.path.dirname(MRJob.mr_job_script())

        with patch.dict('os.environ', A='/path/to/a', B='b'):
            with patch.object(MRJob, 'LIBJARS',
                              ['$A/cookie.jar', '$B/honey.jar']):
                job = MRJob()

                # libjars() peeks into envvars to figure out if the path
                # is relative or absolute
                self.assertEqual(
                    job.job_runner_kwargs()['libjars'],
                    ['$A/cookie.jar', os.path.join(job_dir, '$B/honey.jar')])

    def test_override_libjars(self):
        with patch.object(MRJob, 'libjars', return_value=['honey.jar']):
            job = MRJob(['--libjar', 'cookie.jar'])

            # ignore switch, don't resolve relative path
            self.assertEqual(job.job_runner_kwargs()['libjars'], ['honey.jar'])


class MRSortValuesAndMore(MRSortValues):
    PARTITIONER = 'org.apache.hadoop.mapred.lib.HashPartitioner'

    JOBCONF = {
        'stream.num.map.output.key.fields': 3,
        'mapred.output.key.comparator.class':
        'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
        'mapred.text.key.comparator.options': '-k1 -k2nr',
    }


class HadoopFormatTestCase(TestCase):

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


class PartitionerTestCase(TestCase):

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


class IsTaskTestCase(TestCase):

    def test_is_task(self):
        self.assertEqual(MRJob().is_task(), False)
        self.assertEqual(MRJob(['--mapper']).is_task(), True)
        self.assertEqual(MRJob(['--reducer']).is_task(), True)
        self.assertEqual(MRJob(['--combiner']).is_task(), True)
        self.assertEqual(MRJob(['--spark']).is_task(), True)
        self.assertEqual(MRJob(['--steps']).is_task(), False)

    def test_deprecated_alias(self):
        with logger_disabled('mrjob.launch'):
            self.assertEqual(MRJob().is_mapper_or_reducer(), False)
            self.assertEqual(MRJob(['--mapper']).is_mapper_or_reducer(), True)


class StepNumTestCase(TestCase):

    def test_two_step_job_end_to_end(self):
        # represent input as a list so we can reuse it
        # also, leave off newline (MRJobRunner should fix it)
        mapper0_input_lines = [b'foo', b'bar']

        def test_mapper0(mr_job, input_lines):
            mr_job.sandbox(input_lines)
            mr_job.run_mapper(0)
            self.assertEqual(mr_job.stdout.getvalue(),
                             b'null\t"foo"\n' + b'"foo"\tnull\n' +
                             b'null\t"bar"\n' + b'"bar"\tnull\n')

        mapper0 = MRTwoStepJob()
        test_mapper0(mapper0, mapper0_input_lines)

        # --step-num=0 shouldn't actually be necessary
        mapper0_no_step_num = MRTwoStepJob(['--mapper'])
        test_mapper0(mapper0_no_step_num, mapper0_input_lines)

        # sort output of mapper0
        mapper0_output_input_lines = BytesIO(mapper0.stdout.getvalue())
        reducer0_input_lines = sorted(mapper0_output_input_lines,
                                      key=lambda line: line.split(b'\t'))

        def test_reducer0(mr_job, input_lines):
            mr_job.sandbox(input_lines)
            mr_job.run_reducer(0)
            self.assertEqual(mr_job.stdout.getvalue(),
                             b'"bar"\t1\n' + b'"foo"\t1\n' + b'null\t2\n')

        reducer0 = MRTwoStepJob()
        test_reducer0(reducer0, reducer0_input_lines)

        # --step-num=0 shouldn't actually be necessary
        reducer0_no_step_num = MRTwoStepJob(['--reducer'])
        test_reducer0(reducer0_no_step_num, reducer0_input_lines)

        # mapper can use reducer0's output as-is
        mapper1_input_lines = BytesIO(reducer0.stdout.getvalue())

        def test_mapper1(mr_job, input_lines):
            mr_job.sandbox(input_lines)
            mr_job.run_mapper(1)
            self.assertEqual(mr_job.stdout.getvalue(),
                             b'1\t"bar"\n' + b'1\t"foo"\n' + b'2\tnull\n')

        mapper1 = MRTwoStepJob()
        test_mapper1(mapper1, mapper1_input_lines)

    def test_nonexistent_steps(self):
        mr_job = MRTwoStepJob()
        mr_job.sandbox()
        self.assertRaises(ValueError, mr_job.run_reducer, 1)
        self.assertRaises(ValueError, mr_job.run_mapper, 2)
        self.assertRaises(ValueError, mr_job.run_reducer, -1)

    def test_wrong_type_of_step(self):
        mr_job = MRJob()
        mr_job.spark = MagicMock()

        self.assertRaises(TypeError, mr_job.run_mapper)
        self.assertRaises(TypeError, mr_job.run_combiner)
        self.assertRaises(TypeError, mr_job.run_reducer)


class FileOptionsTestCase(SandboxedTestCase):

    def test_end_to_end(self):
        n_file_path = os.path.join(self.tmp_dir, 'n_file')

        with open(n_file_path, 'w') as f:
            f.write('3')

        os.environ['LOCAL_N_FILE_PATH'] = n_file_path

        stdin = [b'0\n', b'1\n', b'2\n']

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


class RunJobTestCase(SandboxedTestCase):

    def run_job(self, args=()):
        args = ([sys.executable, MRTwoStepJob.mr_job_script()] +
                list(args) + ['--no-conf'])
        # add . to PYTHONPATH (in case mrjob isn't actually installed)
        env = combine_envs(os.environ,
                           {'PYTHONPATH': os.path.abspath('.')})
        proc = Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE, env=env)
        stdout, stderr = proc.communicate(input=b'foo\nbar\nbar\n')
        return stdout, stderr, proc.returncode

    def test_quiet(self):
        stdout, stderr, returncode = self.run_job(['-q'])
        self.assertEqual(sorted(BytesIO(stdout)),
                         [b'1\t"foo"\n', b'2\t"bar"\n', b'3\tnull\n'])

        self.assertEqual(stderr, b'')
        self.assertEqual(returncode, 0)

    def test_verbose(self):
        stdout, stderr, returncode = self.run_job()
        self.assertEqual(sorted(BytesIO(stdout)),
                         [b'1\t"foo"\n', b'2\t"bar"\n', b'3\tnull\n'])

        self.assertNotEqual(stderr, '')
        self.assertEqual(returncode, 0)
        normal_stderr = stderr

        stdout, stderr, returncode = self.run_job(['-v'])
        self.assertEqual(sorted(BytesIO(stdout)),
                         [b'1\t"foo"\n', b'2\t"bar"\n', b'3\tnull\n'])

        self.assertNotEqual(stderr, b'')
        self.assertEqual(returncode, 0)
        self.assertGreater(len(stderr), len(normal_stderr))

    def test_no_output(self):
        self.assertEqual(os.listdir(self.tmp_dir), [])  # sanity check

        args = ['--no-output', '--output-dir', self.tmp_dir]
        stdout, stderr, returncode = self.run_job(args)
        self.assertEqual(stdout, b'')
        self.assertNotEqual(stderr, b'')
        self.assertEqual(returncode, 0)

        # make sure the correct output is in the temp dir
        self.assertNotEqual(os.listdir(self.tmp_dir), [])
        output_lines = []
        for dirpath, _, filenames in os.walk(self.tmp_dir):
            for filename in filenames:
                with open(os.path.join(dirpath, filename), 'rb') as output_f:
                    output_lines.extend(output_f)

        self.assertEqual(sorted(output_lines),
                         [b'1\t"foo"\n', b'2\t"bar"\n', b'3\tnull\n'])


class BadMainTestCase(TestCase):
    """Ensure that the user cannot do anything but just call MRYourJob.run()
    from __main__()"""

    def test_bad_main_catch(self):
        sys.argv.append('--mapper')
        self.assertRaises(UsageError, MRBoringJob().make_runner)
        sys.argv = sys.argv[:-1]


class ProtocolTypeTestCase(TestCase):

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


class StepsTestCase(TestCase):

    class SteppyJob(MRJob):

        def _yield_none(self, *args, **kwargs):
            yield None

        def steps(self):
            return [
                MRStep(mapper_init=self._yield_none, mapper_pre_filter='cat',
                       reducer_cmd='wc -l'),
                JarStep(jar='s3://bookat/binks_jar.jar')]

    class SingleSteppyCommandJob(MRJob):

        def mapper_cmd(self):
            return 'cat'

        def combiner_cmd(self):
            return 'cat'

        def reducer_cmd(self):
            return 'wc -l'

    class SingleStepJobConfMethodJob(MRJob):
        def mapper(self, key, value):
            return None

        def jobconf(self):
            return {'mapred.baz': 'bar'}

    class PreMRFilterJob(MRJob):

        def mapper_pre_filter(self):
            return 'grep m'

        def combiner_pre_filter(self):
            return 'grep c'

        def reducer_pre_filter(self):
            return 'grep r'

    # for spark testing used mock methods instead

    def test_steps(self):
        j = self.SteppyJob(['--no-conf'])
        self.assertEqual(
            j.steps()[0],
            MRStep(
                mapper_init=j._yield_none,
                mapper_pre_filter='cat',
                reducer_cmd='wc -l'))
        self.assertEqual(
            j.steps()[1], JarStep(jar='s3://bookat/binks_jar.jar'))

    def test_cmd_steps(self):
        j = self.SingleSteppyCommandJob(['--no-conf'])
        self.assertEqual(
            j._steps_desc(),
            [{
                'type': 'streaming',
                'mapper': {'type': 'command', 'command': 'cat'},
                'combiner': {'type': 'command', 'command': 'cat'},
                'reducer': {'type': 'command', 'command': 'wc -l'}}])

    def test_can_override_jobconf_method(self):
        # regression test for #656
        j = self.SingleStepJobConfMethodJob(['--no-conf'])

        # overriding jobconf() should affect job_runner_kwargs()
        # but not step definitions
        self.assertEqual(j.job_runner_kwargs()['jobconf'],
                         {'mapred.baz': 'bar'})

        self.assertEqual(
            j.steps()[0],
            MRStep(mapper=j.mapper))

    def test_pre_filters(self):
        j = self.PreMRFilterJob(['--no-conf'])
        self.assertEqual(
            j._steps_desc(),
            [
                dict(
                    type='streaming',
                    mapper=dict(type='script', pre_filter='grep m'),
                    combiner=dict(type='script', pre_filter='grep c'),
                    reducer=dict(type='script', pre_filter='grep r'),
                )
            ])

    def test_spark_method(self):
        j = MRJob(['--no-conf'])
        j.spark = MagicMock()

        self.assertEqual(
            j.steps(),
            [SparkStep(j.spark)]
        )

        self.assertEqual(
            j._steps_desc(),
            [dict(type='spark', jobconf={}, spark_args=[])]
        )

    def test_spark_and_spark_args_methods(self):
        j = MRJob(['--no-conf'])
        j.spark = MagicMock()
        j.spark_args = MagicMock(return_value=['argh', 'ARRRRGH!'])

        self.assertEqual(
            j.steps(),
            [SparkStep(j.spark, spark_args=['argh', 'ARRRRGH!'])]
        )

        self.assertEqual(
            j._steps_desc(),
            [dict(type='spark', jobconf={}, spark_args=['argh', 'ARRRRGH!'])]
        )

    def test_spark_and_streaming_dont_mix(self):
        j = MRJob(['--no-conf'])
        j.mapper = MagicMock()
        j.spark = MagicMock()

        self.assertRaises(ValueError, j.steps)

    def test_spark_args_ignored_without_spark(self):
        j = MRJob(['--no-conf'])
        j.reducer = MagicMock()
        j.spark_args = MagicMock(spark_args=['argh', 'ARRRRGH!'])

        self.assertEqual(j.steps(), [MRStep(reducer=j.reducer)])


class DeprecatedMRMethodTestCase(TestCase):

    def test_mr(self):
        kwargs = {
            'mapper': _IDENTITY_MAPPER,
            'reducer': _IDENTITY_REDUCER,
        }

        with logger_disabled('mrjob.job'):
            self.assertEqual(MRJob.mr(**kwargs), MRStep(**kwargs))


class RunSparkTestCase(TestCase):

    def test_spark(self):
        job = MRJob(['--spark', 'input_dir', 'output_dir'])
        job.spark = MagicMock()

        job.execute()

        job.spark.assert_called_once_with('input_dir', 'output_dir')

    def test_spark_with_step_num(self):
        job = MRJob(['--step-num=1', '--spark', 'input_dir', 'output_dir'])

        mapper = MagicMock()
        spark = MagicMock()

        job.steps = Mock(
            return_value=[MRStep(mapper=mapper), SparkStep(spark)])

        job.execute()

        spark.assert_called_once_with('input_dir', 'output_dir')
        self.assertFalse(mapper.called)

    def test_wrong_step_type(self):
        job = MRJob(['--spark', 'input_dir', 'output_dir'])
        job.mapper = MagicMock()

        self.assertRaises(TypeError, job.execute)

    def test_wrong_step_num(self):
        job = MRJob(['--step-num=1', '--spark', 'input_dir', 'output_dir'])
        job.spark = MagicMock()

        self.assertRaises(ValueError, job.execute)

    def test_too_few_args(self):
        job = MRJob(['--spark'])
        job.spark = MagicMock()

        self.assertRaises(ValueError, job.execute)

    def test_too_many_args(self):
        job = MRJob(['--spark', 'input_dir', 'output_dir', 'error_dir'])
        job.spark = MagicMock()

        self.assertRaises(ValueError, job.execute)


class PrintHelpTestCase(SandboxedTestCase):

    def setUp(self):
        super(PrintHelpTestCase, self).setUp()

        self.exit = self.start(patch('sys.exit'))
        self.stdout = self.start(patch.object(sys, 'stdout', StringIO()))

    def test_basic_help(self):
        MRJob(['--help'])
        self.exit.assert_called_once_with(0)

        output = self.stdout.getvalue()
        # basic option
        self.assertIn('--conf', output)

        # not basic options
        self.assertNotIn('--step-num', output)
        self.assertNotIn('--s3-endpoint', output)

        # deprecated options
        self.assertNotIn('--partitioner', output)
        self.assertIn('add --deprecated', output)
        self.assertNotIn('--deprecated=DEPRECATED', output)

    def test_basic_help_deprecated(self):
        MRJob(['--help', '--deprecated'])
        self.exit.assert_called_once_with(0)

        output = self.stdout.getvalue()
        # basic option
        self.assertIn('--conf', output)

        # not basic options
        self.assertNotIn('--step-num', output)
        self.assertNotIn('--s3-endpoint', output)

        # deprecated options
        self.assertIn('--partitioner', output)
        self.assertNotIn('add --deprecated', output)
        self.assertIn('--deprecated=DEPRECATED', output)

    def test_runner_help(self):
        MRJob(['--help', '-r', 'emr'])
        self.exit.assert_called_once_with(0)

        output = self.stdout.getvalue()
        # EMR runner option
        self.assertIn('--s3-endpoint', output)

        # not runner options
        self.assertNotIn('--conf', output)
        self.assertNotIn('--step-num', output)

        # a runner option, but not for EMR
        self.assertNotIn('--gcp-project', output)

        # deprecated options
        self.assertNotIn('--bootstrap-cmd', output)

    def test_deprecated_runner_help(self):
        MRJob(['--help', '-r', 'emr', '--deprecated'])
        self.exit.assert_called_once_with(0)

        output = self.stdout.getvalue()
        # EMR runner option
        self.assertIn('--s3-endpoint', output)

        # not runner options
        self.assertNotIn('--conf', output)
        self.assertNotIn('--step-num', output)

        # a runner option, but not for EMR
        self.assertNotIn('--gcp-project', output)

        # deprecated options
        self.assertIn('--bootstrap-cmd', output)

    def test_steps_help(self):
        MRJob(['--help', '--steps'])
        self.exit.assert_called_once_with(0)

        output = self.stdout.getvalue()
        # step option
        self.assertIn('--step-num', output)

        # not step options
        self.assertNotIn('--conf', output)
        self.assertNotIn('--s3-endpoint', output)

    def test_passthrough_options(self):
        MRCmdJob(['--help'])
        self.exit.assert_called_once_with(0)

        output = self.stdout.getvalue()
        self.assertIn('--reducer-cmd-2', output)
