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
from subprocess import Popen
from subprocess import PIPE
from StringIO import StringIO
import sys
import tempfile
from testify import TestCase
from testify import assert_equal
from testify import assert_in
from testify import assert_not_equal
from testify import assert_not_in
from testify import assert_gt
from testify import assert_raises
from testify import setup
from testify import teardown
import time

from mrjob.conf import combine_envs
from mrjob.job import MRJob
from mrjob.job import _IDENTITY_MAPPER
from mrjob.job import UsageError
from mrjob.local import LocalMRJobRunner
from mrjob.parse import parse_mr_job_stderr
from mrjob.protocol import JSONProtocol
from mrjob.protocol import PickleProtocol
from mrjob.protocol import RawValueProtocol
from mrjob.protocol import ReprProtocol
from mrjob.util import log_to_stream
from tests.mr_hadoop_format_job import MRHadoopFormatJob
from tests.mr_tower_of_powers import MRTowerOfPowers
from tests.mr_two_step_job import MRTwoStepJob
from tests.mr_nomapper_multistep import MRNoMapper
from tests.quiet import logger_disabled
from tests.quiet import no_handlers_for_logger


def stepdict(mapper=_IDENTITY_MAPPER, reducer=None, combiner=None,
             mapper_init=None, mapper_final=None,
             reducer_init=None, reducer_final=None,
             combiner_init=None, combiner_final=None,
             **kwargs):
    d = dict(mapper=mapper,
             mapper_init=mapper_init,
             mapper_final=mapper_final,
             reducer=reducer,
             reducer_init=reducer_init,
             reducer_final=reducer_final,
             combiner=combiner,
             combiner_init=combiner_init,
             combiner_final=combiner_final)
    d.update(kwargs)
    return d


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


class MRInvisibleMapperJob(MRJob):

    def mapper_init(self):
        self.things = 0

    def mapper(self, key, value):
        self.things += 1

    def mapper_final(self):
        yield None, self.things


class MRInvisibleReducerJob(MRJob):

    def reducer_init(self):
        self.things = 0

    def reducer(self, key, values):
        self.things += len(list(values))

    def reducer_final(self):
        yield None, self.things


class MRInvisibleCombinerJob(MRJob):

    def mapper(self, key, value):
        yield key, 1

    def combiner_init(self):
        self.things = 0

    def combiner(self, key, values):
        self.things += len(list(values))

    def combiner_final(self):
        yield None, self.things


class MRCustomBoringJob(MRBoringJob):

    def configure_options(self):
        super(MRCustomBoringJob, self).configure_options()

        self.add_passthrough_option(
            '--foo-size', '-F', type='int', dest='foo_size', default=5)
        self.add_passthrough_option(
            '--bar-name', '-B', type='string', dest='bar_name', default=None)
        self.add_passthrough_option(
            '--enable-baz-mode', '-M', action='store_true', dest='baz_mode',
            default=False)
        self.add_passthrough_option(
            '--disable-quuxing', '-Q', action='store_false', dest='quuxing',
            default=True)
        self.add_passthrough_option(
            '--pill-type', '-T', type='choice', choices=(['red', 'blue']),
            default='blue')
        self.add_passthrough_option(
            '--planck-constant', '-C', type='float', default=6.626068e-34)
        self.add_passthrough_option(
            '--extra-special-arg', '-S', action='append',
            dest='extra_special_args', default=[])

        self.add_file_option('--foo-config', dest='foo_config', default=None)
        self.add_file_option('--accordian-file', dest='accordian_files',
                             action='append', default=[])


### Test cases ###

class MRTestCase(TestCase):
    # some basic testing for the mr() function
    def test_mr(self):

        def mapper(k, v):
            pass

        def mapper_init():
            pass

        def mapper_final():
            pass

        def reducer(k, vs):
            pass

        def reducer_init():
            pass

        def reducer_final():
            pass

        # make sure it returns the format we currently expect
        assert_equal(MRJob.mr(mapper, reducer),
                     stepdict(mapper, reducer))
        assert_equal(MRJob.mr(mapper, reducer,
                              mapper_init=mapper_init,
                              mapper_final=mapper_final,
                              reducer_init=reducer_init,
                              reducer_final=reducer_final),
                     stepdict(mapper, reducer,
                              mapper_init=mapper_init,
                              mapper_final=mapper_final,
                              reducer_init=reducer_init,
                              reducer_final=reducer_final))
        assert_equal(MRJob.mr(mapper),
                     stepdict(mapper))

    def test_no_mapper(self):

        def mapper_init():
            pass

        def mapper_final():
            pass

        def reducer(k, vs):
            pass

        assert_raises(Exception, MRJob.mr)
        assert_equal(MRJob.mr(reducer=reducer),
                     stepdict(reducer=reducer))
        assert_equal(MRJob.mr(reducer=reducer,
                              mapper_final=mapper_final),
                     stepdict(reducer=reducer,
                              mapper_final=mapper_final))
        assert_equal(MRJob.mr(reducer=reducer,
                              mapper_init=mapper_init),
                     stepdict(reducer=reducer,
                              mapper_init=mapper_init))

    def test_no_reducer(self):

        def reducer_init():
            pass

        def reducer_final():
            pass

        assert_equal(MRJob.mr(reducer_init=reducer_init),
                     stepdict(reducer_init=reducer_init))
        assert_equal(MRJob.mr(reducer_final=reducer_final),
                     stepdict(reducer_final=reducer_final))


class MRInitTestCase(TestCase):

    def test_init_funcs(self):
        num_inputs = 2
        stdin = StringIO("x\n" * num_inputs)
        mr_job = MRInitJob(['-r', 'inline', '--no-conf', '-'])
        mr_job.sandbox(stdin=stdin)

        results = []
        with mr_job.make_runner() as runner:
            runner.run()
            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append(value)
        # these numbers should match if mapper_init, reducer_init, and
        # combiner_init were called as expected
        assert_equal(results[0], num_inputs * 10 * 10 * 2)


class MRNoOutputTestCase(TestCase):

    def _test_no_main_with_class(self, cls):
        num_inputs = 2
        stdin = StringIO("x\n" * num_inputs)
        mr_job = cls(['-r', 'inline', '--no-conf', '--strict-protocols', '-'])
        mr_job.sandbox(stdin=stdin)

        results = []
        with mr_job.make_runner() as runner:
            runner.run()
            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
                results.append(value)
        assert_equal(results[0], num_inputs)

    def test_no_map(self):
        self._test_no_main_with_class(MRInvisibleMapperJob)

    def test_no_reduce(self):
        self._test_no_main_with_class(MRInvisibleReducerJob)

    def test_no_combine(self):
        self._test_no_main_with_class(MRInvisibleCombinerJob)


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

    def test_default_protocols(self):
        mr_job = MRBoringJob()
        assert_equal(mr_job.pick_protocols(0, 'M'),
                     (RawValueProtocol.read, JSONProtocol.write))
        assert_equal(mr_job.pick_protocols(0, 'R'),
                     (JSONProtocol.read, JSONProtocol.write))

    def test_explicit_default_protocols(self):
        mr_job2 = self.MRBoringJob2().sandbox()
        assert_equal(mr_job2.pick_protocols(0, 'M'),
                     (JSONProtocol.read, PickleProtocol.write))
        assert_equal(mr_job2.pick_protocols(0, 'R'),
                     (PickleProtocol.read, ReprProtocol.write))

        mr_job3 = self.MRBoringJob3()
        assert_equal(mr_job3.pick_protocols(0, 'M'),
                     (RawValueProtocol.read, ReprProtocol.write))
        # output protocol should default to JSON
        assert_equal(mr_job3.pick_protocols(0, 'R'),
                     (ReprProtocol.read, JSONProtocol.write))

        mr_job4 = self.MRBoringJob4()
        assert_equal(mr_job4.pick_protocols(0, 'M'),
                     (RawValueProtocol.read, ReprProtocol.write))
        # output protocol should default to JSON
        assert_equal(mr_job4.pick_protocols(0, 'R'),
                     (ReprProtocol.read, JSONProtocol.write))

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


class DeprecatedProtocolsTestCase(TestCase):
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

    class MRInconsistentJob(MRJob):
        DEFAULT_INPUT_PROTOCOL = 'json'
        INPUT_PROTOCOL = ReprProtocol

    class MRInconsistentJob2(MRJob):
        DEFAULT_INPUT_PROTOCOL = 'json'

        def input_protocol(self):
            return ReprProtocol()

    def test_mixed_behavior(self):
        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.job', stderr)
            mr_job = self.MRInconsistentJob()
            assert_equal(mr_job.options.input_protocol, None)
            assert_equal(mr_job.input_protocol().__class__, ReprProtocol)
            assert_in('custom behavior', stderr.getvalue())

    def test_mixed_behavior_2(self):
        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.job', stderr)
            mr_job = self.MRInconsistentJob2()
            assert_equal(mr_job.options.input_protocol, None)
            assert_equal(mr_job.input_protocol().__class__, ReprProtocol)
            assert_in('custom behavior', stderr.getvalue())

    def test_default_protocols(self):
        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.job', stderr)
            mr_job = MRBoringJob()
            assert_equal(mr_job.options.input_protocol, 'raw_value')
            assert_equal(mr_job.options.protocol, 'json')
            assert_equal(mr_job.options.output_protocol, 'json')
            assert_not_in('deprecated', stderr.getvalue())

    def test_explicit_default_protocols(self):
        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.job', stderr)
            mr_job2 = self.MRBoringJob2().sandbox(stderr=stderr)
            assert_equal(mr_job2.options.input_protocol, 'json')
            assert_equal(mr_job2.options.protocol, 'pickle')
            assert_equal(mr_job2.options.output_protocol, 'repr')
            assert_in('deprecated', stderr.getvalue())

        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.job', stderr)
            mr_job3 = self.MRBoringJob3()
            assert_equal(mr_job3.options.input_protocol, 'raw_value')
            assert_equal(mr_job3.options.protocol, 'repr')
            # output protocol should default to protocol
            assert_equal(mr_job3.options.output_protocol, 'repr')
            assert_in('deprecated', stderr.getvalue())

    def test_setting_protocol(self):
        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.job', stderr)
            mr_job2 = MRBoringJob(args=[
                '--input-protocol=json', '--protocol=pickle',
                '--output-protocol=repr'])
            assert_equal(mr_job2.options.input_protocol, 'json')
            assert_equal(mr_job2.options.protocol, 'pickle')
            assert_equal(mr_job2.options.output_protocol, 'repr')
            assert_in('deprecated', stderr.getvalue())

        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.job', stderr)
            mr_job3 = MRBoringJob(args=['--protocol=repr'])
            assert_equal(mr_job3.options.input_protocol, 'raw_value')
            assert_equal(mr_job3.options.protocol, 'repr')
            # output protocol should default to protocol
            assert_equal(mr_job3.options.output_protocol, 'repr')
            assert_in('deprecated', stderr.getvalue())

    def test_overriding_explicit_default_protocols(self):
        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.job', stderr)
            mr_job = self.MRBoringJob2(args=['--protocol=json'])
            assert_equal(mr_job.options.input_protocol, 'json')
            assert_equal(mr_job.options.protocol, 'json')
            assert_equal(mr_job.options.output_protocol, 'repr')
            assert_in('deprecated', stderr.getvalue())

    def test_mapper_raw_value_to_json(self):
        RAW_INPUT = StringIO('foo\nbar\nbaz\n')

        with no_handlers_for_logger():
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

        with no_handlers_for_logger():
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

        with no_handlers_for_logger():
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

        with no_handlers_for_logger():
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

        with no_handlers_for_logger():
            mr_job = MRBoringJob(args=['--reducer', '--strict-protocols'])
        mr_job.sandbox(stdin=BAD_JSON_INPUT)

        # make sure it raises an exception
        assert_raises(Exception, mr_job.run_reducer)

    def test_unencodable_output(self):
        UNENCODABLE_RAW_INPUT = StringIO('foo\n' +
                                         '\xaa\n' +
                                         'bar\n')

        with no_handlers_for_logger():
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

        with no_handlers_for_logger():
            mr_job = MRBoringJob(args=['--mapper', '--strict-protocols'])
        mr_job.sandbox(stdin=UNENCODABLE_RAW_INPUT)

        # make sure it raises an exception
        assert_raises(Exception, mr_job.run_mapper)


class JobConfTestCase(TestCase):

    class MRJobConfJob(MRJob):
        JOBCONF = {'mapred.foo': 'garply',
                   'mapred.bar.bar.baz': 'foo'}

    class MRJobConfMethodJob(MRJob):
        def jobconf(self):
            return {'mapred.baz': 'bar'}

    def test_empty(self):
        mr_job = MRJob()

        assert_equal(mr_job.job_runner_kwargs()['jobconf'], {})

    def test_cmd_line_options(self):
        mr_job = MRJob([
            '--jobconf', 'mapred.foo=bar',
            '--jobconf', 'mapred.foo=baz',
            '--jobconf', 'mapred.qux=quux',
        ])

        assert_equal(mr_job.job_runner_kwargs()['jobconf'],
                     {'mapred.foo': 'baz',  # second option takes priority
                      'mapred.qux': 'quux'})

    def test_jobconf_attr(self):
        mr_job = self.MRJobConfJob()

        assert_equal(mr_job.job_runner_kwargs()['jobconf'],
                     {'mapred.foo': 'garply',
                      'mapred.bar.bar.baz': 'foo'})

    def test_jobconf_attr_and_cmd_line_options(self):
        mr_job = self.MRJobConfJob([
            '--jobconf', 'mapred.foo=bar',
            '--jobconf', 'mapred.foo=baz',
            '--jobconf', 'mapred.qux=quux',
        ])

        assert_equal(mr_job.job_runner_kwargs()['jobconf'],
                     {'mapred.bar.bar.baz': 'foo',
                      'mapred.foo': 'baz',  # command line takes priority
                      'mapred.qux': 'quux'})

    def test_redefined_jobconf_method(self):
        mr_job = self.MRJobConfMethodJob()

        assert_equal(mr_job.job_runner_kwargs()['jobconf'],
                     {'mapred.baz': 'bar'})

    def test_redefined_jobconf_method_overrides_cmd_line(self):
        mr_job = self.MRJobConfMethodJob([
            '--jobconf', 'mapred.foo=bar',
            '--jobconf', 'mapred.baz=foo',
        ])

        # --jobconf is ignored because that's the way we defined jobconf()
        assert_equal(mr_job.job_runner_kwargs()['jobconf'],
                     {'mapred.baz': 'bar'})


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

        assert_equal(mr_job.job_runner_kwargs()['hadoop_input_format'], None)
        assert_equal(mr_job.job_runner_kwargs()['hadoop_output_format'], None)

    def test_hadoop_format_attributes(self):
        mr_job = MRHadoopFormatJob()

        assert_equal(mr_job.job_runner_kwargs()['hadoop_input_format'],
                     'mapred.FooInputFormat')
        assert_equal(mr_job.job_runner_kwargs()['hadoop_output_format'],
                     'mapred.BarOutputFormat')

    def test_hadoop_format_methods(self):
        mr_job = self.MRHadoopFormatMethodJob()

        assert_equal(mr_job.job_runner_kwargs()['hadoop_input_format'],
                     'mapred.ReasonableInputFormat')
        assert_equal(mr_job.job_runner_kwargs()['hadoop_output_format'],
                     'mapred.EbcdicDb2EnterpriseXmlOutputFormat')

    def test_deprecated_command_line_options(self):
        mr_job = MRJob([
            '--hadoop-input-format',
            'org.apache.hadoop.mapred.lib.NLineInputFormat',
            '--hadoop-output-format',
            'org.apache.hadoop.mapred.FileOutputFormat',
            ])

        with logger_disabled('mrjob.job'):
            assert_equal(mr_job.job_runner_kwargs()['hadoop_input_format'],
                         'org.apache.hadoop.mapred.lib.NLineInputFormat')
            assert_equal(mr_job.job_runner_kwargs()['hadoop_output_format'],
                         'org.apache.hadoop.mapred.FileOutputFormat')

    def test_deprecated_command_line_options_override_attrs(self):
        mr_job = MRHadoopFormatJob([
            '--hadoop-input-format',
            'org.apache.hadoop.mapred.lib.NLineInputFormat',
            '--hadoop-output-format',
            'org.apache.hadoop.mapred.FileOutputFormat',
            ])

        with logger_disabled('mrjob.job'):
            assert_equal(mr_job.job_runner_kwargs()['hadoop_input_format'],
                         'org.apache.hadoop.mapred.lib.NLineInputFormat')
            assert_equal(mr_job.job_runner_kwargs()['hadoop_output_format'],
                         'org.apache.hadoop.mapred.FileOutputFormat')


class PartitionerTestCase(TestCase):

    class MRPartitionerJob(MRJob):
        PARTITIONER = 'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner'

    def test_empty(self):
        mr_job = MRJob()

        assert_equal(mr_job.job_runner_kwargs()['partitioner'], None)

    def test_cmd_line_options(self):
        mr_job = MRJob([
            '--partitioner', 'java.lang.Object',
            '--partitioner', 'org.apache.hadoop.mapreduce.Partitioner'
        ])

        # second option takes priority
        assert_equal(mr_job.job_runner_kwargs()['partitioner'],
                     'org.apache.hadoop.mapreduce.Partitioner')

    def test_partitioner_attr(self):
        mr_job = self.MRPartitionerJob()

        assert_equal(mr_job.job_runner_kwargs()['partitioner'],
                     'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner')

    def test_partitioner_attr_and_cmd_line_options(self):
        mr_job = self.MRPartitionerJob([
            '--partitioner', 'java.lang.Object',
            '--partitioner', 'org.apache.hadoop.mapreduce.Partitioner'
        ])

        # command line takes priority
        assert_equal(mr_job.job_runner_kwargs()['partitioner'],
                     'org.apache.hadoop.mapreduce.Partitioner')


class IsMapperOrReducerTestCase(TestCase):

    def test_is_mapper_or_reducer(self):
        assert_equal(MRJob().is_mapper_or_reducer(), False)
        assert_equal(MRJob(['--mapper']).is_mapper_or_reducer(), True)
        assert_equal(MRJob(['--reducer']).is_mapper_or_reducer(), True)
        assert_equal(MRJob(['--combiner']).is_mapper_or_reducer(), True)
        assert_equal(MRJob(['--steps']).is_mapper_or_reducer(), False)


class StepsTestCase(TestCase):

    def test_auto_build_steps(self):
        mrbj = MRBoringJob()
        assert_equal(mrbj.steps(),
                     [stepdict(mapper=mrbj.mapper,
                               reducer=mrbj.reducer)])

        mrfbj = MRFinalBoringJob()
        assert_equal(mrfbj.steps(),
                     [stepdict(mapper=mrfbj.mapper,
                               mapper_final=mrfbj.mapper_final,
                               reducer=mrfbj.reducer)])

    def test_show_steps(self):
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
        assert_equal(mr_two_step_job.stdout.getvalue(), 'MCR M\n')

        mr_no_mapper = MRNoMapper(['--steps'])
        mr_no_mapper.sandbox()
        mr_no_mapper.show_steps()
        assert_equal(mr_no_mapper.stdout.getvalue(), 'MR R\n')

    def test_mapper_and_reducer_as_positional_args(self):
        def mapper(k, v):
            pass

        def reducer(k, v):
            pass

        def combiner(k, v):
            pass

        assert_equal(MRJob.mr(mapper), MRJob.mr(mapper=mapper))

        assert_equal(MRJob.mr(mapper, reducer),
                     MRJob.mr(mapper=mapper, reducer=reducer))

        assert_equal(MRJob.mr(mapper, reducer=reducer),
                     MRJob.mr(mapper=mapper, reducer=reducer))

        assert_equal(MRJob.mr(mapper, reducer, combiner=combiner),
                     MRJob.mr(mapper=mapper, reducer=reducer,
                              combiner=combiner))

        # can't specify something as a positional and keyword arg
        assert_raises(TypeError,
                      MRJob.mr, mapper, mapper=mapper)
        assert_raises(TypeError,
                      MRJob.mr, mapper, reducer, reducer=reducer)

    def test_deprecated_mapper_final_positional_arg(self):
        def mapper(k, v):
            pass

        def reducer(k, v):
            pass

        def mapper_final():
            pass

        stderr = StringIO()
        with no_handlers_for_logger():
            log_to_stream('mrjob.job', stderr)
            step = MRJob.mr(mapper, reducer, mapper_final)

        # should be allowed to specify mapper_final as a positional arg,
        # but we log a warning
        assert_equal(step, MRJob.mr(mapper=mapper,
                                    reducer=reducer,
                                    mapper_final=mapper_final))
        assert_in('mapper_final should be specified', stderr.getvalue())

        # can't specify mapper_final as a positional and keyword arg
        assert_raises(
            TypeError,
            MRJob.mr, mapper, reducer, mapper_final, mapper_final=mapper_final)


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
            assert_equal(mr_job.parse_output(),
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
            assert_equal(mr_job.parse_output(),
                         [(1, 'bar'), (1, 'foo'), (2, None)])

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
        assert_equal(mr_job.generate_passthrough_arguments(), [])

    def test_explicit_passthrough_options(self):
        mr_job = MRCustomBoringJob(args=[
            '-v',
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
        assert_equal(mr_job.options.protocol, 'json')
        assert_equal(mr_job.options.output_protocol, 'json')
        assert_equal(mr_job.options.foo_size, 9)
        assert_equal(mr_job.options.bar_name, 'Alembic')
        assert_equal(mr_job.options.baz_mode, True)
        assert_equal(mr_job.options.quuxing, False)
        assert_equal(mr_job.options.pill_type, 'red')
        assert_equal(mr_job.options.planck_constant, 42)
        assert_equal(mr_job.options.extra_special_args, ['you', 'me'])
        assert_equal(mr_job.options.strict_protocols, True)
        assert_equal(mr_job.generate_passthrough_arguments(),
                     [
                      '--bar-name', 'Alembic',
                      '--enable-baz-mode',
                      '--extra-special-arg', 'you',
                      '--extra-special-arg', 'me',
                      '--foo-size', '9',
                      '--pill-type', 'red',
                      '--planck-constant', '1',
                      '--planck-constant', '42',
                      '--disable-quuxing',
                      '--strict-protocols',
                      ])

    def test_explicit_passthrough_options_short(self):
        mr_job = MRCustomBoringJob(args=[
            '-v',
            '-F9', '-BAlembic', '-MQ', '-T', 'red', '-C1', '-C42',
            '--extra-special-arg', 'you',
            '--extra-special-arg', 'me',
            '--strict-protocols',
            ])

        assert_equal(mr_job.options.input_protocol, 'raw_value')
        assert_equal(mr_job.options.protocol, 'json')
        assert_equal(mr_job.options.output_protocol, 'json')
        assert_equal(mr_job.options.foo_size, 9)
        assert_equal(mr_job.options.bar_name, 'Alembic')
        assert_equal(mr_job.options.baz_mode, True)
        assert_equal(mr_job.options.quuxing, False)
        assert_equal(mr_job.options.pill_type, 'red')
        assert_equal(mr_job.options.planck_constant, 42)
        assert_equal(mr_job.options.extra_special_args, ['you', 'me'])
        assert_equal(mr_job.options.strict_protocols, True)
        assert_equal(mr_job.generate_passthrough_arguments(),
                     [
                        '-B', 'Alembic',
                        '-M',
                         '--extra-special-arg', 'you',
                         '--extra-special-arg', 'me',
                         '-F', '9',
                         '-T', 'red',
                         '-C', '1',
                         '-C', '42',
                         '-Q',
                         '--strict-protocols'
                     ])

    def test_bad_custom_options(self):
        assert_raises(ValueError,
                      MRCustomBoringJob, ['--planck-constant', 'c'])
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

    def test_incorrect_option_types(self):
        assert_raises(ValueError, MRJob, ['--cmdenv', 'cats'])
        assert_raises(ValueError, MRJob, ['--ssh-bind-ports', 'athens'])

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

        mr_job = MRTowerOfPowers(
            ['--no-conf', '-v', '--cleanup=NONE', '--n-file', n_file_path])
        assert_equal(len(mr_job.steps()), 3)

        mr_job.sandbox(stdin=stdin)

        with logger_disabled('mrjob.local'):
            with mr_job.make_runner() as runner:
                assert isinstance(runner, LocalMRJobRunner)
                # make sure our file gets "uploaded"
                assert [fd for fd in runner._files
                        if fd['path'] == n_file_path]

                runner.run()
                output = set()
                for line in runner.stream_output():
                    _, value = mr_job.parse_output_line(line)
                    output.add(value)

        assert_equal(set(output), set([0, 1, ((2 ** 3) ** 3) ** 3]))


class ParseOutputTestCase(TestCase):
    # test parse_output() method

    def test_default(self):
        # test parsing JSON
        mr_job = MRJob()
        output = '0\t1\n"a"\t"b"\n'
        mr_job.stdout = StringIO(output)
        assert_equal(mr_job.parse_output(), [(0, 1), ('a', 'b')])

        # verify that stdout is not cleared
        assert_equal(mr_job.stdout.getvalue(), output)

    def test_protocol_instance(self):
        # see if we can use the repr protocol
        mr_job = MRJob()
        output = "0\t1\n['a', 'b']\tset(['c', 'd'])\n"
        mr_job.stdout = StringIO(output)
        assert_equal(mr_job.parse_output(ReprProtocol()),
                     [(0, 1), (['a', 'b'], set(['c', 'd']))])

        # verify that stdout is not cleared
        assert_equal(mr_job.stdout.getvalue(), output)

    def test_deprecated_protocol_aliases(self):
        # see if we can use the repr protocol
        mr_job = MRJob()
        output = "0\t1\n['a', 'b']\tset(['c', 'd'])\n"
        mr_job.stdout = StringIO(output)
        assert_equal(mr_job.parse_output('repr'),
                     [(0, 1), (['a', 'b'], set(['c', 'd']))])

        # verify that stdout is not cleared
        assert_equal(mr_job.stdout.getvalue(), output)


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
        assert_equal(sorted(StringIO(stdout)), ['1\t"foo"\n',
                                                '2\t"bar"\n',
                                                '3\tnull\n'])
        assert_equal(stderr, '')
        assert_equal(returncode, 0)

    def test_verbose(self):
        stdout, stderr, returncode = self.run_job()
        assert_equal(sorted(StringIO(stdout)), ['1\t"foo"\n',
                                                '2\t"bar"\n',
                                                '3\tnull\n'])
        assert_not_equal(stderr, '')
        assert_equal(returncode, 0)
        normal_stderr = stderr

        stdout, stderr, returncode = self.run_job(['-v'])
        assert_equal(sorted(StringIO(stdout)), ['1\t"foo"\n',
                                                '2\t"bar"\n',
                                                '3\tnull\n'])
        assert_not_equal(stderr, '')
        assert_equal(returncode, 0)
        assert_gt(len(stderr), len(normal_stderr))

    def test_no_output(self):
        assert_equal(os.listdir(self.tmp_dir), [])  # sanity check

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


class BadMainTestCase(TestCase):
    """Ensure that the user cannot do anything but just call MRYourJob.run()
    from __main__()"""

    def test_bad_main_catch(self):
        sys.argv.append('--mapper')
        assert_raises(UsageError, MRBoringJob().make_runner)
        sys.argv = sys.argv[:-1]
