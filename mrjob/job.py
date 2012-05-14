# Copyright 2009-2012 Yelp and Contributors
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
"""To create your own map reduce job, subclass :py:class:`MRJob`, create a
series of mappers and reducers, and override
:py:meth:`~mrjob.job.MRJob.steps`.

For example, a word counter::

    from mrjob.job import MRJob

    class MRWordCounter(MRJob):
        def get_words(self, key, line):
            for word in line.split():
                yield word, 1

        def sum_words(self, word, occurrences):
            yield word, sum(occurrences)

        def steps(self):
            return [self.mr(self.get_words, self.sum_words),]

    if __name__ == '__main__':
        MRWordCounter.run()

The two lines at the bottom are mandatory; this is what allows your class
to be run by Hadoop streaming.

This will take in a file with lines of whitespace separated words, and
output a file with tab-separated lines like: ``"stars"\t5``.

For one-step jobs, you can also just redefine
:py:meth:`~mrjob.job.MRJob.mapper` and :py:meth:`~mrjob.job.MRJob.reducer`::

    from mrjob.job import MRJob

    class MRWordCounter(MRJob):
        def mapper(self, key, line):
            for word in line.split():
                yield word, 1

        def reducer(self, word, occurrences):
            yield word, sum(occurrences)

    if __name__ == '__main__':
        MRWordCounter.run()

To test the job locally, just run:

``python your_mr_job_sub_class.py < log_file_or_whatever > output``

The script will automatically invoke itself to run the various steps,
using :py:class:`~mrjob.local.LocalMRJobRunner`.

You can also run individual steps::

    # test 1st step mapper:
    python your_mr_job_sub_class.py --mapper
    # test 2nd step reducer (--step-num=1 because step numbers are 0-indexed):
    python your_mr_job_sub_class.py --reducer --step-num=1

By default, we read from stdin, but you can also specify one or more
input files. It automatically decompresses .gz and .bz2 files::

    python your_mr_job_sub_class.py log_01.gz log_02.bz2 log_03

You can run on Amazon Elastic MapReduce by specifying ``-r emr`` or
on your own Hadoop cluster by specifying ``-r hadoop``:

``python your_mr_job_sub_class.py -r emr``

Use :py:meth:`~mrjob.job.MRJob.make_runner` to run an
:py:class:`~mrjob.job.MRJob` from another script::

    from __future__ import with_statement  # only needed on Python 2.5

    mr_job = MRWordCounter(args=['-r', 'emr'])
    with mr_job.make_runner() as runner:
        runner.run()
        for line in runner.stream_output():
            key, value = mr_job.parse_output_line(line)
            ...  # do something with the parsed output

See :py:mod:`mrjob.examples` for more examples.
"""
# don't add imports here that aren't part of the standard Python library,
# since MRJobs need to run in Amazon's generic EMR environment
from __future__ import with_statement

import inspect
import itertools
import logging
from optparse import Option
from optparse import OptionParser
from optparse import OptionGroup
from optparse import OptionError
import sys
import time

try:
    from cStringIO import StringIO
    StringIO  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    from StringIO import StringIO

# don't use relative imports, to allow this script to be invoked as __main__
from mrjob.conf import combine_dicts
from mrjob.parse import parse_port_range_list
from mrjob.parse import parse_mr_job_stderr
from mrjob.parse import parse_key_value_list
from mrjob.protocol import DEFAULT_PROTOCOL
from mrjob.protocol import JSONProtocol
from mrjob.protocol import PROTOCOL_DICT
from mrjob.protocol import RawValueProtocol
from mrjob.runner import CLEANUP_CHOICES
from mrjob.util import log_to_null
from mrjob.util import log_to_stream
from mrjob.util import parse_and_save_options
from mrjob.util import read_input


log = logging.getLogger('mrjob.job')


# all the parameters you can specify when definining a job step
_JOB_STEP_PARAMS = (
    'combiner',
    'combiner_init',
    'combiner_final',
    'mapper',
    'mapper_init',
    'mapper_final',
    'reducer',
    'reducer_init',
    'reducer_final',
)


# used by mr() below, to fake no mapper
def _IDENTITY_MAPPER(key, value):
    yield key, value


# sentinel value; used when running MRJob as a script
_READ_ARGS_FROM_SYS_ARGV = '_READ_ARGS_FROM_SYS_ARGV'


# The former custom option class has been removed and this stub will disappear
# permanently in mrjob 0.4.
MRJobOptions = Option


class UsageError(Exception):
    pass


class MRJob(object):
    """The base class for all MapReduce jobs. See :py:meth:`__init__`
    for details."""

    #: :py:class:`optparse.Option` subclass to use with the
    #: :py:class:`optparse.OptionParser` instance.
    OPTION_CLASS = Option

    def __init__(self, args=None):
        """Entry point for running your job from other Python code.

        You can pass in command-line arguments, and the job will act the same
        way it would if it were run from the command line. For example, to
        run your job on EMR::

            mr_job = MRYourJob(args=['-r', 'emr'])
            with mr_job.make_runner() as runner:
                ...

        Passing in ``None`` is the same as passing in ``[]`` (if you want
        to parse args from ``sys.argv``, call :py:meth:`MRJob.run`).

        For a full list of command-line arguments, run:
        ``python -m mrjob.job --help``
        """
        # make sure we respect the $TZ (time zone) environment variable
        if hasattr(time, 'tzset'):
            time.tzset()

        self._passthrough_options = []
        self._file_options = []

        usage = "usage: %prog [options] [input files]"
        self.option_parser = OptionParser(usage=usage,
                                          option_class=self.OPTION_CLASS)
        self.configure_options()

        # don't pass None to parse_args unless we're actually running
        # the MRJob script
        if args is _READ_ARGS_FROM_SYS_ARGV:
            self._cl_args = sys.argv[1:]
        else:
            # don't pass sys.argv to self.option_parser, and have it
            # raise an exception on error rather than printing to stderr
            # and exiting.
            self._cl_args = args or []

            def error(msg):
                raise ValueError(msg)

            self.option_parser.error = error

        self.load_options(self._cl_args)

        # Make it possible to redirect stdin, stdout, and stderr, for testing
        # See sandbox(), below.
        self.stdin = sys.stdin
        self.stdout = sys.stdout
        self.stderr = sys.stderr

    ### Defining one-step jobs ###

    def mapper(self, key, value):
        """Re-define this to define the mapper for a one-step job.

        Yields zero or more tuples of ``(out_key, out_value)``.

        :param key: A value parsed from input.
        :param value: A value parsed from input.

        If you don't re-define this, your job will have a mapper that simply
        yields ``(key, value)`` as-is.

        By default (if you don't mess with :ref:`job-protocols`):
         - ``key`` will be ``None``
         - ``value`` will be the raw input line, with newline stripped.
         - ``out_key`` and ``out_value`` must be JSON-encodable: numeric,
           unicode, boolean, ``None``, list, or dict whose keys are unicodes.
        """
        raise NotImplementedError

    def reducer(self, key, values):
        """Re-define this to define the reducer for a one-step job.

        Yields one or more tuples of ``(out_key, out_value)``

        :param key: A key which was yielded by the mapper
        :param value: A generator which yields all values yielded by the
                      mapper which correspond to ``key``.

        By default (if you don't mess with :ref:`job-protocols`):
         - ``out_key`` and ``out_value`` must be JSON-encodable.
         - ``key`` and ``value`` will have been decoded from JSON (so tuples
           will become lists).
        """
        raise NotImplementedError

    def combiner(self, key, values):
        """Re-define this to define the combiner for a one-step job.

        Yields one or more tuples of ``(out_key, out_value)``

        :param key: A key which was yielded by the mapper
        :param value: A generator which yields all values yielded by one mapper
                      task/node which correspond to ``key``.

        By default (if you don't mess with :ref:`job-protocols`):
         - ``out_key`` and ``out_value`` must be JSON-encodable.
         - ``key`` and ``value`` will have been decoded from JSON (so tuples
           will become lists).
        """
        raise NotImplementedError

    def mapper_init(self):
        """Re-define this to define an action to run before the mapper
        processes any input.

        One use for this function is to initialize mapper-specific helper
        structures.

        Yields one or more tuples of ``(out_key, out_value)``.

        By default, ``out_key`` and ``out_value`` must be JSON-encodable;
        re-define :py:attr:`INTERNAL_PROTOCOL` to change this.
        """
        raise NotImplementedError

    def mapper_final(self):
        """Re-define this to define an action to run after the mapper reaches
        the end of input.

        One way to use this is to store a total in an instance variable, and
        output it after reading all input data. See :py:mod:`mrjob.examples`
        for an example.

        Yields one or more tuples of ``(out_key, out_value)``.

        By default, ``out_key`` and ``out_value`` must be JSON-encodable;
        re-define :py:attr:`INTERNAL_PROTOCOL` to change this.
        """
        raise NotImplementedError

    def reducer_init(self):
        """Re-define this to define an action to run before the reducer
        processes any input.

        One use for this function is to initialize reducer-specific helper
        structures.

        Yields one or more tuples of ``(out_key, out_value)``.

        By default, ``out_key`` and ``out_value`` must be JSON-encodable;
        re-define :py:attr:`INTERNAL_PROTOCOL` to change this.
        """
        raise NotImplementedError

    def reducer_final(self):
        """Re-define this to define an action to run after the reducer reaches
        the end of input.

        Yields one or more tuples of ``(out_key, out_value)``.

        By default, ``out_key`` and ``out_value`` must be JSON-encodable;
        re-define :py:attr:`INTERNAL_PROTOCOL` to change this.
        """
        raise NotImplementedError

    def combiner_init(self):
        """Re-define this to define an action to run before the combiner
        processes any input.

        One use for this function is to initialize combiner-specific helper
        structures.

        Yields one or more tuples of ``(out_key, out_value)``.

        By default, ``out_key`` and ``out_value`` must be JSON-encodable;
        re-define :py:attr:`INTERNAL_PROTOCOL` to change this.
        """
        raise NotImplementedError

    def combiner_final(self):
        """Re-define this to define an action to run after the combiner reaches
        the end of input.

        Yields one or more tuples of ``(out_key, out_value)``.

        By default, ``out_key`` and ``out_value`` must be JSON-encodable;
        re-define :py:attr:`INTERNAL_PROTOCOL` to change this.
        """
        raise NotImplementedError

    ### Defining multi-step jobs ###

    def steps(self):
        """Re-define this to make a multi-step job.

        If you don't re-define this, we'll automatically create a one-step
        job using any of :py:meth:`mapper`, :py:meth:`mapper_init`,
        :py:meth:`mapper_final`, :py:meth:`reducer_init`,
        :py:meth:`reducer_final`, and :py:meth:`reducer` that you've
        re-defined. For example::

            def steps(self):
                return [self.mr(mapper=self.transform_input,
                                reducer=self.consolidate_1),
                        self.mr(reducer_init=self.log_mapper_init,
                                reducer=self.consolidate_2)]

        :return: a list of steps constructed with :py:meth:`mr`
        """
        # Use mapper(), reducer() etc. only if they've been re-defined
        kwargs = dict((func_name, getattr(self, func_name))
                      for func_name in _JOB_STEP_PARAMS
                      if (getattr(self, func_name).im_func is not
                          getattr(MRJob, func_name).im_func))

        return [self.mr(**kwargs)]

    @classmethod
    def mr(cls, mapper=None, reducer=None, _mapper_final=None, **kwargs):
        """Define a step (mapper, reducer, and/or any combination of
        mapper_init, reducer_final, etc.) for your job.

        Used by :py:meth:`steps`. (Don't re-define this, just call it!)

        Accepts the following keyword arguments. For convenience, you may
        specify *mapper* and *reducer* as positional arguments as well.

        :param mapper: function with same function signature as
                       :py:meth:`mapper`, or ``None`` for an identity mapper.
        :param reducer: function with same function signature as
                        :py:meth:`reducer`, or ``None`` for no reducer.
        :param combiner: function with same function signature as
                         :py:meth:`combiner`, or ``None`` for no combiner.
        :param mapper_init: function with same function signature as
                            :py:meth:`mapper_init`, or ``None`` for no initial
                            mapper action.
        :param mapper_final: function with same function signature as
                             :py:meth:`mapper_final`, or ``None`` for no final
                             mapper action.
        :param reducer_init: function with same function signature as
                             :py:meth:`reducer_init`, or ``None`` for no
                             initial reducer action.
        :param reducer_final: function with same function signature as
                              :py:meth:`reducer_final`, or ``None`` for no
                              final reducer action.
        :param combiner_init: function with same function signature as
                              :py:meth:`combiner_init`, or ``None`` for no
                              initial combiner action.
        :param combiner_final: function with same function signature as
                               :py:meth:`combiner_final`, or ``None`` for no
                               final combiner action.

        Please consider the way we represent steps to be opaque, and expect
        it to change in future versions of ``mrjob``.
        """
        # limit which keyword args can be specified
        bad_kwargs = sorted(set(kwargs) - set(_JOB_STEP_PARAMS))
        if bad_kwargs:
            raise TypeError(
                'mr() got an unexpected keyword argument %r' % bad_kwargs[0])

        # handle incorrect usage of positional args. This was wrong in mrjob
        # v0.2 as well, but we didn't issue a warning.
        if _mapper_final is not None:
            if 'mapper_final' in kwargs:
                raise TypeError("mr() got multiple values for keyword argument"
                                " 'mapper_final'")
            else:
                log.warn(
                    'mapper_final should be specified as a keyword argument to'
                    ' mr(), not a positional argument. This will be required'
                    ' in mrjob 0.4.')
                kwargs['mapper_final'] = _mapper_final

        step = dict((f, None) for f in _JOB_STEP_PARAMS)
        step['mapper'] = mapper
        step['reducer'] = reducer
        step.update(kwargs)

        if not any(step.itervalues()):
            raise Exception("Step has no mappers and no reducers")

        # Hadoop streaming requires a mapper, so patch in _IDENTITY_MAPPER
        step['mapper'] = step['mapper'] or _IDENTITY_MAPPER

        return step

    def increment_counter(self, group, counter, amount=1):
        """Increment a counter in Hadoop streaming by printing to stderr.

        :type group: str
        :param group: counter group
        :type counter: str
        :param counter: description of the counter
        :type amount: int
        :param amount: how much to increment the counter by

        Commas in ``counter`` or ``group`` will be automatically replaced
        with semicolons (commas confuse Hadoop streaming).
        """
        # don't allow people to pass in floats
        if not isinstance(amount, (int, long)):
            raise TypeError('amount must be an integer, not %r' % (amount,))

        # Extra commas screw up hadoop and there's no way to escape them. So
        # replace them with the next best thing: semicolons!
        #
        # cast to str() because sometimes people pass in exceptions or whatever
        #
        # The relevant Hadoop code is incrCounter(), here:
        # http://svn.apache.org/viewvc/hadoop/mapreduce/trunk/src/contrib/streaming/src/java/org/apache/hadoop/streaming/PipeMapRed.java?view=markup
        group = str(group).replace(',', ';')
        counter = str(counter).replace(',', ';')

        self.stderr.write('reporter:counter:%s,%s,%d\n' %
                          (group, counter, amount))
        self.stderr.flush()

    def set_status(self, msg):
        """Set the job status in hadoop streaming by printing to stderr.

        This is also a good way of doing a keepalive for a job that goes a
        long time between outputs; Hadoop streaming usually times out jobs
        that give no output for longer than 10 minutes.
        """
        self.stderr.write('reporter:status:%s\n' % (msg,))
        self.stderr.flush()

    ### Running the job ###

    @classmethod
    def run(cls):
        """Entry point for running job from the command-line.

        This is also the entry point when a mapper or reducer is run
        by Hadoop Streaming.

        Does one of:

        * Print step information (:option:`--steps`). See :py:meth:`show_steps`
        * Run a mapper (:option:`--mapper`). See :py:meth:`run_mapper`
        * Run a combiner (:option:`--combiner`). See :py:meth:`run_combiner`
        * Run a reducer (:option:`--reducer`). See :py:meth:`run_reducer`
        * Run the entire job. See :py:meth:`run_job`
        """
        # load options from the command line
        mr_job = cls(args=_READ_ARGS_FROM_SYS_ARGV)
        mr_job.execute()

    def execute(self):
        if self.options.show_steps:
            self.show_steps()

        elif self.options.run_mapper:
            self.run_mapper(self.options.step_num)

        elif self.options.run_combiner:
            self.run_combiner(self.options.step_num)

        elif self.options.run_reducer:
            self.run_reducer(self.options.step_num)

        else:
            self.run_job()

    def make_runner(self):
        """Make a runner based on command-line arguments, so we can
        launch this job on EMR, on Hadoop, or locally.

        :rtype: :py:class:`mrjob.runner.MRJobRunner`
        """
        bad_words = (
            '--steps', '--mapper', '--reducer', '--combiner', '--step-num')
        for w in bad_words:
            if w in sys.argv:
                raise UsageError("make_runner() was called with %s. This"
                                 " probably means you tried to use it from"
                                 " __main__, which doesn't work." % w)

        # have to import here so that we can still run the MRJob
        # without importing boto
        from mrjob.emr import EMRJobRunner
        from mrjob.hadoop import HadoopJobRunner
        from mrjob.local import LocalMRJobRunner
        from mrjob.inline import InlineMRJobRunner

        if self.options.runner == 'emr':
            return EMRJobRunner(**self.emr_job_runner_kwargs())

        elif self.options.runner == 'hadoop':
            return HadoopJobRunner(**self.hadoop_job_runner_kwargs())

        elif self.options.runner == 'inline':
            return InlineMRJobRunner(
                mrjob_cls=self.__class__, **self.inline_job_runner_kwargs())

        else:
            # run locally by default
            return LocalMRJobRunner(**self.local_job_runner_kwargs())

    @classmethod
    def set_up_logging(cls, quiet=False, verbose=False, stream=None):
        """Set up logging when running from the command line. This is also
        used by the various command-line utilities.

        :param bool quiet: If true, don't log. Overrides *verbose*.
        :param bool verbose: If true, set log level to ``DEBUG`` (default is
                             ``INFO``)
        :param bool stream: Stream to log to (default is ``sys.stderr``)

        This will also set up a null log handler for boto, so we don't get
        warnings if boto tries to log about throttling and whatnot.
        """
        if quiet:
            log_to_null(name='mrjob')
        else:
            log_to_stream(name='mrjob', debug=verbose, stream=stream)

        log_to_null(name='boto')

    def run_job(self):
        """Run the all steps of the job, logging errors (and debugging output
        if :option:`--verbose` is specified) to STDERR and streaming the
        output to STDOUT.

        Called from :py:meth:`run`. You'd probably only want to call this
        directly from automated tests.
        """
        self.set_up_logging(quiet=self.options.quiet,
                            verbose=self.options.verbose,
                            stream=self.stderr)

        with self.make_runner() as runner:
            runner.run()

            if not self.options.no_output:
                for line in runner.stream_output():
                    self.stdout.write(line)
                self.stdout.flush()

    def run_mapper(self, step_num=0):
        """Run the mapper and final mapper action for the given step.

        :type step_num: int
        :param step_num: which step to run (0-indexed)

        If we encounter a line that can't be decoded by our input protocol,
        or a tuple that can't be encoded by our output protocol, we'll
        increment a counter rather than raising an exception. If
        --strict-protocols is set, then an exception is raised

        Called from :py:meth:`run`. You'd probably only want to call this
        directly from automated tests.
        """
        steps = self.steps()
        if not 0 <= step_num < len(steps):
            raise ValueError('Out-of-range step: %d' % step_num)
        step = steps[step_num]
        mapper = step['mapper']
        mapper_init = step['mapper_init']
        mapper_final = step['mapper_final']

        # pick input and output protocol
        read_lines, write_line = self._wrap_protocols(step_num, 'M')

        if mapper_init:
            for out_key, out_value in mapper_init() or ():
                write_line(out_key, out_value)

        # run the mapper on each line
        for key, value in read_lines():
            for out_key, out_value in mapper(key, value) or ():
                write_line(out_key, out_value)

        if mapper_final:
            for out_key, out_value in mapper_final() or ():
                write_line(out_key, out_value)

    def run_reducer(self, step_num=0):
        """Run the reducer for the given step.

        :type step_num: int
        :param step_num: which step to run (0-indexed)

        If we encounter a line that can't be decoded by our input protocol,
        or a tuple that can't be encoded by our output protocol, we'll
        increment a counter rather than raising an exception. If
        --strict-protocols is set, then an exception is raised

        Called from :py:meth:`run`. You'd probably only want to call this
        directly from automated tests.
        """
        steps = self.steps()
        if not 0 <= step_num < len(steps):
            raise ValueError('Out-of-range step: %d' % step_num)
        step = steps[step_num]
        reducer = step['reducer']
        reducer_init = step['reducer_init']
        reducer_final = step['reducer_final']
        if reducer is None:
            raise ValueError('No reducer in step %d' % step_num)

        # pick input and output protocol
        read_lines, write_line = self._wrap_protocols(step_num, 'R')

        if reducer_init:
            for out_key, out_value in reducer_init() or ():
                write_line(out_key, out_value)

        # group all values of the same key together, and pass to the reducer
        #
        # be careful to use generators for everything, to allow for
        # very large groupings of values
        for key, kv_pairs in itertools.groupby(read_lines(),
                                               key=lambda(k, v): k):
            values = (v for k, v in kv_pairs)
            for out_key, out_value in reducer(key, values) or ():
                write_line(out_key, out_value)

        if reducer_final:
            for out_key, out_value in reducer_final() or ():
                write_line(out_key, out_value)

    def run_combiner(self, step_num=0):
        """Run the combiner for the given step.

        :type step_num: int
        :param step_num: which step to run (0-indexed)

        If we encounter a line that can't be decoded by our input protocol,
        or a tuple that can't be encoded by our output protocol, we'll
        increment a counter rather than raising an exception. If
        --strict-protocols is set, then an exception is raised

        Called from :py:meth:`run`. You'd probably only want to call this
        directly from automated tests.
        """
        steps = self.steps()
        if not 0 <= step_num < len(steps):
            raise ValueError('Out-of-range step: %d' % step_num)
        step = steps[step_num]
        combiner = step['combiner']
        combiner_init = step['combiner_init']
        combiner_final = step['combiner_final']
        if combiner is None:
            raise ValueError('No combiner in step %d' % step_num)

        # pick input and output protocol
        read_lines, write_line = self._wrap_protocols(step_num, 'C')

        if combiner_init:
            for out_key, out_value in combiner_init() or ():
                write_line(out_key, out_value)

        # group all values of the same key together, and pass to the combiner
        #
        # be careful to use generators for everything, to allow for
        # very large groupings of values
        for key, kv_pairs in itertools.groupby(read_lines(),
                                               key=lambda(k, v): k):
            values = (v for k, v in kv_pairs)
            for out_key, out_value in combiner(key, values) or ():
                write_line(out_key, out_value)

        if combiner_final:
            for out_key, out_value in combiner_final() or ():
                write_line(out_key, out_value)

    def show_steps(self):
        """Print information about how many steps there are, and whether
        they contain a mapper or reducer. Job runners (see :doc:`runners`)
        use this to determine how Hadoop should call this script.

        Called from :py:meth:`run`. You'd probably only want to call this
        directly from automated tests.

        We currently output something like ``MR M R``, but expect this to
        change!
        """
        print >> self.stdout, ' '.join(self._steps_desc())

    def _steps_desc(self):
        res = []
        for step_num, step in enumerate(self.steps()):
            mapper_funcs = ('mapper_init', 'mapper_final')
            reducer_funcs = ('reducer', 'reducer_init', 'reducer_final')
            combiner_funcs = ('combiner', 'combiner_init', 'combiner_final')

            has_explicit_mapper = (step['mapper'] != _IDENTITY_MAPPER or
                                   any(step[k] for k in mapper_funcs))
            has_explicit_reducer = any(step[k] for k in reducer_funcs)
            has_explicit_combiner = any(step[k] for k in combiner_funcs)

            func_strs = []

            # Print a mapper if:
            # - The user specifies one
            # - Different input and output protocols are used (infer from
            #   step number)
            # - We don't have anything else to print (excluding combiners)
            if has_explicit_mapper \
               or step_num == 0 \
               or not has_explicit_reducer:
                func_strs.append('M')

            if has_explicit_combiner:
                func_strs.append('C')

            if has_explicit_reducer:
                func_strs.append('R')

            res.append(''.join(func_strs))
        return res

    @classmethod
    def mr_job_script(cls):
        """Path of this script. This returns the file containing
        this class."""
        return inspect.getsourcefile(cls)

    ### Other useful utilities ###

    def _read_input(self):
        """Read from stdin, or one more files, or directories.
        Yield one line at time.

        - Resolve globs (``foo_*.gz``).
        - Decompress ``.gz`` and ``.bz2`` files.
        - If path is ``-``, read from STDIN.
        - Recursively read all files in a directory
        """
        paths = self.args or ['-']
        for path in paths:
            for line in read_input(path, stdin=self.stdin):
                yield line

    def _wrap_protocols(self, step_num, step_type):
        """Pick the protocol classes to use for reading and writing
        for the given step, and wrap them so that bad input and output
        trigger a counter rather than an exception unless --strict-protocols
        is set.

        Returns a tuple of read_lines, write_line
        read_lines() is a function that reads lines from input, decodes them,
            and yields key, value pairs
        write_line() is a function that takes key and value as args, encodes
            them, and writes a line to output.

        Args:
        step_num -- which step to run (e.g. 0)
        step_type -- 'M' for mapper, 'C' for combiner, 'R' for reducer
        """
        read, write = self.pick_protocols(step_num, step_type)

        def read_lines():
            for line in self._read_input():
                try:
                    key, value = read(line.rstrip('\r\n'))
                    yield key, value
                except Exception, e:
                    if self.options.strict_protocols:
                        raise
                    else:
                        self.increment_counter('Undecodable input',
                                                e.__class__.__name__)

        def write_line(key, value):
            try:
                print >> self.stdout, write(key, value)
            except Exception, e:
                if self.options.strict_protocols:
                    raise
                else:
                    self.increment_counter('Unencodable output',
                                            e.__class__.__name__)

        return read_lines, write_line

    def pick_protocols(self, step_num, step_type):
        """Pick the protocol classes to use for reading and writing
        for the given step.

        :type step_num: int
        :param step_num: which step to run (e.g. ``0`` for the first step)
        :type step_type: str
        :param step_type: ``'M'`` for mapper, ``'C'`` for combiner, ``'R'``
                          for reducer
        :return: (read_function, write_function)

        By default, we use one protocol for reading input, one
        internal protocol for communication between steps, and one
        protocol for final output (which is usually the same as the
        internal protocol). Protocols can be controlled by setting
        :py:attr:`INPUT_PROTOCOL`, :py:attr:`INTERNAL_PROTOCOL`, and
        :py:attr:`OUTPUT_PROTOCOL`.

        Re-define this if you need fine control over which protocols
        are used by which steps.
        """
        steps_desc = self._steps_desc()

        # pick input protocol

        if step_num == 0 and step_type == steps_desc[0][0]:
            read = self.input_protocol().read
        else:
            read = self.internal_protocol().read

        if step_num == len(steps_desc) - 1 and step_type == steps_desc[-1][-1]:
            write = self.output_protocol().write
        else:
            write = self.internal_protocol().write

        return read, write

    ### Command-line arguments ###

    def configure_options(self):
        """Define arguments for this script. Called from :py:meth:`__init__()`.

        Run ``python -m mrjob.job.MRJob --help`` to see all options.

        Re-define to define custom command-line arguments::

            def configure_options(self):
                super(MRYourJob, self).configure_options

                self.add_passthrough_option(...)
                self.add_file_option(...)
                ...
        """
        # To describe the steps
        self.option_parser.add_option(
            '--steps', dest='show_steps', action='store_true', default=False,
            help='show the steps of mappers and reducers')

        # To run mappers or reducers
        self.mux_opt_group = OptionGroup(
            self.option_parser, 'Running specific parts of the job')
        self.option_parser.add_option_group(self.mux_opt_group)

        self.mux_opt_group.add_option(
            '--mapper', dest='run_mapper', action='store_true', default=False,
            help='run a mapper')

        self.mux_opt_group.add_option(
            '--combiner', dest='run_combiner', action='store_true',
            default=False, help='run a combiner')

        self.mux_opt_group.add_option(
            '--reducer', dest='run_reducer', action='store_true',
            default=False, help='run a reducer')

        self.mux_opt_group.add_option(
            '--step-num', dest='step_num', type='int', default=0,
            help='which step to execute (default is 0)')

        # protocol stuff
        protocol_choices = sorted(self.protocols())
        self.proto_opt_group = OptionGroup(
            self.option_parser, 'Protocols')
        self.option_parser.add_option_group(self.proto_opt_group)

        self.add_passthrough_option(
            '--input-protocol', dest='input_protocol',
            opt_group=self.proto_opt_group,
            default=None, choices=protocol_choices,
            help=('DEPRECATED: protocol to read input with (default:'
                  ' raw_value)'))

        self.add_passthrough_option(
            '--output-protocol', dest='output_protocol',
            opt_group=self.proto_opt_group,
            default=self.DEFAULT_OUTPUT_PROTOCOL,
            choices=protocol_choices,
             help='DEPRECATED: protocol for final output (default: %s)' % (
            'same as --protocol' if self.DEFAULT_OUTPUT_PROTOCOL is None
            else '%default'))

        self.add_passthrough_option(
            '-p', '--protocol', dest='protocol',
            opt_group=self.proto_opt_group,
            default=None, choices=protocol_choices,
            help=('DEPRECATED: output protocol for mappers/reducers. Choices:'
                  ' %s (default: json)' % ', '.join(protocol_choices)))

        self.add_passthrough_option(
            '--strict-protocols', dest='strict_protocols', default=None,
            opt_group=self.proto_opt_group,
            action='store_true', help='If something violates an input/output '
            'protocol then raise an exception')

        # options for running the entire job
        self.runner_opt_group = OptionGroup(
            self.option_parser, 'Running the entire job')
        self.option_parser.add_option_group(self.runner_opt_group)

        self.runner_opt_group.add_option(
            '--archive', dest='upload_archives', action='append',
            default=[],
            help=('Unpack archive in the working directory of this script. You'
                  ' can use --archive multiple times.'))

        self.runner_opt_group.add_option(
            '--bootstrap-mrjob', dest='bootstrap_mrjob', action='store_true',
            default=None,
            help=("Automatically tar up the mrjob library and install it when"
                  " we run the mrjob. This is the default. Use"
                  " --no-bootstrap-mrjob if you've already installed mrjob on"
                  " your Hadoop cluster."))

        self.runner_opt_group.add_option(
            '-c', '--conf-path', dest='conf_path', default=None,
            help='Path to alternate mrjob.conf file to read from')

        self.runner_opt_group.add_option(
            '--cleanup', dest='cleanup', default=None,
            help=('Comma-separated list of which directories to delete when'
                  ' a job succeeds, e.g. SCRATCH,LOGS. Choices:'
                  ' %s (default: ALL)' % ', '.join(CLEANUP_CHOICES)))

        self.runner_opt_group.add_option(
            '--cleanup-on-failure', dest='cleanup_on_failure', default=None,
            help=('Comma-separated list of which directories to delete when'
                  ' a job fails, e.g. SCRATCH,LOGS. Choices:'
                  ' %s (default: NONE)' % ', '.join(CLEANUP_CHOICES)))

        self.runner_opt_group.add_option(
            '--cmdenv', dest='cmdenv', default=[], action='append',
            help='set an environment variable for your job inside Hadoop '
            'streaming. Must take the form KEY=VALUE. You can use --cmdenv '
            'multiple times.')

        self.runner_opt_group.add_option(
            '--file', dest='upload_files', action='append',
            default=[],
            help=('Copy file to the working directory of this script. You can'
                  ' use --file multiple times.'))

        self.runner_opt_group.add_option(
            '--no-bootstrap-mrjob', dest='bootstrap_mrjob',
            action='store_false', default=None,
            help=("Don't automatically tar up the mrjob library and install it"
                  " when we run this job. Use this if you've already installed"
                  " mrjob on your Hadoop cluster."))

        self.runner_opt_group.add_option(
            '--no-conf', dest='conf_path', action='store_false', default=None,
            help="Don't load mrjob.conf even if it's available")

        self.runner_opt_group.add_option(
            '--no-output', dest='no_output',
            default=None, action='store_true',
            help="Don't stream output after job completion")

        self.runner_opt_group.add_option(
            '-o', '--output-dir', dest='output_dir', default=None,
            help='Where to put final job output. This must be an s3:// URL ' +
            'for EMR, an HDFS path for Hadoop, and a system path for local,' +
            'and must be empty')

        self.runner_opt_group.add_option(
            '--partitioner', dest='partitioner', default=None,
            help=('Hadoop partitioner class to use to determine how mapper'
                  ' output should be sorted and distributed to reducers. For'
                  ' example: org.apache.hadoop.mapred.lib.HashPartitioner'))

        self.runner_opt_group.add_option(
            '--python-archive', dest='python_archives', default=[],
            action='append',
            help=('Archive to unpack and add to the PYTHONPATH of the mr_job'
                  ' script when it runs. You can use --python-archives'
                  ' multiple times.'))

        self.runner_opt_group.add_option(
            '--python-bin', dest='python_bin', default=None,
            help=("Name/path of alternate python binary for mappers/reducers."
                  " You can include arguments, e.g. --python-bin 'python -v'"))

        self.runner_opt_group.add_option(
            '-q', '--quiet', dest='quiet', default=None,
            action='store_true',
            help="Don't print anything to stderr")

        self.runner_opt_group.add_option(
            '-r', '--runner', dest='runner', default='local',
            choices=('local', 'hadoop', 'emr', 'inline'),
            help=('Where to run the job: local to run locally, hadoop to run'
                  ' on your Hadoop cluster, emr to run on Amazon'
                  ' ElasticMapReduce, and inline for local debugging. Default'
                  ' is local.'))

        self.runner_opt_group.add_option(
            '--setup-cmd', dest='setup_cmds', action='append',
            default=[],
            help=('A command to run before each mapper/reducer step in the'
                  ' shell (e.g. "cd my-src-tree; make") specified as a string.'
                  ' You can use --setup-cmd more than once. Use mrjob.conf to'
                  ' specify arguments as a list to be run directly.'))

        self.runner_opt_group.add_option(
            '--setup-script', dest='setup_scripts', action='append',
            default=[],
            help=('Path to file to be copied into the local working directory'
                  ' and then run. You can use --setup-script more than once.'
                  ' These are run after setup_cmds.'))

        self.runner_opt_group.add_option(
            '--steps-python-bin', dest='steps_python_bin', default=None,
            help='Name/path of alternate python binary to use to query the '
            'job about its steps, if different from the current Python '
            'interpreter. Rarely needed.')

        self.runner_opt_group.add_option(
            '-v', '--verbose', dest='verbose', default=None,
            action='store_true',
            help='print more messages to stderr')

        self.hadoop_opts_opt_group = OptionGroup(
            self.option_parser,
            'Configuring or emulating Hadoop (these apply when you set -r'
            ' hadoop, -r emr, or -r local)')
        self.option_parser.add_option_group(self.hadoop_opts_opt_group)

        self.hadoop_opts_opt_group.add_option(
            '--hadoop-version', dest='hadoop_version', default=None,
            help=('Version of Hadoop to specify to EMR or to emulate for -r'
                  ' local. Default is 0.20.'))

        # for more info about jobconf:
        # http://hadoop.apache.org/mapreduce/docs/current/mapred-default.html
        self.hadoop_opts_opt_group.add_option(
            '--jobconf', dest='jobconf', default=[], action='append',
            help=('-jobconf arg to pass through to hadoop streaming; should'
                  ' take the form KEY=VALUE. You can use --jobconf multiple'
                  ' times.'))

        # options common to Hadoop and EMR
        self.hadoop_emr_opt_group = OptionGroup(
            self.option_parser,
            'Running on Hadoop or EMR (these apply when you set -r hadoop or'
            ' -r emr)')
        self.option_parser.add_option_group(self.hadoop_emr_opt_group)

        self.hadoop_emr_opt_group.add_option(
            '--hadoop-arg', dest='hadoop_extra_args', default=[],
            action='append', help='Argument of any type to pass to hadoop '
            'streaming. You can use --hadoop-arg multiple times.')

        self.hadoop_emr_opt_group.add_option(
            '--hadoop-input-format', dest='hadoop_input_format', default=None,
            help=('DEPRECATED: the hadoop InputFormat class used by the first'
                  ' step of your job to read data. Custom formats must be'
                  ' included in your hadoop streaming jar (see'
                  ' --hadoop-streaming-jar). Current best practice is to'
                  ' redefine HADOOP_INPUT_FORMAT or hadoop_input_format()'
                  ' in your job.'))

        self.hadoop_emr_opt_group.add_option(
            '--hadoop-output-format', dest='hadoop_output_format',
            default=None,
            help=('DEPRECATED: the hadoop OutputFormat class used by the first'
                  ' step of your job to read data. Custom formats must be'
                  ' included in your hadoop streaming jar (see'
                  ' --hadoop-streaming-jar). Current best practice is to'
                  ' redefine HADOOP_OUTPUT_FORMAT or hadoop_output_format()'
                  ' in your job.'))

        self.hadoop_emr_opt_group.add_option(
            '--hadoop-streaming-jar', dest='hadoop_streaming_jar',
            default=None,
            help='Path of your hadoop streaming jar (locally, or on S3/HDFS)')

        self.hadoop_emr_opt_group.add_option(
            '--label', dest='label', default=None,
            help='custom prefix for job name, to help us identify the job')

        self.hadoop_emr_opt_group.add_option(
            '--owner', dest='owner', default=None,
            help='custom username to use, to help us identify who ran the job')

        # options for running the job on Hadoop
        self.hadoop_opt_group = OptionGroup(
            self.option_parser,
            'Running on Hadoop (these apply when you set -r hadoop)')
        self.option_parser.add_option_group(self.hadoop_opt_group)

        self.hadoop_opt_group.add_option(
            '--hadoop-bin', dest='hadoop_bin', default=None,
            help='hadoop binary. Defaults to $HADOOP_HOME/bin/hadoop')

        self.hadoop_opt_group.add_option(
            '--hdfs-scratch-dir', dest='hdfs_scratch_dir',
            default=None,
            help='Scratch space on HDFS (default is tmp/)')

        # options for running the job on EMR
        self.emr_opt_group = OptionGroup(
            self.option_parser,
            'Running on Amazon Elastic MapReduce (these apply when you set -r'
            ' emr)')
        self.option_parser.add_option_group(self.emr_opt_group)

        self.emr_opt_group.add_option(
            '--additional-emr-info', dest='additional_emr_info', default=None,
            help='A JSON string for selecting additional features on EMR')

        self.emr_opt_group.add_option(
            '--ami-version', dest='ami_version', default=None,
            help=(
                'AMI Version to use (currently 1.0, 2.0, or latest).'))

        self.emr_opt_group.add_option(
            '--aws-availability-zone', dest='aws_availability_zone',
            default=None,
            help='Availability zone to run the job flow on')

        self.emr_opt_group.add_option(
            '--aws-region', dest='aws_region', default=None,
            help='Region to connect to S3 and EMR on (e.g. us-west-1).')

        self.emr_opt_group.add_option(
            '--bootstrap-action', dest='bootstrap_actions', action='append',
            default=[],
            help=('Raw bootstrap action scripts to run before any of the other'
                  ' bootstrap steps. You can use --bootstrap-action more than'
                  ' once. Local scripts will be automatically uploaded to S3.'
                  ' To add arguments, just use quotes: "foo.sh arg1 arg2"'))

        self.emr_opt_group.add_option(
            '--bootstrap-cmd', dest='bootstrap_cmds', action='append',
            default=[],
            help=('Commands to run on the master node to set up libraries,'
                  ' etc. You can use --bootstrap-cmd more than once. Use'
                  ' mrjob.conf to specify arguments as a list to be run'
                  ' directly.'))

        self.emr_opt_group.add_option(
            '--bootstrap-file', dest='bootstrap_files', action='append',
            default=[],
            help=('File to upload to the master node before running'
                  ' bootstrap_cmds (for example, debian packages). These will'
                  ' be made public on S3 due to a limitation of the bootstrap'
                  ' feature. You can use --bootstrap-file more than once.'))

        self.emr_opt_group.add_option(
            '--bootstrap-python-package', dest='bootstrap_python_packages',
            action='append', default=[],
            help=('Path to a Python module to install on EMR. These should be'
                  ' standard python module tarballs where you can cd into a'
                  ' subdirectory and run ``sudo python setup.py install``. You'
                  ' can use --bootstrap-python-package more than once.'))

        self.emr_opt_group.add_option(
            '--bootstrap-script', dest='bootstrap_scripts', action='append',
            default=[],
            help=('Script to upload and then run on the master node (a'
                  ' combination of bootstrap_cmds and bootstrap_files). These'
                  ' are run after the command from bootstrap_cmds. You can use'
                  ' --bootstrap-script more than once.'))

        self.emr_opt_group.add_option(
            '--check-emr-status-every', dest='check_emr_status_every',
            default=None, type='int',
            help='How often (in seconds) to check status of your EMR job')

        self.emr_opt_group.add_option(
            '--ec2-instance-type', dest='ec2_instance_type', default=None,
            help=('Type of EC2 instance(s) to launch (e.g. m1.small,'
                  ' c1.xlarge, m2.xlarge). See'
                  ' http://aws.amazon.com/ec2/instance-types/ for the full'
                  ' list.'))

        self.emr_opt_group.add_option(
            '--ec2-key-pair', dest='ec2_key_pair', default=None,
            help='Name of the SSH key pair you set up for EMR')

        self.emr_opt_group.add_option(
            '--ec2-key-pair-file', dest='ec2_key_pair_file', default=None,
            help='Path to file containing SSH key for EMR')

        # EMR instance types
        self.emr_opt_group.add_option(
            '--ec2-core-instance-type', '--ec2-slave-instance-type',
            dest='ec2_core_instance_type', default=None,
            help='Type of EC2 instance for core (or "slave") nodes only')

        self.emr_opt_group.add_option(
            '--ec2-master-instance-type', dest='ec2_master_instance_type',
            default=None,
            help='Type of EC2 instance for master node only')

        self.emr_opt_group.add_option(
            '--ec2-task-instance-type', dest='ec2_task_instance_type',
            default=None,
            help='Type of EC2 instance for task nodes only')

        # EMR instance bid prices
        self.emr_opt_group.add_option(
            '--ec2-core-instance-bid-price',
            dest='ec2_core_instance_bid_price', default=None,
            help=(
                'Bid price to specify for core (or "slave") nodes when'
                ' setting them up as EC2 spot instances (you probably only'
                ' want to set a bid price for task instances).')
            )

        self.emr_opt_group.add_option(
            '--ec2-master-instance-bid-price',
            dest='ec2_master_instance_bid_price', default=None,
            help=(
                'Bid price to specify for the master node when setting it up '
                'as an EC2 spot instance (you probably only want to set '
                'a bid price for task instances).')
            )

        self.emr_opt_group.add_option(
            '--ec2-task-instance-bid-price',
            dest='ec2_task_instance_bid_price', default=None,
            help=(
                'Bid price to specify for task nodes when '
                'setting them up as EC2 spot instances.')
            )

        self.emr_opt_group.add_option(
            '--emr-endpoint', dest='emr_endpoint', default=None,
            help=('Optional host to connect to when communicating with S3'
                  ' (e.g. us-west-1.elasticmapreduce.amazonaws.com). Default'
                  ' is to infer this from aws_region.'))

        self.emr_opt_group.add_option(
            '--emr-job-flow-id', dest='emr_job_flow_id', default=None,
            help='ID of an existing EMR job flow to use')

        self.emr_opt_group.add_option(
            '--enable-emr-debugging', dest='enable_emr_debugging',
            default=None, action='store_true',
            help='Enable storage of Hadoop logs in SimpleDB')

        self.emr_opt_group.add_option(
            '--disable-emr-debugging', dest='enable_emr_debugging',
            action='store_false',
            help='Enable storage of Hadoop logs in SimpleDB')

        self.emr_opt_group.add_option(
            '--hadoop-streaming-jar-on-emr',
            dest='hadoop_streaming_jar_on_emr', default=None,
            help=('Local path of the hadoop streaming jar on the EMR node.'
                  ' Rarely necessary.'))

        self.emr_opt_group.add_option(
            '--no-pool-emr-job-flows', dest='pool_emr_job_flows',
            action='store_false',
            help="Don't try to run our job on a pooled job flow.")

        self.emr_opt_group.add_option(
            '--num-ec2-instances', dest='num_ec2_instances', default=None,
            type='int',
            help='Total number of EC2 instances to launch ')

        # NB: EMR instance counts are only applicable for slave/core and
        # task, since a master count > 1 causes the EMR API to return the
        # ValidationError "A master instance group must specify a single
        # instance".
        self.emr_opt_group.add_option(
            '--num-ec2-core-instances', dest='num_ec2_core_instances',
            default=None, type='int',
            help=('Number of EC2 instances to start as core (or "slave") '
                  'nodes. Incompatible with --num-ec2-instances.'))

        self.emr_opt_group.add_option(
            '--num-ec2-task-instances', dest='num_ec2_task_instances',
            default=None, type='int',
            help=('Number of EC2 instances to start as task '
                  'nodes. Incompatible with --num-ec2-instances.'))

        self.emr_opt_group.add_option(
            '--pool-emr-job-flows', dest='pool_emr_job_flows',
            action='store_true',
            help='Add to an existing job flow or create a new one that does'
                 ' not terminate when the job completes. Overrides other job'
                 ' flow-related options including EC2 instance configuration.'
                 ' Joins pool "default" if emr_job_flow_pool_name is not'
                 ' specified. WARNING: do not run this without'
                 ' mrjob.tools.emr.terminate_idle_job_flows in your crontab;'
                 ' job flows left idle can quickly become expensive!')

        self.emr_opt_group.add_option(
            '--pool-name', dest='emr_job_flow_pool_name', action='store',
            default=None,
            help=('Specify a pool name to join. Set to "default" if not'
                  ' specified.'))

        self.emr_opt_group.add_option(
            '--s3-endpoint', dest='s3_endpoint', default=None,
            help=('Host to connect to when communicating with S3 (e.g.'
                  ' s3-us-west-1.amazonaws.com). Default is to infer this from'
                  ' region (see --aws-region).'))

        self.emr_opt_group.add_option(
            '--s3-log-uri', dest='s3_log_uri', default=None,
            help='URI on S3 to write logs into')

        self.emr_opt_group.add_option(
            '--s3-scratch-uri', dest='s3_scratch_uri', default=None,
            help='URI on S3 to use as our temp directory.')

        self.emr_opt_group.add_option(
            '--s3-sync-wait-time', dest='s3_sync_wait_time', default=None,
            type='float',
            help=('How long to wait for S3 to reach eventual consistency. This'
                  ' is typically less than a second (zero in us-west) but the'
                  ' default is 5.0 to be safe.'))

        self.emr_opt_group.add_option(
            '--ssh-bin', dest='ssh_bin', default=None,
            help=("Name/path of ssh binary. Arguments are allowed (e.g."
                  " --ssh-bin 'ssh -v')"))

        self.emr_opt_group.add_option(
            '--ssh-bind-ports', dest='ssh_bind_ports', default=None,
            help=('A list of port ranges that are safe to listen on, delimited'
                  ' by colons and commas, with syntax like'
                  ' 2000[:2001][,2003,2005:2008,etc].'
                  ' Defaults to 40001:40840.'))

        self.emr_opt_group.add_option(
            '--ssh-tunnel-is-closed', dest='ssh_tunnel_is_open',
            default=None, action='store_false',
            help='Make ssh tunnel accessible from localhost only')

        self.emr_opt_group.add_option(
            '--ssh-tunnel-is-open', dest='ssh_tunnel_is_open',
            default=None, action='store_true',
            help=('Make ssh tunnel accessible from remote hosts (not just'
                  ' localhost).'))

        self.emr_opt_group.add_option(
            '--ssh-tunnel-to-job-tracker', dest='ssh_tunnel_to_job_tracker',
            default=None, action='store_true',
            help='Open up an SSH tunnel to the Hadoop job tracker')

    def all_option_groups(self):
        return (self.option_parser, self.mux_opt_group,
                self.proto_opt_group, self.runner_opt_group,
                self.hadoop_emr_opt_group, self.emr_opt_group,
                self.hadoop_opts_opt_group)

    def add_passthrough_option(self, *args, **kwargs):
        """Function to create options which both the job runner
        and the job itself respect (we use this for protocols, for example).

        Use it like you would use :py:func:`optparse.OptionParser.add_option`::

            def configure_options(self):
                super(MRYourJob, self).configure_options()
                self.add_passthrough_option(
                    '--max-ngram-size', type='int', default=4, help='...')

        Specify an *opt_group* keyword argument to add the option to that
        :py:class:`OptionGroup` rather than the top-level
        :py:class:`OptionParser`.

        If you want to pass files through to the mapper/reducer, use
        :py:meth:`add_file_option` instead.
        """
        if 'opt_group' in kwargs:
            pass_opt = kwargs.pop('opt_group').add_option(*args, **kwargs)
        else:
            pass_opt = self.option_parser.add_option(*args, **kwargs)

        self._passthrough_options.append(pass_opt)

    def add_file_option(self, *args, **kwargs):
        """Add a command-line option that sends an external file
        (e.g. a SQLite DB) to Hadoop::

             def configure_options(self):
                super(MRYourJob, self).configure_options()
                self.add_file_option('--scoring-db', help=...)

        This does the right thing: the file will be uploaded to the working
        dir of the script on Hadoop, and the script will be passed the same
        option, but with the local name of the file in the script's working
        directory.

        We suggest against sending Berkeley DBs to your job, as
        Berkeley DB is not forwards-compatible (so a Berkeley DB that you
        construct on your computer may not be readable from within
        Hadoop). Use SQLite databases instead. If all you need is an on-disk
        hash table, try out the :py:mod:`sqlite3dbm` module.
        """
        pass_opt = self.option_parser.add_option(*args, **kwargs)

        if not pass_opt.type == 'string':
            raise OptionError(
                'passthrough file options must take strings' % pass_opt.type)

        if not pass_opt.action in ('store', 'append'):
            raise OptionError("passthrough file options must use the options"
                              " 'store' or 'append'")

        self._file_options.append(pass_opt)

    def load_options(self, args):
        """Load command-line options into ``self.options``.

        Called from :py:meth:`__init__()` after :py:meth:`configure_options`.

        :type args: list of str
        :param args: a list of command line arguments. ``None`` will be
                     treated the same as ``[]``.

        Re-define if you want to post-process command-line arguments::

            def load_options(self, args):
                super(MRYourJob, self).load_options(args)

                self.stop_words = self.options.stop_words.split(',')
                ...
        """
        self.options, self.args = self.option_parser.parse_args(args)

        # parse custom options here to avoid setting a custom Option subclass
        # and confusing users

        if self.options.ssh_bind_ports:
            try:
                ports = parse_port_range_list(self.options.ssh_bind_ports)
            except ValueError, e:
                self.option_parser.error('invalid port range list "%s": \n%s' %
                                         (self.options.ssh_bind_ports,
                                          e.args[0]))
            self.options.ssh_bind_ports = ports

        cmdenv_err = 'cmdenv argument "%s" is not of the form KEY=VALUE'
        self.options.cmdenv = parse_key_value_list(self.options.cmdenv,
                                                   cmdenv_err,
                                                   self.option_parser.error)

        jobconf_err = 'jobconf argument "%s" is not of the form KEY=VALUE'
        self.options.jobconf = parse_key_value_list(self.options.jobconf,
                                                    jobconf_err,
                                                    self.option_parser.error)

        def parse_commas(cleanup_str):
            cleanup_error = ('cleanup option %s is not one of '
                             + ', '.join(CLEANUP_CHOICES))
            new_cleanup_options = []
            for choice in cleanup_str.split(','):
                if choice in CLEANUP_CHOICES:
                    new_cleanup_options.append(choice)
                else:
                    self.option_parser.error(cleanup_error % choice)
            if ('NONE' in new_cleanup_options and
                len(set(new_cleanup_options)) > 1):
                self.option_parser.error(
                    'Cannot clean up both nothing and something!')
            return new_cleanup_options

        if self.options.cleanup is not None:
            self.options.cleanup = parse_commas(self.options.cleanup)
        if self.options.cleanup_on_failure is not None:
            self.options.cleanup_on_failure = parse_commas(
                self.options.cleanup_on_failure)

        # DEPRECATED protocol stuff

        ignore_switches = (
            self.INPUT_PROTOCOL != RawValueProtocol or
            self.INTERNAL_PROTOCOL != JSONProtocol or
            self.OUTPUT_PROTOCOL != JSONProtocol or
            any(
                (getattr(self, func_name).im_func is not
                 getattr(MRJob, func_name).im_func)
                for func_name in (
                    'input_protocol',
                    'internal_protocol',
                    'output_protocol',
                )
            )
        )

        warn_deprecated = False

        if self.options.protocol is None:
            self.options.protocol = self.DEFAULT_PROTOCOL
            if self.DEFAULT_PROTOCOL != 'json':
                warn_deprecated = True
        else:
            warn_deprecated = True

        if self.options.input_protocol is None:
            self.options.input_protocol = self.DEFAULT_INPUT_PROTOCOL
            if self.DEFAULT_INPUT_PROTOCOL != 'raw_value':
                warn_deprecated = True
        else:
            warn_deprecated = True

        # output_protocol defaults to protocol
        if self.options.output_protocol is None:
            self.options.output_protocol = self.options.protocol
        else:
            warn_deprecated = True

        if warn_deprecated:
            if ignore_switches:
                log.warn('You have specified custom behavior in both'
                         ' deprecated and non-deprecated ways.'
                         ' The custom non-deprecated behavior will override'
                         ' the deprecated behavior in all cases, including'
                         ' command line switches.')
                self.options.input_protocol = None
                self.options.protocol = None
                self.options.output_protocol = None
            else:
                log.warn('Setting protocols via --input-protocol, --protocol,'
                         ' --output-protocol, DEFAULT_INPUT_PROTOCOL,'
                         ' DEFAULT_PROTOCOL, and DEFAULT_OUTPUT_PROTOCOL is'
                         ' deprecated as of mrjob 0.3 and will no longer be'
                         ' supported in mrjob 0.4.')

    def is_mapper_or_reducer(self):
        """True if this is a mapper/reducer.

        This is mostly useful inside :py:meth:`load_options`, to disable
        loading options when we aren't running inside Hadoop Streaming.
        """
        return self.options.run_mapper \
                or self.options.run_combiner \
                or self.options.run_reducer

    def job_runner_kwargs(self):
        """Keyword arguments used to create runners when
        :py:meth:`make_runner` is called.

        :return: map from arg name to value

        Re-define this if you want finer control of runner initialization.

        You might find :py:meth:`mrjob.conf.combine_dicts` useful if you
        want to add or change lots of keyword arguments.
        """
        return {
            'bootstrap_mrjob': self.options.bootstrap_mrjob,
            'cleanup': self.options.cleanup,
            'cleanup_on_failure': self.options.cleanup_on_failure,
            'cmdenv': self.options.cmdenv,
            'conf_path': self.options.conf_path,
            'extra_args': self.generate_passthrough_arguments(),
            'file_upload_args': self.generate_file_upload_args(),
            'hadoop_extra_args': self.options.hadoop_extra_args,
            'hadoop_input_format': self.hadoop_input_format(),
            'hadoop_output_format': self.hadoop_output_format(),
            'hadoop_streaming_jar': self.options.hadoop_streaming_jar,
            'hadoop_version': self.options.hadoop_version,
            'input_paths': self.args,
            'jobconf': self.jobconf(),
            'mr_job_script': self.mr_job_script(),
            'label': self.options.label,
            'output_dir': self.options.output_dir,
            'owner': self.options.owner,
            'partitioner': self.partitioner(),
            'python_archives': self.options.python_archives,
            'python_bin': self.options.python_bin,
            'setup_cmds': self.options.setup_cmds,
            'setup_scripts': self.options.setup_scripts,
            'stdin': self.stdin,
            'steps_python_bin': self.options.steps_python_bin,
            'upload_archives': self.options.upload_archives,
            'upload_files': self.options.upload_files,
        }

    def inline_job_runner_kwargs(self):
        """Keyword arguments to create create runners when
        :py:meth:`make_runner` is called, when we run a job locally
        (``-r inline``).

        :return: map from arg name to value

        Re-define this if you want finer control when running jobs locally.
        """
        return self.job_runner_kwargs()

    def local_job_runner_kwargs(self):
        """Keyword arguments to create create runners when
        :py:meth:`make_runner` is called, when we run a job locally
        (``-r local``).

        :return: map from arg name to value

        Re-define this if you want finer control when running jobs locally.
        """
        return self.job_runner_kwargs()

    def emr_job_runner_kwargs(self):
        """Keyword arguments to create create runners when
        :py:meth:`make_runner` is called, when we run a job on EMR
        (``-r emr``).

        :return: map from arg name to value

        Re-define this if you want finer control when running jobs on EMR.
        """
        return combine_dicts(
            self.job_runner_kwargs(),
            self._get_kwargs_from_opt_group(self.emr_opt_group))

    def hadoop_job_runner_kwargs(self):
        """Keyword arguments to create create runners when
        :py:meth:`make_runner` is called, when we run a job on EMR
        (``-r hadoop``).

        :return: map from arg name to value

        Re-define this if you want finer control when running jobs on hadoop.
        """
        return combine_dicts(
            self.job_runner_kwargs(),
            self._get_kwargs_from_opt_group(self.hadoop_opt_group))

    def _get_kwargs_from_opt_group(self, opt_group):
        """Helper function that returns a dictionary of the values of options
        in the given options group (this works because the options and the
        keyword args we want to set have identical names).
        """
        keys = set(opt.dest for opt in opt_group.option_list)
        return dict((key, getattr(self.options, key)) for key in keys)

    def generate_passthrough_arguments(self):
        """Returns a list of arguments to pass to subprocesses, either on
        hadoop or executed via subprocess.

        These are passed to :py:meth:`mrjob.runner.MRJobRunner.__init__`
        as *extra_args*.
        """
        arg_map = parse_and_save_options(self.option_parser, self._cl_args)
        output_args = []

        passthrough_dests = sorted(set(option.dest for option \
                                       in self._passthrough_options))
        for option_dest in passthrough_dests:
            output_args.extend(arg_map.get(option_dest, []))

        return output_args

    def generate_file_upload_args(self):
        """Figure out file upload args to pass through to the job runner.

        Instead of generating a list of args, we're generating a list
        of tuples of ``('--argname', path)``

        These are passed to :py:meth:`mrjob.runner.MRJobRunner.__init__`
        as ``file_upload_args``.
        """
        file_upload_args = []

        master_option_dict = self.options.__dict__

        for opt in self._file_options:
            opt_prefix = opt.get_opt_string()
            opt_value = master_option_dict[opt.dest]

            if opt_value:
                paths = opt_value if opt.action == 'append' else [opt_value]
                for path in paths:
                    file_upload_args.append((opt_prefix, path))

        return file_upload_args

    ### protocols ###

    def input_protocol(self):
        """Instance of the protocol to use to convert input lines to Python
        objects. Default behavior is to return an instance of
        :py:attr:`INPUT_PROTOCOL`.
        """
        if (self.options.input_protocol is not None and
            self.INPUT_PROTOCOL == RawValueProtocol):
            # deprecated
            protocol_name = self.options.input_protocol
            return self.protocols()[protocol_name]()
        else:
            # non-deprecated
            return self.INPUT_PROTOCOL()

    def internal_protocol(self):
        """Instance of the protocol to use to communicate between steps.
        Default behavior is to return an instance of
        :py:attr:`INTERNAL_PROTOCOL`.
        """
        if (self.options.protocol is not None and
            self.INTERNAL_PROTOCOL == JSONProtocol):
            # deprecated
            protocol_name = self.options.protocol
            return self.protocols()[protocol_name]
        else:
            # non-deprecated
            return self.INTERNAL_PROTOCOL()

    def output_protocol(self):
        """Instance of the protocol to use to convert Python objects to output
        lines. Default behavior is to return an instance of
        :py:attr:`OUTPUT_PROTOCOL`.
        """
        if (self.options.output_protocol is not None and
            self.OUTPUT_PROTOCOL == JSONProtocol):
            # deprecated
            return self.protocols()[self.options.output_protocol]
        else:
            # non-deprecated
            return self.OUTPUT_PROTOCOL()

    @classmethod
    def protocols(cls):
        """Deprecated in favor of :py:attr:`INPUT_PROTOCOL`,
        :py:attr:`OUTPUT_PROTOCOL`, and :py:attr:`INTERNAL_PROTOCOL`.

        Mapping from protocol name to the protocol class to use
        for parsing job input and writing job output. We give protocols names
        so that we can easily choose them from the command line.

        This returns :py:data:`mrjob.protocol.PROTOCOL_DICT` by default.

        To add a custom protocol, define a subclass of
        :py:class:`mrjob.protocol.HadoopStreamingProtocol`, and
        re-define this method::

            @classmethod
            def protocols(cls):
                protocol_dict = super(MRYourJob, cls).protocols()
                protocol_dict['rot13'] = Rot13Protocol
                return protocol_dict

            DEFAULT_PROTOCOL = 'rot13'
        """
        return PROTOCOL_DICT.copy()  # copy to stop monkey-patching

    #: Protocol for reading input to the first mapper in your job.
    #: Default: :py:class:`RawValueProtocol`.
    #:
    #: For example you know your input data were in JSON format, you could
    #: set::
    #:
    #:     INPUT_PROTOCOL = JsonValueProtocol
    #:
    #: in your class, and your initial mapper would receive decoded JSONs
    #: rather than strings.
    #:
    #: See :py:data:`mrjob.protocol` for the full list of protocols.
    INPUT_PROTOCOL = RawValueProtocol

    #: Protocol for communication between steps and final output.
    #: Default: :py:class:`JSONProtocol`.
    #:
    #: For example if your step output weren't JSON-encodable, you could set::
    #:
    #:     INTERNAL_PROTOCOL = PickleProtocol
    #:
    #: and step output would be encoded as string-escaped pickles.
    #:
    #: See :py:data:`mrjob.protocol` for the full list of protocols.
    INTERNAL_PROTOCOL = JSONProtocol

    #: Protocol to use for writing output. Default: :py:class:`JSONProtocol`.
    #:
    #: For example, if you wanted the final output in repr, you could set::
    #:
    #:     OUTPUT_PROTOCOL = ReprProtocol
    #:
    #: See :py:data:`mrjob.protocol` for the full list of protocols.
    OUTPUT_PROTOCOL = JSONProtocol

    #: .. deprecated:: 0.3.0
    #:
    #: Default protocol for reading input to the first mapper in your job
    #: specified by a string.
    #:
    #: Overridden by any changes to :py:attr:`.INPUT_PROTOCOL`.
    #:
    #: See :py:data:`mrjob.protocol.PROTOCOL_DICT` for the full list of
    #: protocol strings. Can be overridden by :option:`--input-protocol`.
    DEFAULT_INPUT_PROTOCOL = 'raw_value'

    #: .. deprecated:: 0.3.0
    #:
    #: Default protocol for communication between steps and final output
    #: specified by a string.
    #:
    #: Overridden by any changes to :py:attr:`.INTERNAL_PROTOCOL`.
    #:
    #: See :py:data:`mrjob.protocol.PROTOCOL_DICT` for the full list of
    #: protocol strings. Can be overridden by :option:`--protocol`.
    DEFAULT_PROTOCOL = DEFAULT_PROTOCOL  # i.e. the one from mrjob.protocols

    #: .. deprecated:: 0.3.0
    #:
    #: Overridden by any changes to :py:attr:`.OUTPUT_PROTOCOL`. If
    #: :py:attr:`.OUTPUT_PROTOCOL` is not set, defaults to
    #: :py:attr:`.DEFAULT_PROTOCOL`.
    #:
    #: See :py:data:`mrjob.protocol.PROTOCOL_DICT` for the full list of
    #: protocol strings. Can be overridden by the :option:`--output-protocol`.
    DEFAULT_OUTPUT_PROTOCOL = None

    def parse_output_line(self, line):
        """
        Parse a line from the final output of this MRJob into
        ``(key, value)``. Used extensively in tests like this::

            runner.run()
            for line in runner.stream_output():
                key, value = mr_job.parse_output_line(line)
        """
        return self.output_protocol().read(line)

    ### Hadoop Input/Output Formats ###

    #: Optional name of an optional Hadoop ``InputFormat`` class, e.g.
    #: ``'org.apache.hadoop.mapred.lib.NLineInputFormat'``.
    #:
    #: Passed to Hadoop with the *first* step of this job with the
    #: ``-inputformat`` option.
    HADOOP_INPUT_FORMAT = None

    def hadoop_input_format(self):
        """Optional Hadoop ``InputFormat`` class to parse input for
        the first step of the job.

        Normally, setting :py:attr:`HADOOP_INPUT_FORMAT` is sufficient;
        redefining this method is only for when you want to get fancy.
        """
        if self.options.hadoop_input_format:
            log.warn('--hadoop-input-format is deprecated as of mrjob 0.3 and'
                     ' will no longer be supported in mrjob 0.4. Redefine'
                     ' HADOOP_INPUT_FORMAT or hadoop_input_format() instead.')
            return self.options.hadoop_input_format
        else:
            return self.HADOOP_INPUT_FORMAT

    #: Optional name of an optional Hadoop ``OutputFormat`` class, e.g.
    #: ``'org.apache.hadoop.mapred.FileOutputFormat'``.
    #:
    #: Passed to Hadoop with the *last* step of this job with the
    #: ``-outputformat`` option.
    HADOOP_OUTPUT_FORMAT = None

    def hadoop_output_format(self):
        """Optional Hadoop ``OutputFormat`` class to write output for
        the last step of the job.

        Normally, setting :py:attr:`HADOOP_OUTPUT_FORMAT` is sufficient;
        redefining this method is only for when you want to get fancy.
        """
        if self.options.hadoop_output_format:
            log.warn('--hadoop-output-format is deprecated as of mrjob 0.3 and'
                     ' will no longer be supported in mrjob 0.4. Redefine '
                     ' HADOOP_OUTPUT_FORMAT or hadoop_output_format() instead.'
                     )
            return self.options.hadoop_output_format
        else:
            return self.HADOOP_OUTPUT_FORMAT

    ### Partitioning ###

    #: Optional Hadoop partitioner class to use to determine how mapper
    #: output should be sorted and distributed to reducers. For example:
    #: ``'org.apache.hadoop.mapred.lib.HashPartitioner'``.
    PARTITIONER = None

    def partitioner(self):
        """Optional Hadoop partitioner class to use to determine how mapper
        output should be sorted and distributed to reducers.

        By default, returns whatever is passed to :option:`--partitioner`,
        of if that option isn't used, :py:attr:`PARTITIONER`.

        You probably don't need to re-define this; it's just here for
        completeness.
        """
        return self.options.partitioner or self.PARTITIONER

    ### Jobconf ###

    #: Optional jobconf arguments we should always pass to Hadoop. This
    #: is a map from property name to value. e.g.:
    #:
    #: ``{'stream.num.map.output.key.fields': '4'}``
    #:
    #: It's recommended that you only use this to hard-code things that
    #: affect the semantics of your job, and leave performance tweaks to
    #: the command line or whatever you use to launch your job.
    JOBCONF = {}

    def jobconf(self):
        """``-jobconf`` args to pass to hadoop streaming. This should be a map
        from property name to value.

        By default, this combines :option:`jobconf` options from the command
        lines with :py:attr:`JOBCONF`, with command line arguments taking
        precedence.

        If you want to re-define this, it's strongly recommended that do
        something like this, so as not to inadvertently disable
        :option:`jobconf`::

            def jobconf(self):
                orig_jobconf = super(MyMRJobClass, self).jobconf()
                custom_jobconf = ...

                return mrjob.conf.combine_dicts(orig_jobconf, custom_jobconf)
        """
        return combine_dicts(self.JOBCONF, self.options.jobconf)

    ### Testing ###

    def sandbox(self, stdin=None, stdout=None, stderr=None):
        """Redirect stdin, stdout, and stderr for automated testing.

        You can set stdin, stdout, and stderr to file objects. By
        default, they'll be set to empty ``StringIO`` objects.
        You can then access the job's file handles through ``self.stdin``,
        ``self.stdout``, and ``self.stderr``. See :ref:`testing` for more
        information about testing.

        You may call sandbox multiple times (this will essentially clear
        the file handles).

        ``stdin`` is empty by default. You can set it to anything that yields
        lines::

            mr_job.sandbox(stdin=StringIO('some_data\\n'))

        or, equivalently::

            mr_job.sandbox(stdin=['some_data\\n'])

        For convenience, this sandbox() returns self, so you can do::

            mr_job = MRJobClassToTest().sandbox()

        Simple testing example::

            mr_job = MRYourJob.sandbox()
            assert_equal(list(mr_job.reducer('foo', ['bar', 'baz'])), [...])

        More complex testing example::

            from StringIO import StringIO

            mr_job = MRYourJob(args=[...])

            fake_input = '"foo"\\t"bar"\\n"foo"\\t"baz"\\n'
            mr_job.sandbox(stdin=StringIO(fake_input))

            mr_job.run_reducer(link_num=0)
            assert_equal(mr_job.parse_output(), ...)
            assert_equal(mr_job.parse_counters(), ...)
        """
        self.stdin = stdin or StringIO()
        self.stdout = stdout or StringIO()
        self.stderr = stderr or StringIO()

        return self

    def parse_counters(self, counters=None):
        """Convenience method for reading counters. This only works
        in sandbox mode. This does not clear ``self.stderr``.

        :return: a map from counter group to counter name to amount.

        To read everything from ``self.stderr`` (including status messages)
        use :py:meth:`mrjob.parse.parse_mr_job_stderr`.

        When writing unit tests, you may find :py:meth:`MRJobRunner.counters()
        <mrjob.runner.MRJobRunner.counters()>` more useful.
        """
        if self.stderr == sys.stderr:
            raise AssertionError('You must call sandbox() first;'
                                 ' parse_counters() is for testing only.')

        stderr_results = parse_mr_job_stderr(self.stderr.getvalue(), counters)
        return stderr_results['counters']

    def parse_output(self, protocol=None):
        """Convenience method for parsing output from any mapper or reducer,
        all at once.

        This helps you test individual mappers and reducers by calling
        run_mapper() or run_reducer(). For example::

            mr_job.sandbox(stdin=your_input)
            mr_job.run_mapper(step_num=0)
            output = mrjob.parse_output()

        :type protocol: str
        :param protocol: A protocol instance to use (e.g. JSONProtocol()),
                         Also accepts protocol names (e.g. ``'json'``), but
                         this is deprecated.

        This only works in sandbox mode. This does not clear ``self.stdout``.
        """
        if self.stdout == sys.stdout:
            raise AssertionError('You must call sandbox() first;'
                                 ' parse_output() is for testing only.')

        if protocol is None:
            protocol = JSONProtocol()
        elif isinstance(protocol, basestring):
            protocol = self.protocols()[protocol]

        lines = StringIO(self.stdout.getvalue())
        return [protocol.read(line) for line in lines]


if __name__ == '__main__':
    MRJob.run()
