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
"""Class to inherit your MapReduce jobs from. See :doc:`guides/writing-mrjobs`
for more information."""
# don't add imports here that aren't part of the standard Python library,
# since MRJobs need to run in Amazon's generic EMR environment
from __future__ import with_statement

import codecs
import inspect
import itertools
import logging
from optparse import OptionGroup
import sys

try:
    from cStringIO import StringIO
    StringIO  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    from StringIO import StringIO

try:
    import simplejson as json
    json  # silence, pyflakes!
except ImportError:
    import json

# don't use relative imports, to allow this script to be invoked as __main__
from mrjob.conf import combine_dicts

from mrjob.parse import parse_mr_job_stderr
from mrjob.protocol import JSONProtocol
from mrjob.protocol import RawValueProtocol
from mrjob.launch import MRJobLauncher
from mrjob.launch import _READ_ARGS_FROM_SYS_ARGV
from mrjob.step import JarStep
from mrjob.step import MRJobStep
from mrjob.step import _JOB_STEP_FUNC_PARAMS
from mrjob.util import read_input


log = logging.getLogger(__name__)


# jobconf options for implementing SORT_VALUES
_SORT_VALUES_JOBCONF = {
    'stream.num.map.output.key.fields': 2,
    'mapred.text.key.partitioner.options': '-k1,1',
    # Hadoop's defaults for these actually work fine; we just want to
    # prevent interference from mrjob.conf.
    'mapred.output.key.comparator.class': None,
    'mapred.text.key.comparator.options': None,
}

# partitioner for sort_values
_SORT_VALUES_PARTITIONER = \
    'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner'


class UsageError(Exception):
    pass


class MRJob(MRJobLauncher):
    """The base class for all MapReduce jobs. See :py:meth:`__init__`
    for details."""

    # inline can be the default because we have the class object in the same
    # process as the launcher
    _DEFAULT_RUNNER = 'inline'

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
        super(MRJob, self).__init__(self.mr_job_script(), args)

    @classmethod
    def _usage(cls):
        return "usage: %prog [options] [input files]"

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

    def mapper_cmd(self):
        """Re-define this to define the mapper for a one-step job **as a shell
        command.** If you define your mapper this way, the command will be
        passed unchanged to Hadoop Streaming, with some minor exceptions. For
        important specifics, see :ref:`cmd-steps`.

        Basic example::

            def mapper_cmd(self):
                return 'cat'
        """
        raise NotImplementedError

    def mapper_pre_filter(self):
        """Re-define this to specify a shell command to filter the mapper's
        input before it gets to your job's mapper in a one-step job. For
        important specifics, see :ref:`cmd-filters`.

        Basic example::

            def mapper_pre_filter(self):
                return 'grep "ponies"'
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

    def reducer_cmd(self):
        """Re-define this to define the reducer for a one-step job **as a shell
        command.** If you define your mapper this way, the command will be
        passed unchanged to Hadoop Streaming, with some minor exceptions. For
        specifics, see :ref:`cmd-steps`.

        Basic example::

            def reducer_cmd(self):
                return 'cat'
        """
        raise NotImplementedError

    def reducer_pre_filter(self):
        """Re-define this to specify a shell command to filter the reducer's
        input before it gets to your job's reducer in a one-step job. For
        important specifics, see :ref:`cmd-filters`.

        Basic example::

            def reducer_pre_filter(self):
                return 'grep "ponies"'
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

    def combiner_cmd(self):
        """Re-define this to define the combiner for a one-step job **as a
        shell command.** If you define your mapper this way, the command will
        be passed unchanged to Hadoop Streaming, with some minor exceptions.
        For specifics, see :ref:`cmd-steps`.

        Basic example::

            def combiner_cmd(self):
                return 'cat'
        """
        raise NotImplementedError

    def combiner_pre_filter(self):
        """Re-define this to specify a shell command to filter the combiner's
        input before it gets to your job's combiner in a one-step job. For
        important specifics, see :ref:`cmd-filters`.

        Basic example::

            def combiner_pre_filter(self):
                return 'grep "ponies"'
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
                      for func_name in _JOB_STEP_FUNC_PARAMS
                      if (getattr(self, func_name).im_func is not
                          getattr(MRJob, func_name).im_func))

        # MRJobStep takes commands as strings, but the user defines them in the
        # class as functions that return strings, so call the functions.
        updates = {}
        for k, v in kwargs.iteritems():
            if k.endswith('_cmd'):
                updates[k] = v()

        kwargs.update(updates)

        return [self.mr(**kwargs)]

    @classmethod
    def mr(cls, *args, **kwargs):
        """Define a Python step (mapper, reducer, and/or any combination of
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
        :param jobconf: dictionary with custom jobconf arguments to pass to
                        hadoop.

        Please consider the way we represent steps to be opaque, and expect
        it to change in future versions of ``mrjob``.
        """
        if args:
            log.warning('Using positional arguments to MRJob.mr() is'
                        ' deprecated.')

        if len(args) > 0:
            kwargs['mapper'] = args[0]

        if len(args) > 1:
            kwargs['reducer'] = args[1]

        if len(args) > 2:
            raise ValueError('mr() can take at most two positional arguments.')

        return MRJobStep(**kwargs)

    @classmethod
    def jar(cls, name, jar, main_class=None, step_args=None):
        """Define a jar step for your job.

        Used by :py:meth:`steps`. (Don't re-define this, just call it!)

        :param name: Name to give the job for display in Hadoop
        :param jar: Hadoop-accessible path to the jar file to run
        :param main_class: Path of the main class in the jar if not default
        :param step_args: Extra arguments to pass the jar when running it

        Please consider the way we represent steps to be opaque, and expect
        it to change in future versions of ``mrjob``.
        """
        return JarStep(name, jar, main_class=main_class, step_args=step_args)

    def increment_counter(self, group, counter, amount=1):
        """Increment a counter in Hadoop streaming by printing to stderr. If
        the type of either **group** or **counter** is ``unicode``, then the
        counter will be written as unicode. Otherwise, the counter will be
        written as ASCII. Although writing non-ASCII will succeed, the
        resulting counter names may not be displayed correctly at the end of
        the job.

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
        if isinstance(group, unicode) or isinstance(counter, unicode):
            group = unicode(group).replace(',', ';')
            counter = unicode(counter).replace(',', ';')
            stderr = codecs.getwriter('utf-8')(self.stderr)
        else:
            group = str(group).replace(',', ';')
            counter = str(counter).replace(',', ';')
            stderr = self.stderr

        stderr.write(
            u'reporter:counter:%s,%s,%d\n' % (group, counter, amount))
        stderr.flush()

    def set_status(self, msg):
        """Set the job status in hadoop streaming by printing to stderr.

        This is also a good way of doing a keepalive for a job that goes a
        long time between outputs; Hadoop streaming usually times out jobs
        that give no output for longer than 10 minutes.

        If the type of **msg** is ``unicode``, then the message will be written
        as unicode. Otherwise, it will be written as ASCII.
        """
        if isinstance(msg, unicode):
            status = u'reporter:status:%s\n' % (msg,)
            stderr = codecs.getwriter('utf-8')(self.stderr)
        else:
            status = 'reporter:status:%s\n' % (msg,)
            stderr = self.stderr
        stderr.write(status)
        stderr.flush()

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
        # MRJob does Hadoop Streaming stuff, or defers to Launcher (superclass)
        # if not otherwise instructed
        if self.options.show_steps:
            self.show_steps()

        elif self.options.run_mapper:
            self.run_mapper(self.options.step_num)

        elif self.options.run_combiner:
            self.run_combiner(self.options.step_num)

        elif self.options.run_reducer:
            self.run_reducer(self.options.step_num)

        else:
            super(MRJob, self).execute()

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

        # support inline runner when running from the MRJob itself
        from mrjob.inline import InlineMRJobRunner

        if self.options.runner == 'inline':
            return InlineMRJobRunner(mrjob_cls=self.__class__,
                                     **self.inline_job_runner_kwargs())

        return super(MRJob, self).make_runner()

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
        read_lines, write_line = self._wrap_protocols(step_num, 'mapper')

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
        read_lines, write_line = self._wrap_protocols(step_num, 'reducer')

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
        read_lines, write_line = self._wrap_protocols(step_num, 'combiner')

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
        they contain a mapper or reducer. Job runners (see
        :doc:`guides/runners`) use this to determine how Hadoop should call
        this script.

        Called from :py:meth:`run`. You'd probably only want to call this
        directly from automated tests.

        We currently output something like ``MR M R``, but expect this to
        change!
        """
        print >> self.stdout, json.dumps(self._steps_desc())

    def _steps_desc(self):
        step_descs = []
        for step_num, step in enumerate(self.steps()):
            step_descs.append(step.description(step_num))
        return step_descs

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

        Returns a tuple of ``(read_lines, write_line)``

        ``read_lines()`` is a function that reads lines from input, decodes
            them, and yields key, value pairs.
        ``write_line()`` is a function that takes key and value as args,
            encodes them, and writes a line to output.

        :param step_num: which step to run (e.g. 0)
        :param step_type: ``'mapper'``, ``'reducer'``, or ``'combiner'`` from
                          :py:mod:`mrjob.step`
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
                        self.increment_counter(
                            'Undecodable input', e.__class__.__name__)

        def write_line(key, value):
            try:
                print >> self.stdout, write(key, value)
            except Exception, e:
                if self.options.strict_protocols:
                    raise
                else:
                    self.increment_counter(
                        'Unencodable output', e.__class__.__name__)

        return read_lines, write_line

    def _step_key(self, step_num, step_type):
        return '%d-%s' % (step_num, step_type)

    def _script_step_mapping(self, steps_desc):
        """Return a mapping of ``self._step_key(step_num, step_type)`` ->
        (place in sort order of all *script* steps), for the purposes of
        choosing which protocols to use for input and output.

        Non-script steps do not appear in the mapping.
        """
        mapping = {}
        script_step_num = 0
        for i, step in enumerate(steps_desc):
            if 'mapper' in step:
                if step['mapper']['type'] == 'script':
                    k = self._step_key(i, 'mapper')
                    mapping[k] = script_step_num
                    script_step_num += 1
            if 'reducer' in step:
                if step['reducer']['type'] == 'script':
                    k = self._step_key(i, 'reducer')
                    mapping[k] = script_step_num
                    script_step_num += 1

        return mapping

    def _mapper_output_protocol(self, step_num, step_map):
        map_key = self._step_key(step_num, 'mapper')
        if map_key in step_map:
            if step_map[map_key] >= (len(step_map) - 1):
                return self.output_protocol()
            else:
                return self.internal_protocol()
        else:
            # mapper is not a script substep, so protocols don't apply at all
            return RawValueProtocol()

    def _pick_protocol_instances(self, step_num, step_type):
        steps_desc = self._steps_desc()

        step_map = self._script_step_mapping(steps_desc)

        # pick input protocol

        if step_type == 'combiner':
            # Combiners read and write the mapper's output protocol because
            # they have to be able to run 0-inf times without changing the
            # format of the data.
            # Combiners for non-script substeps can't use protocols, so this
            # function will just give us RawValueProtocol() in that case.
            previous_mapper_output = self._mapper_output_protocol(
                step_num, step_map)
            return previous_mapper_output, previous_mapper_output
        else:
            step_key = self._step_key(step_num, step_type)

            if step_key not in step_map:
                # It's unlikely that we will encounter this logic in real life,
                # but if asked what the protocol of a non-script step is, we
                # should just say RawValueProtocol because we have no idea what
                # the jars or commands are doing with our precious data.
                # If --strict-protocols, though, we won't stand for these
                # shenanigans!
                if self.options.strict_protocols:
                    raise ValueError(
                        "Can't pick a protocol for a non-script step")
                else:
                    p = RawValueProtocol()
                    return p, p

            real_num = step_map[step_key]
            if real_num == (len(step_map) - 1):
                write = self.output_protocol()
            else:
                write = self.internal_protocol()

            if real_num == 0:
                read = self.input_protocol()
            else:
                read = self.internal_protocol()
            return read, write

    def pick_protocols(self, step_num, step_type):
        """Pick the protocol classes to use for reading and writing for the
        given step.

        :type step_num: int
        :param step_num: which step to run (e.g. ``0`` for the first step)
        :type step_type: str
        :param step_type: one of :py:data:`mrjob.step.'mapper'`,
                          :py:data:`mrjob.step.'combiner'`, or
                          :py:data:`mrjob.step.'reducer'`
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

        # wrapping functionality like this makes testing much simpler
        p_read, p_write = self._pick_protocol_instances(step_num, step_type)

        return p_read.read, p_write.write

    ### Command-line arguments ###

    def configure_options(self):
        super(MRJob, self).configure_options()

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

        # To describe the steps
        self.mux_opt_group.add_option(
            '--steps', dest='show_steps', action='store_true', default=False,
            help=('print the mappers, combiners, and reducers that this job'
                  ' defines'))

    def all_option_groups(self):
        return super(MRJob, self).all_option_groups() + (self.mux_opt_group,)

    def is_mapper_or_reducer(self):
        """True if this is a mapper/reducer.

        This is mostly useful inside :py:meth:`load_options`, to disable
        loading options when we aren't running inside Hadoop Streaming.
        """
        return (self.options.run_mapper or
                self.options.run_combiner or
                self.options.run_reducer)

    def _process_args(self, args):
        """mrjob.launch takes the first arg as the script path, but mrjob.job
        uses all args as input files. This method determines the behavior:
        MRJob uses all args as input files.
        """
        self.args = args

    def _help_main(self):
        self.option_parser.option_groups = [
            self.mux_opt_group,
            self.proto_opt_group,
        ]
        self.option_parser.print_help()
        sys.exit(0)

    ### protocols ###

    def input_protocol(self):
        """Instance of the protocol to use to convert input lines to Python
        objects. Default behavior is to return an instance of
        :py:attr:`INPUT_PROTOCOL`.
        """
        if not isinstance(self.INPUT_PROTOCOL, type):
            log.warn('INPUT_PROTOCOL should be a class, not %s' %
                     self.INPUT_PROTOCOL)
        return self.INPUT_PROTOCOL()

    def internal_protocol(self):
        """Instance of the protocol to use to communicate between steps.
        Default behavior is to return an instance of
        :py:attr:`INTERNAL_PROTOCOL`.
        """
        if not isinstance(self.INTERNAL_PROTOCOL, type):
            log.warn('INTERNAL_PROTOCOL should be a class, not %s' %
                     self.INTERNAL_PROTOCOL)
        return self.INTERNAL_PROTOCOL()

    def output_protocol(self):
        """Instance of the protocol to use to convert Python objects to output
        lines. Default behavior is to return an instance of
        :py:attr:`OUTPUT_PROTOCOL`.
        """
        if not isinstance(self.OUTPUT_PROTOCOL, type):
            log.warn('OUTPUT_PROTOCOL should be a class, not %s' %
                     self.OUTPUT_PROTOCOL)
        return self.OUTPUT_PROTOCOL()

    #: Protocol for reading input to the first mapper in your job.
    #: Default: :py:class:`RawValueProtocol`.
    #:
    #: For example you know your input data were in JSON format, you could
    #: set::
    #:
    #:     INPUT_PROTOCOL = JSONValueProtocol
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
    #:
    #: If you require more sophisticated behavior, try
    #: :py:meth:`hadoop_input_format` or the *hadoop_input_format* argument to
    #: :py:meth:`mrjob.runner.MRJobRunner.__init__`.
    HADOOP_INPUT_FORMAT = None

    def hadoop_input_format(self):
        """Optional Hadoop ``InputFormat`` class to parse input for
        the first step of the job.

        Normally, setting :py:attr:`HADOOP_INPUT_FORMAT` is sufficient;
        redefining this method is only for when you want to get fancy.
        """
        return self.HADOOP_INPUT_FORMAT

    #: Optional name of an optional Hadoop ``OutputFormat`` class, e.g.
    #: ``'org.apache.hadoop.mapred.FileOutputFormat'``.
    #:
    #: Passed to Hadoop with the *last* step of this job with the
    #: ``-outputformat`` option.
    #:
    #: If you require more sophisticated behavior, try
    #: :py:meth:`hadoop_output_format` or the *hadoop_output_format* argument
    #: to :py:meth:`mrjob.runner.MRJobRunner.__init__`.
    HADOOP_OUTPUT_FORMAT = None

    def hadoop_output_format(self):
        """Optional Hadoop ``OutputFormat`` class to write output for
        the last step of the job.

        Normally, setting :py:attr:`HADOOP_OUTPUT_FORMAT` is sufficient;
        redefining this method is only for when you want to get fancy.
        """
        return self.HADOOP_OUTPUT_FORMAT

    ### Partitioning ###

    #: Optional Hadoop partitioner class to use to determine how mapper
    #: output should be sorted and distributed to reducers. For example:
    #: ``'org.apache.hadoop.mapred.lib.HashPartitioner'``.
    #:
    #: If you require more sophisticated behavior, try :py:meth:`partitioner`.
    PARTITIONER = None

    def partitioner(self):
        """Optional Hadoop partitioner class to use to determine how mapper
        output should be sorted and distributed to reducers.

        By default, returns whatever is passed to :option:`--partitioner`,
        or if that option isn't used, :py:attr:`PARTITIONER`, or if that
        isn't set, and :py:attr:`SORT_VALUES` is true, it's set to
        ``'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner'``.

        You probably don't need to re-define this; it's just here for
        completeness.
        """
        return (self.options.partitioner or
                self.PARTITIONER or
                ('org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner' if
                 self.SORT_VALUES else None))

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

        If :py:attr:`SORT_VALUES` is set, we also set these jobconf values::

            stream.num.map.output.key.fields=2
            mapred.text.key.partitioner.options=k1,1

        We also blank out ``mapred.output.key.comparator.class``
        and ``mapred.text.key.comparator.options`` to prevent interference
        from :file:`mrjob.conf`.

        :py:attr:`SORT_VALUES` *can* be overridden by :py:attr:`JOBCONF`, the
        command line, and step-specific ``jobconf`` values.

        For example, if you know your values are numbers, and want to sort
        them in reverse, you could do::

            SORT_VALUES = True

            JOBCONF = {
              'mapred.output.key.comparator.class':
                  'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
              'mapred.text.key.comparator.options': '-k1 -k2nr',
            }

        If you want to re-define this, it's strongly recommended that do
        something like this, so as not to inadvertently disable
        the :option:`jobconf` option::

            def jobconf(self):
                orig_jobconf = super(MyMRJobClass, self).jobconf()
                custom_jobconf = ...

                return mrjob.conf.combine_dicts(orig_jobconf, custom_jobconf)
        """

        # deal with various forms of bad behavior by users
        unfiltered_jobconf = combine_dicts(self.JOBCONF, self.options.jobconf)
        filtered_jobconf = {}

        def format_hadoop_version(v_float):
            if v_float >= 1.0:
                # e.g. 1.0
                return '%.1f' % v_float
            else:
                # e.g. 0.18 or 0.20
                return '%.2f' % v_float

        for key in unfiltered_jobconf:
            unfiltered_val = unfiltered_jobconf[key]
            filtered_val = unfiltered_val

            # boolean values need to be lowercased
            if isinstance(unfiltered_val, bool):
                if unfiltered_val:
                    filtered_val = 'true'
                else:
                    filtered_val = 'false'

            # hadoop_version should be a string
            elif (key == 'hadoop_version' and
                    isinstance(unfiltered_val, float)):
                log.warn('hadoop_version should be a string, not %s' %
                         unfiltered_val)
                filtered_val = format_hadoop_version(unfiltered_val)
            filtered_jobconf[key] = filtered_val

        if self.SORT_VALUES:
            filtered_jobconf = combine_dicts(
                _SORT_VALUES_JOBCONF, filtered_jobconf)

        return filtered_jobconf

    ### Secondary Sort ###

    #: Set this to ``True`` if you would like reducers to receive the values
    #: associated with any key in sorted order (sorted by their *encoded*
    #: value). Also known as secondary sort.
    #:
    #: This can be useful if you expect more values than you can fit in memory
    #: to be associated with one key, but you want to apply information in
    #: a small subset of these values to information in the other values.
    #: For example, you may want to convert counts to percentages, and to do
    #: this you first need to know the total count.
    #:
    #: Even though values are sorted by their encoded value, most encodings
    #: will sort strings in order. For example, you could have values like:
    #: ``['A', <total>]``, ``['B', <count_name>, <count>]``, and the value
    #: containing the total should come first regardless of what protocol
    #: you're using.
    #:
    #: See :py:meth:`jobconf()` and :py:meth:`partitioner()` for more about
    #: how this works.
    #:
    #: .. versionadded:: 0.4.1
    SORT_VALUES = None

    ### Testing ###

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

        :type protocol: protocol
        :param protocol: A protocol instance to use (e.g. JSONProtocol()),

        This only works in sandbox mode. This does not clear ``self.stdout``.
        """
        if self.stdout == sys.stdout:
            raise AssertionError('You must call sandbox() first;'
                                 ' parse_output() is for testing only.')

        if protocol is None:
            protocol = JSONProtocol()

        lines = StringIO(self.stdout.getvalue())
        return [protocol.read(line) for line in lines]


if __name__ == '__main__':
    MRJob.run()
