Writing jobs
============

This guide covers everything you need to know to write your job. You'll
probably need to flip between this guide and :doc:`runners` to find all the
information you need.

.. _writing-basics:

Defining steps
--------------

Your job will be defined in a file to be executed on your machine as a Python
script, as well as on a Hadoop cluster as an individual map, combine, or reduce
task. (See :ref:`how-your-program-is-run` for more on that.)

All dependencies must either be contained within the file, available on the
task nodes, or uploaded to the cluster by mrjob when your job is submitted.
(:doc:`runners` explains how to do those things.)

The following two sections are more reference-oriented versions of
:ref:`writing-your-first-job` and :ref:`writing-your-second-job`.

Single-step jobs
^^^^^^^^^^^^^^^^

The simplest way to write a one-step job is to subclass
:py:class:`~mrjob.job.MRJob` and override a few methods::

    from mrjob.job import MRJob
    import re

    WORD_RE = re.compile(r"[\w']+")


    class MRWordFreqCount(MRJob):

        def mapper(self, _, line):
            for word in WORD_RE.findall(line):
                yield word.lower(), 1

        def combiner(self, word, counts):
            yield word, sum(counts)

        def reducer(self, word, counts):
            yield word, sum(counts)


    if __name__ == '__main__':
        MRWordFreqCount.run()

(See :ref:`writing-your-first-job` for an explanation of this example.)

Here are all the methods you can override to write a one-step job. We'll
explain them over the course of this document.

.. _single-step-method-names:

    * :py:meth:`~mrjob.job.MRJob.mapper`
    * :py:meth:`~mrjob.job.MRJob.combiner`
    * :py:meth:`~mrjob.job.MRJob.reducer`
    * :py:meth:`~mrjob.job.MRJob.mapper_init`
    * :py:meth:`~mrjob.job.MRJob.combiner_init`
    * :py:meth:`~mrjob.job.MRJob.reducer_init`
    * :py:meth:`~mrjob.job.MRJob.mapper_final`
    * :py:meth:`~mrjob.job.MRJob.combiner_final`
    * :py:meth:`~mrjob.job.MRJob.reducer_final`
    * :py:meth:`~mrjob.job.MRJob.mapper_cmd`
    * :py:meth:`~mrjob.job.MRJob.combiner_cmd`
    * :py:meth:`~mrjob.job.MRJob.reducer_cmd`
    * :py:meth:`~mrjob.job.MRJob.mapper_pre_filter`
    * :py:meth:`~mrjob.job.MRJob.combiner_pre_filter`
    * :py:meth:`~mrjob.job.MRJob.reducer_pre_filter`

.. _writing-multi-step-jobs:

Multi-step jobs
^^^^^^^^^^^^^^^

To define multiple steps, override the :py:meth:`~mrjob.job.MRJob.steps`
method to return a list of :py:meth:`~mrjob.job.MRJob.mr` calls::

    from mrjob.job import MRJob
    import re

    WORD_RE = re.compile(r"[\w']+")


    class MRMostUsedWord(MRJob):

        def mapper_get_words(self, _, line):
            # yield each word in the line
            for word in WORD_RE.findall(line):
                yield (word.lower(), 1)

        def combiner_count_words(self, word, counts):
            # sum the words we've seen so far
            yield (word, sum(counts))

        def reducer_count_words(self, word, counts):
            # send all (num_occurrences, word) pairs to the same reducer.
            # num_occurrences is so we can easily use Python's max() function.
            yield None, (sum(counts), word)

        # discard the key; it is just None
        def reducer_find_max_word(self, _, word_count_pairs):
            # each item of word_count_pairs is (count, word),
            # so yielding one results in key=counts, value=word
            yield max(word_count_pairs)

        def steps(self):
            return [
                self.mr(mapper=self.mapper_get_words,
                        combiner=self.combiner_count_words,
                        reducer=self.reducer_count_words),
                self.mr(reducer=self.reducer_find_max_word)
            ]


    if __name__ == '__main__':
        MRMostUsedWord.run()

(This example is explained further in :ref:`job-protocols`.)

The keyword arguments accepted by :py:meth:`~mrjob.job.MRJob.mr` are the same
as the
:ref:`method names listed in the previous section <single-step-method-names>`,
plus a ``jobconf`` argument which takes a
dictionary of jobconf arguments to pass to Hadoop.

.. note::

    If this is your first time learning about mrjob, you should skip down to
    :ref:`job-protocols` and finish this section later.

Setup and teardown of tasks
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Remember from :ref:`how-your-program-is-run` that your script is invoked once
per task by Hadoop Streaming. It starts your script, feeds it stdin, reads its
stdout, and closes it. mrjob lets you write methods to run at the beginning and
end of this process: the :py:func:`*_init` and :py:func:`*_final` methods:

    * :py:meth:`~mrjob.job.MRJob.mapper_init`
    * :py:meth:`~mrjob.job.MRJob.combiner_init`
    * :py:meth:`~mrjob.job.MRJob.reducer_init`
    * :py:meth:`~mrjob.job.MRJob.mapper_final`
    * :py:meth:`~mrjob.job.MRJob.combiner_final`
    * :py:meth:`~mrjob.job.MRJob.reducer_final`

(And the corresponding keyword arguments to :py:meth:`~mrjob.job.MRJob.mr`.)

If you need to load some kind of support file, like a :py:mod:`sqlite3`
database, or perhaps create a temporary file, you can use these methods to do
so. (See :ref:`writing-file-options` for an example.)

:py:func:`*_init` and :py:func:`*_final` methods can yield values just like
normal tasks. Here is our word frequency count example rewritten to use
these methods::

    class MRWordFreqCount(MRJob):

        def init_get_words(self):
            self.words = {}

        def get_words(self, _, line):
            for word in WORD_RE.findall(line):
                word = word.lower()
                self.words.setdefault(word, 0)
                self.words[word] = self.words[word] + 1

        def final_get_words(self):
            for word, val in self.words.iteritems():
                yield word, val

        def sum_words(self, word, counts):
            yield word, sum(counts)

        def steps(self):
            return [self.mr(mapper_init=self.init_get_words,
                            mapper=self.get_words,
                            mapper_final=self.final_get_words,
                            combiner=self.sum_words,
                            reducer=self.sum_words)]

In this version, instead of yielding one line per word, the mapper keeps an
internal count of word occurrences across all lines this mapper has seen so
far. The mapper itself yields nothing. When Hadoop Streaming stops sending data
to the map task, mrjob calls :py:func:`final_get_words()`. That function emits
the totals for this task, which is a much smaller set of output lines than the
mapper would have output.

The optimization above is similar to using :term:`combiners <combiner>`,
demonstrated in :ref:`writing-multi-step-jobs`. It is usually clearer to use a
combiner rather than a custom data structure, and Hadoop may run combiners in
more places than just the ends of tasks.

:ref:`writing-cl-opts` has a partial example that shows how to load a
:py:mod:`sqlite3` database using :py:meth:`~mrjob.job.MRJob.mapper_init`.

.. _cmd-steps:

Shell commands as steps
^^^^^^^^^^^^^^^^^^^^^^^

You can forego scripts entirely for a step by specifying it as shell a command.
To do so, use ``mapper_cmd``, ``combiner_cmd``, or ``reducer_cmd`` as arguments
to :py:meth:`~mrjob.job.MRJob.mr()` or methods on :py:class:`~mrjob.job.MRJob`.
(See :py:meth:`~mrjob.job.MRJob.mapper_cmd`,
:py:meth:`~mrjob.job.MRJob.combiner_cmd`, and
:py:meth:`~mrjob.job.MRJob.reducer_cmd`.)

.. warning::

    The ``inline`` runner does not support :py:func:`*_cmd`. If you want to
    test locally, use the ``local`` runner (``-r local``).

You may mix command and script steps at will. This job will count the number of
lines containing the string "kitty"::

    from mrjob.job import job


    class KittyJob(MRJob):

        OUTPUT_PROTOCOL = JSONValueProtocol

        def mapper_cmd(self):
            return "grep kitty"

        def reducer(self, key, values):
            yield None, sum(1 for _ in values)


    if __name__ == '__main__':
        KittyJob().run()

Step commands are run without a shell. But if you'd like to use shell features
such as pipes, you can use :py:func:`mrjob.util.bash_wrap()` to wrap your
command in a call to ``bash``.

::

    from mrjob.util import bash_wrap

    class DemoJob(MRJob):

        def mapper_cmd(self):
            return bash_wrap("grep 'blah blah' | wc -l")

.. note::

    You may not use :py:func:`*_cmd` with any other options for a task such as
    :py:func:`*_filter`, :py:func:`*_init`, :py:func:`*_final`, or a regular
    mapper/combiner/reducer function.

.. note::

    You might see an opportunity here to write your MapReduce code in whatever
    language you please. If that appeals to you, check out
    :mrjob-opt:`upload_files` for another piece of the puzzle.

.. _cmd-filters:

Filtering task input with shell commands
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can specify a command to filter a task's input before it reaches your task
using the ``mapper_pre_filter`` and ``reducer_pre_filter`` arguments to
:py:meth:`~mrjob.job.MRJob.mr()` or methods on :py:class:`~mrjob.job.MRJob`.
Doing so will cause mrjob to pipe input through that comand before it reaches
your mapper.

.. warning::

    The ``inline`` runner does not support :py:func:`*_cmd`. If you want to
    test locally, use the ``local`` runner (``-r local``).

Here's a job that tests filters using :command:`grep`::

    from mrjob.job import MRJob
    from mrjob.protocol import JSONValueProtocol


    class KittiesJob(MRJob):

        OUTPUT_PROTOCOL = JSONValueProtocol

        def test_for_kitty(self, _, value):
            yield None, 0  # make sure we have some output
            if 'kitty' not in value:
                yield None, 1

        def sum_missing_kitties(self, _, values):
            yield None, sum(values)

        def steps(self):
            return [
                self.mr(mapper_pre_filter='grep "kitty"',
                        mapper=self.test_for_kitty,
                        reducer=self.sum_missing_kitties)]


    if __name__ == '__main__':
        KittiesJob().run()

The output of the job should always be ``0``, since every line that gets to
:py:func:`test_for_kitty()` is filtered by :command:`grep` to have "kitty" in
it.

Filter commands are run without a shell. But if you'd like to use shell
features such as pipes, you can use :py:func:`mrjob.util.bash_wrap()` to wrap
your command in a call to ``bash``. See :ref:`cmd-filters` for an example of
:py:func:`mrjob.util.bash_wrap()`.

.. _non-hadoop-streaming-jar-steps:

Jar steps
^^^^^^^^^

You can ignore Hadoop Streaming entirely for a step by using
:py:meth:`~mrjob.job.MRJob.jar()` instead of :py:meth:`~mrjob.job.MRJob.mr()` .
For example, on EMR you can use a jar to run a script::

    class ScriptyJarJob(MRJob):

        def steps(self):
            return [self.jar(
                name='run a script',
                jar='s3://elasticmapreduce/libs/script-runner/script-runner.jar',
                step_args=['s3://my_bucket/my_script.sh'])]

.. _job-protocols:

Protocols
---------

mrjob assumes that all data is newline-delimited bytes. It automatically
serializes and deserializes these bytes using :term:`protocols <protocol>`.
Each job has an :term:`input protocol`, an :term:`output protocol`, and an
:term:`internal protocol`.

A protocol has a :py:func:`read()` method and a :py:func:`write()` method. The
:py:func:`read()` method converts bytes to pairs of Python objects representing
the keys and values. The :py:func:`write()` method converts a pair of Python
objects back to bytes.

The :term:`input protocol` is used to read the bytes sent to the first mapper
(or reducer, if your first step doesn't use a mapper). The :term:`output
protocol` is used to write the output of the last step to bytes written to the
output file. The :term:`internal protocol` converts the output of one step to
the input of the next if the job has more than one step.

You can specify which protocols your job uses like this::

    class MyMRJob(mrjob.job.MRJob):

        # these are the defaults
        INPUT_PROTOCOL = mrjob.protocol.RawValueProtocol
        INTERNAL_PROTOCOL = mrjob.protocol.JSONProtocol
        OUTPUT_PROTOCOL = mrjob.protocol.JSONProtocol

The default input protocol is |RawValueProtocol|, which reads and writes lines
of raw text with no key. So by default, the first step in your job sees
``(None, <text of the line>)`` for each line of input.

The default output and internal protocols are both |JSONProtocol|, which reads
and writes JSON strings separated by a tab character. (Hadoop Streaming uses
the tab character to separate keys and values within one line when it sorts
your data [#hc]_.)

If your head hurts a bit, think of it this way: use |RawValueProtocol| when you
want to read or write lines of raw text. Use |JSONProtocol| when you want to
read or write key-value pairs where the key and value are JSON-enoded bytes.

.. note::

    Hadoop Streaming does not understand JSON, or mrjob protocols. It simply
    groups lines by doing a string comparison on the keys.

Here are all the protocols mrjob includes:

* :py:class:`~mrjob.protocol.JSONProtocol` /
  :py:class:`~mrjob.protocol.JSONValueProtocol`: JSON
* :py:class:`~mrjob.protocol.PickleProtocol` /
  :py:class:`~mrjob.protocol.PickleValueProtocol`: pickle
* :py:class:`~mrjob.protocol.RawProtocol` /
  :py:class:`~mrjob.protocol.RawValueProtocol`: raw string
* :py:class:`~mrjob.protocol.ReprProtocol` /
  :py:class:`~mrjob.protocol.ReprValueProtocol`: serialize with ``repr()``,
  deserialize with :py:func:`mrjob.util.safeeval`

The :py:class:`*ValueProtocol` protocols assume the input lines don't have
keys, and don't write a key as output.

.. rubric:: Footnotes

.. [#hc] This behavior is configurable, but there is currently no
    mrjob-specific documentation. `Gitub pull requests
    <http://www.github.com/yelp/mrjob>`_ are always
    appreciated.

Data flow walkthrough by example
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Let's revisit our example from :ref:`writing-multi-step-jobs`. It has two
steps and takes a plain text file as input.

::

    class MRMostUsedWord(MRJob):

        def steps(self):
            return [
                self.mr(mapper=self.mapper_get_words,
                        combiner=self.combiner_count_words,
                        reducer=self.reducer_count_words),
                self.mr(reducer=self.reducer_find_max_word)
            ]

The first step starts with :py:func:`mapper_get_words()`::

        def mapper_get_words(self, _, line):
            # yield each word in the line
            for word in WORD_RE.findall(line):
                yield (word.lower(), 1)

Since the input protocol is |RawValueProtocol|, the key will always be ``None``
and the value will be the text of the line.

The function discards the key and yields ``(word, 1)`` for each word in the
line. Since the internal protocol is |JSONProtocol|, each component of the
output is serialized to JSON. The serialized components are written to stdout
separated by a tab character and ending in a newline character, like this::

    "mrjob" 1
    "is"    1
    "a" 1
    "python"    1

The next two parts of the step are the combiner and reducer::

        def combiner_count_words(self, word, counts):
            # sum the words we've seen so far
            yield (word, sum(counts))

        def reducer_count_words(self, word, counts):
            # send all (num_occurrences, word) pairs to the same reducer.
            # num_occurrences is so we can easily use Python's max() function.
            yield None, (sum(counts), word)

In both cases, bytes are deserialized into ``(word, counts)`` by
|JSONProtocol|, and the output is serialized as JSON in the same way (because
both are followed by another step). It looks just like the first mapper output,
but the results are summed::

    "mrjob" 31
    "is"    2
    "a" 2
    "Python"    1

The final step is just a reducer::

        # discard the key; it is just None
        def reducer_find_max_word(self, _, word_count_pairs):
            # each item of word_count_pairs is (count, word),
            # so yielding one results in key=counts, value=word
            yield max(word_count_pairs)

Since all input to this step has the same key (``None``), a single task will
get all rows. Again, |JSONProtocol| will handle deserialization and produce the
arguments to :py:func:`reducer_find_max_word()`.

The output protocol is also |JSONProtocol|, so the final output will be::

    31  "mrjob"

And we're done! But that's a bit ugly; there's no need to write the key out at
all. Let's use :py:class:`~mrjob.protocol.JSONValueProtocol` instead, so we
only see the JSON-encoded value::

    class MRMostUsedWord(MRJob):

        OUTPUT_PROTOCOL = JSONValueProtocol

Now we should have code that is identical to
:file:`examples/mr_most_used_word.py` in mrjob's source code. Let's try running
it (``-q`` prevents debug logging)::

    $ python mr_most_used_word.py README.txt -q
    "mrjob"

Hooray!

Specifying protocols for your job
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Usually, you'll just want to set one or more of the class variables
:py:attr:`~mrjob.job.MRJob.INPUT_PROTOCOL`,
:py:attr:`~mrjob.job.MRJob.INTERNAL_PROTOCOL`, and
:py:attr:`~mrjob.job.MRJob.OUTPUT_PROTOCOL`::

    class BasicProtocolJob(MRJob):

        # get input as raw strings
        INPUT_PROTOCOL = RawValueProtocol
        # pass data internally with pickle
        INTERNAL_PROTOCOL = PickleProtocol
        # write output as JSON
        OUTPUT_PROTOCOL = JSONProtocol

If you need more complex behavior, you can override
:py:meth:`~mrjob.job.MRJob.input_protocol`,
:py:meth:`~mrjob.job.MRJob.internal_protocol`, or
:py:meth:`~mrjob.job.MRJob.output_protocol` and return a protocol object
instance. Here's an example that sneaks a peek at :ref:`writing-cl-opts`::

    class CommandLineProtocolJob(MRJob):

        def configure_options(self):
            super(CommandLineProtocolJob, self).configure_options()
            self.add_passthrough_option(
                '--output-format', default='raw', choices=['raw', 'json'],
                help="Specify the output format of the job")

        def output_protocol(self):
            if self.options.output_format == 'json':
                return JSONValueProtocol()
            elif self.options.output_format == 'raw':
                return RawValueProtocol()

Finally, if you need to use a completely different concept of protocol
assignment, you can override :py:meth:`~mrjob.job.MRJob.pick_protocols`::

    class WhatIsThisIDontEvenProtocolJob(MRJob):

        def pick_protocols(self, step_num, step_type):
            return random.choice([Protocololol, ROFLcol, Trolltocol, Locotorp])

.. _writing-protocols:

Writing custom protocols
^^^^^^^^^^^^^^^^^^^^^^^^

A protocol is an object with methods ``read(self, line)`` and ``write(self,
key, value)``. The ``read()`` method takes a string and returns a 2-tuple of
decoded objects, and ``write()`` takes the key and value and returns the line
to be passed back to Hadoop Streaming or as output.

Here is a simplified version of mrjob's JSON protocol::

    import json


    class JSONProtocol(object):

        def read(self, line):
            k_str, v_str = line.split('\t', 1)
            return json.loads(k_str), json.loads(v_str)

        def write(self, key, value):
            return '%s\t%s' % (json.dumps(key), json.dumps(value))

You can improve performance significantly by caching the
serialization/deserialization results of keys. Look at the source code of
:py:mod:`mrjob.protocol` for an example.

.. _writing-cl-opts:

Defining command line options
-----------------------------

Recall from :ref:`how-your-program-is-run` that your script is executed in
several contexts: once for the initial invokation, and once for each task. If
you just add an option to your job's option parser, that option's value won't
be propagated to other runs of your script. Instead, you can use mrjob's option
API: :py:meth:`~mrjob.job.MRJob.add_passthrough_option` and
:py:meth:`~mrjob.job.MRJob.add_file_option`.

Passthrough options
^^^^^^^^^^^^^^^^^^^

A :dfn:`passthrough option` is an :py:mod:`optparse` option that mrjob is aware
of. mrjob inspects the value of the option when you invoke your script [#popt]_
and reproduces that value when it invokes your script in other contexts. The
command line-switchable protocol example from before uses this feature::

    class CommandLineProtocolJob(MRJob):

        def configure_options(self):
            super(CommandLineProtocolJob, self).configure_options()
            self.add_passthrough_option(
                '--output-format', default='raw', choices=['raw', 'json'],
                help="Specify the output format of the job")

        def output_protocol(self):
            if self.options.output_format == 'json':
                return JSONValueProtocol()
            elif self.options.output_format == 'raw':
                return RawValueProtocol()

When you run your script with ``--output-format=json``, mrjob detects that you
passed ``--output-format`` on the command line. When your script is run in any
other context, such as on Hadoop, it adds ``--output-format=json`` to its
command string.

:py:meth:`~mrjob.job.MRJob.add_passthrough_option` takes the same arguments as
:py:meth:`optparse.OptionParser.add_option`. For more information, see the
`optparse docs`_.

.. _`optparse docs`: http://docs.python.org/library/optparse.html

.. rubric:: Footnotes

.. [#popt] This is accomplished using crazy :py:mod:`optparse` hacks so you
    don't need to limit yourself to certain option types. However, your default
    values need to be compatible with :py:func:`copy.deepcopy`.

.. _writing-file-options:

File options
^^^^^^^^^^^^

A :dfn:`file option` is like a passthrough option, but:

1. Its value must be a string or list of strings (``action="store"`` or
   ``action="append"``), where each string represents either a local path, or
   an HDFS or S3 path that will be accessible from the task nodes.
2. That file will be downloaded to each task's local directory and the value of
   the option will magically be changed to its path.

For example, if you had a map task that required a :py:mod:`sqlite3` database,
you could do this::

    class SqliteJob(MRJob):

        def configure_options(self):
            super(CommandLineProtocolJob, self).configure_options()
            self.add_file_option('--database')

        def mapper_init(self):
            # make sqlite3 database available to mapper
            self.sqlite_conn = sqlite3.connect(self.options.database)

You could call it any of these ways, depending on where the file is::

    $ python sqlite_job.py -r local  --database=/etc/my_db.sqlite3
    $ python sqlite_job.py -r hadoop --database=/etc/my_db.sqlite3
    $ python sqlite_job.py -r hadoop --database=hdfs://my_dir/my_db.sqlite3
    $ python sqlite_job.py -r emr    --database=/etc/my_db.sqlite3
    $ python sqlite_job.py -r emr    --database=s3://my_bucket/my_db.sqlite3

In any of these cases, when your task runs, :file:`my_db.sqlite3` will always
be available in the task's working directory, and the value of
``self.options.database`` will always be set to its path.

See :ref:`configs-making-files-available` if you want to upload a file to your
tasks' working directories without writing a custom command line option.

.. warning::

    You **must** wait to read files until **after class initialization**. That
    means you should use the :ref:`*_init() <single-step-method-names>` methods
    to read files. Trying to read files into class variables will not work.

.. _custom-options:

Custom option types
^^^^^^^^^^^^^^^^^^^

:py:mod:`optparse` allows you to add custom types and actions to your options
(see `Extending optparse`_), but doing so requires passing a custom
:py:class:`Option` object into the :py:class:`~optparse.OptionParser`
constructor.  mrjob creates its own :py:class:`~optparse.OptionParser` object,
so if you want to use a custom :py:class:`~optparse.Option` class, you'll need
to set the :py:attr:`~mrjob.job.MRJob.OPTION_CLASS` attribute.

::

    import optparse

    import mrjob


    class MyOption(optparse.Option):
        pass    # extend optparse as documented by the Python standard library


    class MyJob(mrjob.job.MRJob):

        OPTION_CLASS = MyOption

.. _Extending optparse:
    http://docs.python.org/library/optparse.html#extending-optparse

Counters
--------

Hadoop lets you track :dfn:`counters` that are aggregated over a step. A
counter has a group, a name, and an integer value. Hadoop itself tracks a few
counters automatically. mrjob prints your job's counters to the command line
when your job finishes, and they are available to the runner object if you
invoke it programmatically.

To increment a counter from anywhere in your job, use the
:py:meth:`~mrjob.job.MRJob.increment_counter` method::

    class MRCountingJob(MRJob):

        def mapper(self, _, value):
            self.increment_counter('group', 'counter_name', 1)
            yield _, value

At the end of your job, you'll get the counter's total value::

    group:
        counter_name: 1

.. aliases

.. |JSONProtocol| replace:: :py:class:`~mrjob.protocol.JSONProtocol`
.. |RawValueProtocol| replace:: :py:class:`~mrjob.protocol.RawValueProtocol`
