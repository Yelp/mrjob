Writing MRJobs
==============

.. _writing-basics:

Basics
------

Your job will be defined in a file to be executed on your machine as a Python
script, as well as on a Hadoop cluster as an individual map, combine, or reduce
task. All dependencies must either be contained within the file, made available
on the task nodes before the job is run, or uploaded to the cluster by mrjob
when your job is submitted.

The simplest way to write a job is by overriding :py:class:`~mrjob.job.MRJob`'s
:py:meth:`~mrjob.job.MRJob.mapper`, :py:meth:`~mrjob.job.MRJob.combiner`, and
:py:meth:`~mrjob.job.MRJob.reducer` methods::

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

The default configuration sends input lines to mappers via the value parameter
as a string object, with ``None`` for the key, so the mapper method above
discards the key and operates only on the value. The mapper yields ``(word,
1)`` for each word. The key and value are converted to `JSON`_ for transmission
between tasks and for final output.

.. _`JSON`: http://www.json.org/

The combiner and reducer get a word as the key and an iterator of numbers as
the value. They simply yield the word and the sum of the values.

The final output of the job is a set of lines where each line is a
tab-delimited key-value pair. Each key and value has been converted from its
Python representation to a JSON representation.

::

    "all"   1
    "and"   4
    "bus"   2
    ...

Many jobs require multiple steps. To define multiple steps, override the
:py:meth:`~mrjob.job.MRJob.steps` method::


    class MRDoubleWordFreqCount(MRJob):
        """Word frequency count job with an extra step to double all the
        values"""

        def get_words(self, _, line):
            for word in WORD_RE.findall(line):
                yield word.lower(), 1

        def sum_words(self, word, counts):
            yield word, sum(counts)

        def double_counts(self, word, counts):
            yield word, counts * 2

        def steps(self):
            return [self.mr(mapper=self.get_words,
                            combiner=self.sum_words,
                            reducer=self.sum_words),
                    self.mr(mapper=self.double_counts)]


You may wish to set up or tear down resources for each task. You can do so with
``init`` and ``final`` methods. For one-step jobs, you can override these:

    * :py:meth:`~mrjob.job.MRJob.mapper_init`
    * :py:meth:`~mrjob.job.MRJob.mapper_final`
    * :py:meth:`~mrjob.job.MRJob.combiner_init`
    * :py:meth:`~mrjob.job.MRJob.combiner_final`
    * :py:meth:`~mrjob.job.MRJob.reducer_init`
    * :py:meth:`~mrjob.job.MRJob.reducer_final`

For multi-step jobs, use keyword arguments to the :py:meth:`mrjob.job.MRJob.mr`
function.

``init`` and ``final`` methods can yield values just like normal tasks. Here is
our word frequency count example rewritten to use ``init`` and ``final``
methods::


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
internal count of word occurrences across *all lines this mapper has seen so
far, including multiple input lines.* When Hadoop Streaming stops sending data
to the map task, mrjob calls ``final_get_words()`` and it emits a much smaller
set of output lines.

.. _job-protocols:

Protocols
---------

Input and output goes to and from each task in the form of newline-delimited
bytes. Each line is separated into key and value by a tab character [#hc]_.

When sending lines between tasks, Hadoop Streaming compares and sorts keys
lexicographically, agnostic of encoding [#hc]_. mrjob is responsible for
serializing and deserializing lines to and from the Python objects that your
code operates on. Objects responsible for serializing and deserializing keys
and values from bytes to and from Python objects are called **protocols**.

The **input protocol** converts input lines into the key and value received by
the first task in the first step. Depending on what step components you have
defined this could be either a mapper or a reducer.

The **internal protocol** is used to convert lines for transmission between
tasks in between input and output.

The **output protocol** converts the objects yielded by the final step
component (mapper, combiner, or reducer) to the final output format to be sent
back to the output directory, stdout, etc.

Here are the default values::

    class MyMRJob(mrjob.job.MRJob):

        INPUT_PROTOCOL = mrjob.protocol.RawValueProtocol
        INTERNAL_PROTOCOL = mrjob.protocol.JSONProtocol
        OUTPUT_PROTOCOL = mrjob.protocol.JSONProtocol

The default input protocol, :py:class:`~mrjob.protocol.RawValueProtocol`,
passes the entire line of input as the value parameter to the mapper, with the
key as ``None``. The default internal and output protocols convert both the key
and the value to and from JSON.

Consider a job that must pass values between internal steps that are too
complex for JSON to handle. Such a job might look like this::

    class ComplicatedJob(MRJob):

        INTERNAL_PROTOCOL = mrjob.protocol.PickleProtocol

        def map_1(self, _, value):
            pass # do stuff, yield complicated objects

        def reduce_1(self, key, values):
            pass # do more stuff

        def reduce_2(self, key, values):
            pass # do even more stuff

        def steps(self):
            return [self.mr(mapper=self.map_1,
                            reducer=self.reduce_1),
                    self.mr(reducer=self.reduce_2)]

In this example, ``map_1()`` gets JSON-decoded values. Its output is serialized
and deserialized into ``reduce_1()`` using ``pickle``, and again when sent to
``reduce_2()``. The output keys and values of ``reduce_2()`` are serialized as
JSON.

Here is a complete list of built-in protocols. Classes named ``*ValueProtocol``
ignore the key. For serialization, the value is serialized and sent as the
entire line. For deserialization, the entire line is read as the value and the
key is set to ``None``.

* :py:class:`~mrjob.protocol.JSONProtocol` /
  :py:class:`~mrjob.protocol.JSONValueProtocol`: JSON
* :py:class:`~mrjob.protocol.PickleProtocol` /
  :py:class:`~mrjob.protocol.PickleValueProtocol`: pickle
* :py:class:`~mrjob.protocol.RawProtocol` /
  :py:class:`~mrjob.protocol.RawValueProtocol`: raw string
* :py:class:`~mrjob.protocol.ReprProtocol` /
  :py:class:`~mrjob.protocol.ReprValueProtocol`: serialize with ``repr()``,
  deserialize with :py:func:`mrjob.util.safeeval`

.. rubric:: Footnotes

.. [#hc] This behavior is configurable, but there is currently no
    mrjob-specific documentation. `Gitub pull requests
    <http://www.github.com/yelp/mrjob>`_ are always
    appreciated.

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
instance::

    class CommandLineProtocolJob(MRJob):

        def configure_options(self):
            super(CommandLineProtocolJob, self).configure_options()
            self.add_passthrough_option(
                '--input-format', default='raw', choices=['raw', 'json'])

        def input_protocol(self):
            if self.options.input_format == 'json':
                return JSONValueProtocol()
            elif self.options.input_format == 'raw':
                return RawValueProtocol()

Finally, if you need to use a completely different concept of protocol
assignment, you can override :py:meth:`mrjob.job.MRJob.pick_protocols`::

    class WhatIsThisIDontEvenProtocolJob(MRJob):

        def pick_protocols(self, step_num, step_type):
            # step_type is 'M', 'C', or 'R'
            return random.choice([Protocololol, ROFLcol, Trolltocol, Locotorp])

.. _writing-protocols:

Writing custom protocols
------------------------

A protocol is an object with methods ``read(self, line)`` and ``write(self,
key, value)``. The ``read()`` method takes a string and returns a 2-tuple of
decoded objects, and ``write()`` takes the key and value and returns the line
to be passed back to Hadoop Streaming or as output.

Here is an implementation of a YAML protocol::

    import yaml


    class YAMLProtocol(object):

        def read(self, line):
            k_str, v_str = line.split('\t', 1)
            return yaml.loads(k_str), yaml.loads(v_str)

        def write(self, key, value):
            return '%s\t%s' % (yaml.dumps(key), yaml.dumps(value))

You can improve performance by caching the serialization/deserialization
results of keys. Look at the source code of :py:mod:`mrjob.protocol` for an
example.

.. _writing-cl-opts:

Defining command line options
-----------------------------

