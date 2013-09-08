Concepts
========

MapReduce and Apache Hadoop
---------------------------

*This section uses text from Apache's* `MapReduce Tutorial`_.

.. _`MapReduce Tutorial`: http://hadoop.apache.org/common/docs/current/mapred_tutorial.html

MapReduce is a way of writing programs designed for processing vast amounts of
data, and a system for running those programs in a distributed and
fault-tolerant way. `Apache Hadoop`_ is one such system designed primarily to
run Java code.

.. _`Apache Hadoop`: http://hadoop.apache.org/

A MapReduce job usually splits the input data-set into independent chunks which
are processed by the map tasks in a completely parallel manner. The framework
sorts the outputs of the maps, which are then input to the reduce tasks.
Typically both the input and the output of the job are stored in a file system
shared by all processing nodes. The framework takes care of scheduling tasks,
monitoring them, and re-executing the failed tasks.

The MapReduce framework consists of a single master "job tracker" and one slave
"task tracker" per cluster-node. The master is responsible for scheduling the
jobs' component tasks on the slaves, monitoring them and re-executing the
failed tasks. The slaves execute the tasks as directed by the master.

As the job author, you write :term:`map <mapper>`, :term:`combine <combiner>`,
and :term:`reduce <reducer>` functions that are submitted to the job tracker
for execution.

A :term:`mapper` takes a single key and value as input, and returns zero or
more (key, value) pairs. The pairs from all map outputs of a single step are
grouped by key.

A :term:`combiner` takes a key and a subset of the values for that key as input
and returns zero or more (key, value) pairs. Combiners are optimizations that
run immediately after each mapper and can be used to decrease total data
transfer.  Combiners should be idempotent (produce the same output if run
multiple times in the job pipeline).

A :term:`reducer` takes a key and the complete set of values for that key in
the current step, and returns zero or more arbitrary (key, value) pairs as
output.

After the reducer has run, if there are more steps, the individual results are
arbitrarily assigned to mappers for further processing. If there are no more
steps, the results are sorted and made available for reading.

An example
^^^^^^^^^^

Consider a program that counts how many times words occur in a document. Here
is some input::

    The wheels on the bus go round and round,
    round and round, round and round
    The wheels on the bus go round and round,
    all through the town.

The inputs to the mapper will be (``None``, :samp:`"{one line of text}"`). (The
key is ``None`` because the input is just raw text.)

The mapper converts the line to lowercase, removes punctuation, splits it on
whitespace, and outputs ``(word, 1)`` for each item.

::

    mapper input: (None, "The wheels on the bus go round and round,")
    mapper output:
        "the", 1
        "wheels", 1
        "on", 1
        "the", 1
        "bus", 1
        "go", 1
        "round", 1
        "and", 1
        "round", 1

Each call to the combiner gets a word as the key and a list of 1s as the
value. It sums the 1s and outputs the original key and the sum.

::

    combiner input: ("the", [1, 1])
    combiner output:
        "the", 2

The reducer is identical to the combiner; for each key, it simply outputs the
original key and the sum of the values.

::

    reducer input: ("round", [2, 4, 2])
    reducer output:
        "round", 8 

The final output is collected::

    "all", 1
    "and", 4
    "bus", 2
    "go", 2
    "on", 2
    "round", 8
    "the", 5
    "through", 1
    "town", 1
    "wheels", 2

Your algorithm may require several repetitions of this process.

.. _hadoop-streaming-and-mrjob:

Hadoop Streaming and mrjob
--------------------------

.. note::

    If this is your first exposure to MapReduce or Hadoop, you may want to skip
    this section and come back later. Feel free to stick with it if you feel
    adventurous.

Although Hadoop is primarly designed to work with Java code, it supports other
languages via :term:`Hadoop Streaming`. This jar opens a subprocess to your
code, sends it input via stdin, and gathers results via stdout.

In most cases, the input to a Hadoop Streaming job is a set of
newline-delimited files. Each line of input is passed to your mapper, which
outputs key-value pairs expressed as two strings separated by a tab and ending
with a newline, like this::

    key1\tvalue1\nkey2\nvalue2\n

Hadoop then sorts the output lines by key (the line up to the
first tab character) and passes the sorted lines to the appropriate combiners
or reducers.

mrjob is a framework that assists you in submitting your job to the Hadoop job
tracker and in running each individual step under Hadoop Streaming.

.. _how-your-program-is-run:

How your program is run
^^^^^^^^^^^^^^^^^^^^^^^

Depending on the way your script is invoked on the command line, it will behave
in different ways. You'll only ever use one of these; the rest are for mrjob
and Hadoop Streaming to use.

When you run with no arguments or with ``--runner``, you invoke mrjob's
machinery for running your job or submitting it to the cluster. We'll call it
*Process 1* to disambiguate it later. Your mappers and reducers are not called
in this process at all [#inl]_.

::

    $ python my_job.py -r hadoop input.txt  # run process 1

Within Process 1, mrjob will need to determine what the :term:`steps <step>` of
your project are. It does so by launching another subprocess of your job, this
time with the ``--steps`` argument, which we'll call Process 2::

    $ python my_job.py --steps              # run process 2
    [{"mapper": {"type": "script"},
      "reducer": {"type": "script"},
      "combiner": {"type": "script"},
      "type": "streaming"}]

mrjob now has all the information it needs to send the job to Hadoop [#inl2]_.
It does so. (At this time, please wave your hands in the air to represent
magic.)

mrjob has told Hadoop something like this:

* Run a step with Hadoop Streaming.
* The command for the mapper is ``python my_job.py --step-num=0 --mapper``.
* The command for the combiner is ``python my_job.py --step-num=0 --combiner``.
* The command for the reducer is ``python my_job.py --step-num=0 --reducer``.

When Hadoop distributes tasks among the task nodes, Hadoop Streaming will use
the appropriate command to process the data it is given. (We did not assign
numbers to the above commands because there might be anywhere from 1 to 10,000
processes running on Hadoop.)

You should now have a pretty good idea of the different environments in which
your job is run.

.. rubric:: Footnotes

.. [#inl] Unless you're using the ``inline`` runner, which is a special case
    for debugging.

.. [#inl2] Or run the job locally.
