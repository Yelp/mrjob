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

As the job author, you write **map**, **combine**, and **reduce** functions
that are submitted to the job tracker for execution.

A **mapper** takes a single key and value as input, and returns zero or more
arbitrary (key, value) pairs. The pairs from all map outputs of a single step
are grouped by key.

A **combiner** takes a key and a subset of the values for that key as input and
returns zero or more (key, value) pairs. Combiners are optimizations that run
immediately after each mapper and can be used to decrease total data transfer.
Combiners should be idempotent (produce the same output if run multiple times
in the job pipeline).

A **reducer** takes a key and the complete set of values for that key in the
current step, and returns zero or more arbitrary (key, value) pairs as output.

After the reducer has run, if there are more steps, the individual results are
arbitrarily assigned to mappers for further processing. If there are no more
steps, the results are sorted and made available for reading.

As an example, consider a program that counts how many times words occur in a
document. Each input item has the key ``None`` and the value is one line of the
input text file. Here is some input::

    The wheels on the bus go round and round,
    round and round, round and round
    The wheels on the bus go round and round,
    all through the town.

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

The final output is sorted::

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

Hadoop Streaming and mrjob
--------------------------

Although Hadoop is primarly designed to work with Java code, it supports other
languages via `Hadoop Streaming`_, a special jar which calls an arbitrary
program as a subprocess, passing input via stdin and gathering results via
stdout.

.. _`Hadoop Streaming`: http://hadoop.apache.org/common/docs/current/streaming.html

In most cases, the input to a Hadoop Streaming job is a set of
newline-delimited files. Each line of input is passed to an arbitrary mapper,
which outputs key-value pairs expressed as two strings separated by a tab and
ending with a newline. Hadoop then sorts the output lines by key (the line up
to the first tab character) and passes the sorted lines to the appropriate
combiners or reducers.

mrjob is a framework that assists you in both submitting your job to the Hadoop
job tracker and running each individual step under Hadoop Streaming. When you
run your job on the command line, depending on the options passed, mrjob will
either submit your job to be run, or immediately execute any mapper, combiner,
or reducer that you specify.
