Interactions between runner and job
===================================

.. warning:: This information is **experimentally public** and subject to
    change.

Starting with version 0.4, mrjob is moving toward supporting arbitrary
scripting languages for writing jobs. Jobs that don't use the
:py:class:`~mrjob.job.MRJob` Python class will need to support a simple
interface for informing the runner of their steps and running the correct
steps.

In this document, the **job script** is a file invoked with ``<interpreter>
script.blah``, which supports the interface described in this document and
contains all information about how to run the job. In the normal case, the job
script will be a file containing a single :py:class:`~mrjob.job.MRJob` class
and invocation, and ``<interpreter`` will be ``python``.

All interactions between job and runner are through command line arguments. For
example, to find out what mappers, reducers, and combiners a job has and what
their order is, :py:mod:`~mrjob.runner.MRJobRunner` calls the job script with
the :option:`--steps` argument.

Examples of job input/output are given at the end of this document in
:ref:`job-interface-examples`.

Job Interface
-------------

:option:`--steps`
    Print a JSON-encoded dictionary in the format described in
    :ref:`steps-format` describing the individual steps of the job.

:option:`--step-num`
    Specify the step number to be run. **Always** used with :option:`--mapper`,
    :option:`--combiner`, or :option:`--reducer`.

:option:`--mapper`
    Run the mapper for the specified step. Always used with
    :option:`--step-num`.

:option:`--combiner`
    Run the combiner for the specified step. Always used with
    :option:`--step-num`.

:option:`--reducer`
    Run the reducer for the specified step. Always used with
    :option:`--step-num`.

:option:`--step-num`, :option:`--mapper`, :option:`--combiner`, and
:option:`--reducer` are only necessary for ``script`` steps (see
:ref:`steps-format` below).

When running a mapper, combiner, or reducer, the non-option arguments are input
files, where no args or ``-`` means read from standard input.

.. _steps-format:

Format of ``--steps``
---------------------

Jobs are divided into **steps** which can either be a ``jar`` step or a
``streaming`` step.

Streaming steps
^^^^^^^^^^^^^^^

A ``streaming`` step consists of one or more **substeps** of type ``mapper``,
``combiner``, or ``reducer``. Each substep can have type ``script`` or
``command``. A ``script`` step follows the :option:`--step-num` /
:option:`--mapper` / :option:`--combiner` / :option:`--reducer` interface, and
a ``command`` is a raw command passed to Hadoop Streaming.

**Script substeps**

Here is a one-step streaming job with only a mapper in script format::

    {
        'type': 'streaming',
        'mapper': {
            'type': 'script',
        }
    }

Some Python code that would cause :py:class:`~mrjob.job.MRJob` generate this
data::

    class MRMapperJob(MRJob):

        def steps(self):
            return [MRStep(mapper=self.my_mapper)]

The runners would then invoke Hadoop Streaming with::

    -mapper 'mapper_job.py --mapper --step-num=0'

Script steps may have **pre-filters**, which are just UNIX commands that sit in
front of the script when running the step, used to efficiently filter output
with ``grep`` or otherwise filter and transform data. Filters are specified
using a ``pre_filter`` key in the substep dictionary::

    {
        'type': 'streaming',
        'mapper': {
            'type': 'script',
            'pre_filter': 'grep "specific data"'
        }
    }

:py:class:`~mrjob.job.MRJob` code::

    class MRMapperFilterJob(MRJob):

        def steps(self):
            return [MRStep(mapper=self.my_mapper,
                           mapper_pre_filter='grep "specific data"')]

Hadoop Streaming arguments::

-mapper 'bash -c '\''grep "specific data" | mapper_job.py --mapper --step-num=0'\'''

mrjob does not try to intelligently handle quotes in the contents of filters,
so avoid using single quotes.

Hadoop Streaming requires that all steps have a mapper, so if the job doesn't
specify a mapper, mrjob will use ``cat``.

**Command substeps**

The format for a command substep is very simple.

::

    {
        'type': 'streaming',
        'mapper': {
            'type': 'command',
            'command': 'cat'
        }
    }

:py:class:`~mrjob.job.MRJob` code::

    class MRMapperCommandJob(MRJob):

        def steps(self):
            return [MRStep(mapper_cmd='cat')]

Hadoop Streaming arguments::

    -mapper 'cat'

Jar steps
^^^^^^^^^

Jar steps are used to specify jars that are not Hadoop Streaming. They have two
required arguments and two optional arguments.

::

    {
        'type': 'jar',
        'jar': 'binks.jar.jar',
        'main_class': 'MyMainMan',      # optional
        'args': ['argh', 'argh']   # optional
    }

Further information on jar steps should be sought for in the Hadoop
documentation. Pull requests containing relevant links would be appreciated.

.. _job-interface-examples:

Examples
^^^^^^^^

**Getting steps**

Job with a script mapper and command reducer for the first step and a jar for
the second step::

    > <interpreter> my_script.lang --steps
    [
        {
            'type': 'streaming',
            'mapper': {
                'type': 'script'
            },
            'reducer': {
                'type': 'command',
                'command': 'some_shell_command --arg --arg'
            }
        },
        {
            'type': 'jar',
            'jar': 's3://bucket/jar_jar.jar'
        }
    ]

**Running a step**

::

    > <interpreter> my_script.lang --mapper --step-num=0 input.txt -
    [script iterates over stdin and input.txt]
    key_1	value_1
    key_2	value_2
    ...
