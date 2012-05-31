.. _testing:

Testing jobs
============


Inline vs local runner
----------------------

The ``inline`` runner is the default runner for mrjob 0.4 and later. It runs
your job in the same process as the runner so that you get faster feedback and
simpler tracebacks.

The ``local`` runner runs your job in subprocesses in another directory and
simulates several features of Hadoop, including:

* Multiple concurrent tasks
* ``mapreduce.job.cache.archives``
* ``mapreduce.job.cache.files``
* ``mapreduce.job.cache.local.archives``
* ``mapreduce.job.cache.local.files``
* ``mapreduce.job.id``
* ``mapreduce.job.local.dir``
* ``mapreduce.map.input.file``
* ``mapreduce.map.input.length``
* ``mapreduce.map.input.start``
* ``mapreduce.task.attempt.id``
* ``mapreduce.task.id``
* ``mapreduce.task.ismap``
* ``mapreduce.task.output.dir``
* ``mapreduce.task.partition``

If you specify *hadoop_version* <= 0.18, the simulated environment variables
will change to use the names corresponding with the older Hadoop version.

See :py:class:`~mrjob.local.LocalMRJobRunner` for reference about its behavior.

Anatomy of a test case
----------------------

mrjob's test cases use the :py:mod:`unittest2` module, which is available
for Python 2.3 and up. Most tests also require the :keyword:`with` statement.

::

    from __future__ import with_statement

    try:
        import unittest2 as unittest
    except ImportError:
        import unittest

You probably have your own job to test, but for this example we'll use a
test of the ``*_init()`` methods from the mrjob test cases::

    from mrjob.job import MRJob


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

Without using any mrjob features, we can write a simple test case to make
sure our methods are behaving as expected::

    class MRInitTestCase(unittest.TestCase):

        def test_mapper(self):
            j = MRInitJob()
            j.mapper_init()
            self.assertEqual(j.mapper(None, None).next(), (None, j.sum_amount))

To test the full job, you need to set up input, run the job, and check the
collected output. The most straightforward way to provide input is to use the
:py:meth:`~mrjob.job.MRJob.sandbox()` method. Create a :py:class:`StringIO`
object, populate it with data, initialize your job to read from stdin, and
enable the sandbox with your :py:class:`StringIO` as stdin.

The simplest way to test the full job is with the ``inline`` runner. It runs
the job in the same process as the test, so small jobs tend to run faster and
stack traces are simpler. You'll probably also want to specify ``--no-conf``
so options from your local ``mrjob.conf`` don't pollute your testing
environment.

This example reads from **stdin** (hence the ``-`` parameter)::

        def test_init_funcs(self):
            num_inputs = 2
            stdin = StringIO("x\n" * num_inputs)
            mr_job = MRInitJob(['-r', 'inline', '--no-conf', '-'])
            mr_job.sandbox(stdin=stdin)

To run the job without leaving temp files on your system, use the
:py:meth:`~mrjob.job.MRJob.make_runner()` context manager.
:py:meth:`~mrjob.job.MRJob.make_runner()` creates the runner specified in the
command line arguments and ensures that job cleanup is performed regardless of
the success or failure of the job.

Run the job with :py:meth:`~mrjob.runner.MRJobRunner.run()`. The output lines
are available as a generator through
:py:meth:`~mrjob.runner.MRJobRunner.stream_output()` and can be interpreted
through the job's output protocol with
:py:meth:`~mrjob.job.MRJob.parse_output_line()`. You may choose to collect
these lines in a list and check the contents of the list.

.. warning:: Do not let your tests depend on the input lines being processed in
    a certain order. Input is divided nondeterministically by the ``local``,
    ``hadoop``, and ``emr`` runners.

::

            results = []
            with mr_job.make_runner() as runner:
                runner.run()
                for line in runner.stream_output():
                    # Use the job's specified protocol to read the output
                    key, value = mr_job.parse_output_line(line)
                    results.append(value)

            # these numbers should match if mapper_init, reducer_init, and
            # combiner_init were called as expected
            self.assertEqual(results[0], num_inputs * 10 * 10 * 2)

You should be able to switch out the ``inline`` runner for the ``local`` runner
without changing any other code. The ``local`` runner will launch multiple
subprocesses to run your job, which may expose assumptions about input order
or race conditions.
