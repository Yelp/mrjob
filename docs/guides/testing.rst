.. _testing:

Testing jobs
============

mrjob can run jobs without the help of Hadoop. This isn't very efficient, but
it's a great way to test a job before submitting it to a cluster.

Inline runner
-------------

The ``inline`` runner (:py:class:`~mrjob.inline.InlineMRJobRunner`)
is the default runner for mrjob (it's what's used
when you run :command:`python mr_your_job.py <input>` without any ``-r``
option). It runs your job in a single process so that you get
faster feedback and simpler tracebacks.

Multiple splits
^^^^^^^^^^^^^^^

The ``inline`` runner doesn't run mappers or reducers concurrently, but it
does run at least two mappers and two reducers for each step. This can help
catch bad assumptions about the MapReduce programming model.

For example, say we wanted to write a simple script that counted the number
of lines of input:

.. code-block:: python

   from mrjob.job import MRJob

    class MRCountLinesWrong(MRJob):

        def mapper_init(self):
            self.num_lines = 0

        def mapper(self, _, line):
            self.num_lines += 1

        def mapper_final(self):
            yield None, self.num_lines


    if __name__ == '__main__':
        MRCountLinesWrong.run()

Looks good, but if we run it, we get more than one line count:

.. code-block:: sh

    $ python -m mrjob.examples.mr_count_lines_wrong README.rst 2> /dev/null
    null	77
    null	60

Aha! Because there can be more than one mapper! It's fine to use
:py:meth:`~mrjob.job.MRJob.mapper_final` like this, but we need to reduce on a
single key:

.. code-block:: python

    from mrjob.job import MRJob

    class MRCountLinesRight(MRJob):

        def mapper_init(self):
            self.num_lines = 0

        def mapper(self, _, line):
            self.num_lines += 1

        def mapper_final(self):
            yield None, self.num_lines

        def reducer(self, key, values):
            yield key, sum(values)


    if __name__ == '__main__':
        MRCountLinesRight.run()

.. code-block:: sh

    $ python -m mrjob.examples.mr_count_lines_right README.rst 2> /dev/null
    null	137

Thanks, inline runner!

Isolated working directories
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Just like Hadoop, the inline runner runs each mapper and reducer in its own
(temporary) working directory. It *does* add the original working directory
to :envvar:`$PYTHONPATH` so it can still access your local source tree.

Simulating jobconf
^^^^^^^^^^^^^^^^^^

The inline runner simulates jobconf variables/properties set by Hadoop (and
their Hadoop 1 equivalents):

    * ``mapreduce.job.cache.archives`` (``mapred.cache.archives``)
    * ``mapreduce.job.cache.files`` (``mapred.cache.files``)
    * ``mapreduce.job.cache.local.archives`` (``mapred.cache.localArchives``)
    * ``mapreduce.job.cache.local.files`` (``mapred.cache.localFiles``)
    * ``mapreduce.job.id`` (``mapred.job.id``)
    * ``mapreduce.job.local.dir`` (``job.local.dir``)
    * ``mapreduce.map.input.file`` (``map.input.file``)
    * ``mapreduce.map.input.length`` (``map.input.length``)
    * ``mapreduce.map.input.start`` (``map.input.start``)
    * ``mapreduce.task.attempt.id`` (``mapred.task.id``)
    * ``mapreduce.task.id`` (``mapred.tip.id``)
    * ``mapreduce.task.ismap`` (``mapred.task.is.map``)
    * ``mapreduce.task.output.dir`` (``mapred.work.output.dir``)
    * ``mapreduce.task.partition`` (``mapred.task.partition``)

You can use :py:func:`~mrjob.compat.jobconf_from_env` to read these from
your job's environment. For example:

.. code-block:: python

    from mrjob.compat import jobconf_from_env
    from mrjob.job import MRJob

    class MRCountLinesByFile(MRJob):

        def mapper(self, _, line):
            yield jobconf_from_env('mapreduce.map.input.file'), 1

        def reducer(self, path, ones):
            yield path, sum(ones)


    if __name__ == '__main__':
        MRCountLinesByFile.run()

.. code-block:: sh

    $ python -m mrjob.examples.mr_count_lines_by_file README.rst CHANGES.txt 2> /dev/null
    "CHANGES.txt"	564
    "README.rst"	137

If you only want to simulate jobconf variables from a single version of
Hadoop (for more stringent testing), you can set :mrjob-opt:`hadoop_version`.

Setting number of mappers and reducers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Want more or less splits? You can tell the inline runner the same way
you'd tell hadoop, with the ``mapreduce.job.maps`` and
``mapreduces.job.reduces`` :mrjob-opt:`jobconf` options:

.. code-block:: sh

    $ python -m mrjob.examples.mr_count_lines_wrong --jobconf mapreduce.job.maps=5 README.rst 2> /dev/null
    null	24
    null	33
    null	38
    null	30
    null	12

Local runnner
-------------

The ``local`` runner (:py:class:`~mrjob.local.LocalMRJobRunner`;
run using ``-r local``) supports
the above features, but, unlike the ``inline`` runner, it uses subprocesses.

This means it can be used to test options that don't make sense in a
single-process context, including:

 * :mrjob-opt:`python_bin`
 * :mrjob-opt:`setup`

The local runner *does* run multiple subprocesses concurrently, but it's
not really meant as a replacement for Hadoop; it's just for testing!

Anatomy of a test case
----------------------

So, you've gotten a job working. Great! Here's how you write a regression
test so that future developers won't break it.

For this example we'll use a
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

    from unittest import TestCase

    class MRInitTestCase(TestCase):

        def test_mapper(self):
            j = MRInitJob([])
            j.mapper_init()
            self.assertEqual(j.mapper(None, None).next(), (None, j.sum_amount))

To test the full job, you need to set up input, run the job, and check the
collected output. The most straightforward way to provide input is to use the
:py:meth:`~mrjob.job.MRJob.sandbox()` method. Create a :py:class:`~io.BytesIO`
object, populate it with data, initialize your job to read from stdin, and
enable the sandbox with your :py:class:`~io.BytesIO` as stdin.

You'll probably also want to specify ``--no-conf``
so options from your local ``mrjob.conf`` don't pollute your testing
environment.

This example reads from **stdin** (hence the ``-`` parameter)::

    from io import BytesIO

        def test_init_funcs(self):
            num_inputs = 2
            stdin = BytesIO(b'x\n' * num_inputs)
            mr_job = MRInitJob(['--no-conf'])
            mr_job.sandbox(stdin=stdin)

To run the job without leaving temp files on your system, use the
:py:meth:`~mrjob.job.MRJob.make_runner()` context manager.
:py:meth:`~mrjob.job.MRJob.make_runner()` creates the runner specified in the
command line arguments and ensures that job cleanup is performed regardless of
the success or failure of the job.

Run the job with :py:meth:`~mrjob.runner.MRJobRunner.run()`. The job's output
is available as a generator through
:py:meth:`~mrjob.runner.MRJobRunner.cat_output()` and can be parsed with
the job's output protocol using :py:meth:`~mrjob.job.MRJob.parse_output`::

   results = []
   with mr_job.make_runner() as runner:
       runner.run()
       for key, value in mrjob.parse_output(runner.cat_output()):
           results.append(value)

       # these numbers should match if mapper_init, reducer_init, and
       # combiner_init were called as expected
       self.assertEqual(sorted(results)[0], num_inputs * 10 * 10 * 2)


.. warning:: Do not let your tests depend on the input lines being processed in
             a certain order. Both mrjob and Hadoop divide input
             non-deterministically.
