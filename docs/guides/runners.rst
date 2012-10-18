Runners
=======

While the :py:class:`~mrjob.job.MRJob` class is the part of the framework that
handles the execution of your code in a MapReduce context, the **runner** is
the part that packages and submits your job to be run, and reporting the
results back to you.

In most cases, you will interact with runners via the command line and
configuration files. When you invoke mrjob via the command line, it reads your
command line options (the ``--runner`` parameter) to determine which type of
runner to create. Then it creates the runner, which reads your configuration
files and command line args and starts your job running in whatever context
you chose.

Most of the time, you won't have any reason to construct a runner directly.
Instead you'll invoke your Python script on the command line and it will make a
runner automatically, you'll call ``mrjob run my_script`` to have the ``mrjob``
command build a runner for your script (which may or may not be Python), or
you'll write some sort of wrapper that calls ``my_job.make_runner()``.

Internally, the general order of operations is:

* Get a runner by calling :py:meth:`~mrjob.job.MRJob.make_runner` on your job
* Call :py:meth:`~mrjob.runner.MRJobRunner.run` on your runner. This will:

  * Run your job with :option:`--steps` to find out how many mappers/reducers
    to run
  * Copy your job and supporting files to Hadoop
  * Instruct Hadoop to run your job with the appropriate
    :option:`--mapper`, :option:`--combiner`, :option:`--reducer`, and
    :option:`--step-num` arguments

Each runner runs a single job once; if you want to run a job multiple
times, make multiple runners.

Subclasses: :py:class:`~mrjob.emr.EMRJobRunner`,
:py:class:`~mrjob.hadoop.HadoopJobRunner`,
:py:class:`~mrjob.inline.InlineMRJobRunner`,
:py:class:`~mrjob.local.LocalMRJobRunner`

Testing locally
---------------

To test the job locally, just run::

   python your_mr_job_sub_class.py < log_file_or_whatever > output

The script will automatically invoke itself to run the various steps, using
:py:class:`~mrjob.local.InlineMRJobRunner` (``--runner=inline``). If you want
to simulate Hadoop more closely, you can use ``--runner=local``, which doesn't
add your working directory to the :envvar:`PYTHONPATH`, sets a few Hadoop
environment variables, and uses multiple subprocesses for tasks.

You can also run individual steps:

.. code-block:: sh

    # test 1st step mapper:
    python your_mr_job_sub_class.py --mapper
    # test 2nd step reducer (step numbers are 0-indexed):
    python your_mr_job_sub_class.py --reducer --step-num=1

By default, we read from stdin, but you can also specify one or more
input files. It automatically decompresses .gz and .bz2 files::

    python your_mr_job_sub_class.py log_01.gz log_02.bz2 log_03

See :py:mod:`mrjob.examples` for more examples.

Running on your own Hadoop cluster
----------------------------------

* Set up a hadoop cluster (see http://hadoop.apache.org/common/docs/current/)
* If running Python 2.5 on your cluster, install the :py:mod:`simplejson`
  module on all nodes (recommended but not required for Python 2.6+).
* Make sure :envvar:`HADOOP_HOME` is set
* Run your job with ``-r hadoop``::

    python your_mr_job_sub_class.py -r hadoop < input > output

Running on EMR
--------------

* Set up your Amazon account and credentials (see :ref:`amazon-setup`)
* Run your job with ``-r emr``::

    python your_mr_job_sub_class.py -r emr < input > output

Configuration
-------------

Runners are configured by several methods:

- from ``mrjob.conf`` (see :doc:`configs-basics`)
- from the command line
- by re-defining :py:meth:`~mrjob.job.MRJob.job_runner_kwargs` etc in your
  :py:class:`~mrjob.job.MRJob` (see :ref:`job-configuration`)
- by instantiating the runner directly

In most cases, you should put all configuration in ``mrjob.conf`` and use the
command line args or class variables to customize how individual jobs are run.

.. _runners-programmatically:

Running your job programmatically
---------------------------------

It is fairly common to write an organization-specific wrapper around mrjob. Use
:py:meth:`~mrjob.job.MRJob.make_runner` to run an :py:class:`~mrjob.job.MRJob`
from another Python script. The context manager guarantees that all temporary
files are cleaned up regardless of the success or failure of your job.

This pattern can also be used to write integration tests (see :doc:`testing`).

::

    from __future__ import with_statement # only needed on Python 2.5

    mr_job = MRWordCounter(args=['-r', 'emr'])
    with mr_job.make_runner() as runner:
        runner.run()
        for line in runner.stream_output():
            key, value = mr_job.parse_output_line(line)
            ... # do something with the parsed output

You the :py:class:`~mrjob.job.MRJob`, use a context manager to create the
runner, run the job, iterate over the output lines, and use the job instance to
parse each line with its output protocol.

Further reference:

* :py:meth:`~mrjob.job.MRJob.make_runner`
* :py:meth:`~mrjob.runner.MRJobRunner.stream_output`
* :py:meth:`~mrjob.job.MRJob.parse_output_line`

Counters
^^^^^^^^

Counters may be read through the
:py:meth:`~mrjob.runner.MRJobRunner.counters()` method on the runner. The
example below demonstrates the use of counters in a test case.

``mr_counting_job.py``
::

    from mrjob.job import MRJob


    class MRCountingJob(MRJob):

        def steps(self):
            # 3 steps so we can check behavior of counters for multiple steps
            return [self.mr(self.mapper),
                    self.mr(self.mapper),
                    self.mr(self.mapper)]

        def mapper(self, _, value):
            self.increment_counter('group', 'counter_name', 1)
            yield _, value


    if __name__ == '__main__':
        MRCountingJob.run()

``test_counters.py``
::

    from __future__ import with_statement

    try:
        import unittest2 as unittest
    except ImportError:
        import unittest

    from tests.mr_counting_job import MRCountingJob


    class CounterTestCase(unittest.TestCase):

        def test_counters(self):
            stdin = StringIO('foo\nbar\n')

            mr_job = MRCountingJob(['--no-conf', '-'])
            mr_job.sandbox(stdin=stdin)

            with mr_job.make_runner() as runner:
                runner.run()

                self.assertEqual(runner.counters(),
                                 [{'group': {'counter_name': 2}},
                                  {'group': {'counter_name': 2}},
                                  {'group': {'counter_name': 2}}])
