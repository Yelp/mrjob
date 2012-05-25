Runners - launching your job
============================

Runners are responsible for launching your job on Hadoop Streaming and
fetching the results.

Most of the time, you won't have any reason to construct a runner directly;
it's more like a utility that allows an :py:class:`~mrjob.job.MRJob`
to run itself. Normally things work something like this:

* Get a runner by calling :py:meth:`~mrjob.job.MRJob.make_runner` on your
  job
* Call :py:meth:`~mrjob.runner.MRJobRunner.run` on your runner. This will:

  * Run your job with :option:`--steps` to find out how many
    mappers/reducers to run
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

Running locally
---------------

To test the job locally, just run::

   python your_mr_job_sub_class.py < log_file_or_whatever > output

The script will automatically invoke itself to run the various steps,
using :py:class:`~mrjob.local.LocalMRJobRunner`.

You can also run individual steps:

.. code-block:: sh

    # test 1st step mapper:
    python your_mr_job_sub_class.py --mapper
    # test 2nd step reducer (--step-num=1 because step numbers are 0-indexed):
    python your_mr_job_sub_class.py --reducer --step-num=1

By default, we read from stdin, but you can also specify one or more
input files. It automatically decompresses .gz and .bz2 files::

    python your_mr_job_sub_class.py log_01.gz log_02.bz2 log_03

See :py:mod:`mrjob.examples` for more examples.

Running on EMR
--------------

* Set up your Amazon Account (see :ref:`amazon-setup`)
* Set :envvar:`AWS_ACCESS_KEY_ID` and :envvar:`AWS_SECRET_ACCESS_KEY`
* Run your job with ``-r emr``::

    python your_mr_job_sub_class.py -r emr < input > output

Running on your own Hadoop cluster
----------------------------------

* Set up a hadoop cluster (see http://hadoop.apache.org/common/docs/current/)
* If running Python 2.5 on your cluster, install the :py:mod:`simplejson` module on all nodes. (Recommended but not required for Python 2.6+)
* Make sure :envvar:`HADOOP_HOME` is set
* Run your job with ``-r hadoop``::

    python your_mr_job_sub_class.py -r hadoop < input > output

Configuration
-------------

Runners are configured through keyword arguments to their init methods.

These can be set:

- from :py:mod:`mrjob.conf`
- from the command line
- by re-defining :py:meth:`~mrjob.job.MRJob.job_runner_kwargs` etc in your :py:class:`~mrjob.job.MRJob` (see :ref:`job-configuration`)
- by instantiating the runner directly

.. _runners-programmatically:

Running your job programmatically
---------------------------------

Use :py:meth:`~mrjob.job.MRJob.make_runner` to run an
:py:class:`~mrjob.job.MRJob` from another Python script::

    from __future__ import with_statement # only needed on Python 2.5

    mr_job = MRWordCounter(args=['-r', 'emr'])
    with mr_job.make_runner() as runner:
        runner.run()
        for line in runner.stream_output():
            key, value = mr_job.parse_output_line(line)
            ... # do something with the parsed output
