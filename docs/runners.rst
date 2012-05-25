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
:py:class:`~mrjob.inline.InlineJobRunner`,
:py:class:`~mrjob.local.LocalMRJobRunner`

Configuration
-------------

Runners are configured through keyword arguments to their init methods.

These can be set:

- from :py:mod:`mrjob.conf`
- from the command line
- by re-defining :py:meth:`~mrjob.job.MRJob.job_runner_kwargs` etc in your :py:class:`~mrjob.job.MRJob` (see :ref:`job-configuration`)
- by instantiating the runner directly
