Configuration options
=====================

Runners are configured through keyword arguments to their init methods.

These can be set:

- from :py:mod:`mrjob.conf`
- from the command line
- by re-defining `~mrjob.job.MRJob.job_runner_kwargs` etc in your `~mrjob.MRJob` (see :ref:`job-configuration`)
- by instantiating the runner directly

All runners
-----------

.. automethod:: mrjob.runner.MRJobRunner.__init__

Locally
-------

.. automethod:: mrjob.local.LocalMRJobRunner.__init__

On EMR
------

.. automethod:: mrjob.emr.EMRJobRunner.__init__

On your Hadoop cluster
----------------------

.. automethod:: mrjob.hadoop.HadoopJobRunner.__init__

