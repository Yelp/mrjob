Spark
=====


.. _spark-auto-detection:

Auto-detecting Spark jobs
-------------------------

When you use Spark on EMR, mrjob can generally detect when you're trying to
use Spark and set up your cluster accordingly. This includes installing Spark
(if needed) and picking an instance type large enough to run it (see
:mrjob-opt:`instance_type`).

In case you're curious, here's how mrjob determines you're using Spark:

* any :py:class:`~mrjob.step.SparkStep` or
  :py:class:`~mrjob.step.SparkScriptStep` in your job's steps (including
  implicitly through the :py:class:`~mrjob.job.MRJob.spark` method)
* "Spark" included in :mrjob-opt:`emr_applications` option
* any bootstrap action (see :mrjob-opt:`bootstrap_actions`) ending in
  ``/spark-install`` (this is how you install Spark on 3.x AMIs)
