mrjob.runner - base class for all runners
=========================================

.. py:module:: mrjob.runner

.. autoclass:: MRJobRunner

Runners' constructors take a bewildering array of keyword arguments; we'll
get to that in :doc:`configs-runners`

Running your job
----------------

.. automethod:: MRJobRunner.run
.. automethod:: MRJobRunner.stream_output
.. automethod:: MRJobRunner.cleanup
.. automethod:: MRJobRunner.get_hadoop_version
.. autodata:: mrjob.runner.CLEANUP_CHOICES
.. autodata:: mrjob.runner.CLEANUP_DEFAULT

File management
---------------

Some simple filesystem operations that work on both the local filesystem and
HDFS (when running :py:class:`~mrjob.hadoop.HadoopJobRunner`) or
S3 (when running :py:class:`~mrjob.emr.EMRJobRunner`).

Use ``hdfs://`` and ``s3://`` URIs to refer to remote files.

We don't currently support ``mv()`` and ``cp()`` because S3 doesn't really
have directories, so the semantics get a little weird.

.. automethod:: MRJobRunner.get_output_dir
.. automethod:: MRJobRunner.du
.. automethod:: MRJobRunner.ls
.. automethod:: MRJobRunner.cat
.. automethod:: MRJobRunner.mkdir
.. automethod:: MRJobRunner.path_exists
.. automethod:: MRJobRunner.path_join
.. automethod:: MRJobRunner.rm
.. automethod:: MRJobRunner.touchz
