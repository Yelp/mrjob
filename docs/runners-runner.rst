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
.. autodata:: mrjob.runner.CLEANUP_CHOICES
.. autodata:: mrjob.runner.CLEANUP_DEFAULT

Run Information
---------------

.. automethod:: MRJobRunner.counters
.. automethod:: MRJobRunner.get_hadoop_version

File management
---------------

.. autoattribute:: MRJobRunner.fs

.. py:module:: mrjob.fs.base

.. autoclass:: Filesystem
    :members:
