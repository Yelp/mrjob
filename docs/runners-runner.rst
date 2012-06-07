mrjob.runner - base class for all runners
=========================================

.. py:module:: mrjob.runner

.. autoclass:: MRJobRunner

.. automethod:: mrjob.runner.MRJobRunner.__init__

Running your job
----------------

.. automethod:: MRJobRunner.run
.. automethod:: MRJobRunner.stream_output
.. automethod:: MRJobRunner.cleanup
.. autodata:: mrjob.runner.CLEANUP_CHOICES

Run Information
---------------

.. automethod:: MRJobRunner.counters
.. automethod:: MRJobRunner.get_hadoop_version

Configuration
-------------

.. automethod:: mrjob.runner.MRJobRunner.get_opts

File management
---------------

.. autoattribute:: MRJobRunner.fs

.. py:module:: mrjob.fs.base

.. autoclass:: Filesystem
    :members:
