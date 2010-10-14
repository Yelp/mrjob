Basic
=====

.. currentmodule:: mrjob.job

.. autoclass:: MRJob

One-step jobs
----------------
.. automethod:: MRJob.mapper
.. automethod:: MRJob.reducer
.. automethod:: MRJob.mapper_final

Running the job
---------------
.. automethod:: MRJob.run
.. automethod:: MRJob.__init__
.. automethod:: MRJob.make_runner

Parsing the output
------------------
.. automethod:: MRJob.parse_output_line

Multi-step jobs
---------------
.. automethod:: MRJob.steps
.. automethod:: MRJob.mr

Counters and status messages
----------------------------
.. automethod:: MRJob.increment_counter
.. automethod:: MRJob.set_status

