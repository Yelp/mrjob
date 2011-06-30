mrjob.job.MRJob - base class for all jobs
=========================================

.. py:module:: mrjob.job


Basic
-----

.. currentmodule:: mrjob.job

.. autoclass:: MRJob

One-step jobs
^^^^^^^^^^^^^^^^
.. automethod:: MRJob.mapper
.. automethod:: MRJob.reducer
.. automethod:: MRJob.mapper_final

Running the job
^^^^^^^^^^^^^^^
.. automethod:: MRJob.run
.. automethod:: MRJob.__init__
.. automethod:: MRJob.make_runner

Parsing the output
^^^^^^^^^^^^^^^^^^
.. automethod:: MRJob.parse_output_line

Multi-step jobs
^^^^^^^^^^^^^^^
.. automethod:: MRJob.steps
.. automethod:: MRJob.mr

Counters and status messages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: MRJob.increment_counter
.. automethod:: MRJob.set_status


Advanced
--------

.. _job-protocols:

Protocols
^^^^^^^^^

See :doc:`protocols` for a complete description of protocols.

.. autoattribute:: MRJob.DEFAULT_INPUT_PROTOCOL
.. autoattribute:: MRJob.DEFAULT_PROTOCOL
.. autoattribute:: MRJob.DEFAULT_OUTPUT_PROTOCOL
.. automethod:: MRJob.protocols
.. automethod:: MRJob.pick_protocols

Custom command-line options
^^^^^^^^^^^^^^^^^^^^^^^^^^^

See :doc:`configs-reference` for a complete list of all configuration options.

.. automethod:: MRJob.configure_options
.. automethod:: MRJob.add_passthrough_option
.. automethod:: MRJob.add_file_option
.. automethod:: MRJob.load_options
.. automethod:: MRJob.is_mapper_or_reducer

.. _job-configuration:

Job runner configuration
^^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: MRJob.job_runner_kwargs
.. automethod:: MRJob.local_job_runner_kwargs
.. automethod:: MRJob.emr_job_runner_kwargs
.. automethod:: MRJob.hadoop_job_runner_kwargs
.. automethod:: MRJob.generate_passthrough_arguments
.. automethod:: MRJob.generate_file_upload_args
.. automethod:: MRJob.mr_job_script

How jobs are run
^^^^^^^^^^^^^^^^
.. automethod:: MRJob.run_job
.. automethod:: MRJob.run_mapper
.. automethod:: MRJob.run_reducer
.. automethod:: MRJob.show_steps


Hooks for testing
-----------------

.. currentmodule:: mrjob.job

.. automethod:: MRJob.sandbox
.. automethod:: MRJob.parse_output
.. automethod:: MRJob.parse_counters
