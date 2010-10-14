Advanced
========

.. currentmodule:: mrjob.job

.. _job-protocols:

Protocols
---------
.. autoattribute:: MRJob.DEFAULT_INPUT_PROTOCOL
.. autoattribute:: MRJob.DEFAULT_PROTOCOL
.. autoattribute:: MRJob.DEFAULT_OUTPUT_PROTOCOL
.. automethod:: MRJob.protocols
.. automethod:: MRJob.pick_protocols

Custom command-line options
---------------------------
.. automethod:: MRJob.configure_options
.. automethod:: MRJob.add_passthrough_option
.. automethod:: MRJob.add_file_option
.. automethod:: MRJob.load_options
.. automethod:: MRJob.is_mapper_or_reducer

.. _job-configuration:

Job runner configuration
------------------------
.. automethod:: MRJob.job_runner_kwargs
.. automethod:: MRJob.local_job_runner_kwargs
.. automethod:: MRJob.emr_job_runner_kwargs
.. automethod:: MRJob.hadoop_job_runner_kwargs
.. automethod:: MRJob.generate_passthrough_arguments
.. automethod:: MRJob.generate_file_upload_args
.. automethod:: MRJob.mr_job_script

How jobs are run
----------------
.. automethod:: MRJob.run_job
.. automethod:: MRJob.run_mapper
.. automethod:: MRJob.run_reducer
.. automethod:: MRJob.show_steps

