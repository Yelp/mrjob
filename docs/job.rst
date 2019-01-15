mrjob.job - defining your job
=============================

.. py:module:: mrjob.job

.. currentmodule:: mrjob.job

.. autoclass:: MRJob

.. _writing-one-step-jobs:

One-step jobs
-------------

.. automethod:: MRJob.mapper
.. automethod:: MRJob.reducer
.. automethod:: MRJob.combiner
.. automethod:: MRJob.mapper_init
.. automethod:: MRJob.mapper_final
.. automethod:: MRJob.reducer_init
.. automethod:: MRJob.reducer_final
.. automethod:: MRJob.combiner_init
.. automethod:: MRJob.combiner_final
.. automethod:: MRJob.mapper_cmd
.. automethod:: MRJob.reducer_cmd
.. automethod:: MRJob.combiner_cmd
.. automethod:: MRJob.mapper_pre_filter
.. automethod:: MRJob.reducer_pre_filter
.. automethod:: MRJob.combiner_pre_filter
.. automethod:: MRJob.mapper_raw
.. automethod:: MRJob.spark

Multi-step jobs
---------------

.. automethod:: MRJob.steps

Running the job
---------------

.. automethod:: MRJob.run
.. automethod:: MRJob.__init__
.. automethod:: MRJob.make_runner

Parsing output
--------------

.. automethod:: MRJob.parse_output
.. automethod:: MRJob.parse_output_line

Counters and status messages
----------------------------

.. automethod:: MRJob.increment_counter
.. automethod:: MRJob.set_status

Setting protocols
-----------------

.. autoattribute:: MRJob.INPUT_PROTOCOL
.. autoattribute:: MRJob.INTERNAL_PROTOCOL
.. autoattribute:: MRJob.OUTPUT_PROTOCOL
.. automethod:: MRJob.input_protocol
.. automethod:: MRJob.internal_protocol
.. automethod:: MRJob.output_protocol
.. automethod:: MRJob.pick_protocols

Secondary sort
--------------

.. autoattribute:: MRJob.SORT_VALUES

Command-line options
--------------------

See :ref:`writing-cl-opts` for information on adding command line options to
your job. See :doc:`guides/configs-reference` for a complete list of all
configuration options.

.. automethod:: MRJob.configure_args
.. automethod:: MRJob.add_passthru_arg
.. automethod:: MRJob.add_file_arg
.. automethod:: MRJob.pass_arg_through
.. automethod:: MRJob.load_args
.. automethod:: MRJob.is_task

.. _uploading-support-files:

Uploading support files
-----------------------

.. autoattribute:: MRJob.FILES
.. autoattribute:: MRJob.DIRS
.. autoattribute:: MRJob.ARCHIVES
.. automethod:: MRJob.files
.. automethod:: MRJob.dirs
.. automethod:: MRJob.archives

.. _job-configuration:

Job runner configuration
------------------------
.. automethod:: MRJob.mr_job_script

Running specific parts of jobs
------------------------------
.. automethod:: MRJob.run_job
.. automethod:: MRJob.run_mapper
.. automethod:: MRJob.map_pairs
.. automethod:: MRJob.run_reducer
.. automethod:: MRJob.reduce_pairs
.. automethod:: MRJob.run_combiner
.. automethod:: MRJob.combine_pairs
.. automethod:: MRJob.show_steps

.. _hadoop-config:

Hadoop configuration
--------------------
.. autoattribute:: MRJob.HADOOP_INPUT_FORMAT
.. automethod:: MRJob.hadoop_input_format
.. autoattribute:: MRJob.HADOOP_OUTPUT_FORMAT
.. automethod:: MRJob.hadoop_output_format
.. autoattribute:: MRJob.JOBCONF
.. automethod:: MRJob.jobconf
.. autoattribute:: MRJob.LIBJARS
.. automethod:: MRJob.libjars
.. autoattribute:: MRJob.PARTITIONER
.. automethod:: MRJob.partitioner

Hooks for testing
-----------------

.. currentmodule:: mrjob.job

.. automethod:: MRJob.sandbox
