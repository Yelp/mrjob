mrjob.job.MRJob - base class for all jobs
=========================================

.. py:module:: mrjob.job


Basic
-----

.. currentmodule:: mrjob.job

.. autoclass:: MRJob

Writing one-step jobs
^^^^^^^^^^^^^^^^^^^^^
.. automethod:: MRJob.mapper
.. automethod:: MRJob.reducer
.. automethod:: MRJob.mapper_init
.. automethod:: MRJob.mapper_final
.. automethod:: MRJob.reducer_init
.. automethod:: MRJob.reducer_final

Writing multi-step jobs
^^^^^^^^^^^^^^^^^^^^^^^
.. automethod:: MRJob.steps
.. automethod:: MRJob.mr

Running the job
^^^^^^^^^^^^^^^
.. automethod:: MRJob.run
.. automethod:: MRJob.__init__
.. automethod:: MRJob.make_runner

Parsing the output
^^^^^^^^^^^^^^^^^^
.. automethod:: MRJob.parse_output_line

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

.. autoattribute:: MRJob.INPUT_PROTOCOL
.. autoattribute:: MRJob.PROTOCOL
.. autoattribute:: MRJob.OUTPUT_PROTOCOL
.. autoattribute:: MRJob.DEFAULT_INPUT_PROTOCOL
.. autoattribute:: MRJob.DEFAULT_PROTOCOL
.. autoattribute:: MRJob.DEFAULT_OUTPUT_PROTOCOL
.. automethod:: MRJob.protocols
.. automethod:: MRJob.pick_protocols

Custom Protocols
^^^^^^^^^^^^^^^^

A protocol is a subclass of :py:class:`HadoopStreamingProtocol` with class
methods ``read(cls, line)`` and ``write(cls, key, value)``. The ``read(line)``
method takes a string and returns a 2-tuple of decoded objects, and
``write(cls, key, value)`` takes the key and value and returns the line to be
passed back to Hadoop Streaming or as output. To use a protocol, import it from
:py:mod:`mrjob.protocols` and assign :py:attr:`MRJob.INPUT_PROTOCOL`, 
:py:attr:`MRJob.PROTOCOL`, or :py:attr:`MRJob.OUTPUT_PROTOCOL` as
appropriate.

Custom command-line options
^^^^^^^^^^^^^^^^^^^^^^^^^^^

See :doc:`configs-reference` for a complete list of all configuration options.

.. automethod:: MRJob.configure_options
.. automethod:: MRJob.add_passthrough_option
.. automethod:: MRJob.add_file_option
.. automethod:: MRJob.load_options
.. automethod:: MRJob.is_mapper_or_reducer

Custom command-line types and actions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoattribute:: MRJob.OPTION_CLASS

The :py:mod:`optparse` module allows the addition of new actions and types.
See the `optparse docs <http://docs.python.org/library/optparse.html#extending-optparse>`_
for instructions on defining custom options. The only difference is that
instead of passing *option_class* to the :py:class:`OptionParser` instance
yourself, you must set the :py:attr:`MRJob.OPTION_CLASS` attribute.

Passthrough arguments have the additional caveat that mrjob uses some lesser
magic to reproduce the argument values for the command lines of subprocesses.
In practice you shouldn't encounter any problems here even with relatively
exotic option behavior, but be aware that your options will be processed
twice, with the second round using a copy of your default values produced by
:py:func:`copy.deepcopy`.

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

Running specific parts of jobs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
