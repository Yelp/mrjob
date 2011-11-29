mrjob.job.MRJob - base class for all jobs
=========================================

.. py:module:: mrjob.job


Basic
-----

.. currentmodule:: mrjob.job

.. autoclass:: MRJob

.. _writing-one-step-jobs:

Writing one-step jobs
^^^^^^^^^^^^^^^^^^^^^

.. automethod:: MRJob.mapper
.. automethod:: MRJob.reducer
.. automethod:: MRJob.combiner
.. automethod:: MRJob.mapper_init
.. automethod:: MRJob.mapper_final
.. automethod:: MRJob.reducer_init
.. automethod:: MRJob.reducer_final
.. automethod:: MRJob.combiner_init
.. automethod:: MRJob.combiner_final

.. _writing-multi-step-jobs:

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

Protocols convert data between string representations for Hadoop Streaming
and Python data structures for input and output by steps. Using the default
protocols, the first step will get raw strings as input and should return
JSON-encodable Python objects. The last step should accept JSON-decoded
Python objects and return JSON-encodable Python objects, which will appear
in the final output as JSON.

There are three ways to specify which protocol you want to use for a given
step. The simplest way is to assign a class imported from
:py:mod:`mrjob.protocols` to one of the class variables
:py:attr:`MRJob.INPUT_PROTOCOL`, :py:attr:`MRJob.INTERNAL_PROTOCOL`, or
:py:attr:`MRJob.OUTPUT_PROTOCOL` on your job. The input protocol is the format
of your input data, the internal protocol is the format of data passed between
steps, and the output protocol is what is produced at the end of the job. These
classes are instantiated when your :py:class:`MRJob` is instantiated.

For example, this class accepts raw strings, outputs JSON, and uses ``pickle``
to communicate between steps::

    from mrjob.job import MRJob
    from mrjob.protocol import JSONValueProtocol, PickleProtocol, RawValueProtocol

    class MRProtocolJob1(MRJob)
        INPUT_PROTOCOL = RawValueProtocol
        INTERNAL_PROTOCOL = PickleProtocol
        OUTPUT_PROTOCOL = JSONValueProtocol

The second way is to override :py:meth:`MRJob.input_protocol`,
:py:meth:`MRJob.internal_protocol`, or :py:meth:`MRJob.output_protocol`. These
methods should return *instances* of protocols. For example, if you want your
job to operate on either JSON or raw strings depending on a command line
option, you can do this::

    from mrjob.job import MRJob
    from mrjob.protocol import JSONValueProtocol, RawValueProtocol

    class MRProtocolJob2(MRJob):

        def configure_options(self):
            # set up 'data_format' option

        def input_protocol(self):
            if self.options.data_format == 'json':
                return JSONValueProtocol()
            elif self.options.data_format == 'raw':
                return RawValueProtocol()
            else:
                raise ValueError("Data format must be 'json' or 'raw'")

If you need behavior even more complex than that, including using different
protocols to communicate between different steps (like JSON between steps 1 and
2, but pickle between steps 2 and 3), you can override
:py:meth:`MRJob.pick_protocols` to return different protocols based on the
step number and step type of the current process.

See :doc:`protocols` for more information about protocols, including how
to implement your own.

.. autoattribute:: MRJob.INPUT_PROTOCOL
.. autoattribute:: MRJob.INTERNAL_PROTOCOL
.. autoattribute:: MRJob.OUTPUT_PROTOCOL
.. autoattribute:: MRJob.DEFAULT_INPUT_PROTOCOL
.. autoattribute:: MRJob.DEFAULT_PROTOCOL
.. autoattribute:: MRJob.DEFAULT_OUTPUT_PROTOCOL
.. automethod:: MRJob.input_protocol
.. automethod:: MRJob.internal_protocol
.. automethod:: MRJob.output_protocol
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

.. _custom-options:

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

.. _hadoop-config:

Hadoop Configuration
^^^^^^^^^^^^^^^^^^^^
.. autoattribute:: MRJob.HADOOP_INPUT_FORMAT
.. automethod:: MRJob.hadoop_input_format
.. autoattribute:: MRJob.HADOOP_OUTPUT_FORMAT
.. automethod:: MRJob.hadoop_output_format
.. autoattribute:: MRJob.JOBCONF
.. automethod:: MRJob.jobconf
.. autoattribute:: MRJob.PARTITIONER
.. automethod:: MRJob.partitioner

Hooks for testing
-----------------

.. currentmodule:: mrjob.job

.. automethod:: MRJob.sandbox
.. automethod:: MRJob.parse_output
.. automethod:: MRJob.parse_counters
