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
.. automethod:: MRJob.mapper_init
.. automethod:: MRJob.mapper_final
.. automethod:: MRJob.reducer_init
.. automethod:: MRJob.reducer_final

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

Custom command-line types and actions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :py:mod:`optparse` module allows the addition of new actions and types.
mrjob's own custom actions and types, and especially the passthrough arguments
feature, make defining your own custom actions and types slightly more
complicated.

Adding custom actions and types to your own jobs is essentially the same as it
is normally (see the `optparse docs <http://docs.python.org/library/optparse.html#extending-optparse>`_),
except that you should subclass from :py:class:`mrjob.job.MRJobOption` instead
of :py:class:`optparse.Option`. For example, to add a new action
``double_store``::

    from mrjob.job import MRJobOption

    class MyJobOption():

        ACTIONS = MRJobOption.ACTIONS + ('double_store',)
        STORE_ACTIONS = MRJobOption.STORE_ACTIONS + ('double_store',)
        TYPED_ACTIONS = MRJobOption.TYPED_ACTIONS + ('double_store',)
        ALWAYS_TYPED_ACTIONS = MRJobOption.ALWAYS_TYPED_ACTIONS + ('double_store',)

        def take_action(self, action, dest, opt, value, values, parser):
            if action == 'double_store':
                values.ensure_value(dest, value*2)
            else:
                MRJobOption.take_action(
                    self, action, dest, opt, value, values, parser)

Special care should be taken when defining custom behavior for passthrough
arguments because mrjob must reconstruct the original command line arguments
in order to pass them to your mappers and reducers. In practice you shouldn't
encounter any problems here, but be aware that your options will be processed
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
