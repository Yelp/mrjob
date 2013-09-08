Configuration quick reference
=============================

Setting configuration options
-----------------------------

You can set an option by:

* Passing it on the command line with the switch version (like
  ``--some-option``)
* Passing it as a keyword argument to the runner constructor, if you are
  creating the runner programmatically
* Putting it in one of the included config files under a runner name, like
  this:

  .. code-block:: yaml

    runners:
        local:
            python_bin: python2.6  # only used in local runner
        emr:
            python_bin: python2.5  # only used in Elastic MapReduce runner

  See :ref:`mrjob.conf` for information on where to put config files.

Options that can't be set from mrjob.conf (all runners)
-------------------------------------------------------

For some options, it doesn't make sense to be able to set them in the config
file. These can only be specified when calling the constructor of
:py:class:`~mrjob.runner.MRJobRunner`, as command line options, or sometimes by
overriding some attribute or method of your :py:class:`~mrjob.job.MRJob`
subclass.

Runner kwargs or command line
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. mrjob-optlist:: no_mrjob_conf

Runner kwargs or method overrides
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. use aliases to prevent rst from making our tables huge

.. |extra_args| replace:: :py:meth:`extra_args <mrjob.runner.MRJobRunner.__init__>`
.. |file_upload_args| replace:: :py:meth:`file_upload_args <mrjob.runner.MRJobRunner.__init__>`
.. |hadoop_input_format| replace:: :py:meth:`hadoop_input_format <mrjob.runner.MRJobRunner.__init__>`
.. |hadoop_output_format| replace:: :py:meth:`hadoop_output_format <mrjob.runner.MRJobRunner.__init__>`

.. |add_passthrough_option| replace:: :py:meth:`~mrjob.job.MRJob.add_passthrough_option`
.. |add_file_option| replace:: :py:meth:`~mrjob.job.MRJob.add_file_option`
.. |m_hadoop_input_format| replace:: :py:meth:`~mrjob.job.MRJob.hadoop_input_format`
.. |m_hadoop_output_format| replace:: :py:meth:`~mrjob.job.MRJob.hadoop_output_format`

====================== ======================== ========
Option                 Method                   Default
====================== ======================== ========
|extra_args|           |add_passthrough_option| ``[]``
|file_upload_args|     |add_file_option|        ``[]``
|hadoop_input_format|  |m_hadoop_input_format|  ``None``
|hadoop_output_format| |m_hadoop_output_format| ``None``
====================== ======================== ========

Other options for all runners
-----------------------------

These options can be passed to any runner without an error, though some runners
may ignore some options. See the text after the table for specifics.

.. mrjob-optlist:: all

:py:class:`~mrjob.local.LocalMRJobRunner` takes no additional options, but:

* :mrjob-opt:`bootstrap_mrjob` is ``False`` by default
* :mrjob-opt:`cmdenv` uses the local system path separator instead of ``:`` all
  the time (so ``;`` on Windows, no change elsewhere)
* :mrjob-opt:`python_bin` defaults to the current Python interpreter

In addition, it ignores *hadoop_input_format*, *hadoop_output_format*,
*hadoop_streaming_jar*, and *jobconf*

:py:class:`~mrjob.inline.InlineMRJobRunner` works like
:py:class:`~mrjob.local.LocalMRJobRunner`, only it also ignores
*bootstrap_mrjob*, *cmdenv*, *python_bin*, *setup_cmds*, *setup_scripts*,
*steps_python_bin*, *upload_archives*, and *upload_files*.


Additional options for :py:class:`~mrjob.emr.EMRJobRunner`
----------------------------------------------------------

.. mrjob-optlist:: emr


Additional options for :py:class:`~mrjob.hadoop.HadoopJobRunner`
----------------------------------------------------------------

.. mrjob-optlist:: hadoop
