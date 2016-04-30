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
            python_bin: python2.7  # only used in local runner
        emr:
            python_bin: python2.6  # only used in Elastic MapReduce runner

  See :ref:`mrjob.conf` for information on where to put config files.

Options that can't be set from mrjob.conf (all runners)
-------------------------------------------------------

There are some options that it makes no sense to set in the config file.

These options can be set via command-line switches:

.. mrjob-optlist:: no_mrjob_conf

These options can be set by overriding attributes or methods in your job class:

.. use aliases to prevent rst from making our tables huge

.. |a_hadoop_input_format| replace:: :py:attr:`~mrjob.job.MRJob.HADOOP_INPUT_FORMAT`
.. |a_hadoop_output_format| replace:: :py:attr:`~mrjob.job.MRJob.HADOOP_OUTPUT_FORMAT`
.. |a_partitioner| replace:: :py:attr:`~mrjob.job.MRJob.PARTITIONER`

.. |m_hadoop_input_format| replace:: :py:meth:`~mrjob.job.MRJob.hadoop_input_format`
.. |m_hadoop_output_format| replace:: :py:meth:`~mrjob.job.MRJob.hadoop_output_format`
.. |m_partitioner| replace:: :py:meth:`~mrjob.job.MRJob.partitioner`

====================== ======================== ======================== ========
Option                 Attribute                Method                   Default
====================== ======================== ======================== ========
*hadoop_input_format*  |a_hadoop_input_format|  |m_hadoop_input_format|  ``None``
*hadoop_output_format* |a_hadoop_output_format| |m_hadoop_output_format| ``None``
*partitioner*          |a_partitioner|          |m_partitioner|          ``None``
====================== ======================== ======================== ========

These options can be set by overriding your job's
:py:meth:`~mrjob.job.MRJob.configure_options` to call the appropriate method:

.. |extra_args| replace:: :py:meth:`extra_args <mrjob.runner.MRJobRunner.__init__>`
.. |file_upload_args| replace:: :py:meth:`file_upload_args <mrjob.runner.MRJobRunner.__init__>`

.. |add_passthrough_option| replace:: :py:meth:`~mrjob.job.MRJob.add_passthrough_option`
.. |add_file_option| replace:: :py:meth:`~mrjob.job.MRJob.add_file_option`

====================== ======================== ========
Option                 Method                   Default
====================== ======================== ========
*extra_args*           |add_passthrough_option| ``[]``
*file_upload_args*     |add_file_option|        ``[]``
====================== ======================== ========

All of the above can be passed as keyword arguments to
:py:meth:`MRJobRunner.__init__() <mrjob.runner.MRJobRunner.__init__>`
(this is what makes them runner options), but you usually don't want to
instantiate runners directly.

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


Additional options for :py:class:`~mrjob.dataproc.DataprocJobRunner`
--------------------------------------------------------------------

.. mrjob-optlist:: dataproc


Additional options for :py:class:`~mrjob.emr.EMRJobRunner`
----------------------------------------------------------

.. mrjob-optlist:: emr


Additional options for :py:class:`~mrjob.hadoop.HadoopJobRunner`
----------------------------------------------------------------

.. mrjob-optlist:: hadoop
