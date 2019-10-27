mrjob.cmd: The ``mrjob`` command-line utility
=============================================

The :command:`mrjob` command provides a number of sub-commands that help you
run and monitor jobs.

The :command:`mrjob` command comes with Python-version-specific aliases (e.g.
:command:`mrjob-3`, :command:`mrjob-3.4`), in case you choose to install
``mrjob`` for multiple versions of Python. You may also run it as
:command:`python -m mrjob.cmd <subcommand>`.

.. _audit-emr-usage:

audit-emr-usage
^^^^^^^^^^^^^^^

   .. automodule:: mrjob.tools.emr.audit_usage

boss
^^^^

    .. automodule:: mrjob.tools.emr.mrboss

create-cluster
^^^^^^^^^^^^^^

   .. automodule:: mrjob.tools.emr.create_cluster


.. _diagnose-tool:

diagnose
^^^^^^^^

   .. automodule:: mrjob.tools.diagnose

.. _report-long-jobs:

report-long-jobs
^^^^^^^^^^^^^^^^

    .. automodule:: mrjob.tools.emr.report_long_jobs

s3-tmpwatch
^^^^^^^^^^^

    .. automodule:: mrjob.tools.emr.s3_tmpwatch

.. _spark-submit:

spark-submit
^^^^^^^^^^^^

    .. automodule:: mrjob.tools.spark_submit

terminate-cluster
^^^^^^^^^^^^^^^^^

    .. automodule:: mrjob.tools.emr.terminate_cluster

.. _terminate-idle-clusters:

terminate-idle-clusters
^^^^^^^^^^^^^^^^^^^^^^^

    .. automodule:: mrjob.tools.emr.terminate_idle_clusters
