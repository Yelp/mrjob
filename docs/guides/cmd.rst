.. _mrjob-cmd:

The ``mrjob`` command
=====================

The ``mrjob`` command has two purposes:

1. To provide easy access to EMR tools
2. To eventually let you run Hadoop Streaming jobs written in languages other
   than Python

EMR tools
---------

``mrjob audit-emr-usage [options]``
    Audit EMR usage over the past 2 weeks, sorted by cluster name and user.

    Runs :py:mod:`mrjob.tools.emr.audit_usage`.

``mrjob create-cluster [options]``
    Create a persistent EMR cluster, using bootstrap scripts and other
    configs from :py:mod:`mrjob.conf`, and print the cluster ID to stdout.

    Runs :py:mod:`mrjob.tools.emr.create_cluster`.

``mrjob report-long-jobs [options]``
    Report jobs running for more than a certain number of hours (by default,
    24.0). This can help catch buggy jobs and Hadoop/EMR operational issues.

    Runs :py:mod:`mrjob.tools.emr.report_long_jobs`.

``mrjob s3-tmpwatch [options]``
    Delete all files in a given URI that are older than a specified time.

    Runs :py:mod:`mrjob.tools.emr.s3_tmpwatch`.

``mrjob terminate-idle-clusters [options]``
    Terminate idle EMR clusters that meet the criteria passed in on the
    command line (or, by default, clusters that have been idle for one hour).

    Runs :py:mod:`mrjob.tools.emr.terminate_idle_clusters`.

``mrjob terminate-cluster (cluster ID)``
    Terminate an existing EMR cluster.

    Runs :py:mod:`mrjob.tools.emr.terminate_cluster`.

Running jobs
------------

``mrjob run (path to script or executable) [options]``
    Run a job. Takes same options as invoking a Python job. See
    :doc:`configs-all-runners`, :doc:`configs-hadoopy-runners`, :doc:`dataproc-opts`, and
    :doc:`emr-opts`. While you can use this command to invoke your jobs, you
    can just as easily call ``python my_job.py [options]``.
