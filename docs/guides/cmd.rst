.. _mrjob-cmd:

The ``mrjob`` command
=====================

You can use the ``mrjob`` command to run jobs written in any language or
perform various Elastic MapReduce-related tasks.

``mrjob run (path to script or executable) [options]``
    Run a job. Takes same options as invoking a Python job. See
    :doc:`configs-all-runners`, :doc:`configs-hadoopy-runners`, and
    :doc:`emr-opts`.

``mrjob audit-emr-usage [options]``
    Audit EMR usage over the past 2 weeks, sorted by job flow name and user.

    Alias for :py:mod:`mrjob.tools.emr.audit_usage`.

``mrjob create-job-flow [options]``
    Create a persistent EMR job flow, using bootstrap scripts and other
    configs from :py:mod:`mrjob.conf`, and print the job flow ID to stdout.

    Alias for :py:mod:`mrjob.tools.emr.create_job_flow`.

``mrjob fetch-logs (job flow ID) [options]``
    List, display, and parse Hadoop logs associated with EMR job flows. Useful
    for debugging failed jobs for which mrjob did not display a useful error
    message or for inspecting jobs whose output has been lost.

    Alias for :py:mod:`mrjob.tools.emr.fetch_logs`.

``mrjob report-long-jobs [options]``
    Report jobs running for more than a certain number of hours (by default,
    24.0). This can help catch buggy jobs and Hadoop/EMR operational issues.

    Alias for :py:mod:`mrjob.tools.emr.report_long_jobs`.

``mrjob s3-tmpwatch [options]``
    Delete all files in a given URI that are older than a specified time.

    Alias for :py:mod:`mrjob.tools.emr.s3_tmpwatch`.

``mrjob terminate-idle-job-flows [options]``
    Terminate idle EMR job flows that meet the criteria passed in on the
    command line (or, by default, job flows that have been idle for one hour).

    Alias for :py:mod:`mrjob.tools.emr.terminate_idle_job_flows`.

``mrjob terminate-job-flow (job flow ID)``
    Terminate an existing EMR job flow.

    Alias for :py:mod:`mrjob.tools.emr.terminate_job_flow`.
