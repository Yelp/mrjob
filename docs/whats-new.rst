What's New
==========

For a complete list of changes, see `CHANGES.txt
<https://github.com/Yelp/mrjob/blob/master/CHANGES.txt>`_

0.4.1
-----

The :py:attr:`~mrjob.job.MRJob.SORT_VALUES` option enables secondary sort,
ensuring that your reducer(s) receive values in sorted order. This allows you
to do things with reducers that would otherwise involve storing all the values
in memory, such as:

* Receiving a grand total before any subtotals, so you can calculate
  percentages on the fly. See `mr_next_word_stats.py
  <https://github.com/Yelp/mrjob/blob/master/mrjob/examples/mr_next_word_stats.py>`_ for an example.
* Running a window of fixed length over an arbitrary amount of sorted
  values (e.g. a 24-hour window over timestamped log data).

The :mrjob-opt:`max_hours_idle` option allows you to spin up EMR job flows
that will terminate themselves after being idle for a certain amount of time,
in a way that optimizes EMR/EC2's full-hour billing model.

For development (not production), we now recommend always using
:ref:`job flow pooling <pooling-job-flows>`, with :mrjob-opt:`max_hours_idle`
enabled. Update your :ref:`mrjob.conf <mrjob.conf>` like this:

.. code-block:: yaml

    runners:
      emr:
        max_hours_idle: 0.25
	pool_emr_job_flows: true

.. warning::

   If you enable pooling *without* :mrjob-opt:`max_hours_idle` (or
   cronning :py:mod:`~mrjob.tools.emr.terminate_idle_job_flows`), pooled job
   flows will stay active forever, costing you money!

You can now use :option:`--no-check-input-paths` with the Hadoop runner to
allow jobs to run even if ``hadoop fs -ls`` can't see their input files
(see :mrjob-opt:`check_input_paths`).

Two bits of straggling deprecated functionality were removed:

* Built-in :ref:`protocols <job-protocols>` must be instantiated
  to be used (formerly they had class methods).
* Old locations for :ref:`mrjob.conf <mrjob.conf>` are no longer supported.

This version also contains numerous bugfixes and natural extensions of
existing functionality; many more things will now Just Work (see `CHANGES.txt
<https://github.com/Yelp/mrjob/blob/master/CHANGES.txt>`_).

0.4.0
-----
The default runner is now `inline` instead of `local`. This change will speed
up debugging for many users. Use `local` if you need to simulate more features
of Hadoop.

The EMR tools can now be accessed more easily via the `mrjob` command. Learn
more :doc:`here <guides/cmd>`.

Job steps are much richer now:

* You can now use mrjob to run jar steps other than Hadoop Streaming. :ref:`More info <non-hadoop-streaming-jar-steps>`
* You can filter step input with UNIX commands. :ref:`More info <cmd-filters>`
* In fact, you can use arbitrary UNIX commands as your whole step (mapper/reducer/combiner). :ref:`More info <cmd-steps>`

If you Ctrl+C from the command line, your job will be terminated if you give it time.
If you're running on EMR, that should prevent most accidental runaway jobs. :ref:`More info <configs-all-runners-cleanup>`

mrjob v0.4 requires boto 2.2.

We removed all deprecated functionality from v0.2:

* --hadoop-\*-format
* --\*-protocol switches
* MRJob.DEFAULT_*_PROTOCOL
* MRJob.get_default_opts()
* MRJob.protocols()
* PROTOCOL_DICT
* IF_SUCCESSFUL
* DEFAULT_CLEANUP
* S3Filesystem.get_s3_folder_keys()

We love contributions, so we wrote some :doc:`guidelines<guides/contributing>` to help you help us. See you on Github!

0.3.5
-----

The *pool_wait_minutes* (:option:`--pool-wait-minutes`) option lets your job
delay itself in case a job flow becomes available. Reference:
:doc:`guides/configs-reference`

The ``JOB`` and ``JOB_FLOW`` cleanup options tell mrjob to clean up the job
and/or the job flow on failure (including Ctrl+C). See
:py:data:`~mrjob.runner.CLEANUP_CHOICES` for more information.

0.3.3
-----

You can now :ref:`include one config file from another
<multiple-config-files>`.

0.3.2
-----

The EMR instance type/number options have changed to support spot instances:

* *ec2_core_instance_bid_price*
* *ec2_core_instance_type*
* *ec2_master_instance_bid_price*
* *ec2_master_instance_type*
* *ec2_slave_instance_type* (alias for *ec2_core_instance_type*)
* *ec2_task_instance_bid_price*
* *ec2_task_instance_type*

There is also a new *ami_version* option to change the AMI your job flow uses
for its nodes.

For more information, see :py:meth:`mrjob.emr.EMRJobRunner.__init__`.

The new :py:mod:`~mrjob.tools.emr.report_long_jobs` tool alerts on jobs that
have run for more than X hours.

0.3
---

Features
^^^^^^^^

**Support for Combiners**

    You can now use combiners in your job. Like :py:meth:`.mapper()` and
    :py:meth:`.reducer()`, you can redefine :py:meth:`.combiner()` in your
    subclass to add a single combiner step to run after your mapper but before
    your reducer.  (:py:class:`MRWordFreqCount` does this to improve
    performance.) :py:meth:`.combiner_init()` and :py:meth:`.combiner_final()`
    are similar to their mapper and reducer equivalents.

    You can also add combiners to custom steps by adding keyword argumens to
    your call to :py:meth:`.steps()`.

    More info: :ref:`writing-one-step-jobs`, :ref:`writing-multi-step-jobs`

**\*_init(), \*_final() for mappers, reducers, combiners**

    Mappers, reducers, and combiners have ``*_init()`` and ``*_final()``
    methods that are run before and after the input is run through the main
    function (e.g. :py:meth:`.mapper_init()` and :py:meth:`.mapper_final()`).

    More info: :ref:`writing-one-step-jobs`, :ref:`writing-multi-step-jobs`

**Custom Option Parsers**

    It is now possible to define your own option types and actions using a
    custom :py:class:`OptionParser` subclass.

    More info: :ref:`custom-options`

**Job Flow Pooling**

    EMR jobs can pull job flows out of a "pool" of similarly configured job
    flows. This can make it easier to use a small set of job flows across
    multiple automated jobs, save time and money while debugging, and generally
    make your life simpler.

    More info: :ref:`pooling-job-flows`

**SSH Log Fetching**

    mrjob attempts to fetch counters and error logs for EMR jobs via SSH before
    trying to use S3. This method is faster, more reliable, and works with
    persistent job flows.

    More info: :ref:`ssh-tunneling`

**New EMR Tool: fetch_logs**

    If you want to fetch the counters or error logs for a job after the fact,
    you can use the new ``fetch_logs`` tool.

    More info: :py:mod:`mrjob.tools.emr.fetch_logs`

**New EMR Tool: mrboss**

    If you want to run a command on all nodes and inspect the output, perhaps
    to see what processes are running, you can use the new ``mrboss`` tool.

    More info: :py:mod:`mrjob.tools.emr.mrboss`

Changes and Deprecations
^^^^^^^^^^^^^^^^^^^^^^^^

**Configuration**

    The search path order for ``mrjob.conf`` has changed. The new order is:

    * The location specified by :envvar:`MRJOB_CONF`
    * :file:`~/.mrjob.conf`
    * :file:`~/.mrjob` **(deprecated)**
    * :file:`mrjob.conf` in any directory in :envvar:`PYTHONPATH`
      **(deprecated)**
    * :file:`/etc/mrjob.conf`

    If your :file:`mrjob.conf` path is deprecated, use this table to fix it:

    ================================= ===============================
    Old Location                      New Location
    ================================= ===============================
    :file:`~/.mrjob`                  :file:`~/.mrjob.conf`
    somewhere in :envvar:`PYTHONPATH` Specify in :envvar:`MRJOB_CONF`
    ================================= ===============================

    More info: :py:mod:`mrjob.conf`

**Defining Jobs (MRJob)**

    Mapper, combiner, and reducer methods no longer need to contain a yield
    statement if they emit no data.

    The :option:`--hadoop-*-format` switches are deprecated. Instead, set your
    job's Hadoop formats with
    :py:attr:`.HADOOP_INPUT_FORMAT`/:py:attr:`.HADOOP_OUTPUT_FORMAT`
    or :py:meth:`.hadoop_input_format()`/:py:meth:`.hadoop_output_format()`.
    Hadoop formats can no longer be set from :file:`mrjob.conf`.

    In addition to :option:`--jobconf`, you can now set jobconf values with the
    :py:attr:`.JOBCONF` attribute or the :py:meth:`.jobconf()` method.  To read
    jobconf values back, use :py:func:`mrjob.compat.jobconf_from_env()`, which
    ensures that the correct name is used depending on which version of Hadoop
    is active.

    You can now set the Hadoop partioner class with :option:`--partitioner`,
    the :py:attr:`.PARTITIONER` attribute, or the :py:meth:`.partitioner()`
    method.

    More info: :ref:`hadoop-config`

    **Protocols**

        Protocols can now be anything with a ``read()`` and ``write()``
        method. Unlike previous versions of mrjob, they can be **instance
        methods** rather than class methods. You should use instance methods
        when defining your own protocols.

        The :option:`--*protocol` switches and :py:attr:`DEFAULT_*PROTOCOL`
        are deprecated. Instead, use the :py:attr:`*_PROTOCOL` attributes or
        redefine the :py:meth:`*_protocol()` methods.

        Protocols now cache the decoded values of keys. Informal testing shows
        up to 30% speed improvements.

        More info: :ref:`job-protocols`

**Running Jobs**

    **All Modes**

        All runners are Hadoop-version aware and use the correct jobconf and
        combiner invocation styles. This change should decrease the number
        of warnings in Hadoop 0.20 environments.

        All ``*_bin`` configuration options (``hadoop_bin``, ``python_bin``,
        and ``ssh_bin``) take lists instead of strings so you can add
        arguments (like ``['python', '-v']``).  More info:
        :doc:`guides/configs-reference`

        Cleanup options have been split into ``cleanup`` and
        ``cleanup_on_failure``. There are more granular values for both of
        these options.

        Most limitations have been lifted from passthrough options, including
        the former inability to use custom types and actions. More info:
        :ref:`custom-options`

        The ``job_name_prefix`` option is gone (was deprecated).

        All URIs are passed through to Hadoop where possible. This should
        relax some requirements about what URIs you can use.

        Steps with no mapper use :command:`cat` instead of going through a
        no-op mapper.

        Compressed files can be streamed with the :py:meth:`.cat()` method.

    **EMR Mode**

        The default Hadoop version on EMR is now 0.20 (was 0.18).

        The ``ec2_instance_type`` option only sets the instance type for slave
        nodes when there are multiple EC2 instance. This is because the master
        node can usually remain small without affecting the performance of the
        job.

    **Inline Mode**

        Inline mode now supports the ``cmdenv`` option.

    **Local Mode**

        Local mode now runs 2 mappers and 2 reducers in parallel by default.

        There is preliminary support for simulating some jobconf variables.
        The current list of supported variables is:

        * ``mapreduce.job.cache.archives``
        * ``mapreduce.job.cache.files``
        * ``mapreduce.job.cache.local.archives``
        * ``mapreduce.job.cache.local.files``
        * ``mapreduce.job.id``
        * ``mapreduce.job.local.dir``
        * ``mapreduce.map.input.file``
        * ``mapreduce.map.input.length``
        * ``mapreduce.map.input.start``
        * ``mapreduce.task.attempt.id``
        * ``mapreduce.task.id``
        * ``mapreduce.task.ismap``
        * ``mapreduce.task.output.dir``
        * ``mapreduce.task.partition``

**Other Stuff**

    boto 2.0+ is now required.

    The Debian packaging has been removed from the repostory.
