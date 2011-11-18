What's New in mrjob 0.3
=======================

Features
--------

**Support for Combiners**

    You can now use combiners in your job. Like :py:meth:`.mapper()` and
    :py:meth:`.reducer()`, you can redefine :py:meth:`.combiner()` in your
    subclass to add a single combiner step to run after your method.
    (:py:class:`MRWordFreqCount` does this.) :py:meth:`.combiner_init()` and
    :py:meth:`.combiner_final()` are similar to their mapper and reducer
    equivalents.

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

    EMR jobs can pull job flows out of a "pool" of similarly configured (and
    optionally named) job flows. This can make it easier to use a small set of
    job flows across multiple automated jobs, save time and money while
    debugging, and generally make your life simpler.

    More info: :ref:`pooling-job-flows`

**SSH Log Fetching**

    mrjob attempts to fetch counters and error logs for EMR jobs via SSH before
    trying to use S3. This method is faster, more reliable, and works with
    persistent job flows.

    More info: :ref:`ssh-tunneling`

**New EMR Tool: fetchlogs**

    If you want to fetch the counters or error logs for a job after the fact,
    you can use the new ``fetchlogs`` tool.

    More info: :py:mod:`mrjob.tools.emr.fetch_logs`

**New EMR Tool: mrboss**

    If you want to run a command on all nodes and inspect the output, perhaps
    to see what processes are running, you can use the new ``mrboss`` tool.

    More info: :py:mod:`mrjob.tools.emr.mrboss`

Changes and Deprecations
------------------------

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
    jobconf values back, use :py:func:`mrjob.compat.get_jobconf_value()`, which
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

        More info: :ref:`job-protocols`

**Running Jobs**
   * All modes:
     * All runners are Hadoop-version aware and use the correct jobconf and
       combiner invocation styles (Issue #111)
     * All types of URIs can be passed through to Hadoop (Issue #53)
     * Speed up steps with no mapper by using cat (Issue #5)
     * Stream compressed files with cat() method (Issue #17)
     * hadoop_bin, python_bin, and ssh_bin can now all take switches (Issue #96)
     * job_name_prefix option is gone (was deprecated)
     * Better cleanup (Issue #10):
       * Separate cleanup_on_failure option
       * More granular cleanup options
     * Cleaner handling of passthrough options (Issue #32)
   * emr mode:
     * default Hadoop version on EMR is 0.20 (was 0.18)
     * ec2_instance_type option now only sets instance type for slave nodes
       when there are multiple EC2 instances (Issue #66)
   * inline mode:
     * Supports cmdenv (Issue #136)
   * local mode:
     * Runs 2 mappers and 2 reducers in parallel by default (Issue #228)
     * Preliminary Hadoop simulation for some jobconf variables (Issue #86)
 * Misc:
   * boto 2.0+ is now required (Issue #92)
   * Removed debian packaging (should be handled separately)
