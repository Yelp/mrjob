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

 * Defining Jobs (MRJob):
   * mapper/combiner/reducer methods no longer need to contain a yield
     statement if they emit no data
   * Protocols:
     * Protocols can be anything with read() and write() methods, and are
       instances by default (Issue #229)
     * Set protocols with the *_PROTOCOL attributes or by re-defining the
       *_protocol() methods
     * Built-in protocol classes cache the encoded and decoded value of the
       last key for faster decoding during reducing (Issue #230)
     * --*protocol switches and aliases are deprecated (Issue #106)
   * Set Hadoop formats with HADOOP_*_FORMAT attributes or the hadoop_*_format()
     methods (Issue #241)
     * --hadoop-*-format switches are deprecated
     * Hadoop formats can no longer be set from mrjob.conf
   * Set jobconf with JOBCONF attribute or the jobconf() method (in addition
     to --jobconf)
   * Set Hadoop partitioner class with --partitioner, PARTITIONER, or
     partitioner() (Issue #6)
   * Use mrjob.compat.get_jobconf_value() to get jobconf values from environment
 * Running jobs:
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
