What's New
==========

For a complete list of changes, see `CHANGES.txt
<https://github.com/Yelp/mrjob/blob/master/CHANGES.txt>`_


.. _v0.5.3:

0.5.3
-----

This release adds support for custom :mrjob-opt:`libjars` (such as
`nicknack <http://empiricalresults.github.io/nicknack/>`__), allowing easy
access to custom input and output formats. This works on Hadoop and EMR
(including on a cluster that's already running).

In addition, jobs can specify needed libjars by setting the
:py:attr:`~mrjob.job.MRJob.LIBJARS` attribute or overriding the
:py:meth:`~mrjob.job.MRJob.libjars` method. For examples, see
:ref:`input-and-output-formats`.

The Hadoop runner now tries *even harder* to find your log files without
needing additional configuration (see :mrjob-opt:`hadoop_log_dirs`).

The EMR runner now supports Amazon VPC subnets (see :mrjob-opt:`subnet`), and,
on 4.x AMIs, Application Configurations (see :mrjob-opt:`emr_configurations`).

If your EMR cluster fails during bootstrapping, mrjob can now determine
the probable cause of failure.

There are also some minor improvements to SSH tunneling and a handful of
small bugfixes; see `CHANGES.txt
<https://github.com/Yelp/mrjob/blob/master/CHANGES.txt>`_ for details.

.. _v0.5.2:

0.5.2
-----

This release adds basic support for `Google Cloud Dataproc <https://cloud.google.com/dataproc/overview>`_ which is Google's Hadoop service, roughly analogous to EMR. See :doc:`guides/dataproc-quickstart`. Some features are not yet implemented:

* fetching counters
* finding probable cause of errors
* running Java JARs as steps

Added the :mrjob-opt:`emr_applications` option, which helps you configure
4.x AMIs.

Fixed an EMR bug (introduced in v0.5.0) where we were waiting for steps
to complete in the wrong order (in a multi-step job, we wouldn't register
that the first step had finished until the last one had).

Fixed a bug in SSH tunneling (introduced in v0.5.0) that made connections
to the job tracker/resource manager on EMR time out when running on a 2.x
AMI inside a VPC (Virtual Private Cluster).

Fixed a bug (introduced in v0.4.6) that kept mrjob from interpreting ``~``
(home directory) in includes in :file:`mrjob.conf`.

It is now again possible to run tool modules deprecated in v0.5.0 directly
(e.g. :command:`python -m mrjob.tools.emr.create_job_flow`). This is still a
deprecated feature; it's recommended that you use the appropriate
:command:`mrjob` subcommand instead (e.g. :command:`mrjob create-cluster`).

.. _v0.5.1:

0.5.1
-----

Fixes a bug in the previous relase that broke
:py:attr:`~mrjob.job.MRJob.SORT_VALUES` and any other attempt by the job
to set the partitioner. The ``--partitioner`` switch is now deprecated
(the choice of partitioner is part of your job semantics).

Fixes a bug in the previous release that caused :mrjob-opt:`strict_protocols`
and :mrjob-opt:`check_input_paths` to be ignored in :file:`mrjob.conf`. (We
would much prefer you fixed jobs that are using "loose protocols" rather than
setting ``strict_protocols: false`` in your config file, but we didn't break
this on purpose, we promise!)

``mrjob terminate-idle-clusters`` now correctly handles EMR debugging steps
(see :mrjob-opt:`enable_emr_debugging`) set up by boto 2.40.0.

Fixed a bug that could result in showing a blank probable cause of error
for pre-YARN (Hadoop 1) jobs.

:mrjob-opt:`ssh_bind_ports` now defaults to a ``range`` object (``xrange`` on
Python 2), so that when you run on emr in verbose mode (``-r emr -v``), debug
logging devotes one line to the value of ``ssh_bind_ports`` rather than 840.

.. _v0.5.0:

0.5.0
-----

Python versions
^^^^^^^^^^^^^^^

mrjob now fully supports Python 3.3+ in a way that should be transparent to existing Python 2 users (you don't have to suddenly start handling ``unicode`` instead of ``str``). For more information, see :doc:`guides/py2-vs-py3`.

If you run a job with Python 3, mrjob will automatically install Python 3 on ElasticMapreduce AMIs (see :mrjob-opt:`bootstrap_python`).

When you run jobs on EMR in Python 2, mrjob attempts to match your minor version of Python as well (either :command:`python2.6` or :command:`python2.7`); see :mrjob-opt:`python_bin` for details.

.. note::

   If you're currently running Python 2.7, and
   :ref:`using yum to install python libraries <installing-packages>`, you'll
   want to use the Python 2.7 version of the package (e.g.
   ``python27-numpy`` rather than ``python-numpy``).

The :command:`mrjob` command is now installed with Python-version-specific aliases (e.g. :command:`mrjob-3`, :command:`mrjob-3.4`), in case you install mrjob for multiple versions of Python.

Hadoop
^^^^^^

mrjob should now work out-of-the box on almost any Hadoop setup. If :command:`hadoop` is in your path, or you set any commonly-used :envvar:`$HADOOP_*` environment variable, mrjob will find the Hadoop binary, the streaming jar, and your logs, without any help on your part (see :mrjob-opt:`hadoop_bin`, :mrjob-opt:`hadoop_log_dirs`, :mrjob-opt:`hadoop_streaming_jar`).

mrjob has been updated to fully support Hadoop 2 (YARN), including many updates to :py:class:`~mrjob.fs.hadoop.HadoopFilesystem`. Hadoop 1 is still supported, though anything prior to Hadoop 0.20.203 is not (mrjob is actually a few months older than Hadoop 0.20.203, so this used to matter).

3.x and 4.x AMIs
^^^^^^^^^^^^^^^^

mrjob now fully supports the 3.x and 4.x Elastic MapReduce AMIs, including SSH tunneling to the resource mananager, fetching counters and finding probable cause of job failure.

The default :mrjob-opt:`ami_version` is now ``3.11.0``. Our plan is to continue updating this to the lastest (non-broken) 3.x AMI for each 0.5.x release of mrjob.

The default :mrjob-opt:`ec2_instance_type` is now ``m1.medium`` (``m1.small`` is too small for the 3.x and 4.x AMIs)

You can specify 4.x AMIs with either the new :mrjob-opt:`release_label` option, or continue using :mrjob-opt:`ami_version`; both work.

mrjob continues to support 2.x AMIs. However:

.. warning::

   2.x AMIs are deprecated by AWS, and based on a very old version of Debian (squeeze), which breaks :command:`apt-get` and exposes you to security holes.

Please, please switch if you haven't already.

AWS Regions
^^^^^^^^^^^

The new default :mrjob-opt:`aws_region` is ``us-west-2`` (Oregon). This both matches the default in the EMR console and, according to Amazon, is `carbon neutral <https://aws.amazon.com/about-aws/sustainability/>`__.

An edge case that might affect you: EC2 key pairs (i.e. SSH credentials) are region-specific, so if you've set up SSH but not explicitly specified a region, you may get an error saying your key pair is invalid. The fix is simply to :ref:`create new SSH keys <ssh-tunneling>` for the ``us-west-2`` (Oregon) region.

S3
^^^

mrjob is much smarter about the way it interacts with S3:
 - automatically creates temp bucket in the same region as jobs
 - connects to S3 buckets on the endpoint matching their region (no more 307 errors)

   - :py:class:`~mrjob.emr.EMRJobRunner` and :py:class:`~mrjob.fs.s3.S3Filesystem` methods no longer take ``s3_conn`` args (passing around a single S3 connection no longer makes sense)

 - no longer uses the temp bucket's location to choose where you run your job
 - :py:meth:`~mrjob.fs.s3.S3Filesystem.rm` no longer has special logic for ``*_$folder$`` keys
 - :py:meth:`~mrjob.fs.s3.S3Filesystem.ls` recurses "subdirectories" even if you pass it a URI without a trailing slash

Log interpretation
^^^^^^^^^^^^^^^^^^

The part of mrjob that fetches counters and tells you what probably caused your job to fail was basically unmaintainable and has been totally rewritten. Not only do we now have solid support across Hadoop and EMR AMI versions, but if we missed anything, it should be straightforward to add it.

Once casualty of this change was the :command:`mrjob fetch-logs` command, which means mrjob no longer offers a way to fetch or interpret logs from a *past* job. We do plan to re-introduce this functionality.

Protocols
^^^^^^^^^

Protocols are now strict by default (they simply raise an exception on
unencodable data). "Loose" protocols can be re-enabled with the
``--no-strict-protocols`` switch; see :mrjob-opt:`strict_protocols` for
why this is a bad idea.

Protocols will now use the much faster :py:mod:`ujson` library, if installed,
to encode and decode JSON. This is especially recommended for simple jobs that
spend a significant fraction of their time encoding and data.

.. note::

   If you're using EMR, try out
   :ref:`this bootstrap recipe <installing-ujson>` to install :py:mod:`ujson`.

mrjob will fall back to the :py:mod:`simplejson` library if :py:mod:`ujson`
is not installed, and use the built-in ``json`` module if neither is installed.

You can now explicitly specify which JSON implementation you wish to use
(e.g. :py:class:`~mrjob.protocol.StandardJSONProtocol`, :py:class:`~mrjob.protocol.SimpleJSONProtocol`, :py:class:`~mrjob.protocol.UltraJSONProtocol`).

Status messages
^^^^^^^^^^^^^^^

We've tried to cut the logging messages that your job prints as it runs down to the basics (either useful info, like where a temp directory is, or something that tells you why you're waiting). If there are any messages you miss, try running your job with ``-v``.

When a step in your job fails, mrjob no longer prints a useless stacktrace telling you where in the code the runner raised an exception about your step failing. This is thanks to :py:class:`~mrjob.step.StepFailedException`, which you can also catch and interpret if you're :ref:`running jobs programmatically <runners-programmatically>`.

.. _v0.5.0-deprecation:

Deprecation
^^^^^^^^^^^

Many things that were deprecated in 0.4.6 have been removed:

 - options:

   - :py:data:`~mrjob.runner.IF_SUCCESSFUL` :mrjob-opt:`cleanup` option (use :py:data:`~mrjob.runner.ALL`)
   - *iam_job_flow_role* (use :mrjob-opt:`iam_instance_profile`)

 - functions and methods:

   - positional arguments to :py:meth:`mrjob.job.MRJob.mr()` (don't even use :py:meth:`~mrjob.job.MRJob.mr()`; use :py:class:`mrjob.step.MRStep`)
   - ``mrjob.job.MRJob.jar()`` (use :py:class:`mrjob.step.JarStep`)
   - *step_args* and *name* arguments to :py:class:`mrjob.step.JarStep` (use *args* instead of *step_args*, and don't use *name* at all)
   - :py:class:`mrjob.step.MRJobStep` (use :py:class:`mrjob.step.MRStep`)
   - :py:func:`mrjob.compat.get_jobconf_value` (use to :py:func:`~mrjob.compat.jobconf_from_env`)
   - :py:meth:`mrjob.job.MRJob.parse_counters`
   - :py:meth:`mrjob.job.MRJob.parse_output`
   - :py:func:`mrjob.conf.combine_cmd_lists`
   - :py:meth:`mrjob.fs.s3.S3Filesystem.get_s3_folder_keys`

:py:mod:`mrjob.compat` functions :py:func:`~mrjob.compat.supports_combiners_in_hadoop_streaming`, :py:func:`~mrjob.compat.supports_new_distributed_cache_options`, and :py:func:`~mrjob.compat.uses_generic_jobconf`, which only existed to support very old versions of Hadoop, were removed without deprecation warnings (sorry!).

To avoid a similar wave of deprecation warnings in the future, the name of every part of mrjob that isn't meant to be a stable interface provided by the library now starts with an underscore. You can still use these things (or copy them; it's Open Source), but there's no guarantee they'll exist in the next release.

If you want to get ahead of the game, here is a list of things that are deprecated starting in mrjob 0.5.0 (do these *after* upgrading mrjob):

  - options:

    - *base_tmp_dir* is now :mrjob-opt:`local_tmp_dir`
    - :mrjob-opt:`cleanup` options :py:data:`~mrjob.runner.LOCAL_SCRATCH` and :py:data:`~mrjob.runner.REMOTE_SCRATCH` are now :py:data:`~mrjob.runner.LOCAL_TMP` and :py:data:`~mrjob.runner.REMOTE_TMP`
    - *emr_job_flow_id* is now :mrjob-opt:`cluster_id`
    - *emr_job_flow_pool_name* is now :mrjob-opt:`pool_name`
    - *hdfs_scratch_dir* is now :mrjob-opt:`hadoop_tmp_dir`
    - *pool_emr_job_flows* is now :mrjob-opt:`pool_clusters`
    - *s3_scratch_uri* is now :mrjob-opt:`cloud_tmp_dir`
    - *ssh_tunnel_to_job_tracker* is now simply :mrjob-opt:`ssh_tunnel`

  - functions and methods:

    - :py:meth:`mrjob.job.MRJob.is_mapper_or_reducer` is now :py:meth:`~mrjob.job.MRJob.is_task`
    - :py:class:`~mrjob.fs.base.Filesystem` method ``path_exists()`` is now simply :py:meth:`~mrjob.fs.base.Filesystem.exists`
    - :py:class:`~mrjob.fs.base.Filesystem` method ``path_join()`` is now simply :py:meth:`~mrjob.fs.base.Filesystem.join`
    - Use ``runner.fs`` explicitly when accessing filesystem methods (e.g. ``runner.fs.ls()``, not ``runner.ls()``)

   - :command:`mrjob` subcommands
     - :command:`mrjob create-job-flow` is now :command:`mrjob create-cluster`
     - :command:`mrjob terminate-idle-job-flows` is now :command:`mrjob terminate-idle-clusters`
     - :command:`mrjob terminate-job-flow` is now :command:`mrjob temrinate-cluster`

Other changes
^^^^^^^^^^^^^

 - mrjob now requires ``boto`` 2.35.0 or newer (chances are you're already doing this). Later 0.5.x releases of mrjob may require newer versions of ``boto``.
 - :mrjob-opt:`visible_to_all_users` now defaults to ``True``
 - ``HadoopFilesystem.rm()`` uses ``-skipTrash``
 - new :mrjob-opt:`iam_endpoint` option
 - custom :mrjob-opt:`hadoop_streaming_jar`\ s are properly uploaded
 - :py:data:`~mrjob.runner.JOB` :mrjob-opt:`cleanup` on EMR is temporarily disabled
 - mrjob now follows symlinks when :py:meth:`~mrjob.fs.local.LocalFileSystem.ls`\ ing the local filesystem (beware recursive symlinks!)
 - The :mrjob-opt:`interpreter` option disables :mrjob-opt:`bootstrap_mrjob` by default (:mrjob-opt:`interpreter` is meant for non-Python jobs)
 - :ref:`cluster pooling <pooling-clusters>` now respects :mrjob-opt:`ec2_key_pair`
 - cluster self-termination (see :mrjob-opt:`max_hours_idle`) now respects non-streaming jobs
 - :py:class:`~mrjob.fs.local.LocalFilesystem` now rejects URIs rather than interpreting them as local paths
 - ``local`` and ``inline`` runners no longer have a default :mrjob-opt:`hadoop_version`, instead handling :mrjob-opt:`jobconf` in a version-agnostic way
 - :mrjob-opt:`steps_python_bin` now defaults to the current Python interpreter.
 - minor changes to :py:mod:`mrjob.util`:

   - :py:func:`~mrjob.util.file_ext` takes filename, not path
   - :py:func:`~mrjob.util.gunzip_stream` now yields chunks of bytes, not lines
   - moved :py:func:`~mrjob.util.random_identifier` method here from :py:mod:`mrjob.aws`
   - ``buffer_iterator_to_line_iterator()`` is now named :py:func:`~mrjob.util.to_lines`, and no longer appends a trailing newline to data.


0.4.6
-----

``include:`` in conf files can now use relative paths in a meaningful way.
See :ref:`configs-relative-includes`.

List and environment variable options loaded from included config files can
be totally overridden using the ``!clear`` tag. See :ref:`clearing-configs`.

Options that take lists (e.g. :mrjob-opt:`setup`) now treat scalar values
as single-item lists. See :ref:`this example <configs-list-example>`.

Fixed a bug that kept the ``pool_wait_minutes`` option from being loaded from
config files.


0.4.5
-----

This release moves mrjob off the deprecated `DescribeJobFlows <http://docs.aws.amazon.com/ElasticMapReduce/latest/API/API_DescribeJobFlows.html>`_
EMR API call.

.. warning::

    AWS *again* broke older versions mrjob for at least some new accounts, by
    returning 400s for the deprecated `DescribeJobFlows <http://docs.aws.amazon.com/ElasticMapReduce/latest/API/API_DescribeJobFlows.html>`_
    API call. If you have a newer AWS account (circa July 2015), you must
    use at least this version of mrjob.

The new API does not provide a way to tell when a job flow (now called
a "cluster") stopped provisioning instances and started bootstrapping, so the
clock for our estimates of when we are close to the end of a billing hour now
start at cluster creation time, and are thus more conservative.

Related to this change, :py:mod:`~mrjob.emr.tools.terminate_idle_job_flows`
no longer considers job flows in the ``STARTING`` state idle; use
:py:mod:`~mrjob.emr.tools.report_long_jobs` to catch jobs stuck in
this state.

:py:mod:`~mrjob.emr.tools.terminate_idle_job_flows` performs much better
on large numbers of job flows. Formerly, it collected all job flow information
first, but now it terminates idle job flows as soon as it identifies them.

:py:mod:`~mrjob.emr.tools.collect_emr_stats` and
:py:mod:`~mrjob.emr.tools.job_flow_pool` have *not* been ported to the
new API and will be removed in v0.5.0.

Added an :mrjob-opt:`aws_security_token` option to allow you to run
mrjob on EMR using temporary AWS credentials.

Added an :mrjob-opt:`emr_tags` option to allow you to tag EMR job flows
at creation time.

:py:class:`~mrjob.emr.EMRJobRunner` now has a
:py:meth:`~mrjob.emr.EMRJobRunner.get_ami_version` method.

The :mrjob-opt:`hadoop_version` option no longer has any effect in EMR. This
option only every did anything on the 1.x AMIs, which mrjob no longer supports.

Added many missing switches to the EMR tools (accessible from the
:command:`mrjob` command). Formerly, you had to use a
config file to get at these options.

You can now access the :py:mod:`~mrjob.emr.tools.mrboss` tool from the
command line: :command:`mrjob boss <args>`.

Previous 0.4.x releases have worked with boto as old as 2.2.0, but this one
requires at least boto 2.6.0 (which is still more than two years old). In any
case, it's recommended that you just use the latest version of boto.

This branch has a number of additional deprecation warnings, to help prepare
you for mrjob v0.5.0. Please heed them; a lot of deprecated things really are
going to be completely removed.


0.4.4
-----

mrjob now automatically creates and uses IAM objects as necessary to comply
with `new requirements from Amazon Web Services <http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-iam-roles-creatingroles.html>`_.

(You do not need to install the AWS CLI or run ``aws emr create-default-roles``
as the link above describes; mrjob takes care of this for you.)

.. warning::

   The change that AWS made essentially broke all older versions of mrjob for
   all new accounts. If the first time your AWS account created an Elastic
   MapReduce cluster was on or after April 6, 2015, you should use at least
   this version of mrjob.

   If you *must* use an old version of mrjob with a new AWS account, see
   `this thread <https://groups.google.com/forum/#!topic/mrjob/h7-1UYB7O20>`_
   for a possible workaround.

``--iam-job-flow-role`` has been renamed to ``--iam-instance-profile``.

New ``--iam-service-role`` option.

0.4.3
-----

This release also contains many, many bugfixes, one of which probably
affects you! See `CHANGES.txt
<https://github.com/Yelp/mrjob/blob/master/CHANGES.txt>`_ for details.

Added a new subcommand, ``mrjob collect-emr-active-stats``, to collect stats
about active jobflows and instance counts.

``--iam-job-flow-role`` option allows setting of a specific IAM role to run
this job flow.

You can now use ``--check-input-paths`` and ``--no-check-input-paths`` on EMR
as well as Hadoop.

Files larger than 100MB will be uploaded to S3 using multipart upload if you
have the `filechunkio` module installed. You can change the limit/part size
with the ``--s3-upload-part-size`` option, or disable multipart upload by
setting this option to 0.

.. _ready-for-strict-protocols:

You can now require protocols to be strict from :ref:`mrjob.conf <mrjob.conf>`;
this means unencodable input/output will result in an exception rather
than the job quietly incrementing a counter. It is recommended you set this
for all runners:

.. code-block:: yaml

    runners:
      emr:
        strict_protocols: true
      hadoop:
        strict_protocols: true
      inline:
        strict_protocols: true
      local:
        strict_protocols: true

You can use ``--no-strict-protocols`` to turn off strict protocols for
a particular job.

Tests now support pytest and tox.

Support for Python 2.5 has been dropped.


0.4.2
-----

JarSteps, previously experimental, are now fully integrated into multi-step
jobs, and work with both the Hadoop and EMR runners. You can now use powerful
Java libraries such as `Mahout <http://mahout.apache.org/>`_ in your MRJobs.
For more information, see :ref:`non-hadoop-streaming-jar-steps`.

Many options for setting up your task's environment (``--python-archive``,
``setup-cmd`` and ``--setup-script``) have been replaced by a powerful
``--setup`` option. See the :doc:`guides/setup-cookbook` for examples.

Similarly, many options for bootstrapping nodes on EMR (``--bootstrap-cmd``,
``--bootstrap-file``, ``--bootstrap-python-package`` and
``--bootstrap-script``) have been replaced by a single ``--bootstrap``
option. See the :doc:`guides/emr-bootstrap-cookbook`.

This release also contains many `bugfixes
<https://github.com/Yelp/mrjob/blob/master/CHANGES.txt>`_, including
problems with boto 2.10.0+, bz2 decompression, and Python 2.5.

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
:ref:`job flow pooling <pooling-clusters>`, with :mrjob-opt:`max_hours_idle`
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

    More info: :ref:`pooling-clusters`

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
