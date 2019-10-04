What's New
==========

For a complete list of changes, see `CHANGES.txt
<https://github.com/Yelp/mrjob/blob/master/CHANGES.txt>`_

.. _v0.6.11:

0.6.11
------

Adds support for parsing Spark logs and driver output to determine why a job
failed. This works with with the local, Hadoop, EMR, and Spark runners.

The Spark runner no longer needs :py:mod:`pyspark` in the ``$PYTHONPATH`` to
launch scripts with :command:`spark-submit` (it still needs :py:mod:`pyspark`
to use the Spark harness).

On Python 3.7, you can now intermix positional arguments to
:py:class:`~mrjob.job.MRJob` with switches, similar to how you could back when
mrjob used :py:mod:`optparse`. For example:
:command:`mr_your_script.py file1 -v file2`.

On EMR, the default :mrjob-opt:`image_version` (AMI) is now 5.27.0.

Restored ``m4.large`` as the default instance type pre-5.13.0 AMIs, as they
do not support ``m5.xlarge``. (``m5.xlarge`` is still the default for AMI
5.13.0 and later.)

mrjob can now retry on transient AWS API errors (e.g. throttling) or network
errors when making API calls that use pagination (e.g. listing clusters).

The :mrjob-opt:`emr_configurations` opt now supports the ``!clear`` tag
rather than crashing. You may also override individual configs by setting
a config with the same ``Classification``.

This version restores official support for Python 3.4, as it's the version
of Python 3 installed on EMR AMIs prior to 5.20.0. In order to make this work,
mrjob drops support for Google Cloud services in Python 3.4, as the recent
Google libraries appear to need a later Python version.

.. _v0.6.10:

0.6.10
------

Adds official support for PyPy (that is any version of it compatible with
Python 2.7/3.5+). If you launch a job in PyPy :mrjob-opt:`python_bin` will
automatically default to ``pypy`` or ``pypy3`` as appropriate.

Note that mrjob does not auto-install PyPy for you on EMR (Amazon Linux does
not provide a PyPy package). Installing PyPy yourself at bootstrap time is
fairly straightforward, see :ref:`installing-pypy-on-emr`.

The Spark harness can now be used on EMR, allowing you to run "classic"
MRJobs in Spark, which is often faster. Essentially, you launch jobs in
the Spark runner with ``--spark-submit-bin 'mrjob spark-submit -r emr'``;
see :ref:`mrjobs-on-spark-on-emr` for details.

The Spark runner can now optionally disable internal protocols when running
"classic" MRJobs, eliminating the (usually) unnecessary effort of encoding data
structures into JSON or other string representations and then decoding
them. See :mrjob-opt:`skip_internal_protocol` for details.

The EMR runner's default instance type is now ``m5.xlarge``, which works
with newer reasons and should make it easier to run Spark jobs. The EMR runner
also now logs the DNS of the master node as soon as it is available, to make
it easier to SSH in.

Finally, mrjob gives a much clearer error message if you attempt to read a YAML
mrjob.conf file without :mod:`PyYAML` installed.

.. _v0.6.9:

0.6.9
-----

Drops support for Python 3.4.

Fixes a bug introduced in :ref:`v0.6.8` that could break archives or
directories uploaded into Hadoop or Spark if the name of the unpacked archive
didn't have an archive extension (e.g. ``.tar.gz``).

The Spark runner can now optionally emulate Hadoop's
``mapreduce.map.input.file`` configuration property when running the mapper of
the first step of a streaming job if you enable
:mrjob-opt:`emulate_map_input_file`. This means that jobs that depend on
:py:func:`jobconf_from_env('mapreduce.map.input.file') <mrjob.compat.jobconf_from_env>`
will still work.

The Spark runner also now uses the correct argument names when emulating
:py:meth:`~mrjob.job.MRJob.increment_counter`, and logs a warning if
:mrjob-opt:`spark_tmp_dir` doesn't match :mrjob-opt:`spark_master`.

:ref:`mrjob spark-submit <spark-submit>` can now pass switches to the
Spark script/JAR without explicitly separating them out with ``--``.

The local and inline runners now more correctly emulate the
`mapreduce.map.input.file` config property by making it a ``file://`` URL.

Deprecated methods :py:meth:`~mrjob.job.MRJob.add_file_option` and
:py:meth:`~mrjob.job.MRJob.add_passthrough_option` can now take a type
(e.g. ``int``) as their ``type`` argument, to better emulate :mod:`optparse`.

.. _v0.6.8:

0.6.8
-----

Nearly full support for Spark
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This release adds nearly full support for Spark, including mrjob-speific
features like :mrjob-opt:`setup` scripts and
:ref:`passthrough options <writing-cl-opts>`. See
:ref:`why-mrjob-with-spark` for everything mrjob can do with Spark.

This release adds a :py:class:`~mrjob.spark.runner.SparkMRJobRunner`
(``-r spark``), which
works with any Spark installation, does not require Hadoop, and can access any
filesystem supported by both mrjob and Spark (HDFS, S3, GCS). The Spark runner
is now the default for :ref:`mrjob spark-submit <spark-submit>`.

What's *not* supported? mrjob does not yet support Spark on Google Cloud
Dataproc. The Spark runner does not yet parse logs to determine probable
cause of failure when your job fails (though it does give you the
Spark driver output).

Spark Hadoop Streaming emulation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Not only does the Spark runner not need Hadoop to run Spark jobs, it doesn't
need Hadoop to run most *Hadoop Streaming* jobs, as it knows how to run them
directly on Spark. This means if you want to migrate to a
non-Hadoop Spark cluster, you can take all your old
:py:class:`~mrjob.job.MRJob`\s with you. See :ref:`classic-mrjobs-on-spark`
for details.

The "experimental harness script" mentioned in :ref:`v0.6.7` is now fully
integrated into the Spark runner and is no longer supported as a separate
feature.

Local runner support for Spark
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``local`` and ``inline`` runner can now run Spark scripts locally for
testing, analogous to the way they've supported Hadoop streaming scripts
(except that they *do* require a local Spark installation). See
:ref:`other-ways-to-run-on-spark`.

Other Spark improvements
^^^^^^^^^^^^^^^^^^^^^^^^

:py:class:`~mrjob.job.MRJob`\s are now Spark-serializable without calling
:py:meth:`~mrjob.job.MRJob.sandbox` (there used to be a problematic reference
to ``sys.stdin``). This means you can always pass job methods to
``rdd.flatMap()`` etc.

:mrjob-opt:`setup` scripts are no longer a YARN-specific feature, working
on all Spark masters (except
``local[*]``, which doesn't give executors a separate working directory).

Likewise, you can now specify a different name for files in the job's
working directory (e.g. ``--file foo#bar``) on all Spark masters.

.. note::

   Uploading archives and directories still only works on YARN
   for now; Spark considers ``--archives`` a YARN-specific feature.

When running on a local Spark cluster, uses ``file://...`` rather than just
the path of the file when necessary (e.g. with ``--py-files``).

:py:meth:`~mrjob.runner.MRJobRunner.cat_output` now ignores files and
subdirectories starting with ``"."`` (used to only be ``"_"``). This allows
mrjob to ignore Spark's checksum files (e.g. ``.part-00000.crc``), and also
brings mrjob in closer compliance to the way Hadoop input formats
read directories.

``spark.yarn.appMasterEnv.*`` config properties are only set if you're
actually running on YARN.

The values of :mrjob-opt:`spark_master` and :mrjob-opt:`spark_deploy_mode` can
no longer be overridden with configuration properties
(``-D spark.master=...``). While not exactly a "feature," this means that mrjob
always knows what Spark platform it's running on.

Filesystems
^^^^^^^^^^^

Every runner has an ``fs`` attribute that gives access to all the filesystems
that runner supports.

Added a :py:meth:`~mrjob.fs.base.Filesystem.put` method to all filesystems,
which allows uploading a single file (it used to be that each runner had
custom logic for uploads).

It also used to be that if you wanted to create a bucket on S3 or GCS, you had
to call ``create_bucket(...)`` explicitly. Now
:py:meth:`~mrjob.fs.base.Filesystem.mkdir` will automatically create buckets
as needed.

If you still need to access methods specific to a filesystem, you should do so
through ``fs.<name>``, where ``<name>`` is the (lowercase) name of the
storage service. For example the Spark runner's filesystem offers both
``runner.fs.s3.create_bucket()`` and ``runner.fs.gcs.create_bucket()``.
The old style of implicitly passing through FS-specific methods
(``runner.fs.create_bucket(...)``) is deprecated and going away in v0.7.0.

:py:class:`~mrjob.fs.gcs.GCSFilesystem`\'s constructor had a useless
``local_tmp_dir`` argument, which is now deprecated and going away in v0.7.0.

EMR
^^^

Fixed a bad bug introduced in :ref:`v0.6.7` that could prevent mrjob from
running on EMR with a non-default temp bucket.

You can now set sub-parameters with :mrjob-opt:`extra_cluster_params`. For
example, you can now do:

.. code-block:: sh

   --extra-cluster-param Instances.EmrManagedMasterSecurityGroup=...

without clobbering the zone or instance group/fleet configs
specified in ``Instances``.

Running your job with ``--subnet ''`` now un-sets a :mrjob-opt:`subnet`
specified in your config file (used to be ignored).

If you are using cluster pooling with retries (:mrjob-opt:`pool_wait_minutes`),
mrjob now retains information about clusters that is immutable
(e.g. AMI version), saving API calls.

Dependency upgrades
^^^^^^^^^^^^^^^^^^^

Bumped the required versions of several Google Cloud Python libraries to be
more compatible with current versions of their sub-dependencies
(Google libraries pin a fairly narrow range of dependencies). :py:mod:`mrjob`
now requires:

  * :py:mod:`google-cloud-dataproc` at least 0.3.0,
  * :py:mod:`google-cloud-logging` at least 1.9.0, and
  * :py:mod:`google-cloud-storage` at least 1.13.1.

Also dropped support for :py:mod:`PyYAML` 3.08; now we require at least
:py:mod:`PyYAML` 3.10 (which came out in 2011).

.. note::

  We are aware that the Google libraries' extensive dependencies can be a
  nuisance for mrjob users who don't use Google Cloud. Our tentative
  plan is to make dependencies specific to a third-party service (including
  :py:mod:`google-cloud-*` and :py:mod:`boto3`) optional starting in v0.7.0.

Other bugfixes
^^^^^^^^^^^^^^

Fixed a long-standing bug that would cause the Hadoop runner to hang or raise
cryptic errors if :mrjob-opt:`hadoop_bin` or :mrjob-opt:`spark_submit_bin`
is not executable.

Support files for ``mrjob.examples`` (e.g. ``stop_words.txt`` for
:py:class:`~mrjob.examples.mr_most_used_word.MRMostUsedWord`) are now
installed along with :py:mod:`mrjob`.

Setting a `*_bin` option to an empty value (e.g. ``--hadoop-bin``) now
always instructs mrjob to use the default, rather than disabling core
features or creating cryptic errors. This affects :mrjob-opt:`gcloud_bin`,
:mrjob-opt:`hadoop_bin`, :mrjob-opt:`sh_bin`, and :mrjob-opt:`ssh_bin`;
the various `*python_bin` options already worked this way.

.. _v0.6.7:

0.6.7
-----

:mrjob-opt:`setup` commands now work on Spark (at least on YARN).

Added the :ref:`mrjob spark-submit <spark-submit>` subcommand, which works
as a drop-in replacement for :command:`spark-submit` but with mrjob runners
(e.g EMR) and mrjob features (e.g. :mrjob-opt:`setup`, :mrjob-opt:`cmdenv`).

Fixed a bug that was causing idle timeout scripts to silently fail
on 2.x EMR AMIs.

Fixed a bug that broke :py:meth:`~mrjob.fs.s3.S3Filesystem.create_bucket`
on ``us-east-1``, preventing new mrjob installations from launching on EMR
in that region.

Fixed an :py:class:`ImportError` from attempting to import
:py:data:`os.SIGKILL` on Windows.

The default instance type on EMR is now ``m4.large``.

EMR's cluster pooling now knows the CPU and memory capacity of ``c5`` and
``m5`` instances, allowing it to join "better" clusters.

Added the plural form of several switches (separate multiple values with
commas):

 * ``--applications``
 * ``--archives``
 * ``--dirs``
 * ``--files``
 * ``--libjars``
 * ``--py-files``

Except for ``--application``, the singular version of these switches
(``--archive``, ``--dir``, ``--file``, ``--libjar``, ``--py-file``) is
deprecated for consistency with Hadoop and Spark

:mrjob-opt:`sh_bin` is now fully qualified by default (``/bin/sh -ex``,
not ``sh -ex``). :mrjob-opt:`sh_bin` may no longer be empty, and a warning
is issued if it has more than one argument, to properly support shell script
shebangs (e.g. ``#!/bin/sh -ex``) on Linux.

Runners no longer call :py:class:`~mrjob.job.MRJob`\s with ``--steps``;
instead the job passes its step description to the runner on instantiation.
``--steps`` and :mrjob-opt:`steps_python_bin` are now deprecated.

The Hadoop and EMR runner can now set ``SPARK_PYTHON`` and
``SPARK_DRIVER_PYTHON`` to different values if need be (e.g. to
match :mrjob-opt:`task_python_bin`, or to support :mrjob-opt:`setup`
scripts in client mode).

The inline runner no longer attempts to run command substeps.

The inline and local runner no longer silently pretend to run
non-streaming steps.

The Hadoop runner no longer has the :mrjob-opt:`bootstrap_spark` option,
which did nothing.

:mrjob-opt:`interpreter` and :mrjob-opt:`steps_interpreter` are deprecated,
in anticipation in removing support for writing MRJobs in other
programming languages.

Runners now issue a warning if they receive options that belong to other
runners (e.g. passing :mrjob-opt:`image_version` to the Hadoop runner).

:command:`mrjob create-cluster` now supports ``--emr-action-on-failure``.

Updated deprecate escape sequences in mrjob code that would break
on Python 3.8.

``--help`` message for mrjob subcommands now correctly includes the
subcommand in ``usage``.

mrjob no longer raises :py:class:`AssertionError`, instead raising
:py:class:`ValueError`.

Added an experimental harness script (in ``mrjob/spark``) to run basic
MRJobs on Spark, potentially without Hadoop:

.. code-block:: sh

   spark-submit mrjob_spark_harness.py module.of.YourMRJob input_path output_dir

Added :py:meth:`~mrjob.job.MRJob.map_pairs`,
:py:meth:`~mrjob.job.MRJob.reduce_pairs`,
and :py:meth:`~mrjob.job.MRJob.combine_pairs` methods to
:py:class:`~mrjob.job.MRJob`, to enable the Spark harness script.

.. _v0.6.6:

0.6.6
-----

Fixes a longstanding bug where boolean :mrjob-opt:`jobconf` values
were passed to Hadoop in Python format (``True`` instead of ``true``). You
can now do safely do something like this:

.. code-block:: yaml

   runners:
     emr:
       jobconf:
         mapreduce.output.fileoutputformat.compress: true

whereas in prior versions of mrjob, you had to use ``"true"`` in quotes.

Added ``-D`` as a synonym for ``--jobconf``, to match Hadoop.

On EMR, if you have SSH set up (see :ref:`ssh-tunneling`)
mrjob can fetch your history log directly from HDFS, allowing it
to more quickly diagnose why your job failed.

Added a ``--local-tmp-dir`` switch. If you set :mrjob-opt:`local_tmp_dir`
to empty string, mrjob will use the system default.

You can now pass multiple arguments to Hadoop ``--hadoop-args``
(for example, ``--hadoop-args='-fs hdfs://namenode:port'``), rather
than having to use ``--hadoop-arg`` one argument at time. ``--hadoop-arg``
is now deprecated.

Similarly, you can use ``--spark-args`` to pass arguments to
``spark-submit`` in place of the now-deprecated ``--spark-arg``.

mrjob no longer automatically passes generic arguments (``-D`` and
``-libjars``) to :py:class:`~mrjob.step.JarStep`\s, because this confuses
some JARs. If you want mrjob to pass generic arguments to a JAR, add
:py:data:`~mrjob.step.GENERIC_ARGS` to your
:py:class:`~mrjob.step.JarStep`\'s *args* keyword argument, like you would
with :py:data:`~mrjob.step.INPUT` and :py:data:`~mrjob.step.OUTPUT`.

The Hadoop runner now has a :mrjob-opt:`spark_deploy_mode` option.

Fixed the ``usage: usage:`` typo in ``--help`` messages.

:py:meth:`mrjob.job.MRJob.add_file_arg`
can now take an explicit ``type=str`` (used to cause an error).

The deprecated ``optparse`` emulation methods
:py:meth:`~mrjob.job.MRJob.add_file_option` and
:py:meth:`~mrjob.job.MRJob.add_passthrough_option`
now support ``type='str'`` (used to only accept ``type='string'``).

Fixed a permissions error that was breaking ``inline`` and ``local`` mode
on some versions of Windows.

.. _v0.6.5:

0.6.5
-----

This release fixes an issue with self-termination of idle clusters on EMR
(see :mrjob-opt:`max_mins_idle`) where the master node sometimes
simply ignored ``sudo shutdown -h now``. The idle self termination script
now logs to ``bootstrap-actions/mrjob-idle-termination.log``.

.. note::

   If you are using :ref:`cluster-pooling`, it's highly recommended you upgrade
   to this version to fix the self-termination issue.

You can now turn off log parsing (on all runners) by setting
:mrjob-opt:`read_logs` to false. This can speed up cases where you don't care
why a job failed (e.g. integration tests) or where you'd rather use the
:ref:`diagnose-tool` tool after the fact.

You may specify custom AMIs with the :mrjob-opt:`image_id` option. To find
Amazon Linux AMIs compatible with EMR that you can use as a base for your
custom image, use :py:func:`~mrjob.ami.describe_base_emr_images`.

The default AMI on EMR is now 5.16.0.

New EMR clusters launched by mrjob will be automatically tagged with
``__mrjob_label`` (filename of your mrjob script) and ``__mrjob_owner``
(your username), to make it easier to understand your mrjob usage in
`CloudWatch <https://aws.amazon.com/cloudwatch/>`_ etc. You can change the
value of these tags with the :mrjob-opt:`label` and :mrjob-opt:`owner` options.

You may now set the root EBS volume size for EMR clusters directly with
:mrjob-opt:`ebs_root_volume_gb` (you used to have to use
:mrjob-opt:`instance_groups` or :mrjob-opt:`instance_fleets`).

API clients returned by :py:class:`~mrjob.emr.EMRJobRunner` now retry on
SSL timeouts. EMR clients returned by
:py:meth:`mrjob.emr.EMRJobRunner.make_emr_client` won't retry faster than
:mrjob-opt:`check_cluster_every`, to prevent throttling.

Cluster pooling recovery (relaunching a job when your pooled cluster
self-terminates) now works correctly on single-node clusters.

.. _v0.6.4:

0.6.4
-----

This release makes it easy to attach static files to your
:py:class:`~mrjob.job.MRJob`
with the :py:attr:`~mrjob.job.MRJob.FILES`, :py:attr:`~mrjob.job.MRJob.DIRS`,
and :py:attr:`~mrjob.job.MRJob.ARCHIVES` attributes.

In most cases, you no longer need :mrjob-opt:`setup` scripts to access other
python modules or packages from your job because you can use
:py:attr:`~mrjob.job.MRJob.DIRS` instead. For more details, see
:ref:`uploading-modules-and-packages`.

For completeness, also
added :py:meth:`~mrjob.job.MRJob.files`,
:py:meth:`~mrjob.job.MRJob.dirs`, and :py:meth:`~mrjob.job.MRJob.archives`
methods.

:ref:`terminate-idle-clusters` now skips termination-protected idle clusters,
rather than crashing (this is fixed in :ref:`v0.5.12`, but not
previous 0.6.x versions).

Python 3.3 is no longer supported.

mrjob now requires :mod:`google-cloud-dataproc` 0.2.0+ (this
library used to be vendored).

.. _v0.6.3:

0.6.3
-----

Read arbitrary file formats
^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can now pass entire files in any format to your mapper by defining
:py:meth:`~mrjob.job.MRJob.mapper_raw`. See :ref:`raw-input` for an example.

Google Cloud Datatproc parity
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

mrjob now offers feature parity between Google Cloud Dataproc
and Amazon Elastic MapReduce. Support for :doc:`guides/spark`
and :mrjob-opt:`libjars` will be added in a future release.
(There is no plan to introduce :ref:`cluster-pooling` with Dataproc.)

Specifically, :py:class:`~mrjob.dataproc.DataprocJobRunner` now supports:

* fetching and parsing counters
* parsing logs for probable cause of failure
* job progress messages (% complete)
* :ref:`non-hadoop-streaming-jar-steps`
* these config options:

  * :mrjob-opt:`cloud_part_size_mb` (chunked uploading)
  * :mrjob-opt:`core_instance_config`, :mrjob-opt:`master_instance_config`,
    :mrjob-opt:`task_instance_config`
  * :mrjob-opt:`hadoop_streaming_jar`
  * :mrjob-opt:`network`/:mrjob-opt:`subnet` (running in a VPC)
  * :mrjob-opt:`service_account` (custom IAM account)
  * :mrjob-opt:`service_account_scopes` (fine-grained permissions)
  * :mrjob-opt:`ssh_tunnel`/:mrjob-opt:`ssh_tunnel_is_open` (resource manager)

Improvements to existing Dataproc features:

* :mrjob-opt:`bootstrap` scripts run in a temp dir, rather than ``/``
* uses Dataproc's built-in auto-termination feature, rather than a script
* GCS filesystem:

  * :py:meth:`~mrjob.fs.gcs.GCSFilesystem.cat` streams data rather than dumping
    to a temp file
  * :py:meth:`~mrjob.fs.gcs.GCSFilesystem.exists` no longer swallows all
    exceptions

To get started, read :ref:`google-setup`.

Other changes
^^^^^^^^^^^^^

mrjob no longer streams your job output to the command line if you specify
:mrjob-opt:`output_dir`. You can control this with the :command:`--cat-output`
and :command:`--no-cat-output` switches (:command:`--no-output` is deprecated).

`cloud_upload_part_size` has been renamed to :mrjob-opt:`cloud_part_size_mb`
(the old name will work until v0.7.0).

mrjob can now recognize "not a valid JAR" errors from Hadoop and suggest
them as probable cause of job failure.

mrjob no longer depends on :mod:`google-cloud` (which implies several other
Google libraries). Its current Google library dependencies are
:mod:`google-cloud-logging` 1.5.0+ and :mod:`google-cloud-storage` 1.9.0+.
Future versions of mrjob will depend on :mod:`google-cloud-dataproc` 0.11.0+
(currently included with mrjob because it hasn't yet been released).

:py:class:`~mrjob.retry.RetryWrapper` now sets ``__name__`` when wrapping
methods, making for easier debugging.

.. _v0.6.2:

0.6.2
-----

mrjob is now orders of magnitude quicker at parsing logs, making it practical
to diagnose rare errors from very large jobs. However, on some AMIs, it can no
longer parse errors without waiting for logs to transfer to S3 (this may be
fixed in a future version).

To run jobs on Google Cloud Dataproc, mrjob no longer requires you to install
the :command:`gcloud` util (though if
you do have it installed, mrjob can read credentials from its configs). For
details, see :doc:`guides/dataproc-quickstart`.

mrjob no longer requires you to select a Dataproc :mrjob-opt:`zone` prior
to running jobs. Auto zone placement (just set :mrjob-opt:`region` and let
Dataproc pick a zone) is now enabled, with the default being auto zone
placement in ``us-west1``. mrjob no longer reads zone and region from
:command:`gcloud`\'s compute engine configs.

mrjob's Dataproc code has been ported from the ``google-python-api-client``
library (which is in maintenance mode) to ``google-cloud-sdk``, resulting in
some small changes to the GCS filesystem API. See `CHANGES.txt
<https://github.com/Yelp/mrjob/blob/master/CHANGES.txt>`_ for details.

Local mode now has a :mrjob-opt:`num_cores` option that allow you to control
how tasks it handles simultaneously.

.. _v0.6.1:

0.6.1
-----

Added the :ref:`diagnose-tool` tool (run
:command:`mrjob diagnose j-CLUSTERID`), which determines why a previously run
job failed.

Fixed a serious bug that made mrjob unable to properly parse error logs
in some cases.

Added the :py:meth:`~mrjob.emr.EMRJobRunner.get_job_steps` method to
:py:class:`~mrjob.emr.EMRJobRunner`.

.. _v0.6.0:

0.6.0
-----

Dropped Python 2.6
^^^^^^^^^^^^^^^^^^

mrjob now supports Python 2.7 and Python 3.3+. (Some versions of PyPy
also work but are not officially supported.)

boto3, not boto
^^^^^^^^^^^^^^^

mrjob now uses :py:mod:`boto3` rather than :py:mod:`boto` to talk to AWS.
This makes it much simpler to pass user-defined data structures directly
to the API, enabling a number of features.

At least version 1.4.6 of :py:mod:`boto3` is required to run jobs on EMR.

It is now possible to fully configure instances (including EBS volumes).
See :mrjob-opt:`instance_groups` for an example.

mrjob also now supports Instance Fleets, which may be fully configured
(including EBS volumes) through the :mrjob-opt:`instance_fleets` option.

Methods that took or returned :py:mod:`boto` objects (for example,
``make_emr_conn()``) have been completely removed as there as no way
to make a deprecated shim for them without keeping :py:mod:`boto` as a
dependency. See :py:class:`~mrjob.emr.EMRJobRunner` and
:py:class:`~mrjob.fs.s3.S3Filesystem` for new method names.

Note that :py:mod:`boto3` reads temporary credentials from
:envvar:`$AWS_SESSION_TOKEN`,
not :envvar:`$AWS_SECURITY_TOKEN` as in :py:mod:`boto` (see
:mrjob-opt:`aws_session_token` for details).

argparse, not optparse
^^^^^^^^^^^^^^^^^^^^^^

mrjob now uses :py:mod:`argparse` to parse options, rather than
:py:mod:`optparse`, which has been deprecated since Python 2.7.

:py:mod:`argparse` has slightly different option-parsing logic. A couple
of things you should be aware of:

 * everything that starts with ``-`` is assumed to be a switch.
   ``--hadoop-arg=-verbose`` works, but ``--hadoop-arg -verbose`` does not.
 * positional arguments may not be split.
   ``mr_wc.py CHANGES.txt LICENSE.txt -r local`` will work, but
   ``mr_wc.py CHANGES.txt -r local LICENSE.txt`` will not.

Passthrough options, file options, etc. are now handled with
:py:meth:`~mrjob.job.MRJob.add_file_arg`,
:py:meth:`~mrjob.job.MRJob.add_passthru_arg`,
:py:meth:`~mrjob.job.MRJob.configure_args`,
:py:meth:`~mrjob.job.MRJob.load_args`, and
:py:meth:`~mrjob.job.MRJob.pass_arg_through`. The old
methods with "option" in their name are deprecated but still work.

As part of this refactor, `OptionStore` and its subclasses have been removed;
options are now handled by runners directly.

Chunks, not lines
^^^^^^^^^^^^^^^^^

mrjob no longer assumes that job output will be line-based. If you
:ref:`run your job programmatically <runners-programmatically>`, you should
read your job output with :py:meth:`~mrjob.runner.MRJobRunner.cat_output`,
which yields bytestrings which don't necessarily correspond to lines, and run
these through :py:meth:`~mrjob.job.MRJob.parse_output`, which will convert
them into key/value pairs.

``runner.fs.cat()`` also now yields arbitrary bytestrings, not lines. When it
yields from multiple files, it will yield an empty bytestring (``b''``)
between the chunks from each file.

:py:func:`~mrjob.util.read_file` and :py:func:`~mrjob.util.read_input` are
now deprecated because they are line-based. Try
:py:func:`~mrjob.cat.decompress`, :py:func:`~mrjob.cat.to_chunks`, and
:py:func:`~mrjob.util.to_lines`.

Better local/inline mode
^^^^^^^^^^^^^^^^^^^^^^^^

The sim runners (``inline`` and ``local`` mode) have been completely
rewritten, making it possible to fix a number of outstanding issues.

Local mode now runs one mapper/reducer per CPU, using
:py:mod:`multiprocesssing`, for faster results.

We only sort by reducer key (not the full line) unless
:py:attr:`~mrjob.job.SORT_VALUES` is set, exposing bad assumptions sooner.

The :mrjob-opt:`step_output_dir` option is now supported, making it easier to
debug issues in intermediate steps.

Files in tasks' (e.g. mappers') working directories are marked user-executable,
to better imitate Hadoop Distributed Cache. When possible, we also symlink
to a copy of each file/archive in the "cache," rather than copying them.

If :py:func:`os.symlink` raises an exception, we fall back to copying (this
can be an issue in Python 3 on Windows).

Tasks are run more like they are in Hadoop; input is passed through stdin,
rather than as script arguments. :py:mod:`mrjob.cat` is no longer executable
because local mode no longer needs it.

Cloud runner improvements
^^^^^^^^^^^^^^^^^^^^^^^^^

Much of the common code for the "cloud" runners (Dataproc and EMR) has been
merged, so that new features can be rolled out in parallel.

The :mrjob-opt:`bootstrap` option (for both Dataproc and EMR) can now take
archives and directories as well as files, like the :mrjob-opt:`setup`
option has since version :ref:`v0.5.8`.

The :mrjob-opt:`extra_cluster_params` option allows you to pass arbitrary
JSON to the API at cluster create time (in Dataproc and EMR). The old
`emr_api_params` option is deprecated and disabled.

:mrjob-opt:`max_hours_idle` has been replaced with :mrjob-opt:`max_mins_idle`
(the old option is deprecated but still works). The default is 10 minutes.
Due to a bug, smaller numbers of minutes might cause the cluster to terminate
before the job runs.

It is no longer possible for mrjob to launch a cluster that sits idle
indefinitely (except by setting :mrjob-opt:`max_mins_idle` to an unreasonably
high value). It is still a good idea to run :ref:`report-long-jobs` because
mrjob can't tell if a running job is doing useful work or has stalled.

EMR now bills by the second, not the hour
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Elastic MapReduce recently stopped billing by the full hour, and now
bills by the second. This means that :ref:`cluster-pooling` is no longer
a cost-saving strategy, though developers might find it handy to reduce
wait times when testing.

The :mrjob-opt:`mins_to_end_of_hour` option no longer makes sense, and
has been deprecated and disabled.

:ref:`audit-emr-usage` has been updated to use billing by the second
when approximating time billed and waste.

.. note::

   Pooling was enabled by default for some development versions of v0.6.0,
   prior to the billing change. This did not make it into the release; you
   must still explicitly turn on
   :ref:`cluster pooling <cluster-pooling>`.

Other EMR changes
^^^^^^^^^^^^^^^^^

The default AMI is now 5.8.0. Note that this means you get Spark 2 by default.

Regions are now case-sensitive, and the ``EU`` alias for ``eu-west-1`` no
longer works.

Pooling no longer adds dummy arguments to the master bootstrap script, instead
setting the ``__mrjob_pool_hash`` and ``__mrjob_pool_name`` tags on the
cluster.

mrjob automatically adds the ``__mrjob_version`` tag to clusters it creates.

Jobs will not add tags to clusters they join rather than create.

:mrjob-opt:`enable_emr_debugging` now works on AMI 4.x and later.

AMI 2.4.2 and earlier are no longer supported (no Python 2.7). There is
no longer any special logic for the "latest" AMI alias (which the API no
longer supports).

The SSH filesystem no longer dumps file contents to memory.

Pooling will only join a cluster with enough *running* instances to meet its
specifications; *requested* instances no longer count.

Pooling is now aware of EBS (disk) setup.

Pooling won't join a cluster that has extra instance types that don't have
enough memory or disk space to run your job.

Errors in bootstrapping scripts are no longer dumped as JSON.

:mrjob-opt:`visible_to_all_users` is deprecated.

Massive purge of deprecated code
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

About a hundred functions, methods, options, and more that were deprecated in
v0.5.x have been removed. See `CHANGES.txt
<https://github.com/Yelp/mrjob/blob/master/CHANGES.txt>`_ for details.

.. _v0.5.12:

0.5.12
------

`This release came out after v0.6.3. It was mostly a backport from v0.6.x.`

Python 2.6 and 3.3 are no longer supported.

:py:func:`mrjob.parse.parse_s3_uri` handles ``s3a://`` URIs.

:ref:`terminate-idle-clusters` now skips termination-protected idle clusters,
rather than crashing.

Since `Amazon no longer bills by the full hour <https://aws.amazon.com/about-aws/whats-new/2017/10/amazon-emr-now-supports-per-second-billing/>`__,
the :mrjob-opt:`mins_to_end_of_hour` option now defaults to 60, effectively
disabling it.

When mrjob passes an environment dictionary to subprocesses, it ensures
that the keys and values are always :py:class:`str`\s (this mostly affects
Python 2 on Windows).

.. _v0.5.11:

0.5.11
------

The :ref:`report-long-jobs` utility can now ignore certain clusters based on
EMR tags.

This version deals more gracefully with clusters that use instance fleets,
preventing crashes that may occur in some rare edge cases.

.. _v0.5.10:

0.5.10
------

Fixed an issue where bootstrapping mrjob on Dataproc or EMR could stall if
mrjob was already installed.

The `aws_security_token` option has been renamed to
:mrjob-opt:`aws_session_token`. If you want to set it via environment
variable, you still have to use :envvar:`$AWS_SECURITY_TOKEN` because that's
what boto uses.

Added protocol support for :py:mod:`rapidjson`; see
:py:class:`~mrjob.protocol.RapidJSONProtocol` and
:py:class:`~mrjob.protocol.RapidJSONValueProtocol`. If available,
:py:mod:`rapidjson` will be used as the default JSON implementation if
:py:mod:`ujson` is not installed.

The master bootstrap script on EMR and Dataproc now has the correct
file extension (``.sh``, not ``.py``).

.. _v0.5.9:

0.5.9
-----

Fixed a bug that prevented :mrjob-opt:`setup` scripts from working on EMR AMIs
5.2.0 and later. Our workaround should be completely transparent unless
you use a custom shell binary; see :mrjob-opt:`sh_bin` for details.

The EMR runner now correctly re-starts the SSH tunnel to the job
tracker/resource manager when a cluster it tries to run a job on
auto-terminates. It also no longer requires a working SSH tunnel to
fetch job progress (you still a working SSH; see
:mrjob-opt:`ec2_key_pair_file`).

The `emr_applications` option has been renamed to :mrjob-opt:`applications`.

The :ref:`terminate-idle-clusters` utility is now slightly more robust in
cases where your S3 temp directory is an different region from your clusters.

Finally, there a couple of changes that probably only matter if you're trying
to wrap your Hadoop tasks (mappers, reducers, etc.) in :command:`docker`:

* You can set *just* the python binary for tasks with
  :mrjob-opt:`task_python_bin`. This allows you to use a wrapper script in
  place of Python without perturbing :mrjob-opt:`setup` scripts.
* Local mode now no longer relies on an absolute path to access the
  :py:mod:`mrjob.cat` utility it uses to handle compressed input files;
  copying the job's working directory into Docker is enough.

.. _v0.5.8:

0.5.8
-----

You can now pass directories to jobs, either directly with the
:mrjob-opt:`upload_dirs` option, or through :mrjob-opt:`setup` commands.
For example:

.. code-block:: sh

   --setup 'export PYTHONPATH=$PYTHONPATH:your-src-code/#'

mrjob will automatically tarball these directories and pass them to Hadoop as
archives.

For multi-step jobs, you can now specify where inter-step output goes
with :mrjob-opt:`step_output_dir` (``--step-output-dir``), which can be useful
for debugging.

All :py:mod:`job step types <mrjob.step>` now take the *jobconf* keyword
argument to set Hadoop properties for that step.

Jobs' ``--help`` printout is now better-organized and less verbose.

Made several fixes to pre-filters (commands that pipe into streaming steps):

* you can once again add pre-filters to a single step job by re-defining
  :py:meth:`~mrjob.job.MRJob.mapper_pre_filter`,
  :py:meth:`~mrjob.job.MRJob.combiner_pre_filter`, and/or
  :py:meth:`~mrjob.job.MRJob.reducer_pre_filter`
* local mode now ignores non-zero return codes from pre-filters (this
  matters for BSD grep)
* local mode can now run pre-filters on compressed input files

mrjob now respects :mrjob-opt:`sh_bin` when it needs to wrap a command
in ``sh`` before passing it to Hadoop (e.g. to support pipes)

On EMR, mrjob now fetches logs from task nodes when determining probable cause
of error, not just core nodes (the ones that run tasks and host HDFS).

Several unused functions in :py:mod:`mrjob.util` are now deprecated:

* :py:func:`~mrjob.util.args_for_opt_dest_subset`
* :py:func:`~mrjob.util.bash_wrap`
* :py:func:`~mrjob.util.populate_option_groups_with_options`
* :py:func:`~mrjob.util.scrape_options_and_index_by_dest`
* :py:func:`~mrjob.util.tar_and_gzip`

:py:func:`~mrjob.cat.bunzip2_stream` and :py:func:`~mrjob.cat.gunzip_stream`
have been moved from :py:mod:`mrjob.util` to :py:mod:`mrjob.cat`.

:py:meth:`SSHFilesystem.ssh_slave_hosts() <mrjob.fs.ssh.SSHFilesystem.ssh_slave_hosts>` has been deprecated.

Option group attributes in :py:class:`~mrjob.job.MRJob`\s have been deprecated,
as has the :py:meth:`~mrjob.job.MRJob.get_all_option_groups` method.


.. _v0.5.7:

0.5.7
-----

Spark and related changes
^^^^^^^^^^^^^^^^^^^^^^^^^

mrjob now supports running Spark jobs on your own Hadoop cluster or
Elastic MapReduce. mrjob provides significant benefits over Spark's
built-in Python support; see :ref:`why-mrjob-with-spark` for details.

Added the :mrjob-opt:`py_files` option, to put `.zip` or `.egg` files in your
job's ``PYTHONPATH``. This is based on a Spark feature, but it works with
streaming jobs as well. mrjob is now bootstrapped (see
:mrjob-opt:`bootstrap_mrjob`) as a `.zip` file rather than a tarball.
If for some reason, the bootstrapped mrjob library won't compile, you'll
get much cleaner error messages.

The default AMI version on EMR (see :mrjob-opt:`image_version`) has been bumped
from 3.11.0 to 4.8.2, as 3.11.0's Spark support is spotty.

On EMR, mrjob now defaults to the cheapest instance type that will work (see
:mrjob-opt:`instance_type`). In most cases, this is ``m1.medium``, but it
needs to be ``m1.large`` for Spark worker nodes.

Cluster pooling
^^^^^^^^^^^^^^^

mrjob can now add up to 1,000 steps on
:ref:`pooled clusters <cluster-pooling>` on EMR (except on very old AMIs).
mrjob now prints debug messages explaining why your job matched
a particular pooled cluster when running in verbose mode (the ``-v`` option).
Fixed a bug that caused pooling to fail when there was no need for a master
bootstrap script (e.g. when running with ``--no-bootstrap-mrjob``).

Other improvements
^^^^^^^^^^^^^^^^^^

Log interpretation is much more efficient at determining a job's probable
cause of failure (this works with Spark as well).

When running custom JARs (see :py:class:`~mrjob.step.JarStep`) mrjob now
repects :mrjob-opt:`libjars` and :mrjob-opt:`jobconf`.

The :mrjob-opt:`hadoop_streaming_jar` option now supports environment variables
and ``~``.

The :ref:`terminate-idle-clusters` tool now works with all step types,
including Spark. (It's still recommended that you rely on the
:mrjob-opt:`max_hours_idle` option rather than this tool.)

mrjob now works in Anaconda3 Jupyter Notebook.

Bugfixes
^^^^^^^^

Added several missing command-line switches, including
``--no-bootstrap-python`` on Dataproc. Made a major refactor that should
prevent these kinds of issues in the future.

Fixed a bug that caused mrjob to crash when the ssh binary (see
:mrjob-opt:`ssh_bin`) was missing or not executable.

Fixed a bug that erroneously reported failed or just-started jobs as 100%
complete.

Fixed a bug where timestamps were erroneously recognized as URIs.
mrjob now only recognizes strings containing
``://`` as URIs (see :py:func:`~mrjob.parse.is_uri`).

Deprecation
^^^^^^^^^^^

The following are deprecated and will be removed in v0.6.0:

* :py:class:`~mrjob.step.JarStep`.``INPUT``; use :py:data:`mrjob.step.INPUT`
  instead
* :py:class:`~mrjob.step.JarStep`.``OUTPUT``; use :py:data:`mrjob.step.OUTPUT`
  instead
* non-strict protocols (see `strict_protocols`)
* the *python_archives* option (try
  :ref:`this <cookbook-src-tree-pythonpath>` instead)
* :py:func:`~mrjob.parse.is_windows_path`
* :py:func:`~mrjob.parse.parse_key_value_list`
* :py:func:`~mrjob.parse.parse_port_range_list`
* :py:func:`~mrjob.util.scrape_options_into_new_groups`

.. _v0.5.6:

0.5.6
-----

Fixed a critical bug that caused Dataproc runner to always crash when
determining Hadoop version.

Log interpretation now prioritizes task errors (e.g. a traceback from
your Python script) as probable cause of failure, even if they aren't the most
recent error. Log interpretation will now continue to download and parse
task logs until it finds a non-empty stderr log.

Log interpretation also strips the "subprocess failed" Java stack trace
that appears in task stderr logs from Hadoop 1.

.. _v0.5.5:

0.5.5
-----

Functionally equivalent to :ref:`v0.5.4`, except that it restores
the deprecated *ami_version* option as an alias for :mrjob-opt:`image_version`,
making it easier to upgrade from earlier versions of mrjob.

Also slightly improves :ref:`cluster-pooling` on EMR with
updated information on memory and CPU power of various EC2 instance types, and
by treating application names (e.g. "Spark") as case-insensitive.

.. _v0.5.4:

0.5.4
-----

Pooling and idle cluster self-termination
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. warning::

   This release accidentally removed the *ami_version* option instead
   of merely deprecating it. If you are upgrading from an earlier version
   of mrjob, use version :ref:`v0.5.5` or later.

This release resolves a long-standing EMR API race condition that made it
difficult to use :ref:`cluster-pooling` and idle cluster
self-termination (see :mrjob-opt:`max_hours_idle`) together. Now if your
pooled job unknowingly runs on a cluster that was in the process of shutting
down, it will detect that and re-launch the job on a different cluster.

This means pretty much *everyone* running jobs on EMR should now enable
pooling, with a configuration like this:

.. code-block:: yaml

   runners:
     emr:
       max_hours_idle: 1
       pool_clusters: true

You may *also* run the :ref:`terminate-idle-clusters` script periodically, but
(barring any bugs) this shouldn't be necessary.

.. _generic-emr-option-names:

Generic EMR option names
^^^^^^^^^^^^^^^^^^^^^^^^

Many options to the :doc:`EMR runner <guides/emr-quickstart>` have been
made more generic, to make it easier to share code with the
:doc:`Dataproc runner <guides/dataproc-quickstart>`
(in most cases, the new names are also shorter and easier to remember):

=============================== ======================================
 old option name                 new option name
=============================== ======================================
*ami_version*                   :mrjob-opt:`image_version`
*aws_availablity_zone*          :mrjob-opt:`zone`
*aws_region*                    :mrjob-opt:`region`
*check_emr_status_every*        :mrjob-opt:`check_cluster_every`
*ec2_core_instance_bid_price*   :mrjob-opt:`core_instance_bid_price`
*ec2_core_instance_type*        :mrjob-opt:`core_instance_type`
*ec2_instance_type*             :mrjob-opt:`instance_type`
*ec2_master_instance_bid_price* :mrjob-opt:`master_instance_bid_price`
*ec2_master_instance_type*      :mrjob-opt:`master_instance_type`
*ec2_slave_instance_type*       :mrjob-opt:`core_instance_type`
*ec2_task_instance_bid_price*   :mrjob-opt:`task_instance_bid_price`
*ec2_task_instance_type*        :mrjob-opt:`task_instance_type`
*emr_tags*                      :mrjob-opt:`tags`
*num_ec2_core_instances*        :mrjob-opt:`num_core_instances`
*num_ec2_task_instances*        :mrjob-opt:`num_task_instances`
*s3_log_uri*                    :mrjob-opt:`cloud_log_dir`
*s3_sync_wait_time*             :mrjob-opt:`cloud_fs_sync_secs`
*s3_tmp_dir*                    :mrjob-opt:`cloud_tmp_dir`
*s3_upload_part_size*           *cloud_upload_part_size*
=============================== ======================================

The old option names and command-line switches are now deprecated but will
continue to work until v0.6.0. (Exception: *ami_version* was accidentally
removed; if you need it, use :ref:`v0.5.5` or later.)

`num_ec2_instances` has simply been deprecated (it's just
:mrjob-opt:`num_core_instances` plus one).

:mrjob-opt:`hadoop_streaming_jar_on_emr` has also been deprecated; in its
place, you can now pass a ``file://`` URI to :mrjob-opt:`hadoop_streaming_jar`
to reference a path on the master node.

Log interpretation
^^^^^^^^^^^^^^^^^^

Log interpretation (counters and probable cause of job failure) on Hadoop is
more robust, handing a wider variety of log4j formats and recovering more
gracefully from permissions errors. This includes fixing a crash that
could happen on Python 3 when attempting to read data from HDFS.

Log interpretation used to be partially broken on EMR AMI 4.3.0 and later
due to a permissions issue; this is now fixed.

pass_through_option()
^^^^^^^^^^^^^^^^^^^^^

You can now pass through *existing* command-line switches to your job;
for example, you can tell a job which runner launched it. See
:py:meth:`~mrjob.job.MRJob.pass_through_option` for details.

If you *don't* do this, ``self.options.runner`` will now always be ``None``
in your job (it used to confusingly default to ``'inline'``).

Stop logging credentials
^^^^^^^^^^^^^^^^^^^^^^^^

When mrjob is run in verbose mode (the ``-v`` option), the values of all
runner options are debug-logged to stderr. This has been the case since
the very early days of mrjob.

Unfortunately, this means that if you set your AWS credentials in
:file:`mrjob.conf`, they get logged as well, creating a surprising potential
security vulnerability. (This doesn't happen for AWS credentials set through
environment variables.)

Starting in this version, the values of :mrjob-opt:`aws_secret_access_key`
and `aws_security_token` are shown as ``'...'`` if they are set,
and all but the last four characters of :mrjob-opt:`aws_access_key_id` are
blanked out as well (e.g. ``'...YNDR'``).

Other improvements and bugfixes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ssh tunnel to the resource manager on EMR (see :mrjob-opt:`ssh_tunnel`)
now connects to its correct *internal* IP; this resolves a firewall issue that
existed on some VPC setups.

Uploaded files will no longer be given names starting with ``_`` or ``.``,
since Hadoop's input processing treats these files as "hidden".

The EMR idle cluster self-termination script (see :mrjob-opt:`max_hours_idle`)
now only runs on the master node.

The :ref:`audit-emr-usage` command-line tool should no longer constantly
trigger throttling warnings.

:mrjob-opt:`bootstrap_python` no longer bothers trying to install Python 3
on EMR AMI 4.6.0 and later, since it is already installed.

The ``--ssh-bind-ports`` command-line switch was broken (starting in
:ref:`v0.4.5`!), and is now fixed.

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

Added the `emr_applications` option, which helps you configure 4.x AMIs.

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

Fixes a bug in the previous release that caused `strict_protocols`
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

The default `ami_version` (see :mrjob-opt:`image_version`) is now ``3.11.0``. Our plan is to continue updating this to the lastest (non-broken) 3.x AMI for each 0.5.x release of mrjob.

The default :mrjob-opt:`instance_type` is now ``m1.medium`` (``m1.small`` is too small for the 3.x and 4.x AMIs)

You can specify 4.x AMIs with either the new :mrjob-opt:`release_label` option, or continue using `ami_version`; both work.

mrjob continues to support 2.x AMIs. However:

.. warning::

   2.x AMIs are deprecated by AWS, and based on a very old version of Debian (squeeze), which breaks :command:`apt-get` and exposes you to security holes.

Please, please switch if you haven't already.

AWS Regions
^^^^^^^^^^^

The new default `aws_region` (see :mrjob-opt:`region`) is ``us-west-2`` (Oregon). This both matches the default in the EMR console and, according to Amazon, is `carbon neutral <https://aws.amazon.com/about-aws/sustainability/>`__.

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
``--no-strict-protocols`` switch; see `strict_protocols` for
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
 - :ref:`cluster-pooling` now respects :mrjob-opt:`ec2_key_pair`
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

.. _v0.4.5:

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

Added an `aws_security_token` option to allow you to run
mrjob on EMR using temporary AWS credentials.

Added an `emr_tags` (see :mrjob-opt:`tags`) option to allow you to tag EMR job
flows at creation time.

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
``--setup-cmd`` and ``--setup-script``) have been replaced by a powerful
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
:ref:`job flow pooling <cluster-pooling>`, with :mrjob-opt:`max_hours_idle`
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
more :doc:`here <cmd>`.

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
:py:data:`~mrjob.options.CLEANUP_CHOICES` for more information.

0.3.3
-----

You can now :ref:`include one config file from another
<multiple-config-files>`.

0.3.2
-----

The EMR instance type/number options have changed to support spot instances:

* *core_instance_bid_price*
* *core_instance_type*
* *master_instance_bid_price*
* *master_instance_type*
* *slave_instance_type* (alias for *core_instance_type*)
* *task_instance_bid_price*
* *task_instance_type*

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

**Job Flow Pooling**

    EMR jobs can pull job flows out of a "pool" of similarly configured job
    flows. This can make it easier to use a small set of job flows across
    multiple automated jobs, save time and money while debugging, and generally
    make your life simpler.

    More info: :ref:`cluster-pooling`

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
        the former inability to use custom types and actions.

        The ``job_name_prefix`` option is gone (was deprecated).

        All URIs are passed through to Hadoop where possible. This should
        relax some requirements about what URIs you can use.

        Steps with no mapper use :command:`cat` instead of going through a
        no-op mapper.

        Compressed files can be streamed with the :py:meth:`.cat()` method.

    **EMR Mode**

        The default Hadoop version on EMR is now 0.20 (was 0.18).

        The ``instance_type`` option only sets the instance type for slave
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
