Hadoop-related options
======================

Since mrjob is geared toward Hadoop, there are a few Hadoop-specific options.
However, due to the difference between the different runners, the Hadoop
platform, and Elastic MapReduce, they are not all available for all runners.

Options available to local, hadoop, and emr runners
---------------------------------------------------

These options are both used by Hadoop and simulated by the ``local`` runner to
some degree.

.. _opt_hadoop_version:

**hadoop_version** (:option:`--hadoop-version`)
    Set the version of Hadoop to use on EMR or simulate in the ``local``
    runner. If using EMR, consider setting *ami_version* instead; only AMI
    version 1.0 supports multiple versions of Hadoop anyway. If *ami_version*
    is not set, we'll default to Hadoop 0.20 for backwards compatibility with
    :py:mod:`mrjob` v0.3.0.

.. _opt_jobconf:

**jobconf** (:option:`--jobconf`)
    ``-jobconf`` args to pass to hadoop streaming. This should be a map from
    property name to value.  Equivalent to passing ``['-jobconf',
    'KEY1=VALUE1', '-jobconf', 'KEY2=VALUE2', ...]`` to *hadoop_extra_args*.

Options available to hadoop and emr runners
-------------------------------------------

.. _opt_hadoop_extra_args:

**hadoop_extra_args** (:option:`--hadoop-extra-arg`)
    Extra arguments to pass to hadoop streaming. This option is called
    **extra_args** when passed as a keyword argument to
    :py:class:`MRJobRunner`.

.. _opt_hadoop_streaming_jar:

**hadoop_streaming_jar** (:option:`--hadoop-streaming-jar`)
    Path to a custom hadoop streaming jar. This is optional for the ``hadoop``
    runner, which will search for it in :envvar:`HADOOP_HOME`. The emr runner
    can take a path either local to your machine or on S3.

.. _opt_label:

**label** (:option:`--label`)
    Description of this job to use as the part of its name.  By default, we
    use the script's module name, or ``no_script`` if there is none.

.. _opt_owner:

**owner** (:option:`--owner`)
    Who is running this job. Used solely to set the job name.  By default, we
    use :py:func:`getpass.getuser`, or ``no_user`` if it fails.

.. _opt_partitioner:

**partitioner** (:option:`--partitioner`)
    Optional name of a Hadoop partitoner class, e.g.
    ``'org.apache.hadoop.mapred.lib.HashPartitioner'``. Hadoop Streaming will
    use this to determine how mapper output should be sorted and distributed
    to reducers. You can also set this option on your job class with the
    :py:attr:`~mrjob.job.MRJob.PARTITIONER` attribute or the
    :py:meth:`~mrjob.job.MRJob.partitioner` method.

Options available to hadoop runner only
---------------------------------------

.. _opt_hadoop_bin:

**hadoop_bin** (:option:`--hadoop-bin`)
    Name/path of your hadoop program (may include arguments). Defaults to
    *hadoop_home* plus ``bin/hadoop``.

.. _opt_hadoop_home:

**hadoop_home** (:option:`--hadoop-home`)
    Alternative to setting the :envvar:`HADOOP_HOME` environment variable.

.. _opt_hdfs_scratch_dir:

**hdfs_scratch_dir** (:option:`--hdfs-scratch-dir`)
    Scratch space on HDFS (default is ``tmp/``). This path does not need to be
    fully qualified with ``hdfs://`` URIs because it's understood that it has
    to be on HDFS.

**check_hadoop_input_paths** (:option:`--skip-hadoop-input-check`)
    Option to skip the input path check. With this option all input paths
    to the runner will be passed straight through, without their existence
    being validated.
