Hadoop-related options
======================

Since mrjob is geared toward Hadoop, there are a few Hadoop-specific options.
However, due to the difference between the different runners, the Hadoop
platform, and Elastic MapReduce, they are not all available for all runners.

Options available to local, hadoop, and emr runners
---------------------------------------------------

These options are both used by Hadoop and simulated by the ``local`` runner to
some degree.

.. mrjob-opt::
    :config: hadoop_version
    :switch: --hadoop-version
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: inferred from environment/AWS

    Set the version of Hadoop to use on EMR or simulate in the ``local``
    runner. If using EMR, consider setting *ami_version* instead; only AMI
    version 1.0 supports multiple versions of Hadoop anyway. If *ami_version*
    is not set, we'll default to Hadoop 0.20 for backwards compatibility with
    :py:mod:`mrjob` v0.3.0.

.. mrjob-opt::
    :config: jobconf
    :switch: --jobconf
    :type: :ref:`dict <data-type-plain-dict>`
    :set: all
    :default: ``{}``

    ``-jobconf`` args to pass to hadoop streaming. This should be a map from
    property name to value.  Equivalent to passing ``['-jobconf',
    'KEY1=VALUE1', '-jobconf', 'KEY2=VALUE2', ...]`` to
    :mrjob-opt:`hadoop_extra_args`.

Options available to hadoop and emr runners
-------------------------------------------

.. mrjob-opt::
    :config: hadoop_extra_args
    :switch: --hadoop-extra-arg
    :type: :ref:`string list <data-type-string-list>`
    :set: all
    :default: ``[]``

    Extra arguments to pass to hadoop streaming. This option is called
    **extra_args** when passed as a keyword argument to
    :py:class:`MRJobRunner`.

.. mrjob-opt::
    :config: hadoop_streaming_jar
    :switch: --hadoop-streaming-jar
    :type: :ref:`string <data-type-string>`
    :set: all
    :default: automatic

    Path to a custom hadoop streaming jar. This is optional for the ``hadoop``
    runner, which will search for it in :envvar:`HADOOP_HOME`. The emr runner
    can take a path either local to your machine or on S3.

.. mrjob-opt::
    :config: label
    :switch: --label
    :type: :ref:`string <data-type-string>`
    :set: all
    :default: script's module name, or ``no_script``

    Description of this job to use as the part of its name.

.. mrjob-opt::
    :config: owner
    :switch: --owner
    :type: :ref:`string <data-type-string>`
    :set: all
    :default: :py:func:`getpass.getuser`, or ``no_user`` if that fails

    Who is running this job. Used solely to set the job name.

.. mrjob-opt::
    :config: partitioner
    :switch: --partitioner
    :type: :ref:`string <data-type-string>`
    :set: no_mrjob_conf
    :default: ``None``

    Optional name of a Hadoop partitoner class, e.g.
    ``'org.apache.hadoop.mapred.lib.HashPartitioner'``. Hadoop Streaming will
    use this to determine how mapper output should be sorted and distributed
    to reducers. You can also set this option on your job class with the
    :py:attr:`~mrjob.job.MRJob.PARTITIONER` attribute or the
    :py:meth:`~mrjob.job.MRJob.partitioner` method.

Options available to hadoop runner only
---------------------------------------

.. mrjob-opt::
    :config: check_input_paths
    :switch: --check-input-paths, --no-check-input-paths
    :type: boolean
    :set: hadoop
    :default: ``True``

    Option to skip the input path check. With ``--no-check-input-paths``,
    input paths to the runner will be passed straight through, without
    checking if they exist.

    .. versionadded:: 0.4.1

.. mrjob-opt::
    :config: hadoop_bin
    :switch: --hadoop-bin
    :type: :ref:`command <data-type-command>`
    :set: hadoop
    :default: :mrjob-opt:`hadoop_home` plus ``bin/hadoop``

    Name/path of your hadoop program (may include arguments).

.. mrjob-opt::
    :config: hadoop_home
    :switch: --hadoop-home
    :type: :ref:`path <data-type-path>`
    :set: hadoop
    :default: :envvar:`HADOOP_HOME`

    Alternative to setting the :envvar:`HADOOP_HOME` environment variable.

.. mrjob-opt::
    :config: hdfs_scratch_dir
    :switch: --hdfs-scratch-dir
    :type: :ref:`path <data-type-path>`
    :set: hadoop
    :default: :file:`tmp/`

    Scratch space on HDFS. This path does not need to be fully qualified with
    ``hdfs://`` URIs because it's understood that it has to be on HDFS.
