Spark runner options
====================

All options from :doc:`configs-all-runners` and :doc:`configs-hadoopy-runners`
are available in the Spark runner.

In addition, the Spark runner has the following options in common with other
runners:

  * :mrjob-opt:`aws_access_key_id`
  * :mrjob-opt:`aws_secret_access_key`
  * :mrjob-opt:`aws_session_token`
  * :mrjob-opt:`cloud_fs_sync_secs`
  * :mrjob-opt:`cloud_part_size_mb`
  * :mrjob-opt:`gcs_region`
  * :mrjob-opt:`project_id`
  * :mrjob-opt:`s3_endpoint`
  * :mrjob-opt:`s3_region`

Options unique to the Spark runner:

.. mrjob-opt::
   :config: emulate_map_input_file
   :switch: --emulate-map-input-file, --no-emulate-map-input-file
   :type: boolean
   :set: spark
   :default: ``False``

   Imitate Hadoop by setting :envvar:`$mapreduce_map_input_file`
   to the path of the input file for the current partition. This
   helps support jobs that rely on
   :py:func:`jobconf_from_env('mapreduce.map.input.file') <mrjob.compat.jobconf_from_env>`.

   This feature only applies to the mapper of the job's first step,
   and is ignored by jobs that set
   :py:attr:`~mrjob.job.MRJob.HADOOP_INPUT_FORMAT`.

   .. versionadded:: 0.6.9

.. mrjob-opt::
    :config: gcs_region
    :switch: --gcs-region
    :type: :ref:`string <data-type-string>`
    :set: spark
    :default: ``None``

    The region to use when creating a temporary bucket on Google Cloud Storage.

    Similar in meaning to :mrjob-opt:`region`, but only used to configure GCS
    (not S3)

.. mrjob-opt::
    :config: s3_region
    :switch: --s3-region
    :type: :ref:`string <data-type-string>`
    :set: spark
    :default: ``None``

    The region to use when creating a temporary bucket on S3.

    Similar in meaning to :mrjob-opt:`region`, but only used to configure S3
    (not GCS)

.. mrjob-opt::
    :config: skip_internal_protocol
    :switch: --skip-internal-protocol, --no-skip-internal-protocol
    :type: boolean
    :set: spark
    :default: ``False``

    Don't emulate the job's internal protocol (used for communicating between
    job steps and tasks in the same step), instead relying on Spark to encode
    and decode data structures.

    This should work for most but not all jobs, and make them run at least
    somewhat faster. Some things to keep in mind:

    * data will no longer be "normalized" by being converted to and from string
      representation. For example, running a tuple through
      :py:class:`~mrjob.protocol.JSONProtocol` (the default) implicitly
      converts it to a list because there are no tuples in JSON. With internal
      protocols skipped, it would remain a tuple.
    * if your job uses :py:attr:`~mrjob.job.MRJob.SORT_VALUES`, keep in mind
      that your values will need to be comparable as Spark will be comparing
      them directly, rather than comparing their internal-protocol-encoded
      representation. This may also affect sorting order.

.. mrjob-opt::
    :config: spark_tmp_dir
    :switch: --spark-tmp-dir
    :type: :ref:`string <data-type-string>`
    :set: spark
    :default: (automatic)

    A place to put files where they are visible to Spark executors, similar
    to :mrjob-opt:`cloud_tmp_dir`.

    If running locally, defaults to a directory inside
    :mrjob-opt:`local_tmp_dir`, and if running on a cluster, to
    ``tmp/mrjob`` on HDFS.
