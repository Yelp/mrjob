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
