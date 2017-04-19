mrjob.emr - run on EMR
======================

Job Runner
----------

.. py:module:: mrjob.emr

.. autoclass:: EMRJobRunner

EMR Utilities
-------------

.. automethod:: EMRJobRunner.get_cluster_id
.. automethod:: EMRJobRunner.get_image_version
.. automethod:: EMRJobRunner.make_emr_client

S3 Utilities
------------

.. :py:module:: mrjob.fs.s3

.. autoclass:: S3Filesystem

.. automethod:: S3Filesystem.create_bucket
.. automethod:: S3Filesystem.get_all_bucket_names
.. automethod:: S3Filesystem.get_bucket
.. automethod:: S3Filesystem.make_s3_client
.. automethod:: S3Filesystem.make_s3_resource
