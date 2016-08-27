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
.. automethod:: EMRJobRunner.make_emr_conn

S3 Utilities
------------

.. :py:module:: mrjob.fs.s3

.. autofunction:: mrjob.fs.s3.s3_key_to_uri

.. autoclass:: S3Filesystem

.. automethod:: S3Filesystem.make_s3_conn
.. automethod:: S3Filesystem.get_s3_key
.. automethod:: S3Filesystem.get_s3_keys
.. automethod:: S3Filesystem.make_s3_key
