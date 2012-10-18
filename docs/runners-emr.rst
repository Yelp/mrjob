mrjob.emr - run on EMR
======================

Job Runner
----------

.. py:module:: mrjob.emr

.. autoclass:: EMRJobRunner

EMR Utilities
-------------

.. automethod:: EMRJobRunner.make_emr_conn
.. autofunction:: describe_all_job_flows

S3 Utilities
------------

.. :py:module:: mrjob.fs.s3

.. autofunction:: mrjob.fs.s3.s3_key_to_uri

.. autoclass:: S3Filesystem

.. automethod:: S3Filesystem.make_s3_conn
.. automethod:: S3Filesystem.get_s3_key
.. automethod:: S3Filesystem.get_s3_keys
.. automethod:: S3Filesystem.get_s3_folder_keys
.. automethod:: S3Filesystem.make_s3_key
