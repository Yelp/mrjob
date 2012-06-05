mrjob.emr - run on EMR
======================

Job Runner
----------

.. py:module:: mrjob.emr

.. autoclass:: EMRJobRunner

S3 Utilities
------------

.. automethod:: EMRJobRunner.make_s3_conn
.. autofunction:: parse_s3_uri
.. autofunction:: s3_key_to_uri
.. automethod:: EMRJobRunner.get_s3_key
.. automethod:: EMRJobRunner.get_s3_keys
.. automethod:: EMRJobRunner.get_s3_folder_keys
.. automethod:: EMRJobRunner.make_s3_key

EMR Utilities
-------------

.. automethod:: EMRJobRunner.make_emr_conn
.. autofunction:: describe_all_job_flows
