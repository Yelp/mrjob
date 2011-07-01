mrjob.emr - run on EMR
======================

.. py:module:: mrjob.emr

.. autoclass:: EMRJobRunner

Advanced EMR Usage
------------------

Reusing Job Flows
^^^^^^^^^^^^^^^^^

It can take several minutes to create a job flow. To decrease wait time when running multiple jobs, you may find it convenient to reuse a single job.

mrjob includes a utility to create persistent job flows without running a job. For example, this command will create a job flow with 12 EC2 instances (1 master and 11 slaves), taking all other options from :py:mod:`mrjob.conf`::

    > python mrjob/tools/emr/create_job_flow.py --num-ec2-instances=12
    ...
    Job flow created with ID: j-JOBFLOWID


You can then add jobs to the job flow with the :option:`--emr-job-flow-id` switch or the `emr_job_flow_id` variable in `mrjob.conf` (see :py:meth:`EMRJobRunner.__init__`)::

    > python mr_my_job.py -r emr --emr-job-flow-id=j-JOBFLOWID input_file.txt > out
    ...
    Adding our job to job flow j-SUNG7EGS3ECP
    ...

Debugging will be difficult unless you complete SSH setup (CROSS REFERENCE PLZ) since the logs will not be copied from the master node to S3 before either five minutes pass or the job flow terminates.

S3 utilities
------------

.. automethod:: EMRJobRunner.make_s3_conn
.. autofunction:: parse_s3_uri
.. autofunction:: s3_key_to_uri
.. automethod:: EMRJobRunner.get_s3_key
.. automethod:: EMRJobRunner.get_s3_keys
.. automethod:: EMRJobRunner.get_s3_folder_keys
.. automethod:: EMRJobRunner.make_s3_key

EMR utilities
-------------

.. automethod:: EMRJobRunner.make_emr_conn
.. autofunction:: describe_all_job_flows
