mrjob.emr - run on EMR
======================

.. py:module:: mrjob.emr

.. autoclass:: EMRJobRunner

.. _amazon-setup:

Setting up EMR on Amazon
------------------------

* create an `Amazon Web Services account <http://aws.amazon.com/>`_
* sign up for `Elastic MapReduce <http://aws.amazon.com/elasticmapreduce/>`_
* Get your access and secret keys (click "Security Credentials" on `your account page <http://aws.amazon.com/account/>`_)
* Set the environment variables :envvar:`AWS_ACCESS_KEY_ID` and :envvar:`AWS_SECRET_ACCESS_KEY` accordingly

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
    Adding our job to job flow j-JOBFLOWID
    ...

Debugging will be difficult unless you complete SSH setup (see :ref:`ssh-tunneling`) since the logs will not be copied from the master node to S3 before either five minutes pass or the job flow terminates.


Pooling Job Flows
^^^^^^^^^^^^^^^^^

Manually creating job flows to reuse and specifying the job flow ID for every run can be tedious. In addition, it is not convenient to coordinate job flow use among multiple users.

To mitigate these problems, mrjob provides **job flow pools.** Rather than having to remember to start a job flow and copying its ID, simply pass :option:`--pool-emr-job-flows` on the command line. The first time you do this, a new job flow will be created that does not terminate when the job completes. When you use :option:`--pool-emr-job-flows` the next time, it will identify the job flow and add the job to it rather than creating a new one.

The criteria for finding an appropriate job flow for a job are as follows:

* The job flow must be in the ``WAITING`` state.
* The bootstrap configuration (packages, commands, etc.) must be identical. This is checked using an md5 sum.
* The **pool name** must be the same. You can specify a pool name with :option:`--pool-name`.
* The job flow must have at least as many instances, and  the instance type must have at least as many compute units, as the job configuration specifies. See `Amazon EC2 Instance Types <http://aws.amazon.com/ec2/instance-types/>`_ for a complete listing of instance types and their respective compute units.
* Ties are broken first by total compute units in the job flow as calculated by ``number of instances * instance type compute units``, then by the number of minutes until an even instance hour. This strategy minimizes wasted instance hours.

Most of the time you shouldn't need to worry about these things. Just use pool names to separate job flows into pools representing their type.

EMR provides no way to remove steps from a job flow once they are added. This introduces a race condition where two users identify the same job flow and join it simultaneously, causing one user's job to be delayed until the first user's is finished. mrjob avoids this situation using an S3-based locking mechanism.

**If you use job flow pools, keep** :py:mod:`~mrjob.tools.emr.terminate_idle_job_flows` **in your crontab!** Otherwise you will forget to terminate your job flows and waste a lot of money.

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

.. _ssh-tunneling:

SSH Tunneling
-------------

To enable SSH tunneling, so that you can view the Hadoop Job Tracker in your browser:

* Go to https://console.aws.amazon.com/ec2/home
* Make sure the **Region** dropdown (upper left) matches the region you want to run jobs in (usually "US East").
* Click on **Key Pairs** (lower left)
* Click on **Create Key Pair** (center).
* Name your key pair ``EMR`` (any name will work but that's what we're using in this example)
* Save :file:`EMR.pem` wherever you like (``~/.ssh`` is a good place)
* Run ``chmod og-rwx /path/to/EMR.pem`` so that ``ssh`` will be happy
* Add the following entries to your :py:mod:`mrjob.conf`::

    runners:
      emr:
        ec2_key_pair: EMR
        ec2_key_pair_file: /path/to/EMR.pem # ~/ and $ENV_VARS allowed here!
        ssh_tunnel_to_job_tracker: true
