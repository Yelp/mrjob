Elastic MapReduce Configuration
===============================

.. _amazon-setup:

Configuring AWS credentials
---------------------------

Configuring your AWS credentials allows mrjob to run your jobs on Elastic
MapReduce and use S3.

* Create an `Amazon Web Services account <http://aws.amazon.com/>`_
* Sign up for `Elastic MapReduce <http://aws.amazon.com/elasticmapreduce/>`_
* Get your access and secret keys (click "Security Credentials" on `your
  account page <http://aws.amazon.com/account/>`_)

Now you can either set the environment variables :envvar:`AWS_ACCESS_KEY_ID`
and :envvar:`AWS_SECRET_ACCESS_KEY`, or set **aws_access_key_id** and
**aws_secret_access_key** in your ``mrjob.conf`` file like this::

    runners:
      emr:
        aws_access_key_id: <your key ID>
        aws_secret_access_key: <your secret>

.. _ssh-tunneling:

Configuring SSH credentials
---------------------------

Configuring your SSH credentials lets mrjob open an SSH tunnel to your jobs'
master nodes to view live progress, see the job tracker in your browser, and
fetch error logs quickly.

* Go to https://console.aws.amazon.com/ec2/home
* Make sure the **Region** dropdown (upper left) matches the region you want 
  to run jobs in (usually "US East").
* Click on **Key Pairs** (lower left)
* Click on **Create Key Pair** (center).
* Name your key pair ``EMR`` (any name will work but that's what we're using 
  in this example)
* Save :file:`EMR.pem` wherever you like (``~/.ssh`` is a good place)
* Run ``chmod og-rwx /path/to/EMR.pem`` so that ``ssh`` will be happy
* Add the following entries to your :py:mod:`mrjob.conf`::

    runners:
      emr:
        ec2_key_pair: EMR
        ec2_key_pair_file: /path/to/EMR.pem # ~/ and $ENV_VARS allowed here
        ssh_tunnel_to_job_tracker: true
