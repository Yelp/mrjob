Amazon Web Services Quick Setup
===============================

Amazon Web Services provides a bewildering array of options and services. Here's a quick guide to getting in and getting out.

Minimal Setup
-------------

* create an Amazon Web Services account: http://aws.amazon.com/
* sign up for Elastic MapReduce: http://aws.amazon.com/elasticmapreduce/
* Get your access and secret keys (go to http://aws.amazon.com/account/ and click on "Security Credentials")
* Set the environment variables :envvar:`AWS_ACCESS_KEY_ID` and :envvar:`AWS_SECRET_ACCESS_KEY` accordingly

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

