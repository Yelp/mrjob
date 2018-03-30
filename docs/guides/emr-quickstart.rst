Elastic MapReduce Quickstart
============================

.. _amazon-setup:

Configuring AWS credentials
---------------------------

Configuring your AWS credentials allows mrjob to run your jobs on Elastic
MapReduce and use S3.

* Create an `Amazon Web Services account <http://aws.amazon.com/>`_
* Go to `Security Credentials
  <https://console.aws.amazon.com/iam/home?#security_credential>`__ in the
  login menu (upper right, third from the right), say yes, you want to
  proceed, click
  on **Access Keys**, and then **Create New Access Key**. Make sure to copy the
  secret access key, as there is no way to recover it after creation.

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
* Make sure the **Region** dropdown (upper right, second from the right)
  matches the region you want to run jobs in (usually "Oregon").
* Click on **Key Pairs** (left sidebar, under **Network & Security**)
* Click on **Create Key Pair** (top left).
* Name your key pair ``EMR`` (any name will work but that's what we're using
  in this example)
* Save :file:`EMR.pem` wherever you like (``~/.ssh`` is a good place)
* Run ``chmod og-rwx /path/to/EMR.pem`` so that ``ssh`` will be happy
* Add the following entries to your :py:mod:`mrjob.conf`::

    runners:
      emr:
        ec2_key_pair: EMR
        ec2_key_pair_file: /path/to/EMR.pem # ~/ and $ENV_VARS allowed here
        ssh_tunnel: true

.. _running-an-emr-job:

Running an EMR Job
------------------

Running a job on EMR is just like running it locally or on your own Hadoop
cluster, with the following changes:

* The job and related files are uploaded to S3 before being run
* The job is run on EMR (of course)
* Output is written to S3 before mrjob streams it to stdout locally
* The Hadoop version is specified by the EMR AMI version

This the output of this command should be identical to the output shown in
:doc:`quickstart`, but it should take much longer:

    > python word_count.py -r emr README.txt
    "chars" 3654
    "lines" 123
    "words" 417

Sending Output to a Specific Place
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you'd rather have your output go to somewhere deterministic on S3, use
:option:`--output-dir`::

    > python word_count.py -r emr README.rst \
    >   --output-dir=s3://my-bucket/wc_out/

There are many other ins and outs of effectively using mrjob with EMR. See
:doc:`emr-advanced` for some of the ins, but the outs are left as an exercise
for the reader. This is a strictly no-outs body of documentation!

.. _picking-emr-cluster-config:

Choosing Type and Number of EC2 Instances
-----------------------------------------

When you create a cluster on EMR, you'll have the option of specifying a number
and type of EC2 instances, which are basically virtual machines. Each instance
type has different memory, CPU, I/O and network characteristics, and costs
a different amount of money. See
`Instance Types <http://aws.amazon.com/ec2/instance-types/>`_ and
`Pricing <http://aws.amazon.com/elasticmapreduce/pricing/>`_ for details.

Instances perform one of three roles:

* **Master**: There is always one master instance. It handles scheduling of tasks
  (i.e. mappers and reducers), but does not run them itself.
* **Core**: You may have one or more core instances. These run tasks and host
  HDFS.
* **Task**: You may have zero or more of these. These run tasks, but do *not*
  host HDFS. This is mostly useful because your cluster can lose task instances
  without killing your job (see :ref:`spot-instances`).

There's a special case where your cluster *only* has a single master instance, in which case the master instance schedules tasks, runs them, and hosts HDFS.

By default, :py:mod:`mrjob` runs a single ``m1.medium``, which is a cheap but not very powerful instance type. This can be quite adequate for testing your code on a small subset of your data, but otherwise give little advantage over running a job locally. To get more performance out of your job, you can either add more instances, use more powerful instances, or both.

Here are some things to consider when tuning your instance settings:

* Amazon bills you for the full hour even if your cluster only lasts for a few
  minutes (this is an artifact of the EC2 billing structure), so for many
  jobs that you run repeatedly, it is a good strategy to pick instance settings
  that make your job consistently run in a little less than an hour.
* Your job will take much longer and may fail if any task (usually a reducer)
  runs out of memory and starts using swap. (You can verify this by running
  :command:`mrjob boss j-CLUSTERID vmstat` and then looking in
  ``j-CLUSTERID/*/stdout``.) Restructuring your job is often the best
  solution, but if you can't, consider using a high-memory instance type.
* Larger instance types are usually a better deal if you have the workload
  to justify them. For example, a ``c1.xlarge`` costs about 6 times as much
  as an ``m1.medium``, but it has about 8 times as much processing power
  (and more memory).

The basic way to control type and number of instances is with the
*instance_type* and *num_core_instances* options, on the command line like
this::

    --instance-type c1.medium --num-core-instances 4

or in :py:mod:`mrjob.conf`, like this::

    runners:
      emr:
        instance_type: c1.medium
        num_core_instances: 4

In most cases, your master instance type doesn't need to be larger
than ``m1.medium`` to schedule tasks, so *instance_type* only applies to
the 4 instances that actually run tasks. You *will* need a larger
master instance if you have a very large number of input files; in this case,
use the *master_instance_type* option.

The *num_task_instances* option can be used to run 1 or more task instances
(these run tasks but don't host HDFS). There are also *core_instance_type* and
*task_instance_type* options if you want to set these directly.
