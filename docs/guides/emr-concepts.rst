Concepts
========

Choosing Type and Number of EC2 Instances
-----------------------------------------

When you create a job flow on EMR, you'll have the option of specifying a number
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
  host HDFS. This is mostly useful because your job flow can lose task instances
  without killing your job (see :ref:`spot-instances`).

There's a special case where your job flow *only* has a single master instance, in which case the master instance schedules tasks, runs them, and hosts HDFS.

By default, :py:mod:`mrjob` runs a single ``m1.small``, which is a cheap but not very powerful instance type. This can be quite adequate for testing your code on a small subset of your data, but otherwise give little advantage over running a job locally. To get more performance out of your job, you can either add more instances, use more powerful instances, or both.

Here are some things to consider when tuning your instance settings:

* Amazon bills you for the full hour even if your job flow only lasts for a few
  minutes (this is an artifact of the EC2 billing structure), so for many
  jobs that you run repeatedly, it is a good strategy to pick instance settings
  that make your job consistently run in a little less than an hour.
* Your job will take much longer and may fail if any task (usually a reducer)
  runs out of memory and starts using swap. (You can verify this by using
  :command:`vmstat` with :py:mod:`~mrjob.tools.emr.mrboss`.) Restructuring your
  job is often the best solution, but if you can't, consider using a high-memory
  instance type.
* Larger instance types are usually a better deal if you have the workload
  to justify them. For example, a ``c1.xlarge`` costs about 10 times as much
  as an ``m1.small``, but it has about 20 times as much processing power
  (and more memory).

The basic way to control type and number of instances is with the
*ec2_instance_type* and *num_ec2_instances* options, on the command line like
this::

    --ec2_instance_type c1.medium --num-ec2-instances 5

or in :py:mod:`mrjob.conf`, like this::

    runners:
      emr:
        ec2_instance_type: c1.medium
        num_ec2_instances: 5

In most cases, your master instance type doesn't need to be larger
than``m1.small`` to schedule tasks, so *ec2_instance_type* only applies to
instances that actually run tasks. (In this example, there are 1 ``m1.small``
master instance, and 4 ``c1.medium`` core instances.) You *will* need a larger
master instance if you have a very large number of input files; in this case,
use the *ec2_master_instance_type* option.

If you want to run task instances, you instead must specify the number of core
and task instances directly with the *num_ec2_core_instances* and
*num_ec2_task_instances* options. There are also *ec2_core_instance_type* and
*ec2_task_instance_type* options if you want to set these directly.
