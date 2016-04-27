Advanced EMR usage
==================

.. _reusing-clusters:

Reusing Clusters
-----------------

It can take several minutes to create a cluster. To decrease wait time when
running multiple jobs, you may find it convenient to reuse a single job.

:py:mod:`mrjob` includes a utility to create persistent clusters without
running a job. For example, this command will create a cluster with 12 EC2
instances (1 master and 11 slaves), taking all other options from
:py:mod:`mrjob.conf`::

    $ mrjob create-cluster --num-ec2-instances=12 --max-hours-idle 1
    ...
    j-CLUSTERID


You can then add jobs to the cluster with the :option:`--emr-cluster-id`
switch or the `emr_cluster_id` variable in `mrjob.conf` (see
:py:meth:`EMRJobRunner.__init__`)::

    $ python mr_my_job.py -r emr --emr-cluster-id=j-CLUSTERID input_file.txt > out
    ...
    Adding our job to existing cluster j-CLUSTERID
    ...

Debugging will be difficult unless you complete SSH setup (see
:ref:`ssh-tunneling`) since the logs will not be copied from the master node to
S3 before either five minutes pass or the cluster terminates.

.. _pooling-clusters:

Pooling Clusters
-----------------

Manually creating clusters to reuse and specifying the cluster ID for every
run can be tedious. In addition, it is not convenient to coordinate cluster
use among multiple users.

To mitigate these problems, :py:mod:`mrjob` provides **cluster pools.** Rather
than having to remember to start a cluster and copying its ID, simply pass
:option:`--pool-clusters` on the command line. The first time you do this,
a new cluster will be created that does not terminate when the job completes.
When you use :option:`--pool-clusters` the next time, it will identify the
cluster and add the job to it rather than creating a new one.

.. warning::

    If you use cluster pools, keep
    :command:`mrjob terminate-idle-clusters` in your crontab!
    Otherwise you may forget to terminate your clusters and waste a lot of
    money.

Alternatively, you may use the :mrjob-opt:`max_hours_idle` option to create
self-terminating clusters; the disadvantage is that pooled jobs may
occasionally join clusters with out knowing they are about to self-terminate
(this is better for development than production).

Pooling is designed so that jobs run against the same :py:mod:`mrjob.conf` can
share the same clusters. This means that the version of :py:mod:`mrjob`,
boostrap configuration, Hadoop version and AMI version all need to be exactly
the same.

Pooled jobs will also only use clusters with the same **pool name**, so you
can use the :option:`--pool-name` option to partition your clusters into
separate pools.

Pooling is flexible about instance type and number of instances; it will
attempt to select the most powerful cluster available as long as the
cluster's instances provide at least as much memory and at least as much CPU as
your job requests. If there is a tie, it picks clusters that are closest to
the end of a full hour, to minimize wasted instance hours.

Amazon limits clusters to 256 steps total; pooling respects this and won't try
to use pooled clusters that are "full." :py:mod:`mrjob` also uses an S3-based
"locking" mechanism to prevent two jobs from simultaneously joining the same
cluster. This is somewhat ugly but works in practice, and avoids
:py:mod:`mrjob` depending on Amazon services other than EMR and S3.

.. warning::

    If S3 eventual consistency takes longer than *s3_sync_wait_time*, then you
    may encounter race conditions when using pooling, e.g. two jobs claiming
    the same cluster at the same time, or the idle cluster killer shutting
    down your job before it has started to run. Regions with read-after-write
    consistency (i.e. every region except US Standard) should not experience
    these issues.

You can allow jobs to wait for an available cluster instead of immediately
starting a new one by specifying a value for `--pool-wait-minutes`. mrjob will
try to find a cluster every 30 seconds for **pool_wait_minutes**. If none is
found during that time, mrjob will start a new one.

.. _spot-instances:

Spot Instances
--------------

Amazon also has a spot market for EC2 instances. You can potentially save money
by using the spot market. The catch is that if someone bids more for instances
that you're using, they can be taken away from your cluster. If this happens,
you aren't charged, but your job may fail.

You can specify spot market bid prices using the *ec2_core_instance_bid_price*,
*ec2_master_instance_bid_price*, and *ec2_task_instance_bid_price* options to
specify a price in US dollars. For example, on the command line::

    --ec2-task-instance-bid-price 0.42

or in :py:mod:`mrjob.conf`::

    runners:
      emr:
        ec2_task_instance_bid_price: '0.42'

(Note the quotes; bid prices are strings, not floats!)

Amazon has a pretty thorough explanation of why and when you'd want to use spot
instances `here
<http://docs.amazonwebservices.com/ElasticMapReduce/latest/DeveloperGuide/UsingEMR_SpotInstances.html?r=9215>`_.
The brief summary is that either you don't care if your job fails, in which
case you want to purchase all your instances on the spot market, or you'd need
your job to finish but you'd like to save time and money if you can, in which
case you want to run task instances on the spot market and purchase master and
core instances the regular way.

Cluster pooling interacts with bid prices more or less how you'd expect; a job
will join a pool with spot instances only if it requested spot instances at the
same price or lower.

Custom Python packages
----------------------

See :ref:`using-pip` and :ref:`installing-packages`.

.. _bootstrap-time-configuration:

Bootstrap-time configuration
----------------------------

Some Hadoop options, such as the maximum number of running map tasks per node,
must be set at bootstrap time and will not work with `--jobconf`. You must use
Amazon's `configure-hadoop` script for this. For example, this limits the
number of mappers and reducers to one per node::

    --bootstrap-action="s3://elasticmapreduce/bootstrap-actions/configure-hadoop \
    -m mapred.tasktracker.map.tasks.maximum=1 \
    -m mapred.tasktracker.reduce.tasks.maximum=1"

.. note::

   This doesn't work on AMI version 4.0.0 and later.

Setting up Ganglia
------------------

`Ganglia <http://www.ganglia.info>`_ is a scalable distributed monitoring
system for high-performance computing systems. You can enable it for your
EMR cluster with Amazon's `install-ganglia`_ bootstrap action::

    --bootstrap-action="s3://elasticmapreduce/bootstrap-actions/install-ganglia

.. _install-ganglia: http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/UsingEMR_Ganglia.html

.. note::

   This doesn't work on AMI version 4.0.0 and later.
