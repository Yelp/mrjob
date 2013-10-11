Advanced EMR usage
==================

.. _reusing-job-flows:

Reusing Job Flows
-----------------

It can take several minutes to create a job flow. To decrease wait time when
running multiple jobs, you may find it convenient to reuse a single job.

:py:mod:`mrjob` includes a utility to create persistent job flows without
running a job. For example, this command will create a job flow with 12 EC2
instances (1 master and 11 slaves), taking all other options from
:py:mod:`mrjob.conf`::

    > python mrjob/tools/emr/create_job_flow.py --num-ec2-instances=12
    ...
    Job flow created with ID: j-JOBFLOWID


You can then add jobs to the job flow with the :option:`--emr-job-flow-id`
switch or the `emr_job_flow_id` variable in `mrjob.conf` (see
:py:meth:`EMRJobRunner.__init__`)::

    > python mr_my_job.py -r emr --emr-job-flow-id=j-JOBFLOWID input_file.txt > out
    ...
    Adding our job to job flow j-JOBFLOWID
    ...

Debugging will be difficult unless you complete SSH setup (see
:ref:`ssh-tunneling`) since the logs will not be copied from the master node to
S3 before either five minutes pass or the job flow terminates.

.. _pooling-job-flows:

Pooling Job Flows
-----------------

Manually creating job flows to reuse and specifying the job flow ID for every
run can be tedious. In addition, it is not convenient to coordinate job flow
use among multiple users.

To mitigate these problems, :py:mod:`mrjob` provides **job flow pools.** Rather
than having to remember to start a job flow and copying its ID, simply pass
:option:`--pool-emr-job-flows` on the command line. The first time you do this,
a new job flow will be created that does not terminate when the job completes.
When you use :option:`--pool-emr-job-flows` the next time, it will identify the
job flow and add the job to it rather than creating a new one.

.. warning::

    If you use job flow pools, keep
    :py:mod:`~mrjob.tools.emr.terminate_idle_job_flows` in your crontab!
    Otherwise you may forget to terminate your job flows and waste a lot of
    money.

Alternatively, you may use the :mrjob-opt:`max_hours_idle` option to create
self-terminating job flows; the disadvantage is that pooled jobs may
occasionally join job flows with out knowing they are about to self-terminate
(this is better for development than production).

Pooling is designed so that jobs run against the same :py:mod:`mrjob.conf` can
share the same job flows. This means that the version of :py:mod:`mrjob`,
boostrap configuration, Hadoop version and AMI version all need to be exactly
the same.

Pooled jobs will also only use job flows with the same **pool name**, so you
can use the :option:`--pool-name` option to partition your job flows into
separate pools.

Pooling is flexible about instance type and number of instances; it will
attempt to select the most powerful job flow available as long as the job
flow's instances provide at least as much memory and at least as much CPU as
your job requests. If there is a tie, it picks job flows that are closest to
the end of a full hour, to minimize wasted instance hours.

Amazon limits job flows to 256 steps total; pooling respects this and won't try
to use pooled job flows that are "full." :py:mod:`mrjob` also uses an S3-based
"locking" mechanism to prevent two jobs from simultaneously joining the same
job flow. This is somewhat ugly but works in practice, and avoids
:py:mod:`mrjob` depending on Amazon services other than EMR and S3.

.. warning::

    If S3 eventual consistency takes longer than *s3_sync_wait_time*, then you
    may encounter race conditions when using pooling, e.g. two jobs claiming
    the same job flow at the same time, or the idle job flow killer shutting
    down your job before it has started to run. Regions with read-after-write
    consistency (i.e. every region except US Standard) should not experience
    these issues.

You can allow jobs to wait for an available job flow instead of immediately
starting a new one by specifying a value for `--pool-wait-minutes`. mrjob will
try to find a job flow every 30 seconds for **pool_wait_minutes**. If none is
found during that time, mrjob will start a new one.

.. _spot-instances:

Spot Instances
--------------

Amazon also has a spot market for EC2 instances. You can potentially save money
by using the spot market. The catch is that if someone bids more for instances
that you're using, they can be taken away from your job flow. If this happens,
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

Job flow pooling interacts with bid prices more or less how you'd expect; a job
will join a pool with spot instances only if it requested spot instances at the
same price or lower.

Custom Python packages
----------------------

There are a couple of ways to install Python packages that are not in the
standard library. If there is a Debian package, you can add a call to
``apt-get`` as a ``bootstrap_cmd``::

    runners:
      emr:
        bootstrap_cmds:
        - sudo apt-get install -y python-simplejson

If there is no Debian package or you prefer to use your own tarballs for some
other reason, you can specify tarballs in ``bootstrap_python_packages``, which
supports glob syntax::

    runners:
      emr:
        bootstrap_python_packages:
        - $MY_SOURCE_TREE/emr_packages/*.tar.gz

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

Setting up Ganglia
------------------

`Ganglia <http://www.ganglia.info>`_ is a scalable distributed monitoring
system for high-performance computing systems. You can enable it for your
EMR cluster with Amazon's `install-ganglia`_ bootstrap action::

    --bootstrap-action="s3://elasticmapreduce/bootstrap-actions/install-ganglia

.. _install-ganglia: http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/UsingEMR_Ganglia.html

Enabling Python core dumps
--------------------------

Particularly bad errors may leave no traceback in the logs. To enable core
dumps on your EMR instances, put this script in ``core_dump_bootstrap.sh``::

    #!/bin/sh

    chk_root () {
        if [ ! $( id -u ) -eq 0 ]; then
            exec sudo sh ${0}
            exit ${?}
        fi
    }

    chk_root

    mkdir /tmp/cores
    chmod -R 1777 /tmp/cores
    echo "\n* soft core unlimited" >> /etc/security/limits.conf
    echo "ulimit -c unlimited" >> /etc/profile
    echo "/tmp/cores/core.%e.%p.%h.%t" > /proc/sys/kernel/core_pattern

Use the script as a bootstrap action in your job::

    --bootstrap-action=core_dump_setup.sh

You'll probably want to use a version of Python with debugging symbols, so
install it and use it as ``python_bin``::

    --bootstrap-cmd="sudo apt-get install -y python2.6-dbg" \
    --python-bin=python2.6-dbg

Run your job in a persistent job flow. When it fails, you can SSH to your nodes
to inspect the core dump files::

    you@local: emr --ssh j-MYJOBFLOWID

    hadoop@ip-10-160-75-214:~$ gdb `which python` /tmp/cores/core.python.blah

If you have multiple nodes, you may have to :command:`scp` your identity file
to the master node and use it to SSH to the slave nodes, where the core dumps
are located::

    hadoop@ip-10-160-75-214:~$ hadoop dfsadmin -report | grep ^Name
    Name: 10.166.50.85:9200
    Name: 10.177.63.114:9200

    hadoop@ip-10-160-75-214:~$ ssh -i uploaded_key.pem 10.166.50.85

    hadoop@ip-10-166-50-85:~$ gdb `which python2.6-dbg` /tmp/cores/core.python.blah
