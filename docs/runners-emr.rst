mrjob.emr - run on EMR
======================

Setting up EMR on Amazon
------------------------

* create an `Amazon Web Services account <http://aws.amazon.com/>`_
* sign up for `Elastic MapReduce <http://aws.amazon.com/elasticmapreduce/>`_
* Get your access and secret keys (click "Security Credentials" on `your account page <http://aws.amazon.com/account/>`_)
* Set the environment variables :envvar:`AWS_ACCESS_KEY_ID` and :envvar:`AWS_SECRET_ACCESS_KEY` accordingly

Once you've done this, add ``-r emr`` to your job's command line to run it on EMR.

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

.. _amazon-setup:

.. _ssh-tunneling:

SSH Tunneling and Log Fetching
------------------------------

To enable SSH tunneling and log fetching, so that you can view the Hadoop Job
Tracker in your browser and see error logs faster:

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

In most cases, your master instance type doesn't need to be larger than``m1.small`` to schedule tasks, so *ec2_instance_type* only applies to instances that actually run tasks. (In this example, there are 1 ``m1.small`` master instance, and 4 ``c1.medium`` core instances.) If you do need a bigger master instance, there is an *ec2_master_instance_type* option.

If you want to run task instances, you instead must specify the number of core and task instances directly with the *num_ec2_core_instances* and *num_ec2_task_instances* options. There are also *ec2_core_instance_type* and *ec2_task_instance_type* options if you want to set these directly.

Advanced EMR Strategies
-----------------------

.. _reusing-job-flows:

Reusing Job Flows
^^^^^^^^^^^^^^^^^

It can take several minutes to create a job flow. To decrease wait time when running multiple jobs, you may find it convenient to reuse a single job.

:py:mod:`mrjob` includes a utility to create persistent job flows without running a job. For example, this command will create a job flow with 12 EC2 instances (1 master and 11 slaves), taking all other options from :py:mod:`mrjob.conf`::

    > python mrjob/tools/emr/create_job_flow.py --num-ec2-instances=12
    ...
    Job flow created with ID: j-JOBFLOWID


You can then add jobs to the job flow with the :option:`--emr-job-flow-id` switch or the `emr_job_flow_id` variable in `mrjob.conf` (see :py:meth:`EMRJobRunner.__init__`)::

    > python mr_my_job.py -r emr --emr-job-flow-id=j-JOBFLOWID input_file.txt > out
    ...
    Adding our job to job flow j-JOBFLOWID
    ...

Debugging will be difficult unless you complete SSH setup (see :ref:`ssh-tunneling`) since the logs will not be copied from the master node to S3 before either five minutes pass or the job flow terminates.

.. _pooling-job-flows:

Pooling Job Flows
^^^^^^^^^^^^^^^^^

Manually creating job flows to reuse and specifying the job flow ID for every run can be tedious. In addition, it is not convenient to coordinate job flow use among multiple users.

To mitigate these problems, :py:mod:`mrjob` provides **job flow pools.** Rather than having to remember to start a job flow and copying its ID, simply pass :option:`--pool-emr-job-flows` on the command line. The first time you do this, a new job flow will be created that does not terminate when the job completes. When you use :option:`--pool-emr-job-flows` the next time, it will identify the job flow and add the job to it rather than creating a new one.

**If you use job flow pools, keep** :py:mod:`~mrjob.tools.emr.terminate_idle_job_flows` **in your crontab!** Otherwise you will forget to terminate your job flows and waste a lot of money.

Pooling is designed so that jobs run against the same :py:mod:`mrjob.conf` can share the same job flows. This means that the version of :py:mod:`mrjob`, boostrap configuration, Hadoop version and AMI version all need to be exactly the same.

Pooled jobs will also only use job flows with the same **pool name**, so you can use the :option:`--pool-name` option to partition your job flows into separate pools.

Pooling is flexible about instance type and number of instances; it will attempt to select the most powerful job flow available as long as the job flow's instances provide at least as much memory and at least as much CPU as your job requests. If there is a tie, it picks job flows that are closest to the end of a full hour, to minimize wasted instance hours.

Amazon limits job flows to 256 steps total; pooling respects this and won't try to use pooled job flows that are "full." :py:mod:`mrjob` also uses an S3-based "locking" mechanism to prevent two jobs from simultaneously joining the same job flow. This is somewhat ugly but works in practice, and avoids :py:mod:`mrjob` depending on Amazon services other than EMR and S3.

.. _spot-instances:

Spot Instances
^^^^^^^^^^^^^^

Amazon also has a spot market for EC2 instances. You can potentially save money by using the spot market. The catch is that if someone bids more for instances that you're using, they can be taken away from your job flow. If this happens, you aren't charged, but your job may fail.

You can specify spot market bid prices using the *ec2_core_instance_bid_price*,
*ec2_master_instance_bid_price*, and *ec2_task_instance_bid_price* options to specify a price in US dollars. For example, on the command line::

    --ec2-task-instance-bid-price 0.42

or in :py:mod:`mrjob.conf`::

    runners:
      emr:
        ec2_task_instance_bid_price: '0.42'

(Note the quotes; bid prices are strings, not floats!)

Amazon has a pretty thorough explanation of why and when you'd want to use spot
instances
`here <http://docs.amazonwebservices.com/ElasticMapReduce/latest/DeveloperGuide/UsingEMR_SpotInstances.html?r=9215>`_. The brief summary is that either
you don't care if your job fails, in which case you want to purchase all
your instances on the spot market, or you'd need your job to finish but you'd
like to save time and money if you can, in which case you want to run
task instances on the spot market and purchase master and core instances
the regular way.

Job flow pooling interacts with bid prices more or less how you'd expect;
a job will join a pool with spot instances only if it requested spot instances
at the same price or lower.

Troubleshooting
---------------

Many things can go wrong in an EMR job, and the system's distributed nature
can make it difficult to find the source of a problem. :py:mod:`mrjob` attempts to
simplify the debugging process by automatically scanning logs for probable
causes of failure. Specifically, it looks at logs relevant to your job for
these errors:

* Python tracebacks and Java stack traces in ``$S3_LOG_URI/task-attempts/*``
* Hadoop Streaming errors in ``$S3_LOG_URI/steps/*``
* Timeout errors in ``$S3_LOG_URI/jobs/*``

As mentioned above, in addition to looking at S3, :py:mod:`mrjob` can be configured to
also use SSH to fetch error logs directly from the master and slave nodes.
This can speed up debugging significantly, because logs are only available on
S3 five minutes after the job completes, or immediately after the job flow
terminates.

Using Persistent Job Flows
^^^^^^^^^^^^^^^^^^^^^^^^^^

When troubleshooting a job, it can be convenient to use a persistent job flow
to avoid having to wait for bootstrapping every run. **If you decide to use
persistent job flows, add** :py:mod:`mrjob.tools.emr.terminate_idle_job_flows`
**to your crontab or you will be billed for unused CPU time if you forget to
explicitly terminate job flows.**

First, use the :py:mod:`mrjob.tools.emr.create_job_flow` tool to create a
persistent job flow::

    > python -m mrjob.tools.emr.create_job_flow
    using configs in /etc/mrjob.conf
    Creating persistent job flow to run several jobs in...
    creating tmp directory /scratch/username/no_script.username.20110811.185141.422311
    writing master bootstrap script to /scratch/username/no_script.username.20110811.185141.422311/b.py
    Copying non-input files into s3://scratch-bucket/tmp/no_script.username.20110811.185141.422311/files/
    Waiting 5.0s for S3 eventual consistency
    Creating Elastic MapReduce job flow
    Job flow created with ID: j-1NXMMBNEQHAFT
    j-1NXMMBNEQHAFT

Now you can use the job flow ID to start the troublesome job::

    > python mrjob/buggy_job.py -r emr --emr-job-flow-id=j-1NXMMBNEQHAFT input/* > out
    using configs in /etc/mrjob.conf
    Uploading input to s3://scratch-bucket/tmp/buggy_job.username.20110811.185410.536519/input/
    creating tmp directory /scratch/username/buggy_job.username.20110811.185410.536519
    writing wrapper script to /scratch/username/buggy_job.username.20110811.185410.536519/wrapper.py
    Copying non-input files into s3://scratch-bucket/tmp/buggy_job.username.20110811.185410.536519/files/
    Adding our job to job flow j-1NXMMBNEQHAFT
    Job launched 30.1s ago, status BOOTSTRAPPING: Running bootstrap actions
    Job launched 60.1s ago, status BOOTSTRAPPING: Running bootstrap actions
    Job launched 90.2s ago, status BOOTSTRAPPING: Running bootstrap actions
    Job launched 120.3s ago, status BOOTSTRAPPING: Running bootstrap actions
    Job launched 150.3s ago, status BOOTSTRAPPING: Running bootstrap actions
    Job launched 180.4s ago, status RUNNING: Running step (buggy_job.username.20110811.185410.536519: Step 1 of 1)
    Opening ssh tunnel to Hadoop job tracker
    Connect to job tracker at: http://dev5sj.sjc.yelpcorp.com:40753/jobtracker.jsp
    Job launched 211.5s ago, status RUNNING: Running step (buggy_job.username.20110811.185410.536519: Step 1 of 1)
     map 100% reduce   0%
    Job launched 241.8s ago, status RUNNING: Running step (buggy_job.username.20110811.185410.536519: Step 1 of 1)
     map   0% reduce   0%
    Job launched 271.9s ago, status RUNNING: Running step (buggy_job.username.20110811.185410.536519: Step 1 of 1)
     map 100% reduce 100%
    Job failed with status WAITING: Waiting after step failed
    Logs are in s3://scratch-bucket/tmp/logs/j-1NXMMBNEQHAFT/
    Scanning SSH logs for probable cause of failure
    Probable cause of failure (from ssh://ec2-50-18-136-229.us-west-1.compute.amazonaws.com/mnt/var/log/hadoop/userlogs/attempt_201108111855_0001_m_000001_3/stderr):
    Traceback (most recent call last):
      File "buggy_job.py", line 36, in <module>
        MRWordFreqCount.run()
      File "/usr/lib/python2.5/site-packages/mrjob/job.py", line 448, in run
        mr_job.execute()
      File "/usr/lib/python2.5/site-packages/mrjob/job.py", line 455, in execute
        self.run_mapper(self.options.step_num)
      File "/usr/lib/python2.5/site-packages/mrjob/job.py", line 548, in run_mapper
        for out_key, out_value in mapper(key, value) or ():
      File "buggy_job.py", line 24, in mapper
        raise IndexError
    IndexError
    Traceback (most recent call last):
      File "wrapper.py", line 16, in <module>
        check_call(sys.argv[1:])
      File "/usr/lib/python2.5/subprocess.py", line 462, in check_call
        raise CalledProcessError(retcode, cmd)
    subprocess.CalledProcessError: Command '['python', 'buggy_job.py', '--step-num=0', '--mapper']' returned non-zero exit status 1
    (while reading from s3://scratch-bucket/tmp/buggy_job.username.20110811.185410.536519/input/00000-README.rst)
    Killing our SSH tunnel (pid 988)
    ...


The same job flow ID can be used to start new jobs without waiting for a new
job flow to bootstrap.

Note that SSH must be set up for logs to be scanned from persistent jobs.

Finding Failures After the Fact
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you are trying to look at a failure after the original process has exited,
you can use the :py:mod:`mrjob.tools.emr.fetch_logs` tool to scan the logs::

    > python -m mrjob.tools.emr.fetch_logs --find-failure j-1NXMMBNEQHAFT
    using configs in /etc/mrjob.conf
    Scanning SSH logs for probable cause of failure
    Probable cause of failure (from ssh://ec2-50-18-136-229.us-west-1.compute.amazonaws.com/mnt/var/log/hadoop/userlogs/attempt_201108111855_0001_m_000001_3/stderr):
    Traceback (most recent call last):
      File "buggy_job.py", line 36, in <module>
        MRWordFreqCount.run()
      File "/usr/lib/python2.5/site-packages/mrjob/job.py", line 448, in run
        mr_job.execute()
      File "/usr/lib/python2.5/site-packages/mrjob/job.py", line 455, in execute
        self.run_mapper(self.options.step_num)
      File "/usr/lib/python2.5/site-packages/mrjob/job.py", line 548, in run_mapper
        for out_key, out_value in mapper(key, value) or ():
      File "buggy_job.py", line 24, in mapper
        raise IndexError
    IndexError
    Traceback (most recent call last):
      File "wrapper.py", line 16, in <module>
        check_call(sys.argv[1:])
      File "/usr/lib/python2.5/subprocess.py", line 462, in check_call
        raise CalledProcessError(retcode, cmd)
    subprocess.CalledProcessError: Command '['python', 'buggy_job.py', '--step-num=0', '--mapper']' returned non-zero exit status 1
    (while reading from s3://scratch-bucket/tmp/buggy_job.username.20110811.185410.536519/input/00000-README.rst)
    Removing all files in s3://scratch-bucket/tmp/no_script.username.20110811.190217.810442/

Determining Cause of Failure when mrjob Can't
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In some cases, :py:mod:`mrjob` will be unable to find the reason your job failed, or it will report an error that was merely a symptom of a larger problem. You
can look at the logs yourself by using Amazon's `elastic-mapreduce <http://aws.amazon.com/developertools/2264>`_ tool to SSH to the master node::

    > elastic-mapreduce --ssh j-1NXMMBNEQHAFT
    ssh -i /nail/etc/EMR.pem.dev hadoop@ec2-50-18-136-229.us-west-1.compute.amazonaws.com
    ...
    hadoop@ip-10-172-51-151:~$ grep --recursive 'Traceback' /mnt/var/log/hadoop
    /mnt/var/log/hadoop/userlogs/attempt_201108111855_0001_m_000000_0/stderr:Traceback (most recent call last):
    ...
    hadoop@ip-10-172-51-151:~$ cat /mnt/var/log/hadoop/userlogs/attempt_201108111855_0001_m_000000_0/stderr
    Exception exceptions.RuntimeError: 'generator ignored GeneratorExit' in <generator object at 0x94d57cc> ignored
    Traceback (most recent call last):
      File "mr_word_freq_count.py", line 36, in <module>
        MRWordFreqCount.run()
      File "/usr/lib/python2.5/site-packages/mrjob/job.py", line 448, in run
        mr_job.execute()
      File "/usr/lib/python2.5/site-packages/mrjob/job.py", line 455, in execute
        self.run_mapper(self.options.step_num)
      File "/usr/lib/python2.5/site-packages/mrjob/job.py", line 548, in run_mapper
        for out_key, out_value in mapper(key, value) or ():
      File "mr_word_freq_count.py", line 24, in mapper
        raise IndexError
    IndexError
    Traceback (most recent call last):
      File "wrapper.py", line 16, in <module>
        check_call(sys.argv[1:])
      File "/usr/lib/python2.5/subprocess.py", line 462, in check_call
        raise CalledProcessError(retcode, cmd)
    subprocess.CalledProcessError: Command '['python', 'mr_word_freq_count.py', '--step-num=0', '--mapper']' returned non-zero exit status 1
    java.lang.RuntimeException: PipeMapRed.waitOutputThreads(): subprocess failed with code 1
        at org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads(PipeMapRed.java:372)
        at org.apache.hadoop.streaming.PipeMapRed.mapRedFinished(PipeMapRed.java:582)
        at org.apache.hadoop.streaming.PipeMapper.close(PipeMapper.java:135)
        at org.apache.hadoop.mapred.MapRunner.run(MapRunner.java:57)
        at org.apache.hadoop.streaming.PipeMapRunner.run(PipeMapRunner.java:36)
        at org.apache.hadoop.mapred.MapTask.runOldMapper(MapTask.java:363)
        at org.apache.hadoop.mapred.MapTask.run(MapTask.java:312)
        at org.apache.hadoop.mapred.Child.main(Child.java:170)
