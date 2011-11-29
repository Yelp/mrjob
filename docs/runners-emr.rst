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

.. _pooling-job-flows:

Pooling Job Flows
^^^^^^^^^^^^^^^^^

Manually creating job flows to reuse and specifying the job flow ID for every run can be tedious. In addition, it is not convenient to coordinate job flow use among multiple users.

To mitigate these problems, mrjob provides **job flow pools.** Rather than having to remember to start a job flow and copying its ID, simply pass :option:`--pool-emr-job-flows` on the command line. The first time you do this, a new job flow will be created that does not terminate when the job completes. When you use :option:`--pool-emr-job-flows` the next time, it will identify the job flow and add the job to it rather than creating a new one.

**If you use job flow pools, keep** :py:mod:`~mrjob.tools.emr.terminate_idle_job_flows` **in your crontab!** Otherwise you will forget to terminate your job flows and waste a lot of money.

The criteria for finding an appropriate job flow for a job are as follows:

* The job flow must be in the ``WAITING`` state.
* The bootstrap configuration (actions, packages, commands, etc.) must be identical. This is checked using an md5 sum.
* The **pool name** must be the same. You can specify a pool name with :option:`--pool-name`.
* The job flow must have at least as many instances, and  the instance type must have at least as many compute units and GB of memory, as the job configuration specifies. See `Amazon EC2 Instance Types <http://aws.amazon.com/ec2/instance-types/>`_ for a complete listing of instance types and their respective compute units.
* Ties are broken first by total compute units in the job flow as calculated by ``number of instances * instance type compute units``, then by the number of minutes until an even instance hour. This strategy minimizes wasted instance hours.

Most of the time you shouldn't need to worry about these things. Just use pool names to separate job flows into pools representing their type. As long as you keep passing the same bootstrapping arguments into your scripts, they will keep creating correctly-configured job flows.

EMR provides no way to remove steps from a job flow once they are added. This introduces a race condition where two users identify the same job flow and join it simultaneously, causing one user's job to be delayed until the first user's is finished. mrjob avoids this situation using an S3-based locking mechanism.

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

SSH tunneling and log fetching
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

Troubleshooting
---------------

Many things can go wrong in an EMR job, and the system's distributed nature
can make it difficult to find the source of a problem. mrjob attempts to
simplify the debugging process by automatically scanning logs for probable
causes of failure. Specifically, it looks at logs relevant to your job for
these errors:

* Python tracebacks and Java stack traces in ``$S3_LOG_URI/task-attempts/*``
* Hadoop Streaming errors in ``$S3_LOG_URI/steps/*``
* Timeout errors in ``$S3_LOG_URI/jobs/*``

As mentioned above, in addition to looking at S3, mrjob can be configured to
also use SSH to fetch error logs directly from the master and slave nodes.
This can speed up debugging significantly, because logs are only available on
S3 five minutes after the job completes, or immediately after the job flow
terminates.

Using persistent job flows
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

Finding failures after the fact
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

Determining cause of failure when mrjob can't
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In some cases, mrjob will be unable to find the reason your job failed. You
can look at the logs yourself by using Amazon's `elastic-mapreduce` tool to
SSH to the master node::

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
