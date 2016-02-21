Troubleshooting
===============

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
S3 five minutes after the job completes, or immediately after the cluster
terminates.

Using persistent clusters
--------------------------

When troubleshooting a job, it can be convenient to use a persistent cluster
to avoid having to wait for bootstrapping every run. **If you decide to use
persistent clusters, add** :py:mod:`mrjob.tools.emr.terminate_idle_clusters`
**to your crontab or you will be billed for unused CPU time if you forget to
explicitly terminate clusters.**

First, use the :py:mod:`mrjob.tools.emr.create_cluster` tool to create a
persistent cluster::

    $ mrjob create-cluster
    using configs in /etc/mrjob.conf
    Creating persistent cluster to run several jobs in...
    creating tmp directory /scratch/username/no_script.username.20110811.185141.422311
    writing master bootstrap script to /scratch/username/no_script.username.20110811.185141.422311/b.py
    Copying non-input files into s3://scratch-bucket/tmp/no_script.username.20110811.185141.422311/files/
    Waiting 5.0s for S3 eventual consistency
    Creating Elastic MapReduce cluster
    Cluster created with ID: j-1NXMMBNEQHAFT
    j-1NXMMBNEQHAFT

Now you can use the cluster ID to start the troublesome job::

    $ python mrjob/buggy_job.py -r emr --cluster-id=j-1NXMMBNEQHAFT input/* > out
    using configs in /etc/mrjob.conf
    Uploading input to s3://scratch-bucket/tmp/buggy_job.username.20110811.185410.536519/input/
    creating tmp directory /scratch/username/buggy_job.username.20110811.185410.536519
    writing wrapper script to /scratch/username/buggy_job.username.20110811.185410.536519/wrapper.py
    Copying non-input files into s3://scratch-bucket/tmp/buggy_job.username.20110811.185410.536519/files/
    Adding our job to cluster j-1NXMMBNEQHAFT
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
      File "/usr/lib/python2.7/site-packages/mrjob/job.py", line 448, in run
        mr_job.execute()
      File "/usr/lib/python2.7/site-packages/mrjob/job.py", line 455, in execute
        self.run_mapper(self.options.step_num)
      File "/usr/lib/python2.7/site-packages/mrjob/job.py", line 548, in run_mapper
        for out_key, out_value in mapper(key, value) or ():
      File "buggy_job.py", line 24, in mapper
        raise IndexError
    IndexError
    Traceback (most recent call last):
      File "wrapper.py", line 16, in <module>
        check_call(sys.argv[1:])
      File "/usr/lib/python2.7/subprocess.py", line 462, in check_call
        raise CalledProcessError(retcode, cmd)
    subprocess.CalledProcessError: Command '['python', 'buggy_job.py', '--step-num=0', '--mapper']' returned non-zero exit status 1
    (while reading from s3://scratch-bucket/tmp/buggy_job.username.20110811.185410.536519/input/00000-README.rst)
    Killing our SSH tunnel (pid 988)
    ...


The same cluster ID can be used to start new jobs without waiting for a new
cluster to bootstrap.

Note that SSH must be set up for logs to be scanned from persistent jobs.

Determining cause of failure when mrjob can't
---------------------------------------------

In some cases, :py:mod:`mrjob` will be unable to find the reason your job
failed, or it will report an error that was merely a symptom of a larger
problem. You can look at the logs yourself by using the `AWS Command Line
Interface <https://aws.amazon.com/cli/>`_ to SSH to the master node::

    > aws emr ssh --cluster-id j-1NXMMBNEQHAFT --key-pair-file /nail/etc/EMR.pem.dev
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
      File "/usr/lib/python2.7/site-packages/mrjob/job.py", line 448, in run
        mr_job.execute()
      File "/usr/lib/python2.7/site-packages/mrjob/job.py", line 455, in execute
        self.run_mapper(self.options.step_num)
      File "/usr/lib/python2.7/site-packages/mrjob/job.py", line 548, in run_mapper
        for out_key, out_value in mapper(key, value) or ():
      File "mr_word_freq_count.py", line 24, in mapper
        raise IndexError
    IndexError
    Traceback (most recent call last):
      File "wrapper.py", line 16, in <module>
        check_call(sys.argv[1:])
      File "/usr/lib/python2.7/subprocess.py", line 462, in check_call
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
