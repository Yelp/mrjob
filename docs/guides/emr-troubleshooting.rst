Troubleshooting
===============

Many things can go wrong in an EMR job, and the system's distributed nature
can make it difficult to find the source of a problem. :py:mod:`mrjob` attempts to
simplify the debugging process by automatically scanning logs for probable
causes of failure.

In addition to looking at S3, :py:mod:`mrjob` can be configured to
also use SSH to fetch error logs directly from the master and worker nodes.
This can speed up debugging significantly (EMR only transfers logs to S3
every five minutes).

Using persistent clusters
--------------------------

When troubleshooting a job, it can be convenient to use a persistent cluster
to avoid having to wait for bootstrapping every run.

.. warning::

   Make sure you either use the ``--max-hours-idle`` option or have
   :command:`mrjob terminate-idle-clusters` in your crontab, or you will
   billed for unused CPU time on any clusters you forget to terminate.

First, use the :command:`mrjob create-cluster` to create a
persistent cluster::

    $ mrjob create-cluster --max-hours-idle 1
    Using configs in /Users/davidmarin/.mrjob.conf
    Using s3://mrjob-35cdec11663cb1cb/tmp/ as our temp dir on S3
    Creating persistent cluster to run several jobs in...
    Creating temp directory /var/folders/zv/jmtt5bxs6xl3kzt38470hcxm0000gn/T/no_script.davidmarin.20160324.231018.720057
    Copying local files to s3://mrjob-35cdec11663cb1cb/tmp/no_script.davidmarin.20160324.231018.720057/files/...
    j-3BYHP30KB81XE

Now you can use the cluster ID to start the troublesome job::

    $ python mrjob/examples/mr_boom.py README.rst -r emr --cluster-id j-3BYHP30KB81XE
    Using configs in /Users/davidmarin/.mrjob.conf
    Using s3://mrjob-35cdec11663cb1cb/tmp/ as our temp dir on S3
    Creating temp directory /var/folders/zv/jmtt5bxs6xl3kzt38470hcxm0000gn/T/mr_boom.davidmarin.20160324.231045.501027
    Copying local files to s3://mrjob-35cdec11663cb1cb/tmp/mr_boom.davidmarin.20160324.231045.501027/files/...
    Adding our job to existing cluster j-3BYHP30KB81XE
    Waiting for step 1 of 1 (s-SGVW9B5LEXF5) to complete...
      PENDING (cluster is STARTING: Provisioning Amazon EC2 capacity)
      PENDING (cluster is STARTING: Provisioning Amazon EC2 capacity)
      PENDING (cluster is STARTING: Provisioning Amazon EC2 capacity)
      PENDING (cluster is STARTING: Provisioning Amazon EC2 capacity)
      PENDING (cluster is STARTING: Provisioning Amazon EC2 capacity)
      PENDING (cluster is BOOTSTRAPPING: Running bootstrap actions)
      PENDING (cluster is BOOTSTRAPPING: Running bootstrap actions)
      PENDING (cluster is BOOTSTRAPPING: Running bootstrap actions)
      PENDING (cluster is BOOTSTRAPPING: Running bootstrap actions)
      PENDING (cluster is BOOTSTRAPPING: Running bootstrap actions)
      Opening ssh tunnel to resource manager...
      Connect to resource manager at: http://localhost:40069/cluster
      RUNNING for 9.2s
      RUNNING for 42.3s
         0.0% complete
      RUNNING for 72.6s
         5.0% complete
      RUNNING for 102.9s
         5.0% complete
      RUNNING for 133.4s
       100.0% complete
      FAILED
    Cluster j-3BYHP30KB81XE is WAITING: Cluster ready after last step failed.
    Attempting to fetch counters from logs...
    Looking for step log in /mnt/var/log/hadoop/steps/s-SGVW9B5LEXF5 on ec2-52-37-112-240.us-west-2.compute.amazonaws.com...
      Parsing step log: ssh://ec2-52-37-112-240.us-west-2.compute.amazonaws.com/mnt/var/log/hadoop/steps/s-SGVW9B5LEXF5/syslog
    Counters: 9
    	Job Counters
    		Data-local map tasks=1
    		Failed map tasks=4
    		Launched map tasks=4
    		Other local map tasks=3
    		Total megabyte-seconds taken by all map tasks=58125312
    		Total time spent by all map tasks (ms)=75684
    		Total time spent by all maps in occupied slots (ms)=227052
    		Total time spent by all reduces in occupied slots (ms)=0
    		Total vcore-seconds taken by all map tasks=75684
    Scanning logs for probable cause of failure...
    Looking for task logs in /mnt/var/log/hadoop/userlogs/application_1458861299388_0001 on ec2-52-37-112-240.us-west-2.compute.amazonaws.com and task/core nodes...
      Parsing task syslog: ssh://ec2-52-37-112-240.us-west-2.compute.amazonaws.com/mnt/var/log/hadoop/userlogs/application_1458861299388_0001/container_1458861299388_0001_01_000005/syslog
      Parsing task stderr: ssh://ec2-52-37-112-240.us-west-2.compute.amazonaws.com/mnt/var/log/hadoop/userlogs/application_1458861299388_0001/container_1458861299388_0001_01_000005/stderr
    Probable cause of failure:

    PipeMapRed failed!
    java.lang.RuntimeException: PipeMapRed.waitOutputThreads(): subprocess failed with code 1
    	at org.apache.hadoop.streaming.PipeMapRed.waitOutputThreads(PipeMapRed.java:330)
    	at org.apache.hadoop.streaming.PipeMapRed.mapRedFinished(PipeMapRed.java:543)
    	at org.apache.hadoop.streaming.PipeMapper.close(PipeMapper.java:130)
    	at org.apache.hadoop.mapred.MapRunner.run(MapRunner.java:81)
    	at org.apache.hadoop.streaming.PipeMapRunner.run(PipeMapRunner.java:34)
    	at org.apache.hadoop.mapred.MapTask.runOldMapper(MapTask.java:432)
    	at org.apache.hadoop.mapred.MapTask.run(MapTask.java:343)
    	at org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:175)
    	at java.security.AccessController.doPrivileged(Native Method)
    	at javax.security.auth.Subject.doAs(Subject.java:415)
    	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1548)
    	at org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:170)

    (from lines 37-50 of ssh://ec2-52-37-112-240.us-west-2.compute.amazonaws.com/mnt/var/log/hadoop/userlogs/application_1458861299388_0001/container_1458861299388_0001_01_000005/syslog)

    caused by:

    Traceback (most recent call last):
      File "mr_boom.py", line 10, in <module>
        MRBoom.run()
      File "/usr/lib/python3.4/dist-packages/mrjob/job.py", line 430, in run
        mr_job.execute()
      File "/usr/lib/python3.4/dist-packages/mrjob/job.py", line 439, in execute
        self.run_mapper(self.options.step_num)
      File "/usr/lib/python3.4/dist-packages/mrjob/job.py", line 499, in run_mapper
        for out_key, out_value in mapper_init() or ():
      File "mr_boom.py", line 7, in mapper_init
        raise Exception('BOOM')
    Exception: BOOM

    (from lines 1-12 of ssh://ec2-52-37-112-240.us-west-2.compute.amazonaws.com/mnt/var/log/hadoop/userlogs/application_1458861299388_0001/container_1458861299388_0001_01_000005/stderr)

    while reading input from s3://mrjob-35cdec11663cb1cb/tmp/mr_boom.davidmarin.20160324.231045.501027/files/README.rst


    Step 1 of 1 failed
    Killing our SSH tunnel (pid 52847)

Now you can fix the bug and try again, without having to wait for a new
cluster to bootstrap.

.. note::

   mrjob *can* fetch logs from persistent jobs even without SSH set up, but
   it has to pause 10 minutes to wait for EMR to transfer logs to S3, which
   defeats the purpose of rapid iteration.
