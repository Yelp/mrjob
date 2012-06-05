EMR runner options
==================

All options from :doc:`configs-all-runners` and :doc:`configs-hadoopy-runners`
are available to the emr runner.

Amazon credentials
------------------

See :doc:`emr-config` for specific instructions about setting these options.

**aws_access_key_id** (:option:`--aws_access_key_id`)
    "username" for Amazon web services.

**aws_secret_access_key** (:option:`--aws_secret_access_key`)
    your "password" on AWS

**ec2_key_pair** (:option:`--ec2_key_pair`)
    name of the SSH key you set up for EMR.

**ec2_key_pair_file** (:option:`--ec2_key_pair_file`)
    path to file containing the SSH key for EMR

Job flow creation and configuration
-----------------------------------

**additional_emr_info** (:option:`--additional_emr_info`)
    Special parameters to select additional features, mostly to support beta
    EMR features. Pass a JSON string on the command line or use data
    structures in the config file (which is itself basically JSON).

**ami_version** (:option:`--ami_version`)
    EMR AMI version to use. This controls which Hadoop version(s) are
    available and which version of Python is installed, among other things;
    see `the AWS docs on specifying the AMI version`_.  for details.
    Implicitly defaults to AMI version 1.0 (this will change to 2.0 in mrjob
    v0.4).

    .. _`the AWS docs on specifying the AMI version`:
        http://docs.amazonwebservices.com/ElasticMapReduce/latest/DeveloperGuide/EnvironmentConfig_AMIVersion.html

**aws_availability_zone** (:option:`--aws_availability_zone`)
    availability zone to run the job in

**aws_region** (:option:`--aws_region`)
    region to connect to S3 and EMR on (e.g.  ``us-west-1``). If you want to
    use separate regions for S3 and EMR, set *emr_endpoint* and *s3_endpoint*.

**emr_endpoint** (:option:`--emr_endpoint`)
    optional host to connect to when communicating with S3 (e.g.
    ``us-west-1.elasticmapreduce.amazonaws.com``).  Default is to infer this
    from *aws_region*.

**hadoop_streaming_jar_on_emr** (:option:`--hadoop_streaming_jar_on_emr`)
    Like *hadoop_streaming_jar*, except that it points to a path on the EMR
    instance, rather than to a local file or one on S3. Rarely necessary to
    set this by hand.

Bootstrapping
-------------

These options apply at *bootstrap time*, before the Hadoop cluster has
started. Bootstrap time is a good time to install Debian packages or compile
and install another Python binary. See :ref:`configs-making-files-available`
for task-time setup.

**bootstrap_actions** (:option:`--bootstrap_actions`)
    a list of raw bootstrap actions (essentially scripts) to run prior to any
    of the other bootstrap steps. Any arguments should be separated from the
    command by spaces (we use :py:func:`shlex.split`). If the action is on the
    local filesystem, we'll automatically upload it to S3.

**bootstrap_cmds** (:option:`--bootstrap_cmds`)
    a list of commands to run on the master node to set up libraries, etc.
    Like *setup_cmds*, these can be strings, which will be run in the shell,
    or lists of args, which will be run directly.  Prepend ``sudo`` to
    commands to do things that require root privileges.

**bootstrap_files** (:option:`--bootstrap_files`)
    files to download to the bootstrap working directory on the master node
    before running *bootstrap_cmds* (for example, Debian packages). May be
    local files for mrjob to upload to S3, or any URI that ``hadoop fs`` can
    handle.

**bootstrap_python_packages** (:option:`--bootstrap_python_packages`)
    paths of python modules to install on EMR. These should be standard Python
    module tarballs. If a module is named ``foo.tar.gz``, we expect to be able
    to run ``tar xfz foo.tar.gz; cd foo; sudo python setup.py install``.

**bootstrap_scripts** (:option:`--bootstrap_scripts`)
    scripts to upload and then run on the master node (a combination of
    *bootstrap_cmds* and *bootstrap_files*). These are run after the command
    from bootstrap_cmds.

Monitoring the job flow
-----------------------

**check_emr_status_every** (:option:`--check_emr_status_every`)
    How often to check on the status of EMR jobs. Default is 30 seconds (too
    often and AWS will throttle you anyway).

**enable_emr_debugging** (:option:`--enable_emr_debugging`)
    store Hadoop logs in SimpleDB

Number and type of instances
----------------------------

**ec2_instance_type** (:option:`--ec2_instance_type`)
    What sort of EC2 instance(s) to use on the nodes that actually run tasks
    (see http://aws.amazon.com/ec2/instance-types/).  When you run multiple
    instances (see *num_ec2_instances*), the master node is just coordinating
    the other nodes, so usually the default instance type (``m1.small``) is
    fine, and using larger instances is wasteful.

**ec2_core_instance_type** (:option:`--ec2_core_instance_type`)
    like *ec2_instance_type*, but only for the core (also know as "slave")
    Hadoop nodes; these nodes run tasks and host HDFS. Usually you just want
    to use *ec2_instance_type*. Defaults to ``'m1.small'``.

**ec2_core_instance_bid_price** (:option:`--ec2_core_instance_bid_price`)
    when specified and not "0", this creates the master Hadoop node as a spot
    instance at this bid price.  You usually only want to set bid price for
    task instances.

**ec2_master_instance_type** (:option:`--ec2_master_instance_type`)
    like *ec2_instance_type*, but only for the master Hadoop node. This node
    hosts the task tracker and HDFS, and runs tasks if there are no other
    nodes. Usually you just want to use *ec2_instance_type*. Defaults to
    ``'m1.small'``.

**ec2_master_instance_bid_price** (:option:`--ec2_master_instance_bid_price`)
    when specified and not "0", this creates the master Hadoop node as a spot
    instance at this bid price. You usually only want to set bid price for
    task instances unless the master instance is your only instance.

**ec2_slave_instance_type** (:option:`--ec2_slave_instance_type`)
    An alias for *ec2_core_instance_type*, for consistency with the EMR API.

**ec2_task_instance_type** (:option:`--ec2_task_instance_type`)
    like *ec2_instance_type*, but only for the task Hadoop nodes; these nodes
    run tasks but do not host HDFS. Usually you just want to use
    *ec2_instance_type*. Defaults to the same instance type as
    *ec2_core_instance_type*.

**ec2_task_instance_bid_price** (:option:`--ec2-task-instance-bid-price`)
    when specified and not "0", this creates the master Hadoop node as a spot
    instance at this bid price.  (You usually only want to set bid price for
    task instances.)

**num_ec2_core_instances** (:option:`--num_ec2_core_instances`)
    Number of core (or "slave") instances to start up. These run your job and
    host HDFS. Incompatible with *num_ec2_instances*. This is in addition to
    the single master instance.

**num_ec2_instances** (:option:`--num_ec2_instances`)
    Total number of instances to start up; basically the number of core
    instance you want, plus 1 (there is always one master instance). Default
    is ``1``. Incompatible with *num_ec2_core_instances* and
    *num_ec2_task_instances*.

**num_ec2_task_instances** (:option:`--num_ec2_task_instances`)
    number of task instances to start up.  These run your job but do not host
    HDFS. Incompatible with *num_ec2_instances*. If you use this, you must
    set *num_ec2_core_instances*; EMR does not allow you to run task instances
    without core instances (because there's nowhere to host HDFS).

Choosing/creating a job flow to join
------------------------------------

**emr_job_flow_id** (:option:`--emr_job_flow_id`)
    the ID of a persistent EMR job flow to run jobs in (normally we launch our
    own job flow). It's fine for other jobs to be using the job flow; we give
    our job's steps a unique ID.

**emr_job_flow_pool_name** (:option:`--emr_job_flow_pool_name`)
    Specify a pool name to join. Is set to ``'default'`` if not specified.
    Does not imply ``pool_emr_job_flows``.

**pool_emr_job_flows** (:option:`--pool_emr_job_flows`)
    Try to run the job on a ``WAITING`` pooled job flow with the same
    bootstrap configuration. Prefer the one with the most compute units. Use
    S3 to "lock" the job flow and ensure that the job is not scheduled behind
    another job. If no suitable job flow is `WAITING`, create a new pooled job
    flow.  **WARNING**: do not run this without having\
    :py:mod:`mrjob.tools.emr.terminate.idle_job_flows` in your crontab; job
    flows left idle can quickly become expensive!

S3 paths and options
--------------------

**s3_endpoint** (:option:`--s3_endpoint`)
    Host to connect to when communicating with S3 (e.g.
    ``s3-us-west-1.amazonaws.com``). Default is to infer this from
    *aws_region*.

**s3_log_uri** (:option:`--s3_log_uri`)
    where on S3 to put logs, for example ``s3://yourbucket/logs/``. Logs for
    your job flow will go into a subdirectory, e.g.
    ``s3://yourbucket/logs/j-JOBFLOWID/``. in this example
    s3://yourbucket/logs/j-YOURJOBID/). Default is to append ``logs/`` to
    *s3_scratch_uri*.

**s3_scratch_uri** (:option:`--s3_scratch_uri`)
    S3 directory (URI ending in ``/``) to use as scratch space, e.g.
    ``s3://yourbucket/tmp/``.  Default is ``tmp/mrjob/`` in the first bucket
    belonging to you.

**s3_sync_wait_time** (:option:`--s3_sync_wait_time`)
    How long to wait for S3 to reach eventual consistency. This is typically
    less than a second (zero in U.S. West) but the default is 5.0 to be safe.

SSH access and tunneling
------------------------

**ssh_bin** (:option:`--ssh_bin`)
    path to the ssh binary; may include switches (e.g.  ``'ssh -v'`` or
    ``['ssh', '-v']``). Defaults to :command:`ssh`

**ssh_bind_ports** (:option:`--ssh_bind_ports`)
    a list of ports that are safe to listen on.  Defaults to ports ``40001``
    thru ``40840``.

**ssh_tunnel_to_job_tracker** (:option:`--ssh_tunnel_to_job_tracker`)
    If True, create an ssh tunnel to the job tracker and listen on a randomly
    chosen port. This requires you to set *ec2_key_pair* and
    *ec2_key_pair_file*. See :ref:`ssh-tunneling` for detailed instructions.

**ssh_tunnel_is_open** (:option:`--ssh_tunnel_is_open`)
    if True, any host can connect to the job tracker through the SSH tunnel
    you open.  Mostly useful if your browser is running on a different machine
    from your job runner.
