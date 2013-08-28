EMR runner options
==================

All options from :doc:`configs-all-runners` and :doc:`configs-hadoopy-runners`
are available to the emr runner.

Amazon credentials
------------------

See :ref:`amazon-setup` and :ref:`ssh-tunneling` for specific instructions
about setting these options.

.. mrjob-opt::
    :config: aws_access_key_id
    :switch: --aws-access-key-id
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    "username" for Amazon web services.

.. _opt_aws_secret_access_key:

.. mrjob-opt::
    :config: aws_secret_access_key
    :switch: --aws-secret-access-key
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    your "password" on AWS

.. _opt_ec2_key_pair:

.. mrjob-opt::
    :config: ec2_key_pair
    :switch: --ec2-key-pair
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    name of the SSH key you set up for EMR.

.. _opt_ec2_key_pair_file:

.. mrjob-opt::
    :config: ec2_key_pair_file
    :switch: --ec2-key-pair-file
    :type: :ref:`path <data-type-path>`
    :set: emr
    :default: ``None``

    path to file containing the SSH key for EMR

Job flow creation and configuration
-----------------------------------

.. _opt_additional_emr_info:

.. mrjob-opt::
    :config: additional_emr_info
    :switch: --additional-emr-info
    :type: special
    :set: emr
    :default: ``None``

    Special parameters to select additional features, mostly to support beta
    EMR features. Pass a JSON string on the command line or use data
    structures in the config file (which is itself basically JSON).

.. _opt_ami_version:

.. mrjob-opt::
    :config: ami_version
    :switch: --ami-version
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``'latest'``

    EMR AMI version to use. This controls which Hadoop version(s) are
    available and which version of Python is installed, among other things;
    see `the AWS docs on specifying the AMI version`_.  for details.

    .. _`the AWS docs on specifying the AMI version`:
        http://docs.amazonwebservices.com/ElasticMapReduce/latest/DeveloperGuide/EnvironmentConfig_AMIVersion.html

.. _opt_aws_availability_zone:

.. mrjob-opt::
    :config: aws_availability_zone
    :switch: --aws-availability-zone
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: AWS default

    Availability zone to run the job in

.. _opt_aws_region:

.. mrjob-opt::
    :config: aws_region
    :switch: --aws-region
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: infer from scrach bucket region

    region to connect to S3 and EMR on (e.g.  ``us-west-1``). If you want to
    use separate regions for S3 and EMR, set
    :ref:`emr_endpoint <opt_emr_endpoint>` and
    :ref:`s3_endpoint <opt_s3_endpoint>`.

.. _opt_emr_endpoint:

.. mrjob-opt::
    :config: emr-endpoint
    :switch: --emr-endpoint
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: infer from :ref:`aws_region <opt_aws_region>`

    optional host to connect to when communicating with S3 (e.g.
    ``us-west-1.elasticmapreduce.amazonaws.com``).

.. _opt_hadoop_streaming_jar_on_emr:

.. mrjob-opt::
    :config: hadoop_streaming_jar_on_emr
    :switch: --hadoop-streaming-jar-on-emr
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: AWS default

    Like :ref:`hadoop_streaming_jar <opt_hadoop_streaming_jar>`, except that it
    points to a path on the EMR instance, rather than to a local file or one on
    S3. Rarely necessary to set this by hand.

.. _opt_max_hours_idle:

.. mrjob-opt::
    :config: max_hours_idle
    :switch: --max-hours-idle
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    If we create a persistent job flow, have it automatically terminate itself
    after it's been idle this many hours AND we're within
    :ref:`mins_to_end_of_hour <opt_mins_to_end_of_hour>` of an EC2 billing
    hour.

.. _opt_mins_to_end_of_hour:

.. mrjob-opt::
    :config: mins_to_end_of_hour
    :switch: --mins-to-end-of-hour
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: 5.0

    If :ref:`max_hours_idle <opt_max_hours_idle>` is set, controls how close
    to the end of an EC2 billing hour the job flow can automatically terminate
    itself.

Bootstrapping
-------------

These options apply at *bootstrap time*, before the Hadoop cluster has
started. Bootstrap time is a good time to install Debian packages or compile
and install another Python binary. See :ref:`configs-making-files-available`
for task-time setup.

.. _opt_bootstrap_actions:

.. mrjob-opt::
    :config: bootstrap_actions
    :switch: --bootstrap-actions
    :type: :ref:`string list <data-type-string-list>`
    :set: emr
    :default: ``[]``

    a list of raw bootstrap actions (essentially scripts) to run prior to any
    of the other bootstrap steps. Any arguments should be separated from the
    command by spaces (we use :py:func:`shlex.split`). If the action is on the
    local filesystem, we'll automatically upload it to S3.

.. _opt_bootstrap_cmds:

.. mrjob-opt::
    :config: bootstrap_cmds
    :switch: --bootstrap-cmd
    :type: :ref:`string list <data-type-string-list>`
    :set: emr
    :default: ``[]``

    a list of commands to run on the master node to set up libraries, etc.
    Like *setup_cmds*, these can be strings, which will be run in the shell,
    or lists of args, which will be run directly.  Prepend ``sudo`` to
    commands to do things that require root privileges.

.. _opt_bootstrap_files:

.. mrjob-opt::
    :config: bootstrap_files
    :switch: --bootstrap-file
    :type: :ref:`path list <data-type-path-list>`
    :set: emr
    :default: ``[]``

    files to download to the bootstrap working directory on the master node
    before running :ref:`bootstrap_cmds <opt_bootstrap_cmds>` (for example,
    Debian packages). May be local files for mrjob to upload to S3, or any URI
    that ``hadoop fs`` can handle.

.. _opt_bootstrap_python_packages:

.. mrjob-opt::
    :config: bootstrap_python_packages
    :switch: --bootstrap-python-package
    :type: :ref:`path list <data-type-path-list>`
    :set: emr
    :default: ``[]``

    paths of python modules to install on EMR. These should be standard Python
    module tarballs. If a module is named ``foo.tar.gz``, we expect to be able
    to run ``tar xfz foo.tar.gz; cd foo; sudo python setup.py install``.

.. _opt_bootstrap_scripts:

.. mrjob-opt::
    :config: bootstrap_scripts
    :switch: --bootstrap-script
    :type: :ref:`path list <data-type-path-list>`
    :set: emr
    :default: ``[]``

    scripts to upload and then run on the master node (a combination of
    :ref:`bootstrap_cmds <opt_bootstrap_cmds>` and
    :ref:`bootstrap_files <opt_bootstrap_files>`). These are run after the commands
    from :ref:`bootstrap_cmds <opt_bootstrap_cmds>`.

Monitoring the job flow
-----------------------

.. _opt_check_emr_status_every:

.. mrjob-opt::
    :config: check_emr_status_every
    :switch: --check-emr-status-every
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: 30

    How often to check on the status of EMR jobs in seconds. If you set this
    too low, AWS will throttle you.

.. _opt_enable_emr_debugging:

.. mrjob-opt::
    :config: enable_emr_debugging
    :switch: --enable-emr-debugging
    :type: boolean
    :set: emr
    :default: ``False``

    store Hadoop logs in SimpleDB

Number and type of instances
----------------------------

.. _opt_ec2_instance_type:

.. mrjob-opt::
    :config: ec2_instance_type
    :switch: --ec2-instance-type
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``'m1.small'``

    What sort of EC2 instance(s) to use on the nodes that actually run tasks
    (see http://aws.amazon.com/ec2/instance-types/).  When you run multiple
    instances (see :ref:`num_ec2_instances <opt_num_ec2_instances>`), the
    master node is just coordinating the other nodes, so usually the default
    instance type (``m1.small``) is fine, and using larger instances is
    wasteful.

.. _opt_ec2_core_instance_type:

.. mrjob-opt::
    :config: ec2_core_instance_type
    :switch: --ec2-core-instance-type
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``'m1.small'``

    like :ref:`ec2_instance_type <opt_ec2_instance_type>`, but only for the
    core (also know as "slave") Hadoop nodes; these nodes run tasks and host
    HDFS. Usually you just want to use
    :ref:`ec2_instance_type <opt_ec2_instance_type>`.

.. _opt_ec2_core_instance_bid_price:

.. mrjob-opt::
    :config: ec2_core_instance_bid_price
    :switch: --ec2-core-instance-bid-price
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    When specified and not "0", this creates the master Hadoop node as a spot
    instance at this bid price.  You usually only want to set bid price for
    task instances.

.. _opt_ec2_master_instance_type:

.. mrjob-opt::
    :config: ec2_master_instance_type
    :switch: --ec2-master-instance-type
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``'m1.small'``

    like :ref:`ec2_instance_type <opt_ec2_instance_type>`, but only for the
    master Hadoop node. This node hosts the task tracker and HDFS, and runs
    tasks if there are no other nodes. Usually you just want to use
    :ref:`ec2_instance_type <opt_ec2_instance_type>`.

.. _opt_ec2_master_instance_bid_price:

.. mrjob-opt::
    :config: ec2_master_instance_bid_price
    :switch: --ec2-master-instance-bid-price
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    When specified and not "0", this creates the master Hadoop node as a spot
    instance at this bid price. You usually only want to set bid price for
    task instances unless the master instance is your only instance.

.. _opt_ec2_slave_instance_type:

.. mrjob-opt::
    :config: ec2_slave_instance_type
    :switch: --ec2-slave-instance-type
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: value of :ref:`ec2_core_instance_type <opt_ec2_core_instance_type>`

    An alias for :ref:`ec2_core_instance_type <opt_ec2_core_instance_type>`,
    for consistency with the EMR API.

.. _opt_ec2_task_instance_type:

.. mrjob-opt::
    :config: ec2_task_instance_type
    :switch: --ec2-task-instance-type
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: value of :ref:`ec2_core_instance_type <opt_ec2_core_instance_type>`

    like :ref:`ec2_instance_type <opt_ec2_instance_type>`, but only for the
    task Hadoop nodes; these nodes run tasks but do not host HDFS. Usually you
    just want to use :ref:`ec2_instance_type <opt_ec2_instance_type>`.

.. _opt_ec2_task_instance_bid_price:

.. mrjob-opt::
    :config: ec2_task_instance_bid_price
    :switch: --ec2-task-instance-bid-price
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    When specified and not "0", this creates the master Hadoop node as a spot
    instance at this bid price.  (You usually only want to set bid price for
    task instances.)

.. _opt_num_ec2_core_instances:

.. mrjob-opt::
    :config: num_ec2_core_instances
    :switch: --num-ec2-core-instances
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: 0

    Number of core (or "slave") instances to start up. These run your job and
    host HDFS. Incompatible with
    :ref:`num_ec2_instances <opt_num_ec2_instances>`. This is in addition to
    the single master instance.

.. _opt_num_ec2_instances:

.. mrjob-opt::
    :config: num_ec2_instances
    :switch: --num-ec2-instances
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: 1

    Total number of instances to start up; basically the number of core
    instance you want, plus 1 (there is always one master instance).
    Incompatible with
    :ref:`num_ec2_core_instances <opt_num_ec2_core_instances>` and
    :ref:`num_ec2_task_instances <opt_num_ec2_task_instances>`.

.. _opt_num_ec2_task_instances:

.. mrjob-opt::
    :config: num_ec2_task_instances
    :switch: --num-ec2-task-instances
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: 0

    Number of task instances to start up.  These run your job but do not host
    HDFS. Incompatible with :ref:`num_ec2_instances <opt_num_ec2_instances>`.
    If you use this, you must set
    :ref:`num_ec2_core_instances <opt_num_ec2_core_instances>`; EMR does not
    allow you to run task instances without core instances (because there's
    nowhere to host HDFS).

Choosing/creating a job flow to join
------------------------------------

.. _opt_emr_job_flow_id:

.. mrjob-opt::
    :config: emr_job_flow_id
    :switch: --emr-job-flow-id
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: automatically create a job flow and use it

    The ID of a persistent EMR job flow to run jobs in.  It's fine for other
    jobs to be using the job flow; we give our job's steps a unique ID.

.. _opt_emr_job_flow_pool_name:

.. mrjob-opt::
    :config: emr_job_flow_pool_name
    :switch: --emr-job-flow-pool-name
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``'default'``

    Specify a pool name to join. Does not imply ``pool_emr_job_flows``.

.. _opt_pool_emr_job_flows:

.. mrjob-opt::
    :config: pool_emr_job_flows
    :switch: --pool-emr-job-flows
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``False``

    Try to run the job on a ``WAITING`` pooled job flow with the same
    bootstrap configuration. Prefer the one with the most compute units. Use
    S3 to "lock" the job flow and ensure that the job is not scheduled behind
    another job. If no suitable job flow is `WAITING`, create a new pooled job
    flow.  **WARNING**: do not run this without having\
    :py:mod:`mrjob.tools.emr.terminate.idle_job_flows` in your crontab; job
    flows left idle can quickly become expensive!

.. _opt_pool_wait_minutes:

.. mrjob-opt::
    :config: pool_wait_minutes
    :switch: --pool-wait-minutes
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: 0

    If pooling is enabled and no job flow is available, retry finding a job
    flow every 30 seconds until this many minutes have passed, then start a new
    job flow instead of joining one.

S3 paths and options
--------------------

.. _opt_s3_endpoint:

.. mrjob-opt::
    :config: s3_endpoint
    :switch: --s3-endpoint
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: infer from :ref:`aws_region <opt_aws_region>`

    Host to connect to when communicating with S3 (e.g.
    ``s3-us-west-1.amazonaws.com``).

.. _opt_s3_log_uri:

.. mrjob-opt::
    :config: s3_log_uri
    :switch: --s3-log-uri
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: append ``logs`` to :ref:`s3_scratch_uri <opt_s3_scratch_uri>`

    Where on S3 to put logs, for example ``s3://yourbucket/logs/``. Logs for
    your job flow will go into a subdirectory, e.g.
    ``s3://yourbucket/logs/j-JOBFLOWID/``. in this example
    s3://yourbucket/logs/j-YOURJOBID/).

.. _opt_s3_scratch_uri:

.. mrjob-opt::
    :config: s3_scratch_uri
    :switch: --s3-scratch-uri
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``tmp/mrjob`` in the first bucket belonging to you

    S3 directory (URI ending in ``/``) to use as scratch space, e.g.
    ``s3://yourbucket/tmp/``.

.. _opt_s3_sync_wait_time:

.. mrjob-opt::
    :config: s3_sync_wait_time
    :switch: --s3-sync-wait-time
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: 5.0

    How long to wait for S3 to reach eventual consistency. This is typically
    less than a second (zero in U.S. West), but the default is 5.0 to be safe.

SSH access and tunneling
------------------------

.. _opt_ssh_bin:

.. mrjob-opt::
    :config: ssh_bin
    :switch: --ssh-bin
    :type: :ref:`command <data-type-command>`
    :set: emr
    :default: ``'ssh'``

    Path to the ssh binary; may include switches (e.g.  ``'ssh -v'`` or
    ``['ssh', '-v']``). Defaults to :command:`ssh`

.. _opt_ssh_bind_ports:

.. mrjob-opt::
    :config: ssh_bind_ports
    :switch: --ssh-bind-ports
    :type: special
    :set: emr
    :default: ``[40001, ..., 40840]``

    A list of ports that are safe to listen on. The command line syntax looks
    like ``2000[:2001][,2003,2005:2008,etc]``, where commas separate ranges and
    colons separate range endpoints.

.. _opt_ssh_tunnel_to_job_tracker:

.. mrjob-opt::
    :config: ssh_tunnel_to_job_tracker
    :switch: --ssh-tunnel-to-job-tracker
    :type: boolean
    :set: emr
    :default: ``False``

    If True, create an ssh tunnel to the job tracker and listen on a randomly
    chosen port. This requires you to set
    :ref:`ec2_key_pair <opt_ec2_key_pair>` and
    :ref:`ec2_key_pair_file <opt_ec2_key_pair_file>`.
    See :ref:`ssh-tunneling` for detailed instructions.

.. _opt_ssh_tunnel_is_open:

.. mrjob-opt::
    :config: ssh_tunnel_is_open
    :switch: --ssh-tunnel-is-open
    :type: boolean
    :set: emr
    :default: ``False``

    if True, any host can connect to the job tracker through the SSH tunnel
    you open.  Mostly useful if your browser is running on a different machine
    from your job runner.
