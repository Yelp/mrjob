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

.. mrjob-opt::
    :config: aws_secret_access_key
    :switch: --aws-secret-access-key
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    your "password" on AWS

.. mrjob-opt::
    :config: ec2_key_pair
    :switch: --ec2-key-pair
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    name of the SSH key you set up for EMR.

.. mrjob-opt::
    :config: ec2_key_pair_file
    :switch: --ec2-key-pair-file
    :type: :ref:`path <data-type-path>`
    :set: emr
    :default: ``None``

    path to file containing the SSH key for EMR

Job flow creation and configuration
-----------------------------------

.. mrjob-opt::
    :config: additional_emr_info
    :switch: --additional-emr-info
    :type: special
    :set: emr
    :default: ``None``

    Special parameters to select additional features, mostly to support beta
    EMR features. Pass a JSON string on the command line or use data
    structures in the config file (which is itself basically JSON).

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

.. mrjob-opt::
    :config: aws_availability_zone
    :switch: --aws-availability-zone
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: AWS default

    Availability zone to run the job in

.. mrjob-opt::
    :config: aws_region
    :switch: --aws-region
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: infer from scrach bucket region

    region to connect to S3 and EMR on (e.g.  ``us-west-1``). If you want to
    use separate regions for S3 and EMR, set :mrjob-opt:`emr_endpoint` and
    :mrjob-opt:`s3_endpoint`.

.. mrjob-opt::
    :config: emr_endpoint
    :switch: --emr-endpoint
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: infer from :mrjob-opt:`aws_region`

    optional host to connect to when communicating with S3 (e.g.
    ``us-west-1.elasticmapreduce.amazonaws.com``).

.. mrjob-opt::
    :config: hadoop_streaming_jar_on_emr
    :switch: --hadoop-streaming-jar-on-emr
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: AWS default

    Like :mrjob-opt:`hadoop_streaming_jar`, except that it points to a path on
    the EMR instance, rather than to a local file or one on S3. Rarely
    necessary to set this by hand.

.. mrjob-opt::
    :config: max_hours_idle
    :switch: --max-hours-idle
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    If we create a persistent job flow, have it automatically terminate itself
    after it's been idle this many hours AND we're within
    :mrjob-opt:`mins_to_end_of_hour` of an EC2 billing hour.

    .. versionadded:: 0.4.1

.. mrjob-opt::
    :config: mins_to_end_of_hour
    :switch: --mins-to-end-of-hour
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: 5.0

    If :mrjob-opt:`max_hours_idle` is set, controls how close to the end of an
    EC2 billing hour the job flow can automatically terminate itself.

    .. versionadded:: 0.4.1

.. mrjob-opt::
    :config: visible_to_all_users
    :switch: --visible-to-all-users
    :type: boolean
    :set: emr
    :default: ``False``

    If ``True``, EMR job flows will be visible to all IAM users. If ``False``,
    the job flow will only be visible to the IAM user that created it.

    .. versionadded:: 0.4.1

Bootstrapping
-------------

These options apply at *bootstrap time*, before the Hadoop cluster has
started. Bootstrap time is a good time to install Debian packages or compile
and install another Python binary. See :ref:`configs-making-files-available`
for task-time setup.

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

.. mrjob-opt::
    :config: bootstrap_files
    :switch: --bootstrap-file
    :type: :ref:`path list <data-type-path-list>`
    :set: emr
    :default: ``[]``

    files to download to the bootstrap working directory on the master node
    before running :mrjob-opt:`bootstrap_cmds` (for example, Debian packages).
    May be local files for mrjob to upload to S3, or any URI that ``hadoop fs``
    can handle.

.. mrjob-opt::
    :config: bootstrap_python_packages
    :switch: --bootstrap-python-package
    :type: :ref:`path list <data-type-path-list>`
    :set: emr
    :default: ``[]``

    paths of python modules to install on EMR. These should be standard Python
    module tarballs. If a module is named ``foo.tar.gz``, we expect to be able
    to run ``tar xfz foo.tar.gz; cd foo; sudo python setup.py install``.

.. mrjob-opt::
    :config: bootstrap_scripts
    :switch: --bootstrap-script
    :type: :ref:`path list <data-type-path-list>`
    :set: emr
    :default: ``[]``

    scripts to upload and then run on the master node (a combination of
    :mrjob-opt:`bootstrap_cmds` and :mrjob-opt:`bootstrap_files`). These are
    run after the commands from :mrjob-opt:`bootstrap_cmds`.

Monitoring the job flow
-----------------------

.. mrjob-opt::
    :config: check_emr_status_every
    :switch: --check-emr-status-every
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: 30

    How often to check on the status of EMR jobs in seconds. If you set this
    too low, AWS will throttle you.

.. mrjob-opt::
    :config: enable_emr_debugging
    :switch: --enable-emr-debugging
    :type: boolean
    :set: emr
    :default: ``False``

    store Hadoop logs in SimpleDB

Number and type of instances
----------------------------

.. mrjob-opt::
    :config: ec2_instance_type
    :switch: --ec2-instance-type
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``'m1.small'``

    What sort of EC2 instance(s) to use on the nodes that actually run tasks
    (see http://aws.amazon.com/ec2/instance-types/).  When you run multiple
    instances (see :mrjob-opt:`num_ec2_instances`), the master node is just
    coordinating the other nodes, so usually the default instance type
    (``m1.small``) is fine, and using larger instances is wasteful.

.. mrjob-opt::
    :config: ec2_core_instance_type
    :switch: --ec2-core-instance-type
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``'m1.small'``

    like :mrjob-opt:`ec2_instance_type`, but only for the core (also know as
    "slave") Hadoop nodes; these nodes run tasks and host HDFS. Usually you
    just want to use :mrjob-opt:`ec2_instance_type`.

.. mrjob-opt::
    :config: ec2_core_instance_bid_price
    :switch: --ec2-core-instance-bid-price
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    When specified and not "0", this creates the master Hadoop node as a spot
    instance at this bid price.  You usually only want to set bid price for
    task instances.

.. mrjob-opt::
    :config: ec2_master_instance_type
    :switch: --ec2-master-instance-type
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``'m1.small'``

    like :mrjob-opt:`ec2_instance_type`, but only for the master Hadoop node.
    This node hosts the task tracker and HDFS, and runs tasks if there are no
    other nodes. Usually you just want to use :mrjob-opt:`ec2_instance_type`.

.. mrjob-opt::
    :config: ec2_master_instance_bid_price
    :switch: --ec2-master-instance-bid-price
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    When specified and not "0", this creates the master Hadoop node as a spot
    instance at this bid price. You usually only want to set bid price for
    task instances unless the master instance is your only instance.

.. mrjob-opt::
    :config: ec2_slave_instance_type
    :switch: --ec2-slave-instance-type
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: value of :mrjob-opt:`ec2_core_instance_type`

    An alias for :mrjob-opt:`ec2_core_instance_type`, for consistency with the
    EMR API.

.. mrjob-opt::
    :config: ec2_task_instance_type
    :switch: --ec2-task-instance-type
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: value of :mrjob-opt:`ec2_core_instance_type`

    like :mrjob-opt:`ec2_instance_type`, but only for the task Hadoop nodes;
    these nodes run tasks but do not host HDFS. Usually you just want to use
    :mrjob-opt:`ec2_instance_type`.

.. mrjob-opt::
    :config: ec2_task_instance_bid_price
    :switch: --ec2-task-instance-bid-price
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    When specified and not "0", this creates the master Hadoop node as a spot
    instance at this bid price.  (You usually only want to set bid price for
    task instances.)

.. mrjob-opt::
    :config: num_ec2_core_instances
    :switch: --num-ec2-core-instances
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: 0

    Number of core (or "slave") instances to start up. These run your job and
    host HDFS. Incompatible with :mrjob-opt:`num_ec2_instances`. This is in
    addition to the single master instance.

.. mrjob-opt::
    :config: num_ec2_instances
    :switch: --num-ec2-instances
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: 1

    Total number of instances to start up; basically the number of core
    instance you want, plus 1 (there is always one master instance).
    Incompatible with :mrjob-opt:`num_ec2_core_instances` and
    :mrjob-opt:`num_ec2_task_instances`.

.. mrjob-opt::
    :config: num_ec2_task_instances
    :switch: --num-ec2-task-instances
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: 0

    Number of task instances to start up.  These run your job but do not host
    HDFS. Incompatible with :mrjob-opt:`num_ec2_instances`. If you use this,
    you must set :mrjob-opt:`num_ec2_core_instances`; EMR does not allow you to
    run task instances without core instances (because there's nowhere to host
    HDFS).

Choosing/creating a job flow to join
------------------------------------

.. mrjob-opt::
    :config: emr_job_flow_id
    :switch: --emr-job-flow-id
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: automatically create a job flow and use it

    The ID of a persistent EMR job flow to run jobs in.  It's fine for other
    jobs to be using the job flow; we give our job's steps a unique ID.

.. mrjob-opt::
    :config: emr_job_flow_pool_name
    :switch: --emr-job-flow-pool-name
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``'default'``

    Specify a pool name to join. Does not imply ``pool_emr_job_flows``.

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
    flow.

    .. warning:: Do not run this without either setting
        :mrjob-opt:`max_hours_idle` or putting
        :py:mod:`mrjob.tools.emr.terminate.idle_job_flows` in your crontab; job
        flows left idle can quickly become expensive!

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

.. mrjob-opt::
    :config: s3_endpoint
    :switch: --s3-endpoint
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: infer from :mrjob-opt:`aws_region`

    Host to connect to when communicating with S3 (e.g.
    ``s3-us-west-1.amazonaws.com``).

.. mrjob-opt::
    :config: s3_log_uri
    :switch: --s3-log-uri
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: append ``logs`` to :mrjob-opt:`s3_scratch_uri`

    Where on S3 to put logs, for example ``s3://yourbucket/logs/``. Logs for
    your job flow will go into a subdirectory, e.g.
    ``s3://yourbucket/logs/j-JOBFLOWID/``. in this example
    s3://yourbucket/logs/j-YOURJOBID/).

.. mrjob-opt::
    :config: s3_scratch_uri
    :switch: --s3-scratch-uri
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``tmp/mrjob`` in the first bucket belonging to you

    S3 directory (URI ending in ``/``) to use as scratch space, e.g.
    ``s3://yourbucket/tmp/``.

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

.. mrjob-opt::
    :config: ssh_bin
    :switch: --ssh-bin
    :type: :ref:`command <data-type-command>`
    :set: emr
    :default: ``'ssh'``

    Path to the ssh binary; may include switches (e.g.  ``'ssh -v'`` or
    ``['ssh', '-v']``). Defaults to :command:`ssh`

.. mrjob-opt::
    :config: ssh_bind_ports
    :switch: --ssh-bind-ports
    :type: special
    :set: emr
    :default: ``[40001, ..., 40840]``

    A list of ports that are safe to listen on. The command line syntax looks
    like ``2000[:2001][,2003,2005:2008,etc]``, where commas separate ranges and
    colons separate range endpoints.

.. mrjob-opt::
    :config: ssh_tunnel_to_job_tracker
    :switch: --ssh-tunnel-to-job-tracker
    :type: boolean
    :set: emr
    :default: ``False``

    If True, create an ssh tunnel to the job tracker and listen on a randomly
    chosen port. This requires you to set :mrjob-opt:`ec2_key_pair` and
    :mrjob-opt:`ec2_key_pair_file`. See :ref:`ssh-tunneling` for detailed
    instructions.

.. mrjob-opt::
    :config: ssh_tunnel_is_open
    :switch: --ssh-tunnel-is-open
    :type: boolean
    :set: emr
    :default: ``False``

    if True, any host can connect to the job tracker through the SSH tunnel
    you open.  Mostly useful if your browser is running on a different machine
    from your job runner.
