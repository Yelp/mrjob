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
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    "Username" for Amazon web services.

    There isn't a command-line switch for this option because credentials are
    supposed to be secret! Use the environment variable
    :envvar:`AWS_ACCESS_KEY_ID` instead.

.. mrjob-opt::
    :config: aws_secret_access_key
    :switch: --aws-secret-access-key
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    Your "password" on AWS.

    There isn't a command-line switch for this option because credentials are
    supposed to be secret! Use the environment variable
    :envvar:`AWS_SECRET_ACCESS_KEY` instead.

.. mrjob-opt::
    :config: aws_security_token
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    Temporary AWS session token, used along with :mrjob-opt:`aws_access_key_id`
    and :mrjob-opt:`aws_secret_access_key` when using temporary credentials.

    There isn't a command-line switch for this option because credentials are
    supposed to be secret! Use the environment variable
    :envvar:`AWS_SECURITY_TOKEN` instead.

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

Cluster creation and configuration
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
    :default: ``'3.11.0'``

    EMR AMI version to use. This controls which Hadoop version(s) are
    available and which version of Python is installed, among other things;
    see `the AWS docs on specifying the AMI version`_.  for details.

    .. _`the AWS docs on specifying the AMI version`:
        http://docs.amazonwebservices.com/ElasticMapReduce/latest/DeveloperGuide/EnvironmentConfig_AMIVersion.html

    This works for 4.x AMIs as well; mrjob will just prepend ``emr-`` and
    use that as the :mrjob-opt:`release_label`.

    .. warning::

        The 1.x series of AMIs is no longer supported because they use Python
        2.5.

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
    :default: ``'us-west-2'``

    region to run EMR jobs on (e.g.  ``us-west-1``). Also used by mrjob
    to create temporary buckets if you don't set :mrjob-opt:`cloud_tmp_dir`
    explicitly.

.. mrjob-opt::
    :config: emr_api_params
    :switch: --emr-api-param, --no-emr-api-param
    :type: :ref:`dict <data-type-plain-dict>`
    :set: emr
    :default: ``{}``

    Additional raw parameters to pass directly to the EMR API when creating a
    cluster. This allows old versions of `mrjob` to access new API features.
    See `the API documentation for RunJobFlow`_ for the full list of options.

    .. _`the API documentation for RunJobFlow`:
        http://docs.aws.amazon.com/ElasticMapReduce/latest/API/API_RunJobFlow.html

    Option names and values are strings. On the command line, to set an option
    use ``--emr-api-param KEY=VALUE``:

    .. code-block:: sh

        --emr-api-param Instances.Ec2SubnetId=someID

    and to suppress a value that would normally be passed to the API, use
    ``--no-emr-api-param``:

    .. code-block:: sh

        --no-emr-api-param VisibleToAllUsers

    In the config file, ``emr_api_params`` is a dict; params can be suppressed
    by setting them to ``null``:

    .. code-block:: yaml

        runners:
          emr:
            emr_api_params:
              Instances.Ec2SubnetId: someID
              VisibleToAllUsers: null

    .. versionadded:: 0.4.3

.. mrjob-opt::
   :config: emr_applications
   :switch: --emr-application
   :type: :ref:`string list <data-type-string-list>`
   :set: emr
   :default: ``[]``

   Additional applications to run on 4.x AMIs (e.g. ``'Ganglia'``,
   ``'Mahout'``, ``'Spark'``). You do not need to specify ``'Hadoop'``;
   mrjob will always include it automatically.

   See `Applications <http://docs.aws.amazon.com/ElasticMapReduce/latest/ReleaseGuide/emr-release-components.html>`_ in the EMR docs for more details.

   .. versionadded:: 0.5.2

.. mrjob-opt::
    :config: emr_configurations
    :switch: --emr-configuration
    :type: list of dicts
    :set: emr
    :default: ``[]``

    Configurations for 4.x AMIs. For example:

    .. code-block:: yaml

        runners:
          emr:
            emr_configurations:
            - Classification: core-site
              Properties:
                hadoop.security.groups.cache.secs: 250

    On the command line, configurations should be JSON-encoded:

    .. code-block:: sh

        --emr-configuration '{"Classification": "core-site", ...}

    See `Configuring Applications <http://docs.aws.amazon.com/ElasticMapReduce/latest/ReleaseGuide/emr-configure-apps.html>`_ in the EMR docs for more details.

    .. versionadded:: 0.5.3

.. mrjob-opt::
    :config: emr_endpoint
    :switch: --emr-endpoint
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: infer from :mrjob-opt:`aws_region`

    Force mrjob to connect to EMR on this endpoint (e.g.
    ``us-west-1.elasticmapreduce.amazonaws.com``).

    Mostly exists as a workaround for network issues.

.. mrjob-opt::
    :config: emr_tags
    :switch: --emr-tag
    :type: :ref:`dict <data-type-plain-dict>`
    :set: emr
    :default: ``{}``

    Metadata tags to apply to the EMR cluster after its
    creation. See `Tagging Amazon EMR Clusters`_ for more information
    on applying metadata tags to EMR clusters.

    .. _`Tagging Amazon EMR Clusters`:
        http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-plan-tags.html

    Tag names and values are strings. On the command line, to set a tag
    use ``--emr-tag KEY=VALUE``:

    .. code-block:: sh

        --emr-tag team=development

    In the config file, ``emr_tags`` is a dict:

    .. code-block:: yaml

        runners:
          emr:
            emr_tags:
              team: development
              project: mrjob

    .. versionadded:: 0.4.5

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
    :config: iam_endpoint
    :switch: --iam-endpoint
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: (automatic)

    Force mrjob to connect to IAM on this endpoint (e.g.
    ``iam.us-gov.amazonaws.com``).

    Mostly exists as a workaround for network issues.

.. mrjob-opt::
    :config: iam_instance_profile
    :switch: --iam-instance-profile
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: (automatic)

    Name of an IAM instance profile to use for EC2 clusters created by EMR. See
    http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-iam-roles.html
    for more details on using IAM with EMR.

    .. versionadded:: 0.4.3

.. mrjob-opt::
    :config: iam_service_role
    :switch: --iam-service-role
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: (automatic)

    Name of an IAM role for the EMR service to use. See
    http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-iam-roles.html
    for more details on using IAM with EMR.

    .. versionadded:: 0.4.4

.. mrjob-opt::
    :config: max_hours_idle
    :switch: --max-hours-idle
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    If we create a persistent cluster, have it automatically terminate itself
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
    EC2 billing hour the cluster can automatically terminate itself.

    .. versionadded:: 0.4.1

.. mrjob-opt::
    :config: release_label
    :switch: --release-label
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    EMR Release to use (e.g. ``emr-4.0.0``). This overrides
    :mrjob-opt:`ami_version`.

    For more information about Release Labels, see
    `Differences Introduced in 4.x`_.

    .. _`Differences Introduced in 4.x`:
        http://docs.aws.amazon.com/ElasticMapReduce/latest/ReleaseGuide/emr-release-differences.html

    .. versionadded:: 0.5.0

.. mrjob-opt::
   :config: subnet
   :switch: --subnet
   :type: :ref:`string <data-type-string>`
   :set: emr
   :default: ``None``

   ID of Amazon VPC subnet to launch cluster in (e.g. ``'subnet-12345678'``).
   If this is not set, or an empty string, cluster will be launched in the
   normal AWS cloud.

   .. versionadded:: 0.5.3

.. mrjob-opt::
    :config: visible_to_all_users
    :switch: --visible-to-all-users, --no-visible-to-all-users
    :type: boolean
    :set: emr
    :default: ``True``

    If true (the default) EMR clusters will be visible to all IAM users.
    Otherwise, the cluster will only be visible to the IAM user that created
    it.

    .. warning::

        You should almost certainly not set this to ``False`` if you are
        :ref:`pooling-clusters` with other users; other users will
        not be able to reuse your clusters, and
        :command:`mrjob terminate-idle-clusters` won't be
        able to shut them down when they become idle.

    .. versionadded:: 0.4.1

Bootstrapping
-------------

These options apply at *bootstrap time*, before the Hadoop cluster has
started. Bootstrap time is a good time to install Debian packages or compile
and install another Python binary.

.. mrjob-opt::
    :config: bootstrap
    :switch: --bootstrap
    :type: :ref:`string list <data-type-string-list>`
    :set: all
    :default: ``[]``

    A list of lines of shell script to run once on each node in your cluster,
    at bootstrap time.

    This option is complex and powerful; the best way to get started is to
    read the :doc:`emr-bootstrap-cookbook`.

    Passing expressions like ``path#name`` will cause
    *path* to be automatically uploaded to the task's working directory
    with the filename *name*, marked as executable, and interpolated into the
    script by their absolute path on the machine running the script.

    *path*
    may also be a URI, and ``~`` and environment variables within *path*
    will be resolved based on the local environment. *name* is optional.
    For details of parsing, see :py:func:`~mrjob.setup.parse_setup_cmd`.

    Unlike with :mrjob-opt:`setup`, archives are not supported (unpack them
    yourself).

    Remember to put ``sudo`` before commands requiring root privileges!

.. mrjob-opt::
    :config: bootstrap_actions
    :switch: --bootstrap-actions
    :type: :ref:`string list <data-type-string-list>`
    :set: emr
    :default: ``[]``

    A list of raw bootstrap actions (essentially scripts) to run prior to any
    of the other bootstrap steps. Any arguments should be separated from the
    command by spaces (we use :py:func:`shlex.split`). If the action is on the
    local filesystem, we'll automatically upload it to S3.

    This has little advantage over :mrjob-opt:`bootstrap`; it is included
    in order to give direct access to the EMR API.

.. mrjob-opt::
    :config: bootstrap_cmds
    :switch: --bootstrap-cmd
    :type: :ref:`string list <data-type-string-list>`
    :set: emr
    :default: ``[]``

    .. deprecated:: 0.4.2

    A list of commands to run at bootstrap time. Basically
    :mrjob-opt:`bootstrap` without automatic file uploading/interpolation.
    Can also take commands as lists of arguments.

.. mrjob-opt::
    :config: bootstrap_files
    :switch: --bootstrap-file
    :type: :ref:`path list <data-type-path-list>`
    :set: emr
    :default: ``[]``

    .. deprecated:: 0.4.2

    Files to download to the bootstrap working directory before running
    bootstrap commands. Use the :mrjob-opt:`bootstrap` option's file
    auto-upload/interpolation feature instead.

.. mrjob-opt::
   :config: bootstrap_python
   :switch: --bootstrap-python, --no-bootstrap-python
   :type: boolean
   :set: emr
   :default: (automatic)

   Attempt to install a compatible (major) version of Python at bootstrap time,
   including header files and :command:`pip` (see :ref:`using-pip`).

   In Python 2, this never does anything.

   In Python 3, this runs
   :command:`sudo yum install -y python34 python34-devel python34-pip`
   by default on AMIs prior to 4.6.0 (starting with AMI 4.6.0, Python 3 is
   pre-installed).

   .. versionadded:: 0.5.0

   .. versionchanged:: 0.5.4

      no longer installs Python 3 on AMI version 4.6.0+ by default

.. mrjob-opt::
    :config: bootstrap_python_packages
    :switch: --bootstrap-python-package
    :type: :ref:`path list <data-type-path-list>`
    :set: emr
    :default: ``[]``

    .. deprecated:: 0.4.2

    Paths of python modules tarballs to install on EMR. Pass
    :command:`sudo` :mrjob-opt:`python_bin`
    :command:`-m pip install path/to/package.tar.gz#` to
    :mrjob-opt:`bootstrap` instead.

    Also, please consider installing packages directly from
    `PyPI <https://pypi.python.org/pypi>`_ instead (
    :command:`sudo` :mrjob-opt:`python_bin`
    :command:`-m pip install package1 package2`); PyPI is much, much more
    robust/production-ready than when this option was first created.

.. mrjob-opt::
    :config: bootstrap_scripts
    :switch: --bootstrap-script
    :type: :ref:`path list <data-type-path-list>`
    :set: emr
    :default: ``[]``

    .. deprecated:: 0.4.2

    Scripts to upload and then run at bootstrap time. Pass
    ``path/to/script# args`` to :mrjob-opt:`bootstrap` instead.

Monitoring the cluster
-----------------------

.. mrjob-opt::
    :config: check_cluster_every
    :switch: --check-cluster-every
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: 30

    How often to check on the status of EMR jobs in seconds. If you set this
    too low, AWS will throttle you.

    .. versionchanged:: 0.5.4

       This option used to be called ``check_emr_status_every``

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
    :config: instance_type
    :switch: --instance-type
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``'m1.medium'``

    What sort of EC2 instance(s) to use on the nodes that actually run tasks
    (see http://aws.amazon.com/ec2/instance-types/).  When you run multiple
    instances (see :mrjob-opt:`num_core_instances`), the master node is just
    coordinating the other nodes, so usually the default instance type
    (``m1.medium``) is fine, and using larger instances is wasteful.

    .. versionchanged:: 0.5.4

       This option used to be named ``ec2_instance_type``.

.. mrjob-opt::
    :config: core_instance_type
    :switch: --core-instance-type
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``'m1.medium'``

    like :mrjob-opt:`instance_type`, but only for the core Hadoop nodes; these
    nodes run tasks and host HDFS. Usually you just want to use
    :mrjob-opt:`instance_type`.

    .. versionchanged:: 0.5.4

       This replaces the ``ec2_core_instance_type`` and
       ``ec2_slave_instance_type`` options.

.. mrjob-opt::
    :config: core_instance_bid_price
    :switch: --core-instance-bid-price
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    When specified and not "0", this creates the core Hadoop nodes as spot
    instances at this bid price.  You usually only want to set bid price for
    task instances.

    .. versionchanged:: 0.5.4

       This option used to be named ``ec2_core_instance_bid_price``.

.. mrjob-opt::
    :config: master_instance_type
    :switch: --master-instance-type
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``'m1.medium'``

    like :mrjob-opt:`instance_type`, but only for the master Hadoop node.
    This node hosts the task tracker and HDFS, and runs tasks if there are no
    other nodes. Usually you just want to use :mrjob-opt:`instance_type`.

    .. versionchanged:: 0.5.4

       This option used to be named ``ec2_master_instance_type``.

.. mrjob-opt::
    :config: master_instance_bid_price
    :switch: --master-instance-bid-price
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    When specified and not "0", this creates the master Hadoop node as a spot
    instance at this bid price. You usually only want to set bid price for
    task instances unless the master instance is your only instance.

    .. versionchanged:: 0.5.4

       This option used to be named ``ec2_master_instance_bid_price``.

.. mrjob-opt::
    :config: task_instance_type
    :switch: --task-instance-type
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: value of :mrjob-opt:`core_instance_type`

    like :mrjob-opt:`instance_type`, but only for the task Hadoop nodes;
    these nodes run tasks but do not host HDFS. Usually you just want to use
    :mrjob-opt:`instance_type`.

    .. versionchanged:: 0.5.4

       This option used to be named ``ec2_task_instance_type``.

.. mrjob-opt::
    :config: task_instance_bid_price
    :switch: --task-instance-bid-price
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    When specified and not "0", this creates the master Hadoop node as a spot
    instance at this bid price.  (You usually only want to set bid price for
    task instances.)

    .. versionchanged:: 0.5.4

       This option used to be named ``ec2_task_instance_bid_price``.

.. mrjob-opt::
    :config: num_core_instances
    :switch: --num-core-instances
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: 0

    Number of core instances to start up. These run your job and
    host HDFS. This is in addition to the single master instance.

    .. versionchanged:: 0.5.4

       This option used to be named ``num_ec2_core_instances``.

.. mrjob-opt::
    :config: num_ec2_instances
    :switch: --num-ec2-instances
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: 1

    Total number of instances to start up; basically the number of core
    instance you want, plus 1 (there is always one master instance).
    Incompatible with :mrjob-opt:`num_core_instances` and
    :mrjob-opt:`num_task_instances`.

    .. deprecated:: 0.5.4

       Just subtract one and pass that to :mrjob-opt:`num_core_instances`.

.. mrjob-opt::
    :config: num_task_instances
    :switch: --num-task-instances
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: 0

    Number of task instances to start up.  These run your job but do not host
    HDFS. Incompatible with :mrjob-opt:`num_ec2_instances`. If you use this,
    you must set :mrjob-opt:`num_core_instances`; EMR does not allow you to
    run task instances without core instances (because there's nowhere to host
    HDFS).

    .. versionchanged:: 0.5.4

       This used to be called ``num_ec2_task_instances``.

Choosing/creating a cluster to join
------------------------------------

.. mrjob-opt::
    :config: emr_action_on_failure
    :switch: --emr-action-on-failure
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: (automatic)

    What happens if step of your job fails

    * ``'CANCEL_AND_WAIT'`` cancels all steps on the cluster
    * ``'CONTINUE'`` continues to the next step (useful when submitting several
        jobs to the same cluster)
    * ``'TERMINATE_CLUSTER'`` shuts down the cluster entirely

    The default is ``'CANCEL_AND_WAIT'`` when using pooling (see
    :mrjob-opt:`pool_clusters`) or an existing cluster (see
    :mrjob-opt:`cluster_id`), and ``'TERMINATE_CLUSTER'`` otherwise.

    .. versionadded:: 0.4.3

.. mrjob-opt::
    :config: cluster_id
    :switch: --cluster-id
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: automatically create a cluster and use it

    The ID of a persistent EMR cluster to run jobs in.  It's fine for other
    jobs to be using the cluster; we give our job's steps a unique ID.

.. mrjob-opt::
    :config: pool_name
    :switch: --pool-name
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``'default'``

    Specify a pool name to join. Does not imply :mrjob-opt:`pool_clusters`.

.. mrjob-opt::
    :config: pool_clusters
    :switch: --pool-clusters
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``False``

    Try to run the job on a ``WAITING`` pooled cluster with the same
    bootstrap configuration. Prefer the one with the most compute units. Use
    S3 to "lock" the cluster and ensure that the job is not scheduled behind
    another job. If no suitable cluster is `WAITING`, create a new pooled
    cluster.

    .. warning:: Do not run this without either setting
        :mrjob-opt:`max_hours_idle` or putting
        :command:`mrjob terminate-idle-clusters` in your crontab;
        clusters left idle can quickly become expensive!

.. mrjob-opt::
    :config: pool_wait_minutes
    :switch: --pool-wait-minutes
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: 0

    If pooling is enabled and no cluster is available, retry finding a cluster
    every 30 seconds until this many minutes have passed, then start a new
    cluster instead of joining one.


S3 paths and options
--------------------
MRJob uses boto to manipulate/access S3.

.. mrjob-opt::
    :config: s3_endpoint
    :switch: --s3-endpoint
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: (automatic)

    Force mrjob to connect to S3 on this endpoint, rather than letting it
    choose the appropriate endpoint for each S3 bucket.

    Mostly exists as a workaround for network issues.

    .. warning:: If you set this to a region-specific endpoint
                 (e.g. ``'s3-us-west-1.amazonaws.com'``) mrjob will not
                 be able to access buckets located in other regions.

.. mrjob-opt::
    :config: cloud_log_dir
    :switch: --cloud-log-dir
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: append ``logs`` to :mrjob-opt:`cloud_tmp_dir`

    Where on S3 to put logs, for example ``s3://yourbucket/logs/``. Logs for
    your cluster will go into a subdirectory, e.g.
    ``s3://yourbucket/logs/j-CLUSTERID/``.

    .. versionchanged:: 0.5.4

       This option used to be named ``s3_log_uri``

.. mrjob-opt::
    :config: cloud_tmp_dir
    :switch: --cloud-tmp-dir
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: (automatic)

    S3 directory (URI ending in ``/``) to use as temp space, e.g.
    ``s3://yourbucket/tmp/``.

    By default, mrjob looks for a bucket belong to you whose name starts with
    ``mrjob-`` and which matches :mrjob-opt:`aws_region`. If it can't find
    one, it creates one with a random name. This option is then set to `tmp/`
    in this bucket (e.g. ``s3://mrjob-01234567890abcdef/tmp/``).

    .. versionchanged:: 0.5.4

       This option used to be called ``s3_tmp_dir``.

    .. versionchanged:: 0.5.0

       This option used to be called ``s3_scratch_uri``.

.. mrjob-opt::
    :config: s3_sync_wait_time
    :switch: --s3-sync-wait-time
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: 5.0

    How long to wait for S3 to reach eventual consistency. This is typically
    less than a second (zero in U.S. West), but the default is 5.0 to be safe.

.. mrjob-opt::
   :config: cloud_upload_part_size
   :switch: --cloud-upload-part-size
   :type: integer
   :set: emr
   :default: 100

   Upload files to S3 in parts no bigger than this many megabytes
   (technically, `mebibytes`_). Default is 100 MiB, as
   `recommended by Amazon`_. Set to 0 to disable multipart uploading
   entirely.

   Currently, Amazon `requires parts to be between 5 MiB and 5 GiB`_.
   mrjob does not enforce these limits.

   .. _`mebibytes`:
       http://en.wikipedia.org/wiki/Mebibyte
   .. _`recommended by Amazon`:
       http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html
   .. _`requires parts to be between 5 MiB and 5 GiB`:
       http://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html

   .. versionchanged:: 0.5.4

      This option used to be called ``s3_upload_part_size``.

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
    :default: ``range(40001, 40841)``

    A list of ports that are safe to listen on. The command line syntax looks
    like ``2000[:2001][,2003,2005:2008,etc]``, where commas separate ranges and
    colons separate range endpoints.

.. mrjob-opt::
    :config: ssh_tunnel
    :switch: --ssh-tunnel, --no-ssh-tunnel
    :type: boolean
    :set: emr
    :default: ``False``

    If True, create an ssh tunnel to the job tracker/resource manager and
    listen on a randomly chosen port. This requires you to set
    :mrjob-opt:`ec2_key_pair` and :mrjob-opt:`ec2_key_pair_file`. See
    :ref:`ssh-tunneling` for detailed instructions.

    .. versionchanged:: 0.5.0

       This option used to be named ``ssh_tunnel_to_job_tracker``.

.. mrjob-opt::
    :config: ssh_tunnel_is_open
    :switch: --ssh-tunnel-is-open
    :type: boolean
    :set: emr
    :default: ``False``

    if True, any host can connect to the job tracker through the SSH tunnel
    you open.  Mostly useful if your browser is running on a different machine
    from your job runner.
