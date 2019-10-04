EMR runner options
==================

All options from :doc:`configs-all-runners`, :doc:`configs-hadoopy-runners`,
and :doc:`cloud-opts` are available when running jobs on Amazon Elastic
MapReduce.

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
    :config: aws_session_token
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    Temporary AWS session token, used along with :mrjob-opt:`aws_access_key_id`
    and :mrjob-opt:`aws_secret_access_key` when using temporary credentials.

    There isn't a command-line switch for this option because credentials are
    supposed to be secret! Use the environment variable
    :envvar:`AWS_SESSION_TOKEN` instead.

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

.. mrjob-opt::
    :config: iam_instance_profile
    :switch: --iam-instance-profile
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: (automatic)

    Name of an IAM instance profile to use for EC2 clusters created by EMR. See
    http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-iam-roles.html
    for more details on using IAM with EMR.

.. mrjob-opt::
    :config: iam_service_role
    :switch: --iam-service-role
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: (automatic)

    Name of an IAM role for the EMR service to use. See
    http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-iam-roles.html
    for more details on using IAM with EMR.

Instance configuration
----------------------

On EMR, there are three ways to configure instances:

* :mrjob-opt:`instance_fleets`
* :mrjob-opt:`instance_groups`
* individual instance options:

  * :mrjob-opt:`core_instance_bid_price`
  * :mrjob-opt:`core_instance_type`
  * :mrjob-opt:`instance_type`
  * :mrjob-opt:`master_instance_bid_price`
  * :mrjob-opt:`master_instance_type`
  * :mrjob-opt:`num_core_instances`
  * :mrjob-opt:`num_task_instances`
  * :mrjob-opt:`task_instance_bid_price`,
  * :mrjob-opt:`task_instance_type`

If there is a conflict, whichever comes later in the config
files takes precedence, and the command line beats config files. In
the case of a tie, `instance_fleets` beats `instance_groups` beats
other instance options.

You may set :mrjob-opt:`ebs_root_volume_gb` regardless of which style
of instance configuration you use.


.. mrjob-opt::
    :config: instance_fleets
    :switch: --instance-fleet
    :set: emr
    :default: ``None``

    A list of instance fleet definitions to pass to the EMR API. Pass a JSON
    string on the command line or use data structures in the config file
    (which is itself basically JSON). For example:

    .. code-block:: yaml

        runners:
          emr:
            instance_fleets:
            - InstanceFleetType: MASTER
              InstanceTypeConfigs:
              - InstanceType: m1.medium
              TargetOnDemandCapacity: 1
            - InstanceFleetType: CORE
              TargetSpotCapacity: 2
              TargetOnDemandCapacity: 2
              LaunchSpecifications:
                SpotSpecification:
                  TimeoutDurationMinutes: 20
                  TimeoutAction: SWITCH_TO_ON_DEMAND
              InstanceTypeConfigs:
              - InstanceType: m1.medium
                BidPriceAsPercentageOfOnDemandPrice: 50
                WeightedCapacity: 1
              - InstanceType: m1.large
                BidPriceAsPercentageOfOnDemandPrice: 50
                WeightedCapacity: 2

.. mrjob-opt::
    :config: instance_groups
    :switch: --instance-groups
    :set: emr
    :default: ``None``

    A list of instance group definitions to pass to the EMR API. Pass a JSON
    string on the command line or use data structures in the config file
    (which is itself basically JSON).

    This allows for more fine-tuned EBS volume configuration than
    :mrjob-opt:`ebs_root_volume_gb`. For example:

    .. code-block:: yaml

        runners:
          emr:
            instance_groups:
            - InstanceRole: MASTER
              InstanceCount: 1
              InstanceType: m1.medium
            - InstanceRole: CORE
              InstanceCount: 10
              InstanceType: c1.xlarge
              EbsConfiguration:
                EbsOptimized: true
                EbsBlockDeviceConfigs:
                - VolumeSpecification:
                    SizeInGB: 100
                    VolumeType: gp2

    `instance_groups` is incompatible with :mrjob-opt:`instance_fleets`
    and other instance options. See :mrjob-opt:`instance_fleets` for
    details.

.. mrjob-opt::
    :config: core_instance_bid_price
    :switch: --core-instance-bid-price
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    When specified and not "0", this creates the core Hadoop nodes as spot
    instances at this bid price.  You usually only want to set bid price for
    task instances.

.. mrjob-opt::
    :config: master_instance_bid_price
    :switch: --master-instance-bid-price
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    When specified and not "0", this creates the master Hadoop node as a spot
    instance at this bid price. You usually only want to set bid price for
    task instances unless the master instance is your only instance.

.. mrjob-opt::
    :config: task_instance_bid_price
    :switch: --task-instance-bid-price
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    When specified and not "0", this creates the master Hadoop node as a spot
    instance at this bid price.  (You usually only want to set bid price for
    task instances.)

.. mrjob-opt::
    :config: ebs_root_volume_gb
    :switch: --ebs-root-volume-gb
    :type: integer
    :set: emr
    :default: ``None``

    When specified (and not zero), sets the size of the root EBS volume,
    in GiB.

    .. versionadded:: 0.6.5


Cluster software configuration
------------------------------

See also :mrjob-opt:`bootstrap`, :mrjob-opt:`image_id`, and
:mrjob-opt:`image_version`.

.. mrjob-opt::
   :config: applications
   :switch: --application, --applications
   :type: :ref:`string list <data-type-string-list>`
   :set: emr
   :default: ``[]``

   Additional applications to run on 4.x AMIs (e.g. ``'Ganglia'``,
   ``'Mahout'``, ``'Spark'``).

   You do not need to specify ``'Hadoop'``; mrjob will always include it
   automatically. In most cases it'll auto-detect when to include ``'Spark'``
   as well.

   See `Applications <http://docs.aws.amazon.com/ElasticMapReduce/latest/ReleaseGuide/emr-release-components.html>`_ in the EMR docs for more details.

   .. versionchanged:: 0.6.7

      Added :option:`--applications` switch

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
   :config: bootstrap_spark
   :switch: --bootstrap-spark, --no-bootstrap-spark
   :type: boolean
   :set: emr
   :default: (automatic)

   Install Spark on the cluster. This works on AMI version 3.x and later.

   By default, we automatically install Spark only if our job has Spark steps.

   In case you're curious, here's how mrjob determines you're using Spark:

   * any :py:class:`~mrjob.step.SparkStep` or
     :py:class:`~mrjob.step.SparkScriptStep` in your job's steps (including
     implicitly through the :py:class:`~mrjob.job.MRJob.spark` method)
   * "Spark" included in :mrjob-opt:`applications` option
   * any bootstrap action (see :mrjob-opt:`bootstrap_actions`) ending in
     ``/spark-install`` (this is how you install Spark on 3.x AMIs)

.. mrjob-opt::
    :config: emr_configurations
    :switch: --emr-configuration
    :type: list of dicts
    :set: emr
    :default: ``[]``

    Cluster configs for AMI version 4.x and later. For example:

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

    .. versionchanged:: 0.6.11

       ``!clear`` tag works. Later config dicts will overwrite earlier ones
       with the same ``Classification``. If the later dict has empty
       ``Properties`` and ``Configurations``, the earlier dict will be simply
       deleted.


.. mrjob-opt::
    :config: release_label
    :switch: --release-label
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    EMR Release to use (e.g. ``emr-4.0.0``). This overrides
    :mrjob-opt:`image_version`.

    For more information about Release Labels, see
    `Differences Introduced in 4.x`_.

    .. _`Differences Introduced in 4.x`:
        http://docs.aws.amazon.com/ElasticMapReduce/latest/ReleaseGuide/emr-release-differences.html

Monitoring your job
-------------------

See also :mrjob-opt:`check_cluster_every`, :mrjob-opt:`ssh_tunnel`.

.. mrjob-opt::
    :config: enable_emr_debugging
    :switch: --enable-emr-debugging
    :type: boolean
    :set: emr
    :default: ``False``

    store Hadoop logs in SimpleDB

Cluster pooling
---------------

.. mrjob-opt::
    :config: pool_clusters
    :switch: --pool-clusters
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``True``

    Try to run the job on a ``WAITING`` pooled cluster with the same
    bootstrap configuration. Prefer the one with the most compute units. Use
    S3 to "lock" the cluster and ensure that the job is not scheduled behind
    another job. If no suitable cluster is `WAITING`, create a new pooled
    cluster.

.. mrjob-opt::
    :config: pool_name
    :switch: --pool-name
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``'default'``

    Specify a pool name to join. Does not imply :mrjob-opt:`pool_clusters`.

.. mrjob-opt::
    :config: pool_wait_minutes
    :switch: --pool-wait-minutes
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: 0

    If pooling is enabled and no cluster is available, retry finding a cluster
    every 30 seconds until this many minutes have passed, then start a new
    cluster instead of joining one.

S3 Filesystem
-------------

See also :mrjob-opt:`cloud_tmp_dir`, :mrjob-opt:`cloud_part_size_mb`

.. mrjob-opt::
    :config: cloud_log_dir
    :switch: --cloud-log-dir
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: append ``logs`` to :mrjob-opt:`cloud_tmp_dir`

    Where on S3 to put logs, for example ``s3://yourbucket/logs/``. Logs for
    your cluster will go into a subdirectory, e.g.
    ``s3://yourbucket/logs/j-CLUSTERID/``.

API Endpoints
-------------

.. note ::

   You usually don't want to set ``*_endpoint`` options unless you have a
   challenging network situation (e.g. you have to use a proxy to get around
   a firewall).

.. mrjob-opt::
    :config: ec2_endpoint
    :switch: --ec2-endpoint
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: (automatic)

    .. versionadded:: 0.6.5

    Force mrjob to connect to EC2 on this endpoint (e.g.
    ``ec2.us-gov-west-1.amazonaws.com``).

.. mrjob-opt::
    :config: emr_endpoint
    :switch: --emr-endpoint
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: infer from :mrjob-opt:`region`

    Force mrjob to connect to EMR on this endpoint (e.g.
    ``us-west-1.elasticmapreduce.amazonaws.com``).

.. mrjob-opt::
    :config: iam_endpoint
    :switch: --iam-endpoint
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: (automatic)

    Force mrjob to connect to IAM on this endpoint (e.g.
    ``iam.us-gov.amazonaws.com``).

.. mrjob-opt::
    :config: s3_endpoint
    :switch: --s3-endpoint
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: (automatic)

    Force mrjob to connect to S3 on this endpoint, rather than letting it
    choose the appropriate endpoint for each S3 bucket.

    .. warning:: If you set this to a region-specific endpoint
                 (e.g. ``'s3-us-west-1.amazonaws.com'``) mrjob may not
                 be able to access buckets located in other regions.

Other rarely used options
-------------------------

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

.. mrjob-opt::
    :config: hadoop_streaming_jar_on_emr
    :switch: --hadoop-streaming-jar-on-emr
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: AWS default

.. mrjob-opt::
    :config: mins_to_end_of_hour
    :switch: --mins-to-end-of-hour
    :type: float
    :set: emr
    :default: 5.0

    .. deprecated:: 0.6.0

        This option was created back when EMR billed by the full hour, and
        does nothing as of v0.6.0. If using versions prior to v0.6.0, it's
        recommended you set this to 60.0 to effectively disable this feature.

.. mrjob-opt::
    :config: ssh_bin
    :switch: --ssh-bin
    :type: :ref:`command <data-type-command>`
    :set: emr
    :default: ``'ssh'``

    Path to the ssh binary; may include switches (e.g.  ``'ssh -v'`` or
    ``['ssh', '-v']``). Defaults to :command:`ssh`.

    On EMR, mrjob uses SSH to tunnel to the job tracker (see
    :mrjob-opt:`ssh_tunnel`), as a fallback way of fetching job progress,
    and as a quicker way of accessing your job's logs.

    .. versionchanged:: 0.6.8

       Setting this to an empty value (``--ssh-bin ''``) instructs mrjob to use
       the default value (used to effectively disable SSH).

.. mrjob-opt::
    :config: tags
    :switch: --tag
    :type: :ref:`dict <data-type-plain-dict>`
    :set: emr
    :default: ``{}``

    Metadata tags to apply to the EMR cluster after its
    creation. See `Tagging Amazon EMR Clusters`_ for more information
    on applying metadata tags to EMR clusters.

    .. _`Tagging Amazon EMR Clusters`:
        http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-plan-tags.html

    Tag names and values are strings. On the command line, to set a tag
    use ``--tag KEY=VALUE``:

    .. code-block:: sh

        --tag team=development

    In the config file, ``tags`` is a dict:

    .. code-block:: yaml

        runners:
          emr:
            tags:
              team: development
              project: mrjob

.. mrjob-opt::
    :config: visible_to_all_users
    :switch: --visible-to-all-users, --no-visible-to-all-users
    :type: boolean
    :set: emr
    :default: ``True``

    If true (the default) EMR clusters will be visible to all IAM users.
    Otherwise, the cluster will only be visible to the IAM user that created
    it.

    .. deprecated:: 0.6.0

       Hiding clusters from other users on the same account is not very useful.
       If you don't want to share pooled clusters, try :mrjob-opt:`pool_name`.
