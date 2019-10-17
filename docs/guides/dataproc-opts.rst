Dataproc runner options
=======================

All options from :doc:`configs-all-runners`, :doc:`configs-hadoopy-runners`,
and :doc:`cloud-opts` are available when running jobs on Google Cloud Dataproc.

Google credentials
------------------

Basic credentials are not set in the config file; see :ref:`google-setup` for
details.

.. mrjob-opt::
    :config: project_id
    :switch: --project-id
    :type: :ref:`string <data-type-string>`
    :set: dataproc
    :default: read from credentials config file

    The ID of the Google Cloud Project to run under.

    .. versionchanged:: 0.6.2

       This used to be called *gcp_project*

.. mrjob-opt::
   :config: service_account
   :switch: --service-account
   :set: dataproc
   :default: ``None``

   Optional service account to use when creating a cluster. For more
   information see `Service Accounts <https://cloud.google.com/compute/docs/access/service-accounts#custom_service_accounts>`__.

   .. versionadded:: 0.6.3

.. mrjob-opt::
   :config: service_account_scopes
   :switch: --service-account-scopes
   :set: dataproc
   :default: (automatic)

   Optional service account scopes to pass to the API when creating a cluster.

   Generally it's suggested that you instead create a
   :mrjob-opt:`service_account` with the scopes you want.

   .. versionadded:: 0.6.3

Job placement
-------------

See also :mrjob-opt:`subnet`, :mrjob-opt:`region`, :mrjob-opt:`zone`

.. mrjob-opt::
   :config: network
   :switch: --network
   :type: :ref:`string <data-type-string>`
   :set: dataproc
   :default: ``None``

   Name or URI of network to launch cluster in. Incompatible with
   with :mrjob-opt:`subnet`.

   .. versionadded:: 0.6.3

Cluster configuration
---------------------

.. mrjob-opt::
   :config: cluster_properties
   :switch: --cluster-property
   :set: dataproc
   :default: ``None``

   A dictionary of properties to set in the cluster's config files
   (e.g. ``mapred-site.xml``). For details, see
   `Cluster properties <https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/cluster-properties>`__.

.. mrjob-opt::
   :config: core_instance_config
   :switch: --core-instance-config
   :set: dataproc
   :default: ``None``

   A dictionary of additional parameters to pass as ``config.worker_config``
   when creating the cluster. Follows the format of
   `InstanceGroupConfig <https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters#InstanceGroupConfig>`__ except that it uses
   `snake_case` instead of `camel_case`.

   For example, to specify 100GB of disk space on core instances, add this to
   your config file:

   .. code-block:: yaml

       runners:
         dataproc:
           core_instance_config:
             disk_config:
               boot_disk_size_gb: 100

   To set this option on the command line, pass in JSON:

   .. code-block:: sh

       --core-instance-config '{"disk_config": {"boot_disk_size_gb": 100}}'

   This option *can* be used to set number of core instances
   (``num_instances``) or instance type (``machine_type_uri``), but usually
   you'll want to use :mrjob-opt:`num_core_instances` and
   :mrjob-opt:`core_instance_type` along with this option.

   .. versionadded:: 0.6.3

.. mrjob-opt::
   :config: master_instance_config
   :switch: --master-instance-config
   :set: dataproc
   :default: ``None``

   A dictionary of additional parameters to pass as ``config.master_config``
   when creating the cluster. See :mrjob-opt:`core_instance_config` for
   more details.

   .. versionadded:: 0.6.3

.. mrjob-opt::
   :config: task_instance_config
   :switch: --task-instance-config
   :set: dataproc
   :default: ``None``

   A dictionary of additional parameters to pass as
   ``config.secondary_worker_config``
   when creating the cluster. See :mrjob-opt:`task_instance_config` for
   more details.

   To make task instances preemptible, add this to your config file:

   .. code-block:: yaml

       runners:
         dataproc:
           task_instance_config:
             is_preemptible: true

   Note that this config won't be applied unless you specify at least one
   task instance (either through :mrjob-opt:`num_task_instances` or
   by passing ``num_instances`` to this option).

   .. versionadded:: 0.6.3

Other rarely used options
=========================

.. mrjob-opt::
    :config: gcloud_bin
    :switch: --gcloud-bin
    :type: :ref:`command <data-type-command>`
    :set: dataproc
    :default: ``'gcloud'``

    Path to the gcloud binary; may include switches (e.g.  ``'gcloud -v'`` or
    ``['gcloud', '-v']``). Defaults to :command:`gcloud`.

    Used only as a way to create an SSH tunnel to the Resource Manager.

    .. versionchanged:: 0.6.8

       Setting this to an empty value (``--gcloud-bin ''``) instructs mrjob to
       use the default (used to disable SSH).
