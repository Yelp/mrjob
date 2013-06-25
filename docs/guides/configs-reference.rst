Configuration quick reference
=============================

Setting configuration options
-----------------------------

You can set an option by:

* Passing it on the command line with the switch version (like
  ``--some-option``)
* Passing it as a keyword argument to the runner constructor, if you are
  creating the runner programmatically
* Putting it in one of the included config files under a runner name, like
  this:

  .. code-block:: yaml

    runners:
        local:
            python_bin: python2.6  # only used in local runner
        emr:
            python_bin: python2.5  # only used in Elastic MapReduce runner

  See :ref:`mrjob.conf` for information on where to put config files.

Options that can't be set from mrjob.conf (all runners)
-------------------------------------------------------

For some options, it doesn't make sense to be able to set them in the config
file. These can only be specified when calling the constructor of
:py:class:`~mrjob.runner.MRJobRunner`, as command line options, or sometimes by
overriding some attribute or method of your :py:class:`~mrjob.job.MRJob`
subclass.

See :py:meth:`mrjob.runner.MRJobRunner.__init__` for documentation about
options below that do not link elsewhere.

======================================= ========================================================================== =======================================================
Option                                  Switches                                                                   Default
======================================= ========================================================================== =======================================================
:ref:`conf_paths <opt_conf_paths>`      :option:`-c`, :option:`--conf-path`, :option:`--no-conf`                   (automatic; see :py:func:`~mrjob.conf.find_mrjob_conf`)
*extra_args*                            (see :py:meth:`~mrjob.job.MRJob.add_passthrough_option`)                   ``[]``
*file_upload_args*                      (see :py:meth:`~mrjob.job.MRJob.add_file_option`)                          ``[]``
*hadoop_input_format*                   (see :py:meth:`~mrjob.job.MRJob.hadoop_input_format`)                      ``None``
*hadoop_output_format*                  (see :py:meth:`~mrjob.job.MRJob.hadoop_output_format`)                     ``None``
:ref:`output_dir <opt_output_dir>`      :option:`-o`, :option:`--output-dir`                                       (automatic)
:ref:`no_output <opt_no_output>`        :option:`--no-output`                                                      ``False``
:ref:`partitioner <opt_partitioner>`    :option:`--partitioner` (see also :py:meth:`~mrjob.job.MRJob.partitioner`) ``None``
======================================= ========================================================================== =======================================================

Other options for all runners
-----------------------------

These options can be passed to any runner without an error, though some runners
may ignore some options. See the text after the table for specifics.

.. RST TABLES SUCK SO MUCH

======================================================= ================================================================== ============================== ================
Option                                                  Switches                                                           Default                        Data type
======================================================= ================================================================== ============================== ================
:ref:`base_tmp_dir <opt_base_tmp_dir>`                  (set :envvar:`TMPDIR`)                                             (automatic)                    |dt-path|
:ref:`bootstrap_mrjob <opt_bootstrap_mrjob>`            :option:`--boostrap-mrjob`, :option:`--no-bootstrap-mrjob`         ``True``                       |dt-string|
:ref:`cleanup <opt_cleanup>`                            :option:`--cleanup`                                                ``'ALL'``                      |dt-string|
:ref:`cleanup_on_failure <opt_cleanup_on_failure>`      :option:`--cleanup-on-failure`                                     ``'NONE'``                     |dt-string|
:ref:`cmdenv <opt_cmdenv>`                              :option:`--cmdenv`                                                 ``{}``                         |dt-env-dict|
:ref:`hadoop_extra_args <opt_hadoop_extra_args>`        :option:`--hadoop-arg`                                             ``[]``                         |dt-string-list|
:ref:`hadoop_streaming_jar <opt_hadoop_streaming_jar>`  :option:`--hadoop-streaming-jar`                                   (automatic)                    |dt-string|
:ref:`interpreter <opt_interpreter>`                    :option:`--interpreter`                                            (value of *python_bin*)        |dt-command|
:ref:`jobconf <opt_jobconf>`                            :option:`--jobconf` (see also :py:meth:`~mrjob.job.MRJob.jobconf`) ``{}``                         |dt-plain-dict|
:ref:`label <opt_label>`                                :option:`--label`                                                  (automatic)                    |dt-string|
:ref:`owner <opt_owner>`                                :option:`--owner`                                                  (automatic)                    |dt-string|
:ref:`python_archives <opt_python_archives>`            :option:`--python-archive`                                         ``[]``                         |dt-path-list|
:ref:`python_bin <opt_python_bin>`                      :option:`--python-bin`                                             :command:`python`              |dt-command|
:ref:`setup_cmds <opt_setup_cmds>`                      :option:`--setup-cmd`                                              ``[]``                         |dt-string-list|
:ref:`setup_scripts <opt_setup_scripts>`                :option:`--setup-script`                                           ``[]``                         |dt-path-list|
:ref:`steps_python_bin <opt_steps_python_bin>`          :option:`--steps-python-bin`                                       (current Python interpreter)   |dt-command|
:ref:`upload_archives <opt_upload_archives>`            :option:`--archive`                                                ``[]``                         |dt-path-list|
:ref:`upload_files <opt_upload_files>`                  :option:`--file`                                                   ``[]``                         |dt-path-list|
======================================================= ================================================================== ============================== ================

:py:class:`~mrjob.local.LocalMRJobRunner` takes no additional options, but:

* :ref:`bootstrap_mrjob <opt_bootstrap_mrjob>` is ``False`` by default
* :ref:`cmdenv <opt_cmdenv>` uses the local system path separator instead of ``:`` all the time (so ``;`` on Windows, no change elsewhere)
* :ref:`python_bin <opt_python_bin>` defaults to the current Python interpreter

In addition, it ignores *hadoop_input_format*, *hadoop_output_format*,
*hadoop_streaming_jar*, and *jobconf*

:py:class:`~mrjob.inline.InlineMRJobRunner` works like
:py:class:`~mrjob.local.LocalMRJobRunner`, only it also ignores
*bootstrap_mrjob*, *cmdenv*, *python_bin*, *setup_cmds*, *setup_scripts*,
*steps_python_bin*, *upload_archives*, and *upload_files*.


Additional options for :py:class:`~mrjob.emr.EMRJobRunner`
----------------------------------------------------------

=========================================================================== =================================================================== ============================== =========================================
Option                                                                      Switches                                                            Default                        Data type
=========================================================================== =================================================================== ============================== =========================================
:ref:`additional_emr_info <opt_additional_emr_info>`                        :option:`--additional-emr-info`                                     ``None``                       |dt-string|
:ref:`ami_version <opt_ami_version>`                                        :option:`--ami-version`                                             ``None``                       |dt-string|
:ref:`aws_access_key_id <opt_aws_access_key_id>`                            (set :envvar:`AWS_ACCESS_KEY_ID`)                                   (automatic)                    |dt-string|
:ref:`aws_availability_zone <opt_aws_availability_zone>`                    :option:`--aws-availability-zone`                                   (automatic)                    |dt-string|
:ref:`aws_region <opt_aws_region>`                                          :option:`--aws-region`                                              (automatic)                    |dt-string|
:ref:`aws_secret_access_key <opt_aws_secret_access_key>`                    (set :envvar:`AWS_SECRET_ACCESS_KEY`)                               (automatic)                    |dt-string|
:ref:`bootstrap_actions <opt_bootstrap_actions>`                            :option:`--bootstrap-action`                                        ``[]``                         |dt-string-list|
:ref:`bootstrap_cmds <opt_bootstrap_cmds>`                                  :option:`--bootstrap-cmd`                                           ``[]``                         |dt-string-list|
:ref:`bootstrap_files <opt_bootstrap_files>`                                :option:`--bootstrap-file`                                          ``[]``                         |dt-path-list|
:ref:`bootstrap_python_packages <opt_bootstrap_python_packages>`            :option:`--bootstrap-python-package`                                ``[]``                         |dt-path-list|
:ref:`bootstrap_scripts <opt_bootstrap_scripts>`                            :option:`--bootstrap-script`                                        ``[]``                         |dt-string-list|
:ref:`check_emr_status_every <opt_check_emr_status_every>`                  :option:`--check-emr-status-every`                                  ``30``                         |dt-string|
:ref:`ec2_core_instance_bid_price <opt_ec2_core_instance_bid_price>`        :option:`--ec2-core-instance-bid-price`                             ``None``                       |dt-string|
:ref:`ec2_core_instance_type <opt_ec2_core_instance_type>`                  :option:`--ec2-core-instance-type`                                  ``'m1.small'``                 |dt-string|
:ref:`ec2_instance_type <opt_ec2_instance_type>`                            :option:`--ec2-instance-type`                                       (effectively ``m1.small``)     |dt-string|
:ref:`ec2_key_pair <opt_ec2_key_pair>`                                      :option:`--ec2-key-pair`                                            ``None``                       |dt-string|
:ref:`ec2_key_pair_file <opt_ec2_key_pair_file>`                            :option:`--ec2-key-pair-file`                                       ``None``                       |dt-path|
:ref:`ec2_master_instance_bid_price <opt_ec2_master_instance_bid_price>`    :option:`--ec2-master-instance-bid-price`                           ``None``                       |dt-string|
:ref:`ec2_master_instance_type <opt_ec2_master_instance_type>`              :option:`--ec2-master-instance-type`                                ``'m1.small'``                 |dt-string|
:ref:`ec2_slave_instance_type <opt_ec2_slave_instance_type>`                :option:`--ec2-slave-instance-type`                                 (see *ec2_core_instance_type*) |dt-string|
:ref:`ec2_task_instance_bid_price <opt_ec2_task_instance_bid_price>`        :option:`--ec2-task-instance-bid-price`                             ``None``                       |dt-string|
:ref:`ec2_task_instance_type <opt_ec2_task_instance_type>`                  :option:`--ec2-task-instance-type`                                  (effectively ``'m1.small'``)   |dt-string|
:ref:`emr_endpoint <opt_emr_endpoint>`                                      :option:`--emr-endpoint`                                            (automatic)                    |dt-string|
:ref:`emr_job_flow_id <opt_emr_job_flow_id>`                                :option:`--emr-job-flow-id`                                         (create our own job flow)      |dt-string|
:ref:`emr_job_flow_pool_name <opt_emr_job_flow_pool_name>`                  :option:`--pool-name`                                               ``'default'``                  |dt-string|
:ref:`enable_emr_debugging <opt_enable_emr_debugging>`                      :option:`--enable-emr-debugging`, :option:`--disable-emr-debugging` ``False``                      |dt-string|
:ref:`hadoop_streaming_jar_on_emr <opt_hadoop_streaming_jar_on_emr>`        :option:`--hadoop-streaming-jar-on-emr`                             ``None``                       |dt-string|
:ref:`hadoop_version <opt_hadoop_version>`                                  :option:`--hadoop-version`                                          ``'0.20'``                     |dt-string|
:ref:`num_ec2_core_instances <opt_num_ec2_core_instances>`                  :option:`--num-ec2-core-instances`                                  ``None``                       |dt-string|
:ref:`num_ec2_instances <opt_num_ec2_instances>`                            :option:`--num-ec2-instances`                                       ``1``                          |dt-string|
:ref:`num_ec2_task_instances <opt_num_ec2_task_instances>`                  :option:`--num-ec2-task-instances`                                  ``None``                       |dt-string|
:ref:`pool_emr_job_flows <opt_pool_emr_job_flows>`                          :option:`--pool-emr-job-flows`, :option:`--no-pool-emr-job-flows`   ``False``                      |dt-string|
:ref:`pool_wait_minutes <opt_pool_wait_minutes>`                            :option:`--pool-wait-minutes`                                       ``0``                          |dt-string|
:ref:`s3_endpoint <opt_s3_endpoint>`                                        :option:`--s3-endpoint`                                             (automatic)                    |dt-path|
:ref:`s3_log_uri <opt_s3_log_uri>`                                          :option:`--s3-log-uri`                                              (automatic)                    |dt-path|
:ref:`s3_scratch_uri <opt_s3_scratch_uri>`                                  :option:`--s3-scratch-uri`                                          (automatic)                    |dt-string|
:ref:`s3_sync_wait_time <opt_s3_sync_wait_time>`                            :option:`--s3-sync-wait-time`                                       ``5.0``                        |dt-string|
:ref:`ssh_bin <opt_ssh_bin>`                                                :option:`--ssh-bin`                                                 :command:`ssh`                 |dt-command|
:ref:`ssh_bind_ports <opt_ssh_bind_ports>`                                  :option:`--ssh-bind-ports`                                          ``range(40001, 40841)``        |dt-string|
:ref:`ssh_tunnel_is_open <opt_ssh_tunnel_is_open>`                          :option:`--ssh-tunnel-is-open`, :option:`--ssh-tunnel-is-closed`    ``False``                      |dt-string|
:ref:`ssh_tunnel_to_job_tracker <opt_ssh_tunnel_to_job_tracker>`            :option:`--ssh-tunnel-to-job-tracker`                               ``False``                      |dt-string|
=========================================================================== =================================================================== ============================== =========================================

Additional options for :py:class:`~mrjob.hadoop.HadoopJobRunner`
----------------------------------------------------------------

=============================================== ================================ =========================== =====================================
Option                                          Switches                         Default                     Combined by
=============================================== ================================ =========================== =====================================
:ref:`hadoop_bin <opt_hadoop_bin>`              :option:`--hadoop-bin`           (automatic)                 |dt-command|
:ref:`hadoop_home <opt_hadoop_home>`            (set :envvar:`HADOOP_HOME`)      :envvar:`HADOOP_HOME`       |dt-string|
:ref:`hdfs_scratch_dir <opt_hdfs_scratch_dir>`  :option:`--hdfs-scratch-dir`     ``tmp/mrjob`` (in HDFS)     |dt-path|
=============================================== ================================ =========================== =====================================

.. |dt-string|          replace:: :ref:`string <data-type-string>`
.. |dt-command|         replace:: :ref:`command <data-type-command>`
.. |dt-path|            replace:: :ref:`path <data-type-path-list>`
.. |dt-string-list|     replace:: :ref:`string list <data-type-string-list>`
.. |dt-path-list|       replace:: :ref:`path list <data-type-path-list>`
.. |dt-plain-dict|      replace:: :ref:`plain dict <data-type-plain-dict>`
.. |dt-env-dict|        replace:: :ref:`environment variable dict <data-type-env-dict>`
