Configuration quick reference
=============================

Options that can't be set from mrjob.conf (all runners)
-------------------------------------------------------

See :py:meth:`mrjob.runner.MRJobRunner.__init__` for documentation about
options below that do not link elsewhere.

======================================= ======================================================= ==========================================================================
Option                                  Default                                                 Switches
======================================= ======================================================= ==========================================================================
:ref:`conf_paths <opt_conf_paths>`      (automatic; see :py:func:`~mrjob.conf.find_mrjob_conf`) :option:`-c`, :option:`--conf-path`, :option:`--no-conf`
*extra_args*                            ``[]``                                                  (see :py:meth:`~mrjob.job.MRJob.add_passthrough_option`)
*file_upload_args*                      ``[]``                                                  (see :py:meth:`~mrjob.job.MRJob.add_file_option`)
*hadoop_input_format*                   ``None``                                                (see :py:meth:`~mrjob.job.MRJob.hadoop_input_format`)
*hadoop_output_format*                  ``None``                                                (see :py:meth:`~mrjob.job.MRJob.hadoop_output_format`)
:ref:`output_dir <opt_output_dir>`      (automatic)                                             :option:`-o`, :option:`--output-dir`
:ref:`no_output <opt_no_output>`        ``False``                                               :option:`--no-output`
:ref:`partitioner <opt_partitioner>`    ``None``                                                :option:`--partitioner` (see also :py:meth:`~mrjob.job.MRJob.partitioner`)
======================================= ======================================================= ==========================================================================

Other options for all runners
-----------------------------

.. RST TABLES SUCK SO MUCH

======================================================= ============================== ========================================= ==================================================================
Option                                                  Default                        Combined by                               Switches
======================================================= ============================== ========================================= ==================================================================
:ref:`base_tmp_dir <opt_base_tmp_dir>`                  (automatic)                    :py:func:`~mrjob.conf.combine_paths`      (set :envvar:`TMPDIR`)
:ref:`bootstrap_mrjob <opt_bootstrap_mrjob>`            ``True``                       :py:func:`~mrjob.conf.combine_values`     :option:`--boostrap-mrjob`, :option:`--no-bootstrap-mrjob`
:ref:`cleanup <opt_cleanup>`                            ``'ALL'``                      :py:func:`~mrjob.conf.combine_values`     :option:`--cleanup`
:ref:`cleanup_on_failure <opt_cleanup_on_failure>`      ``'NONE'``                     :py:func:`~mrjob.conf.combine_values`     :option:`--cleanup-on-failure`
:ref:`cmdenv <opt_cmdenv>`                              ``{}``                         :py:func:`~mrjob.conf.combine_envs`       :option:`--cmdenv`
:ref:`hadoop_extra_args <opt_hadoop_extra_args>`        ``[]``                         :py:func:`~mrjob.conf.combine_lists`      :option:`--hadoop-arg`
:ref:`hadoop_streaming_jar <opt_hadoop_streaming_jar>`  (automatic)                    :py:func:`~mrjob.conf.combine_values`     :option:`--hadoop-streaming-jar`
:ref:`interpreter <opt_interpreter>`                    (value of *python_bin*)        :py:func:`~mrjob.conf.combine_cmds`       :option:`--interpreter`
:ref:`jobconf <opt_jobconf>`                            ``{}``                         :py:func:`~mrjob.conf.combine_dicts`      :option:`--jobconf` (see also :py:meth:`~mrjob.job.MRJob.jobconf`)
:ref:`label <opt_label>`                                (automatic)                    :py:func:`~mrjob.conf.combine_values`     :option:`--label`
:ref:`owner <opt_owner>`                                (automatic)                    :py:func:`~mrjob.conf.combine_values`     :option:`--owner`
:ref:`python_archives <opt_python_archives>`            ``[]``                         :py:func:`~mrjob.conf.combine_path_lists` :option:`--python-archive`
:ref:`python_bin <opt_python_bin>`                      :command:`python`              :py:func:`~mrjob.conf.combine_cmds`       :option:`--python-bin`
:ref:`setup_cmds <opt_setup_cmds>`                      ``[]``                         :py:func:`~mrjob.conf.combine_lists`      :option:`--setup-cmd`
:ref:`setup_scripts <opt_setup_scripts>`                ``[]``                         :py:func:`~mrjob.conf.combine_path_lists` :option:`--setup-script`
:ref:`steps_python_bin <opt_steps_python_bin>`          (current Python interpreter)   :py:func:`~mrjob.conf.combine_cmds`       :option:`--steps-python-bin`
:ref:`upload_archives <opt_upload_archives>`            ``[]``                         :py:func:`~mrjob.conf.combine_path_lists` :option:`--archive`
:ref:`upload_files <opt_upload_files>`                  ``[]``                         :py:func:`~mrjob.conf.combine_path_lists` :option:`--file`
======================================================= ============================== ========================================= ==================================================================

:py:class:`~mrjob.local.LocalMRJobRunner` takes no additional options, but:

* :ref:`bootstrap_mrjob <opt_bootstrap_mrjob>` is ``False`` by default
* :ref:`cmdenv <opt_cmdenv>` is combined with :py:func:`~mrjob.conf.combine_local_envs`
* :ref:`python_bin <opt_python_bin>` defaults to the current Python interpreter

In addition, it ignores *hadoop_input_format*, *hadoop_output_format*,
*hadoop_streaming_jar*, and *jobconf*

:py:class:`~mrjob.inline.InlineMRJobRunner` works like
:py:class:`~mrjob.local.LocalMRJobRunner`, only it also ignores
*bootstrap_mrjob*, *cmdenv*, *python_bin*, *setup_cmds*, *setup_scripts*,
*steps_python_bin*, *upload_archives*, and *upload_files*.


Additional options for :py:class:`~mrjob.emr.EMRJobRunner`
----------------------------------------------------------

=========================================================================== ============================== ========================================= ===================================================================
Option                                                                      Default                        Combined by                               Switches
=========================================================================== ============================== ========================================= ===================================================================
:ref:`additional_emr_info <opt_additional_emr_info>`                        ``None``                       :py:func:`~mrjob.conf.combine_values`     :option:`--additional-emr-info`
:ref:`ami_version <opt_ami_version>`                                        ``None``                       :py:func:`~mrjob.conf.combine_values`     :option:`--ami-version`
:ref:`aws_access_key_id <opt_aws_access_key_id>`                            (automatic)                    :py:func:`~mrjob.conf.combine_values`     (set :envvar:`AWS_ACCESS_KEY_ID`)
:ref:`aws_availability_zone <opt_aws_availability_zone>`                    (automatic)                    :py:func:`~mrjob.conf.combine_values`     :option:`--aws-availability-zone`
:ref:`aws_region <opt_aws_region>`                                          (automatic)                    :py:func:`~mrjob.conf.combine_values`     :option:`--aws-region`
:ref:`aws_secret_access_key <opt_aws_secret_access_key>`                    (automatic)                    :py:func:`~mrjob.conf.combine_values`     (set :envvar:`AWS_SECRET_ACCESS_KEY`)
:ref:`bootstrap_actions <opt_bootstrap_actions>`                            ``[]``                         :py:func:`~mrjob.conf.combine_lists`      :option:`--bootstrap-action`
:ref:`bootstrap_cmds <opt_bootstrap_cmds>`                                  ``[]``                         :py:func:`~mrjob.conf.combine_lists`      :option:`--bootstrap-cmd`
:ref:`bootstrap_files <opt_bootstrap_files>`                                ``[]``                         :py:func:`~mrjob.conf.combine_path_lists` :option:`--bootstrap-file`
:ref:`bootstrap_python_packages <opt_bootstrap_python_packages>`            ``[]``                         :py:func:`~mrjob.conf.combine_path_lists` :option:`--bootstrap-python-package`
:ref:`bootstrap_scripts <opt_bootstrap_scripts>`                            ``[]``                         :py:func:`~mrjob.conf.combine_lists`      :option:`--bootstrap-script`
:ref:`check_emr_status_every <opt_check_emr_status_every>`                  ``30``                         :py:func:`~mrjob.conf.combine_values`     :option:`--check-emr-status-every`
:ref:`ec2_core_instance_bid_price <opt_ec2_core_instance_bid_price>`        ``None``                       :py:func:`~mrjob.conf.combine_values`     :option:`--ec2-core-instance-bid-price`
:ref:`ec2_core_instance_type <opt_ec2_core_instance_type>`                  ``'m1.small'``                 :py:func:`~mrjob.conf.combine_values`     :option:`--ec2-core-instance-type`
:ref:`ec2_instance_type <opt_ec2_instance_type>`                            (effectively ``m1.small``)     :py:func:`~mrjob.conf.combine_values`     :option:`--ec2-instance-type`
:ref:`ec2_key_pair <opt_ec2_key_pair>`                                      ``None``                       :py:func:`~mrjob.conf.combine_values`     :option:`--ec2-key-pair`
:ref:`ec2_key_pair_file <opt_ec2_key_pair_file>`                            ``None``                       :py:func:`~mrjob.conf.combine_paths`      :option:`--ec2-key-pair-file`
:ref:`ec2_master_instance_bid_price <opt_ec2_master_instance_bid_price>`    ``None``                       :py:func:`~mrjob.conf.combine_values`     :option:`--ec2-master-instance-bid-price`
:ref:`ec2_master_instance_type <opt_ec2_master_instance_type>`              ``'m1.small'``                 :py:func:`~mrjob.conf.combine_values`     :option:`--ec2-master-instance-type`
:ref:`ec2_slave_instance_type <opt_ec2_slave_instance_type>`                (see *ec2_core_instance_type*) :py:func:`~mrjob.conf.combine_values`     :option:`--ec2-slave-instance-type`
:ref:`ec2_task_instance_bid_price <opt_ec2_task_instance_bid_price>`        ``None``                       :py:func:`~mrjob.conf.combine_values`     :option:`--ec2-task-instance-bid-price`
:ref:`ec2_task_instance_type <opt_ec2_task_instance_type>`                  (effectively ``'m1.small'``)   :py:func:`~mrjob.conf.combine_values`     :option:`--ec2-task-instance-type`
:ref:`emr_endpoint <opt_emr_endpoint>`                                      (automatic)                    :py:func:`~mrjob.conf.combine_values`     :option:`--emr-endpoint`
:ref:`emr_job_flow_id <opt_emr_job_flow_id>`                                (create our own job flow)      :py:func:`~mrjob.conf.combine_values`     :option:`--emr-job-flow-id`
:ref:`emr_job_flow_pool_name <opt_emr_job_flow_pool_name>`                  ``'default'``                  :py:func:`~mrjob.conf.combine_values`     :option:`--pool-name`
:ref:`enable_emr_debugging <opt_enable_emr_debugging>`                      ``False``                      :py:func:`~mrjob.conf.combine_values`     :option:`--enable-emr-debugging`, :option:`--disable-emr-debugging`
:ref:`hadoop_streaming_jar_on_emr <opt_hadoop_streaming_jar_on_emr>`        ``None``                       :py:func:`~mrjob.conf.combine_values`     :option:`--hadoop-streaming-jar-on-emr`
:ref:`hadoop_version <opt_hadoop_version>`                                  ``'0.20'``                     :py:func:`~mrjob.conf.combine_values`     :option:`--hadoop-version`
:ref:`num_ec2_core_instances <opt_num_ec2_core_instances>`                  ``None``                       :py:func:`~mrjob.conf.combine_values`     :option:`--num-ec2-core-instances`
:ref:`num_ec2_instances <opt_num_ec2_instances>`                            ``1``                          :py:func:`~mrjob.conf.combine_values`     :option:`--num-ec2-instances`
:ref:`num_ec2_task_instances <opt_num_ec2_task_instances>`                  ``None``                       :py:func:`~mrjob.conf.combine_values`     :option:`--num-ec2-task-instances`
:ref:`pool_emr_job_flows <opt_pool_emr_job_flows>`                          ``False``                      :py:func:`~mrjob.conf.combine_values`     :option:`--pool-emr-job-flows`, :option:`--no-pool-emr-job-flows`
:ref:`pool_wait_minutes <opt_pool_wait_minutes>`                            ``0``                          :py:func:`~mrjob.conf.combine_values`     :option:`--pool-wait-minutes`
:ref:`s3_endpoint <opt_s3_endpoint>`                                        (automatic)                    :py:func:`~mrjob.conf.combine_paths`      :option:`--s3-endpoint`
:ref:`s3_log_uri <opt_s3_log_uri>`                                          (automatic)                    :py:func:`~mrjob.conf.combine_paths`      :option:`--s3-log-uri`
:ref:`s3_scratch_uri <opt_s3_scratch_uri>`                                  (automatic)                    :py:func:`~mrjob.conf.combine_values`     :option:`--s3-scratch-uri`
:ref:`s3_sync_wait_time <opt_s3_sync_wait_time>`                            ``5.0``                        :py:func:`~mrjob.conf.combine_values`     :option:`--s3-sync-wait-time`
:ref:`ssh_bin <opt_ssh_bin>`                                                :command:`ssh`                 :py:func:`~mrjob.conf.combine_cmds`       :option:`--ssh-bin`
:ref:`ssh_bind_ports <opt_ssh_bind_ports>`                                  ``range(40001, 40841)``        :py:func:`~mrjob.conf.combine_values`     :option:`--ssh-bind-ports`
:ref:`ssh_tunnel_is_open <opt_ssh_tunnel_is_open>`                          ``False``                      :py:func:`~mrjob.conf.combine_values`     :option:`--ssh-tunnel-is-open`, :option:`--ssh-tunnel-is-closed`
:ref:`ssh_tunnel_to_job_tracker <opt_ssh_tunnel_to_job_tracker>`            ``False``                      :py:func:`~mrjob.conf.combine_values`     :option:`--ssh-tunnel-to-job-tracker`
=========================================================================== ============================== ========================================= ===================================================================

Additional options for :py:class:`~mrjob.hadoop.HadoopJobRunner`
----------------------------------------------------------------

=============================================== =========================== ===================================== ================================
Option                                          Default                     Combined by                           Switches
=============================================== =========================== ===================================== ================================
:ref:`hadoop_bin <opt_hadoop_bin>`              (automatic)                 :py:func:`~mrjob.conf.combine_cmds`   :option:`--hadoop-bin`
:ref:`hadoop_home <opt_hadoop_home>`            :envvar:`HADOOP_HOME`       :py:func:`~mrjob.conf.combine_values` (set :envvar:`HADOOP_HOME`)
:ref:`hdfs_scratch_dir <opt_hdfs_scratch_dir>`  ``tmp/mrjob`` (in HDFS)     :py:func:`~mrjob.conf.combine_paths`  :option:`--hdfs-scratch-dir`
=============================================== =========================== ===================================== ================================
