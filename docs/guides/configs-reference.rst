Configuration quick reference
=============================

Options that can't be set from mrjob.conf (all runners)
-------------------------------------------------------

====================== ======================================================= ==========================================================================
Option                 Default                                                 Switches
====================== ======================================================= ==========================================================================
*conf_path*            (automatic; see :py:func:`~mrjob.conf.find_mrjob_conf`) :option:`-c`, :option:`--conf-path`, :option:`--no-conf`
*extra_args*           ``[]``                                                  (see :py:meth:`~mrjob.job.MRJob.add_passthrough_option`)
*file_upload_args*     ``[]``                                                  (see :py:meth:`~mrjob.job.MRJob.add_file_option`)
*hadoop_input_format*  ``None``                                                (see :py:meth:`~mrjob.job.MRJob.hadoop_input_format`)
*hadoop_output_format* ``None``                                                (see :py:meth:`~mrjob.job.MRJob.hadoop_output_format`)
*output_dir*           (automatic)                                             :option:`-o`, :option:`--output-dir`
*no_output*            ``False``                                               :option:`--no-output`
*partitioner*          ``None``                                                :option:`--partitioner` (see also :py:meth:`~mrjob.job.MRJob.partitioner`)
====================== ======================================================= ==========================================================================

See :py:meth:`mrjob.runner.MRJobRunner.__init__` for details.

Other options for all runners
-----------------------------

====================== ============================== ========================================= ==================================================================
Option                 Default                        Combined by                               Switches
====================== ============================== ========================================= ==================================================================
*base_tmp_dir*         (automatic)                    :py:func:`~mrjob.conf.combine_paths`      (set :envvar:`TMPDIR`)
*bootstrap_mrjob*      ``True``                       :py:func:`~mrjob.conf.combine_values`     :option:`--boostrap-mrjob`, :option:`--no-bootstrap-mrjob`
*cleanup*              ``'ALL'``                      :py:func:`~mrjob.conf.combine_values`     :option:`--cleanup`
*cleanup_on_failure*   ``'NONE'``                     :py:func:`~mrjob.conf.combine_values`     :option:`--cleanup-on-failure`
*cmdenv*               ``{}``                         :py:func:`~mrjob.conf.combine_envs`       :option:`--cmdenv`
*hadoop_extra_args*    ``[]``                         :py:func:`~mrjob.conf.combine_lists`      :option:`--hadoop-arg`
*hadoop_streaming_jar* (automatic)                    :py:func:`~mrjob.conf.combine_values`     :option:`--hadoop-streaming-jar`
*interpreter*          (value of *python_bin*)        :py:func:`~mrjob.conf.combine_cmds`       :option:`--interpreter`
*jobconf*              ``{}``                         :py:func:`~mrjob.conf.combine_dicts`      :option:`--jobconf` (see also :py:meth:`~mrjob.job.MRJob.jobconf`)
*label*                (automatic)                    :py:func:`~mrjob.conf.combine_values`     :option:`--label`
*owner*                (automatic)                    :py:func:`~mrjob.conf.combine_values`     :option:`--owner`
*python_archives*      ``[]``                         :py:func:`~mrjob.conf.combine_path_lists` :option:`--python-archive`
*python_bin*           :command:`python`              :py:func:`~mrjob.conf.combine_cmds`       :option:`--python-bin`
*setup_cmds*           ``[]``                         :py:func:`~mrjob.conf.combine_lists`      :option:`--setup-cmd`
*setup_scripts*        ``[]``                         :py:func:`~mrjob.conf.combine_path_lists` :option:`--setup-script`
*steps_python_bin*     (current Python interpreter)   :py:func:`~mrjob.conf.combine_cmds`       :option:`--steps-python-bin`
*upload_archives*      ``[]``                         :py:func:`~mrjob.conf.combine_path_lists` :option:`--archive`
*upload_files*         ``[]``                         :py:func:`~mrjob.conf.combine_path_lists` :option:`--file`
====================== ============================== ========================================= ==================================================================

See :py:meth:`mrjob.runner.MRJobRunner.__init__` for details.

:py:class:`~mrjob.local.LocalMRJobRunner` takes no additional options, but:

* *cmdenv* is combined with :py:func:`~mrjob.conf.combine_local_envs`
* *python_bin* defaults to the current Python interpreter

In addition, it ignores *hadoop_input_format*, *hadoop_output_format*, *hadoop_streaming_jar*, and *jobconf*

:py:class:`~mrjob.inline.InlineMRJobRunner` works like :py:class:`~mrjob.local.LocalMRJobRunner`, only it also ignores 
*cmdenv*, *python_bin*, *setup_cmds*, *setup_scripts*, *steps_python_bin*, *upload_archives*, and *upload_files*.


Additional options for :py:class:`~mrjob.emr.EMRJobRunner`
----------------------------------------------------------

=============================== ============================== ========================================= ===================================================================
Option                          Default                        Combined by                               Switches
=============================== ============================== ========================================= ===================================================================
*additional_emr_info*           ``None``                       :py:func:`~mrjob.conf.combine_values`     :option:`--additional-emr-info`
*ami_version*                   ``None``                       :py:func:`~mrjob.conf.combine_values`     :option:`--ami-version`
*aws_access_key_id*             (automatic)                    :py:func:`~mrjob.conf.combine_values`     (set :envvar:`AWS_ACCESS_KEY_ID`)
*aws_availability_zone*         (automatic)                    :py:func:`~mrjob.conf.combine_values`     :option:`--aws-availability-zone`
*aws_region*                    (automatic)                    :py:func:`~mrjob.conf.combine_values`     :option:`--aws-region`
*aws_secret_access_key*         (automatic)                    :py:func:`~mrjob.conf.combine_values`     (set :envvar:`AWS_SECRET_ACCESS_KEY`)
*bootstrap_actions*             ``[]``                         :py:func:`~mrjob.conf.combine_lists`      :option:`--bootstrap-action`
*bootstrap_cmds*                ``[]``                         :py:func:`~mrjob.conf.combine_lists`      :option:`--bootstrap-cmd`
*bootstrap_files*               ``[]``                         :py:func:`~mrjob.conf.combine_path_lists` :option:`--bootstrap-file`
*bootstrap_python_packages*     ``[]``                         :py:func:`~mrjob.conf.combine_path_lists` :option:`--bootstrap-python-package`
*bootstrap_scripts*             ``[]``                         :py:func:`~mrjob.conf.combine_lists`      :option:`--bootstrap-script`
*check_emr_status_every*        ``30``                         :py:func:`~mrjob.conf.combine_values`     :option:`--check-emr-status-every`
*ec2_core_instance_bid_price*   ``None``                       :py:func:`~mrjob.conf.combine_values`     :option:`--ec2-core-instance-bid-price`
*ec2_core_instance_type*        ``'m1.small'``                 :py:func:`~mrjob.conf.combine_values`     :option:`--ec2-core-instance-type`
*ec2_instance_type*             (effectively ``m1.small``)     :py:func:`~mrjob.conf.combine_values`     :option:`--ec2-instance-type`
*ec2_key_pair*                  ``None``                       :py:func:`~mrjob.conf.combine_values`     :option:`--ec2-key-pair`
*ec2_key_pair_file*             ``None``                       :py:func:`~mrjob.conf.combine_paths`      :option:`--ec2-key-pair-file`
*ec2_master_instance_bid_price* ``None``                       :py:func:`~mrjob.conf.combine_values`     :option:`--ec2-master-instance-bid-price`
*ec2_master_instance_type*      ``'m1.small'``                 :py:func:`~mrjob.conf.combine_values`     :option:`--ec2-master-instance-type`
*ec2_slave_instance_type*       (see *ec2_core_instance_type*) :py:func:`~mrjob.conf.combine_values`     :option:`--ec2-slave-instance-type`
*ec2_task_instance_bid_price*   ``None``                       :py:func:`~mrjob.conf.combine_values`     :option:`--ec2-task-instance-bid-price`
*ec2_task_instance_type*        (effectively ``'m1.small'``)   :py:func:`~mrjob.conf.combine_values`     :option:`--ec2-task-instance-type`
*emr_endpoint*                  (automatic)                    :py:func:`~mrjob.conf.combine_values`     :option:`--emr-endpoint`
*emr_job_flow_id*               (create our own job flow)      :py:func:`~mrjob.conf.combine_values`     :option:`--emr-job-flow-id`
*emr_job_flow_pool_name*        ``'default'``                  :py:func:`~mrjob.conf.combine_values`     :option:`--pool-name`
*enable_emr_debugging*          ``False``                      :py:func:`~mrjob.conf.combine_values`     :option:`--enable-emr-debugging`, :option:`--disable-emr-debugging`
*hadoop_streaming_jar_on_emr*   ``None``                       :py:func:`~mrjob.conf.combine_values`     :option:`--hadoop-streaming-jar-on-emr`
*hadoop_version*                ``'0.20'``                     :py:func:`~mrjob.conf.combine_values`     :option:`--hadoop-version`
*num_ec2_core_instances*        ``None``                       :py:func:`~mrjob.conf.combine_values`     :option:`--num-ec2-core-instances`
*num_ec2_instances*             ``1``                          :py:func:`~mrjob.conf.combine_values`     :option:`--num-ec2-instances`
*num_ec2_task_instances*        ``None``                       :py:func:`~mrjob.conf.combine_values`     :option:`--num-ec2-task-instances`
*pool_emr_job_flows*            ``False``                      :py:func:`~mrjob.conf.combine_values`     :option:`--pool-emr-job-flows`, :option:`--no-pool-emr-job-flows`
*pool_wait_minutes*             ``0``                          :py:func:`~mrjob.conf.combine_values`     :option:`--pool-wait-minutes`
*s3_endpoint*                   (automatic)                    :py:func:`~mrjob.conf.combine_paths`      :option:`--s3-endpoint`
*s3_log_uri*                    (automatic)                    :py:func:`~mrjob.conf.combine_paths`      :option:`--s3-log-uri`
*s3_scratch_uri*                (automatic)                    :py:func:`~mrjob.conf.combine_values`     :option:`--s3-scratch-uri`
*s3_sync_wait_time*             ``5.0``                        :py:func:`~mrjob.conf.combine_values`     :option:`--s3-sync-wait-time`
*ssh_bin*                       :command:`ssh`                 :py:func:`~mrjob.conf.combine_cmds`       :option:`--ssh-bin`
*ssh_bind_ports*                ``range(40001, 40841)``        :py:func:`~mrjob.conf.combine_values`     :option:`--ssh-bind-ports`
*ssh_tunnel_is_open*            ``False``                      :py:func:`~mrjob.conf.combine_values`     :option:`--ssh-tunnel-is-open`, :option:`--ssh-tunnel-is-closed`
*ssh_tunnel_to_job_tracker*     ``False``                      :py:func:`~mrjob.conf.combine_values`     :option:`--ssh-tunnel-to-job-tracker`
=============================== ============================== ========================================= ===================================================================

See :py:meth:`mrjob.emr.EMRJobRunner.__init__` for details.

Additional options for :py:class:`~mrjob.hadoop.HadoopJobRunner`
----------------------------------------------------------------

====================== =========================== ===================================== ================================
Option                 Default                     Combined by                           Switches
====================== =========================== ===================================== ================================
*hadoop_bin*           (automatic)                 :py:func:`~mrjob.conf.combine_cmds`   :option:`--hadoop-bin`
*hadoop_home*          :envvar:`HADOOP_HOME`       :py:func:`~mrjob.conf.combine_values` (set :envvar:`HADOOP_HOME`)
*hdfs_scratch_dir*     ``tmp/mrjob`` (in HDFS)     :py:func:`~mrjob.conf.combine_paths`  :option:`--hdfs-scratch-dir`
====================== =========================== ===================================== ================================

See :py:meth:`mrjob.hadoop.HadoopJobRunner.__init__` for details.
