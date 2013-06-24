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

.. RST TABLES SUCK SO MUCH

======================================================= ================================================================== ============================== =========================================
Option                                                  Switches                                                           Default                        Combined by                              
======================================================= ================================================================== ============================== =========================================
:ref:`base_tmp_dir <opt_base_tmp_dir>`                  (set :envvar:`TMPDIR`)                                             (automatic)                    :py:func:`~mrjob.conf.combine_paths`     
:ref:`bootstrap_mrjob <opt_bootstrap_mrjob>`            :option:`--boostrap-mrjob`, :option:`--no-bootstrap-mrjob`         ``True``                       :py:func:`~mrjob.conf.combine_values`    
:ref:`cleanup <opt_cleanup>`                            :option:`--cleanup`                                                ``'ALL'``                      :py:func:`~mrjob.conf.combine_values`    
:ref:`cleanup_on_failure <opt_cleanup_on_failure>`      :option:`--cleanup-on-failure`                                     ``'NONE'``                     :py:func:`~mrjob.conf.combine_values`    
:ref:`cmdenv <opt_cmdenv>`                              :option:`--cmdenv`                                                 ``{}``                         :py:func:`~mrjob.conf.combine_envs`      
:ref:`hadoop_extra_args <opt_hadoop_extra_args>`        :option:`--hadoop-arg`                                             ``[]``                         :py:func:`~mrjob.conf.combine_lists`     
:ref:`hadoop_streaming_jar <opt_hadoop_streaming_jar>`  :option:`--hadoop-streaming-jar`                                   (automatic)                    :py:func:`~mrjob.conf.combine_values`    
:ref:`interpreter <opt_interpreter>`                    :option:`--interpreter`                                            (value of *python_bin*)        :py:func:`~mrjob.conf.combine_cmds`      
:ref:`jobconf <opt_jobconf>`                            :option:`--jobconf` (see also :py:meth:`~mrjob.job.MRJob.jobconf`) ``{}``                         :py:func:`~mrjob.conf.combine_dicts`     
:ref:`label <opt_label>`                                :option:`--label`                                                  (automatic)                    :py:func:`~mrjob.conf.combine_values`    
:ref:`owner <opt_owner>`                                :option:`--owner`                                                  (automatic)                    :py:func:`~mrjob.conf.combine_values`    
:ref:`python_archives <opt_python_archives>`            :option:`--python-archive`                                         ``[]``                         :py:func:`~mrjob.conf.combine_path_lists`
:ref:`python_bin <opt_python_bin>`                      :option:`--python-bin`                                             :command:`python`              :py:func:`~mrjob.conf.combine_cmds`      
:ref:`setup_cmds <opt_setup_cmds>`                      :option:`--setup-cmd`                                              ``[]``                         :py:func:`~mrjob.conf.combine_lists`     
:ref:`setup_scripts <opt_setup_scripts>`                :option:`--setup-script`                                           ``[]``                         :py:func:`~mrjob.conf.combine_path_lists`
:ref:`steps_python_bin <opt_steps_python_bin>`          :option:`--steps-python-bin`                                       (current Python interpreter)   :py:func:`~mrjob.conf.combine_cmds`      
:ref:`upload_archives <opt_upload_archives>`            :option:`--archive`                                                ``[]``                         :py:func:`~mrjob.conf.combine_path_lists`
:ref:`upload_files <opt_upload_files>`                  :option:`--file`                                                   ``[]``                         :py:func:`~mrjob.conf.combine_path_lists`
======================================================= ================================================================== ============================== =========================================

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

=========================================================================== =================================================================== ============================== =========================================
Option                                                                      Switches                                                            Default                        Combined by                              
=========================================================================== =================================================================== ============================== =========================================
:ref:`additional_emr_info <opt_additional_emr_info>`                        :option:`--additional-emr-info`                                     ``None``                       :py:func:`~mrjob.conf.combine_values`    
:ref:`ami_version <opt_ami_version>`                                        :option:`--ami-version`                                             ``None``                       :py:func:`~mrjob.conf.combine_values`    
:ref:`aws_access_key_id <opt_aws_access_key_id>`                            (set :envvar:`AWS_ACCESS_KEY_ID`)                                   (automatic)                    :py:func:`~mrjob.conf.combine_values`    
:ref:`aws_availability_zone <opt_aws_availability_zone>`                    :option:`--aws-availability-zone`                                   (automatic)                    :py:func:`~mrjob.conf.combine_values`    
:ref:`aws_region <opt_aws_region>`                                          :option:`--aws-region`                                              (automatic)                    :py:func:`~mrjob.conf.combine_values`    
:ref:`aws_secret_access_key <opt_aws_secret_access_key>`                    (set :envvar:`AWS_SECRET_ACCESS_KEY`)                               (automatic)                    :py:func:`~mrjob.conf.combine_values`    
:ref:`bootstrap_actions <opt_bootstrap_actions>`                            :option:`--bootstrap-action`                                        ``[]``                         :py:func:`~mrjob.conf.combine_lists`     
:ref:`bootstrap_cmds <opt_bootstrap_cmds>`                                  :option:`--bootstrap-cmd`                                           ``[]``                         :py:func:`~mrjob.conf.combine_lists`     
:ref:`bootstrap_files <opt_bootstrap_files>`                                :option:`--bootstrap-file`                                          ``[]``                         :py:func:`~mrjob.conf.combine_path_lists`
:ref:`bootstrap_python_packages <opt_bootstrap_python_packages>`            :option:`--bootstrap-python-package`                                ``[]``                         :py:func:`~mrjob.conf.combine_path_lists`
:ref:`bootstrap_scripts <opt_bootstrap_scripts>`                            :option:`--bootstrap-script`                                        ``[]``                         :py:func:`~mrjob.conf.combine_lists`     
:ref:`check_emr_status_every <opt_check_emr_status_every>`                  :option:`--check-emr-status-every`                                  ``30``                         :py:func:`~mrjob.conf.combine_values`    
:ref:`ec2_core_instance_bid_price <opt_ec2_core_instance_bid_price>`        :option:`--ec2-core-instance-bid-price`                             ``None``                       :py:func:`~mrjob.conf.combine_values`    
:ref:`ec2_core_instance_type <opt_ec2_core_instance_type>`                  :option:`--ec2-core-instance-type`                                  ``'m1.small'``                 :py:func:`~mrjob.conf.combine_values`    
:ref:`ec2_instance_type <opt_ec2_instance_type>`                            :option:`--ec2-instance-type`                                       (effectively ``m1.small``)     :py:func:`~mrjob.conf.combine_values`    
:ref:`ec2_key_pair <opt_ec2_key_pair>`                                      :option:`--ec2-key-pair`                                            ``None``                       :py:func:`~mrjob.conf.combine_values`    
:ref:`ec2_key_pair_file <opt_ec2_key_pair_file>`                            :option:`--ec2-key-pair-file`                                       ``None``                       :py:func:`~mrjob.conf.combine_paths`     
:ref:`ec2_master_instance_bid_price <opt_ec2_master_instance_bid_price>`    :option:`--ec2-master-instance-bid-price`                           ``None``                       :py:func:`~mrjob.conf.combine_values`    
:ref:`ec2_master_instance_type <opt_ec2_master_instance_type>`              :option:`--ec2-master-instance-type`                                ``'m1.small'``                 :py:func:`~mrjob.conf.combine_values`    
:ref:`ec2_slave_instance_type <opt_ec2_slave_instance_type>`                :option:`--ec2-slave-instance-type`                                 (see *ec2_core_instance_type*) :py:func:`~mrjob.conf.combine_values`    
:ref:`ec2_task_instance_bid_price <opt_ec2_task_instance_bid_price>`        :option:`--ec2-task-instance-bid-price`                             ``None``                       :py:func:`~mrjob.conf.combine_values`    
:ref:`ec2_task_instance_type <opt_ec2_task_instance_type>`                  :option:`--ec2-task-instance-type`                                  (effectively ``'m1.small'``)   :py:func:`~mrjob.conf.combine_values`    
:ref:`emr_endpoint <opt_emr_endpoint>`                                      :option:`--emr-endpoint`                                            (automatic)                    :py:func:`~mrjob.conf.combine_values`    
:ref:`emr_job_flow_id <opt_emr_job_flow_id>`                                :option:`--emr-job-flow-id`                                         (create our own job flow)      :py:func:`~mrjob.conf.combine_values`    
:ref:`emr_job_flow_pool_name <opt_emr_job_flow_pool_name>`                  :option:`--pool-name`                                               ``'default'``                  :py:func:`~mrjob.conf.combine_values`    
:ref:`enable_emr_debugging <opt_enable_emr_debugging>`                      :option:`--enable-emr-debugging`, :option:`--disable-emr-debugging` ``False``                      :py:func:`~mrjob.conf.combine_values`    
:ref:`hadoop_streaming_jar_on_emr <opt_hadoop_streaming_jar_on_emr>`        :option:`--hadoop-streaming-jar-on-emr`                             ``None``                       :py:func:`~mrjob.conf.combine_values`    
:ref:`hadoop_version <opt_hadoop_version>`                                  :option:`--hadoop-version`                                          ``'0.20'``                     :py:func:`~mrjob.conf.combine_values`    
:ref:`num_ec2_core_instances <opt_num_ec2_core_instances>`                  :option:`--num-ec2-core-instances`                                  ``None``                       :py:func:`~mrjob.conf.combine_values`    
:ref:`num_ec2_instances <opt_num_ec2_instances>`                            :option:`--num-ec2-instances`                                       ``1``                          :py:func:`~mrjob.conf.combine_values`    
:ref:`num_ec2_task_instances <opt_num_ec2_task_instances>`                  :option:`--num-ec2-task-instances`                                  ``None``                       :py:func:`~mrjob.conf.combine_values`    
:ref:`pool_emr_job_flows <opt_pool_emr_job_flows>`                          :option:`--pool-emr-job-flows`, :option:`--no-pool-emr-job-flows`   ``False``                      :py:func:`~mrjob.conf.combine_values`    
:ref:`pool_wait_minutes <opt_pool_wait_minutes>`                            :option:`--pool-wait-minutes`                                       ``0``                          :py:func:`~mrjob.conf.combine_values`    
:ref:`s3_endpoint <opt_s3_endpoint>`                                        :option:`--s3-endpoint`                                             (automatic)                    :py:func:`~mrjob.conf.combine_paths`     
:ref:`s3_log_uri <opt_s3_log_uri>`                                          :option:`--s3-log-uri`                                              (automatic)                    :py:func:`~mrjob.conf.combine_paths`     
:ref:`s3_scratch_uri <opt_s3_scratch_uri>`                                  :option:`--s3-scratch-uri`                                          (automatic)                    :py:func:`~mrjob.conf.combine_values`    
:ref:`s3_sync_wait_time <opt_s3_sync_wait_time>`                            :option:`--s3-sync-wait-time`                                       ``5.0``                        :py:func:`~mrjob.conf.combine_values`    
:ref:`ssh_bin <opt_ssh_bin>`                                                :option:`--ssh-bin`                                                 :command:`ssh`                 :py:func:`~mrjob.conf.combine_cmds`      
:ref:`ssh_bind_ports <opt_ssh_bind_ports>`                                  :option:`--ssh-bind-ports`                                          ``range(40001, 40841)``        :py:func:`~mrjob.conf.combine_values`    
:ref:`ssh_tunnel_is_open <opt_ssh_tunnel_is_open>`                          :option:`--ssh-tunnel-is-open`, :option:`--ssh-tunnel-is-closed`    ``False``                      :py:func:`~mrjob.conf.combine_values`    
:ref:`ssh_tunnel_to_job_tracker <opt_ssh_tunnel_to_job_tracker>`            :option:`--ssh-tunnel-to-job-tracker`                               ``False``                      :py:func:`~mrjob.conf.combine_values`    
=========================================================================== =================================================================== ============================== =========================================

Additional options for :py:class:`~mrjob.hadoop.HadoopJobRunner`
----------------------------------------------------------------

=============================================== ================================ =========================== =====================================
Option                                          Switches                         Default                     Combined by                          
=============================================== ================================ =========================== =====================================
:ref:`hadoop_bin <opt_hadoop_bin>`              :option:`--hadoop-bin`           (automatic)                 :py:func:`~mrjob.conf.combine_cmds`  
:ref:`hadoop_home <opt_hadoop_home>`            (set :envvar:`HADOOP_HOME`)      :envvar:`HADOOP_HOME`       :py:func:`~mrjob.conf.combine_values`
:ref:`hdfs_scratch_dir <opt_hdfs_scratch_dir>`  :option:`--hdfs-scratch-dir`     ``tmp/mrjob`` (in HDFS)     :py:func:`~mrjob.conf.combine_paths` 
=============================================== ================================ =========================== =====================================

Option data types
-----------------

The same option may be specified multiple times and be one of several data
types. For example, the AWS region may be specified in ``mrjob.conf``, in the
arguments to ``EMRJobRunner``, and on the command line. These are the rules
used to determine what value to use at runtime.

Values specified "later" refer to an option being specified at a higher
priority. For example, a value in ``mrjob.conf`` is specified "earlier" than a
value passed on the command line.

Simple data types
^^^^^^^^^^^^^^^^^

When these are specified more than once, the last non-``None`` value is used.

.. _data-type-string:

**String**
    Simple, unchanged string.

.. _data-type-command:

**Command**
    String containing all ASCII characters to be parsed with
    :py:func:`shlex.split`, or list of command + arguments.

.. _data-type-path:

**Path**
    Local path with ``~`` and environment variables (e.g. ``$TMPDIR``)
    resolved.

List data types
^^^^^^^^^^^^^^^

.. _data-type-plain-list:
.. _data-type-command-list:
.. _data-type-path-list:

The values of these options are specified as lists. When specified more than
once, the lists are concatenated together.

See :ref:`String <data-type-string>`, :ref:`Command <data-type-command>`, and
:ref:`Path <data-type-path>` for specifics about the data types contained in
the specific type of list you are concerned with (string list, command list, or
path list).

Dict data types
^^^^^^^^^^^^^^^

The values of these options are specified as dictionaries. When specified more
than once, each has custom behavior described below.

**Plain dict**
    Values specified later override values specified earlier.

**Environment variable dict**
    Values specified later override values specified earlier, **except for
    those with keys ending in ``PATH``**, in which values are concatenated and
    separated by a colon (``:``) rather than overwritten. The later value comes
    first.

    For example, this config:

    .. code-block:: yaml

        runners: {emr: {cmdenv: {PATH: "/usr/bin"}}}

    when run with this command::

        python my_job.py --cmdenv PATH=/usr/local/bin

    will result in the following value of ``cmdenv``:

        ``/usr/local/bin:/usr/bin``

    **The one exception** to this behavior is in the ``local`` runner, which
    uses the local system separator (on Windows ``;``, on everything else still
    ``:``) instead of always using ``:``.
