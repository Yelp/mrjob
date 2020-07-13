.. _cluster-pooling:

Cluster Pooling
===============

Clusters on EMR take several minutes to spin up, which can make development
painfully slow.

To get around this, :py:mod:`mrjob` provides
**cluster pooling.**. If you set :mrjob-opt:`pool_clusters` to true,
once your job completes, the cluster will stay open to accept
additional jobs, and eventually shut itself down after it has been idle
for a certain amount of time (by default, ten minutes; see
:mrjob-opt:`max_mins_idle`).

.. note::

   Pooling is a way to reduce latency, not to save money. Though
   pooling was originally created to optimize AWS's practice of billing by
   the full hour, this `ended in October 2017 <https://aws.amazon.com/about-aws/whats-new/2017/10/amazon-emr-now-supports-per-second-billing/>`_.

Pooling is designed so that jobs run with the same version of mrjob and the
same (or similar) :py:mod:`mrjob.conf` can share the same clusters. Options
that affect which cluster a job can join:

* :mrjob-opt:`additional_emr_info`: (or lack thereof) must match
* :mrjob-opt:`applications`: must match
* :mrjob-opt:`bootstrap`: must match, and files referenced must
  have identical contents
* :mrjob-opt:`bootstrap_actions`: must match
* :mrjob-opt:`image_version`\/:mrjob-opt:`release_label`: must match
* :mrjob-opt:`image_id` (or lack thereof) must match
* :mrjob-opt:`ec2_key_pair`: if specified, only join clusters with the same key
  pair
* :mrjob-opt:`emr_configurations`: (or lack thereof) must match
* :mrjob-opt:`subnet`: only join clusters with the same EC2 subnet ID (or
  lack thereof)

Pooled jobs will also only use clusters with the same **pool name**, so you
can use the :mrjob-opt:`pool_name` option to partition your clusters into
separate pools.

Pooling is flexible about instance type and number of instances. It will
attempt to select the cluster with the greatest CPU capacity
(based on ``NormalizedInstanceHours`` in the cluster summary returned
by the ``ListClusters`` API call), as long as the cluster's instances provide
at least as much memory and at least as much CPU as your job requests.

Pooling is also somewhat flexible about EBS volumes (see
:mrjob-opt:`instance_groups`). Each volume must have the same volume type,
but larger volumes or volumes with more I/O ops per second are acceptable,
as are additional volumes of any type.

Pooling cannot match configurations with explicitly set
:mrjob-opt:`ebs_root_volume_gb` against clusters that use the default (or vice
versa) because the EMR API does not report what the default value is.

If you are using :mrjob-opt:`instance_fleets`, your jobs will only join other
clusters which use instance fleets. The rules are similar, but jobs will
only join clusters whose fleets use the same set of instances or a subset;
there is no concept of "better" instances.

Pooling uses EMR tags to implement a simple "locking" mechanism that keeps
two jobs from joining the same cluster simultaneously. Locks automatically
expire after a minute (which is more than long enough for a new step to be
submitted to the EMR API and enter the ``RUNNING`` state).

You can allow jobs to wait for an available cluster instead of immediately
starting a new one by specifying a value for `--pool-wait-minutes`. mrjob will
try to find a cluster every 30 seconds for :mrjob-opt:`pool_wait_minutes`. If
none is found during that time, mrjob will start a new one.
