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

.. warning::

   When using cluster pooling prior to v0.6.0, make sure to set
   :mrjob-opt:`max_hours_idle`, or your cluster will never shut down.

.. note::

   Pooling is a way to reduce latency, not to save money. Though
   pooling was originally created to optimize AWS's practice of billing by
   the full hour, this `ended in October 2017 <https://aws.amazon.com/about-aws/whats-new/2017/10/amazon-emr-now-supports-per-second-billing/>`_.

Pooling is designed so that jobs run against the same :py:mod:`mrjob.conf` can
share the same clusters. This means that the version of :py:mod:`mrjob` and
bootstrap configuration. Other options that affect which cluster a job can
join:

* :mrjob-opt:`image_version`\/:mrjob-opt:`release_label`: must match
* :mrjob-opt:`image_id` (or lack thereof) must match
* :mrjob-opt:`applications`: require *at least* these applications
  (extra ones okay)
* :mrjob-opt:`emr_configurations`: must match
* :mrjob-opt:`ec2_key_pair`: if specified, only join clusters with the same key
  pair
* :mrjob-opt:`subnet`: only join clusters with the same EC2 subnet ID (or
  lack thereof)

Pooled jobs will also only use clusters with the same **pool name**, so you
can use the :mrjob-opt:`pool_name` option to partition your clusters into
separate pools.

Pooling is flexible about instance type and number of instances; it will
attempt to select the most powerful cluster available as long as the
cluster's instances provide at least as much memory and at least as much CPU as
your job requests.

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

mrjob's pooling won't add more than 1000 steps to a cluster, as the
EMR API won't show more than this many steps. (For `very old AMIs <http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/AddingStepstoaJobFlow.html>`__
there is a stricter limit of 256 steps).

:py:mod:`mrjob` also uses an S3-based
"locking" mechanism to prevent two jobs from simultaneously joining the same
cluster. This is somewhat ugly but works in practice, and avoids
:py:mod:`mrjob` depending on Amazon services other than EMR and S3.

.. warning::

    If S3 eventual consistency takes longer than
    :mrjob-opt:`cloud_fs_sync_secs`, then you
    may encounter race conditions when using pooling, e.g. two jobs claiming
    the same cluster at the same time, or the idle cluster killer shutting
    down your job before it has started to run. Regions with read-after-write
    consistency (i.e. every region except US Standard) should not experience
    these issues.

You can allow jobs to wait for an available cluster instead of immediately
starting a new one by specifying a value for `--pool-wait-minutes`. mrjob will
try to find a cluster every 30 seconds for :mrjob-opt:`pool_wait_minutes`. If
none is found during that time, mrjob will start a new one.
