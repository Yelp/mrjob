.. _cluster-pooling:

Cluster Pooling
===============

Clusters on EMR take several minutes to spin up. Also, EMR bills by the full
hour, so if you run, say, a 10-minute job and then shut down the cluster, the
other 50 minutes are wasted.

To mitigate these problems, :py:mod:`mrjob` provides **cluster pools.** By
default, once your job completes, the cluster will stay open to accept
additional jobs, and eventually shut itself down after it has been idle
for a certain amount of time (see :mrjob-opt:`max_hours_idle` and
:mrjob-opt:`mins_to_end_of_hour`).

.. note::

   Cluster pooling was not turned on by default in versions prior to 0.6.0.
   To get the same behavior in previous versions :mrjob-opt:`pool_clusters` to
   ``True`` and :mrjob-opt:`max_hours_idle` to 0.5 (don't forget to set
   `max_hours_idle`, or your clusters will never shut down).

Pooling is designed so that jobs run against the same :py:mod:`mrjob.conf` can
share the same clusters. This means that the version of :py:mod:`mrjob` and
bootstrap configuration. Other options that affect which cluster a job can
join:

* :mrjob-opt:`image_version`\/:mrjob-opt:`release_label`: must match
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
your job requests. If there is a tie, it picks clusters that are closest to
the end of a full hour, to minimize wasted instance hours.

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
