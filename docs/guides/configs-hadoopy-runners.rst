Hadoop-related options
======================

Since mrjob is geared toward Hadoop, there are a few Hadoop-specific options.
However, due to the difference between the different runners, the Hadoop
platform, and Elastic MapReduce, they are not all available for all runners.


Options specific to the local and inline runners
------------------------------------------------

.. mrjob-opt::
    :config: hadoop_version
    :switch: --hadoop-version
    :type: :ref:`string <data-type-string>`
    :set: emr
    :default: ``None``

    Set the version of Hadoop to simulate (this currently only matters for
    :mrjob-opt:`jobconf`).

    If you don't set this, the ``local`` and
    ``inline`` runners will run in a version-agnostic mode, where anytime
    the runner sets a simulated jobconf variable, it'll use *every* possible
    name for it (e.g. ``user.name`` *and* ``mapreduce.job.user.name``).


Options available to local, hadoop, and emr runners
---------------------------------------------------

These options are both used by Hadoop and simulated by the ``local``
and ``inline`` runners to some degree.

.. mrjob-opt::
    :config: jobconf
    :switch: --jobconf
    :type: :ref:`dict <data-type-plain-dict>`
    :set: all
    :default: ``{}``

    ``-D`` args to pass to hadoop streaming. This should be a map from
    property name to value.  Equivalent to passing ``['-D',
    'KEY1=VALUE1', '-D', 'KEY2=VALUE2', ...]`` to
    :mrjob-opt:`hadoop_extra_args`


Options available to hadoop and emr runners
-------------------------------------------

.. mrjob-opt::
    :config: hadoop_extra_args
    :switch: --hadoop-arg
    :type: :ref:`string list <data-type-string-list>`
    :set: all
    :default: ``[]``

    Extra arguments to pass to hadoop streaming.

.. mrjob-opt::
    :config: hadoop_streaming_jar
    :switch: --hadoop-streaming-jar
    :type: :ref:`string <data-type-string>`
    :set: all
    :default: (automatic)

    Path to a custom hadoop streaming jar.

    On EMR, this can be either a local path or a URI (``s3://...``). If you
    want to use a jar at a path on the master node, use a ``file://`` URI.

    On Hadoop, mrjob tries its best to find your hadoop streaming jar,
    searching these directories (recursively) for a ``.jar`` file with
    ``hadoop`` followed by ``streaming`` in its name:

    * :mrjob-opt:`hadoop_home` (the deprecated runner option)
    * ``$HADOOP_PREFIX``
    * ``$HADOOP_HOME``
    * ``$HADOOP_INSTALL``
    * ``$HADOOP_MAPRED_HOME``
    * the parent of the directory containing the Hadoop binary (see :mrjob-opt:`hadoop_bin`), unless it's one of ``/``, ``/usr`` or ``/usr/local``
    * ``$HADOOP_*_HOME`` (in alphabetical order by environment variable name)
    * ``/home/hadoop/contrib``
    * ``/usr/lib/hadoop-mapreduce``

    (The last two paths allow the Hadoop runner to work out-of-the box
    inside EMR.)

.. mrjob-opt::
   :config: libjars
   :switch: --libjar
   :type: :ref:`string list <data-type-string-list>`
   :set: all
   :default: ``[]``

   List of paths of JARs to be passed to Hadoop with the ``-libjar`` switch.

   ``~`` and environment variables within paths will be resolved based on the
   local environment.

   .. versionadded:: 0.5.3

.. mrjob-opt::
    :config: label
    :switch: --label
    :type: :ref:`string <data-type-string>`
    :set: all
    :default: script's module name, or ``no_script``

    Alternate label for the job

.. mrjob-opt::
    :config: owner
    :switch: --owner
    :type: :ref:`string <data-type-string>`
    :set: all
    :default: :py:func:`getpass.getuser`, or ``no_user`` if that fails

    Who is running this job (if different from the current user)

.. mrjob-opt::
    :config: check_input_paths
    :switch: --check-input-paths, --no-check-input-paths
    :type: boolean
    :set: all
    :default: ``True``

    Option to skip the input path check. With ``--no-check-input-paths``,
    input paths to the runner will be passed straight through, without
    checking if they exist.

    .. versionadded:: 0.4.1

.. mrjob-opt::
    :config: spark_args
    :switch: --spark-arg
    :type: :ref:`string list <data-type-string-list>`
    :set: all
    :default: ``[]``

    Extra arguments to pass to :command:`spark-submit`.

    .. versionadded:: 0.5.7


Options available to hadoop runner only
---------------------------------------

.. mrjob-opt::
    :config: hadoop_bin
    :switch: --hadoop-bin
    :type: :ref:`command <data-type-command>`
    :set: hadoop
    :default: (automatic)

    Name/path of your :command:`hadoop` binary (may include arguments).

    mrjob tries its best to find :command:`hadoop`, checking all of the
    following places for an executable file named ``hadoop``:

    * :mrjob-opt:`hadoop_home`/``bin`` (deprecated)
    * ``$HADOOP_PREFIX/bin``
    * ``$HADOOP_HOME/bin``
    * ``$HADOOP_INSTALL/bin``
    * ``$HADOOP_INSTALL/hadoop/bin``
    * ``$PATH``
    * ``$HADOOP_*_HOME/bin`` (in alphabetical order by environment variable name)

    If all else fails, we just use ``hadoop`` and hope for the best.

.. mrjob-opt::
    :config: hadoop_home
    :switch: --hadoop-home
    :type: :ref:`path <data-type-path>`
    :set: hadoop
    :default: ``None``

    .. deprecated:: 0.5.0

    Hint about where to find the hadoop binary and streaming jar. In most
    cases, mrjob will now find these on its own. If not, set
    :mrjob-opt:`hadoop_bin` and/or :mrjob-opt:`hadoop_streaming_jar` as
    needed.

.. mrjob-opt::
   :config: hadoop_log_dirs
   :switch: --hadoop-log-dir
   :type: :ref:`path list <data-type-path-list>`
   :set: hadoop
   :default: (automatic)

   Where to look for Hadoop logs (to find counters and probable cause of
   job failure). These can be (local) paths or URIs (``hdfs:///...``).

   If this is *not* set, mrjob will try its best to find the logs, searching in:

   * ``$HADOOP_LOG_DIR``
   * ``$YARN_LOG_DIR`` (on YARN only)
   * ``hdfs:///tmp/hadoop-yarn/staging`` (on YARN only)
   * ``<job output dir>/_logs`` (usually this is on HDFS)
   * ``$HADOOP_PREFIX/logs``
   * ``$HADOOP_HOME/logs``
   * ``$HADOOP_INSTALL/logs``
   * ``$HADOOP_MAPRED_HOME/logs``
   * ``<dir containing hadoop bin>/logs`` (see :mrjob-opt:`hadoop_bin`), unless the hadoop binary is in ``/bin``, ``/usr/bin``, or ``/usr/local/bin``
   * ``$HADOOP_*_HOME/logs`` (in alphabetical order by environment variable name)
   * ``/var/log/hadoop-yarn`` (on YARN only)
   * ``/mnt/var/log/hadoop-yarn`` (on YARN only)
   * ``/var/log/hadoop``
   * ``/mnt/var/log/hadoop``

   .. versionadded:: 0.5.0

   .. versionchanged:: 0.5.3

       Added paths in ``/var/log`` and ``/mnt/var/log/hadoop-yarn``

.. mrjob-opt::
    :config: hadoop_tmp_dir
    :switch: --hadoop-tmp-dir
    :type: :ref:`path <data-type-path>`
    :set: hadoop
    :default: :file:`tmp/mrjob`

    Scratch space on HDFS. This path does not need to be fully qualified with
    ``hdfs://`` URIs because it's understood that it has to be on HDFS.

    .. versionchanged:: 0.5.0

       This option used to be named ``hdfs_scratch_dir``.

.. mrjob-opt::
    :config: spark_master
    :switch: --spark-master
    :type: :ref:`string <data-type-string>`
    :set: hadoop
    :default: ``'yarn'``

    Name or URL to pass to the ``--master`` argument of
    :command:`spark-submit` (e.g. ``spark://host:port``, ``yarn``).

    Note that archives (see :mrjob-opt:`upload_archives`) only work
    when this is set to ``yarn``.

.. mrjob-opt::
    :config: spark_submit_bin
    :switch: --spark-submit-bin
    :type: :ref:`command <data-type-command>`
    :set: hadoop
    :default: (automatic)

    Name/path of your :command:`spark-submit` binary (may include arguments).

    mrjob tries its best to find :command:`spark-submit`, checking all of the
    following places for an executable file named ``spark-submit``:

    * ``$SPARK_HOME/bin``
    * ``$PATH``
    * ``/usr/lib/spark/bin``
    * ``/usr/local/spark/bin``
    * ``/usr/local/lib/spark/bin``

    If all else fails, we just use ``spark-submit`` and hope for the best.

    .. versionadded:: 0.5.7
