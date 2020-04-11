================================
Options available to all runners
================================

The format of each item in this document is:

.. mrjob-opt::
    :config: mrjob_conf_option_name
    :switch: --command-line-option-name
    :type: option_type
    :set: none
    :default: default value

    Description of option behavior

Options that take multiple values can be passed multiple times on the command
line. All options can be passed as keyword arguments to the runner if
initialized programmatically.

.. _configs-making-files-available:

Making files available to tasks
===============================

Most jobs have dependencies of some sort - Python packages, Debian packages,
data files, etc. This section covers options available to all runners that
mrjob uses to upload files to your job's execution environments. See
:ref:`writing-file-options` if you want to write your own command line
options related to file uploading.

.. warning::

    You **must** wait to read files until **after class initialization**. That
    means you should use the :ref:`*_init() <single-step-method-names>` methods
    to read files. Trying to read files into class variables will not work.

.. mrjob-opt::
    :config: bootstrap_mrjob
    :switch: --bootstrap-mrjob, --no-bootstrap-mrjob
    :type: boolean
    :set: all
    :default: ``True``

    Should we automatically zip up the mrjob library and install it when we run
    job?

    Set this to ``False`` if you've already installed ``mrjob`` on your
    Hadoop cluster or install it by some other method.

.. mrjob-opt::
   :config: py_files
   :switch: --py-files
   :type: :ref:`path list <data-type-path-list>`
   :set: all
   :default: ``[]``

   List of ``.egg`` or ``.zip`` files to add to your job's ``PYTHONPATH``.

   This is based on a Spark feature, but it works just as well with streaming
   jobs.

   .. versionchanged:: 0.6.7

      Deprecated :option:`--py-file` in favor of :option:`--py-files`

.. mrjob-opt::
    :config: upload_archives
    :switch: --archives
    :type: :ref:`path list <data-type-path-list>`
    :set: all
    :default: ``[]``

    A list of archives (e.g. tarballs) to unpack in the local directory of the
    mr_job script when it runs. You can set the name in the job's working
    directory we unpack into by appending ``#nameinworkingdir`` to the path;
    otherwise we just use the
    name of the archive file (e.g. ``foo.tar.gz`` is unpacked to the directory
    ``foo.tar.gz/``, and ``foo.tar.gz#stuff`` is unpacked to the directory
    ``stuff/``).

    .. versionchanged:: 0.6.7

       Deprecated :option:`--archive` in favor of :option:`--archives`

.. mrjob-opt::
    :config: upload_dirs
    :switch: --dirs
    :type: :ref:`path list <data-type-path-list>`
    :set: all
    :default: ``[]``

    A list of directories to copy to the local directory of the
    mr_job script when it runs (mrjob does this by tarballing the directory
    and submitting the tarball to Hadoop as an archive).

    You can set the name in the job's working directory of the directory
    we copy by appending
    ``#nameinworkingdir`` to the path; otherwise we just use its name.

    This works with Spark on YARN only.

    .. versionchanged:: 0.6.7

       Deprecated :option:`--dir` in favor of :option:`--dirs`


.. mrjob-opt::
    :config: upload_files
    :switch: --files
    :type: :ref:`path list <data-type-path-list>`
    :set: all
    :default: ``[]``

    Files to copy to the local directory of the mr_job script when it runs. You
    can set the name of the file in the job's working directory by appending
    ``#nameinworkingdir`` to the path; otherwise we just use the name of the
    file.

    In the config file::

        upload_files:
          - file_1.txt
          - file_2.sqlite

    On the command line::

        --files file_1.txt,file_2.sqlite

    .. versionchanged:: 0.6.8

       In Spark, can use ``#nameinworkingdir`` even when not on YARN.

    .. versionchanged:: 0.6.7

       Deprecated :option:`--file` in favor of :option:`--files`


Temp files and cleanup
======================

.. _configs-all-runners-cleanup:

.. mrjob-opt::
   :config: cleanup
   :switch: --cleanup
   :type: :ref:`string <data-type-string>`
   :set: all
   :default: ``'ALL'``

    List of which kinds of directories to delete when a job succeeds. Valid
    choices are:

    * ``'ALL'``: delete logs and local and remote temp files; stop cluster
        if on EMR and the job is not done when cleanup is run.
    * ``'CLUSTER'``: terminate EMR cluster if job not done when cleanup is run
    * ``'JOB'``: stop job if not done when cleanup runs (temporarily disabled)
    * ``'LOCAL_TMP'``: delete local temp files only
    * ``'LOGS'``: delete logs only
    * ``'NONE'``: delete nothing
    * ``'REMOTE_TMP'``: delete remote temp files only
    * ``'TMP'``: delete local and remote temp files, but not logs

    In the config file::

        cleanup: [LOGS, JOB]

    On the command line::

        --cleanup=LOGS,JOB

.. mrjob-opt::
   :config: cleanup_on_failure
   :switch: --cleanup-on-failure
   :type: :ref:`string <data-type-string>`
   :set: all
   :default: ``'NONE'``

    Which kinds of directories to clean up when a job fails. Valid choices are
    the same as **cleanup**.

.. mrjob-opt::
    :config: local_tmp_dir
    :switch: --local-tmp-dir
    :type: :ref:`path <data-type-path>`
    :set: all
    :default: value of :py:func:`tempfile.gettempdir`

    Alternate local temp directory.

    ``--local-tmp-dir ''`` tells mrjob to ignore the config file
    and use the default temp directory
    (:py:func:`tempfile.gettempdir`)

    .. versionchanged:: 0.6.6

       Added `--local-tmp-dir` switch.

.. mrjob-opt::
   :config: output_dir
   :switch: --output-dir
   :type: :ref:`string <data-type-string>`
   :set: no_mrjob_conf
   :default: (automatic)

    An empty/non-existent directory where Hadoop streaming should put the
    final output from the job.  If you don't specify an output directory,
    we'll output into a subdirectory of this job's temporary directory. You
    can control this from the command line with ``--output-dir``. This option
    cannot be set from configuration files. If used with the ``hadoop`` runner,
    this path does not need to be fully qualified with ``hdfs://`` URIs
    because it's understood that it has to be on HDFS.

.. mrjob-opt::
    :config: cat_output
    :switch: --cat-output, --no-cat-output
    :type: boolean
    :set: no_mrjob_conf
    :default: output if :mrjob-opt:`output_dir` is not set

    Should we stream job output to STDOUT after completion?

    .. versionchanged:: 0.6.3

       used to be ``--no-output``.

.. mrjob-opt::
   :config: step_output_dir
   :switch: --step-output-dir
   :type: :ref:`string <data-type-string>`
   :set: no_mrjob_conf
   :default: (automatic)

   For a multi-step job, where to put output from job steps other than
   the last one. Each step's output will go into a numbered subdirectory
   of this one (``0000/``, ``0001/``, etc.)

   This option can be useful for debugging. By default, intermediate output
   goes into HDFS, which is fastest but not easily accessible on EMR or
   Dataproc.

   This option currently does nothing on local and inline runners.

Job execution context
=====================

.. mrjob-opt::
    :config: cmdenv
    :switch: --cmdenv
    :type: :ref:`environment variable dict <data-type-env-dict>`
    :set: all
    :default: ``{}``

    Dictionary of environment variables to pass to the job inside Hadoop
    streaming.

    In the config file::

        cmdenv:
            PYTHONPATH: $HOME/stuff
            TZ: America/Los_Angeles

    On the command line::

        --cmdenv PYTHONPATH=$HOME/stuff,TZ=America/Los_Angeles

.. mrjob-opt::
    :config: python_bin
    :switch: --python-bin
    :type: :ref:`command <data-type-command>`
    :set: all
    :default: (automatic)

    Name/path of alternate Python binary for wrapper scripts and
    mappers/reducers (e.g. ``'python -v'``).

    If you're on Python 3, this always defaults to ``'python3'``.

    If you're on Python 2, this defaults to ``'python2.7'``.

    If you're using PyPy, this defaults to ``'pypy'`` (not ``'pypy2.7'``) or
    ``'pypy3'`` depending on your version.

    This option also affects which Python binary is used for file locking in
    :mrjob-opt:`setup` scripts. It's also
    used by :py:class:`~mrjob.emr.EMRJobRunner` to compile mrjob after
    bootstrapping it (see :mrjob-opt:`bootstrap_mrjob`).

    .. versionchanged:: 0.7.2

       Defaults to ``'python2.7'`` (not ``'python'``) on Python 2.

    .. versionchanged:: 0.6.10

       added ``'pypy'`` and ``'pypy3'`` as possible defaults

    .. note::

       mrjob does not auto-install PyPy for you on EMR; see
       :ref:`installing-pypy-on-emr` for how to do this

.. mrjob-opt::
    :config: setup
    :switch: --setup
    :type: :ref:`string list <data-type-string-list>`
    :set: all
    :default: ``[]``

    A list of lines of shell script to run before each task (mapper/reducer).

    This option is complex and powerful; the best way to get started is to
    read the :doc:`setup-cookbook`.

    Using this option replaces your task with a shell "wrapper" script that
    executes the setup commands, and then executes the task as the last line
    of the script. This means that environment variables set by hadoop
    (e.g. ``$mapred_job_id``) are available to setup commands, and that you
    can pass environment variables to the task (e.g. ``$PYTHONPATH``) using
    ``export``.

    We use file locking around the setup commands (not the task)
    to ensure that multiple tasks running on the same node won't run them
    simultaneously (it's safe to run ``make``). Before running the task,
    we ``cd`` back to the original working directory.

    In addition, passing expressions like ``path#name`` will cause
    *path* to be automatically uploaded to the task's working directory
    with the filename *name*, marked as executable, and interpolated into the
    script by its absolute path on the machine running the script.

    *path* may also be a URI, and ``~`` and environment variables within *path*
    will be resolved based on the local environment. *name* is optional.

    You can indicate that an archive should be unarchived into a directory by
    putting a ``/`` after *name* (e.g. ``foo.tar.gz#foo/``).

    You can indicate that a directory should be copied into the job's
    working directory by putting a ``/`` after *path* (e.g. ``src-tree/#``).
    You may optionally put a ``/`` after *name* as well
    (e.g. ``cd src-tree/#/subdir``).

    This works for Spark as well (except on the ``local[*]`` master,
    where it doesn't make sense). The setup
    script is run before every executor, but only run before the driver in
    cluster mode.

    .. note::

       Uploading archives and directories (e.g. ``src-tree/#``) to Spark's
       working directory still only works on YARN.

    .. versionchanged:: 0.6.8

       added full support for Spark

    .. versionchanged:: 0.6.7

       added support for Spark on YARN only

    For more details of parsing, see
    :py:func:`~mrjob.setup.parse_setup_cmd`.

.. mrjob-opt::
    :config: sh_bin
    :switch: --sh-bin
    :type: :ref:`command <data-type-command>`
    :set: all
    :default: :command:`/bin/sh -ex`

    Name/path of alternate shell binary to use for :mrjob-opt:`setup` and
    :mrjob-opt:`bootstrap`. Needs to be backwards compatible with
    Bourne Shell (e.g. :command:`bash`, :command:`zsh`).

    If you want to add an argument, use an absolute path
    (:command:`/bin/bash -x`, not :command:`bash -x`). Please do not pass
    multiple args to your shell binary (this plays poorly with Linux
    shebang syntax).

    This is also used to wrap mappers, reducers, etc. that require piping
    one command into another (see e.g.
    :py:meth:`~mrjob.job.MRJob.mapper_pre_filter`).

    .. versionchanged:: 0.6.8

       Setting this to an empty value (``--sh-bin ''``) means to use the
       default (used to cause an error).

    .. versionchanged:: 0.6.7

       Used to be :command:`sh -ex` on local and Hadoop runners

.. mrjob-opt::
    :config: task_python_bin
    :switch: --task-python-bin
    :type: :ref:`command <data-type-command>`
    :set: all
    :default: same as :mrjob-opt:`python_bin`

    Name/path of alternate python binary to run the job (invoking it with
    ``--mapper``, ``--reducer``, ``--spark``, etc.).

    In most cases, you're better off setting :mrjob-opt:`python_bin`, which
    this defaults to. This option exists mostly to support running tasks
    inside Docker while using a normal Python binary in setup wrapper scripts.


Other
=====

.. mrjob-opt::
    :config: conf_paths
    :switch: -c, --conf-path, --no-conf
    :type: :ref:`path list <data-type-path-list>`
    :set: no_mrjob_conf
    :default: see :py:func:`~mrjob.conf.find_mrjob_conf`

    List of paths to configuration files. This option cannot be used in
    configuration files, because that would cause a universe-ending causality
    paradox. Use `--no-conf` on the command line or `conf_paths=[]` to force
    mrjob to load no configuration files at all. If no config path flags are
    given, mrjob will look for one in the locations specified in
    :ref:`mrjob.conf`.

    Config path flags can be used multiple times to combine config files, much
    like the **include** config file directive. Using :option:`--no-conf` will
    cause mrjob to ignore all preceding config path flags.

    For example, this line will cause mrjob to combine settings from
    ``left.conf`` and ``right .conf``::

        python my_job.py -c left.conf -c right.conf

    This line will cause mrjob to read no config file at all::

        python my_job.py --no-conf

    This line will cause mrjob to read only ``right.conf``, because
    ``--no-conf`` nullifies ``-c left.conf``::

        python my_job.py -c left.conf --no-conf -c right.conf

.. mrjob-opt::
   :config: read_logs
   :switch: --read-logs, --no-read-logs
   :type: boolean
   :set: all
   :default: ``True``

   .. versionadded:: 0.6.5

   If set to false, don't list or read the contents of log files generated
   in the course of running your job.

   The main impact of turning off *read_logs* is that if your job fails,
   mrjob won't spend any time or effort determining why it failed. On EMR, this
   effectively disables counter fetching as well.

   This option does not stop the Hadoop and Dataproc runners from reading
   the output of the job driver (i.e. ``hadoop jar ...``), so you will continue
   to get counters and high-level Java errors on these platforms.

Options ignored by the local and inline runners
===============================================

These options are ignored because they require a real instance of Hadoop:

* :py:meth:`hadoop_input_format <mrjob.runner.MRJobRunner.__init__>`
* :py:meth:`hadoop_output_format <mrjob.runner.MRJobRunner.__init__>`
* :mrjob-opt:`libjars`
* :py:meth:`partitioner <mrjob.runner.MRJobRunner.__init__>`


Options ignored by the inline runner
====================================

These options are ignored because the ``inline`` runner does not invoke the job
as a subprocess:

* :mrjob-opt:`bootstrap_mrjob`
* :mrjob-opt:`py_files`
* :mrjob-opt:`python_bin`
* :mrjob-opt:`read_logs`
* :mrjob-opt:`setup`
