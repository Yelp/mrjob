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
    :default: (automatic)

    Should we automatically tar up the mrjob library and install it when we run
    job? By default, we do unless :mrjob-opt:`interpreter` is set.

    Set this to ``False`` if you've already installed ``mrjob`` on your
    Hadoop cluster or install it by some other method.

.. mrjob-opt::
    :config: upload_files
    :switch: --file
    :type: :ref:`path list <data-type-path-list>`
    :set: all
    :default: ``[]``

    Files to copy to the local directory of the mr_job script when it runs. You
    can set the local name of the dir we unpack into by appending
    ``#localname`` to the path; otherwise we just use the name of the file.

    In the config file::

        upload_files:
          - file_1.txt
          - file_2.sqlite

    On the command line::

        --file file_1.txt --file file_2.sqlite

.. mrjob-opt::
    :config: upload_archives
    :switch: --archive
    :type: :ref:`path list <data-type-path-list>`
    :set: all
    :default: ``[]``

    A list of archives (e.g. tarballs) to unpack in the local directory of the
    mr_job script when it runs. You can set the local name of the dir we unpack
    into by appending ``#localname`` to the path; otherwise we just use the
    name of the archive file (e.g. ``foo.tar.gz`` is unpacked to the directory
    ``foo.tar.gz/``, and ``foo.tar.gz#stuff`` is unpacked to the directory
    ``stuff/``).

.. mrjob-opt::
    :config: python_archives
    :switch: --python-archive
    :type: :ref:`path list <data-type-path-list>`
    :set: all
    :default: ``[]``

    Same as upload_archives, except they get added to the job's
    :envvar:`PYTHONPATH`.

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
    * ``'JOB'``: stop job if on EMR and the job is not done when cleanup runs
    * ``'LOCAL_TMP'``: delete local temp files only
    * ``'LOGS'``: delete logs only
    * ``'NONE'``: delete nothing
    * ``'REMOTE_TMP'``: delete remote temp files only
    * ``'TMP'``: delete local and remote temp files, but not logs

    In the config file::

        cleanup: [LOGS, JOB]

    On the command line::

        --cleanup=LOGS,JOB

    .. versionchanged:: 0.5.0

       Options ending in ``TMP`` used to end in ``SCRATCH``

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
    :type: :ref:`path <data-type-path>`
    :set: all
    :default: value of :py:func:`tempfile.gettempdir`

    Alternate local temp directory.

    There isn't a command-line switch for this option; just set
    :envvar:`TMPDIR` or any other environment variable respected by
    :py:func:`tempfile.gettempdir`.

    .. versionchanged:: 0.5.0

       This option used to be named ``base_tmp_dir``.

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
    :config: no_output
    :switch: --no-output
    :type: boolean
    :set: no_mrjob_conf
    :default: ``False``

    Don't stream output to STDOUT after job completion.  This is often used in
    conjunction with ``--output-dir`` to store output only in HDFS or S3.

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
    :config: interpreter
    :switch: --interpreter
    :type: :ref:`string <data-type-string>`
    :set: all
    :default: ``None``

    Non-Python command to launch your script with (e.g. ``'ruby'``).
    This will also be used to query the script about steps unless you set
    :mrjob-opt:`steps_interpreter`.

    If you want to use an alternate Python command to run the job, use
    :mrjob-opt:`python_bin`.

    This takes precedence over :mrjob-opt:`python_bin` and
    :mrjob-opt:`steps_python_bin`.

.. mrjob-opt::
    :config: python_bin
    :switch: --python-bin
    :type: :ref:`command <data-type-command>`
    :set: all
    :default: (automatic)

    Name/path of alternate Python binary for wrapper scripts and
    mappers/reducers (e.g. ``'python -v'``). This defaults to ``'python'`` if
    you're in Python 2 and ``'python3'`` if you're in Python 3.

    This option also affects which Python binary is used for file locking in
    :mrjob-opt:`setup` scripts, so it might be useful to set even if you're
    using a non-Python :mrjob-opt:`interpreter`. It's also used by
    :py:class:`~mrjob.emr.EMRJobRunner` to compile mrjob after bootstrapping it
    (see :mrjob-opt:`bootstrap_mrjob`).

    Unlike :mrjob-opt:`interpreter`, this does not affect the binary used to
    query the job about its steps (use :mrjob-opt:`steps_python_bin`).

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
    putting a ``/`` after *name*. For details of parsing, see
    :py:func:`~mrjob.setup.parse_setup_cmd`.

.. mrjob-opt::
    :config: setup_cmds
    :switch: --setup-cmd
    :type: :ref:`string list <data-type-string-list>`
    :set: all
    :default: ``[]``

    .. deprecated:: 0.4.2

    A list of commands to run before each mapper/reducer step. Basically
    :mrjob-opt:`setup` without automatic file uploading/interpolation.
    Can also take commands as lists of arguments.

.. mrjob-opt::
    :config: setup_scripts
    :switch: --setup-script
    :type: :ref:`path list <data-type-path-list>`
    :set: all
    :default: ``[]``

    .. deprecated:: 0.4.2

    Files that will be copied into the local working directory and then run.

    Pass ``'path/to/script#'`` to :mrjob-opt:`setup` instead.

.. mrjob-opt::
    :config: sh_bin
    :switch: --sh-bin
    :type: :ref:`command <data-type-command>`
    :set: all
    :default: :command:`sh -e` (:command:`/bin/sh -e` on EMR)

    Name/path of alternate shell binary to use for :mrjob-opt:`setup` and
    :mrjob-opt:`bootstrap`. Needs to be backwards compatible with
    Bourne Shell (e.g. ``'bash'``).

    To force setup/bootstrapping to terminate when any command exits with
    an error, use ``'sh -e'``.

.. mrjob-opt::
    :config: steps_interpreter
    :switch: --steps-interpreter
    :type: :ref:`command <data-type-command>`
    :set: all
    :default: current Python interpreter

    Alternate (non-Python) command to use to query the job about
    its steps. Usually it's good enough to set :mrjob-opt:`interpreter`.

    If you want to use an alternate Python command to get the job's steps,
    use :mrjob-opt:`steps_python_bin`.

    This takes precedence over :mrjob-opt:`steps_python_bin`.

.. mrjob-opt::
    :config: steps_python_bin
    :switch: --steps-python-bin
    :type: :ref:`command <data-type-command>`
    :set: all
    :default: (current Python interpreter)

    Name/path of alternate python binary to use to query the job about its
    steps. Rarely needed. If not set, we use ``sys.executable`` (the current
    Python interpreter).

.. mrjob-opt::
    :config: strict_protocols
    :switch: --strict-protocols, --no-strict-protocols
    :type: boolean
    :set: all
    :default: ``True``

    If you set this to false, when jobs encounter unencodable input or output,
    they'll increment a counter rather than simply raising an exception.

    .. warning::

       Non-strict protocols are going away in v0.6.0. There is no limit on
       how much data on-strict protocols can silently swallow (potentially
       *all* of it). If you have a problem caused by character encoding in
       log files, consider using
       :py:class:`~mrjob.protocol.TextValueProtocol` instead.

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


Options ignored by the local and inline runners
===============================================

These options are ignored because they require a real instance of Hadoop:

* :mrjob-opt:`hadoop_extra_args`
* :py:meth:`hadoop_input_format <mrjob.runner.MRJobRunner.__init__>`
* :py:meth:`hadoop_output_format <mrjob.runner.MRJobRunner.__init__>`
* :mrjob-opt:`hadoop_streaming_jar`
* :mrjob-opt:`partitioner`


Options ignored by the inline runner
====================================

These options are ignored because the ``inline`` runner does not invoke the job
as a subprocess:

* :mrjob-opt:`bootstrap_mrjob`
* :mrjob-opt:`python_bin`
* :mrjob-opt:`setup`
* :mrjob-opt:`setup_cmds`
* :mrjob-opt:`setup_scripts`
* :mrjob-opt:`steps_python_bin`
