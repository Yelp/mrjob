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
-------------------------------

Most jobs have dependencies of some sort - Python packages, Debian packages,
data files, etc. This section covers options available to all runners that
mrjob uses to upload files to your job's execution environments.

.. mrjob-opt::
    :config: bootstrap_mrjob
    :switch: --bootstrap-mrjob, --no-bootstrap-mrjob
    :type: boolean
    :set: all
    :default: ``True``

    Should we automatically tar up the mrjob library and install it when we run
    job?  Set this to ``False`` if you've already installed ``mrjob`` on your
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
----------------------

.. mrjob-opt::
    :config: base_tmp_dir
    :switch: --base-tmp-dir
    :type: :ref:`path <data-type-path>`
    :set: all
    :default: value of :py:func:`tempfile.gettempdir`

    Path to put local temp dirs inside.

.. _configs-all-runners-cleanup:

.. mrjob-opt::
   :config: cleanup
   :switch: --cleanup
   :type: :ref:`string <data-type-string>`
   :set: all
   :default: ``'ALL'``

    List of which kinds of directories to delete when a job succeeds. Valid
    choices are:

    * ``'ALL'``: delete local scratch, remote scratch, and logs; stop job flow
        if on EMR and the job is not done when cleanup is run.
    * ``'LOCAL_SCRATCH'``: delete local scratch only
    * ``'LOGS'``: delete logs only
    * ``'NONE'``: delete nothing
    * ``'REMOTE_SCRATCH'``: delete remote scratch only
    * ``'SCRATCH'``: delete local and remote scratch, but not logs
    * ``'JOB'``: stop job if on EMR and the job is not done when cleanup runs
    * ``'JOB_FLOW'``: terminate the job flow if on EMR and the job is not done
        on cleanup
    * ``'IF_SUCCESSFUL'`` (deprecated): same as ``ALL``. Not supported for
        ``cleanup_on_failure``.

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
---------------------

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
    :default: value of :mrjob-opt:`python_bin` (``'python'``)

    Interpreter to launch your script with. Defaults to the value of
    **python_bin**, which is deprecated. Change this if you're using a
    language besides Python 2.5-2.7 or if you're running using
    :py:mod:`virtualenv`.

.. mrjob-opt::
    :config: python_bin
    :switch: --python-bin
    :type: :ref:`command <data-type-command>`
    :set: all
    :default: ``'python'``

    Deprecated. Name/path of alternate Python binary for wrapper scripts and
    mappers/reducers (e.g. for use with :py:mod:`virtualenv`).

.. mrjob-opt::
    :config: setup_cmds
    :switch: --setup_cmd
    :type: :ref:`string list <data-type-string-list>`
    :set: all
    :default: ``[]``

    A list of commands to run before each mapper/reducer step (e.g.  ``['cd
    my-src-tree; make', 'mkdir -p /tmp/foo']``).  You can specify commands as
    strings, which will be run through the shell, or lists of args, which will
    be invoked directly. We'll use file locking to ensure that multiple
    mappers/reducers running on the same node won't run *setup_cmds*
    simultaneously (it's safe to run ``make``).

.. mrjob-opt::
    :config: setup_scripts
    :switch: --setup-script
    :type: :ref:`path list <data-type-path-list>`
    :set: all
    :default: ``[]``

    files that will be copied into the local working directory and then run.
    These are run after *setup_cmds*. Like with *setup_cmds*, we use file
    locking to keep multiple mappers/reducers on the same node from running
    *setup_scripts* simultaneously.

.. mrjob-opt::
    :config: steps_python_bin
    :switch: --steps-python-bin
    :type: :ref:`command <data-type-command>`
    :set: all
    :default: current Python interpreter

    Name/path of alternate python binary to use to query the job about its
    steps (e.g. for use with :py:mod:`virtualenv`). Rarely needed. Defaults
    to ``sys.executable`` (the current Python interpreter).

Other
-----

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

Options ignored by the inline runner
------------------------------------

These options are ignored because they require a real instance of Hadoop:

* :mrjob-opt:`hadoop_extra_args`
* :py:meth:`hadoop_input_format <mrjob.runner.MRJobRunner.__init__>`
* :py:meth:`hadoop_output_format <mrjob.runner.MRJobRunner.__init__>`,
* :mrjob-opt:`hadoop_streaming_jar`
* :mrjob-opt:`jobconf`
* :mrjob-opt:`partitioner`

These options are ignored because the ``inline`` runner does not invoke the job
as a subprocess or run it in its own directory:

* :mrjob-opt:`cmdenv`
* :mrjob-opt:`python_bin`
* :mrjob-opt:`setup_cmds`
* :mrjob-opt:`setup_scripts`
* :mrjob-opt:`steps_python_bin`
* :mrjob-opt:`upload_archives`
* :mrjob-opt:`upload_files`
