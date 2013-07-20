Options available to all runners
================================

The format of each item in this document is:

**mrjob_conf_option_name** (:option:`--command-line-option-name`)
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


.. _opt_bootstrap_mrjob:

**bootstrap_mrjob** (:option:`--bootstrap-mrjob`, :option:`--no-bootstrap-mrjob`)
    Should we automatically tar up the mrjob library and install it when we run
    job?  Set this to ``False`` if you've already installed ``mrjob`` on your
    Hadoop cluster or install it by some other method.

.. _opt_upload_files:

**upload_files** (:option:`--file`)
    Files to copy to the local directory of the mr_job script when it runs. You
    can set the local name of the dir we unpack into by appending
    ``#localname`` to the path; otherwise we just use the name of the file.

    In the config file::

        upload_files:
          - file_1.txt
          - file_2.sqlite

    On the command line::

        --file file_1.txt --file file_2.sqlite

.. _opt_upload_archives:

**upload_archives** (:option:`--archive`)

    A list of archives (e.g. tarballs) to unpack in the local directory of the
    mr_job script when it runs. You can set the local name of the dir we unpack
    into by appending ``#localname`` to the path; otherwise we just use the
    name of the archive file (e.g. ``foo.tar.gz`` is unpacked to the directory
    ``foo.tar.gz/``, and ``foo.tar.gz#stuff`` is unpacked to the directory
    ``stuff/``).

.. _opt_python_archives:

**python_archives** (:option:`--python-archive`)
    Same as upload_archives, except they get added to the job's
    :envvar:`PYTHONPATH`.

Temp files and cleanup
----------------------

.. _opt_base_tmp_dir:

**base_tmp_dir** (:option:`--base-tmp-dir`)
    Path to put local temp dirs inside. By default we just call
    :py:func:`tempfile.gettempdir`

.. _configs-all-runners-cleanup:
.. _opt_cleanup:

**cleanup** (:option:`--cleanup`)
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

.. _opt_cleanup_on_failure:

**cleanup_on_failure** (:option:`--cleanup-on-failure`)
    Which kinds of directories to clean up when a job fails. Valid choices are
    the same as **cleanup**.

.. _opt_output_dir:

**output_dir** (:option:`-o`, :option:`--output-dir`)
    An empty/non-existent directory where Hadoop streaming should put the
    final output from the job.  If you don't specify an output directory,
    we'll output into a subdirectory of this job's temporary directory. You
    can control this from the command line with ``--output-dir``. This option
    cannot be set from configuration files. If used with the ``hadoop`` runner,
    this path does not need to be fully qualified with ``hdfs://`` URIs
    because it's understood that it has to be on HDFS.

.. _opt_no_output:

**no_output** (:option:`--no-output`)
    Don't stream output to STDOUT after job completion.  This is often used in
    conjunction with ``--output-dir`` to store output only in HDFS or S3.

Job execution context
---------------------

.. _opt_cmdenv:

**cmdenv** (:option:`--cmdenv`)
    Dictionary of environment variables to pass to the job inside Hadoop
    streaming.

    In the config file::

        cmdenv:
            PYTHONPATH: $HOME/stuff
            TZ: America/Los_Angeles

    On the command line::

        --cmdenv PYTHONPATH=$HOME/stuff,TZ=America/Los_Angeles

.. _opt_interpreter:

**interpreter** (:option:`--interpreter`)
    Interpreter to launch your script with. Defaults to the value of
    **python_bin**. Change this if you're using a language besides Python
    2.5-2.7 or if you're running using :py:mod:`virtualenv`.

.. _opt_python_bin:

**python_bin** (:option:`--python-bin`)
    Name/path of alternate Python binary for wrapper scripts and
    mappers/reducers (e.g. for use with :py:mod:`virtualenv`). Defaults to
    ``'python'``.

.. _opt_setup_cmds:

**setup_cmds** (:option:`--setup-cmd`)
    A list of commands to run before each mapper/reducer step (e.g.  ``['cd
    my-src-tree; make', 'mkdir -p /tmp/foo']``).  You can specify commands as
    strings, which will be run through the shell, or lists of args, which will
    be invoked directly. We'll use file locking to ensure that multiple
    mappers/reducers running on the same node won't run *setup_cmds*
    simultaneously (it's safe to run ``make``).

.. _opt_setup_scripts:

**setup_scripts** (:option:`--setup-script`)
    files that will be copied into the local working directory and then run.
    These are run after *setup_cmds*. Like with *setup_cmds*, we use file
    locking to keep multiple mappers/reducers on the same node from running
    *setup_scripts* simultaneously.

.. _opt_steps_python_bin:

**steps_python_bin** (:option:`--steps-python-bin`)
    Name/path of alternate python binary to use to query the job about its
    steps (e.g. for use with :py:mod:`virtualenv`). Rarely needed. Defaults
    to ``sys.executable`` (the current Python interpreter).

Other
-----

.. _opt_conf_paths:

**conf_paths** (:option:`-c`, :option:`--conf-path`, :option:`--no-conf`)
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

* :ref:`hadoop_extra_args <opt_hadoop_extra_args>`
* :py:meth:`hadoop_input_format <mrjob.runner.MRJobRunner.__init__>`
* :py:meth:`hadoop_output_format <mrjob.runner.MRJobRunner.__init__>`,
* :ref:`hadoop_streaming_jar <opt_hadoop_streaming_jar>`
* :ref:`jobconf <opt_jobconf>`
* :ref:`partitioner <opt_partitioner>`

These options are ignored because the ``inline`` runner does not invoke the job
as a subprocess or run it in its own directory:

* :ref:`cmdenv <opt_cmdenv>`
* :ref:`python_bin <opt_python_bin>`
* :ref:`setup_cmds <opt_setup_cmds>`
* :ref:`setup_scripts <opt_setup_scripts>`
* :ref:`steps_python_bin <opt_steps_python_bin>`
* :ref:`upload_archives <opt_upload_archives>`
* :ref:`upload_files <opt_upload_files>`
