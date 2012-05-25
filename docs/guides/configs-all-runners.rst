Options available to all runners
================================

The format of each item in this document is:

**mrjob_conf_option_name** (:option:`--command-line-option-name`)
    Description of option behavior

Options that take multiple values can be passed multiple times on the command
line.

.. _configs-making-files-available:

Making files available to tasks
-------------------------------

Most jobs have dependencies of some sort - Python packages, Debian packages,
data files, etc. This section covers options available to all runners that
mrjob uses to upload files to your job's execution environments.


**bootstrap_mrjob** (:option:`--bootstrap-mrjob`)
    Should we automatically tar up the mrjob library and install it when we run
    job?  Set this to ``False`` if you've already installed ``mrjob`` on your
    Hadoop cluster or install it by some other method.

**upload_files** (:option:`--upload-file`)
    A list of files to copy to the local directory of the mr_job script when it
    runs. You can set the local name of the dir we unpack into by appending
    ``#localname`` to the path; otherwise we just use the name of the file.

**upload_archives** (:option:`--upload-archive`)

    A list of archives (e.g. tarballs) to unpack in the local directory of the
    mr_job script when it runs. You can set the local name of the dir we unpack
    into by appending ``#localname`` to the path; otherwise we just use the
    name of the archive file (e.g. ``foo.tar.gz`` is unpacked to the directory
    ``foo.tar.gz/``, and ``foo.tar.gz#stuff`` is unpacked to the directory
    ``stuff/``).

**python_archives** (:option:`--python-archive`)
    Same as upload_archives, except they get added to the job's
    :envvar:`PYTHONPATH`.

Temp files and cleanup
----------------------

**base_tmp_dir** (:option:`--base-tmp-dir`)
    Path to put local temp dirs inside. By default we just call
    :py:func:`tempfile.gettempdir`

**cleanup** (:option:`--cleanup`)
    List of which kinds of directories to delete when a job succeeds. Valid
    choices are:

    * ``ALL``: delete local scratch, remote scratch, and logs
    * ``LOCAL_SCRATCH``: delete local scratch only
    * ``LOGS``: delete logs only
    * ``NONE``: delete nothing
    * ``REMOTE_SCRATCH``: delete remote scratch only
    * ``SCRATCH``: delete local and remote scratch, but not logs

**cleanup_on_failure** (:option:`--cleanup-on-failure`)
    Which kinds of directories to clean up when a job fails. Valid choices are
    the same as **cleanup**.

Job execution context
---------------------

**cmdenv** (:option:`--cmdenv`)
    Environment variables to pass to the job inside Hadoop streaming

**label** (:option:`--label`)
    Description of this job to use as the part of its name.  By default, we
    use the script's module name, or ``no_script`` if there is none.

**owner** (:option:`--owner`)
    Who is running this job. Used solely to set the job name.  By default, we
    use :py:func:`getpass.getuser`, or ``no_user`` if it fails.

**python_bin** (:option:`--python-bin`)
    Name/path of alternate python binary for mappers/reducers (e.g. for use
    with :py:mod:`virtualenv`). Defaults to ``'python'``.

**setup_cmds** (:option:`--setup-cmd`)
    A list of commands to run before each mapper/reducer step (e.g.  ``['cd
    my-src-tree; make', 'mkdir -p /tmp/foo']``).  You can specify commands as
    strings, which will be run through the shell, or lists of args, which will
    be invoked directly. We'll use file locking to ensure that multiple
    mappers/reducers running on the same node won't run *setup_cmds*
    simultaneously (it's safe to run ``make``).

**setup_scripts** (:option:`--setup-script`)
    files that will be copied into the local working directory and then run.
    These are run after *setup_cmds*. Like with *setup_cmds*, we use file
    locking to keep multiple mappers/reducers on the same node from running
    *setup_scripts* simultaneously.

Hadoop configuration
--------------------

**hadoop_extra_args** (:option:`--hadoop-extra-arg`)
    Extra arguments to pass to hadoop streaming

**hadoop_streaming_jar** (:option:`--hadoop-streaming-jar`)
    Path to a custom hadoop streaming jar

**jobconf** (:option:`--jobconf`)
    ``-jobconf`` args to pass to hadoop streaming. This should be a map from
    property name to value.  Equivalent to passing ``['-jobconf',
    'KEY1=VALUE1', '-jobconf', 'KEY2=VALUE2', ...]`` to *hadoop_extra_args*.

Other
-----

**steps_python_bin** (:option:`--steps-python-bin`)
    Name/path of alternate python binary to use to query the job about its
    steps (e.g. for use with :py:mod:`virtualenv`). Rarely needed. Defaults
    to ``sys.executable`` (the current Python interpreter).
