.. _mrjob.conf:

Config file format and location
===============================

We look for :file:`mrjob.conf` in these locations:

- The location specified by :envvar:`MRJOB_CONF`
- :file:`~/.mrjob.conf`
- :file:`/etc/mrjob.conf`

You can specify one or more configuration files with the :option:`--conf-path`
flag. See :doc:`configs-all-runners` for more information.

The point of :file:`mrjob.conf` is to let you set up things you want every
job to have access to so that you don't have to think about it. For example:

- libraries and source code you want to be available for your jobs
- where temp directories and logs should go
- security credentials

:file:`mrjob.conf` is just a `YAML <http://www.yaml.org>`_- or `JSON
<http://www.json.org>`_-encoded dictionary containing default values to pass in
to the constructors of the various runner classes. Here's a minimal
:file:`mrjob.conf`:

.. code-block:: yaml

    runners:
      emr:
        cmdenv:
          TZ: America/Los_Angeles

Now whenever you run ``mr_your_script.py -r emr``,
:py:class:`~mrjob.emr.EMRJobRunner` will automatically set :envvar:`TZ` to
``America/Los_Angeles`` in your job's environment when it runs on EMR.

If you don't have the :py:mod:`yaml` module installed, you can use JSON
in your :file:`mrjob.conf` instead (JSON is a subset of YAML, so it'll still
work once you install :py:mod:`yaml`). Here's how you'd render the above
example in JSON:

.. code-block:: js

    {
      "runners": {
        "emr": {
          "cmdenv": {
            "TZ": "America/Los_Angeles"
          }
        }
      }
    }

Precedence and combining options
--------------------------------

Options specified on the command-line take precedence over
:file:`mrjob.conf`. Usually this means simply overriding the option in
:file:`mrjob.conf`. However, we know that *cmdenv* contains environment
variables, so we do the right thing. For example, if your :file:`mrjob.conf`
contained:

.. code-block:: yaml

    runners:
      emr:
        cmdenv:
          PATH: /usr/local/bin
          TZ: America/Los_Angeles

and you ran your job as::

    mr_your_script.py -r emr --cmdenv TZ=Europe/Paris --cmdenv PATH=/usr/sbin

We'd automatically handle the :envvar:`PATH`
variables and your job's environment would be::

    {'TZ': 'Europe/Paris', 'PATH': '/usr/sbin:/usr/local/bin'}

What's going on here is that *cmdenv* is associated with
:py:func:`combine_envs`. Each option is associated with an appropriate
combiner function that that combines options in an appropriate way.

Combiner functions can also do useful things like expanding environment
variables and globs in paths. For example, you could set:

.. code-block:: yaml

    runners:
      local:
        upload_files: &upload_files
        - $DATA_DIR/*.db
      hadoop:
        upload_files: *upload_files
      emr:
        upload_files: *upload_files

and every time you ran a job, every job in your ``.db`` file in ``$DATA_DIR``
would automatically be loaded into your job's current working directory.

Also, if you specified additional files to upload with :option:`--file`, those
files would be uploaded in addition to the ``.db`` files, rather than instead
of them.

See :doc:`configs-reference` for the entire dizzying array of configurable
options.

Option data types
-----------------

The same option may be specified multiple times and be one of several data
types. For example, the AWS region may be specified in ``mrjob.conf``, in the
arguments to ``EMRJobRunner``, and on the command line. These are the rules
used to determine what value to use at runtime.

Values specified "later" refer to an option being specified at a higher
priority. For example, a value in ``mrjob.conf`` is specified "earlier" than a
value passed on the command line.

When there are multiple values, they are "combined with" a *combiner function*.
The combiner function for each data type is listed in its description.

Simple data types
^^^^^^^^^^^^^^^^^

When these are specified more than once, the last non-``None`` value is used.

.. _data-type-string:

**String**
    Simple, unchanged string. Combined with
    :py:func:`~mrjob.conf.combine_values`.

.. _data-type-command:

**Command**
    String containing all ASCII characters to be parsed with
    :py:func:`shlex.split`, or list of command + arguments. Combined with
    :py:func:`~mrjob.conf.combine_cmds`.

.. _data-type-path:

**Path**
    Local path with ``~`` and environment variables (e.g. ``$TMPDIR``)
    resolved. Combined with :py:func:`~mrjob.conf.combine_paths`.

List data types
^^^^^^^^^^^^^^^

The values of these options are specified as lists. When specified more than
once, the lists are concatenated together.

.. _data-type-string-list:

**String list**
    List of :ref:`strings <data-type-string>`. Combined with
    :py:func:`~mrjob.conf.combine_lists`.

.. _data-type-path-list:

**Path list**
    List of :ref:`paths <data-type-path>`. Combined with
    :py:func:`~mrjob.conf.combine_path_lists`.

.. _configs-list-example:

Strings and non-sequence data types (e.g. numbers) are treated as
single-item lists.

For example,

.. code-block:: yaml

    runners:
      emr:
        setup: /run/some/command with args

is equivalent to:

.. code-block:: yaml

    runners:
      emr:
        setup:
        - /run/some/command with args

Dict data types
^^^^^^^^^^^^^^^

The values of these options are specified as dictionaries. When specified more
than once, each has custom behavior described below.

.. _data-type-plain-dict:

**Plain dict**
    Values specified later override values specified earlier. Combined with
    :py:func:`~mrjob.conf.combine_dicts`.

.. _data-type-env-dict:

**Environment variable dict**
    Values specified later override values specified earlier, **except for
    those with keys ending in PATH**, in which values are concatenated and
    separated by a colon (``:``) rather than overwritten. The later value comes
    first.

    For example, this config:

    .. code-block:: yaml

        runners:
          emr:
            cmdenv:
              PATH: /usr/bin

    when run with this command::

        python my_job.py --cmdenv PATH=/usr/local/bin

    will result in the following value of ``cmdenv``:

        ``/usr/local/bin:/usr/bin``

    The function that handles this is :py:func:`~mrjob.conf.combine_envs`.

    **The one exception** to this behavior is in the ``local`` runner, which
    uses the local system separator (on Windows ``;``, on everything else still
    ``:``) instead of always using ``:``. In local mode, the function that
    combines config values is :py:func:`~mrjob.conf.combine_local_envs`.

.. _multiple-config-files:

Using multiple config files
---------------------------

If you have several standard configurations, you may want to have several
config files "inherit" from a base config file. For example, you may have one
set of AWS credentials, but two code bases and default instance sizes. To
accomplish this, use the ``include`` option:

:file:`~/mrjob.very-large.conf`:

.. code-block:: yaml

    include: ~/.mrjob.base.conf
    runners:
      emr:
        num_core_instances: 20
        core_instance_type: m1.xlarge

:file:`~/mrjob.very-small.conf`:

.. code-block:: yaml

    include: $HOME/.mrjob.base.conf
    runners:
      emr:
        num_core_instances: 2
        core_instance_type: m1.small

:file:`~/.mrjob.base.conf`:

.. code-block:: yaml

    runners:
      emr:
        aws_access_key_id: HADOOPHADOOPBOBADOOP
        aws_secret_access_key: MEMIMOMADOOPBANANAFANAFOFADOOPHADOOP
        region: us-west-1

Options that are lists, commands, dictionaries, etc. combine the same way they
do between the config files and the command line (with combiner functions).

You can use ``$ENVIRONMENT_VARIABLES`` and ``~/file_in_your_home_dir`` inside
``include``.

You can inherit from multiple config files by passing ``include`` a list
instead of a string. Files on the right will have precedence over files on the
left. To continue the above examples, this config:

:file:`~/.mrjob.everything.conf`

.. code-block:: yaml

    include:
    - ~/.mrjob.very-small.conf
    - ~/.mrjob.very-large.conf

will be equivalent to this one:

:file:`~/.mrjob.everything-2.conf`

.. code-block:: yaml

    runners:
      emr:
        aws_access_key_id: HADOOPHADOOPBOBADOOP
        aws_secret_access_key: MEMIMOMADOOPBANANAFANAFOFADOOPHADOOP
        core_instance_type: m1.xlarge
        num_core_instances: 20
        region: us-west-1

In this case, :file:`~/.mrjob.very-large.conf` has taken precedence over
:file:`~/.mrjob.very-small.conf`.

.. _configs-relative-includes:

Relative includes
^^^^^^^^^^^^^^^^^

Relative ``include:`` paths are relative to the real (after resolving
symlinks) path of the including conf file.

For example, you could do this:

:file:`~/.mrjob/base.conf`:

.. code-block:: yaml

    runners:
      ...

:file:`~/.mrjob/default.conf`:

.. code-block:: yaml

    include: base.conf

You could then load your configs via a symlink :file:`~/.mrjob.conf` to
:file:`~/.mrjob/default.conf` and :file:`~/.mrjob/base.conf` would still be
included (even though it's not in the same directory as the symlink).

.. _clearing-configs:

Clearing configs
----------------

Sometimes, you just want to override a list-type config (e.g. ``setup``) or
a ``*PATH`` environment variable, rather than having mrjob cleverly concatenate
it with previous configs.

You can do this in YAML config files by tagging the values you want to take
precedence with the ``!clear`` tag.

For example:

:file:`~/.mrjob.base.conf`

.. code-block:: yaml

    runners:
      emr:
        aws_access_key_id: HADOOPHADOOPBOBADOOP
        aws_secret_access_key: MEMIMOMADOOPBANANAFANAFOFADOOPHADOOP
        cmdenv:
          PATH: /this/nice/path
          PYTHONPATH: /here/be/serpents
          USER: dave
        setup:
        - /run/this/command

:file:`~/.mrjob.conf`

.. code-block:: yaml

    include: ~/mrjob.base.conf
    runners:
      emr:
        cmdenv:
          PATH: !clear /this/even/better/path/yay
          PYTHONPATH: !clear
        setup: !clear
        - /run/this/other/command

is equivalent to:

.. code-block:: yaml

    runners:
      emr:
        aws_access_key_id: HADOOPHADOOPBOBADOOP
        aws_secret_access_key: MEMIMOMADOOPBANANAFANAFOFADOOPHADOOP
        cmdenv:
          PATH: /this/even/better/path/yay
          USER: dave
        setup:
        - /run/this/other/command

If you specify multiple config files (e.g.
``-c ~/mrjob.base.conf -c ~/mrjob.conf``), a ``!clear`` in a later file will
override earlier files. ``include:`` is really just another way to prepend
to the list of config files to load.

If you find it more readable, you may put the ``!clear`` tag *before* the
key you want to clear. For example,

.. code-block:: yaml

    runners:
      emr:
        !clear setup:
        - /run/this/other/command

is equivalent to:

.. code-block:: yaml

    runners:
      emr:
        setup: !clear
        - /run/this/other/command

``!clear`` tags in lists are ignored. You cannot currently clear an entire set
of configs (e.g. ``runners: emr: !clear ...`` does not work).
