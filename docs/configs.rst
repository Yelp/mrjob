Runner configuration
====================
.. toctree::

    configs-runners.rst
    configs-reference.rst
    configs-conf.rst

.. _mrjob.conf:

Config file format and location
-------------------------------

We look for :file:`mrjob.conf` in these locations:

- The location specified by :envvar:`MRJOB_CONF`
- :file:`~/.mrjob.conf`
- :file:`~/.mrjob` **(deprecated)**
- :file:`mrjob.conf` in any directory in :envvar:`PYTHONPATH` **(deprecated)**
- :file:`/etc/mrjob.conf`

If your :file:`mrjob.conf` path is deprecated, use this table to fix it:

================================= ===============================
Old Location                      New Location
================================= ===============================
:file:`~/.mrjob`                  :file:`~/.mrjob.conf`
somewhere in :envvar:`PYTHONPATH` Specify in :envvar:`MRJOB_CONF`
================================= ===============================

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

Combiners can also do useful things like expanding environment variables and
globs in paths. For example, you could set:

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

See :doc:`configs-runners` for the entire dizzying array of configurable
options.
