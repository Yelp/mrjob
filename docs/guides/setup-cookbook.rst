Job Environment Setup Cookbook
==============================

Many jobs have significant external dependencies, both libraries and other
source code.

Combining shell syntax with Hadoop's DistributedCache notation, mrjob's
:mrjob-opt:`setup` option provides a powerful, dynamic alternative to
pre-installing your Hadoop dependencies on every node.

All our :file:`mrjob.conf` examples below are for the ``hadoop`` runner,
but these work equally well with the ``emr`` runner. Also, if you are using
EMR, take a look at the :doc:`emr-bootstrap-cookbook`.

.. _cookbook-src-tree-pythonpath:

Putting your source tree in :envvar:`PYTHONPATH`
------------------------------------------------

First you need to make a tarball of your source tree. Make sure that the root
of your source tree is at the root of the tarball's file listing (e.g. the
module ``foo.bar`` appears as ``foo/bar.py`` and not
``your-src-dir/foo/bar.py``).

For reference, here is a command line that will put an entire source directory
into a tarball:

.. code-block:: sh

    tar -C your-src-code -f your-src-code.tar.gz -z -c .

Then, run your job with:

.. code-block:: sh

    --setup 'export PYTHONPATH=$PYTHONPATH:your-src-dir.tar.gz#/'

If every job you run is going to want to use ``your-src-code.tar.gz``, you can do
this in your :file:`mrjob.conf`:

.. code-block:: yaml

    runners:
      hadoop:
        setup:
        - export PYTHONPATH=$PYTHONPATH:your-src-code.tar.gz#/

Running a makefile inside your source dir
-----------------------------------------

.. code-block:: sh

    --setup 'cd your-src-dir.tar.gz#/' --setup 'make'

or, in mrjob.conf:

.. code-block:: yaml

    runners:
      hadoop:
        setup:
        - cd your-src-dir.tar.gz#
        - make

If Hadoop runs multiple tasks on the same node, your source dir will be shared
between them. This is not a problem; mrjob automatically adds locking around
setup commands to ensure that multiple copies of your setup script don't
run simultaneously.

Making data files available to your job
---------------------------------------

Best practice for one or a few files is to use passthrough options; see
:py:meth:`~mrjob.job.MRJob.add_passthrough_option`.

You can also use :mrjob-opt:`upload_files` to upload file(s) into a task's
working directory (or :mrjob-opt:`upload_archives` for tarballs and other
archives).

If you're a :mrjob-opt:`setup` purist, you can also do something like this:

.. code-block:: sh

    --setup 'true your-file#desired-name'

since :command:`true` has no effect and ignores its arguments.

Using a virtualenv
------------------

What if you can't install the libraries you need on your Hadoop cluster?

You could do something like this in your :file:`mrjob.conf`:

.. code-block:: yaml

    runners:
      hadoop:
        setup:
        - virtualenv venv
        - . venv/bin/activate
        - pip install mr3po simplejson

However, now the locking feature that protects :command:`make` becomes a
liability; each task on the same node has its own virtualenv, but one task has
to finish setting up before the next can start.

The solution is to share the virtualenv between all tasks on the same
machine, something like this:

.. code-block:: yaml

    runners:
      hadoop:
        setup:
        - VENV=/tmp/$mapreduce_job_id
        - if [ ! -d $VENV ]; then virtualenv $VENV ; fi
        - . $VENV/bin/activate
        - pip install mr3po simplejson
        
With older versions of Hadoop (0.20 and earlier, and the 1.x series), you'd
want to use ``$mapred_job_id``.

Other ways to use pip to install Python packages
------------------------------------------------

If you have a lot of dependencies, best practice is to make a
`pip requirements file <http://www.pip-installer.org/en/latest/cookbook.html>`_
and use the ``-r`` switch:

.. code-block:: sh

    --setup 'pip install -r path/to/requirements.txt#'

Note that :command:`pip` can also install from tarballs (which is useful
for custom-built packages):

.. code-block:: sh

    --setup 'pip install $MY_PYTHON_PKGS/*.tar.gz#'
