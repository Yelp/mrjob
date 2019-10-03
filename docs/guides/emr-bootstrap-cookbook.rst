============================
 EMR Bootstrapping Cookbook
============================

Bootstrapping allows you to run commands to customize EMR machines, at the
time the cluster is created.

When to use bootstrap, and when to use setup
============================================

You can use :mrjob-opt:`bootstrap` and :mrjob-opt:`setup` together.

Generally, you want to use :mrjob-opt:`bootstrap` for things that are
part of your general production environment, and :mrjob-opt:`setup`
for things that are specific to your particular job. This makes things
work as expected if you are using :ref:`cluster-pooling`.

EMR will generally not allow you to use :command:`sudo` in
:mrjob-opt:`setup` commands. See :doc:`setup-cookbook` for how to install
libraries, etc. without using :command:`sudo`.

.. _using-pip:

Installing Python packages with pip
===================================

The only tricky thing is making sure you install packages for the correct
version of Python.

.. _installing-ujson:

Figure out which version of Python you'll be running on EMR (see
:mrjob-opt:`python_bin` for defaults).

 * If it's Python 2, use :command:`pip-2.7` (just plain :command:`pip` also
   works on AMI 4.3.0 and later)
 * If it's Python 3, use :command:`pip-3.6` on AMI 5.20.0+,
   and :command:`pip-3.4` for earlier AMIs

For example, to install :py:mod:`ujson` on Python 2:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        - sudo pip-2.7 install ujson

See `PyPI <https://pypi.python.org/pypi>`_ for a the full list of available
Python packages.

You can also install packages from a `requirements <https://pip.pypa.io/en/stable/user_guide/#requirements-files>`__ file:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        - sudo pip-2.7 install -r /local/path/of/requirements.txt#

Or a tarball:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        - sudo pip-2.7 install /local/path/of/tarball.tar.gz#

.. warning::

   If you're trying to run jobs on AMI version 3.0.0 (protip: don't do that)
   :command:`pip` appears not to work due to out-of-date SSL
   certificate information.

.. _installing-pypy-on-emr:

Installing PyPy
===============

First, download the version of PyPy you want to use from
`Portable PyPy Distributions for Linux <https://bitbucket.org/squeaky/portable-pypy/downloads/>`__.

Then instruct EMR to un-tar it and link to the binary in ``/usr/bin``. For example:

.. code-block:: yaml

   runners:
     emr:
       bootstrap:
       - sudo tar xvfj /local/path/to/pypy-7.1.1-linux_x86_64-portable.tar.bz2# -C /opt
       - sudo ln -s /opt/pypy-7.1.1-linux_x86_64-portable/bin/pypy /usr/bin/pypy

.. _installing-packages:

Installing System Packages
==========================

EMR gives you access to a variety of different Amazon Machine Images, or AMIs
for short (see :mrjob-opt:`image_version`).

3.x and later AMIs
------------------

Starting with 3.0.0, EMR AMIs use Amazon Linux, which uses :command:`yum` to
install packages. For example, to install NumPy:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        - sudo yum install -y python-numpy

(Don't forget the ``-y``!)

Amazon Linux's Python packages generally only work for Python 2.
If you're on Python 3, just :ref:`use pip <using-pip>`.

The most recent list of Amazon linux packages can be found `here <https://aws.amazon.com/amazon-linux-ami/>`__ (click on "Packages List" in the left sidebar).

2.x AMIs
--------

Probably not worth the trouble. The 2.x AMIs are based on a version of Debian
that is so old it has been "archived," which makes their package installer,
:command:`apt-get`, no longer work out-of-the-box. Moreover, Python system
packages work for Python 2.6, not 2.7.

Instead, just use :command:`pip-2.7` to install Python libraries.
