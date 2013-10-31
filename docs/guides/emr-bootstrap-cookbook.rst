EMR Bootstrapping Cookbook
==========================

Bootstrapping allows you duplicate your production setup within EMR.

Installing Python packages with pip
-----------------------------------

Make a `pip requirements file <http://www.pip-installer.org/en/1.1/requirements.html>`_, and run your script with

.. code-block:: sh

    --bootstrap 'sudo pip install -r path/to/requirements.txt#'

Or, equivalently, in :file:`mrjob.conf`:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        - pip install -r requirements.txt#

Installing Python packages from tarballs
----------------------------------------

``pip`` can be used to install custom python packages from tarballs
as well:

.. code-block:: sh

    --bootstrap 'sudo pip install path/to/packages/*.tar.gz#'

Installing Debian packages
--------------------------

You can use ``apt-get`` to install Debian packages. For example, to install
Python 3:

.. code-block:: sh

    --bootstrap 'sudo apt-get install -y python3'

If you have particular ``.deb`` files you want to install, do:

.. code-block:: sh

    --bootstrap 'sudo dpkg -i path/to/packages/*.deb#'

Upgrading Python from source
----------------------------

To upgrade Python on EMR, you will probably have to build it from source
(Debian packages tend to lag the current versions of software, and EMR
AMIs tend to lag the current version of Debian).

First, download the latest version of the Python source `here <http://www.python.org/getit/>`_.

Then add this to your :file:`mrjob.conf`:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        - tar xfz path/to/Python-x.y.z.tgz#
        - cd Python-x.y.z
	- ./configure && make && sudo make install

:mrjob-opt:`bootstrap_mrjob` runs *last*, so mrjob will get bootstrapped
into your newly upgraded version of Python. Of course, if you also install
other Python libraries with bootstrap commands, you should run them *after*
upgrading Python.

Catching errors in your bootstrap script
----------------------------------------

By default, shell scripts ignore errors and simply move to the next line
when they fail.

To fail on errors, use:

.. code-block:: sh

    --bootstrap 'set -e'

Using bash
----------

By default, :mrjob-opt:`bootstrap` uses :command:`sh` (Bourne shell).

To use bash instead, do:

.. code-block:: sh

    --sh-bin bash

This only works with shells that are backwards-compatible with Bourne shell.

When to use bootsrap, and when to use setup
-------------------------------------------

You can use :mrjob-opt:`bootstrap` and :mrjob-opt:`setup` together.

Generally, you want to use :mrjob-opt:`bootstrap` for things that are
part of your general production environment, and :mrjob-opt:`setup`
for things that are specific to your particular job. This makes things
work as expected if you are :ref:`pooling-job-flows`.
