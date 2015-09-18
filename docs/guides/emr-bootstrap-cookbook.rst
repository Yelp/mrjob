EMR Bootstrapping Cookbook
==========================

Bootstrapping allows you to configure EMR machines to your needs.

AMI 2.x and AMI 3.x version differences
-----------------------------------------------
AMI versions 2.x are based on `Debian 6.0.2 (Squeeze)
<http://www.debian.org/News/2011/20110625>`_.  The package management system is ``apt-get``. Any package distributed with Debian Squeeze should be available.

AMI versions 3.x are based on `Amazon Linux Release 2012.09
<https://aws.amazon.com/amazon-linux-ami/2012.09-release-notes/>`_. This major bump changed the package management system to ``yum``. You can view the list of RPM packages Amazon distributed with the 2012.09.1 release `here
<https://aws.amazon.com/amazon-linux-ami/2012.09-packages/>`__.

You can follow the AMI changelog `here
<http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-plan-ami.html>`__.

Installing Python packages with pip
-----------------------------------

First you need to install :command:`pip`.

For AMI 2.x versions use ``apt-get``:

.. code-block:: sh

   --bootstrap 'sudo apt-get install -y python-pip'

For AMI 3.x versions use ``yum``:

.. code-block:: sh

   --bootstrap 'sudo yum install -y python-pip'

Then install the packages you want:

.. code-block:: sh

    --bootstrap 'sudo pip install --upgrade mr3po ujson'

To support both AMI 2.x and AMI 3.x:

.. code-block:: sh

    --bootstrap 'sudo apt-get install -y python-pip || sudo yum install -y python-pip'

Or, equivalently, in :file:`mrjob.conf`:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        - sudo apt-get install -y python-pip || sudo yum install -y python-pip
        - sudo pip install boto mr3po

Installing ujson
----------------

mrjob can use Python's built-in JSON library, but the ujson library is much
faster.

To use the latest (fastest) version of ujson, do:

.. code-block:: sh

    --bootstrap 'sudo pip install --upgrade ujson'

Other ways to use pip to install Python packages
------------------------------------------------

If you have a lot of dependencies, best practice is to make a
`pip requirements file <http://www.pip-installer.org/en/latest/cookbook.html>`_
and use the ``-r`` switch:

.. code-block:: sh

    --bootstrap 'sudo pip install -r path/to/requirements.txt#'

Note that :command:`pip` can also install from tarballs (which is useful
for custom-built packages):

.. code-block:: sh

    --bootstrap 'sudo pip install $MY_PYTHON_PKGS/*.tar.gz#'

Installing Debian packages on AMI 2.x:
--------------------------------------

As we did with :command:`pip`, you can use ``apt-get`` to install any
package from the Debian archive. For example, to install Python 3:

.. code-block:: sh

    --bootstrap 'sudo apt-get install -y python3'

If you have particular ``.deb`` files you want to install, do:

.. code-block:: sh

    --bootstrap 'sudo dpkg -i path/to/packages/*.deb#'

Installing RPM Packages on AMI 3.x:
-----------------------------------

Conversely, while running on an AMI 3.x you can install the Python 3 RPM archive by using ``yum``:

.. code-block:: sh

    --bootstrap 'sudo yum install -y python3'

Likewise, if you have a particular ``.rpm`` files you want to install, do:

.. code-block:: sh

    --bootstrap 'sudo yum install -y path/to/packages/*.rpm#'

.. _bootstrap-python-source:

Installing Python from source
-----------------------------

If you're using an AMI version before 3.7.0, and you want to use Python 2.7,
or any version of Python 3 there is not an easy way to upgrade
Python from a package (okay, technically, the 2.x AMIs have a Python 3.1
package, but that's not helpful.)

Here's what to add to your :file:`mrjob.conf` to download the Python source,
compile, and install it:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        - wget -S -T 10 -t 5 https://www.python.org/ftp/python/x.y.z/Python-x.y.z.tgz
        - tar xvfz Python-x.y.z.tgz
        - cd Python-x.y.z
        - ./configure && make && sudo make install
        - cd ..
        - sudo rm /usr/bin/python
        - sudo ln -s /usr/local/bin/pythonx.y /usr/bin/python
        bootstrap_python: false  # don't try to auto-install Python
        python_bin: python  # make this config work in Python 3 too

Replace *x.y.z* with the version of Python you want. Note that the
python binary we symlink to is named *pythonx.y*, not *pythonx.y.z*.

:mrjob-opt:`bootstrap_mrjob` runs *last*, so mrjob *will* get bootstrapped
into your newly upgraded version of Python. If you use other
bootstrap commands to install/upgrade Python libraries, you should also
run them *after* upgrading Python.

When to use bootstrap, and when to use setup
--------------------------------------------

You can use :mrjob-opt:`bootstrap` and :mrjob-opt:`setup` together.

Generally, you want to use :mrjob-opt:`bootstrap` for things that are
part of your general production environment, and :mrjob-opt:`setup`
for things that are specific to your particular job. This makes things
work as expected if you are :ref:`pooling-job-flows`.
