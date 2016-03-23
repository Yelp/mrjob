EMR Bootstrapping Cookbook
==========================

Bootstrapping allows you to run commands to customize EMR machines, at the
time the cluster is created.

When to use bootstrap, and when to use setup
--------------------------------------------

You can use :mrjob-opt:`bootstrap` and :mrjob-opt:`setup` together.

Generally, you want to use :mrjob-opt:`bootstrap` for things that are
part of your general production environment, and :mrjob-opt:`setup`
for things that are specific to your particular job. This makes things
work as expected if you are :ref:`pooling-clusters`.

All these examples use :mrjob-opt:`bootstrap`. Not saying it's a good idea, but
all these examples will work with :mrjob-opt:`setup` as well (yes, Hadoop
tasks on EMR apparently have access to :command:`sudo`).


.. _using-pip:

Installing Python packages with pip
-----------------------------------

The only tricky thing is making sure you install packages for the correct
version of Python.

.. _installing-ujson:

Figure out which Python binary you're using on EMR (it defaults to one
of :command:`python2.6`, :command:`python2.7`, or :command:`python3`; see
:mrjob-opt:`python_bin`), and then use
``sudo <python binary> -m pip <packages>``.

For example, to install :py:mod:`ujson` on Python 2.7:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        - sudo python2.7 -m pip ujson

See `PyPI <https://pypi.python.org/pypi>`_ for a the full list of available
Python packages.

You can also install packages from a `requirements <https://pip.pypa.io/en/stable/user_guide/#requirements-files>`__ file:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        - sudo python2.7 -m pip install -r /local/path/of/requirements.txt#

Or a tarball:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        - sudo python2.7 -m pip install /local/path/of/tarball.tar.gz#

.. note::

  If for some reason you must run on AMI version 2.4.2 or earlier (protip:
  don't do that), see :ref:`below <installing-pip-on-2.x-amis>` for how to get
  :command:`pip` working.

.. warning::

   If you're trying to run jobs on AMI version 3.0.0 (protip: don't do that
   either) :command:`pip` appears not to work due to out-of-date SSL
   certificate information.


Installing System Packages
--------------------------

EMR gives you access to a variety of different Amazon Machine Images, or AMIs
for short (see :mrjob-opt:`ami_version`).

3.x and 4.x AMIs
^^^^^^^^^^^^^^^^

Starting with 3.0.0, EMR AMIs use Amazon Linux, which uses :command:`yum` to
install packages. For example, to install NumPy:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        - sudo yum install -y python-numpy

(Don't forget the ``-y``!)

Here are the package lists for all the various versions of Amazon Linux used
by EMR:

 * `2015.09 <http://aws.amazon.com/amazon-linux-ami/2015.09-packages/>`__ (3.11.0 and 4.2.0-4.4.0)
 * `2015.03 <http://aws.amazon.com/amazon-linux-ami/2015.03-packages/>`__ (3.7.90-3.10.0 and 4.0.0-4.1.0)
 * `2014.09 <http://aws.amazon.com/amazon-linux-ami/2014.09-packages/>`__ (3.4.0-3.6.0)
 * `2014.03 <http://aws.amazon.com/amazon-linux-ami/2014.03-packages/>`__ (3.1.0-3.3.2)
 * `2013.09 <http://aws.amazon.com/amazon-linux-ami/2014.09-packages/>`__ (3.0.0-3.0.4)

2.x AMIs
^^^^^^^^

The 2.x AMIs are based on a version of Debian that is so old it has been
"archived," which makes their package installer, :command:`apt-get`, no
longer work out-of-the-box.

.. _installing-pip-on-2.x-amis:

If you *must* use the 2.x AMIs, you can get :command:`apt-get` working
again by updating ``/etc/apt/sources.list``. For example, to
install :command:`pip` for Python 2.6:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        - sudo echo "deb http://archive.debian.org/debian/ squeeze main contrib non-free" > /etc/apt/sources.list
        - sudo apt-get install -y python-pip

.. note::

   :command:`pip-2.7` is already installed by default on AMI version 2.4.3 and
   later.

See the `full list of Squeeze packages
<https://packages.debian.org/squeeze/>`__ for all the (very old versions of)
software you can install.

.. _installing-python-from-source:

Installing Python from source
-----------------------------

If you really must use a version of Python that's not available on EMR
(e.g. Python 3.5 or a very specific patch version), you can
download and compile Python from source.

.. note::

   This adds an extra 5 to 10 minutes before the cluster can run your job.

Here's how you download and install a Python tarball:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        - wget -S -T 10 -t 5 https://www.python.org/ftp/python/x.y.z/Python-x.y.z.tgz
        - tar xfz Python-x.y.z.tgz
        - cd Python-x.y.z; ./configure && make && sudo make install; cd ..
        bootstrap_python: false
        python_bin: /usr/local/bin/python

(Replace ``x.y.z`` with a specific version of Python.)

Python 3.4+ comes with :command:`pip` by default, but earlier versions do not,
so you'll want to tack on ``get-pip.py``:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        ...
        - wget -S -T 10 -t 5 https://bootstrap.pypa.io/get-pip.py
        - sudo /usr/local/bin/python get-pip.py

Also, :command:`pip` will be installed in ``/usr/local/bin``, which is not in
the path for :command:`sudo`. Running pip with the :command:`python` binary
you just compiled will work for any version of Python:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        ...
        - sudo /usr/local/bin/python -m pip ...
