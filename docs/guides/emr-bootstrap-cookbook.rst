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

:command:`pip` is the standard way to install Python packages.

on Python 2
^^^^^^^^^^^

As long as you're using AMI version 3.0.0 or later, mrjob automatically
installs :command:`pip` by default in Python 2 (see
:mrjob-opt:`bootstrap_mrjob`). All you have to do is run it with
:command:`sudo`. For example:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        - sudo pip install dateglob mr3po

See `PyPI <https://pypi.python.org/pypi>`_ for a the full list of available
Python packages.

You can also install packages from a `requirements <https://pip.pypa.io/en/stable/user_guide/#requirements-files>`__ file:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        - sudo pip install -r /local/path/of/requirements.txt#

Or a tarball:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        - sudo pip install /local/path/of/tarball.tar.gz#

If you *must* run on the (deprecated) 2.x AMIs, see
:ref:`below <using-ujson-py2-ami-v2>` for what it takes to get :command:`pip`
working.

If you turned off :mrjob-opt:`bootstrap_mrjob` but still want :command:`pip`,
the relevant package is ``python-pip``; see :ref:`bootstrap-system-packages`.

.. _using-pip-py3:

on Python 3
^^^^^^^^^^^

Python 3 is available on AMI versions 3.7.0 and later, but Amazon's package
is very minimal; it doesn't include Python source, or even :command:`pip`.

If you want to install a pure-python package, it's enough to install
pip using `PyPA's get-pip.py script <http://pip-python3.readthedocs.org/en/latest/installing.html>`__:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        - wget -S -T 10 -t 5 https://bootstrap.pypa.io/get-pip.py
        - sudo python3 get-pip.py
        - sudo python3 -m pip install dateglob mr3po

This works with requirements files or tarballs too:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        ...
        - sudo python3 -m pip install -r /local/path/of/requirements.txt#
        - sudo python3 -m pip install /local/path/of/tarball.tar.gz#

If you want to install a Python package with C bindings (e.g. ``numpy``)
you'll first need to compile Python from source. See
:ref:`Installing ujson on Python 3 <using-ujson-py3>` for how this works.


Installing ujson
----------------

``ujson`` is a fast, pure-C library; if installed, mrjob will automatically
use it to turbocharge JSON-serialization.

on Python 2 (3.x AMIs and later)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

On Python 2, mrjob automatically installs ``ujson`` for you as long as you're
using AMI version 3.0.0 or later. Done!

.. _using-ujson-py2-ami-v2:

on Python 2 (2.x AMIs)
^^^^^^^^^^^^^^^^^^^^^^

The 2.x AMI series is based on version of Debian
(`Squeeze <http://www.debian.org/News/2011/20110625>`_) that is so old it
has been "archived", which means that you need to update
``/etc/apt/sources.list`` before you can install packages.

Here's how to update ``sources.list``, install ``pip``,
and then :command:`pip install` the ``ujson`` library:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        - sudo echo "deb http://archive.debian.org/debian/ squeeze main contrib non-free" > /etc/apt/sources.list
        - sudo apt-get install -y python-pip
        - sudo pip install ujson

.. _using-ujson-py3:

on Python 3
^^^^^^^^^^^

Amazon's ``python34`` package doesn't have the bindings to compile Python
packages that use C, so let's skip that and install Python from source instead:

.. code-block:: yaml

    runners:
      emr:
        bootstrap:
        - wget -S -T 10 -t 5 https://www.python.org/ftp/python/3.y.z/Python-3.y.z.tgz
        - tar xfz Python-3.y.z.tgz
        - cd Python-3.y.z; ./configure && make && sudo make install; cd ..
        - sudo /usr/local/bin/python -m pip install ujson
        bootstrap_python: false
        python_bin: /usr/local/bin/python

(Replace ``3.y.z`` with the specific version of Python you want.)

The downside is that it will now take an extra 5-10 minutes for your cluster
to spin up (because it's compiling Python), so you have to weigh that against
the potential speed improvement from ``ujson``. If it matters, try it and see
what's faster.

The most efficient solution would be to build your own Python 3 RPM and just
install that, but that's beyond the scope of this cookbook.

.. _bootstrap-system-packages:

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

AMI versions 3.7.0 and later use Amazon Linux 2015.03; here is
`the full list of 2015.03 packages <http://aws.amazon.com/amazon-linux-ami/2015.03-packages/>`__.

If you need to use an earlier AMI version, look it up
`here <http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/ami-versions-supported.html>`__
and then go to ``http://aws.amazon.com/amazon-linux-ami/YYYY.MM-packages``
(replace ``YYYY.MM`` with the Amazon Linux version).

Keep in mind that Amazon Linux has zero support for Python 3 outside
the ``python34`` and ``python34-docs`` packages themselves;
:ref:`install and use pip <using-pip-py3>` instead.

2.x AMIs
^^^^^^^^

The 2.x AMIs are based on a very old version of Debian. You probably shouldn't
be using them at all, but if you do, you'll need to apply a small fix
before you can :command:`apt-get install -y` packages; see
:ref:`above <using-ujson-py2-ami-v2>` for an example of how to do this.

See the `full list of Squeeze packages
<https://packages.debian.org/squeeze/>`__ for all the (very old versions of)
software you can install.

.. _installing-python-from-source:

Installing Python from source
-----------------------------

We mostly covered this when we
:ref:`installed ujson on Python 3 <using-ujson-py3>`, but here it
is, for reference:

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
