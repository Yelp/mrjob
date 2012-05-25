mrjob
=====

**mrjob lets you write MapReduce jobs in Python 2.5+ and run them on several
platforms.** You can:

* Write multi-step MapReduce jobs in pure Python
* Test on your local machine
* Run on a Hadoop cluster
* Run in the cloud using `Amazon Elastic MapReduce (EMR)`_.

.. _Amazon Elastic MapReduce (EMR): http://aws.amazon.com/documentation/elasticmapreduce/

To get started, install with ``pip``::

    pip install mrjob

Guides
======

**Basics**

* :doc:`quickstart`
* :doc:`concepts`

**Writing jobs**

* :ref:`writing-basics`
* :ref:`job-protocols`,
* :ref:`writing-protocols`
* :ref:`writing-cl-opts`

**Running jobs**

* Runners
* Running jobs programmatically
* Making files available to tasks

**Configuration**

* Config file format and location
* Hadoop options
* Other options
* :doc:`configs-reference`

**Cookbook**

* Putting your source tree in PYTHONPATH
* Increasing task timeout
* Writing compressed output

**Testing**

* :ref:`testing-anatomy`
* :ref:`testing-counters`

**Running jobs on Elastic MapReduce**

* Concepts
* Configuring AWS credentials
* Configuring SSH credentials
* EMR runner options
* :doc:`tools`
* Troubleshooting
* Advanced EMR strategies

Reference
=========

.. toctree::
    :maxdepth: 3

    whats-new.rst
    writing-and-running.rst
    job.rst
    configs.rst
    protocols.rst
    runners.rst
    utils.rst
    tools.rst
    testing.rst

Indices and tables
==================

* :ref:`search`
