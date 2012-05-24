mrjob
=====

.. image:: logos/logo_medium.png

**mrjob lets you write MapReduce jobs in Python 2.5+ and run them on several
platforms.** You can:

* Write multi-step MapReduce jobs in pure Python
* Test on your local machine
* Run on a Hadoop cluster
* Run in the cloud using `Amazon Elastic MapReduce (EMR)`_.

.. _Amazon Elastic MapReduce (EMR): http://aws.amazon.com/documentation/elasticmapreduce/

To get started, install with ``pip``::

    pip install mrjob

Then read :doc:`writing-and-running`. Other common documentation destinations
are:

* :ref:`mrjob.conf`
* :doc:`configs-reference`
* :doc:`job`
* :doc:`runners-emr`
* :doc:`tools`

Guides
======

* :doc:`quickstart`
* :doc:`concepts`

Writing MRJobs
--------------

* :ref:`writing-basics`
* :ref:`using-protocols`
* :ref:`writing-protocols`
* :ref:`writing-cl-opts`

Running MRJobs
--------------

* **Runners**
* **Running jobs programmatically**
* **Making files available to tasks**

Configuration
-------------

* **Config file format and location**
* **Hadoop options**
* **Other options**

Cookbook
--------

* **Putting your source tree in PYTHONPATH**
* **Increasing task timeout**
* **Writing compressed output**

Testing
-------

* **Anatomy of a test case**
* **Counters**

Running MRJobs on Elastic MapReduce
-----------------------------------

* **Concepts**
* **Configuring AWS credentials**
* **Configuring SSH credentials**
* **EMR runner options**
* **Tools**
* **Troubleshooting**
* **Advanced EMR strategies**

Table of Contents
=================

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
