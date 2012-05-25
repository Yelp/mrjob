mrjob
=====

**mrjob lets you write MapReduce jobs in Python 2.5+ and run them on several
platforms.** You can:

* Write multi-step MapReduce jobs in pure Python
* Test on your local machine
* Run on a Hadoop cluster
* Run in the cloud using `Amazon Elastic MapReduce (EMR)`_

.. _Amazon Elastic MapReduce (EMR): http://aws.amazon.com/documentation/elasticmapreduce/

To get started, install with ``pip``::

    pip install mrjob

Guides
======

**Basics**

* :doc:`guides/quickstart`
* :doc:`guides/concepts`

**Writing jobs**

* :ref:`writing-basics`
* :ref:`job-protocols`
* :ref:`writing-protocols`
* :ref:`writing-cl-opts`

**Running jobs**

* :doc:`guides/runners`
* :ref:`runners-programmatically`

**Configuration**

* :doc:`guides/configs-basics`
* Making files available to tasks
* Hadoop options
* Other options
* :doc:`guides/configs-reference`

**Cookbook**

* :ref:`cookbook-src-tree-pythonpath`
* :ref:`cookbook-task-timeout`
* :ref:`cookbook-compressed-output`

**Testing**

* :ref:`testing-anatomy`
* :ref:`testing-counters`

**Running jobs on Elastic MapReduce**

* :doc:`guides/emr-concepts`
* :ref:`amazon-setup`
* :ref:`ssh-tunneling`
* :doc:`guides/emr-opts`
* :doc:`guides/emr-tools`
* :doc:`guides/emr-troubleshooting`
* :doc:`guides/emr-advanced`

Reference
=========

.. toctree::
    :maxdepth: 2

    whats-new.rst
    reference.rst
    guides.rst

Indices and tables
==================

* :ref:`search`
