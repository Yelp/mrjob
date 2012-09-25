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
* :ref:`cmd-filters`
* :ref:`cmd-steps`

**Running jobs**

* :doc:`guides/runners`
* :ref:`runners-programmatically`
* :doc:`guides/cmd`
* :doc:`guides/testing`

**Configuration**

* :doc:`guides/configs-basics`
* :doc:`guides/configs-all-runners`
* :doc:`guides/configs-hadoopy-runners`
* :doc:`guides/configs-reference`

**Cookbook**

* :ref:`cookbook-src-tree-pythonpath`
* :ref:`cookbook-task-timeout`
* :ref:`cookbook-compressed-output`

**Running jobs on Elastic MapReduce**

* :ref:`amazon-setup`
* :ref:`ssh-tunneling`
* :ref:`running-an-emr-job`
* :ref:`picking-job-flow-config`
* :doc:`guides/emr-opts`
* :doc:`guides/emr-tools`
* :doc:`guides/emr-troubleshooting`
* :doc:`guides/emr-advanced`

**Working on mrjob**

* :doc:`guides/contributing`
* :doc:`guides/runner-job-interactions`

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
