mrjob
=====

.. image:: logos/logo_medium.png

mrjob is a Python 2.5+ package that helps you write and run Hadoop Streaming
jobs.

mrjob fully supports Amazon's Elastic MapReduce (EMR) service, which allows you
to buy time on a Hadoop cluster on an hourly basis. It also works with your own
Hadoop cluster.

To get started, install with ``pip``::

    pip install mrjob

Then read :doc:`writing-and-running`. Other common documentation destinations
are:

* :ref:`mrjob.conf`
* :doc:`configs-reference`
* :doc:`job`
* :doc:`runners-emr`
* :doc:`tools`

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
