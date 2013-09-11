Contributing to mrjob
=====================

Contribution guidelines
-----------------------

mrjob is developed using a standard Github pull request process. Almost all
code is reviewed in pull requests.

The general process for working on mrjob is:

* `Fork the project on Github`_
* Clone your fork to your local machine
* Create a feature branch from master (e.g. ``git branch delete_all_the_code``)
* Write code, commit often
* Write test cases for all changed functionality
* Submit a pull request against ``master`` on Github
* Wait for code review!

It would also help to discuss your ideas on the `mailing list`_ so we can warn
you of possible merge conflicts with ongoing work or offer suggestions for
where to put code.

.. _`mailing list`: http://groups.google.com/group/mrjob
.. _`Fork the project on Github`: http://www.github.com/Yelp/mrjob

Things that will make your branch more likely to be pulled:

* Comprehensive, fast test cases
* Detailed explanation of what the change is and how it works
* Reference relevant issue numbers in the tracker
* API backward compatibility

If you add a new configuration option, please try to do all of these things:

* Make its name unambiguous in the context of multiple runners (e.g.
  ``ec2_task_instance_type`` instead of ``instance_type``)
* Add command line switches that allow full control over the option
* Document the option and its switches in the appropriate file under ``docs``

A quick tour through the code
-----------------------------

mrjob's modules can be put in four categories:

* Reading command line arguments and config files, and invoking machinery
  accordingly

  * :py:mod:`mrjob.conf`: Read config files

  * :py:mod:`mrjob.launch`: Invoke runners based on command line and configs

  * :py:mod:`mrjob.options`: Define command line options

* Interacting with Hadoop Streaming

  * :py:mod:`mrjob.job`: Python interface for writing jobs

  * :py:mod:`mrjob.protocol`: Defining data formats between Python steps

* Runners and support; submitting the job to various MapReduce environments

  * :py:mod:`mrjob.runner`: Common functionality across runners

  * :py:mod:`mrjob.hadoop`: Submit jobs to Hadoop

  * :py:mod:`mrjob.step`: Define/implement interface between runners and
    script steps

  * Local

    * :py:mod:`mrjob.inline`: Run Python-only jobs in-process

    * :py:mod:`mrjob.local`: Run Hadoop Streaming-only jobs in subprocesses

  * Amazon Elastic MapReduce

    * :py:mod:`mrjob.emr`: Submit jobs to EMR

    * :py:mod:`mrjob.pool`: Utilities for job flow pooling functionality

    * :py:mod:`mrjob.retry`: Wrapper for S3 and EMR connections to handle
      recoverable errors

    * :py:mod:`mrjob.ssh`: Run commands on EMR cluster machines

* Interacting with different "filesystems"

  * :py:mod:`mrjob.fs.base`: Common functionality

  * :py:mod:`mrjob.fs.composite`: Support multiple filesystems; if one fails,
    "fall through" to another

  * :py:mod:`mrjob.fs.hadoop`: HDFS

  * :py:mod:`mrjob.fs.local`: Local filesystem

  * :py:mod:`mrjob.fs.s3`: S3

  * :py:mod:`mrjob.fs.ssh`: SSH

* Utilities

  * :py:mod:`mrjob.compat`: Transparently handle differences between Hadoop
    versions

  * :py:mod:`mrjob.logparsers`: Find interesting information (errors,
    counters) in Hadoop logs (used by ``hadoop`` and ``emr`` runners)

  * :py:mod:`mrjob.parse`: Parsing utilities for URIs, logs, command line
    options, etc.

  * :py:mod:`mrjob.util`: Utilities for dealing with files, command line
    options, various other things
