Hadoop Cookbook
===============

.. _cookbook-task-timeout:

Increasing the task timeout
---------------------------

.. warning::

    Some EMR AMIs appear to not support setting parameters like
    timeout with ``jobconf`` at run time. Instead, you must use
    :ref:`bootstrap-time-configuration`.

If your mappers or reducers take a long time to process a single step, you may
want to increase the amount of time Hadoop lets them run before failing them
as timeouts. You can do this with ``jobconf`` and the version-appropriate
Hadoop environment variable. For example, this configuration will set the
timeout to one hour:

.. code-block:: yaml

    runners:
      hadoop: # this will work for both hadoop and emr
        jobconf:
          # Hadoop 0.18
          mapred.task.timeout: 3600000
          # Hadoop 0.21+
          mapreduce.task.timeout: 3600000

mrjob will convert your ``jobconf`` options between Hadoop versions if
necessary. In this example, either ``jobconf`` line could be removed and the
timeout would still be changed when using either version of Hadoop.

.. _cookbook-compressed-output:

Writing compressed output
-------------------------

To save space, you can have Hadoop automatically save your job's output as
compressed files. This can be done using the same method as changing the task
timeout, with ``jobconf`` and the appropriate environment variables:

.. code-block:: yaml

    runners:
      hadoop: # this will work for old hadoop versions
        jobconf:
          # "true" must be a string argument, not a boolean! (#323)
          mapreduce.output.compress: "true"
          mapreduce.output.compression.codec: org.apache.hadoop.io.compress.BZip2Codec

For Hadoop 2.0 and the current default EMR Hadoop version.

.. code-block:: yaml

    runners:
      hadoop: # or emr if appropriate
        jobconf:
          # "true" must be a string argument, not a boolean! (#323)
          mapreduce.output.fileoutputformat.compress: "true"
          mapreduce.output.fileoutputformat.compress.codec: org.apache.hadoop.io.compress.BZip2Codec


