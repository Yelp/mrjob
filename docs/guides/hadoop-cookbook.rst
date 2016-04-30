Hadoop Cookbook
===============

.. _cookbook-task-timeout:

Increasing the task timeout
---------------------------

.. warning::

    Some EMR AMIs appear to not support setting parameters like
    timeout with :mrjob-opt:`jobconf` at run time. Instead, you must use
    :ref:`bootstrap-time-configuration`.

If your mappers or reducers take a long time to process a single step, you may
want to increase the amount of time Hadoop lets them run before failing them
as timeouts.

You can do this with :mrjob-opt:`jobconf`. For example, to set the timeout to
one hour:

.. code-block:: yaml

    runners:
      hadoop: # also works for emr runner
        jobconf:
          mapreduce.task.timeout: 3600000

.. note::

    If you're using Hadoop 1, which uses ``mapred.task.timeout``, don't worry:
    this example still works because mrjob auto-converts your
    :mrjob-opt:`jobconf` options between Hadoop versions.

.. _cookbook-compressed-output:

Writing compressed output
-------------------------

To save space, you can have Hadoop automatically save your job's output as
compressed files. Here's how you tell it to bzip them:

.. code-block:: yaml

    runners:
      hadoop: # also works for emr runner
        jobconf:
          # "true" must be a string argument, not a boolean! (Issue #323)
          mapreduce.output.fileoutputformat.compress: "true"
          mapreduce.output.fileoutputformat.compress.codec: org.apache.hadoop.io.compress.BZip2Codec


.. note::

   You could also gzip your files with
   ``org.apache.hadoop.io.compress.GzipCodec``. Usually bzip is a better
   option, as ``.bz2`` files are splittable, and ``.gz`` files are not. For
   example, if you use ``.gz`` files as input, Hadoop has no choice but to
   create one mapper per ``.gz`` file.
