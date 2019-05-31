=====
Spark
=====

.. _why-mrjob-with-spark:

Why use mrjob with Spark?
=========================

mrjob augments `Spark <http://spark.apache.org/>`__\'s native Python support
with the following features familiar to users of mrjob:

* automatically upload input and other support files to HDFS, GCS, or S3
  (see :mrjob-opt:`upload_files`, :mrjob-opt:`upload_archives`,
  and :mrjob-opt:`py_files`)
* run :command:`make` and other command before running Spark
  tasks (see :mrjob-opt:`setup`).
* passthrough and file arguments (see :ref:`writing-cl-opts`)
* automatically parse logs to explain errors and other Spark job failures
* easily pass through environment variables (see :mrjob-opt:`cmdenv`)
* support for :mrjob-opt:`libjars`
* automatic matching of Python version (see :mrjob-opt:`python_bin`)
* automatically set up Spark on EMR (see :mrjob-opt:`bootstrap_spark`)
* automatically making the mrjob library available to your job
  (see :mrjob-opt:`bootstrap_mrjob`)

mrjob spark-submit
==================

.. versionadded: 0.6.7

If you already have a Spark script written, the easiest way to access mrjob's
features is to run your job with :command:`mrjob spark-submit`, just like you
would normally run it with :command:`spark-submit`. This can, for instance,
make running a Spark job on EMR as easy as running it locally, or allow
you to access features (e.g. :mrjob-opt:`setup`) not natively supported by
Spark.

For more details, see :ref:`mrjob spark-submit <spark-submit>`.

Writing your first Spark MRJob
==============================

Another way to integrate mrjob with Spark is to add a
:py:meth:`~mrjob.job.MRJob.spark` method to your :py:class:`~mrjob.job.MRJob`
class, and put your Spark code inside it. This will allow you to access
features only availble to MRJobs (e.g. :py:attr:`~mrjob.job.MRJob.FILES`).

Here's how you'd implement a word frequency count job in Spark::

  import re
  from operator import add

  from mrjob.job import MRJob

  WORD_RE = re.compile(r"[\w']+")


  class MRSparkWordcount(MRJob):

      def spark(self, input_path, output_path):
          # Spark may not be available where script is launched
          from pyspark import SparkContext

          sc = SparkContext(appName='mrjob Spark wordcount script')

          lines = sc.textFile(input_path)

          counts = (
              lines.flatMap(self.get_words)
              .map(lambda word: (word, 1))
              .reduceByKey(add))

          counts.saveAsTextFile(output_path)

          sc.stop()

      def get_words(self, line):
          return WORD_RE.findall(line)


  if __name__ == '__main__':
      MRSparkWordcount.run()

Since Spark already supports Python, mrjob takes care of setting up your
cluster, passes in input and output paths, and otherwise gets out of the way.
If you pass in multiple input paths, *input_path* will be these paths joined
by a comma (:py:meth:`SparkContext.textFile` will accept this).

Note that :py:mod:`pyspark` is imported *inside* the
:py:meth:`~mrjob.job.MRJob.spark` method. This allows your job to run whether
:py:mod:`pyspark` is installed locally or not.

The :py:meth:`~mrjob.job.MRJob.spark` method can be used to execute arbitrary
code, so there's nothing stopping you from using *SparkSession* instead of
*SparkContext* in Spark 2, or writing a streaming-mode job rather than a
batch one.

.. warning::

   Prior to v0.6.8, to pass job methods into Spark
   (e.g. ``rdd.flatMap(self.get_words)``), you first had to call
   :py:meth:`self.sandbox() <mrjob.job.MRJob.sandbox>`; otherwise
   Spark would error because *self* was not serializable.

.. _running-on-your-spark-cluster:

Running on your Spark cluster
=============================

By default, mrjob runs your job on the inline runner (see below).
If you want to run your job on your own Spark cluster, run it with
``-r spark``:

.. code-block sh::

   python mr_spark_wordcount.py -r spark input.txt

Use ``--spark-master`` (see :mrjob-opt:`spark_master`) to control where your
job runs.

You can pass in spark options with ``-D`` (see :mrjob-opt:`jobconf`) and
set deploy mode (client or cluster) with ``--spark-deploy-mode``. If you need
to pass other arguments to :command:`spark-submit`, use
:mrjob-opt:`spark_args`.

The Spark runner can also run "classic" MRJobs (i.e. those made
by defining :py:meth:`~mrjob.job.MRJob.mapper` etc. or with
:py:class:`~mrjob.step.MRStep`\s) directly on Spark, allowing you to move
off Hadoop without rewriting your jobs. See
:ref:`below <classic-mrjobs-on-spark>` for details.

.. warning::

   If you don't set :mrjob-opt:`spark_master`, your job will run on Spark's
   default ``local[*]`` master, which can't handle :mrjob-opt:`setup` scripts
   or ``--files`` because it doesn't give tasks their own working directory.

.. note::

   mrjob needs to know what master and deploy mode you're using, so it will
   override attempts to set spark master or deploy mode through
   :mrjob-opt:`jobconf` (e.g. ``-D spark.master=...``).

Using remote filesystems other than HDFS
========================================

By default, if you use a remote Spark master (i.e. not ``local`` or
``local-cluster``), Spark will assume you want to use HDFS for your job's
temp space, and that you will want to access it through :command:`hadoop fs`.

Some Spark installations don't use HDFS at all. Fortunately, the Spark runner
also supports S3 and GCS. Use :mrjob-opt:`spark_tmp_dir` to specify a remote
temp directory not on HDFS (e.g. ``--spark-tmp-dir s3a://bucket/path``).

For more information on accessing S3 or GCS, see :ref:`amazon-setup` (S3)
or :ref:`google-credentials-setup` (GCS).

.. _other-ways-to-run-on-spark:

Other ways to run on Spark
==========================

Inline runner
-------------

Running your Spark job with ``-r inline`` (the default) will launch it
directly through the :mod:`pyspark` library, effectively running it on the
``local[*]`` master. This is convenient for debugging because exceptions will
bubble up directly to your Python process.

The inline runner also builds a simulated working directory for your job,
making it possible to test scripts that rely on certain files being in the
working directory (it doesn't run :mrjob-opt:`setup` scripts).

.. note::

   If you don't have a local Spark installation, the pyspark library
   on PyPI is a pretty quick way to get one (``pip install pyspark``).

Local runner
------------

Running your Spark job with ``-r local`` will launch it through
:command:`spark-submit` on a ``local-cluster`` master. ``local-cluster``
is designed to simulate a real Spark cluster, so :mrjob-opt:`setup`
will work as expected.

By default, the local runner launches Spark jobs with as many executors
as your system has CPUs. Use ``--num-cores`` (see :mrjob-opt:`num_cores`
to change this).

By default, the local runner gives each executor 1 GB of memory. If you need
more, you can specify it through jobconf, e.g. ``-D spark.core.memory=4g``.

EMR runner
----------

Running your Spark job with ``-r emr`` will launch it in Amazon Elastic
MapReduce (EMR), with the same seamless integration and features mrjob provides
for Hadoop jobs on EMR.

The EMR runner will always run your job on the ``yarn`` Spark master in
``cluster`` deploy mode.

Hadoop runner
-------------

Running your Spark job with ``-r hadoop`` will launch it on your own Hadoop
cluster. This is not significantly different than the Spark runner. The main
advantage of the Hadoop runner is that is has more knowledge about how to find
logs and can be better at finding the relevant error if your job fails.

Unlike the Spark runner, the Hadoop runner's default spark master is ``yarn``.

.. note::

   mrjob does not yet support Spark on Google Cloud Dataproc.

Passing in libraries
====================

Use ``--py-files`` to pass in ``.zip`` or ``.egg`` files full of Python code::

  python your_mr_spark_job -r hadoop --py-files lib1.zip,lib2.egg

Or set :mrjob-opt:`py_files` in ``mrjob.conf``.

Command-line options
====================

Command-line options (passthrough options, etc) work exactly like they
do with regular streaming jobs (even :py:meth:`~mrjob.job.MRJob.add_file_arg`
on the ``local[*]`` Spark master. See :ref:`writing-cl-opts`.

Uploading files to the working directory
========================================

:mrjob-opt:`upload_files`, :py:attr:`~mrjob.job.MRJob.FILES`, and files
uploaded via :mrjob-opt:`setup` scripts all should work as expected (except
on ``local`` masters because there is no working directory).

Note that you can give files a different name in the working directory
(e.g. ``--file foo#bar``) on all Spark masters, even though Spark treats
that as a YARN-specific feature.

Archives and directories
========================

Spark treats ``--archives`` as a YARN-specific feature. This means that
:mrjob-opt:`upload_archives`, :py:attr:`~mrjob.job.MRJob.ARCHIVES`,
:py:attr:`~mrjob.job.MRJob.DIRS`, etc. will be ignored on non-``yarn``
Spark masters.

Future versions of mrjob may simulate archives on non-``yarn`` masters
using a :mrjob-opt:`setup` script.

Multi-step jobs
===============

There generally isn't a need to define multiple Spark steps (Spark lets
you map/reduce as many times as you want). However, it may sometimes be useful
to pre- or post-process Spark data using a
:py:class:`streaming <mrjob.step.MRStep>` or
:py:class:`jar <mrjob.step.JarStep>` step.

This is accomplished by overriding your job's :py:meth:`~mrjob.job.MRJob.steps`
method and using the :py:class:`~mrjob.step.SparkStep` class::

  def steps():
      return [
          MRStep(mapper=self.preprocessing_mapper),
          SparkStep(spark=self.spark),
      ]

External Spark scripts
======================

mrjob can also be used to launch external (non-mrjob) Spark scripts using
the :py:class:`~mrjob.step.SparkScriptStep` class, which specifies the
path (or URI) of the script and its arguments.

As with :py:class:`~mrjob.step.JarStep`\s, you can interpolate input
and output paths using :py:data:`~mrjob.step.INPUT` and
:py:data:`~mrjob.step.OUTPUT` constants. For example, you could set your job's
:py:meth:`~mrjob.job.MRJob.steps` method up like this::

  def steps():
      return [
          SparkScriptStep(
             script=os.path.join(
                 os.path.dirname(__file__), 'my_spark_script.py'),
             args=[INPUT, '-o', OUTPUT, '--other-switch'],
          ),
      ]

Custom input and output formats
===============================

mrjob allows you to use input and output formats from custom JARs with Spark,
just like you can :ref:`with streaming jobs <input-and-output-formats>`.

First `download your JAR <https://github.com/empiricalresults/nicknack/releases/download/v1.0.0/nicknack-1.0.0.jar>`__
to the same directory as your job, and add it to your job class with the
:py:attr:`~mrjob.job.MRJob.LIBJARS` attribute::

  LIBJARS = ['nicknack-1.0.0.jar']

Then use Spark's own capabilities to reference your input or output format,
keeping in mind the data types they expect.

For example, nicknack's ``MultipleValueOutputFormat`` expects ``<Text,Text>``,
so if we wanted to integrate it with our wordcount example, we'd have to
convert the count to a string::

  def spark(self, input_path, output_path):
      from pyspark import SparkContext

      sc = SparkContext(appName='mrjob Spark wordcount script')

      lines = sc.textFile(input_path)

      counts = (
          lines.flatMap(self.get_words
          .map(lambda word: (word, 1))
          .reduceByKey(add))

      # MultipleValueOutputFormat expects Text, Text
      # w_c is (word, count)
      counts = counts.map(lambda w_c: (w_c[0], str(w_c[1])))

      counts.saveAsHadoopFile(output_path,
                              'nicknack.MultipleValueOutputFormat')

      sc.stop()

.. _classic-mrjobs-on-spark:

Running "classic" MRJobs on Spark
=================================

The Spark runner provides near-total support for running "classic"
:py:class:`~mrjob.job.MRJob`\s (the sort described in
:ref:`writing-your-first-job` and :ref:`writing-your-second-job`)
directly on any Spark installation, even
though these jobs were originally designed to run on Hadoop Streaming.
Support includes:

 * :py:meth:`*_init() <mrjob.job.MRJob.mapper_init>` and
   :py:meth:`*_final() <mrjob.job.MRJob.mapper_final>` methods
 * :py:attr:`~mrjob.job.MRJob.HADOOP_INPUT_FORMAT` and
   :py:attr:`~mrjob.job.MRJob.HADOOP_OUTPUT_FORMAT`
 * :py:attr:`~mrjob.job.MRJob.SORT_VALUES`
 * :ref:`passthrough arguments <writing-cl-opts>`
 * :py:meth:`~mrjob.job.MRJob.increment_counter`

Jobs will often run more quickly on Spark than Hadoop Streaming, so it's worth
trying even if you don't plan to move off Hadoop in the forseeable future.

Multiple steps are run as a single job
--------------------------------------

If you have a job with multiple consecutive :py:class:`~mrjob.step.MRStep`\s,
the Spark runner will run them all as a single Spark job. This is usually what
you want (more efficient), but it can make debugging slightly more
challenging (step failure exceptions
give a range of steps, no way to access intermediate data).

To force the Spark runner to run steps separately, you can initialize
each :py:class:`~mrjob.step.MRStep` with a different ``jobconf``
dictionary.

No support for subprocesses
---------------------------

Pre-filters (e.g. :py:meth:`~mrjob.job.MRJob.mapper_pre_filter`) and
command steps (e.g. :py:meth:`~mrjob.job.MRJob.reducer_cmd`) are not
supported because they require launching subprocesses.

It wouldn't be *impossible* to emulate this inside Spark, but then we'd
essentially be turning Spark *into* Hadoop Streaming. (If you have a use case
for this seemingly implausible feature, let us know
`through GitHub <https://github.com/Yelp/mrjob/issues>`_.)

Spark loves combiners
---------------------

Hadoop's "reduce" paradigm is a lot more heavyweight than Spark's; whereas a
Spark reducer just wants to know how to combine two values into one, a Hadoop
reducer expects to be able to see all the values for a given key, and to emit
zero or more key-value pairs.

In fact, Spark reducers are a lot more like Hadoop combiners. The Spark runner
knows how to translate something like::

  def combiner(self, key, values):
      yield key, sum(values)

into Spark's reduce paradigm--basically it'll pass your combiner two values
at a time, and hope it emits one. If your combiner does *not* behave like a
Spark reducer function (emitting multiple or zero values), the Spark runner
handles that gracefully as well.

Counter emulation is *almost* perfect
-------------------------------------

Counters (see :py:meth:`~mrjob.job.MRJob.increment_counter`) are a feature
specific to Hadoop. mrjob emulates them on Spark anyway. If you have a
multi-step job, mrjob will dutifully print out counters for each step and
make them available through :py:meth:`~mrjob.runner.MRJobRunner.counters`.

The only drawback is that while Hadoop has the ability to "take back" counters
produced by a failed task, there isn't a clean way to do this with Spark
accumulators. Therefore, the counters produced by the Spark runner's
Hadoop emulation may be overestimates.

Spark does not stream data
--------------------------

While Hadoop streaming (as its name implies) passes a stream of data to your
job, Spark instead operates on *partitions*, which are loaded into memory.

A reducer like this can't run out of memory on Hadoop streaming, no matter
how many values there are for *key*::

  def reducer(self, key, values):
      yield key, sum(values)

However, on Spark, simply storing the partition that contains these values
can cause Spark to run out of memory.

If this happens, you can let Spark use more memory
(``-D spark.executor.memory=10g``) or add a combiner to your job.

Compression emulation
---------------------

It's fairly common for people to request compressed output from Hadoop via
configuration properties, for example:

.. code-block:: sh

   python mr_your_job.py -D mapreduce.output.fileoutputformat.compress=true -D\
    mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.BZip2Codec ...

This works with ``-r spark`` too; the Spark runner knows how to recognize these
properties and pass the codec specified to Spark when it writes output.

Spark won't split .gz files either
----------------------------------

A common trick on Hadoop to ensure that segments of your data don't get split
between mappers is to gzip each segment (since .gz is not a seekable
compression format).

This works on Spark as well.

Controlling number of output files
----------------------------------

By default, Spark will write one output file per partition. This may give more
output files than you expect, since Hadoop and Spark are tuned differently.

The Spark runner knows how to emulate the Hadoop configuration property that
sets number of reducers on Hadoop (e.g. ``-D mapreduce.job.reduces=100``),
which will control the number of output files (assuming your last step has a
reducer).

However, this is a somewhat heavyweight solution; once Spark runs a step's
reducer, mrjob has to forbid Spark from re-partitioning until the end
of the step.

A lighter weight solution is ``--max-output-files``, allows you to limit
the number of output files by running ``coalesce()`` just before
writing output. Running your job with ``--max-output-files=100`` would ensure
it produces no more than 100 output files (but it could output less).
