Spark
=====

.. _why-mrjob-with-spark:

Why use mrjob with Spark?
-------------------------

mrjob augments `Spark <http://spark.apache.org/>`__\'s native Python support with
the following features familiar to users of mrjob:

* automatically parse logs to explain errors and other Spark job failures
* automatic matching of Python version (see :mrjob-opt:`python_bin`)
* easily pass through environment variables (see :mrjob-opt:`cmdenv`)
* support for :mrjob-opt:`libjars`
* passthrough and file options (see :ref:`writing-cl-opts`)
* automatically upload input and other support files to HDFS or S3 (see :mrjob-opt:`upload_files`, :mrjob-opt:`upload_archives`, and :mrjob-opt:`py_files`)
* automatically run :command:`make` and other command before running Spark tasks (see :mrjob-opt:`setup`).
* automatically set up Spark on EMR (see :mrjob-opt:`bootstrap_spark`)
* automatically making the mrjob library available to your job
  (see :mrjob-opt:`bootstrap_mrjob`)

.. note::

   mrjob does not yet support Spark on Google Cloud Dataproc.

mrjob spark-submit
------------------

.. versionadded: 0.6.7

If you already have a Spark script written, the easiest way to access mrjob's
features is to run your job with :command:`mrjob spark-submit`, just like you
would normally run it with :command:`spark-submit`. This can, for instance,
make running a Spark job on EMR as easy as running it locally, or allow
you to access features (e.g. :mrjob-opt:`setup`) not natively supported by
Spark.

For more details, see :ref:`mrjob spark-submit <spark-submit>`.

Writing your first Spark MRJob
------------------------------

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
              lines.flatMap(lambda line: WORD_RE.findall(line))
              .map(lambda word: (word, 1))
              .reduceByKey(add))

          counts.saveAsTextFile(output_path)

          sc.stop()


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

Running on your own Hadoop cluster
----------------------------------

Run your script with ``-r hadoop``::

  python your_mr_spark_job -r hadoop input_file1 input_file2 > output

There isn't currently a "local" or "inline" mode that works independently
from Spark, but you can use the :mrjob-opt:`spark_master` option to run in
Spark's local mode::

  python your_mr_spark_job -r hadoop --spark-master local input > output

The Hadoop runner always submits jobs to Spark in ``client`` mode, though
you could change this using the :mrjob-opt:`spark_args` option.

Also, note that if you set the Spark master to anything but ``yarn``
(the default), Spark will ignore archive files (see
:mrjob-opt:`upload_archives`).

Running on EMR
--------------

Run your script with ``-r emr``::

  python your_mr_spark_job -r emr input_file1 input_file2 > output

The default EMR image should work fine for most Spark 1 jobs.

If you want to run on Spark 2, please set :mrjob-opt:`image_version` to
5.0.0 or higher::

  python your_mr_spark2_job -r emr --image-version 5.0.0 input > output

EMR introduced Spark support in AMI version 3.8.0, but it's not recommended
to use the 3.x AMIs if you can avoid; they only support Python 2
and have trouble detecting when Spark jobs fail (instead silently producing
no output).

The EMR runner always submits jobs to Spark in ``cluster`` mode, which it needs
to access files on S3.

Passing in libraries
--------------------

Use ``--py-files`` to pass in ``.zip`` or ``.egg`` files full of Python code::

  python your_mr_spark_job -r hadoop --py-files lib1.zip,lib2.egg

Or set :mrjob-opt:`py_files` in ``mrjob.conf``.

Command-line options
--------------------

Command-line options (passthrough options, etc.) work exactly like they
do with regular streaming jobs. See :ref:`writing-cl-opts`.

No setup scripts
----------------

Unlike with streaming jobs, you can't wrap Spark jobs in
:doc:`setup scripts <setup-cookbook>`;
once Spark starts operating on serialized data, it's operating in pure
Python/Java and there's not a way to slip in a shell script.

If you're running in EMR, you can use
:doc:`bootstrap scripts <emr-bootstrap-cookbook>` to set up your
environment when the cluster is created.

Multi-step jobs
---------------

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
----------------------

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
        script=os.path.join(os.path.dirname(__file__), 'my_spark_script.py'),
        args=[INPUT, '-o', OUTPUT, '--other-switch'],
      ),
    ]

Custom input and output formats
-------------------------------

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
        lines.flatMap(lambda line: WORD_RE.findall(line))
        .map(lambda word: (word, 1))
        .reduceByKey(add))

    # MultipleValueOutputFormat expects Text, Text
    # w_c is (word, count)
    counts = counts.map(lambda w_c: (w_c[0], str(w_c[1])))

    counts.saveAsHadoopFile(output_path,
                            'nicknack.MultipleValueOutputFormat')

    sc.stop()
