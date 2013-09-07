Fundamentals
============

Installation
------------

Install with ``pip``::

    pip install mrjob

or from a `git`_ clone of the `source code`_::

    python setup.py test && python setup.py install

.. _`git`: http://www.git-scm.org/
.. _`source code`: http://www.github.com/yelp/mrjob

Writing your first job
----------------------

Open a file called :file:`word_count.py` and type this into it::

    from mrjob.job import MRJob


    class MRWordCounter(MRJob):

        def mapper(self, key, line):
            for word in line.split():
                yield word, 1

        def reducer(self, word, occurrences):
            yield word, sum(occurrences)


    if __name__ == '__main__':
        MRWordCounter.run()

Now go back to the command line, find your favorite body of text (such mrjob's
:file:`README.rst`, or even your new file :file:`word_count.py`), and try
this::

  $ python word_count.py my_file.txt

You should see something like this::

    "chars" 3654
    "lines" 123
    "words" 417

Congratulations! You've just written and run your first program with mrjob.

What's happening
^^^^^^^^^^^^^^^^

A job is defined by a class that inherits from :py:class:`~mrjob.job.MRJob`.
This class contains methods that define the :term:`steps` of your job.

A "step" consists of a mapper, a combiner, and a reducer. All of those are
optional, though you must have at least one. So you could have a step that's
just a mapper, or just a combiner and a reducer.

When you only have one step, all you have to do is write methods called
:py:meth:`~mrjob.job.MRJob.mapper`, :py:meth:`~mrjob.job.MRJob.combiner`, and
:py:meth:`~mrjob.job.MRJob.reducer`.

.. warning::

  Forgetting the following information will result in confusion.

The final required component of a job file is these two lines at the end of the
file, **every time**::

    if __name__ == '__main__':
        MRWordCounter.run()  # where MRWordCounter is your job class

These lines pass control over the command line arguments and execution to
mrjob. **Without them, your job will not work.** For more information, see
:ref:`hadoop-streaming-and-mrjob`.

Writing your second job
-----------------------

Most of the time, you'll need more than one step in your job. To define
multiple steps, override :py:meth:`~mrjob.job.MRJob.steps` and return a list of
steps constructed via :py:meth:`~mrjob.job.MRJob.mr`

Here's another way to write the job from the previous section::

    from mrjob.job import MRJob


    class MRWordCounter(MRJob):

        def get_words(self, key, line):
            for word in line.split():
                yield word, 1

        def sum_words(self, word, occurrences):
            yield word, sum(occurrences)

        def steps(self):
            return [self.mr(mapper=self.get_words, reducer=self.sum_words),]


    if __name__ == '__main__':
        MRWordCounter.run()

Running your job different ways
-------------------------------

The most basic way to run your job is on the command line::

  $ python my_job.py input.txt

By default, output will be written to stdout.

You can pass input via stdin, but be aware that mrjob will just dump it to a
file first::

  $ python my_job.py < input.txt

You can pass multiple input files, mixed with stdin (using the ``-``
character)::

  $ python my_job.py input1.txt input2.txt - < input3.txt

By default, mrjob will run your job in a single Python process. This provides
the friendliest debugging experience, but it's not exactly distributed
computing!

You change the way the job is run with the ``-r``/``--runner`` option. You can
use ``-r inline`` (the default), ``-r local``, ``-r hadoop``, or ``-r emr``.

To run your job in multiple subprocesses with a few Hadoop features simulated,
use ``-r local``.

To run it on your Hadoop cluster, use ``-r hadoop``.

If you have Elastic MapReduce configured (see :doc:`emr-quickstart`), you can
run it there with ``-r emr``.

Your input files can come from HDFS if you're using Hadoop, or S3 if you're
using EMR::

  $ python my_job.py -r emr s3://my-inputs/input.txt
  $ python my_job.py -r hadoop hdfs://my_home/input.txt

If your code spans multiple files, see :ref:`cookbook-src-tree-pythonpath`.

Configuration
-------------

mrjob has an overflowing cornucopia of configuration options. You'll want to
specify some on the command line, some in a config file.

You can put a config file at ``/etc/mrjob.conf``, ``~/.mrjob.conf``, or
``./mrjob.conf`` for mrjob to find it without passing it via ``--conf-path``.

Config files are interpreted as YAML if you have the :py:mod:`yaml` module
installed. Otherwise, they are interpreted as JSON.

See :doc:`configs-basics` for in-depth information. Here is an example file::

  runners:
    emr:
      aws-region: us-west-1
      python_archives:
        - a_library_I_use_on_emr.tar.gz
    inline:
      base_tmp_dir: $HOME/.tmp
