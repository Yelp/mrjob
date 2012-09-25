Quickstart
==========

Installation
------------

Install with ``pip``::

    pip install mrjob

or from a `git`_ clone of the `source code`_::

    python setup.py test && python setup.py install

.. _`git`: http://www.git-scm.org/
.. _`source code`: http://www.github.com/yelp/mrjob

Writing a job
-------------

A job consists of an :py:class:`~mrjob.job.MRJob` subclass, one or more mapper,
combiner, or reducer methods, a call at the bottom to invoke mrjob's logic, and
all associated dependencies.

For jobs that consist only of one mapper, combiner, and reducer, in that order,
you can simply override one or more of the :py:meth:`~mrjob.job.MRJob.mapper`,
:py:meth:`~mrjob.job.MRJob.combiner`, and :py:meth:`~mrjob.job.MRJob.reducer`
methods::

    from mrjob.job import MRJob


    class MRWordCounter(MRJob):

        def mapper(self, key, line):
            for word in line.split():
                yield word, 1

        def reducer(self, word, occurrences):
            yield word, sum(occurrences)


    if __name__ == '__main__':
        MRWordCounter.run()

You may leave out any of the mapper, combiner, or reducer, and mrjob will use
an identity function in its place.

The ``if __name__ == '__main__':`` block is necessary for the ``local``,
``hadoop``, and ``emr`` runners. See :doc:`concepts` for more information.

To define multiple steps, override :py:meth:`~mrjob.job.MRJob.steps` and return
steps constructed via :py:meth:`~mrjob.job.MRJob.mr`::

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

Running a job
-------------

Jobs can be directly executed from the command line. All jobs can use files or
stdin as the job input. Output is written to stdout.

By default, your job will run in a single process::

    > python word_count.py README.txt
    "chars" 3654
    "lines" 123
    "words" 417

You can use ``-r local`` to run your job in a subprocess with a few Hadoop
features simulated. If you have configured mrjob to use Hadoop, you can use
``-r hadoop`` to run your job on the Hadoop cluster. If you have configured AWS
credentials, you can use ``-r emr`` to run your job on EMR (see
:doc:`emr-quickstart` for more information).

If your code spans multiple files, see :ref:`cookbook-src-tree-pythonpath`.

Configuration
-------------

You can put a config file at ``/etc/mrjob.conf``, ``~/.mrjob.conf``, or
``./mrjob.conf`` for mrjob to find it without passing it via ``--conf-path``.
Here is an example file:

.. include:: ../../mrjob.conf.example
    :literal:
