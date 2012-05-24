Writing MRJobs
==============

.. _writing-basics:

Basics
------

Your job will be defined in a file to be executed on your machine as a Python
script, as well as on a Hadoop cluster as an individual map, combine, or reduce
task. All dependencies must either be contained within the file, made available
on the task nodes before the job is run, or uploaded to the cluster by mrjob
when your job is submitted.

The simplest way to write a job is by overriding :py:class:`~mrjob.job.MRJob`'s
':py:meth:`~mrjob.job.MRJob.mapper`, :py:meth:`~mrjob.job.MRJob.combiner`, and
:py:meth:`~mrjob.job.MRJob.reducer` methods::

    from mrjob.job import MRJob
    import re

    WORD_RE = re.compile(r"[\w']+")


    class MRWordFreqCount(MRJob):

        def mapper(self, _, line):
            for word in WORD_RE.findall(line):
                yield (word.lower(), 1)

        def combiner(self, word, counts):
            yield (word, sum(counts))

        def reducer(self, word, counts):
            yield (word, sum(counts))


    if __name__ == '__main__':
        MRWordFreqCount.run()

The default configuration sends input lines to mappers via the value parameter
as a string object, with ``None`` for the key, so the mapper method above
discards the key and operates only on the value. The mapper yields ``(word,
1)`` for each word. The key and value are converted to `JSON`_ for transmission
between tasks and for final output.

.. _`JSON`: http://www.json.org/

The combiner and reducer get a word as the key and an iterator of numbers as
the value. They simply yield the word and the sum of the values.

The final output of the job is a set of lines where each line is a
tab-delimited key-value pair. Each key and value has been converted from its
Python representation to a JSON representation.

::

    "all"   1
    "and"   4
    "bus"   2
    ...

Many jobs require multiple steps. To define multiple steps, override the
:py:meth:`~mrjob.job.MRJob.steps` method::


    class MRDoubleWordFreqCount(MRJob):
        """Word frequency count job with an extra step to double all the
        values"""

        def get_words(self, _, line):
            for word in WORD_RE.findall(line):
                yield (word.lower(), 1)

        def sum_words(self, word, counts):
            yield (word, sum(counts))

        def double_counts(self, word, counts):
            yield word, counts * 2

        def steps(self):
            return [self.mr(mapper=self.get_words,
                            combiner=self.sum_words,
                            reducer=self.sum_words),
                    self.mr(mapper=self.double_counts)]


You may wish to set up or tear down resources for each task. You can do so with
``init`` and ``final`` methods. For one-step jobs, you can override these:

    * :py:meth:`~mrjob.job.MRJob.mapper_init`
    * :py:meth:`~mrjob.job.MRJob.mapper_final`
    * :py:meth:`~mrjob.job.MRJob.combiner_init`
    * :py:meth:`~mrjob.job.MRJob.combiner_final`
    * :py:meth:`~mrjob.job.MRJob.reducer_init`
    * :py:meth:`~mrjob.job.MRJob.reducer_final`

For multi-step jobs, use keyword arguments to the :py:meth:`mrjob.job.MRJob.mr`
function.

``init`` and ``final`` methods can yield values just like normal tasks. Here is
our word frequency count example rewritten to use ``init`` and ``final``
methods::


    class MRWordFreqCount(MRJob):

        def init_get_words(self):
            self.words = {}

        def get_words(self, _, line):
            for word in WORD_RE.findall(line):
                word = word.lower()
                self.words.setdefault(word, 0)
                self.words[word] = self.words[word] + 1

        def final_get_words(self):
            for word, val in self.words.iteritems():
                yield (word, val)

        def sum_words(self, word, counts):
            yield (word, sum(counts))

        def steps(self):
            return [self.mr(mapper_init=self.init_get_words,
                            mapper=self.get_words,
                            mapper_final=self.final_get_words,
                            combiner=self.sum_words,
                            reducer=self.sum_words)]

In this version, instead of yielding one line per word, the mapper yields one
line per unique word in an input line. Fewer lines will then be sent to the
combiner, but lines with many unique words (say, a trillion) would probably
cause the task to run out of memory and crash.

.. _writing-protocols:

Protocols
---------

.. _writing-cl-opts:

Defining command line options
-----------------------------

