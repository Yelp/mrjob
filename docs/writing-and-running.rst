Getting started
===============

Writing and running a job
-------------------------

To create your own map reduce job, subclass :py:class:`MRJob`, create a
series of mappers and reducers, and override :py:meth:`~mrjob.job.MRJob.steps`. For example, a word counter::

    from mrjob.job import MRJob

    class MRWordCounter(MRJob):
        def get_words(self, key, line):
            for word in line.split():
                yield word, 1

        def sum_words(self, word, occurrences):
            yield word, sum(occurrences)

        def steps(self):
            return [self.mr(self.get_words, self.sum_words),]

    if __name__ == '__main__':
        MRWordCounter.run()

The two lines at the bottom are mandatory; this is what allows your class
to be run by Hadoop streaming.

This will take in a file with lines of whitespace separated words, and
output a file with tab-separated lines like: ``"stars"\t5``.

For one-step jobs, you can also just redefine :py:meth:`~mrjob.job.MRJob.mapper` and :py:meth:`~mrjob.job.MRJob.reducer`::

    from mrjob.job import MRJob

    class MRWordCounter(MRJob):
        def mapper(self, key, line):
            for word in line.split():
                yield word, 1

        def reducer(self, word, occurrences):
            yield word, sum(occurrences)

    if __name__ == '__main__':
        MRWordCounter.run()

To test the job locally, just run::

   python your_mr_job_sub_class.py < log_file_or_whatever > output

The script will automatically invoke itself to run the various steps,
using :py:class:`~mrjob.local.LocalMRJobRunner`.

You can also run individual steps:

.. code-block:: sh

    # test 1st step mapper:
    python your_mr_job_sub_class.py --mapper 
    # test 2nd step reducer (--step-num=1 because step numbers are 0-indexed):
    python your_mr_job_sub_class.py --reducer --step-num=1

By default, we read from stdin, but you can also specify one or more
input files. It automatically decompresses .gz and .bz2 files::

    python your_mr_job_sub_class.py log_01.gz log_02.bz2 log_03

See :py:mod:`mrjob.examples` for more examples.

Running on EMR
--------------

* Set up your Amazon Account (see :doc:`amazon`)
* Set :envvar:`AWS_ACCESS_KEY_ID` and :envvar:`AWS_SECRET_ACCESS_KEY`
* Run your job with ``-r emr``::

    python your_mr_job_sub_class.py -r emr < input > output

Running on your own Hadoop cluster
----------------------------------

* Set up a hadoop cluster (see http://hadoop.apache.org/common/docs/current/)
* If running Python 2.5 on your cluster, install the :py:mod:`simplejson` module on all nodes. (Recommended but not required for Python 2.6+)
* Make sure :envvar:`HADOOP_HOME` is set
* Run your job with ``-r hadoop``::

    python your_mr_job_sub_class.py -r hadoop < input > output

Running from another script
---------------------------

Use :py:meth:`~mrjob.job.MRJob.make_runner` to run an
:py:class:`~mrjob.job.MRJob` from another Python script::

    from __future__ import with_statement # only needed on Python 2.5

    mr_job = MRWordCounter(args=['-r', 'emr'])
    with mr_job.make_runner() as runner:
        runner.run()
        for line in runner.stream_output():
            key, value = mr_job.parse_output_line(line)
            ... # do something with the parsed output
