mrjob
=====

.. image:: http://github.com/yelp/mrjob/raw/master/docs/logos/logo_medium.png

mrjob is a Python 2.5+ package that helps you write and run Hadoop Streaming
jobs.

`Stable version (v0.4.1) documentation <http://packages.python.org/mrjob/>`_

`Development version documentation <http://mrjob.readthedocs.org/en/latest/>`_

.. image:: https://travis-ci.org/Yelp/mrjob.png
   :target: https://travis-ci.org/Yelp/mrjob

mrjob fully supports Amazon's Elastic MapReduce (EMR) service, which allows you
to buy time on a Hadoop cluster on an hourly basis. It also works with your own
Hadoop cluster.

Some important features:

* Run jobs on EMR, your own Hadoop cluster, or locally (for testing).
* Write multi-step jobs (one map-reduce step feeds into the next)
* Duplicate your production environment inside Hadoop
    * Upload your source tree and put it in your job's ``$PYTHONPATH``
    * Run make and other setup scripts
    * Set environment variables (e.g. ``$TZ``)
    * Easily install python packages from tarballs (EMR only)
    * Setup handled transparently by ``mrjob.conf`` config file
* Automatically interpret error logs from EMR
* SSH tunnel to hadoop job tracker on EMR
* Minimal setup
    * To run on EMR, set ``$AWS_ACCESS_KEY_ID`` and ``$AWS_SECRET_ACCESS_KEY``
    * To run on your Hadoop cluster, install ``simplejson`` and make sure
      ``$HADOOP_HOME`` is set.

Installation
------------

From PyPI:

``pip install mrjob``

From source:

``python setup.py install``


A Simple Map Reduce Job
-----------------------

Code for this example and more live in ``mrjob/examples``.

.. code:: python

   """The classic MapReduce job: count the frequency of words.
   """
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

Try It Out!
-----------

::

    # locally
    python mrjob/examples/mr_word_freq_count.py README.rst > counts
    # on EMR
    python mrjob/examples/mr_word_freq_count.py README.rst -r emr > counts
    # on your Hadoop cluster
    python mrjob/examples/mr_word_freq_count.py README.rst -r hadoop > counts

Setting up EMR on Amazon
------------------------

* create an `Amazon Web Services account <http://aws.amazon.com/>`_
* sign up for `Elastic MapReduce <http://aws.amazon.com/elasticmapreduce/>`_
* Get your access and secret keys (click "Security Credentials" on
  `your account page <http://aws.amazon.com/account/>`_)
* Set the environment variables ``$AWS_ACCESS_KEY_ID`` and
  ``$AWS_SECRET_ACCESS_KEY`` accordingly

Advanced Configuration
----------------------

To run in other AWS regions, upload your source tree, run ``make``, and use
other advanced mrjob features, you'll need to set up ``mrjob.conf``. mrjob looks
for its conf file in:

* The contents of ``$MRJOB_CONF``
* ``~/.mrjob.conf``
* ``/etc/mrjob.conf``

See `the mrjob.conf documentation
<http://packages.python.org/mrjob/guides/configs-basics.html>`_ for more information.


Project Links
-------------

* `Source code <http://github.com/Yelp/mrjob>`_
* `Documentation <http://packages.python.org/mrjob/>`_
* `Discussion group <http://groups.google.com/group/mrjob>`_

Reference
---------

* `Hadoop MapReduce <http://hadoop.apache.org/mapreduce/>`_
* `Elastic MapReduce <http://aws.amazon.com/documentation/elasticmapreduce/>`_

More Information
----------------

* `PyCon 2011 mrjob overview <http://blip.tv/pycon-us-videos-2009-2010-2011/pycon-2011-mrjob-distributed-computing-for-everyone-4898987/>`_
* `Introduction to Recommendations and MapReduce with mrjob <http://aimotion.blogspot.com/2012/08/introduction-to-recommendations-with.html>`_
  (`source code <https://github.com/marcelcaraciolo/recsys-mapreduce-mrjob>`_)
* `Social Graph Analysis Using Elastic MapReduce and PyPy <http://postneo.com/2011/05/04/social-graph-analysis-using-elastic-mapreduce-and-pypy>`_

Thanks to `Greg Killion <mailto:greg@blind-works.net>`_
(`blind-works.net <http://www.blind-works.net/>`_) for the logo.
