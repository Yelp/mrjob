mrjob: the Python MapReduce library
===================================

.. image:: https://github.com/Yelp/mrjob/raw/master/docs/logos/logo_medium.png

mrjob is a Python 2.6+/3.3+ package that helps you write and run Hadoop
Streaming jobs.

`Stable version (v0.5.3) documentation <http://packages.python.org/mrjob/>`_

`Development version documentation <http://mrjob.readthedocs.org/en/latest/>`_

.. image:: https://travis-ci.org/Yelp/mrjob.png
   :target: https://travis-ci.org/Yelp/mrjob

mrjob fully supports Amazon's Elastic MapReduce (EMR) service, which allows you
to buy time on a Hadoop cluster on an hourly basis. mrjob has basic support for Google Cloud Dataproc (Dataproc)
which allows you to buy time on a Hadoop cluster on a minute-by-minute basis.  It also works with your own
Hadoop cluster.

Some important features:

* Run jobs on EMR, Google Cloud Dataproc, your own Hadoop cluster, or locally (for testing).
* Write multi-step jobs (one map-reduce step feeds into the next)
* Duplicate your production environment inside Hadoop
    * Upload your source tree and put it in your job's ``$PYTHONPATH``
    * Run make and other setup scripts
    * Set environment variables (e.g. ``$TZ``)
    * Easily install python packages from tarballs (EMR only)
    * Setup handled transparently by ``mrjob.conf`` config file
* Automatically interpret error logs (EMR only)
* SSH tunnel to hadoop job tracker (EMR only)
* Minimal setup
    * To run on EMR, set ``$AWS_ACCESS_KEY_ID`` and ``$AWS_SECRET_ACCESS_KEY``
    * To run on Dataproc, set up your Google account and credentials (see `Dataproc Quickstart <http://pythonhosted.org/mrjob/guides/dataproc-quickstart.html>`_).
    * To run on your Hadoop cluster, just make sure ``$HADOOP_HOME`` is set.

Installation
------------

From PyPI:

``pip install mrjob``

From source:

``python setup.py install``


A Simple Map Reduce Job
-----------------------

Code for this example and more live in ``mrjob/examples``.

::

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
    # on Dataproc
    python mrjob/examples/mr_word_freq_count.py README.rst -r dataproc > counts
    # on your Hadoop cluster
    python mrjob/examples/mr_word_freq_count.py README.rst -r hadoop > counts


Setting up EMR on Amazon
------------------------

* create an `Amazon Web Services account <http://aws.amazon.com/>`_
* Get your access and secret keys (click "Security Credentials" on
  `your account page <http://aws.amazon.com/account/>`_)
* Set the environment variables ``$AWS_ACCESS_KEY_ID`` and
  ``$AWS_SECRET_ACCESS_KEY`` accordingly

Setting up Dataproc on Google
-----------------------------

* `Create a Google Cloud Platform account <http://cloud.google.com/>`_, see top-right
* `Learn about Google Cloud Platform "projects" <https://cloud.google.com/docs/overview/#projects>`_
* `Select or create a Cloud Platform Console project <https://console.cloud.google.com/project>`_
* `Enable billing for your project <https://console.cloud.google.com/billing>`_
* Go to the `API Manager <https://console.cloud.google.com/apis>`_ and search for / enable the following APIs...

  * Google Cloud Storage
  * Google Cloud Storage JSON API
  * Google Cloud Dataproc API

* Under Credentials, **Create Credentials** and select **Service account key**.  Then, select **New service account**, enter a Name and select **Key type** JSON.

* Install the `Google Cloud SDK <https://cloud.google.com/sdk/>`_

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

* `Source code <http://github.com/Yelp/mrjob>`__
* `Documentation <http://packages.python.org/mrjob/>`_
* `Discussion group <http://groups.google.com/group/mrjob>`_

Reference
---------

* `Hadoop Streaming <http://hadoop.apache.org/docs/stable1/streaming.html>`_
* `Elastic MapReduce <http://aws.amazon.com/documentation/elasticmapreduce/>`_
* `Google Cloud Dataproc <https://cloud.google.com/dataproc/overview>`_

More Information
----------------

* `PyCon 2011 mrjob overview <http://blip.tv/pycon-us-videos-2009-2010-2011/pycon-2011-mrjob-distributed-computing-for-everyone-4898987/>`_
* `Introduction to Recommendations and MapReduce with mrjob <http://aimotion.blogspot.com/2012/08/introduction-to-recommendations-with.html>`_
  (`source code <https://github.com/marcelcaraciolo/recsys-mapreduce-mrjob>`__)
* `Social Graph Analysis Using Elastic MapReduce and PyPy <http://postneo.com/2011/05/04/social-graph-analysis-using-elastic-mapreduce-and-pypy>`_

Thanks to `Greg Killion <mailto:greg@blind-works.net>`_
(`ROMEO ECHO_DELTA <http://www.romeoechodelta.net/>`_) for the logo.
