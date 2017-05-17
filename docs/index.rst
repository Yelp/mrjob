mrjob
=====

**mrjob lets you write MapReduce jobs in Python 2.7/3.3+ and run them on
several platforms.** You can:

* Write multi-step MapReduce jobs in pure Python
* Test on your local machine
* Run on a Hadoop cluster
* Run in the cloud using `Amazon Elastic MapReduce (EMR)`_
* Run in the cloud using `Google Cloud Dataproc (Dataproc)`_
* Easily run :doc:`Spark <guides/spark>` jobs on EMR or your own Hadoop cluster

.. _Google Cloud Dataproc (Dataproc): https://cloud.google.com/dataproc/overview
.. _Amazon Elastic MapReduce (EMR): http://aws.amazon.com/documentation/elasticmapreduce/


mrjob is licensed under the `Apache License, Version 2.0.`_

.. _Apache License, Version 2.0.: https://raw.github.com/Yelp/mrjob/master/LICENSE.txt

To get started, install with ``pip``::

    pip install mrjob

and begin reading the tutorial below.

.. ifconfig:: 'dev' in release

    .. note::

        This documentation is for |release|, which is currently in
        development. Documentation for the stable version of mrjob is hosted
        at `http://pythonhosted.org/mrjob <http://pythonhosted.org/mrjob>`_.

.. toctree::
    :maxdepth: 3

    guides.rst
    reference.rst
    whats-new.rst
    glossary.rst

.. rubric:: Appendices

:ref:`genindex`

:ref:`modindex`

:ref:`search`
