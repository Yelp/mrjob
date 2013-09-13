mrjob
=====

**mrjob lets you write MapReduce jobs in Python 2.5+ and run them on several
platforms.** You can:

* Write multi-step MapReduce jobs in pure Python
* Test on your local machine
* Run on a Hadoop cluster
* Run in the cloud using `Amazon Elastic MapReduce (EMR)`_

.. _Amazon Elastic MapReduce (EMR): http://aws.amazon.com/documentation/elasticmapreduce/

To get started, install with ``pip``::

    pip install mrjob

and begin reading the tutorial below.

.. ifconfig:: release.endswith('-dev')

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
