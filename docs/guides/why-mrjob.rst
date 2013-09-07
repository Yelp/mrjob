Why mrjob?
==========

Overview
--------

mrjob is the easiest route to writing Python programs that run on Hadoop. If
you use mrjob, you'll be able to test your code locally without installing
Hadoop or run it on a cluster of your choice.

Additionally, mrjob has extensive integration with Amazon Elastic MapReduce.
Once you're set up, it's as easy to run your job in the cloud as it is to run
it on your laptop.

Here are a number of features of mrjob that make writing MapReduce jobs easier:

* Keep all MapReduce code for one job in a single class
* Easily upload and install code and data dependencies at runtime
* Switch input and output formats with a single line of code
* Automatically download and parse error logs for Python tracebacks
* Put command line filters before or after your Python code

If you don't want to be a Hadoop expert but need the computing power of
MapReduce, mrjob might be just the thing for you.

Why use mrjob instead of X?
---------------------------

Where X is any other library that helps Hadoop and Python interface with each
other.

1. mrjob has more documentation than any other framework or library we are
   aware of. If you're reading this, it's probably your first contact with the
   library, which means you are in a great position to `provide valuable
   feedback about our documentation.
   <http://github.com/yelp/mrjob/issues/new>`_ Let us know if anything is
   unclear or hard to understand.

2. mrjob lets you run your code without Hadoop at all. Other frameworks
   require a Hadoop instance to function at all. If you use mrjob, you'll be
   able to write proper tests for your MapReduce code.

3. mrjob provides a consistent interface across every environment it supports.
   No matter whether you're running locally, in the cloud, or on your own
   cluster, your Python code doesn't change at all.

4. mrjob handles much of the machinery of getting code and data to and from
   the cluster your job runs on. You don't need a series of scripts to install
   dependencies or upload files.

5. mrjob makes debugging much easier. Locally, it can run a simple MapReduce
   implementation in-process, so you get a traceback in your console instead
   of in an obscure log file. On a cluster or on Elastic MapReduce, it parses
   error logs for Python tracebacks and other likely causes of failure.

6. mrjob automatically serializes and deserializes data going into and coming
   out of each task so you don't need to constantly ``json.loads()`` and
   ``json.dumps()``.

Why use X instead of mrjob?
---------------------------

The flip side to mrjob's ease of use is that it doesn't give you the same
level of access to Hadoop APIs that Dumbo and Pydoop do. It's simplified a
great deal. But that hasn't stopped several companies, including Yelp, from
using it for day-to-day heavy lifting. For common (and many uncommon) cases,
the abstractions help rather than hinder.

Other libraries can be faster if you use typedbytes. There have been several
attempts at integrating it with mrjob, and it may land eventually, but it
doesn't exist yet.
