Dataproc Quickstart
===================

.. _google-setup:

Getting started with Google Cloud
---------------------------------

Using mrjob with Google Cloud Dataproc is as simple creating an account,
enabling Google Cloud Dataproc, and creating credentials.

Creating an account
^^^^^^^^^^^^^^^^^^^

* Go to `cloud.google.com <https://cloud.google.com>`__.
* Click the circle in the upper right, and select your Google account (if you
  don't have one sign up `here <https://accounts.google.com/SignUp>`__. `If
  you have multiple Google accounts, sign out first, and then sign into
  the account you want to use.`
* Click **Try it Free** in the upper right
* Enter your name and payment information
* Wait a few minutes while your first project is created


Enabling Google Cloud Dataproc
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Go `here <https://console.cloud.google.com/apis/library/dataproc.googleapis.com/>`__ (or search for "dataproc" under **APIs & Services > Library** in the upper left-hand menu)
* Click **Enable**

Creating credentials
^^^^^^^^^^^^^^^^^^^^

* Go `here <https://console.cloud.google.com/apis/credentials>`__ (or pick **APIs & Services > Credentials** in the upper left-hand menu)
* Pick **Create credentials > Service account key**
* Select **Compute engine default service account**
* Click **Create** to download a JSON file.

Then you should either install and set up the optional :command:`gcloud`
utility (see below) or point **$GOOGLE_APPLICATION_CREDENTIALS** at the file
you downloaded (``export GOOGLE_APPLICATION_CREDENTIALS="/path/to/Your Credentials.json"``).

.. _installing-gcloud:

Installing gcloud, gsutil, and other utilities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

mrjob does not require you to install the :command:`gcloud` command in order
to run jobs on Google Cloud Dataproc, unless you want to set up an SSH
tunnel to the Hadoop resource manager (see :mrjob-opt:`ssh_tunnel`).

The :command:`gcloud` command can be very useful for monitoring your job.
The :command:`gsutil` utility, packaged with it, is very helpful for
dealing with Google Storage, the cloud filesystem that Google Cloud Dataproc
uses.

To install :command:`gcloud` and :command:`gsutil`:

* Follow `these three steps <https://cloud.google.com/sdk/downloads#interactive>`__ to install the utilities
* Log in with your Google credentials (these will launch a browser):
  * :command:`gcloud auth login`
  * :command:`gcloud auth application-default init`

It's also helpful to set :command:`gcloud`\'s :mrjob-opt:`region` and
:mrjob-opt:`zone` to match mrjob's defaults:

* :command:`gcloud config set compute/region us-west1`
* :command:`gcloud config set compute/zone us-west1-a`
* :command:`gcloud config set dataproc/region us-west1`

.. _running-a-dataproc-job:

Running a Dataproc Job
----------------------

Running a job on Dataproc is just like running it locally or on your own Hadoop
cluster, with the following changes:

* The job and related files are uploaded to GCS before being run
* The job is run on Dataproc (of course)
* Output is written to GCS before mrjob streams it to stdout locally
* The Hadoop version is specified by the `Dataproc version <https://cloud.google.com/dataproc/dataproc-versions>`_

This the output of this command should be identical to the output shown in
:doc:`quickstart`, but it should take much longer::

    > python word_count.py -r dataproc README.txt
    "chars" 3654
    "lines" 123
    "words" 417

Sending Output to a Specific Place
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you'd rather have your output go to somewhere deterministic on GCS,
use :option:`--output-dir`::

    > python word_count.py -r dataproc README.rst \
    >   --output-dir=gs://my-bucket/wc_out/

.. _picking-dataproc-cluster-config:

Choosing Type and Number of GCE Instances
-----------------------------------------

When you create a cluster on Dataproc, you'll have the option of specifying a number
and type of GCE instances, which are basically virtual machines. Each instance
type has different memory, CPU, I/O and network characteristics, and costs
a different amount of money. See
`Machine Types <https://cloud.google.com/compute/docs/machine-types>`_ and
`Pricing <https://cloud.google.com/compute/pricing>`_ for details.

Instances perform one of three roles:

* **Master**: There is always one master instance. It handles scheduling of tasks
  (i.e. mappers and reducers), but does not run them itself.
* **Worker**: You may have one or more worker instances. These run tasks and host
  HDFS.
* **Preemptible Worker**: You may have zero or more of these. These run tasks, but do *not*
  host HDFS. This is mostly useful because your cluster can lose task instances
  without killing your job (see `Preemptible VMs <https://cloud.google.com/dataproc/preemptible-vms>`_).

By default, :py:mod:`mrjob` runs a single ``n1-standard-1``, which is a cheap but not
very powerful instance type. This can be quite adequate for testing your code on a small subset of your
data, but otherwise give little advantage over running a job locally. To get more performance out of
your job, you can either add more instances, use more powerful instances, or both.

Here are some things to consider when tuning your instance settings:

* Google Cloud bills you a 10-minute minimum even if your cluster only lasts for a few
  minutes (this is an artifact of the Google Cloud billing structure), so for many
  jobs that you run repeatedly, it is a good strategy to pick instance settings
  that make your job consistently run in a little less than 10 minutes.
* Your job will take much longer and may fail if any task (usually a reducer)
  runs out of memory and starts using swap. (You can verify this by using
  :command:`vmstat`.) Restructuring your
  job is often the best solution, but if you can't, consider using a high-memory
  instance type.
* Larger instance types are usually a better deal if you have the workload
  to justify them. For example, a ``n1-highcpu-8`` costs about 6 times as much
  as an ``n1-standard-1``, but it has about 8 times as much processing power
  (and more memory).

The basic way to control type and number of instances is with the
:mrjob-opt:`instance_type` and :mrjob-opt:`num_core_instances` options, on the command line like
this::

    --instance-type n1-highcpu-8 --num-core-instances 4

or in :py:mod:`mrjob.conf`, like this::

    runners:
      dataproc:
        instance_type: n1-highcpu-8
        num_core_instances: 4

In most cases, your master instance type doesn't need to be larger
than ``n1-standard-1`` to schedule tasks.  *instance_type* only applies to
instances that actually run tasks. (In this example, there are 1 ``n1-standard-1``
master instance, and 4 ``n1-highcpu-8`` worker instances.) You *will* need a larger
master instance if you have a very large number of input files; in this case,
use the :mrjob-opt:`master_instance_type` option.

If you want to run preemptible instances, use the :mrjob-opt:`task_instance_type` and :mrjob-opt:`num_task_instances` options.
