Python 2 vs. Python 3
=====================

Raw protocols
-------------

Both because we don't want to break mrjob for Python 2 users, and to make writing jobs simple, jobs read their input as ``str``\ s by default (even though ``str`` means bytes in Python 2 and unicode in Python 3).

The way this works in mrjob is that :py:class:`~mrjob.protocol.RawValueProtocol` is actually an alias for one of two classes, :py:class:`~mrjob.protocol.BytesValueProtocol` if you're in Python 2, and :py:class:`~mrjob.protocol.TextValueProtocol` if you're in Python 3.

If you care about this distinction, you may want to explicitly set :py:attr:`~mrjob.job.MRJob.INPUT_PROTOCOL` to one of these. If your input has a well-defined encoding, probably you want :py:class:`~mrjob.protocol.BytesValueProtocol`, and if it's a bunch of text that's mostly ASCII, with like, some stuff that... might be UTF-8? (i.e. most log files), you probably want :py:class:`~mrjob.protocol.TextValueProtocol`. But most of the time it'll just work.

Bytes vs. strings
-----------------

The following things are bytes in any version of Python (which means you need to use the ``bytes`` type and/or ``b'...'`` constant in Python 3):
 - data read or written by :ref:`job-protocols`
 - lines yielded by :py:meth:`~mrjob.runner.MRJobRunner.stream_output`
 - anything read from :py:meth:`~mrjob.fs.base.Filesystem.cat`

The :py:attr:`~mrjob.job.MRJob.stdin`, :py:attr:`~mrjob.job.MRJob.stdout`, and :py:attr:`~mrjob.job.MRJob.stderr` attributes of :py:class:`~mrjob.job.MRJob`\ s are always bytestreams (so, for example, ``self.stderr`` defaults to ``sys.stderr.buffer`` in Python 3).

Everything else (including file paths, URIs, arguments to commands, and logging messages) are strings; that is, ``str`` on Python 3, and either ``str`` or ``unicode`` on Python 2. Like with :py:class:`~mrjob.protocol.RawValueProtocol`, most of the time it'll just work even if you don't think about it.

python_bin
----------

:mrjob-opt:`python_bin` defaults to :command:`python` in Python 2, and :command:`python3` in Python 3 (except in Python 2 on EMR, where it's one of :command:`python2.6` or :command:`python2.7`; see :mrjob-opt:`python_bin` for details).

Your Hadoop cluster
-------------------

Whatever version of Python you use, you'll have to have a compatible version of Python installed on your Hadoop cluster. mrjob does its best to make this work on Elastic MapReduce (see :mrjob-opt:`bootstrap_python`), but if you're running on your own Hadoop cluster, this is up to you.
