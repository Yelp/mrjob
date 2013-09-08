Glossary
========

..  glossary::

    combiner
        A function that converts one key and a list of values that share that
        key (not necessarily all values for the key) to zero or more key-value
        pairs based on some function. See :doc:`guides/concepts` for details.

    Hadoop Streaming
        A special jar that lets you run code written in any language on Hadoop.
        It launches a subprocess, passes it input on stdin, and receives output
        on stdout. `Read more here.`_

    input protocol
        The :term:`protocol` that converts the input file to the key-value
        pairs seen by the first step. See :ref:`job-protocols` for details.

    internal protocol
        The :term:`protocol` that converts the output of one step to the intput
        of the next.  See :ref:`job-protocols` for details.

    mapper
        A function that convertsone key-value pair to zero or more key-value
        pairs based on some function. See :doc:`guides/concepts` for details.

    output protocol
        The :term:`protocol` that converts the output of the last step to the
        bytes written to the output file. See :ref:`job-protocols` for details.

    protocol
        An object that converts a stream of bytes to and from Python objects.
        See :ref:`job-protocols` for details.

    reducer
        A function that converts one key and all values that share that key to
        zero or more key-value pairs based on some function. See
        :doc:`guides/concepts` for details.

    step
        One :term:`mapper`, :term:`combiner`, and :term:`reducer`. Any of
        these may be omitted from a mrjob step as long as at least one is
        included.

.. _Read more here.: http://hadoop.apache.org/docs/stable/streaming.html
