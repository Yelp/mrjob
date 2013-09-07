Glossary
========

..  glossary::

    Hadoop Streaming
        A special jar that lets you run code written in any language on Hadoop.
        It launches a subprocess, passes it input on stdin, and receives output
        on stdout. `Read more here.`_

    map
        The act of converting one key-value pair to zero or more key-value
        pairs based on some function.

    mapper
        A function that performs a :term:`map` operation.

    combine
        The act of converting one key and a list of values that share that key
        (not necessarily all values for the key) to zero or more key-value
        pairs based on some function.

    combiner
        A function that performs a :term:`combine` operation.

    reduce
        The act of converting one key and all values that share that key to
        zero or more key-value pairs based on some function.

    reducer
        A function that performs a :term:`reduce` operation.

    step
        One :term:`mapper`, :term:`combiner`, and :term:`reducer`. Any of
        these may be omitted from a mrjob step as long as at least one is
        included.

.. _Read more here.: http://hadoop.apache.org/docs/stable/streaming.html
