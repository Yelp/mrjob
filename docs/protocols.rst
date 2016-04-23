mrjob.protocol - input and output
=================================
.. automodule:: mrjob.protocol


Strings
-------
.. py:class:: RawValueProtocol

    Just output ``value`` (a ``str``), and discard
    ``key`` (``key`` is read in as ``None``).

    **This is the default protocol used by jobs to read input.**

    This is an alias for :py:class:`RawValueProtocol` on Python 2 and
    :py:class:`TextValueProtocol` on Python 3.

.. autoclass:: BytesValueProtocol
.. autoclass:: TextValueProtocol

.. py:class:: RawProtocol

    Output ``key`` (``str``) and ``value`` (``str``),
    separated by a tab character.

    This is an alias for :py:class:`BytesProtocol` on Python 2 and
    :py:class:`TextProtocol` on Python 3.

.. autoclass:: BytesProtocol
.. autoclass:: TextProtocol


JSON
----
.. py:class:: JSONProtocol

    Encode ``(key, value)`` as two JSONs separated by a tab.

    **This is the default protocol used by jobs to write output and communicate
    between steps.**

    This is an alias for :py:class:`UltraJSONProtocol` if :py:mod:`ujson`
    is installed, :py:class:`SimpleJSONProtocol` if :py:mod:`simplejson`
    is installed and :py:mod:`ujson` is not and
    :py:class:`StandardJSONProtocol` if neither is installed.

.. autoclass:: UltraJSONProtocol
.. autoclass:: SimpleJSONProtocol
.. autoclass:: StandardJSONProtocol

.. py:class:: JSONValueProtocol

   Encode ``value`` as a JSON and discard ``key`` (``key`` is read in as
   ``None``).

    This is an alias for :py:class:`UltraJSONValueProtocol` if :py:mod:`ujson`
    is installed, :py:class:`SimpleJSONValueProtocol` if :py:mod:`simplejson`
    is installed and :py:mod:`ujson` is not and
    :py:class:`StandardJSONValueProtocol` if neither is installed.

.. autoclass:: UltraJSONValueProtocol
.. autoclass:: SimpleJSONValueProtocol
.. autoclass:: StandardJSONValueProtocol

Repr
----
.. autoclass:: ReprProtocol
.. autoclass:: ReprValueProtocol

Pickle
------
.. autoclass:: PickleProtocol
.. autoclass:: PickleValueProtocol
