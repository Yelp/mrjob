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

   This is an alias for the first one of :py:class:`UltraJSONProtocol`,
   :py:class:`RapidJSONProtocol`, :py:class:`SimpleJSONProtocol`,
   or :py:class:`StandardJSONProtocol` for which the underlying library is
   available.

.. autoclass:: UltraJSONProtocol
.. autoclass:: RapidJSONProtocol
.. autoclass:: SimpleJSONProtocol
.. autoclass:: StandardJSONProtocol

.. py:class:: JSONValueProtocol

   Encode ``value`` as a JSON and discard ``key`` (``key`` is read in as
   ``None``).

   This is an alias for the first one of :py:class:`UltraJSONValueProtocol`,
   :py:class:`RapidJSONValueProtocol`, :py:class:`SimpleJSONValueProtocol`,
   or :py:class:`StandardJSONValueProtocol` for which the underlying library is
   available.

.. autoclass:: UltraJSONValueProtocol
.. autoclass:: RapidJSONValueProtocol
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
