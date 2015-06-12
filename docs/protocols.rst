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
    is installed, and :py:class:`StandardJSONProtocol` otherwise.

    .. warning::

        :py:mod:`ujson` is about five times faster than the standard
        implementation, but is more willing to encode things that aren't
        strictly JSON-encodable, including sets, dictionaries with
        tuples as keys, UTF-8 encoded bytes, and objects (!). Relying on this
        behavior won't stop your job from working, but it can
        make your job *dependent* on :py:mod:`ujson`, rather than just using
        it as a speedup.

    .. note::

        :py:mod:`ujson` also differs from the standard implementation in that
        it doesn't  add spaces to its JSONs (``{"foo":"bar"}`` versus
        ``{"foo": "bar"}``). This probably won't affect anything but test
        cases.

.. autoclass:: StandardJSONProtocol
.. autoclass:: UltraJSONProtocol

.. py:class:: JSONValueProtocol

   Encode ``value`` as a JSON and discard ``key`` (``key`` is read in as
   ``None``).

   This is an alias for :py:class:`UltraJSONValueProtocol` if :py:mod:`ujson`
   is installed, and :py:class:`StandardJSONValueProtocol` otherwise.

.. autoclass:: StandardJSONValueProtocol
.. autoclass:: UltraJSONValueProtocol

Repr
----
.. autoclass:: ReprProtocol
.. autoclass:: ReprValueProtocol

Pickle
------
.. autoclass:: PickleProtocol
.. autoclass:: PickleValueProtocol
