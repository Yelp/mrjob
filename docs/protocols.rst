mrjob.protocol - input and output
=================================

.. automodule:: mrjob.protocol

.. autodata:: DEFAULT_PROTOCOL
.. autoclass:: ProtocolRegistrar

----

Abstract base classes

.. autoclass:: HadoopStreamingProtocol
    :members: __init__, read, write

.. autoclass:: TabSplitProtocol
    :show-inheritance:
    :members: load_from_string, dump_to_string
.. autoclass:: ValueOnlyProtocol
    :show-inheritance:
    :members: load_from_string, dump_to_string

----

Implemented protocols, available via :py:attr:`ProtocolRegistrar.mapping`

============ ===============================
name         class
============ ===============================
json         :py:class:`JSONProtocol`
json_value   :py:class:`JSONValueProtocol`
pickle       :py:class:`PickleProtocol`
pickle_value :py:class:`PickleValueProtocol`
raw_value    :py:class:`RawValueProtocol`
repr         :py:class:`ReprProtocol`
repr_value   :py:class:`ReprValueProtocol`
============ ===============================

.. autoclass:: JSONProtocol
.. autoclass:: JSONValueProtocol
.. autoclass:: PickleProtocol
.. autoclass:: PickleValueProtocol
.. autoclass:: RawValueProtocol
.. autoclass:: ReprProtocol
.. autoclass:: ReprValueProtocol
