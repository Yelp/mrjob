# Copyright 2009-2010 Yelp
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Protocols are what allow :py:class:`mrjob.job.MRJob` to input and
output arbitrary values, rather than just strings.

We use JSON as our default protocol rather than something more
powerful because we want to encourage interoperability with other
languages. If you need more power, you can represent values as reprs
or pickles.

Also, if know that your input will always be in JSON format, consider
the ``json_value`` protocol as an alternative to ``raw_value``.

For more information on using alternate protocols in your job, see
:ref:`job-protocols`.
"""
# don't add imports here that aren't part of the standard Python library,
# since MRJobs need to run in Amazon's generic EMR environment
import cPickle

from mrjob.util import safeeval

try:
    import simplejson as json # preferred because of C speedups
except ImportError:
    import json # built in to Python 2.6 and later


class ProtocolRegistrar(type):
    """Central registry for all declared HadoopStreamingProtocols"""
    _name_to_class_map = {}

    def __new__(mcs, name, bases, attrs):
        """For every new Protocol declaration, register the class here"""
        new_cls = super(ProtocolRegistrar, mcs).__new__(mcs, name, bases, attrs)

        mapping_name = new_cls.name
        if new_cls.__name__ == 'HadoopStreamingProtocol':
            return new_cls
        elif mapping_name in mcs._name_to_class_map:
            raise ValueError('Protocol already registered - %s' % mapping_name)

        mcs._name_to_class_map[mapping_name] = new_cls
        return new_cls

    @classmethod
    def mapping(mcs):
        return mcs._name_to_class_map

class HadoopStreamingProtocol(object):
    """Abstract base class for all protocols. Inherit from it and define
    your own :py:meth:`read` and :py:meth:`write` functions.
    """
    __metaclass__ = ProtocolRegistrar
    name = None

    def __init__(self, step_type=None):
        """Initialize a new protocol.

        :type step_type: str
        :param step_type: 'M' / 'R' / None - Mapper / Reducer / Unknown
        """
        self._step_type = step_type
        assert self.name is not None, "Protocol name missing"

    def read(self, line):
        """Decode a line of input.

        :type line: str
        :param line: A line of raw input to the job, without trailing newline.

        :return: A tuple of ``(key, value)``."""
        raise NotImplementedError

    def write(self, key, value):
        """Encode a key and value.

        :param key: A key (of any type) yielded by a mapper/reducer
        :param value: A value (of any type) yielded by a mapper/reducer

        :rtype: str
        :return: A line, without trailing newline."""
        raise NotImplementedError

class TabSplitProtocol(HadoopStreamingProtocol):
    """Abstract base class for all protocol that splits keys and values with '\t' characters.
    Inherit from it and define your own :py:meth:`load_from_string` and :py:meth:`dump_to_string` functions.
    """
    name = '__tab_split__'

    def __init__(self, step_type=None):
        super(TabSplitProtocol, self).__init__(step_type=step_type)
        self._last_reduced_key_raw = None
        self._last_reduced_key_loaded = None

    def read(self, line):
        raw_key, raw_value = line.split('\t')

        # For reducers, we'll be fed the same raw key multiple times
        # Instead of incurring the deserialization penalty N times with the exact same key
        # Deserialize once and save the result
        if self._step_type == 'R':
            if raw_key == self._last_reduced_key_raw:
                object_key = self._last_reduced_key_loaded
            else:
                object_key = self.load_from_string(raw_key)
                self._last_reduced_key_raw = raw_key
                self._last_reduced_key_loaded = object_key
        else:
            object_key = self.load_from_string(raw_key)

        # Always decode the value
        object_value = self.load_from_string(raw_value)
        return object_key, object_value

    def write(self, object_key, object_value):
        raw_key = self.dump_to_string(object_key)
        raw_value = self.dump_to_string(object_value)
        return '%s\t%s' % (raw_key, raw_value)

    def load_from_string(self, string_to_read):
        """Deserialize an object from a string.

        :type string_to_read: str
        :param string_to_read: A string to deserialize into a Python object.

        :return: A python object of `decoded_item`."""
        raise NotImplementedError

    def dump_to_string(self, object_to_dump):
        """Serialize an object to a string.

        :type object_to_dump: object
        :param object_to_dump: An object to serialize to a string.

        :return: A string representing the serialized form of object_to_dump."""
        raise NotImplementedError

class ValueOnlyProtocol(HadoopStreamingProtocol):
    """Abstract base class for all protocol that reads/writes lines only as values.
    Inherit from it and define your own :py:meth:`load_from_string` and :py:meth:`dump_to_string` functions.
    """
    name = '__value_only__'

    def read(self, line):
        return None, self.load_from_string(line)

    def write(self, object_key, object_value):
        return self.dump_to_string(object_value)

    def load_from_string(self, string_to_read):
        """Deserialize an object from a string.

        :type string_to_read: str
        :param string_to_read: A string to deserialize into a Python object.

        :return: A python object of `decoded_item`."""
        raise NotImplementedError

    def dump_to_string(self, object_item):
        """Serialize an object to a string.

        :type object_to_dump: object
        :param object_to_dump: An object to serialize to a string.

        :return: A string representing the serialized form of object_to_dump."""
        raise NotImplementedError

class JSONMixin(object):
    def load_from_string(self, string_to_read):
        return json.loads(string_to_read)

    def dump_to_string(self, object_to_dump):
        return json.dumps(object_to_dump)

class JSONProtocol(JSONMixin, TabSplitProtocol):
    """Encode ``(key, value)`` as two JSONs separated by a tab.

    Note that JSON has some limitations; dictionary keys must be strings,
    and there's no distinction between lists and tuples."""
    name = 'json'

class JSONValueProtocol(JSONMixin, ValueOnlyProtocol):
    """Encode ``value`` as a JSON and discard ``key``
    (``key`` is read in as ``None``).
    """
    name = 'json_value'

class PickleMixin(object):
    def load_from_string(self, string_to_read):
        return cPickle.loads(string_to_read.decode('string_escape'))

    def dump_to_string(self, object_to_dump):
        return cPickle.dumps(object_to_dump).encode('string_escape')

class PickleProtocol(PickleMixin, TabSplitProtocol):
    """Encode ``(key, value)`` as two string-escaped pickles separated
    by a tab.

    We string-escape the pickles to avoid having to deal with stray
    ``\\t`` and ``\\n`` characters, which would confuse Hadoop
    Streaming.

    Ugly, but should work for any type.
    """
    name = 'pickle'

class PickleValueProtocol(PickleMixin, ValueOnlyProtocol):
    """Encode ``value`` as a string-escaped pickle and discard ``key``
    (``key`` is read in as ``None``).
    """
    name = 'pickle_value'

class RawMixin(object):
    def load_from_string(self, string_to_read):
        return string_to_read

    def dump_to_string(self, object_to_dump):
        return object_to_dump

class RawValueProtocol(RawMixin, ValueOnlyProtocol):
    """Read in a line as ``(None, line)``. Write out ``(key, value)``
    as ``value``. ``value`` must be a ``str``.

    The default way for a job to read its initial input.
    """
    name = 'raw_value'

class ReprMixin(object):
    def load_from_string(self, string_to_read):
        return safeeval(string_to_read)

    def dump_to_string(self, object_to_dump):
        return repr(object_to_dump)

class ReprProtocol(ReprMixin, TabSplitProtocol):
    """Encode ``(key, value)`` as two reprs separated by a tab.

    This only works for basic types (we use :py:func:`mrjob.util.safeeval`).
    """
    name = 'repr'

class ReprValueProtocol(ReprMixin, ValueOnlyProtocol):
    """Encode ``value`` as a repr and discard ``key`` (``key`` is read
    in as None).

    This only works for basic types (we use :py:func:`mrjob.util.safeeval`).
    """
    name = 'repr_value'

#: The default protocol for all encoded input and output: ``'json'``
DEFAULT_PROTOCOL = JSONProtocol.name
