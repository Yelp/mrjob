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

try:
    import msgpack
except ImportError:
    msgpack = None

class HadoopStreamingProtocol(object):
    """Abstract base class for all protocols. Inherit from it and define
    your own :py:meth:`read` and :py:meth:`write` functions.
    """
    @classmethod
    def read(cls, line):
        """Decode a line of input.

        :type line: str
        :param line: A line of raw input to the job, without trailing newline.

        :return: A tuple of ``(key, value)``."""
        raise NotImplementedError

    @classmethod
    def write(cls, key, value):
        """Encode a key and value.

        :param key: A key (of any type) yielded by a mapper/reducer
        :param value: A value (of any type) yielded by a mapper/reducer

        :rtype: str
        :return: A line, without trailing newline."""
        raise NotImplementedError

class JSONProtocol(HadoopStreamingProtocol):
    """Encode ``(key, value)`` as two JSONs separated by a tab.

    Note that JSON has some limitations; dictionary keys must be strings,
    and there's no distinction between lists and tuples."""
    @classmethod
    def read(cls, line):
        key, value = line.split('\t')
        return json.loads(key), json.loads(value)

    @classmethod
    def write(cls, key, value):
        return '%s\t%s' % (json.dumps(key), json.dumps(value))

class JSONValueProtocol(HadoopStreamingProtocol):
    """Encode ``value`` as a JSON and discard ``key``
    (``key`` is read in as ``None``).
    """
    @classmethod
    def read(cls, line):
        return (None, json.loads(line))

    @classmethod
    def write(cls, key, value):
        return json.dumps(value)

class MsgPackProtocol(HadoopStreamingProtocol):
    """Encode ``(key, value)`` as two MsgPacks separated by a tab.

    Note that MsgPack has some limitations; dictionary keys must be strings,
    and there's no distinction between lists and tuples."""
    @classmethod
    def read(cls, line):
        key, value = line.split('\t')
        return msgpack.unpackb(key), msgpack.unpackb(value)

    @classmethod
    def write(cls, key, value):
        return '%s\t%s' % (msgpack.packb(key), msgpack.packb(value))

class MsgPackValueProtocol(HadoopStreamingProtocol):
    """Encode ``value`` as a MsgPack and discard ``key``
    (``key`` is read in as ``None``).
    """
    @classmethod
    def read(cls, line):
        return (None, msgpack.unpackb(line))

    @classmethod
    def write(cls, key, value):
        return msgpack.packb(value)

class PickleProtocol(HadoopStreamingProtocol):
    """Encode ``(key, value)`` as two string-escaped pickles separated
    by a tab.

    We string-escape the pickles to avoid having to deal with stray
    ``\\t`` and ``\\n`` characters, which would confuse Hadoop
    Streaming.

    Ugly, but should work for any type.
    """
    @classmethod
    def read(cls, line):
        key, value = line.split('\t')
        return (cPickle.loads(key.decode('string_escape')),
                cPickle.loads(value.decode('string_escape')))

    @classmethod
    def write(cls, key, value):
        return '%s\t%s' % (
            cPickle.dumps(key).encode('string_escape'),
            cPickle.dumps(value).encode('string_escape'))

class PickleValueProtocol(HadoopStreamingProtocol):
    """Encode ``value`` as a string-escaped pickle and discard ``key``
    (``key`` is read in as ``None``).
    """
    @classmethod
    def read(cls, line):
        return (None, cPickle.loads(line.decode('string_escape')))

    @classmethod
    def write(cls, key, value):
        return cPickle.dumps(value).encode('string_escape')

class RawValueProtocol(HadoopStreamingProtocol):
    """Read in a line as ``(None, line)``. Write out ``(key, value)``
    as ``value``. ``value`` must be a ``str``.

    The default way for a job to read its initial input.
    """
    @classmethod
    def read(cls, line):
        return (None, line)

    @classmethod
    def write(cls, key, value):
        return value

class ReprProtocol(HadoopStreamingProtocol):
    """Encode ``(key, value)`` as two reprs separated by a tab.

    This only works for basic types (we use :py:func:`mrjob.util.safeeval`).
    """
    @classmethod
    def read(cls, line):
        key, value = line.split('\t')
        return safeeval(key), safeeval(value)

    @classmethod
    def write(cls, key, value):
        return '%s\t%s' % (repr(key), repr(value))

class ReprValueProtocol(HadoopStreamingProtocol):
    """Encode ``value`` as a repr and discard ``key`` (``key`` is read
    in as None).

    This only works for basic types (we use :py:func:`mrjob.util.safeeval`).
    """
    @classmethod
    def read(cls, line):
        return (None, safeeval(line))

    @classmethod
    def write(cls, key, value):
        return repr(value)

#: The default protocol for all encoded input and output: ``'json'``
DEFAULT_PROTOCOL = 'msgpack'

#: Default mapping from protocol name to class:
#:
#: ============= ===============================
#: name          class
#: ============= ===============================
#: json          :py:class:`JSONProtocol`
#: json_value    :py:class:`JSONValueProtocol`
#: msgpack       :py:class:`MsgPackProtocol`
#: msgpack_value :py:class:`MsgPackValueProtocol`
#: pickle        :py:class:`PickleProtocol`
#: pickle_value  :py:class:`PickleValueProtocol`
#: raw_value     :py:class:`RawValueProtocol`
#: repr          :py:class:`ReprProtocol`
#: repr_value    :py:class:`ReprValueProtocol`
#: ============= ===============================
PROTOCOL_DICT = {
    'json': JSONProtocol,
    'json_value': JSONValueProtocol,
    'msgpack': MsgPackProtocol,
    'msgpack_value': MsgPackValueProtocol,
    'pickle': PickleProtocol,
    'pickle_value': PickleValueProtocol,
    'raw_value': RawValueProtocol,
    'repr': ReprProtocol,
    'repr_value': ReprValueProtocol,
}
