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
:py:class:`JSONValueProtocol` as an alternative to
:py:class:`RawValueProtocol`.

Custom Protocols
^^^^^^^^^^^^^^^^

A protocol is an object with methods ``read(self, line)`` and
``write(self, key, value)``. The ``read(line)`` method takes a string and
returns a 2-tuple of decoded objects, and ``write(cls, key, value)`` takes
the key and value and returns the line to be passed back to Hadoop Streaming
or as output.

The built-in protocols use class methods instead of instance methods for
legacy reasons, but you should use instance methods.

For more information on using alternate protocols in your job, see
:ref:`job-protocols`.
"""
# don't add imports here that aren't part of the standard Python library,
# since MRJobs need to run in Amazon's generic EMR environment
import cPickle

from mrjob.util import safeeval

try:
    import simplejson as json  # preferred because of C speedups
    json  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import json  # built in to Python 2.6 and later


# DEPRECATED: Abstract base class for all protocols. Now just an alias for
# ``object``.
HadoopStreamingProtocol = object


class _ClassBasedKeyCachingProtocol(object):
    """Protocol that caches the last decoded key and uses class methods
    instead of instance methods. Do not inherit from this.
    """

    _last_key_encoded = None
    _last_key_decoded = None

    @classmethod
    def load_from_string(self, value):
        raise NotImplementedError

    @classmethod
    def dump_to_string(self, value):
        raise NotImplementedError

    @classmethod
    def read(cls, line):
        """Decode a line of input.

        :type line: str
        :param line: A line of raw input to the job, without trailing newline.

        :return: A tuple of ``(key, value)``."""

        raw_key, raw_value = line.split('\t')

        if raw_key != cls._last_key_encoded:
            cls._last_key_encoded = raw_key
            cls._last_key_decoded = cls.load_from_string(raw_key)
        return (cls._last_key_decoded, cls.load_from_string(raw_value))

    @classmethod
    def write(cls, key, value):
        """Encode a key and value.

        :param key: A key (of any type) yielded by a mapper/reducer
        :param value: A value (of any type) yielded by a mapper/reducer

        :rtype: str
        :return: A line, without trailing newline."""
        return '%s\t%s' % (cls.dump_to_string(key),
                           cls.dump_to_string(value))


class JSONProtocol(_ClassBasedKeyCachingProtocol):
    """Encode ``(key, value)`` as two JSONs separated by a tab.

    Note that JSON has some limitations; dictionary keys must be strings,
    and there's no distinction between lists and tuples."""

    @classmethod
    def load_from_string(cls, value):
        return json.loads(value)

    @classmethod
    def dump_to_string(cls, value):
        return json.dumps(value)


class JSONValueProtocol(object):
    """Encode ``value`` as a JSON and discard ``key``
    (``key`` is read in as ``None``).
    """
    @classmethod
    def read(cls, line):
        return (None, json.loads(line))

    @classmethod
    def write(cls, key, value):
        return json.dumps(value)


class PickleProtocol(_ClassBasedKeyCachingProtocol):
    """Encode ``(key, value)`` as two string-escaped pickles separated
    by a tab.

    We string-escape the pickles to avoid having to deal with stray
    ``\\t`` and ``\\n`` characters, which would confuse Hadoop
    Streaming.

    Ugly, but should work for any type.
    """

    @classmethod
    def load_from_string(cls, value):
        return cPickle.loads(value.decode('string_escape'))

    @classmethod
    def dump_to_string(cls, value):
        return cPickle.dumps(value).encode('string_escape')


class PickleValueProtocol(object):
    """Encode ``value`` as a string-escaped pickle and discard ``key``
    (``key`` is read in as ``None``).
    """
    @classmethod
    def read(cls, line):
        return (None, cPickle.loads(line.decode('string_escape')))

    @classmethod
    def write(cls, key, value):
        return cPickle.dumps(value).encode('string_escape')


# This was added in 0.3, so no @classmethod for backwards compatibility
class RawProtocol(object):
    """Encode ``(key, value)`` as ``key`` and ``value`` separated by
    a tab (``key`` and ``value`` should be bytestrings).

    If ``key`` or ``value`` is ``None``, don't include a tab. When decoding a
    line with no tab in it, ``value`` will be ``None``.

    When reading from a line with multiple tabs, we break on the first one.

    Your key should probably not be ``None`` or have tab characters in it, but
    we don't check.
    """
    def read(cls, line):
        key_value = line.split('\t', 1)
        if len(key_value) == 1:
            key_value.append(None)

        return tuple(key_value)

    def write(cls, key, value):
        return '\t'.join(x for x in (key, value) if x is not None)


class RawValueProtocol(object):
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


class ReprProtocol(_ClassBasedKeyCachingProtocol):
    """Encode ``(key, value)`` as two reprs separated by a tab.

    This only works for basic types (we use :py:func:`mrjob.util.safeeval`).
    """

    @classmethod
    def load_from_string(cls, value):
        return safeeval(value)

    @classmethod
    def dump_to_string(cls, value):
        return repr(value)


class ReprValueProtocol(object):
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

#: .. deprecated:: 0.3.0
#:
#: Formerly the default protocol for all encoded input and output: ``'json'``
DEFAULT_PROTOCOL = 'json'

#: .. deprecated:: 0.3.0
#:
#: Default mapping from protocol name to class:
#:
#: ============ ===============================
#: name         class
#: ============ ===============================
#: json         :py:class:`JSONProtocol`
#: json_value   :py:class:`JSONValueProtocol`
#: pickle       :py:class:`PickleProtocol`
#: pickle_value :py:class:`PickleValueProtocol`
#: raw_value    :py:class:`RawValueProtocol`
#: repr         :py:class:`ReprProtocol`
#: repr_value   :py:class:`ReprValueProtocol`
#: ============ ===============================
PROTOCOL_DICT = {
    'json': JSONProtocol,
    'json_value': JSONValueProtocol,
    'pickle': PickleProtocol,
    'pickle_value': PickleValueProtocol,
    'raw_value': RawValueProtocol,
    'repr': ReprProtocol,
    'repr_value': ReprValueProtocol,
}
