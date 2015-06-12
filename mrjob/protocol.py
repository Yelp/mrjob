# Copyright 2009-2013 Yelp and Contributors
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

"""Protocols deserialize and serialize the input and output of tasks to raw
bytes for Hadoop to distribute to the next task or to write as output. For more
information, see :ref:`job-protocols` and :ref:`writing-protocols`.
"""
# This is one of the few places where efficiency really matters; to that end,
# we maintain separate code for Python 2 and 3 where necessary. Tests of
# protocols should *not* have different code for different versions of Python.

# don't add imports here that aren't part of the standard Python library,
# since MRJobs need to run in Amazon's generic EMR environment
import json

try:
    import cPickle as pickle  # Python 2 only
except ImportError:
    import pickle

from mrjob.py2 import PY2
from mrjob.util import safeeval

try:
    import ujson
    ujson  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    ujson = None


class _KeyCachingProtocol(object):
    """Protocol that caches the last decoded key.

    We're not currently exposing this class; inheriting from this class
    will result in almost as much code as simply writing your own read/write
    methods. You should probably cache keys, but in a way that makes sense for
    your use case.
    """
    _last_key_encoded = None
    _last_key_decoded = None

    def _loads(self, value):
        """Decode a single key/value, and return it."""
        raise NotImplementedError

    def _dumps(self, value):
        """Encode a single key/value, and return it."""
        raise NotImplementedError

    def read(self, line):
        """Decode a line of input.

        :type line: str
        :param line: A line of raw input to the job, without trailing newline.

        :return: A tuple of ``(key, value)``."""

        raw_key, raw_value = line.split(b'\t', 1)

        if raw_key != self._last_key_encoded:
            self._last_key_encoded = raw_key
            self._last_key_decoded = self._loads(raw_key)
        return (self._last_key_decoded, self._loads(raw_value))

    def write(self, key, value):
        """Encode a key and value.

        :param key: A key (of any type) yielded by a mapper/reducer
        :param value: A value (of any type) yielded by a mapper/reducer

        :rtype: str
        :return: A line, without trailing newline."""
        return self._dumps(key) + b'\t' + self._dumps(value)


# JSONProtocol (below) is just an alias, but we treat it as a class for the
# purpose of documentation. It encodes key and value as two JSONs separated
# by a tab. Same for JSONValueProtocol, except it encodes only the value (key
# is read as ``None``).

class StandardJSONProtocol(_KeyCachingProtocol):
    """Implements :py:class:`JSONProtocol` using Python's built-in JSON
    library.

    Note that the built-in library is (appropriately) strict about the JSON
    standard; it won't accept dictionaries with non-string keys, sets, or
    (on Python 3) bytestrings.
    """
    if PY2:
        def _loads(self, value):
            return json.loads(value)

        def _dumps(self, value):
            return json.dumps(value)
    else:
        def _loads(self, value):
            # Python 3's json module does not accept bytes
            return json.loads(value.decode('utf_8'))

        def _dumps(self, value):
            return json.dumps(value).encode('utf_8')


class StandardJSONValueProtocol(object):
    """Implements :py:class:`JSONValueProtocol` using Python's built-in JSON
    library.
    """
    if PY2:
        def read(self, line):
            return (None, json.loads(line))

        def write(self, key, value):
            return json.dumps(value).encode('utf_8')
    else:
        def read(self, line):
            # Python 3's json module does not accept bytes
            return (None, json.loads(line.decode('utf_8')))

        def write(self, key, value):
            return json.dumps(value)


class UltraJSONProtocol(_KeyCachingProtocol):
    """Implements :py:class:`JSONProtocol` using the :py:mod:`ujson` library.
    """
    def _loads(self, value):
        # ujson can handle bytes even in Python 3
        return ujson.loads(value)

    if PY2:
        def _dumps(self, value):
            return ujson.dumps(value)
    else:
        def _dumps(self, value):
            return ujson.dumps(value).encode('utf_8')


class UltraJSONValueProtocol(object):
    """Implements :py:class:`JSONValueProtocol` using the :py:mod:`ujson`
    library.
    """
    def read(self, line):
        # ujson can handle bytes even in Python 3
        return (None, ujson.loads(line))

    if PY2:
        def write(self, key, value):
            return ujson.dumps(value).encode('utf_8')
    else:
        def write(self, key, value):
            return ujson.dumps(value)


if ujson:
    JSONProtocol = UltraJSONProtocol
    JSONValueProtocol = UltraJSONValueProtocol
else:
    JSONProtocol = StandardJSONProtocol
    JSONValueProtocol = StandardJSONValueProtocol


class PickleProtocol(_KeyCachingProtocol):
    """Encode ``(key, value)`` as two string-escaped pickles separated
    by a tab.

    We string-escape the pickles to avoid having to deal with stray
    ``\\t`` and ``\\n`` characters, which would confuse Hadoop
    Streaming.

    Ugly, but should work for any type.

    .. warning::

        Pickling is only *backwards*-compatible across Python versions. If your
        job uses this as an output protocol, you should use at least the same
        version of Python to parse the job's output. Vice versa for using this
        as an input protocol.
    """

    # string_escape doesn't exist on Python 3 (you can't .decode() bytes).
    # Since efficiency matters for protocols, keeping separate code
    # for Python 2 and 3
    if PY2:
        def _loads(self, value):
            return pickle.loads(value.decode('string_escape'))

        def _dumps(self, value):
            return pickle.dumps(value).encode('string_escape')
    else:
        def _loads(self, value):
            return pickle.loads(
                value.decode('unicode_escape').encode('latin_1'))

        def _dumps(self, value):
            return pickle.dumps(value).decode(
                'latin_1').encode('unicode_escape')


class PickleValueProtocol(object):
    """Encode ``value`` as a string-escaped pickle and discard ``key``
    (``key`` is read in as ``None``).

    See :py:class:`PickleProtocol` for details.
    """
    if PY2:
        def read(self, line):
            return (None, pickle.loads(line.decode('string_escape')))

        def write(self, key, value):
            return pickle.dumps(value).encode('string_escape')
    else:
        def read(self, line):
            return (None, pickle.loads(
                line.decode('unicode_escape').encode('latin_1')))

        def write(self, key, value):
            return pickle.dumps(value).decode(
                'latin_1').encode('unicode_escape')


class RawProtocol(object):
    """Encode ``(key, value)`` as ``key`` and ``value`` separated by
    a tab (``key`` and ``value`` should be bytestrings).

    If ``key`` or ``value`` is ``None``, don't include a tab. When decoding a
    line with no tab in it, ``value`` will be ``None``.

    When reading from a line with multiple tabs, we break on the first one.

    Your key should probably not be ``None`` or have tab characters in it, but
    we don't check.
    """
    def read(self, line):
        key_value = line.split(b'\t', 1)
        if len(key_value) == 1:
            key_value.append(None)

        return tuple(key_value)

    def write(self, key, value):
        return b'\t'.join(x for x in (key, value) if x is not None)


class RawValueProtocol(object):
    """Read in a line as ``(None, line)``. Write out ``(key, value)``
    as ``value``. ``value`` must be bytes.

    The default way for a job to read its initial input.
    """
    def read(self, line):
        return (None, line)

    def write(self, key, value):
        return value


class ReprProtocol(_KeyCachingProtocol):
    """Encode ``(key, value)`` as two reprs separated by a tab.

    This only works for basic types (we use :py:func:`mrjob.util.safeeval`).

    .. warning::

        The repr format changes between different versions of Python (for
        example, braces for sets in Python 2.7, and different string contants
        in Python 3). Plan accordingly.
    """

    def _loads(self, value):
        return safeeval(value)

    if PY2:
        def _dumps(self, value):
            return repr(value)
    else:
        def _dumps(self, value):
            return repr(value).encode('utf_8')


class ReprValueProtocol(object):
    """Encode ``value`` as a repr and discard ``key`` (``key`` is read
    in as None).

    See :py:class:`ReprProtocol` for details.
    """
    def read(self, line):
        return (None, safeeval(line))

    if PY2:
        def write(self, key, value):
            return repr(value)
    else:
        def write(self, key, value):
            return repr(value).encode('utf_8')
