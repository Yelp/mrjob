# Copyright 2009-2013 Yelp and Contributors
# Copyright 2015 Yelp
# Copyright 2017 Yelp
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

"""Make sure all of our protocols work as advertised."""
import unittest
from unittest import TestCase
from unittest import skipIf

from mrjob.protocol import BytesProtocol
from mrjob.protocol import BytesValueProtocol
from mrjob.protocol import JSONProtocol
from mrjob.protocol import JSONValueProtocol
from mrjob.protocol import PickleProtocol
from mrjob.protocol import PickleValueProtocol
from mrjob.protocol import RapidJSONProtocol
from mrjob.protocol import RapidJSONValueProtocol
from mrjob.protocol import RawProtocol
from mrjob.protocol import RawValueProtocol
from mrjob.protocol import ReprProtocol
from mrjob.protocol import ReprValueProtocol
from mrjob.protocol import SimpleJSONProtocol
from mrjob.protocol import SimpleJSONValueProtocol
from mrjob.protocol import StandardJSONProtocol
from mrjob.protocol import StandardJSONValueProtocol
from mrjob.protocol import TextProtocol
from mrjob.protocol import TextValueProtocol
from mrjob.protocol import UltraJSONProtocol
from mrjob.protocol import UltraJSONValueProtocol
from mrjob.protocol import rapidjson
from mrjob.protocol import simplejson
from mrjob.protocol import ujson
from mrjob.py2 import PY2


class Point(object):
    """A simple class to test encoding of objects."""

    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __repr__(self):
        return '%s.%s(%r, %r)' % (self.__module__, self.__class__.__name__,
                                  self.x, self.y)

    def __eq__(self, other):
        if not isinstance(other, Point):
            return False

        return (self.x, self.y) == (other.x, other.y)

    def __ne__(self, other):
        return not self.__eq__(other)


# keys and values that JSON protocols should encode/decode correctly
JSON_KEYS_AND_VALUES = [
    (None, None),
    (1, 2),
    ('foo', 'bar'),
    ([1, 2, 3], []),
    ({'apples': 5}, {'oranges': 20}),
    (u'Qu\xe9bec', u'Ph\u1ede'),
    ('\t', '\n'),
]

# keys and values that repr protocols should encode/decode correctly
REPR_KEYS_AND_VALUES = JSON_KEYS_AND_VALUES + [
    ((1, 2), (3, 4)),
    (b'0\xa2', b'\xe9'),
    (set([1]), set()),
]

# keys and values that pickle protocols should encode/decode properly
PICKLE_KEYS_AND_VALUES = REPR_KEYS_AND_VALUES + [
    (Point(2, 3), Point(1, 4)),
]


class ProtocolTestCase(TestCase):

    def assertRoundTripOK(self, protocol, key, value):
        """Assert that we can encode and decode the given key and value,
        and get the same key and value we started with."""
        self.assertEqual((key, value),
                         protocol.read(protocol.write(key, value)))

    def assertRoundTripWithTrailingTabOK(self, protocol, key, value):
        """Assert that we can encode the given key and value, add a
        trailing tab (which Hadoop sometimes does), and decode it
        to get the same key and value we started with."""
        self.assertEqual((key, value),
                         protocol.read(protocol.write(key, value) + b'\t'))

    def assertCantEncode(self, protocol, key, value):
        self.assertRaises(Exception, protocol.write, key, value)

    def assertCantDecode(self, protocol, data):
        self.assertRaises(Exception, protocol.read, data)


class JSONProtocolAliasesTestCase(TestCase):

    def test_use_ujson_or_simplejson_if_installed(self):
        # these aliases are determined at compile time, so there
        # isn't a straightforward way to test this through patches
        if ujson:
            self.assertEqual(JSONProtocol, UltraJSONProtocol)
            self.assertEqual(JSONValueProtocol, UltraJSONValueProtocol)
        elif rapidjson and not PY2:
            self.assertEqual(JSONProtocol, RapidJSONProtocol)
            self.assertEqual(JSONValueProtocol, RapidJSONValueProtocol)
        elif simplejson:
            self.assertEqual(JSONProtocol, SimpleJSONProtocol)
            self.assertEqual(JSONValueProtocol, SimpleJSONValueProtocol)
        else:
            self.assertEqual(JSONProtocol, StandardJSONProtocol)
            self.assertEqual(JSONValueProtocol, StandardJSONValueProtocol)


class StandardJSONProtocolTestCase(ProtocolTestCase):

    PROTOCOL = StandardJSONProtocol()

    def test_round_trip(self):
        for k, v in JSON_KEYS_AND_VALUES:
            self.assertRoundTripOK(self.PROTOCOL, k, v)

    def test_round_trip_with_trailing_tab(self):
        for k, v in JSON_KEYS_AND_VALUES:
            self.assertRoundTripWithTrailingTabOK(self.PROTOCOL, k, v)

    def test_uses_json_format(self):
        KEY = ['a', 1]
        VALUE = {'foo': 'bar'}
        ENCODED = b'["a", 1]\t{"foo": "bar"}'

        self.assertEqual((KEY, VALUE), self.PROTOCOL.read(ENCODED))
        # ujson and rapidjson don't use spaces
        self.assertEqual(self.PROTOCOL.write(KEY, VALUE).replace(b' ', b''),
                         ENCODED.replace(b' ', b''))

    def test_tuples_become_lists(self):
        # JSON should convert tuples into lists
        self.assertEqual(
            ([1, 2], [3, 4]),
            self.PROTOCOL.read(self.PROTOCOL.write((1, 2), (3, 4))))

    def test_numerical_keys_become_strs(self):
        # JSON should convert numbers to strings when they are dict keys
        self.assertEqual(
            ({'1': 2}, {'3': 4}),
            self.PROTOCOL.read(self.PROTOCOL.write({1: 2}, {3: 4})))

    def test_bad_data(self):
        self.assertCantDecode(self.PROTOCOL, b'{@#$@#!^&*$%^')

    def test_bad_keys_and_values(self):
        # only unicodes (or bytes in utf-8) are allowed
        self.assertCantEncode(self.PROTOCOL, b'0\xa2', b'\xe9')

        # dictionaries have to have strings as keys
        self.assertCantEncode(self.PROTOCOL, {(1, 2): 3}, None)

        # sets don't exist in JSON
        self.assertCantEncode(self.PROTOCOL, set([1]), set())

        # Point class has no representation in JSON
        self.assertCantEncode(self.PROTOCOL, Point(2, 3), Point(1, 4))


@skipIf(simplejson is None, 'simplejson module not installed')
class SimpleJSONProtocolTestCase(StandardJSONProtocolTestCase):

    PROTOCOL = SimpleJSONProtocol()


@skipIf(rapidjson is None, 'rapidjson module not installed')
class RapidJSONProtocolTestCase(StandardJSONProtocolTestCase):

    PROTOCOL = RapidJSONProtocol()

    @unittest.skip('rapidjson only allows strings as keys')
    def test_numerical_keys_become_strs(self):
        pass

    def test_bad_keys_and_values(self):
        # dictionaries have to have strings as keys
        self.assertCantEncode(self.PROTOCOL, {(1, 2): 3}, None)

        # sets don't exist in JSON
        self.assertCantEncode(self.PROTOCOL, set([1]), set())

        # Point class has no representation in JSON
        self.assertCantEncode(self.PROTOCOL, Point(2, 3), Point(1, 4))


@skipIf(ujson is None, 'ujson module not installed')
class UltraJSONProtocolTestCase(StandardJSONProtocolTestCase):

    PROTOCOL = UltraJSONProtocol()

    def test_bad_keys_and_values(self):
        # seems like the only thing ujson won't encode is non-UTF-8 bytes
        self.assertCantEncode(self.PROTOCOL, b'0\xa2', b'\xe9')


class StandardJSONValueProtocolTestCase(ProtocolTestCase):

    PROTOCOL = StandardJSONValueProtocol()

    def test_round_trip(self):
        for _, v in JSON_KEYS_AND_VALUES:
            self.assertRoundTripOK(self.PROTOCOL, None, v)

    def test_round_trip_with_trailing_tab(self):
        for _, v in JSON_KEYS_AND_VALUES:
            self.assertRoundTripWithTrailingTabOK(self.PROTOCOL, None, v)

    def test_uses_json_format(self):
        VALUE = {'foo': 'bar'}
        ENCODED = b'{"foo": "bar"}'

        self.assertEqual((None, VALUE), self.PROTOCOL.read(ENCODED))
        self.assertEqual(self.PROTOCOL.write(None, VALUE).replace(b' ', b''),
                         ENCODED.replace(b' ', b''))

    def test_tuples_become_lists(self):
        # JSON should convert tuples into lists
        self.assertEqual(
            (None, [3, 4]),
            self.PROTOCOL.read(self.PROTOCOL.write(None, (3, 4))))

    def test_numerical_keys_become_strs(self):
        # JSON should convert numbers to strings when they are dict keys
        self.assertEqual(
            (None, {'3': 4}),
            self.PROTOCOL.read(self.PROTOCOL.write(None, {3: 4})))

    def test_bad_data(self):
        self.assertCantDecode(self.PROTOCOL, b'{@#$@#!^&*$%^')

    def test_bad_keys_and_values(self):
        # seems like the only thing ujson won't encode is non-UTF-8 bytes
        self.assertCantEncode(self.PROTOCOL, None, b'\xe9')

        # dictionaries have to have strings as keys
        self.assertCantEncode(self.PROTOCOL, None, {(1, 2): 3})

        # sets don't exist in JSON
        self.assertCantEncode(self.PROTOCOL, None, set())

        # Point class has no representation in JSON
        self.assertCantEncode(self.PROTOCOL, None, Point(1, 4))


@skipIf(simplejson is None, 'simplejson module not installed')
class SimpleJSONValueProtocolTestCase(StandardJSONValueProtocolTestCase):

    PROTOCOL = SimpleJSONValueProtocol()


@skipIf(rapidjson is None, 'rapidjson module not installed')
class RapidJSONValueProtocolTestCase(StandardJSONValueProtocolTestCase):

    PROTOCOL = RapidJSONValueProtocol()

    @unittest.skip('rapidjson only allows strings as keys')
    def test_numerical_keys_become_strs(self):
        pass

    def test_bad_keys_and_values(self):
        # dictionaries have to have strings as keys
        self.assertCantEncode(self.PROTOCOL, None, {(1, 2): 3})

        # sets don't exist in JSON
        self.assertCantEncode(self.PROTOCOL, None, set())

        # Point class has no representation in JSON
        self.assertCantEncode(self.PROTOCOL, None, Point(1, 4))


@skipIf(ujson is None, 'ujson module not installed')
class UltraJSONValueProtocolTestCase(StandardJSONValueProtocolTestCase):

    PROTOCOL = UltraJSONValueProtocol()

    def test_uses_json_format(self):
        VALUE = {'foo': 'bar'}
        ENCODED = b'{"foo":"bar"}'  # no whitespace in ujson

        self.assertEqual((None, VALUE), self.PROTOCOL.read(ENCODED))
        self.assertEqual(self.PROTOCOL.write(None, VALUE), ENCODED)

    def test_bad_keys_and_values(self):
        # seems like the only thing ujson won't encode is non-UTF-8 bytes
        self.assertCantEncode(self.PROTOCOL, None, b'\xe9')


class PickleProtocolTestCase(ProtocolTestCase):

    def test_round_trip(self):
        for k, v in PICKLE_KEYS_AND_VALUES:
            self.assertRoundTripOK(PickleProtocol(), k, v)

    def test_round_trip_with_trailing_tab(self):
        for k, v in PICKLE_KEYS_AND_VALUES:
            self.assertRoundTripWithTrailingTabOK(PickleProtocol(), k, v)

    def test_bad_data(self):
        self.assertCantDecode(PickleProtocol(), b'{@#$@#!^&*$%^')

    # no tests of what encoded data looks like; pickle is an opaque protocol


class PickleValueProtocolTestCase(ProtocolTestCase):

    def test_round_trip(self):
        for _, v in PICKLE_KEYS_AND_VALUES:
            self.assertRoundTripOK(PickleValueProtocol(), None, v)

    def test_round_trip_with_trailing_tab(self):
        for _, v in PICKLE_KEYS_AND_VALUES:
            self.assertRoundTripWithTrailingTabOK(
                PickleValueProtocol(), None, v)

    def test_bad_data(self):
        self.assertCantDecode(PickleValueProtocol(), b'{@#$@#!^&*$%^')

    # no tests of what encoded data looks like; pickle is an opaque protocol


class RawProtocolAliasesTestCase(TestCase):

    def test_raw_protocol_aliases(self):
        if PY2:
            self.assertEqual(RawProtocol, BytesProtocol)
            self.assertEqual(RawValueProtocol, BytesValueProtocol)
        else:
            self.assertEqual(RawProtocol, TextProtocol)
            self.assertEqual(RawValueProtocol, TextValueProtocol)


class BytesValueProtocolTestCase(ProtocolTestCase):

    def test_dumps_keys(self):
        self.assertEqual(BytesValueProtocol().write(b'foo', b'bar'), b'bar')

    def test_reads_raw_line(self):
        self.assertEqual(BytesValueProtocol().read(b'foobar'),
                         (None, b'foobar'))

    def test_bytestrings(self):
        self.assertRoundTripOK(BytesValueProtocol(), None, b'\xe90\c1a')

    def test_no_strip(self):
        self.assertEqual(BytesValueProtocol().read(b'foo\t \n\n'),
                         (None, b'foo\t \n\n'))


class TextValueProtocolTestCase(ProtocolTestCase):

    def test_dumps_keys(self):
        self.assertEqual(TextValueProtocol().write(u'foo', u'bar'), b'bar')

    def test_converts_raw_line_to_unicode(self):
        self.assertEqual(TextValueProtocol().read(b'foobar'),
                         (None, u'foobar'))

    def test_utf_8_decode(self):
        self.assertEqual(TextValueProtocol().read(b'caf\xc3\xa9'),
                         (None, u'caf\xe9'))

    def test_fall_back_to_latin_1(self):
        self.assertEqual(TextValueProtocol().read(b'caf\xe9'),
                         (None, u'caf\xe9'))

    def test_no_strip(self):
        self.assertEqual(TextValueProtocol().read(b'foo\t \n\n'),
                         (None, u'foo\t \n\n'))


class BytesProtocolTestCase(ProtocolTestCase):

    def test_round_trip(self):
        self.assertRoundTripOK(BytesProtocol(), b'foo', b'bar')
        self.assertRoundTripOK(BytesProtocol(), b'foo', None)
        self.assertRoundTripOK(BytesProtocol(), b'foo', b'')
        self.assertRoundTripOK(BytesProtocol(), b'caf\xe9', b'\xe90\c1a')

    def test_no_tabs(self):
        self.assertEqual(BytesProtocol().write(b'foo', None), b'foo')
        self.assertEqual(BytesProtocol().write(None, b'foo'), b'foo')
        self.assertEqual(BytesProtocol().read(b'foo'), (b'foo', None))

        self.assertEqual(BytesProtocol().write(b'', None), b'')
        self.assertEqual(BytesProtocol().write(None, None), b'')
        self.assertEqual(BytesProtocol().write(None, b''), b'')
        self.assertEqual(BytesProtocol().read(b''), (b'', None))

    def test_extra_tabs(self):
        self.assertEqual(BytesProtocol().write(b'foo', b'bar\tbaz'),
                         b'foo\tbar\tbaz')
        self.assertEqual(BytesProtocol().write(b'foo\tbar', b'baz'),
                         b'foo\tbar\tbaz')
        self.assertEqual(BytesProtocol().read(b'foo\tbar\tbaz'),
                         (b'foo', b'bar\tbaz'))

    def test_no_strip(self):
        self.assertEqual(BytesProtocol().read(b'foo\t \n\n'),
                         (b'foo', b' \n\n'))


class TextProtocolTestCase(ProtocolTestCase):

    def test_round_trip(self):
        self.assertRoundTripOK(TextProtocol(), u'foo', u'bar')
        self.assertRoundTripOK(TextProtocol(), u'foo', None)
        self.assertRoundTripOK(TextProtocol(), u'foo', u'')
        self.assertRoundTripOK(TextProtocol(), u'caf\xe9', u'\Xe90\c1a')

    def test_no_tabs(self):
        self.assertEqual(TextProtocol().write(u'foo', None), b'foo')
        self.assertEqual(TextProtocol().write(None, u'foo'), b'foo')
        self.assertEqual(TextProtocol().read(b'foo'), (u'foo', None))

        self.assertEqual(TextProtocol().write(u'', None), b'')
        self.assertEqual(TextProtocol().write(None, None), b'')
        self.assertEqual(TextProtocol().write(None, u''), b'')
        self.assertEqual(TextProtocol().read(b''), (u'', None))

    def test_extra_tabs(self):
        self.assertEqual(TextProtocol().write(u'foo', u'bar\tbaz'),
                         b'foo\tbar\tbaz')
        self.assertEqual(TextProtocol().write(u'foo\tbar', u'baz'),
                         b'foo\tbar\tbaz')
        self.assertEqual(TextProtocol().read(b'foo\tbar\tbaz'),
                         (u'foo', u'bar\tbaz'))

    def test_no_strip(self):
        self.assertEqual(TextProtocol().read(b'foo\t \n\n'),
                         (u'foo', u' \n\n'))

    def test_utf_8(self):
        self.assertEqual(TextProtocol().read(b'caf\xc3\xa9\tol\xc3\xa9'),
                         (u'caf\xe9', u'ol\xe9'))

    def test_latin_1_fallback(self):
        # we fall back to latin-1 for the whole line, not individual fields
        self.assertEqual(TextProtocol().read(b'caf\xe9\tol\xc3\xa9'),
                         (u'caf\xe9', u'ol\xc3\xa9'))


class ReprProtocolTestCase(ProtocolTestCase):

    def test_round_trip(self):
        for k, v in REPR_KEYS_AND_VALUES:
            self.assertRoundTripOK(ReprProtocol(), k, v)

    def test_round_trip_with_trailing_tab(self):
        for k, v in REPR_KEYS_AND_VALUES:
            self.assertRoundTripWithTrailingTabOK(ReprProtocol(), k, v)

    def test_uses_repr_format(self):
        KEY = ['a', 1]
        VALUE = {'foo': {'bar': 3}, 'baz': None}
        ENCODED = ('%r\t%r' % (KEY, VALUE)).encode('ascii')

        self.assertEqual((KEY, VALUE), ReprProtocol().read(ENCODED))
        self.assertEqual(ENCODED, ReprProtocol().write(KEY, VALUE))

    def test_bad_data(self):
        self.assertCantDecode(ReprProtocol(), b'{@#$@#!^&*$%^')

    def test_can_encode_point_but_not_decode(self):
        points_encoded = ReprProtocol().write(Point(2, 3), Point(1, 4))
        self.assertCantDecode(ReprProtocol(), points_encoded)


class ReprValueProtocolTestCase(ProtocolTestCase):

    def test_round_trip(self):
        for _, v in REPR_KEYS_AND_VALUES:
            self.assertRoundTripOK(ReprValueProtocol(), None, v)

    def test_round_trip_with_trailing_tab(self):
        for _, v in REPR_KEYS_AND_VALUES:
            self.assertRoundTripWithTrailingTabOK(ReprValueProtocol(), None, v)

    def test_uses_repr_format(self):
        VALUE = {'foo': {'bar': 3}, 'baz': None, 'quz': ['a', 1]}

        self.assertEqual((None, VALUE), ReprValueProtocol().read(repr(VALUE)))
        self.assertEqual(repr(VALUE).encode('ascii'),
                         ReprValueProtocol().write(None, VALUE))

    def test_bad_data(self):
        self.assertCantDecode(ReprValueProtocol(), b'{@#$@#!^&*$%^')

    def test_can_encode_point_but_not_decode(self):
        points_encoded = ReprValueProtocol().write(None, Point(1, 4))
        self.assertCantDecode(ReprValueProtocol(), points_encoded)
