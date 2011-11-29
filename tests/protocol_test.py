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

"""Make sure all of our protocols work as advertised."""
from testify import TestCase
from testify import assert_equal
from testify import assert_raises

from mrjob.protocol import JSONProtocol
from mrjob.protocol import JSONValueProtocol
from mrjob.protocol import PickleProtocol
from mrjob.protocol import PickleValueProtocol
from mrjob.protocol import RawValueProtocol
from mrjob.protocol import ReprProtocol
from mrjob.protocol import ReprValueProtocol


# keys and values that should encode/decode properly in all protocols
SAFE_KEYS_AND_VALUES = [
    (None, None),
    (1, 2),
    ('foo', 'bar'),
    ([1, 2, 3], []),
    ({'apples': 5}, {'oranges': 20}),
    (u'Qu\xe9bec', u'Ph\u1ede'),
    ('\t', '\n'),
]


def assert_round_trip_ok(protocol, key, value):
    """Assert that we can encode and decode the given key and value,
    and get the same key and value we started with."""
    assert_equal((key, value), protocol.read(protocol.write(key, value)))


def assert_cant_encode(protocol, key, value):
    assert_raises(Exception, protocol.write, key, value)


def assert_cant_decode(protocol, data):
    assert_raises(Exception, protocol.read, data)


class Point(object):
    """A simple class to test encoding of objects."""

    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __repr__(self):
        return '%s.%s(%r, %r)' % (self.__module__, self.__class__.__name__,
                                  self.x, self.y)

    def __cmp__(self, other):
        if not isinstance(other, Point):
            return 1
        else:
            return cmp((self.x, self.y), (other.x, other.y))


class JSONProtocolTestCase(TestCase):

    def test_round_trip(self):
        for k, v in SAFE_KEYS_AND_VALUES:
            assert_round_trip_ok(JSONProtocol, k, v)

    def test_uses_json_format(self):
        KEY = ['a', 1]
        VALUE = {'foo': {'bar': 3}, 'baz': None}
        ENCODED = '["a", 1]\t{"foo": {"bar": 3}, "baz": null}'

        assert_equal((KEY, VALUE), JSONProtocol.read(ENCODED))
        assert_equal(ENCODED, JSONProtocol.write(KEY, VALUE))

    def test_tuples_become_lists(self):
        # JSON should convert tuples into lists
        assert_equal(([1, 2], [3, 4]),
                     JSONProtocol.read(JSONProtocol.write((1, 2), (3, 4))))

    def test_numerical_keys_become_strs(self):
        # JSON should convert numbers to strings when they are dict keys
        assert_equal(({'1': 2}, {'3': 4}),
                     JSONProtocol.read(JSONProtocol.write({1: 2}, {3: 4})))

    def test_bad_data(self):
        assert_cant_decode(JSONProtocol, '{@#$@#!^&*$%^')

    def test_bad_keys_and_values(self):
        # dictionaries have to have strings as keys
        assert_cant_encode(JSONProtocol, {(1, 2): 3}, None)

        # only unicodes (or bytes in utf-8) are allowed
        assert_cant_encode(JSONProtocol, '0\xa2', '\xe9')

        # sets don't exist in JSON
        assert_cant_encode(JSONProtocol, set([1]), set())

        # Point class has no representation in JSON
        assert_cant_encode(JSONProtocol, Point(2, 3), Point(1, 4))


class JSONValueProtocolTestCase(TestCase):

    def test_round_trip(self):
        for _, v in SAFE_KEYS_AND_VALUES:
            assert_round_trip_ok(JSONValueProtocol, None, v)

    def test_uses_json_format(self):
        VALUE = {'foo': {'bar': 3}, 'baz': None, 'quz': ['a', 1]}
        ENCODED = '{"foo": {"bar": 3}, "baz": null, "quz": ["a", 1]}'

        assert_equal((None, VALUE), JSONValueProtocol.read(ENCODED))
        assert_equal(ENCODED, JSONValueProtocol.write(None, VALUE))

    def test_tuples_become_lists(self):
        # JSON should convert tuples into lists
        assert_equal(
            (None, [3, 4]),
            JSONValueProtocol.read(JSONValueProtocol.write(None, (3, 4))))

    def test_numerical_keys_become_strs(self):
        # JSON should convert numbers to strings when they are dict keys
        assert_equal(
            (None, {'3': 4}),
            JSONValueProtocol.read(JSONValueProtocol.write(None, {3: 4})))

    def test_bad_data(self):
        assert_cant_decode(JSONValueProtocol, '{@#$@#!^&*$%^')

    def test_bad_keys_and_values(self):
        # dictionaries have to have strings as keys
        assert_cant_encode(JSONValueProtocol, None, {(1, 2): 3})

        # only unicodes (or bytes in utf-8) are allowed
        assert_cant_encode(JSONValueProtocol, None, '\xe9')

        # sets don't exist in JSON
        assert_cant_encode(JSONValueProtocol, None, set())

        # Point class has no representation in JSON
        assert_cant_encode(JSONValueProtocol, None, Point(1, 4))


class PickleProtocolTestCase(TestCase):

    def test_round_trip(self):
        for k, v in SAFE_KEYS_AND_VALUES:
            assert_round_trip_ok(PickleProtocol, k, v)
        assert_round_trip_ok(PickleProtocol, (1, 2), (3, 4))
        assert_round_trip_ok(PickleProtocol, '0\xa2', '\xe9')
        assert_round_trip_ok(PickleProtocol, set([1]), set())
        assert_round_trip_ok(PickleProtocol, Point(2, 3), Point(1, 4))

    def test_bad_data(self):
        assert_cant_decode(PickleProtocol, '{@#$@#!^&*$%^')

    # no tests of what encoded data looks like; pickle is an opaque protocol


class PickleValueProtocolTestCase(TestCase):

    def test_round_trip(self):
        for _, v in SAFE_KEYS_AND_VALUES:
            assert_round_trip_ok(PickleValueProtocol, None, v)
        assert_round_trip_ok(PickleValueProtocol, None, (3, 4))
        assert_round_trip_ok(PickleValueProtocol, None, '\xe9')
        assert_round_trip_ok(PickleValueProtocol, None, set())
        assert_round_trip_ok(PickleValueProtocol, None, Point(1, 4))

    def test_bad_data(self):
        assert_cant_decode(PickleValueProtocol, '{@#$@#!^&*$%^')

    # no tests of what encoded data looks like; pickle is an opaque protocol


class RawValueProtocolTestCase(TestCase):

    def test_dumps_keys(self):
        assert_equal(RawValueProtocol.write('foo', 'bar'), 'bar')

    def test_reads_raw_line(self):
        assert_equal(RawValueProtocol.read('foobar'), (None, 'foobar'))

    def test_bytestrings(self):
        assert_round_trip_ok(RawValueProtocol, None, '\xe90\c1a')

    def test_no_strip(self):
        assert_equal(RawValueProtocol.read('foo\t \n\n'), (None, 'foo\t \n\n'))


class ReprProtocolTestCase(TestCase):

    def test_round_trip(self):
        for k, v in SAFE_KEYS_AND_VALUES:
            assert_round_trip_ok(ReprProtocol, k, v)
        assert_round_trip_ok(ReprProtocol, (1, 2), (3, 4))
        assert_round_trip_ok(ReprProtocol, '0\xa2', '\xe9')
        assert_round_trip_ok(ReprProtocol, set([1]), set())

    def test_uses_repr_format(self):
        KEY = ['a', 1]
        VALUE = {'foo': {'bar': 3}, 'baz': None}
        ENCODED = '%r\t%r' % (KEY, VALUE)

        assert_equal((KEY, VALUE), ReprProtocol.read(ENCODED))
        assert_equal(ENCODED, ReprProtocol.write(KEY, VALUE))

    def test_bad_data(self):
        assert_cant_decode(ReprProtocol, '{@#$@#!^&*$%^')

    def test_can_encode_point_but_not_decode(self):
        points_encoded = ReprProtocol.write(Point(2, 3), Point(1, 4))
        assert_cant_decode(ReprProtocol, points_encoded)


class ReprValueProtocolTestCase(TestCase):

    def test_round_trip(self):
        for _, v in SAFE_KEYS_AND_VALUES:
            assert_round_trip_ok(ReprValueProtocol, None, v)
        assert_round_trip_ok(ReprValueProtocol, None, (3, 4))
        assert_round_trip_ok(ReprValueProtocol, None, '\xe9')
        assert_round_trip_ok(ReprValueProtocol, None, set())

    def test_uses_repr_format(self):
        VALUE = {'foo': {'bar': 3}, 'baz': None, 'quz': ['a', 1]}

        assert_equal((None, VALUE), ReprValueProtocol.read(repr(VALUE)))
        assert_equal(repr(VALUE), ReprValueProtocol.write(None, VALUE))

    def test_bad_data(self):
        assert_cant_decode(ReprValueProtocol, '{@#$@#!^&*$%^')

    def test_can_encode_point_but_not_decode(self):
        points_encoded = ReprValueProtocol.write(None, Point(1, 4))
        assert_cant_decode(ReprValueProtocol, points_encoded)
