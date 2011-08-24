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
import string
from testify import TestCase, assert_equal, assert_raises
from mrjob.protocol import *

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

class TestROT13Protocol(TabSplitProtocol):
    name = "__test_rot13__"
    def __init__(self, *args, **kwargs):
        super(TestROT13Protocol, self).__init__(*args, **kwargs)
        self.reset_counters()

    def reset_counters(self):
        self.load_counter = 0
        self.dump_counter = 0

    def load_from_string(self, string_to_read):
        self.load_counter += 1
        return string_to_read.decode('rot-13')

    def dump_to_string(self, object_to_dump):
        self.dump_counter += 1
        return object_to_dump.encode('rot-13')

class IdenticalReduceKeyTestCase(TestCase):
    protocol = TestROT13Protocol(step_type='R')

    def setUp(self):
        self.protocol.reset_counters()

    def test_round_trip(self):
        for k, v in zip(string.letters, reversed(string.letters)):
            assert_round_trip_ok(self.protocol, k, v)

        total_letters = len(string.letters)
        assert_equal(self.protocol.dump_counter, total_letters * 2)
        assert_equal(self.protocol.load_counter, total_letters * 2)

    def test_load_reducer_key_once(self):
        constant_key = "hello world"
        for current_letter in string.letters:
            encoded_stuff = self.protocol.write(constant_key, current_letter)
            self.protocol.read(encoded_stuff)

        # We need to encode keys and values every time, 2 * N
        # We need to decode keys only 1 time.  Values get decoded N times
        total_letters = len(string.letters)
        assert_equal(self.protocol.dump_counter, total_letters * 2)
        assert_equal(self.protocol.load_counter, total_letters + 1)

class JSONProtocolTestCase(TestCase):
    protocol = JSONProtocol()

    def test_round_trip(self):
        for k, v in SAFE_KEYS_AND_VALUES:
            assert_round_trip_ok(self.protocol, k, v)

    def test_uses_json_format(self):
        KEY = ['a', 1]
        VALUE = {'foo': {'bar': 3}, 'baz': None}
        ENCODED = '["a", 1]\t{"foo": {"bar": 3}, "baz": null}'

        assert_equal((KEY, VALUE), self.protocol.read(ENCODED))
        assert_equal(ENCODED, self.protocol.write(KEY, VALUE))

    def test_tuples_become_lists(self):
        # JSON should convert tuples into lists
        assert_equal(([1,2], [3,4]),
                     self.protocol.read(self.protocol.write((1,2), (3,4))))

    def test_numerical_keys_become_strs(self):
        # JSON should convert numbers to strings when they are dict keys
        assert_equal(({'1': 2}, {'3': 4}),
                     self.protocol.read(self.protocol.write({1: 2}, {3: 4})))

    def test_bad_data(self):
        assert_cant_decode(self.protocol, '{@#$@#!^&*$%^')

    def test_bad_keys_and_values(self):
        # dictionaries have to have strings as keys
        assert_cant_encode(self.protocol, {(1,2): 3}, None)

        # only unicodes (or bytes in utf-8) are allowed
        assert_cant_encode(self.protocol, '0\xa2', '\xe9')

        # sets don't exist in JSON
        assert_cant_encode(self.protocol, set([1]), set())

        # Point class has no representation in JSON
        assert_cant_encode(self.protocol, Point(2, 3), Point(1, 4))

class JSONValueProtocolTestCase(TestCase):
    protocol = JSONValueProtocol()

    def test_round_trip(self):
        for _, v in SAFE_KEYS_AND_VALUES:
            assert_round_trip_ok(self.protocol, None, v)

    def test_uses_json_format(self):
        VALUE = {'foo': {'bar': 3}, 'baz': None, 'quz': ['a', 1]}
        ENCODED = '{"foo": {"bar": 3}, "baz": null, "quz": ["a", 1]}'

        assert_equal((None, VALUE), self.protocol.read(ENCODED))
        assert_equal(ENCODED, self.protocol.write(None, VALUE))

    def test_tuples_become_lists(self):
        # JSON should convert tuples into lists
        assert_equal(
            (None, [3,4]),
            self.protocol.read(self.protocol.write(None, (3,4))))

    def test_numerical_keys_become_strs(self):
        # JSON should convert numbers to strings when they are dict keys
        assert_equal(
            (None, {'3': 4}),
            self.protocol.read(self.protocol.write(None, {3: 4})))

    def test_bad_data(self):
        assert_cant_decode(self.protocol, '{@#$@#!^&*$%^')

    def test_bad_keys_and_values(self):
        # dictionaries have to have strings as keys
        assert_cant_encode(self.protocol, None, {(1,2): 3})

        # only unicodes (or bytes in utf-8) are allowed
        assert_cant_encode(self.protocol, None, '\xe9')

        # sets don't exist in JSON
        assert_cant_encode(self.protocol, None, set())

        # Point class has no representation in JSON
        assert_cant_encode(self.protocol, None, Point(1, 4))

class PickleProtocolTestCase(TestCase):
    protocol = PickleProtocol()

    def test_round_trip(self):
        for k, v in SAFE_KEYS_AND_VALUES:
            assert_round_trip_ok(self.protocol, k, v)
        assert_round_trip_ok(self.protocol, (1, 2), (3, 4))
        assert_round_trip_ok(self.protocol, '0\xa2', '\xe9')
        assert_round_trip_ok(self.protocol, set([1]), set())
        assert_round_trip_ok(self.protocol, Point(2, 3), Point(1, 4))

    def test_bad_data(self):
        assert_cant_decode(self.protocol, '{@#$@#!^&*$%^')

    # no tests of what encoded data looks like; pickle is an opaque protocol

class PickleValueProtocolTestCase(TestCase):
    protocol = PickleValueProtocol()

    def test_round_trip(self):
        for _, v in SAFE_KEYS_AND_VALUES:
            assert_round_trip_ok(self.protocol, None, v)
        assert_round_trip_ok(self.protocol, None, (3, 4))
        assert_round_trip_ok(self.protocol, None, '\xe9')
        assert_round_trip_ok(self.protocol, None, set())
        assert_round_trip_ok(self.protocol, None, Point(1, 4))

    def test_bad_data(self):
        assert_cant_decode(self.protocol, '{@#$@#!^&*$%^')

    # no tests of what encoded data looks like; pickle is an opaque protocol

class RawValueProtocolTestCase(TestCase):
    protocol = RawValueProtocol()

    def test_dumps_keys(self):
        assert_equal(self.protocol.write('foo', 'bar'), 'bar')

    def test_reads_raw_line(self):
        assert_equal(self.protocol.read('foobar'), (None, 'foobar'))

    def test_bytestrings(self):
        assert_round_trip_ok(self.protocol, None, '\xe90\c1a')

    def test_no_strip(self):
        assert_equal(self.protocol.read('foo\t \n\n'), (None, 'foo\t \n\n'))

class ReprProtocolTestCase(TestCase):
    protocol = ReprProtocol()

    def test_round_trip(self):
        for k, v in SAFE_KEYS_AND_VALUES:
            assert_round_trip_ok(self.protocol, k, v)
        assert_round_trip_ok(self.protocol, (1, 2), (3, 4))
        assert_round_trip_ok(self.protocol, '0\xa2', '\xe9')
        assert_round_trip_ok(self.protocol, set([1]), set())

    def test_uses_repr_format(self):
        KEY = ['a', 1]
        VALUE = {'foo': {'bar': 3}, 'baz': None}
        ENCODED = '%r\t%r' % (KEY, VALUE)

        assert_equal((KEY, VALUE), self.protocol.read(ENCODED))
        assert_equal(ENCODED, self.protocol.write(KEY, VALUE))

    def test_bad_data(self):
        assert_cant_decode(self.protocol, '{@#$@#!^&*$%^')

    def test_can_encode_point_but_not_decode(self):
        points_encoded = self.protocol.write(Point(2, 3), Point(1, 4))
        assert_cant_decode(self.protocol, points_encoded)

class ReprValueProtocolTestCase(TestCase):
    protocol = ReprValueProtocol()

    def test_round_trip(self):
        for _, v in SAFE_KEYS_AND_VALUES:
            assert_round_trip_ok(self.protocol, None, v)
        assert_round_trip_ok(self.protocol, None, (3, 4))
        assert_round_trip_ok(self.protocol, None, '\xe9')
        assert_round_trip_ok(self.protocol, None, set())

    def test_uses_repr_format(self):
        VALUE = {'foo': {'bar': 3}, 'baz': None, 'quz': ['a', 1]}

        assert_equal((None, VALUE), self.protocol.read(repr(VALUE)))
        assert_equal(repr(VALUE), self.protocol.write(None, VALUE))

    def test_bad_data(self):
        assert_cant_decode(self.protocol, '{@#$@#!^&*$%^')

    def test_can_encode_point_but_not_decode(self):
        points_encoded = self.protocol.write(None, Point(1, 4))
        assert_cant_decode(self.protocol, points_encoded)
