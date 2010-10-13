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

"""Protocols for inputting and outputting data from hadoop."""
# don't add imports here that aren't part of the standard Python library,
# since MRJobs need to run in Amazon's generic EMR environment
import cPickle

from mrjob.util import safeeval

try:
    import simplejson as json # preferred because of C speedups
except ImportError:
    import json # built in to Python 2.6 and later

class HadoopStreamingProtocol(object):
    """An ABC that reads & writes the protocol hadoop understands

    This class isn't usable by itself: inherit from it and define
    your own decode & encode functions (see JSONProtocol or
    ReprProtocol as an example).
    """
    @classmethod
    def read(cls, line):
        """Read in a line from hadoop (without trailing \n), and return
        (key, value)."""
        raise NotImplementedError

    @classmethod
    def write(cls, key, value):
        """Write out key and value as a line (without trailing \n)."""
        raise NotImplementedError

class JSONProtocol(HadoopStreamingProtocol):
    """Encoder / decoder for storing data using JSON. The default format
    for output and communication between steps.

    Note that JSON has some limitations; dictionary keys must be strings,
    and there's no distinction between lists and tuples. We use it as
    the default protocol rather than something with more support for Python
    data structures to make it easy to work with other languages."""
    @classmethod
    def read(cls, line):
        key, value = line.split('\t')
        return json.loads(key), json.loads(value)

    @classmethod
    def write(cls, key, value):
        return '%s\t%s' % (json.dumps(key), json.dumps(value))

class JSONValueProtocol(HadoopStreamingProtocol):
    """Encoder / decoder for reading in lines containing a single JSON
    as (None, decoded_json). A useful alternative to RawValueProtocol
    for reading input.
    """
    @classmethod
    def read(cls, line):
        return (None, json.loads(line))

    @classmethod
    def write(cls, key, value):
        return json.dumps(value)

class PickleProtocol(HadoopStreamingProtocol):
    """Encoder / decoder for storing data using cPickle. We also string-escape
    the pickles to avoid having to deal with stray \t and \n characters
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
    """Encoder / decoder for reading in lines containing a single string-escaped
    pickle as (None, unpickled_value). A useful alternative to RawValueProtocol
    for reading input.
    """
    @classmethod
    def read(cls, line):
        return (None, cPickle.loads(line.decode('string_escape')))

    @classmethod
    def write(cls, key, value):
        return cPickle.dumps(value).encode('string_escape')

class RawValueProtocol(HadoopStreamingProtocol):
    """Read in a line as (None, line). Write out key, value as value. The
    default way of reading input files.
    """
    @classmethod
    def read(cls, line):
        return (None, line)

    @classmethod
    def write(cls, key, value):
        return value

class ReprProtocol(HadoopStreamingProtocol):
    """Encoder / decoder for storing basic Python data types using
    safeeval()."""
    @classmethod
    def read(cls, line):
        key, value = line.split('\t')
        return safeeval(key), safeeval(value)

    @classmethod
    def write(cls, key, value):
        return '%s\t%s' % (repr(key), repr(value))

class ReprValueProtocol(HadoopStreamingProtocol):
    """Encoder / decoder for reading in lines containing a single repr
    as (None, evaled_repr). A useful alternative to RawValueProtocol
    for reading input.
    """
    @classmethod
    def read(cls, line):
        return (None, safeeval(line))

    @classmethod
    def write(cls, key, value):
        return repr(value)

PROTOCOL_DICT = {
    'json': JSONProtocol,
    'json_value': JSONValueProtocol,
    'pickle': PickleProtocol,
    'pickle_value': PickleValueProtocol,
    'raw_value': RawValueProtocol,
    'repr': ReprProtocol,
    'repr_value': ReprValueProtocol,
}

DEFAULT_PROTOCOL = 'json'
