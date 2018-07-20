# Copyright 2015-2016 Yelp
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
"""Minimal utilities to make Python work for 2.7+ and 3.4+

Strategies for making `mrjob` work across Python versions:

Bytes vs. Unicode
-----------------

It's tempting to use `from __future__ import unicode_literals` and require
that all non-byte strings be unicode. But that doesn't really make sense for
Python 2, where str (bytes) and unicode can be used interchangeably.

So really our string datatypes fall into two categories, bytes, and
"strings", which is either ``str`` (i.e., bytes) or ``unicode`` in Python 2,
and ``str`` (i.e. unicode) in Python 3.

These things should always be bytes:

- input data files
  - use ``'b'`` when opening files: ``open(..., 'rb')``
  - read data from ``sys.stdin.buffer`` in Python 3, not ``sys.stdin``
- data encoded by protocols
- data from subprocesses (this already happens by default)
- log files parsed by mrjob
- file content from our filesystem interfaces.

Instead of using ``StringIO`` to deal with these, use ``io.BytesIO``.

Note that both Python 2.6+ and Python 3.3+ have the ``bytes`` type and
``b''`` constants built-in.

These things should always be strings:

- streams that you print() to (e.g. ``sys.stdout`` if you mock it out)
- streams that you log to
- paths on filesystem
- URIs
- arguments to commands
- option names
- Hadoop counter names and groups
- Hadoop status messages
- anything else we parse out of log files

These things are strings because it makes for simpler code:

- contents of config files
- contents of scripts output by mrjob (e.g. the setup wrapper script)
- contents of empty files

Use the ``StringIO`` from this module to deal with strings (it's
``StringIO.StringIO`` in Python 2 and ``io.StringIO`` in Python 3).

Please use ``%`` for format strings and not ``format()``, which is much more
picky about mixing unicode and bytes.

We don't provide a ``unicode`` type:

- Use ``isinstance(..., string_types)`` to check if something is a string
- Use ``not isinstance(..., bytes)`` to check if a string is Unicode
- To convert ``bytes`` to ``unicode``, use ``.decode('utf_8')`.
- Python 3.3+ has ``u''`` literals; please use sparingly

If you need to convert bytes of unknown encoding to a string (e.g. to
``print()`` or log them), use ``to_string()`` from this module.

Iterables
---------

Using ``.iteritems()`` or ``.itervalues()`` in Python 2 to iterate over a
dictionary when you don't need a list is best practice, but it's also (in most
cases) an over-optimization. We'd prefer clean code; just use ``.items()``
and ``.values()``.

If you *do* have concerns about memory usage, ``for k in some_dict`` does not
create a list in either version of Python.

Same goes for ``xrange``; plain-old `range`` is almost always fine.

Miscellany
----------

We provide an ``integer_types`` tuple so you can check if something is an
integer: ``isinstance(..., integer_types)``.

Any standard library function that deals with URLs (e.g. ``urlparse()``) should
probably be imported from this module.

You *usually* want to do ``from __future__ import print_function`` in modules
where you use ``print()``. ``print(...)`` works fine, but
``print(..., file=...)`` doesn't, and ``print()`` prints ``()`` on Python 2.

You shouldn't need any other ``__future__`` imports.
"""
import sys

# use this to check if we're in Python 2
PY2 = (sys.version_info[0] == 2)

# ``string_types``, for ``isinstance(..., string_types)``
if PY2:
    string_types = (basestring,)
else:
    string_types = (str,)
string_types

# ``integer_types``, for ``isinstance(..., integer_types)``
if PY2:
    integer_types = (int, long)
else:
    integer_types = (int,)
integer_types

# ``StringIO``. Useful for mocking out ``sys.stdout``, etc.
if PY2:
    from StringIO import StringIO
else:
    from io import StringIO
StringIO  # quiet, pyflakes

# ``xrange``. Plain old ``range`` is almost always fine
if PY2:
    xrange = xrange
else:
    xrange = range
xrange  # quiet, pyflakes

# urllib stuff
# in most cases you should use ``mrjob.parse.urlparse()``
if PY2:
    from urlparse import ParseResult
    from urllib import quote
    from urllib import unquote
    from urllib2 import urlopen
    from urlparse import urlparse
else:
    from urllib.parse import ParseResult
    from urllib.parse import quote
    from urllib.parse import unquote
    from urllib.request import urlopen
    from urllib.parse import urlparse
ParseResult
quote
unquote
urlopen
urlparse


def to_string(s):
    """Convert ``bytes`` to ``str``, leaving ``unicode`` unchanged.

    (This means on Python 2, ``to_string()`` does nothing.)

    Use this if you need to ``print()`` or log bytes of an unknown encoding,
    or to parse strings out of bytes of unknown encoding (e.g. a log file).
    """
    if not isinstance(s, string_types + (bytes,)):
        raise TypeError

    if PY2 or isinstance(s, str):
        return s

    try:
        return s.decode('utf_8')
    except UnicodeDecodeError:
        return s.decode('latin_1')
