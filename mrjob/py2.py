"""Minimal utilities to make Python work for 2.6+ and 3.3+

Strategies for making `mrjob` work across Python versions:

Bytes vs. Unicode
-----------------

It's tempting to use `from __future__ import unicode_literals` and require
that all non-byte strings be unicode. But that doesn't really make sense for
Python 2, where str (bytes) and unicode can be used interchangeably.

So really our string datatypes fall into two categories, bytes, and
"strings", which are either ``str`` (i.e., bytes) or ``unicode`` in Python 2,
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

Not that both Python 2.6+ and Python 3.3+ have the ``bytes`` type and
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

This module provides a ``string_types`` tuple so you can check if something
is a "string".

We don't provide a ``unicode`` constant:

- Use ``not isinstance(..., bytes)`` to check if a string is Unicode
- To convert ``bytes`` to ``unicode``, use ``.decode('utf_8')`.
- Python 3.3+ has ``u''`` literals; please use sparingly

If you need to convert bytes of unknown encoding to a "string" (e.g.
to ``print()`` or log them), you can use ``to_string()``.

Iterables
---------

Using ``.iteritems()`` or ``.itervalues()`` in Python 2 to iterate over a
dictionary when you don't need a list is best practice, but it's also (in most
cases) an over-optimization. We'd prefer clean code; just use ``.items()``
and ``.values()``.

If you *do* every need that extra efficiency ``for k in some_dict`` does not
create a list in either version of Python, and you for performance-critical
code, it's fine to write custom code for each Python version (use ``PY2``
from this module to check which version you're in).

Same goes for ``xrange``; just use ``range`` (this module provides ``xrange``
solely to support ``ReprProtocol``).

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

# urlparse() (in most cases you should use ``mrjob.parse.urlparse()``)
if PY2:
    from urllib import quote
    from urllib import unquote
    from urllib2 import urlopen
    from urlparse import urlparse
    from urlparse import ParseResult
else:
    from urllib.parse import quote
    from urllib.parse import unquote
    from urllib.request import urlopen
    from urllib.parse import urlparse
    from urllib.parse import ParseResult
quote
unquote
urlopen
urlparse
ParseResult

# ``xrange``, for ``ReprProtocol``
#
# Please just use ``range`` unless you really need the optimization.
if PY2:
    xrange = xrange
else:
    xrange = range
xrange


def to_string(s):
    """On Python 3, convert ``bytes`` to ``str`` (i.e. unicode).

    (In Python 2, either ``str`` or ``unicode`` is acceptable as a string.)

    Use this if you need to ``print()`` or log bytes of an unknown encoding,
    or to parse strings out of bytes of unknown encoding (e.g. a log file).
    """
    if not isinstance(s, (bytes, str)):
        raise TypeError

    if PY2 or isinstance(s, str):
        return s

    try:
        return s.decode('utf_8')
    except UnicodeDecodeError:
        return s.decode('latin_1')
