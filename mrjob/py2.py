"""Minimal utilities to make Python work for 2.6+ and 3.3+

Strategies for making `mrjob` work across Python versions:

Bytes vs. Unicode
-----------------

It's tempting to use `from __future__ import unicode_literals` and require
that all non-byte strings be unicode. But that doesn't really make sense for
Python 2, where str (bytes) and unicode can be used interchangeably.

So really our string datatypes fall into two categories, bytes, and
"strings", which are either str (i.e., bytes) or unicode in Python 2, and
str (i.e. unicode) in Python 3.

These things should always be bytes:

- input data files
  - use `'b'` when opening files: `open(..., 'rb')`
  - read data from `sys.stdin.buffer` in Python 3, not `sys.stdin`
- data from subprocesses (this already happens by default)
- file content from our filesystem interfaces.

Instead of using `StringIO` to deal with these, use `io.BytesIO`.

Not that both Python 2.6+ and Python 3.3+ have the `bytes` type and
`b''` constants built-in.

These things should always be strings:

- streams that you print() to (e.g. sys.stdout if you mock it out)
- streams that you log to
- paths
- arguments to commands
- option names

These things are strings because it makes for simpler code:

- contents of config files
- contents of scripts output by mrjob (e.g. the setup wrapper script)
- contents of empty files (`open(..., 'w')` is fine)

Use the `StringIO` from this module to deal with strings (it's
`StringIO.StringIO` in Python 2 and `io.StringIO` in Python 3).

Please use `%` for format strings and not `format()`, which is much more
picky about mixing unicode and bytes.

This module provides a `basestring` type so you can check if something
is a "string".

We don't provide a `unicode` constant:

- Use `not isinstance(..., bytes)` to check if a string is Unicode
- To convert `bytes` to `unicode`, use `.decode('utf-8')`.
- Python 3.3+ has `u''` literals; please use sparingly

Iterables
---------

Using `.iteritems()` or `.itervalues()` in Python 2 to iterate over a
dictionary when you don't need a list is best practice, but it's also (in most
cases) an over-optimization. We'd prefer clean code; just use `.items()`
and `.values()`.

If you *do* every need that extra efficiency `for k in some_dict` does not
create a list in either version of Python, and you for performance-critical
code, it's fine to write custom code for each Python version (use `IN_PY2`
from this module to check which version you're in).

Same goes for `xrange`; just use `range` (this module provides `xrange`
solely to support `ReprProtocol`).

Miscellany
----------

We provide a `long` type (aliased to `int` on Python 3) so you can
check if something is an integer: `isinstance(..., (int, long))`.

Any standard library function starting with "url" (e.g. `urlparse()`) should
be imported from this module.
"""
import sys

# use this to check if we're in Python 2
IN_PY2 = (sys.version_info[0] == 2)

# `basestring`, for `isinstance(..., basestring)`
if IN_PY2:
    basestring = basestring
else:
    basestring = str
basestring

# `long`, for `isinstance(..., (int, long))`
if IN_PY2:
    long = long
else:
    long = int
long

# `StringIO`, for mocking out `sys.stdout`, etc. You probably won't need
# this outside of
if IN_PY2:
    from StringIO import StringIO
else:
    from io import StringIO
StringIO  # quiet, pyflakes

# urlopen()
if IN_PY2:
    from urllib2 import urlopen
else:
    from urllib.request import urlopen
urlopen

# urlparse() (in most cases you should use `mrjob.parse.urlparse()`)
if IN_PY2:
    from urllib import quote
    from urllib import unquote
    from urlparse import urlparse
    from urlparse import ParseResult
else:
    from urllib.parse import quote
    from urllib.parse import unquote
    from urllib.parse import urlparse
    from urllib.parse import ParseResult
quote
unquote
urlparse
ParseResult

# `xrange`, for `ReprProtocol`
#
# Please just use `range` unless you really need the optimization.
if IN_PY2:
    xrange = xrange
else:
    xrange = range
xrange
