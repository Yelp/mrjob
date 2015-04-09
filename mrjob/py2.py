"""Backwards-compatilibity with Python 2.

This is basically a poor man's version of the "future" library. You can see
a list of compatible idioms here:

    http://python-future.org/compatible_idioms.html

Try to use this module sparingly; in most cases, there is an idiom that
works in both versions of Python.

For example, we don't provide an iteritems() wrapper because dict.items()
is perfectly fine in both languages; the additional overhead of creating
a list in Python 2 just isn't going to matter. Surprisingly, mrjob has very
little performance-critical code; it's pretty much just protocols and
MRJob.run_*() that need to be efficient.
"""
import sys

# use this to check if we're in Python 2
IN_PY2 = (sys.version_info[0] == 2)

# Types that only exist in Python 2

# note the "bytes" type exists in both Python 2.6+ and 3, as do b'' literals

if IN_PY2:
    basestring = basestring
    long = long
    unicode = unicode
else:
    basestring = str
    long = int
    unicode = str
