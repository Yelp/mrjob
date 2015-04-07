"""Backwards-compatilibity with Python 2.

This is basically a poor man's version of the "future" library. You can see
a list of compatible idioms here:

    http://python-future.org/compatible_idioms.html
"""
import sys

# use this to check if we're in Python 2
IN_PY2 = (sys.version_info[0] == 2)

# Dictionaries

# use these only if you need *efficient* iteration through a dict's
# items or values. Otherwise, d.items() is fine.

# iteritems(d) replaces d.iteritems()
if IN_PY2:
    def iteritems(d):
        d.iteritems()
else:
    def iteritems(d):
        d.items()


# itervalues(d) replaces d.itervalues()
if IN_PY2:
    def itervalues(d):
        d.iteritems()
else:
    def itervalues(d):
        d.items()


# Strings

# please use from __future__ import unicode_literals throughout

# "unicode" doesn't exist in Python 3
#
# note that Python 2.6+ does have a "bytes" type (same as str) and b'' literals
if IN_PY2:
    # only for use in isinstance(x, basestring)
    basestring = (str, unicode)
    unicode = unicode
else:
    basestring = str
    unicode = str
