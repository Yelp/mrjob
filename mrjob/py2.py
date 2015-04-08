"""Backwards-compatilibity with Python 2.

This is basically a poor man's version of the "future" library. You can see
a list of compatible idioms here:

    http://python-future.org/compatible_idioms.html
"""
import sys

# use this to check if we're in Python 2
IN_PY2 = (sys.version_info[0] == 2)


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
