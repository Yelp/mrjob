"""Backwards-compatilibity with Python 2.6+ (from Python 3.3+)

It's almost always better to use an idiom that works in both flavors of
Python than to fill your code with wrapper functions. Most of mrjob's code
is aimed at launching jobs, so the overhead of, say, creating a list
rather than using an iterator just isn't going to matter.

(Efficiency *does* matter for code that's going to run as many times
as there are lines in your job, such as protocols. In these cases, you
shouldn't be using wrappers anyway; use IN_PY2 and write a separate
efficient version for both flavors of Python.)

Specific suggestions:

Dictionary iterators: iteritems(), itervalues()

    Just use items() or values()

Is it an integer?

    from mrjob.py2 import long
    isinstance(..., (int, long))

Is it a string?

    from mrjob.py2 import basestring
    isinstance(..., basestring)

Is this string unicode?

    not isinstance(..., bytes)

StringIO

    Replace:

        try:
            from cStringIO import StringIO
        except ImportError:
            from StringIO import StringIO

    with:

        from io import BytesIO

    and use BytesIO instead of StringIO. It works the same way, exists
    in Python 3, and appears (in my rudimentary hand-testing) to be faster!

xrange

    Is it going to contain less than a million items? Just use range()

More idioms can be found here:

    http://python-future.org/compatible_idioms.html

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
    xrange = xrange
else:
    basestring = str
    long = int
    unicode = str
    xrange = range


# standard input/output/error in binary mode

if IN_PY2:
    stderr = sys.stderr
    stdin = sys.stdin
    stdout = sys.stdout
else:
    stderr = sys.stderr.buffer
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer
