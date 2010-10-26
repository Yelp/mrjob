# Version number -> executable name
import sys
info = sys.version_info
executable_name = 'python%d.%d' % (info[0], info[1])

# unicode() vs str()
try:
    str_func = unicode
except NameError:
    str_func = str

# location of StringIO
try:
    from cStringIO import StringIO
except ImportError:
    try:
        from StringIO import StringIO
    except ImportError:
        from io import StringIO
