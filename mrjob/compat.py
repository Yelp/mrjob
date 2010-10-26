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