"""More Python 2/3 compatibility stuff that is only used for testing."""
from mrjob.py2 import IN_PY2

# a StringIO that you can safely set sys.stdout or sys.stderr to
# (for logging or printing)
#
# Don't use this for mocking out files or subprocess stdout/stderr;
# use io.BytesIO instead
#
# TODO: maybe move this to tests.py2?
if IN_PY2:
    from StringIO import StringIO
else:
    from io import StringIO
