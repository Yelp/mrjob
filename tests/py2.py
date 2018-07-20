# Copyright 2015 Yelp
# Copyright 2017 Yelp
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
"""Compatilibity layer for test code, mostly to deal with the fact
that mock is a library in Python 2 but built into Python 3.

Also provides utility code for mocking ``sys.stdout`` and ``sys.stderr``.
"""
import codecs
import sys
from io import BytesIO

from mrjob.py2 import PY2
from mrjob.py2 import StringIO

# mock is built into unittest in Python 3.3+
if sys.version_info < (3, 3):
    import mock
else:
    from unittest import mock

MagicMock = mock.MagicMock
MagicMock

Mock = mock.Mock
Mock

call = mock.call
call

patch = mock.patch
patch


def mock_stdout_or_stderr():
    """Use this to make a mock stdout/stderr (e.g. for a method
    that calls print()).

    In Python 2, this is just a StringIO.

    In Python 3, this is a BytesIO wrapped in a UTF-8 codec writer. You can
    access the BytesIO through the "buffer" attribute.

    In either case, you can get the mock output (as bytes) by calling
    getvalue().
    """
    if PY2:
        return StringIO()

    buf = BytesIO()
    writer = codecs.getwriter('utf_8')(buf)
    writer.buffer = buf
    writer.getvalue = lambda: buf.getvalue()

    return writer
