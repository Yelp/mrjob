# Copyright 2012 Yelp
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
"""Test OptionStore and subclasses"""

from __future__ import with_statement

from StringIO import StringIO

try:
    import unittest2 as unittest
    unittest  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import unittest

try:
    import boto
    import boto.emr
    import boto.emr.connection
    import boto.exception
    boto  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    boto = None

from mrjob.emr import EMRJobRunner
from mrjob.runner import RunnerOptionStore
from mrjob.util import log_to_stream
from tests.quiet import no_handlers_for_logger
from tests.sandbox import EmptyMrjobConfTestCase


class RunnerOptionStoreTestCase(EmptyMrjobConfTestCase):

    def _assert_interp(self, val, **kwargs):
        opts = RunnerOptionStore('inline', kwargs, [])
        self.assertEqual(opts['interpreter'], val)

    def test_interpreter_fallback(self):
        self._assert_interp(['python'])

    def test_interpreter_fallback_2(self):
        self._assert_interp(['python', '-v'], python_bin=['python', '-v'])

    def test_interpreter(self):
        self._assert_interp(['ruby'], interpreter=['ruby'])
