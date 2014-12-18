# Copyright 2009-2013 Yelp and Contributors
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

"""Test for the mrjob.version module"""

try:
    import unittest2 as unittest
    unittest  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import unittest

import mrjob
import mrjob.version


class VersionTestCase(unittest.TestCase):
    def test_version_module(self):
        self.assertTrue(hasattr(mrjob.version, '__version__'))
        self.assertTrue(hasattr(mrjob, '__version__'))
        self.assertEqual(mrjob.version.__version__, mrjob.__version__)
