# Copyright 2019 Yelp
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
import os
import os.path  # for os.path.sep
import sys
from os.path import dirname
from os.path import relpath
from os.path import sep as path_separator
from unittest import skipIf

import mrjob

from tests.py2 import patch
from tests.sandbox import BasicTestCase

try:
    import setuptools
except ImportError:
    setuptools = None


@skipIf(setuptools is None, 'setuptools not installed')
class SetupDotPyTestCase(BasicTestCase):

    def setUp(self):
        # 'import setup' triggers running the setup script
        if 'setup' in sys.modules:
            del sys.modules['setup']

        self.setup_func = self.start(patch('setuptools.setup'))

    def test_packages(self):
        import setup

        self.assertEqual(self.setup_func.call_count, 1)

        args, kwargs = self.setup_func.call_args

        self.assertEqual(kwargs.get('packages'), mrjob_packages())


def mrjob_packages():
    """the mrjob package and its subpackage, in sorted order"""

    results = set()

    mrjob_pkg_dir = dirname(mrjob.__file__)

    for dir_path, _, filenames in os.walk(mrjob_pkg_dir):
        if '__init__.py' in filenames:
            package_dir = relpath(dir_path, mrjob_pkg_dir)

            if package_dir == '.':
                package = 'mrjob'
            else:
                package = 'mrjob.' + package_dir.replace(os.path.sep, '.')

            results.add(package)

    return sorted(results)
