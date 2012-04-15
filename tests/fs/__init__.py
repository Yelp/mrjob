# Copyright 2009-2012 Yelp
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
from __future__ import with_statement

import os
from shutil import rmtree
from StringIO import StringIO
import subprocess
from tempfile import mkdtemp

try:
    from unittest2 import TestCase
except ImportError:
    from unittest2 import TestCase


class TempdirTestCase(TestCase):

    def setUp(self):
        self.root = mkdtemp()
        self.addCleanup(rmtree, self.root)

    def makedirs(self, path):
        abs_path = os.path.join(self.root, path)
        if not os.path.isdir(abs_path):
            os.makedirs(abs_path)
        return abs_path

    def makefile(self, path, contents):
        self.makedirs(os.path.split(path)[0])
        abs_path = os.path.join(self.root, path)
        with open(abs_path, 'w') as f:
            f.write(contents)
        return abs_path

    def abs_paths(self, *paths):
        return [os.path.join(self.root, path) for path in paths]


class MockSubprocessTestCase(TempdirTestCase):

    def mock_popen(self, module, main_func):
        """Main func should take the arguments
        (stdin, stdout, stderr, argv, environ_dict).
        """
        self.command_log = []
        self.io_log = []

        PopenClass = self._make_popen_class(main_func)

        original_popen = module.Popen
        module.Popen = PopenClass

        self.addCleanup(setattr, module, 'Popen', original_popen)

    def _make_popen_class(outer, func):

        class MockPopen(object):

            def __init__(self, args, stdin=None, stdout=None, stderr=None):
                self.args = args

                # discard incoming stdin/stdout/stderr objects
                self.stdin = StringIO()
                self.stdout = StringIO()
                self.stderr = StringIO()

                # pre-emptively run the "process"
                self.returncode = func(
                    self.stdin, self.stdout, self.stderr, self.args,
                    outer.hadoop_env)

                # log what happened
                outer.command_log.append(self.args)
                outer.io_log.append((stdout, stderr))

                # store the result
                self.stdout_result, self.stderr_result = (
                    self.stdout.getvalue(), self.stderr.getvalue())

                # expose the results as readable file objects
                self.stdout = StringIO(self.stdout_result)
                self.stderr = StringIO(self.stderr_result)

            def communicate(self):
                return self.stdout_result, self.stderr_result

            def wait(self):
                return self.returncode

        return MockPopen
