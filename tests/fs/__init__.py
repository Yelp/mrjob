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
from mrjob.py2 import StringIO

from tests.sandbox import SandboxedTestCase


class MockSubprocessTestCase(SandboxedTestCase):

    def mock_popen(self, module, main_func, env):
        """Main func should take the arguments
        (stdin, stdout, stderr, argv, environ_dict).
        """
        PopenClass = self._make_popen_class(main_func, env)

        original_popen = module.Popen
        module.Popen = PopenClass

        self.addCleanup(setattr, module, 'Popen', original_popen)

    def _make_popen_class(outer, func, env):

        class MockPopen(object):

            def __init__(self, args, stdin=None, stdout=None, stderr=None):
                self.args = args

                # ignore stdin/stdout/stderr
                self.stdin = StringIO()
                self.stdout = StringIO()
                self.stderr = StringIO()

                self._run()

            def _run(self):
                # pre-emptively run the "process"
                self.returncode = func(
                    self.stdin, self.stdout, self.stderr, self.args, env)

                # store the result
                self.stdout_result = self.stdout.getvalue()
                self.stderr_result = self.stderr.getvalue()

                # expose the results as readable file objects
                self.stdout = StringIO(self.stdout_result)
                self.stderr = StringIO(self.stderr_result)

            def communicate(self, stdin=None):
                if stdin is not None:
                    self.stdin = stdin
                    self._run()

                # need to return bytes
                def _to_bytes(x):
                    if isinstance(x, bytes):
                        return x
                    else:
                        return x.encode('latin_1')

                return (_to_bytes(self.stdout_result),
                        _to_bytes(self.stderr_result))

            def wait(self):
                return self.returncode

        return MockPopen
