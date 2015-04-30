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
import codecs
from io import BytesIO

from mrjob.py2 import IN_PY2
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
                self.stdin = BytesIO()
                self.stdout = BytesIO()
                self.stderr = BytesIO()

                self._run()

            def _run(self):
                # pre-emptively run the "process"

                # make fake versions of sys.stdout/stderr
                # punting on stdin for now; tests don't care
                stdout = self._stdwriter()
                stderr = self._stdwriter()

                self.returncode = func(
                    self.stdin, stdout, stderr, self.args, env)

                # expose the results as readable file objects
                self.stdout = BytesIO(self._get_writer_value(stdout))
                self.stderr = BytesIO(self._get_writer_value(stderr))

            def _stdwriter(self):
                """Make a fake stdout/err"""
                if IN_PY2:
                    return StringIO()
                else:
                    buf = BytesIO()
                    writer = codecs.getwriter('utf_8')(buf)
                    writer.buffer = buf
                    return writer

            def _get_writer_value(self, writer):
                if IN_PY2:
                    return writer.getvalue()
                else:
                    return writer.buffer.getvalue()

            def communicate(self, input=None):
                # ignoring input for now
                return self.stdout.getvalue(), self.stderr.getvalue()

            def wait(self):
                return self.returncode

        return MockPopen
