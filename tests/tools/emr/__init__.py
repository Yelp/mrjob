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

from StringIO import StringIO
import sys

from mrjob.emr import EMRJobRunner

from tests.test_emr import MockEMRAndS3TestCase


class ToolTestCase(MockEMRAndS3TestCase):

    def setUp(self):
        super(ToolTestCase, self).setUp()
        self._original_argv = sys.argv
        self._original_stdout = sys.stdout
        self.stdout = StringIO()
        self.stderr = StringIO()

    def tearDown(self):
        super(ToolTestCase, self).tearDown()
        sys.argv = self._original_argv
        sys.stdout = self._original_stdout

    def monkey_patch_argv(self, *args):
        sys.argv = [sys.argv[0]] + list(args)

    def monkey_patch_stdout(self):
        sys.stdout = self.stdout

    def monkey_patch_stderr(self):
        sys.stderr = self.stderr

    def make_job_flow(self, **kwargs):
        self.add_mock_s3_data({'walrus': {}})
        kwargs.update(dict(
            conf_paths=[],
            s3_scratch_uri='s3://walrus/',
            s3_sync_wait_time=0))
        with EMRJobRunner(**kwargs) as runner:
            return runner.make_persistent_job_flow()
