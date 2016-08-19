# Copyright 2009-2012 Yelp
# Copyright 2015-2016 Yelp
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
import sys

from mrjob.emr import EMRJobRunner

from tests.py2 import mock_stdout_or_stderr
from tests.py2 import patch
from tests.mockboto import MockBotoTestCase


class ToolTestCase(MockBotoTestCase):

    def monkey_patch_argv(self, *args):
        p = patch('sys.argv', [sys.argv[0]] + list(args))
        self.addCleanup(p.stop)
        p.start()

    def monkey_patch_stdout(self):
        p = patch('sys.stdout', mock_stdout_or_stderr())
        self.addCleanup(p.stop)
        p.start()

    def monkey_patch_stderr(self):
        p = patch('sys.stderr', mock_stdout_or_stderr())
        self.addCleanup(p.stop)
        p.start()

    def make_cluster(self, **kwargs):
        self.add_mock_s3_data({'walrus': {}})
        kwargs.update(dict(
            conf_paths=[],
            cloud_tmp_dir='s3://walrus/',
            cloud_fs_sync_secs=0))
        with EMRJobRunner(**kwargs) as runner:
            return runner.make_persistent_cluster()
