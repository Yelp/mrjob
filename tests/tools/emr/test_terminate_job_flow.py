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

"""Test the job flow termination tool"""

from __future__ import with_statement

from optparse import OptionError

from boto.exception import S3ResponseError

from mrjob.emr import EMRJobRunner
from mrjob.tools.emr.terminate_job_flow import main as terminate_main
from mrjob.tools.emr.terminate_job_flow import make_option_parser

from tests.quiet import no_handlers_for_logger
from tests.tools.emr import ToolTestCase


class TerminateToolTestCase(ToolTestCase):

    def test_make_option_parser(self):
        make_option_parser()
        self.assertEqual(True, True)

    def test_terminate_job_flow(self):
        self.make_job_flow(pool_emr_job_flows=True)
        self.monkey_patch_argv('--verbose', '--no-conf', 'j-MOCKJOBFLOW0')
        self.monkey_patch_stderr()

        terminate_main()

        value = self.stderr.getvalue()
        self.assertIn('Terminating job flow j-MOCKJOBFLOW0', value)
        self.assertIn('Terminated job flow j-MOCKJOBFLOW0', value)
