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
"""Test the diagnose script"""

from mrjob.tools.diagnose import main

from tests.mock_boto3 import MockBoto3TestCase
from tests.py2 import patch


class StepPickingTestCase(MockBoto3TestCase):

    def setUp(self):
        super(StepPickingTestCase, self).setUp()

        self.pick_error = self.start(
            patch('mrjob.emr.EMRJobRunner._pick_error',
                  side_effect=StopIteration))

        self.log = self.start(
            patch('mrjob.tools.diagnose.log'))

    def test_single_failed_job(self):
        self.mock_emr_clusters['j-MOCKCLUSTER0'] = dict(
            _Steps=[
                dict(
                    Config=dict(Args=['python', 'mr_bad.py', '--mapper']),
                    Id='s-MOCKSTEP0',
                    Name='mr_failure',
                    Status=dict(State='FAILED'),
                )
            ]
        )

        self.assertRaises(StopIteration, main, ['j-MOCKCLUSTER0'])

        self.pick_error.assert_called_once_with(
            dict(step_id='s-MOCKSTEP0'), 'streaming')



# to test:
#
# - detect failed step in multi-step job
# - detect spark and spark-script steps
# - handle
