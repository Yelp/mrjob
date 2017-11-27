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

from mrjob.tools.diagnose import main as diagnose_main

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

    def test_single_failed_step(self):
        self.mock_emr_clusters['j-MOCKCLUSTER0'] = dict(
            _Steps=[
                dict(
                    Config=dict(Args=['python', 'mr_failure.py', '--mapper']),
                    Id='s-MOCKSTEP0',
                    Name='mr_failure',
                    Status=dict(State='FAILED'),
                )
            ]
        )

        self.assertRaises(StopIteration, diagnose_main, ['j-MOCKCLUSTER0'])

        self.pick_error.assert_called_once_with(
            dict(step_id='s-MOCKSTEP0'), 'streaming')

    def test_failed_step_followed_by_successful_step(self):
        self.mock_emr_clusters['j-MOCKCLUSTER0'] = dict(
            _Steps=[
                dict(
                    Config=dict(Args=['python', 'mr_failure.py', '--mapper']),
                    Id='s-MOCKSTEP0',
                    Name='mr_failure',
                    Status=dict(State='FAILED'),
                ),
                dict(
                    Config=dict(Args=['python', 'mr_success.py', '--mapper']),
                    Id='s-MOCKSTEP1',
                    Name='mr_success',
                    Status=dict(State='COMPLETED'),
                ),
            ]
        )

        self.assertRaises(StopIteration, diagnose_main, ['j-MOCKCLUSTER0'])

        self.pick_error.assert_called_once_with(
            dict(step_id='s-MOCKSTEP0'), 'streaming')

    def test_two_failed_steps(self):
        self.mock_emr_clusters['j-MOCKCLUSTER0'] = dict(
            _Steps=[
                dict(
                    Config=dict(Args=['python', 'mr_failure.py', '--mapper']),
                    Id='s-MOCKSTEP0',
                    Name='mr_failure',
                    Status=dict(State='FAILED'),
                ),
                dict(
                    Config=dict(Args=['python', 'mr_failure.py', '--mapper']),
                    Id='s-MOCKSTEP1',
                    Name='mr_failure',
                    Status=dict(State='FAILED'),
                ),
            ]
        )

        self.assertRaises(StopIteration, diagnose_main, ['j-MOCKCLUSTER0'])

        self.pick_error.assert_called_once_with(
            dict(step_id='s-MOCKSTEP1'), 'streaming')

    def test_no_failed_steps(self):
        self.mock_emr_clusters['j-MOCKCLUSTER0'] = dict(
            _Steps=[
                dict(
                    Config=dict(Args=['python', 'mr_success.py', '--mapper']),
                    Id='s-MOCKSTEP0',
                    Name='mr_success',
                    Status=dict(State='COMPLETED'),
                ),
            ]
        )

        self.assertRaises(SystemExit, diagnose_main, ['j-MOCKCLUSTER0'])

        self.assertTrue(self.log.error.called)
        self.assertFalse(self.pick_error.called)

    def test_failed_spark_step(self):
        self.mock_emr_clusters['j-MOCKCLUSTER0'] = dict(
            _Steps=[
                dict(
                    Config=dict(Args=[
                        'custom-spark-submit',
                        '--master',
                        'yarn',
                        '--deploy-mode',
                        'cluster',
                        'custom-spark.jar',
                        'more-args',
                    ]),
                    Id='s-MOCKSTEP0',
                    Name='mr_spark_jar',
                    Status=dict(State='FAILED'),
                ),
            ]
        )

        self.assertRaises(StopIteration, diagnose_main, ['j-MOCKCLUSTER0'])

        # doesn't distinguish between different kinds of spark steps
        self.pick_error.assert_called_once_with(
            dict(step_id='s-MOCKSTEP0'), 'spark')

    def test_pick_step_id(self):
        self.mock_emr_clusters['j-MOCKCLUSTER0'] = dict(
            _Steps=[
                dict(
                    Config=dict(Args=['python', 'mr_failure.py', '--mapper']),
                    Id='s-MOCKSTEP0',
                    Name='mr_failure',
                    Status=dict(State='FAILED'),
                ),
                dict(
                    Config=dict(Args=['python', 'mr_failure.py', '--mapper']),
                    Id='s-MOCKSTEP1',
                    Name='mr_failure',
                    Status=dict(State='FAILED'),
                ),
            ]
        )

        self.assertRaises(StopIteration, diagnose_main,
                          ['j-MOCKCLUSTER0', '--step-id', 's-MOCKSTEP0'])

        self.pick_error.assert_called_once_with(
            dict(step_id='s-MOCKSTEP0'), 'streaming')

    def test_pick_completed_step(self):
        self.mock_emr_clusters['j-MOCKCLUSTER0'] = dict(
            _Steps=[
                dict(
                    Config=dict(Args=['python', 'mr_failure.py', '--mapper']),
                    Id='s-MOCKSTEP0',
                    Name='mr_failure',
                    Status=dict(State='FAILED'),
                ),
                dict(
                    Config=dict(Args=['python', 'mr_success.py', '--mapper']),
                    Id='s-MOCKSTEP1',
                    Name='mr_success',
                    Status=dict(State='COMPLETED'),
                ),
            ]
        )

        self.assertRaises(StopIteration, diagnose_main,
                          ['j-MOCKCLUSTER0', '--step-id', 's-MOCKSTEP1'])

        self.pick_error.assert_called_once_with(
            dict(step_id='s-MOCKSTEP1'), 'streaming')

        self.assertTrue(self.log.warning.called)

    def test_bad_step_id(self):
        self.mock_emr_clusters['j-MOCKCLUSTER0'] = dict(
            _Steps=[
                dict(
                    Config=dict(Args=['python', 'mr_failure.py', '--mapper']),
                    Id='s-MOCKSTEP0',
                    Name='mr_failure',
                    Status=dict(State='FAILED'),
                )
            ]
        )

        # shouldn't trigger an error from the EMR API
        self.assertRaises(SystemExit, diagnose_main,
                          ['j-MOCKCLUSTER0', '--step-id', 's-ILLY'])

        self.assertTrue(self.log.error.called)
        self.assertFalse(self.pick_error.called)
