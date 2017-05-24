# Copyright 2016 Yelp
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
from unittest import TestCase

from tests.sandbox import PatcherTestCase
from tests.py2 import Mock
from tests.py2 import patch

from mrjob.logs.bootstrap import _check_for_nonzero_return_code
from mrjob.logs.bootstrap import _match_emr_bootstrap_stderr_path
from mrjob.logs.bootstrap import _interpret_emr_bootstrap_stderr


class CheckForNonzeroReturnCodeTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(
            _check_for_nonzero_return_code(''),
            None)

    def test_nonzero_return_code_on_master_instance(self):
        self.assertEqual(
            _check_for_nonzero_return_code(
                'On the master instance (i-96c21a39), bootstrap action 2'
                ' returned a non-zero return code'),
            # action_num is 0-indexed
            dict(action_num=1, node_id='i-96c21a39'),
        )

    def test_nonzero_return_code_on_two_slave_instances(self):
        self.assertEqual(
            _check_for_nonzero_return_code(
                'On 2 slave instances (including i-105af6bf and i-b659f519),'
                ' bootstrap action 1 returned a non-zero return code'),
            dict(action_num=0, node_id='i-105af6bf')
        )

    def test_failed_to_download_bootstrap_action(self):
        self.assertEqual(
            _check_for_nonzero_return_code(
                'Master instance (i-ec41ed43) failed attempting to download'
                ' bootstrap action 1 file from S3'),
            None)


class MatchEMRBootstrapStderrPathTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(
            _match_emr_bootstrap_stderr_path(''),
            None)

    def test_stderr(self):
        self.assertEqual(
            _match_emr_bootstrap_stderr_path(
                's3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/i-e647eb49/'
                'bootstrap-actions/2/stderr'),
            dict(action_num=1, node_id='i-e647eb49')
        )

    def test_stderr_gz(self):
        self.assertEqual(
            _match_emr_bootstrap_stderr_path(
                's3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/i-e647eb49/'
                'bootstrap-actions/1/stderr.gz'),
            dict(action_num=0, node_id='i-e647eb49')
        )

    def test_syslog(self):
        self.assertEqual(
            _match_emr_bootstrap_stderr_path(
                's3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/i-e647eb49/'
                'bootstrap-actions/1/syslog'),
            None
        )

    def test_filter_by_action_num(self):
        self.assertEqual(
            _match_emr_bootstrap_stderr_path(
                's3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/i-e647eb49/'
                'bootstrap-actions/2/stderr',
                action_num=1),
            dict(action_num=1, node_id='i-e647eb49')
        )

        self.assertEqual(
            _match_emr_bootstrap_stderr_path(
                's3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/i-e647eb49/'
                'bootstrap-actions/2/stderr',
                action_num=0),
            None
        )

    def test_filter_by_node_id(self):
        self.assertEqual(
            _match_emr_bootstrap_stderr_path(
                's3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/i-e647eb49/'
                'bootstrap-actions/2/stderr',
                node_id='i-e647eb49'),
            dict(action_num=1, node_id='i-e647eb49')
        )

        self.assertEqual(
            _match_emr_bootstrap_stderr_path(
                's3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/i-e647eb49/'
                'bootstrap-actions/2/stderr',
                node_id='i-105af6bf'),
            None
        )


class InterpretEMRBootstrapStderrTestCase(PatcherTestCase):

    def setUp(self):
        super(InterpretEMRBootstrapStderrTestCase, self).setUp()

        self.mock_fs = Mock()

        self.mock_parse_task_stderr = self.start(
            patch('mrjob.logs.bootstrap._parse_task_stderr',
                  return_value=dict(message='BOOM!\n')))

        self.mock_cat_log = self.start(patch('mrjob.logs.bootstrap._cat_log'))

    def interpret_bootstrap_stderr(self, matches, **kwargs):
        """Wrap _interpret_emr_bootstrap_stderr(), since fs doesn't matter"""
        return _interpret_emr_bootstrap_stderr(self.mock_fs, matches, **kwargs)

    def test_empty(self):
        self.assertEqual(self.interpret_bootstrap_stderr([]), {})

    def test_single_stderr_log(self):
        self.assertEqual(
            self.interpret_bootstrap_stderr([dict(
                action_num=0,
                node_id='i-b659f519',
                path=('s3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/'
                      'i-b659f519/bootstrap-actions/1/stderr.gz'),
            )]),
            dict(
                errors=[
                    dict(
                        action_num=0,
                        node_id='i-b659f519',
                        task_error=dict(
                            message='BOOM!\n',
                            path=('s3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/'
                                  'i-b659f519/bootstrap-actions/1/stderr.gz'),
                        ),
                    ),
                ],
                partial=True,
            )
        )

    def test_ignore_multiple_matches(self):
        self.assertEqual(
            self.interpret_bootstrap_stderr([
                dict(
                    action_num=0,
                    node_id='i-b659f519',
                    path=('s3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/'
                          'i-b659f519/bootstrap-actions/1/stderr.gz'),
                ),
                dict(
                    action_num=0,
                    node_id='i-e647eb49',
                    path=('s3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/'
                          'i-e647eb49/bootstrap-actions/1/stderr.gz'),
                ),

            ]),
            dict(
                errors=[
                    dict(
                        action_num=0,
                        node_id='i-b659f519',
                        task_error=dict(
                            message='BOOM!\n',
                            path=('s3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/'
                                  'i-b659f519/bootstrap-actions/1/stderr.gz'),
                        ),
                    ),
                ],
                partial=True,
            )
        )

        self.mock_cat_log.called_once_with(
            self.mock_fs, ('s3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/'
                           'i-b659f519/bootstrap-actions/1/stderr.gz'))

    def test_use_all_matches(self):
        self.assertEqual(
            self.interpret_bootstrap_stderr(
                [
                    dict(
                        action_num=0,
                        node_id='i-b659f519',
                        path=('s3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/'
                              'i-b659f519/bootstrap-actions/1/stderr.gz'),
                    ),
                    dict(
                        action_num=0,
                        node_id='i-e647eb49',
                        path=('s3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/'
                              'i-e647eb49/bootstrap-actions/1/stderr.gz'),
                    ),
                ],
                partial=False,
            ),
            dict(
                errors=[
                    dict(
                        action_num=0,
                        node_id='i-b659f519',
                        task_error=dict(
                            message='BOOM!\n',
                            path=('s3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/'
                                  'i-b659f519/bootstrap-actions/1/stderr.gz'),
                        ),
                    ),
                    dict(
                        action_num=0,
                        node_id='i-e647eb49',
                        task_error=dict(
                            message='BOOM!\n',
                            path=('s3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/'
                                  'i-e647eb49/bootstrap-actions/1/stderr.gz'),
                        ),
                    ),
                ],
            )
        )

    maxDiff = None

    def test_skip_blank_log(self):
        self.mock_parse_task_stderr.side_effect = [
            None,
            dict(message='ARGH!\n')
        ]

        self.assertEqual(
            self.interpret_bootstrap_stderr([
                dict(
                    action_num=0,
                    node_id='i-b659f519',
                    path=('s3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/'
                          'i-b659f519/bootstrap-actions/1/stderr.gz'),
                ),
                dict(
                    action_num=0,
                    node_id='i-e647eb49',
                    path=('s3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/'
                          'i-e647eb49/bootstrap-actions/1/stderr.gz'),
                ),

            ]),
            dict(
                errors=[
                    dict(
                        action_num=0,
                        node_id='i-e647eb49',
                        task_error=dict(
                            message='ARGH!\n',
                            path=('s3://bucket/tmp/logs/j-1EE0CL1O7FDXU/node/'
                                  'i-e647eb49/bootstrap-actions/1/stderr.gz'),
                        ),
                    ),
                ],
                partial=True,  # we still don't check
            )
        )

        self.assertEqual(self.mock_cat_log.call_count, 2)
