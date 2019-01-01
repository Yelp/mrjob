# -*- coding: utf-8 -*-
# Copyright 2009-2019 Yelp and Contributors
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
"""Tests for YarnEMRJobRunner."""
import copy
import os

from tests.mock_boto3 import MockBoto3TestCase
from tests.mr_null_spark import MRNullSpark
from tests.py2 import Mock
from tests.py2 import patch
from tests.sandbox import mrjob_conf_patcher


class YarnEMRJobRunnerEndToEndTestCase(MockBoto3TestCase):

    MRJOB_CONF_CONTENTS = dict(
        runners=dict(
            yarnemr=dict(
                # yarnemr required
                ec2_key_pair_file='meh',
                expected_cores=1,
                expected_memory=10,
                yarn_logs_output_base='meh'
            ),
        ),
    )

    def setUp(self):
        super(YarnEMRJobRunnerEndToEndTestCase, self).setUp()

        # _spark_submit_args() is tested elsewhere
        self.start(patch(
            'mrjob.bin.MRJobBinRunner._spark_submit_args',
            return_value=['<spark submit args>']))

    def _set_in_mrjob_conf(self, **kwargs):
        emr_opts = copy.deepcopy(self.MRJOB_CONF_CONTENTS)
        emr_opts['runners']['yarnemr'].update(kwargs)
        patcher = mrjob_conf_patcher(emr_opts)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_end_to_end(self):
        self._set_in_mrjob_conf(yarn_logs_output_base=self.tmp_dir)

        input1 = os.path.join(self.tmp_dir, 'input1')
        open(input1, 'w').close()
        input2 = os.path.join(self.tmp_dir, 'input2')
        open(input2, 'w').close()

        job = MRNullSpark(['-r', 'yarnemr', input1, input2])
        job.sandbox()

        with job.make_runner() as runner:
            # skip waiting
            runner._wait_for_cluster = Mock()

            # mock ssh
            runner.fs._ssh_run = Mock()
            mock_stderr = 'whooo stderr Submitting application application_15'\
                          '50537538614_0001 to ResourceManager stderr logs'
            runner.fs._ssh_run.return_value = ('meh stdour', mock_stderr)

            runner._get_application_info = Mock()
            runner._get_application_info.side_effect = [
                {
                  'state': 'NOT FINISHED',
                  'finalStatus': 'NOT SUCCESS',
                  'elapsedTime': 25.12345,
                  'progress': 5000
                },
                {
                  'state': 'FINISHED',
                  'finalStatus': 'SUCCEEDED',
                  'elapsedTime': 50.12345,
                  'progress': 10000
                },
            ]

            runner.run()
