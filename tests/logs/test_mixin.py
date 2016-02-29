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
from mrjob.logs.mixin import LogInterpretationMixin

from tests.py2 import Mock
from tests.py2 import patch
from tests.sandbox import PatcherTestCase


class MockRunner(Mock, LogInterpretationMixin):
    pass


class LogInterpretationMixinTestCase(PatcherTestCase):

    def setUp(self):
        self.log_interpretation = {}
        self.runner = MockRunner()

        self.log = self.start(patch('mrjob.logs.mixin.log'))


class InterpretStepLogTestCase(LogInterpretationMixinTestCase):

    def test_step_interpretation(self):
        self.runner._get_step_log_interpretation = Mock(
            return_value=dict(job_id='job_1'))

        self.assertEqual(self.log_interpretation, {})

        self.runner._interpret_step_log(self.log_interpretation)

        self.assertEqual(
            self.log_interpretation,
            dict(step=dict(job_id='job_1')))

        self.runner._get_step_log_interpretation.assert_called_once_with(
            self.log_interpretation)

        # calling _interpret_step_log again shouldn't result in another
        # call to _get_step_log_interpretation
        self.runner._interpret_step_log(self.log_interpretation)
        self.runner._get_step_log_interpretation.assert_called_once_with(
            self.log_interpretation)

    def test_no_step_interpretation(self):
        self.runner._get_step_log_interpretation = Mock(
            return_value=None)

        self.assertEqual(self.log_interpretation, {})

        self.runner._interpret_step_log(self.log_interpretation)

        self.assertEqual(self.log_interpretation, {})

        self.runner._get_step_log_interpretation.assert_called_once_with(
            self.log_interpretation)
