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
        self.runner = MockRunner()
        self.log = self.start(patch('mrjob.logs.mixin.log'))


class InterpretStepLogTestCase(LogInterpretationMixinTestCase):

    def test_step_interpretation(self):
        self.runner._get_step_log_interpretation = Mock(
            return_value=dict(job_id='job_1'))

        log_interpretation = {}

        self.runner._interpret_step_log(log_interpretation)

        self.assertEqual(
            log_interpretation,
            dict(step=dict(job_id='job_1')))

        self.runner._get_step_log_interpretation.assert_called_once_with(
            log_interpretation)

        # calling _interpret_step_log again shouldn't result in another
        # call to _get_step_log_interpretation
        self.runner._interpret_step_log(log_interpretation)
        self.runner._get_step_log_interpretation.assert_called_once_with(
            log_interpretation)

    def test_no_step_interpretation(self):
        self.runner._get_step_log_interpretation = Mock(
            return_value=None)

        log_interpretation = {}

        self.runner._interpret_step_log(log_interpretation)

        self.assertEqual(log_interpretation, {})

        self.runner._get_step_log_interpretation.assert_called_once_with(
            log_interpretation)


class PickCountersTestCase(LogInterpretationMixinTestCase):

    def setUp(self):
        super(PickCountersTestCase, self).setUp()

        self.runner._interpret_history_log = Mock()
        self.runner._interpret_step_log = Mock()

        # no need to mock mrjob.logs.counters._pick_counters();
        # what it does is really straightforward

    def test_counter_already_present(self):
        log_interpretation = dict(
            step=dict(counters={'foo': {'bar': 1}}))

        self.assertEqual(
            self.runner._pick_counters(log_interpretation),
            {'foo': {'bar': 1}})

        # don't log anything if runner._pick_counters() doesn't have
        # to fetch any new information
        self.assertFalse(self.log.info.called)
        self.assertFalse(self.runner._interpret_step_log.called)
        self.assertFalse(self.runner._interpret_history_log.called)

    def test_counter_from_step_logs(self):
        def mock_interpret_step_log(log_interpretation):
            log_interpretation['step'] = dict(
                counters={'foo': {'bar': 1}})

        self.runner._interpret_step_log = Mock(
            side_effect=mock_interpret_step_log)

        log_interpretation = {}

        self.assertEqual(
            self.runner._pick_counters(log_interpretation),
            {'foo': {'bar': 1}})

        self.assertTrue(self.log.info.called)  # 'Attempting to fetch...'
        self.assertTrue(self.runner._interpret_step_log.called)
        self.assertFalse(self.runner._interpret_history_log.called)

    def test_counter_from_history_logs(self):
        def mock_interpret_history_log(log_interpretation):
            log_interpretation['history'] = dict(
                counters={'foo': {'bar': 1}})

        self.runner._interpret_history_log = Mock(
            side_effect=mock_interpret_history_log)

        log_interpretation = {}

        self.assertEqual(
            self.runner._pick_counters(log_interpretation),
            {'foo': {'bar': 1}})

        self.assertTrue(self.log.info.called)  # 'Attempting to fetch...'
        self.assertTrue(self.runner._interpret_step_log.called)
        self.assertTrue(self.runner._interpret_history_log.called)


class PickErrorsTestCase(LogInterpretationMixinTestCase):

    def setUp(self):
        super(PickErrorsTestCase, self).setUp()

        self.runner._interpret_history_log = Mock()
        self.runner._interpret_step_log = Mock()
        self.runner._interpret_task_logs = Mock()

        self.mock_pick_error = self.start(
            patch('mrjob.logs.mixin._pick_error'))

    def test_logs_already_interpreted(self):
        log_interpretation = dict(
            job={}, step={}, task={})

        self.assertEqual(
            self.runner._pick_error(log_interpretation),
            self.mock_pick_error.return_value)

        # don't log a message or interpret logs
        self.assertFalse(self.log.info.called)
        self.assertFalse(self.runner._interpret_history_log.called)
        self.assertFalse(self.runner._interpret_step_log.called)
        self.assertFalse(self.runner._interpret_task_logs.called)

    def _test_interpret_all_logs(self, log_interpretation):
        self.assertEqual(
            self.runner._pick_error(log_interpretation),
            self.mock_pick_error.return_value)

        # log a message ('Scanning logs...') and call _interpret() methods
        self.assertTrue(self.log.info.called)
        self.assertTrue(self.runner._interpret_history_log.called)
        self.assertTrue(self.runner._interpret_step_log.called)
        self.assertTrue(self.runner._interpret_task_logs.called)

    def test_empty_log_interpretation(self):
        self._test_interpret_all_logs({})

    def test_step_log_only(self):
        self._test_interpret_all_logs(dict(step={}))

    def test_step_and_task_logs_only(self):
        self._test_interpret_all_logs(dict(step={}, history={}))

    def test_step_and_task_logs_only(self):
        self._test_interpret_all_logs(dict(step={}, task={}))
