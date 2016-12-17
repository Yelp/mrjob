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
from copy import deepcopy

from mrjob.logs.mixin import LogInterpretationMixin
from mrjob.logs.mixin import _log_parsing_task_log

from tests.py2 import Mock
from tests.py2 import patch
from tests.sandbox import PatcherTestCase


class LogInterpretationMixinTestCase(PatcherTestCase):

    class MockRunner(Mock, LogInterpretationMixin):
        pass

    def setUp(self):
        self.runner = self.MockRunner()
        self.log = self.start(patch('mrjob.logs.mixin.log'))


class InterpretHistoryLogTestCase(LogInterpretationMixinTestCase):

    def setUp(self):
        super(InterpretHistoryLogTestCase, self).setUp()

        self.runner._ls_history_logs = Mock()
        self._interpret_history_log = (
            self.start(patch('mrjob.logs.mixin._interpret_history_log')))

    def test_history_interpretation_already_filled(self):
        log_interpretation = dict(history={})

        self.runner._interpret_history_log(log_interpretation)

        self.assertEqual(
            log_interpretation, dict(history={}))

        self.assertFalse(self.log.warning.called)
        self.assertFalse(self._interpret_history_log.called)
        self.assertFalse(self.runner._ls_history_logs.called)

    def test_no_job_id(self):
        log_interpretation = dict(step={})

        self.runner._interpret_history_log(log_interpretation)

        self.assertEqual(
            log_interpretation, dict(step={}))

        self.assertTrue(self.log.warning.called)
        self.assertFalse(self._interpret_history_log.called)
        self.assertFalse(self.runner._ls_history_logs.called)

    def test_with_job_id(self):
        self._interpret_history_log.return_value = dict(
            counters={'foo': {'bar': 1}})

        log_interpretation = dict(step=dict(job_id='job_1'))

        self.runner._interpret_history_log(log_interpretation)

        self.assertEqual(
            log_interpretation,
            dict(step=dict(job_id='job_1'),
                 history=dict(counters={'foo': {'bar': 1}})))

        self.assertFalse(self.log.warning.called)
        self.runner._ls_history_logs.assert_called_once_with(
            job_id='job_1', output_dir=None)
        self._interpret_history_log.assert_called_once_with(
            self.runner.fs, self.runner._ls_history_logs.return_value)

    def test_with_job_id_and_output_dir(self):
        self._interpret_history_log.return_value = dict(
            counters={'foo': {'bar': 1}})

        log_interpretation = dict(
            step=dict(job_id='job_1', output_dir='hdfs:///path/'))

        self.runner._interpret_history_log(log_interpretation)

        self.assertEqual(
            log_interpretation,
            dict(step=dict(job_id='job_1', output_dir='hdfs:///path/'),
                 history=dict(counters={'foo': {'bar': 1}})))

        self.assertFalse(self.log.warning.called)
        self.runner._ls_history_logs.assert_called_once_with(
            job_id='job_1', output_dir='hdfs:///path/')
        self._interpret_history_log.assert_called_once_with(
            self.runner.fs, self.runner._ls_history_logs.return_value)


class InterpretStepLogTestCase(LogInterpretationMixinTestCase):

    def setUp(self):
        super(InterpretStepLogTestCase, self).setUp()

        self.runner._get_step_log_interpretation = Mock()

    def test_step_interpretation_already_filled(self):
        log_interpretation = dict(step={})

        self.runner._interpret_step_logs(log_interpretation, 'streaming')

        self.assertEqual(
            log_interpretation, dict(step={}))

        self.assertFalse(self.runner._get_step_log_interpretation.called)

    def test_step_interpretation(self):
        self.runner._get_step_log_interpretation.return_value = dict(
            job_id='job_1')

        log_interpretation = {}

        self.runner._interpret_step_logs(log_interpretation, 'streaming')

        self.assertEqual(
            log_interpretation,
            dict(step=dict(job_id='job_1')))

        self.runner._get_step_log_interpretation.assert_called_once_with(
            log_interpretation, 'streaming')

    def test_spark_step_interpretation(self):
        self.runner._get_step_log_interpretation.return_value = dict(
            job_id='job_1')

        log_interpretation = {}

        self.runner._interpret_step_logs(log_interpretation, 'spark')

        self.assertEqual(
            log_interpretation,
            dict(step=dict(job_id='job_1')))

        self.runner._get_step_log_interpretation.assert_called_once_with(
            log_interpretation, 'spark')

    def test_no_step_interpretation(self):
        self.runner._get_step_log_interpretation.return_value = None

        log_interpretation = {}

        self.runner._interpret_step_logs(log_interpretation, 'streaming')

        self.assertEqual(log_interpretation, {})

        self.runner._get_step_log_interpretation.assert_called_once_with(
            log_interpretation, 'streaming')


class InterpretTaskLogsTestCase(LogInterpretationMixinTestCase):

    def setUp(self):
        super(InterpretTaskLogsTestCase, self).setUp()

        self.runner._ls_task_logs = Mock()
        self._interpret_task_logs = (
            self.start(patch('mrjob.logs.mixin._interpret_task_logs')))
        self._interpret_spark_task_logs = (
            self.start(patch('mrjob.logs.mixin._interpret_spark_task_logs')))
        self.runner.get_hadoop_version = Mock(return_value='2.7.1')

    def _test_task_interpretation_already_filled(
            self, log_interpretation, step_type='streaming', **kwargs):
        orig_log_interpretation = deepcopy(log_interpretation)

        self.runner._interpret_task_logs(
            log_interpretation, step_type, **kwargs)

        self.assertEqual(log_interpretation, orig_log_interpretation)

        self.assertFalse(self.log.warning.called)
        self.assertFalse(self._interpret_task_logs.called)
        self.assertFalse(self.runner._ls_task_logs.called)
        self.assertFalse(self.runner._ls_spark_task_logs.called)

    def test_task_interpretation_already_filled(self):
        log_interpretation = dict(task={})

        self._test_task_interpretation_already_filled(log_interpretation)

    def test_spark_task_interpretation_already_filled(self):
        log_interpretation = dict(task={})

        self._test_task_interpretation_already_filled(log_interpretation,
                                                      'spark')

    def test_task_interpretation_already_partially_filled(self):
        log_interpretation = dict(task=dict(partial=True))

        self._test_task_interpretation_already_filled(log_interpretation)

    def test_task_interpretation_already_fully_filled(self):
        # if partial=False, only accept complete task interpretations
        log_interpretation = dict(task={})

        self._test_task_interpretation_already_filled(
            log_interpretation, partial=False)

    def test_replace_partial_interpretation_with_full(self):
        log_interpretation = dict(
            step=dict(application_id='app_id'),
            task=dict(partial=True))

        self.runner._interpret_task_logs(
            log_interpretation, 'streaming', partial=False)

        self.assertTrue(self.runner._ls_task_logs.called)
        self.assertTrue(self._interpret_task_logs.called)

    def test_application_id(self):
        self._interpret_task_logs.return_value = dict(
            counters={'foo': {'bar': 1}})

        log_interpretation = dict(step=dict(application_id='app_1'))

        self.runner._interpret_task_logs(log_interpretation, 'streaming')

        self.assertEqual(
            log_interpretation,
            dict(step=dict(application_id='app_1'),
                 task=dict(counters={'foo': {'bar': 1}})))

        self.assertFalse(self.log.warning.called)
        self.runner._ls_task_logs.assert_called_once_with(
            'streaming',
            application_id='app_1',
            job_id=None,
            output_dir=None)
        self._interpret_task_logs.assert_called_once_with(
            self.runner.fs,
            self.runner._ls_task_logs.return_value,
            partial=True,
            log_callback=_log_parsing_task_log)

    def test_spark(self):
        # don't need to test spark with job_id, since it doesn't run
        # in Hadoop 1
        self._interpret_spark_task_logs.return_value = dict(
            counters={'foo': {'bar': 1}})

        log_interpretation = dict(step=dict(application_id='app_1'))

        self.runner._interpret_task_logs(log_interpretation, 'spark')

        self.assertEqual(
            log_interpretation,
            dict(step=dict(application_id='app_1'),
                 task=dict(counters={'foo': {'bar': 1}})))

        self.assertFalse(self.log.warning.called)
        self.runner._ls_task_logs.assert_called_once_with(
            'spark',
            application_id='app_1',
            job_id=None,
            output_dir=None)
        self._interpret_spark_task_logs.assert_called_once_with(
            self.runner.fs,
            self.runner._ls_task_logs.return_value,
            partial=True,
            log_callback=_log_parsing_task_log)

    def test_job_id(self):
        self.runner.get_hadoop_version.return_value = '1.0.3'

        self._interpret_task_logs.return_value = dict(
            counters={'foo': {'bar': 1}})

        log_interpretation = dict(step=dict(job_id='job_1'))

        self.runner._interpret_task_logs(log_interpretation, 'streaming')

        self.assertEqual(
            log_interpretation,
            dict(step=dict(job_id='job_1'),
                 task=dict(counters={'foo': {'bar': 1}})))

        self.assertFalse(self.log.warning.called)
        self.runner._ls_task_logs.assert_called_once_with(
            'streaming',
            application_id=None,
            job_id='job_1',
            output_dir=None)
        self._interpret_task_logs.assert_called_once_with(
            self.runner.fs,
            self.runner._ls_task_logs.return_value,
            partial=True,
            log_callback=_log_parsing_task_log)

    def test_output_dir(self):
        self._interpret_task_logs.return_value = dict(
            counters={'foo': {'bar': 1}})

        log_interpretation = dict(
            step=dict(application_id='app_1', output_dir='hdfs:///path/'))

        self.runner._interpret_task_logs(log_interpretation, 'streaming')

        self.assertEqual(
            log_interpretation,
            dict(step=dict(application_id='app_1', output_dir='hdfs:///path/'),
                 task=dict(counters={'foo': {'bar': 1}})))

        self.assertFalse(self.log.warning.called)
        self.runner._ls_task_logs.assert_called_once_with(
            'streaming',
            application_id='app_1',
            job_id=None,
            output_dir='hdfs:///path/')
        self._interpret_task_logs.assert_called_once_with(
            self.runner.fs,
            self.runner._ls_task_logs.return_value,
            partial=True,
            log_callback=_log_parsing_task_log)

    def test_missing_application_id(self):
        log_interpretation = dict(step=dict(job_id='job_1'))

        self.runner._interpret_task_logs(log_interpretation, 'streaming')

        self.assertEqual(
            log_interpretation, dict(step=dict(job_id='job_1')))

        self.assertTrue(self.log.warning.called)
        self.assertFalse(self._interpret_task_logs.called)
        self.assertFalse(self.runner._ls_task_logs.called)

    def test_missing_job_id(self):
        self.runner.get_hadoop_version.return_value = '1.0.3'

        log_interpretation = dict(step=dict(app_id='app_1'))

        self.runner._interpret_task_logs(log_interpretation, 'streaming')

        self.assertEqual(
            log_interpretation,
            dict(step=dict(app_id='app_1')))

        self.assertTrue(self.log.warning.called)
        self.assertFalse(self._interpret_task_logs.called)
        self.assertFalse(self.runner._ls_task_logs.called)


class PickCountersTestCase(LogInterpretationMixinTestCase):

    def setUp(self):
        super(PickCountersTestCase, self).setUp()

        self.runner._interpret_history_log = Mock()
        self.runner._interpret_step_logs = Mock()

        # no need to mock mrjob.logs.counters._pick_counters();
        # what it does is really straightforward

    def test_counters_already_present(self):
        log_interpretation = dict(
            step=dict(counters={'foo': {'bar': 1}}))

        self.assertEqual(
            self.runner._pick_counters(log_interpretation, 'streaming'),
            {'foo': {'bar': 1}})

        # don't log anything if runner._pick_counters() doesn't have
        # to fetch any new information
        self.assertFalse(self.log.info.called)
        self.assertFalse(self.runner._interpret_step_logs.called)
        self.assertFalse(self.runner._interpret_history_log.called)

    def test_counters_from_step_logs(self):
        def mock_interpret_step_logs(log_interpretation, step_type):
            log_interpretation['step'] = dict(
                counters={'foo': {'bar': 1}})

        self.runner._interpret_step_logs = Mock(
            side_effect=mock_interpret_step_logs)

        log_interpretation = {}

        self.assertEqual(
            self.runner._pick_counters(log_interpretation, 'streaming'),
            {'foo': {'bar': 1}})

        self.assertTrue(self.log.info.called)  # 'Attempting to fetch...'
        self.assertTrue(self.runner._interpret_step_logs.called)
        self.assertFalse(self.runner._interpret_history_log.called)

    def test_counters_from_history_logs(self):
        def mock_interpret_history_log(log_interpretation):
            log_interpretation['history'] = dict(
                counters={'foo': {'bar': 1}})

        self.runner._interpret_history_log = Mock(
            side_effect=mock_interpret_history_log)

        log_interpretation = {}

        self.assertEqual(
            self.runner._pick_counters(log_interpretation, 'streaming'),
            {'foo': {'bar': 1}})

        self.assertTrue(self.log.info.called)  # 'Attempting to fetch...'
        self.assertTrue(self.runner._interpret_step_logs.called)
        self.assertTrue(self.runner._interpret_history_log.called)

    def test_do_nothing_for_spark_logs(self):
        log_interpretation = {}

        self.assertEqual(
            self.runner._pick_counters(log_interpretation, 'spark'),
            {})

        self.assertFalse(self.log.info.called)  # 'Attempting to fetch...'
        self.assertFalse(self.runner._interpret_step_logs.called)
        self.assertFalse(self.runner._interpret_history_log.called)


class LsHistoryLogsTestCase(LogInterpretationMixinTestCase):

    def setUp(self):
        super(LsHistoryLogsTestCase, self).setUp()

        self._ls_history_logs = self.start(patch(
            'mrjob.logs.mixin._ls_history_logs'))
        self.runner._stream_history_log_dirs = Mock()

    def test_basic(self):
        # the _ls_history_logs() method is a very thin wrapper. Just
        # verify that the keyword args get passed through and
        # that logging happens in the right order

        self._ls_history_logs.return_value = [
            dict(path='hdfs:///history/history.jhist'),
        ]

        results = self.runner._ls_history_logs(
            job_id='job_1', output_dir='hdfs:///output/')

        self.assertFalse(self.log.info.called)

        self.assertEqual(next(results),
                         dict(path='hdfs:///history/history.jhist'))

        self.runner._stream_history_log_dirs.assert_called_once_with(
            output_dir='hdfs:///output/')
        self._ls_history_logs.assert_called_once_with(
            self.runner.fs,
            self.runner._stream_history_log_dirs.return_value,
            job_id='job_1')

        self.assertEqual(self.log.info.call_count, 1)
        self.assertIn('hdfs:///history/history.jhist',
                      self.log.info.call_args[0][0])

        self.assertRaises(StopIteration, next, results)


class LsTaskLogsTestCase(LogInterpretationMixinTestCase):

    def setUp(self):
        super(LsTaskLogsTestCase, self).setUp()

        self._ls_task_logs = self.start(patch(
            'mrjob.logs.mixin._ls_task_logs'))
        self._ls_spark_task_logs = self.start(patch(
            'mrjob.logs.mixin._ls_spark_task_logs'))

        self.runner._stream_task_log_dirs = Mock()

    def test_streaming_step(self):
        # the _ls_task_logs() method is a very thin wrapper. Just
        # verify that the keyword args get passed through and
        # that logging happens in the right order

        self._ls_task_logs.return_value = [
            dict(path='hdfs:///userlogs/1/syslog'),
            dict(path='hdfs:///userlogs/2/syslog'),
        ]

        results = self.runner._ls_task_logs(
            'streaming',
            application_id='app_1',
            job_id='job_1', output_dir='hdfs:///output/')

        self.assertFalse(self.log.info.called)

        self.assertEqual(next(results), dict(path='hdfs:///userlogs/1/syslog'))

        self.runner._stream_task_log_dirs.assert_called_once_with(
            application_id='app_1',
            output_dir='hdfs:///output/')

        self._ls_task_logs.assert_called_once_with(
            self.runner.fs,
            self.runner._stream_task_log_dirs.return_value,
            application_id='app_1',
            job_id='job_1')
        self.assertFalse(self._ls_spark_task_logs.called)

        self.assertEqual(
            list(results),
            [dict(path='hdfs:///userlogs/2/syslog')]
        )

        # unlike most of the _ls_*() methods, logging is handled elsewhere
        # with a callback
        self.assertFalse(self.log.info.called)

    def test_spark_step(self):
        # the _ls_task_logs() method is a very thin wrapper. Just
        # verify that the keyword args get passed through and
        # that logging happens in the right order

        self._ls_spark_task_logs.return_value = [
            dict(path='hdfs:///userlogs/1/stderr'),
            dict(path='hdfs:///userlogs/2/stderr'),
        ]

        results = self.runner._ls_task_logs(
            'spark',
            application_id='app_1',
            job_id='job_1', output_dir='hdfs:///output/')

        self.assertFalse(self.log.info.called)

        self.assertEqual(next(results), dict(path='hdfs:///userlogs/1/stderr'))

        self.runner._stream_task_log_dirs.assert_called_once_with(
            application_id='app_1',
            output_dir='hdfs:///output/')

        self._ls_spark_task_logs.assert_called_once_with(
            self.runner.fs,
            self.runner._stream_task_log_dirs.return_value,
            application_id='app_1',
            job_id='job_1')
        self.assertFalse(self._ls_task_logs.called)

        self.assertEqual(
            list(results),
            [dict(path='hdfs:///userlogs/2/stderr')]
        )

        # unlike most of the _ls_*() methods, logging is handled elsewhere
        # with a callback
        self.assertFalse(self.log.info.called)


class PickErrorTestCase(LogInterpretationMixinTestCase):

    def setUp(self):
        super(PickErrorTestCase, self).setUp()

        self.runner._interpret_history_log = Mock()
        self.runner._interpret_step_logs = Mock()
        self.runner._interpret_task_logs = Mock()

        self._pick_error = self.start(
            patch('mrjob.logs.mixin._pick_error'))

    def test_logs_already_interpreted(self):
        log_interpretation = dict(
            job={}, step={}, task={})

        self.assertEqual(
            self.runner._pick_error(log_interpretation, 'streaming'),
            self._pick_error.return_value)

        # don't log a message or interpret logs
        self.assertFalse(self.log.info.called)
        self.assertFalse(self.runner._interpret_history_log.called)
        self.assertFalse(self.runner._interpret_step_logs.called)
        self.assertFalse(self.runner._interpret_task_logs.called)

    def _test_interpret_all_logs(self, log_interpretation):
        self.assertEqual(
            self.runner._pick_error(log_interpretation, 'streaming'),
            self._pick_error.return_value)

        # log a message ('Scanning logs...') and call _interpret() methods
        self.assertTrue(self.log.info.called)
        self.assertTrue(self.runner._interpret_history_log.called)
        self.assertTrue(self.runner._interpret_step_logs.called)
        self.assertTrue(self.runner._interpret_task_logs.called)

    def test_empty_log_interpretation(self):
        self._test_interpret_all_logs({})

    def test_step_log_only(self):
        self._test_interpret_all_logs(dict(step={}))

    def test_step_and_history_logs_only(self):
        self._test_interpret_all_logs(dict(step={}, history={}))

    def test_step_and_task_logs_only(self):
        self._test_interpret_all_logs(dict(step={}, task={}))
