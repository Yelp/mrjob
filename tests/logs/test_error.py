# Copyright 2015 Yelp
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
from mrjob.logs.errors import _merge_and_sort_errors
from mrjob.logs.errors import _pick_error
from mrjob.logs.ids import _attempt_id_to_task_id

from tests.py2 import TestCase


class PickErrorTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(_pick_error({}), None)
        # make sure we can handle log interpretations without error
        self.assertEqual(_pick_error(dict(history={})), None)

    def test_pick_most_recent_error(self):
        log_interpretation=dict(
            history=dict(
                errors=[
                    dict(
                        container_id='container_1450486922681_0005_01_000003',
                        hadoop_error=dict(message='BOOM'),
                        task_error=dict(message='things exploding'),
                    ),
                    dict(
                        container_id='container_1450486922681_0005_01_000004',
                        hadoop_error=dict(message='elephant problems'),
                    ),
                ],
            ),
        )

        self.assertEqual(
            _pick_error(log_interpretation),
            dict(
                container_id='container_1450486922681_0005_01_000004',
                hadoop_error=dict(message='elephant problems'),
            ),
        )

    def test_merge_order(self):
        # task logs usually have the best info and should be merged last
        log_interpretation = dict(
            step=dict(
                errors=[
                    dict(
                        container_id='container_1450486922681_0005_01_000004',
                        hadoop_error=dict(message='BOOM'),
                    ),
                ],
            ),
            history=dict(
                errors=[
                    dict(
                        container_id='container_1450486922681_0005_01_000004',
                        hadoop_error=dict(
                            message='BOOM',
                            path='history.jhist',
                        ),
                        split=dict(path='snake_facts.txt'),
                    ),
                ],
            ),
            task=dict(
                errors=[
                    dict(
                        container_id='container_1450486922681_0005_01_000004',
                        hadoop_error=dict(
                            message='BOOM',
                            path='some_syslog',
                        ),
                        task_error=dict(
                            message='exploding snakes, now?!',
                            path='some_stderr',
                        ),
                    ),
                ],
            )
        )

        self.assertEqual(
            _pick_error(log_interpretation),
            dict(
                container_id='container_1450486922681_0005_01_000004',
                hadoop_error=dict(
                    message='BOOM',
                    path='some_syslog',
                ),
                split=dict(path='snake_facts.txt'),
                task_error=dict(
                    message='exploding snakes, now?!',
                    path='some_stderr',
                ),
            ),
        )



    def test_container_to_attempt_id(self):
        container_id = 'container_1449525218032_0005_01_000010'
        attempt_id = 'attempt_1449525218032_0005_m_000000_3'
        task_id = _attempt_id_to_task_id(attempt_id)

        container_to_attempt_id = {container_id: attempt_id}

        log_interpretation = dict(
            history=dict(
                container_to_attempt_id=container_to_attempt_id,
                errors=[
                    dict(
                        attempt_id=attempt_id,
                        hadoop_error=dict(message='SwordsMischiefException'),
                        task_id=task_id,
                    ),
                ],
            ),
            task=dict(
                errors=[
                    dict(
                        container_id=container_id,
                        hadoop_error=dict(message='SwordsMischiefException'),
                        task_error=dict(message='en garde!'),
                    ),
                ],
            ),
        )

        self.assertEqual(
            _pick_error(log_interpretation),
            dict(
                attempt_id=attempt_id,
                container_id=container_id,
                hadoop_error=dict(message='SwordsMischiefException'),
                task_error=dict(message='en garde!'),
                task_id=task_id,
            ))





class MergeAndSortErrorsTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(_merge_and_sort_errors([]), [])

    def test_single_error(self):
        error = dict(
            container_id='container_1450486922681_0005_01_000003',
            hadoop_error=dict(message='BOOM'),
        )

        self.assertEqual(_merge_and_sort_errors([error]), [error])

    def test_merge_errors(self):
        errors = [
            dict(
                container_id='container_1450486922681_0005_01_000003',
                hadoop_error=dict(message='BOOM')
            ),
            dict(  # from a different container, shouldn't be merged
                container_id='container_1450486922681_0005_01_000004',
                hadoop_error=dict(message='bad stuff, maybe?')
            ),
            dict(
                container_id='container_1450486922681_0005_01_000003',
                hadoop_error=dict(
                    message='BOOM',
                    path='history.jhist',
                ),
                split=dict(path='tricky_input')
            ),
            dict(
                container_id='container_1450486922681_0005_01_000003',
                hadoop_error=dict(
                    message='BOOM\n',
                    path='some_syslog',
                ),
                task_error=dict(
                    message='it was probably snakes',
                    path='some_stderr',
                ),
            ),
        ]

        self.assertEqual(
            _merge_and_sort_errors(errors),
            [
                dict(
                    container_id='container_1450486922681_0005_01_000004',
                    hadoop_error=dict(message='bad stuff, maybe?')
                ),
                dict(
                    container_id='container_1450486922681_0005_01_000003',
                    hadoop_error=dict(
                        message='BOOM\n',
                        path='some_syslog',
                    ),
                    split=dict(path='tricky_input'),
                    task_error=dict(
                        message='it was probably snakes',
                        path='some_stderr'
                    ),
                )
            ])

    def test_can_merge_with_incomplete_ids(self):
        # this shouldn't happen if the _interpret_*() methods are
        # written correctly, but just in case

        errors = [
            dict(
                attempt_id='attempt_201512232143_0008_r_000000_0',
                hadoop_error=dict(message='BOOM'),
                split=dict(path='trade_secrets.dat'),
            ),
            dict(
                attempt_id='attempt_201512232143_0008_r_000000_0',
                hadoop_error=dict(message='BOOM'),
                task_id='task_201512232143_0008_r_000000',
            ),
        ]

        self.assertEqual(
            _merge_and_sort_errors(errors),
            [
                dict(
                    attempt_id='attempt_201512232143_0008_r_000000_0',
                    hadoop_error=dict(message='BOOM'),
                    split=dict(path='trade_secrets.dat'),
                    task_id='task_201512232143_0008_r_000000',
                ),
            ]
        )
