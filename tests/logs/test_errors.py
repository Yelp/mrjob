# Copyright 2015-2017 Yelp
# Copyright 2018 Yelp
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
from mrjob.logs.errors import _format_error
from mrjob.logs.errors import _merge_and_sort_errors
from mrjob.logs.errors import _pick_error

from tests.logs.test_spark import _MULTI_LINE_ERROR
from tests.logs.test_spark import _MULTI_LINE_WARNING
from tests.logs.test_spark import _SINGLE_LINE_ERROR
from tests.sandbox import BasicTestCase


class PickErrorTestCase(BasicTestCase):

    def test_empty(self):
        self.assertEqual(_pick_error({}), None)
        # make sure we can handle log interpretations without error
        self.assertEqual(_pick_error(dict(history={})), None)

    def test_pick_most_recent_error(self):
        log_interpretation = dict(
            history=dict(
                errors=[
                    dict(
                        container_id='container_1450486922681_0005_01_000003',
                        hadoop_error=dict(message='BOOM'),
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
            )
        )

    def test_task_error_beats_recency(self):
        log_interpretation = dict(
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
                container_id='container_1450486922681_0005_01_000003',
                hadoop_error=dict(message='BOOM'),
                task_error=dict(message='things exploding'),
            )
        )

    def test_task_error_beats_timestamp(self):
        log_interpretation = dict(
            history=dict(
                errors=[
                    dict(
                        container_id='container_1450486922681_0005_01_000003',
                        hadoop_error=dict(message='BOOM'),
                        task_error=dict(message='things exploding'),
                    ),
                    dict(
                        container_id='container_1450489999999_0005_01_000004',
                        hadoop_error=dict(message='elephant problems'),
                    ),
                ],
            ),
        )

        self.assertEqual(
            _pick_error(log_interpretation),
            dict(
                container_id='container_1450486922681_0005_01_000003',
                hadoop_error=dict(message='BOOM'),
                task_error=dict(message='things exploding'),
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
            )
        )

    def test_pick_shortest_spark_error(self):
        log_interpretation = dict(
            step=dict(
                errors=[
                    dict(
                        spark_error=dict(
                            message=_MULTI_LINE_ERROR[37:],
                            start_line=1,
                            num_lines=10,
                        )
                    ),
                    dict(
                        spark_error=dict(
                            message=_MULTI_LINE_WARNING[180:],
                            start_line=12,
                            num_lines=13,
                        )
                    ),
                ]
            )
        )

        self.assertEqual(
            _pick_error(log_interpretation),
            dict(
                spark_error=dict(
                    message=_MULTI_LINE_ERROR[37:],
                    start_line=1,
                    num_lines=10,
                )
            )
        )

    def test_multiline_spark_error_beats_single_line(self):
        log_interpretation = dict(
            step=dict(
                errors=[
                    dict(
                        spark_error=(
                            dict(
                                message=_SINGLE_LINE_ERROR[49:],
                                start_line=0,
                                num_lines=1,
                            )
                        )
                    ),
                    dict(
                        spark_error=dict(
                            message=_MULTI_LINE_WARNING[180:],
                            start_line=12,
                            num_lines=13,
                        )
                    ),
                ]
            )
        )

        self.assertEqual(
            _pick_error(log_interpretation),
            dict(
                spark_error=dict(
                    message=_MULTI_LINE_WARNING[180:],
                    start_line=12,
                    num_lines=13,
                )
            )
        )

    def test_can_get_spark_errors_from_task_logs(self):
        log_interpretation = dict(
            task=dict(
                application_id='application_1566607039137_0001',
                errors=[
                    dict(
                        container_id='container_1450486922681_0005_01_000004',
                        spark_error=dict(
                            message=_MULTI_LINE_ERROR[37:],
                            start_line=1,
                            num_lines=10,
                        ),
                    ),
                ]
            )
        )

        self.assertEqual(
            _pick_error(log_interpretation),
            dict(
                container_id='container_1450486922681_0005_01_000004',
                spark_error=dict(
                    message=_MULTI_LINE_ERROR[37:],
                    start_line=1,
                    num_lines=10,
                )
            )
        )

    def test_spark_error_beats_task_error(self):
        log_interpretation = dict(
            task=dict(
                application_id='application_1566607039137_0001',
                errors=[
                    dict(
                        container_id='container_1450486922681_0001_01_000001',
                        spark_error=dict(
                            message=_MULTI_LINE_ERROR[37:],
                            start_line=1,
                            num_lines=10,
                        ),
                    ),
                    dict(
                        container_id='container_1450486922681_0005_01_000004',
                        task_error=dict(
                            message='exploding snakes, now?!',
                            path='some_stderr',
                        ),
                    ),
                ]
            )
        )

        self.assertEqual(
            _pick_error(log_interpretation),
            dict(
                container_id='container_1450486922681_0001_01_000001',
                spark_error=dict(
                    message=_MULTI_LINE_ERROR[37:],
                    start_line=1,
                    num_lines=10,
                )
            )
        )


class MergeAndSortErrorsTestCase(BasicTestCase):

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
                ),
                dict(
                    container_id='container_1450486922681_0005_01_000004',
                    hadoop_error=dict(message='bad stuff, maybe?')
                ),
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

    def test_attempt_to_container_id(self):
        errors = [
            dict(
                attempt_id='attempt_201512232143_0008_r_000000_0',
                hadoop_error=dict(message='BOOM'),
            ),
            dict(
                attempt_id='attempt_201512232143_0008_r_000000_1',
                hadoop_error=dict(message='BOOM again'),
            ),
            dict(
                container_id='container_1450486922681_0005_01_000003',
                task_error=dict(message='it was probably snakes'),
            ),
        ]

        attempt_to_container_id = {
            'attempt_201512232143_0008_r_000000_1':
            'container_1450486922681_0005_01_000003',
        }

        self.assertEqual(
            _merge_and_sort_errors(errors, attempt_to_container_id),
            [
                dict(
                    attempt_id='attempt_201512232143_0008_r_000000_1',
                    container_id='container_1450486922681_0005_01_000003',
                    hadoop_error=dict(message='BOOM again'),
                    task_error=dict(message='it was probably snakes'),
                ),
                dict(
                    attempt_id='attempt_201512232143_0008_r_000000_0',
                    hadoop_error=dict(message='BOOM'),
                ),
            ],
        )


class FormatErrorTestCase(BasicTestCase):

    def test_empty(self):
        self.assertEqual(_format_error({}), '')

    def test_fall_back_to_json(self):
        self.assertEqual(_format_error([]), '[]')

    def test_hadoop_error(self):
        self.assertEqual(
            _format_error(dict(hadoop_error=dict(
                message='DevastatingJavaException')
            )),
            'DevastatingJavaException')

        self.assertEqual(
            _format_error(dict(hadoop_error=dict(
                message='DevastatingJavaException',
                path='history.jhist'
            ))),
            'DevastatingJavaException\n\n(from history.jhist)')

        self.assertEqual(
            _format_error(dict(hadoop_error=dict(
                message='DevastatingJavaException',
                path='history.jhist',
                start_line=23,
                num_lines=1
            ))),
            'DevastatingJavaException\n\n(from line 24 of history.jhist)')

        self.assertEqual(
            _format_error(dict(hadoop_error=dict(
                message='DevastatingJavaException',
                path='history.jhist',
                start_line=23,
                num_lines=3
            ))),
            'DevastatingJavaException\n\n(from lines 24-26 of history.jhist)')

    def test_task_error(self):
        self.assertEqual(
            _format_error(dict(task_error=dict(
                message='system will self-destruct in 5s'
            ))),
            'system will self-destruct in 5s')

        # everything uses the same code to format path + line range, so
        # don't worry about testing all the options each time
        self.assertEqual(
            _format_error(dict(task_error=dict(
                message='system will self-destruct in 5s',
                path='/path/to/stderr',
                start_line=0,
                num_lines=1))),
            'system will self-destruct in 5s'
            '\n\n(from line 1 of /path/to/stderr)')

    def test_split(self):
        self.assertEqual(
            _format_error(dict(split=dict(
                path='very_troubling.log',
                start_line=665,
                num_lines=334))),
            '\n\nwhile reading input from lines 666-999 of very_troubling.log')

    def test_spark_error(self):
        self.assertEqual(
            _format_error(dict(spark_error=dict(
                message='Task attempt_20190829211242_0004_m_000000_0 aborted.',
                start_line=0,
                num_lines=1,
            ))),
            'Task attempt_20190829211242_0004_m_000000_0 aborted.')

        self.assertEqual(
            _format_error(dict(spark_error=dict(
                message='Task attempt_20190829211242_0004_m_000000_0 aborted.',
                start_line=0,
                num_lines=1,
                path='/path/to/log'
            ))),
            ('Task attempt_20190829211242_0004_m_000000_0 aborted.'
             '\n\n(from line 1 of /path/to/log)'))

    def test_spark_error_hides_other_errors(self):
        self.assertEqual(
            _format_error(dict(
                hadoop_error=dict(
                    message='DevastatingJavaException',
                    path='history.jhist',
                    start_line=23,
                    num_lines=1,
                ),
                spark_error=dict(
                    message='Aborting task',
                    start_line=99,
                    num_lines=1,
                ),
                task_error=dict(
                    message='system will self-destruct in 5s'
                ),
            )),
            'Aborting task')

    def test_trim_spark_stacktrace(self):
        self.assertEqual(
            _format_error(dict(spark_error=dict(
                message=_MULTI_LINE_ERROR[37:],
                start_line=0,
                num_lines=10,
            ))),
            _MULTI_LINE_ERROR[37:423]
        )
