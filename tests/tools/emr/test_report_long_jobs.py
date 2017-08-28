# Copyright 2011-2012 Yelp
# Copyright 2015-2017 Yelp
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
"""Very basic tests for the audit_usage script"""
import sys
from datetime import datetime
from datetime import timedelta

from mrjob.py2 import StringIO
from mrjob.tools.emr.report_long_jobs import _find_long_running_jobs
from mrjob.tools.emr.report_long_jobs import main

from tests.mockboto import MockEmrObject
from tests.mockboto import MockBotoTestCase

CLUSTERS = [
    MockEmrObject(
        id='j-STARTING',
        name='mr_grieving',
        status=MockEmrObject(
            state='STARTING',
            timeline=MockEmrObject(
                creationdatetime='2010-06-06T00:05:00Z',
            ),
        ),
        tags=[],
        _steps=[],
    ),
    MockEmrObject(
        id='j-BOOTSTRAPPING',
        name='mr_grieving',
        status=MockEmrObject(
            state='BOOTSTRAPPING',
            timeline=MockEmrObject(
                creationdatetime='2010-06-06T00:05:00Z',
            ),
        ),
        tags=[],
        _steps=[],
    ),
    MockEmrObject(
        id='j-RUNNING1STEP',
        name='mr_grieving',
        status=MockEmrObject(
            state='RUNNING',
            timeline=MockEmrObject(
                creationdatetime='2010-06-06T00:00:00Z',
                readydatetime='2010-06-06T00:15:00Z',
            ),
        ),
        tags=[MockEmrObject(key='my_key', value='my_value')],
        _steps=[
            MockEmrObject(
                name='mr_denial: Step 1 of 5',
                status=MockEmrObject(
                    state='RUNNING',
                    timeline=MockEmrObject(
                        startdatetime='2010-06-06T00:20:00Z',
                    ),
                ),
            ),
        ],
    ),
    MockEmrObject(
        id='j-RUNNING2STEPS',
        name='mr_grieving',
        status=MockEmrObject(
            state='RUNNING',
            timeline=MockEmrObject(
                creationdatetime='2010-06-06T00:00:00Z',
                readydatetime='2010-06-06T00:15:00Z',
            ),
        ),
        tags=[],
        _steps=[
            MockEmrObject(
                name='mr_denial: Step 1 of 5',
                status=MockEmrObject(
                    state='COMPLETED',
                    timeline=MockEmrObject(
                        enddatetime='2010-06-06T00:25:00Z',
                        startdatetime='2010-06-06T00:20:00Z',
                    ),
                ),
            ),
            MockEmrObject(
                name='mr_anger: Step 2 of 5',
                status=MockEmrObject(
                    state='RUNNING',
                    timeline=MockEmrObject(
                        startdatetime='2010-06-06T00:30:00Z',
                    ),
                ),
            ),
        ]
    ),
    MockEmrObject(
        id='j-RUNNINGANDPENDING',
        name='mr_grieving',
        status=MockEmrObject(
            state='RUNNING',
            timeline=MockEmrObject(
                creationdatetime='2010-06-06T00:00:00Z',
                readydatetime='2010-06-06T00:15:00Z',
            ),
        ),
        tags=[],
        _steps=[
            MockEmrObject(
                name='mr_denial: Step 1 of 5',
                status=MockEmrObject(
                    state='COMPLETED',
                    timeline=MockEmrObject(
                        enddatetime='2010-06-06T00:25:00Z',
                        startdatetime='2010-06-06T00:20:00Z',
                    ),
                ),
            ),
            MockEmrObject(
                name='mr_anger: Step 2 of 5',
                status=MockEmrObject(
                    state='RUNNING',
                    timeline=MockEmrObject(
                        startdatetime='2010-06-06T00:30:00Z',
                    ),
                ),
            ),
            MockEmrObject(
                name='mr_bargaining: Step 3 of 5',
                status=MockEmrObject(
                    state='PENDING',
                ),
            ),
        ]
    ),
    MockEmrObject(
        id='j-PENDING1STEP',
        name='mr_grieving',
        status=MockEmrObject(
            state='RUNNING',
            timeline=MockEmrObject(
                creationdatetime='2010-06-06T00:00:00Z',
                readydatetime='2010-06-06T00:15:00Z',
            ),
        ),
        tags=[],
        _steps=[
            MockEmrObject(
                name='mr_bargaining: Step 3 of 5',
                status=MockEmrObject(
                    state='PENDING',
                ),
            ),
        ]
    ),
    MockEmrObject(
        id='j-PENDING2STEPS',
        name='mr_grieving',
        status=MockEmrObject(
            state='RUNNING',
            timeline=MockEmrObject(
                creationdatetime='2010-06-06T00:00:00Z',
                readydatetime='2010-06-06T00:15:00Z',
            ),
        ),
        tags=[],
        _steps=[
            MockEmrObject(
                name='mr_bargaining: Step 3 of 5',
                status=MockEmrObject(
                    state='COMPLETED',
                    timeline=MockEmrObject(
                        enddatetime='2010-06-06T00:35:00Z',
                        startdatetime='2010-06-06T00:20:00Z',
                    ),
                ),
            ),
            MockEmrObject(
                name='mr_depression: Step 4 of 5',
                status=MockEmrObject(
                    state='PENDING',
                ),
            ),
        ]
    ),
    MockEmrObject(
        id='j-COMPLETED',
        name='mr_grieving',
        status=MockEmrObject(
            state='COMPLETED',
            timeline=MockEmrObject(
                creationdatetime='2010-06-06T00:00:00Z',
                readydatetime='2010-06-06T00:15:00Z',
            ),
        ),
        state='COMPLETED',
        tags=[],
        _steps=[
            MockEmrObject(
                name='mr_acceptance: Step 5 of 5',
                status=MockEmrObject(
                    state='COMPLETED',
                    timeline=MockEmrObject(
                        enddatetime='2010-06-06T00:40:00Z',
                        startdatetime='2010-06-06T00:20:00Z',
                    ),
                ),
            ),
        ]
    ),
]

CLUSTERS_BY_ID = dict((cluster.id, cluster) for cluster in CLUSTERS)

CLUSTER_SUMMARIES_BY_ID = dict(
    (cluster.id, MockEmrObject(
        id=cluster.id,
        name=cluster.name,
        status=cluster.status))
    for cluster in CLUSTERS)


class ReportLongJobsTestCase(MockBotoTestCase):

    def setUp(self):
        super(ReportLongJobsTestCase, self).setUp()
        # redirect print statements to self.stdout
        self._real_stdout = sys.stdout
        self.stdout = StringIO()
        sys.stdout = self.stdout

    def tearDown(self):
        sys.stdout = self._real_stdout
        super(ReportLongJobsTestCase, self).tearDown()

    def test_with_no_clusters(self):
        main(['-q', '--no-conf'])  # just make sure it doesn't crash

    def test_with_all_clusters(self):
        for cluster in CLUSTERS:
            self.add_mock_emr_cluster(cluster)

        emr_conn = self.connect_emr()
        emr_conn.run_jobflow('no name',
                             job_flow_role='fake-instance-profile',
                             service_role='fake-service-role')
        main(['-q', '--no-conf'])

        lines = [line for line in StringIO(self.stdout.getvalue())]
        self.assertEqual(len(lines), len(CLUSTERS_BY_ID) - 1)
        self.assertNotIn('j-COMPLETED', self.stdout.getvalue())

    def test_exclude(self):
        for cluster in CLUSTERS:
            self.add_mock_emr_cluster(cluster)

        main(['-q', '--no-conf', '-x', 'my_key,my_value'])

        lines = [line for line in StringIO(self.stdout.getvalue())]
        self.assertEqual(len(lines), len(CLUSTERS_BY_ID) - 2)
        self.assertNotIn('j-COMPLETED', self.stdout.getvalue())
        self.assertNotIn('j-RUNNING1STEP', self.stdout.getvalue())


class FindLongRunningJobsTestCase(MockBotoTestCase):

    maxDiff = None  # show whole diff when tests fail

    def setUp(self):
        super(FindLongRunningJobsTestCase, self).setUp()

        for cluster in CLUSTERS:
            self.add_mock_emr_cluster(cluster)

    def _find_long_running_jobs(self, cluster_summaries, min_time, now):
        emr_conn = self.connect_emr()

        return _find_long_running_jobs(
            emr_conn,
            cluster_summaries,
            min_time=min_time,
            now=now)

    def test_starting(self):
        self.assertEqual(
            list(self._find_long_running_jobs(
                [CLUSTER_SUMMARIES_BY_ID['j-STARTING']],
                min_time=timedelta(hours=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [{'cluster_id': u'j-STARTING',
              'name': u'mr_grieving',
              'state': u'STARTING',
              'time': timedelta(hours=3, minutes=55)}])

    def test_bootstrapping(self):
        self.assertEqual(
            list(self._find_long_running_jobs(
                [CLUSTER_SUMMARIES_BY_ID['j-BOOTSTRAPPING']],
                min_time=timedelta(hours=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [{'cluster_id': u'j-BOOTSTRAPPING',
              'name': u'mr_grieving',
              'state': u'BOOTSTRAPPING',
              'time': timedelta(hours=3, minutes=55)}])

    def test_running_one_step(self):
        self.assertEqual(
            list(self._find_long_running_jobs(
                [CLUSTER_SUMMARIES_BY_ID['j-RUNNING1STEP']],
                min_time=timedelta(hours=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [{'cluster_id': u'j-RUNNING1STEP',
              'name': u'mr_denial: Step 1 of 5',
              'state': u'RUNNING',
              'time': timedelta(hours=3, minutes=40)}])

        # job hasn't been running for 1 day
        self.assertEqual(
            list(self._find_long_running_jobs(
                [CLUSTER_SUMMARIES_BY_ID['j-RUNNING1STEP']],
                min_time=timedelta(days=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [])

    def test_running_two_steps(self):
        self.assertEqual(
            list(self._find_long_running_jobs(
                [CLUSTER_SUMMARIES_BY_ID['j-RUNNING2STEPS']],
                min_time=timedelta(hours=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [{'cluster_id': u'j-RUNNING2STEPS',
              'name': u'mr_anger: Step 2 of 5',
              'state': u'RUNNING',
              'time': timedelta(hours=3, minutes=30)}])

        # job hasn't been running for 1 day
        self.assertEqual(
            list(self._find_long_running_jobs(
                [CLUSTER_SUMMARIES_BY_ID['j-RUNNING2STEPS']],
                min_time=timedelta(days=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [])

    def test_running_and_pending(self):
        self.assertEqual(
            list(self._find_long_running_jobs(
                [CLUSTER_SUMMARIES_BY_ID['j-RUNNINGANDPENDING']],
                min_time=timedelta(hours=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [{'cluster_id': u'j-RUNNINGANDPENDING',
              'name': u'mr_anger: Step 2 of 5',
              'state': u'RUNNING',
              'time': timedelta(hours=3, minutes=30)}])

        # job hasn't been running for 1 day
        self.assertEqual(
            list(self._find_long_running_jobs(
                [CLUSTER_SUMMARIES_BY_ID['j-RUNNINGANDPENDING']],
                min_time=timedelta(days=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [])

    def test_pending_one_step(self):
        self.assertEqual(
            list(self._find_long_running_jobs(
                [CLUSTER_SUMMARIES_BY_ID['j-PENDING1STEP']],
                min_time=timedelta(hours=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [{'cluster_id': u'j-PENDING1STEP',
              'name': u'mr_bargaining: Step 3 of 5',
              'state': u'PENDING',
              'time': timedelta(hours=3, minutes=45)}])

        # job hasn't been running for 1 day
        self.assertEqual(
            list(self._find_long_running_jobs(
                [CLUSTER_SUMMARIES_BY_ID['j-PENDING1STEP']],
                min_time=timedelta(days=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [])

    def test_pending_two_steps(self):
        self.assertEqual(
            list(self._find_long_running_jobs(
                [CLUSTER_SUMMARIES_BY_ID['j-PENDING2STEPS']],
                min_time=timedelta(hours=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [{'cluster_id': u'j-PENDING2STEPS',
              'name': u'mr_depression: Step 4 of 5',
              'state': u'PENDING',
              'time': timedelta(hours=3, minutes=25)}])

        # job hasn't been running for 1 day
        self.assertEqual(
            list(self._find_long_running_jobs(
                [CLUSTER_SUMMARIES_BY_ID['j-PENDING2STEPS']],
                min_time=timedelta(days=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [])

    def test_completed(self):
        self.assertEqual(
            list(self._find_long_running_jobs(
                [CLUSTER_SUMMARIES_BY_ID['j-COMPLETED']],
                min_time=timedelta(hours=1),
                now=datetime(2010, 6, 6, 4)
            )),
            []
        )

    def test_all_together(self):
        self.assertEqual(
            list(self._find_long_running_jobs(
                CLUSTERS,
                min_time=timedelta(hours=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [{'cluster_id': u'j-STARTING',
              'name': u'mr_grieving',
              'state': u'STARTING',
              'time': timedelta(hours=3, minutes=55)},
             {'cluster_id': u'j-BOOTSTRAPPING',
              'name': u'mr_grieving',
              'state': u'BOOTSTRAPPING',
              'time': timedelta(hours=3, minutes=55)},
             {'cluster_id': u'j-RUNNING1STEP',
              'name': u'mr_denial: Step 1 of 5',
              'state': u'RUNNING',
              'time': timedelta(hours=3, minutes=40)},
             {'cluster_id': u'j-RUNNING2STEPS',
              'name': u'mr_anger: Step 2 of 5',
              'state': u'RUNNING',
              'time': timedelta(hours=3, minutes=30)},
             {'cluster_id': u'j-RUNNINGANDPENDING',
              'name': u'mr_anger: Step 2 of 5',
              'state': u'RUNNING',
              'time': timedelta(hours=3, minutes=30)},
             {'cluster_id': u'j-PENDING1STEP',
              'name': u'mr_bargaining: Step 3 of 5',
              'state': u'PENDING',
              'time': timedelta(hours=3, minutes=45)},
             {'cluster_id': u'j-PENDING2STEPS',
              'name': u'mr_depression: Step 4 of 5',
              'state': u'PENDING',
              'time': timedelta(hours=3, minutes=25)}])
