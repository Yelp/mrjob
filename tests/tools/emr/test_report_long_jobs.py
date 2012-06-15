# Copyright 2011 Yelp
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
from datetime import datetime
from datetime import timedelta
from StringIO import StringIO
import sys

from mrjob.emr import EMRJobRunner
from mrjob.tools.emr.report_long_jobs import find_long_running_jobs
from mrjob.tools.emr.report_long_jobs import main
from tests.mockboto import MockEmrObject
from tests.test_emr import MockEMRAndS3TestCase

try:
    import unittest2 as unittest
    unittest  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import unittest


JOB_FLOWS = [
    MockEmrObject(
        creationdatetime='2010-06-06T00:00:00Z',
        jobflowid='j-BOOTSTRAPPING',
        name='mr_grieving',
        startdatetime='2010-06-06T00:05:00Z',
        state='BOOTSTRAPPING',
        steps=[],
    ),
    MockEmrObject(
        creationdatetime='2010-06-06T00:00:00Z',
        jobflowid='j-RUNNING1STEP',
        name='mr_grieving',
        readydatetime='2010-06-06T00:15:00Z',
        state='RUNNING',
        steps=[
            MockEmrObject(
                name='mr_denial: Step 1 of 5',
                startdatetime='2010-06-06T00:20:00Z',
                state='RUNNING',
            ),
        ]
    ),
    MockEmrObject(
        creationdatetime='2010-06-06T00:00:00Z',
        jobflowid='j-RUNNING2STEPS',
        name='mr_grieving',
        readydatetime='2010-06-06T00:15:00Z',
        state='RUNNING',
        steps=[
            MockEmrObject(
                enddatetime='2010-06-06T00:25:00Z',
                name='mr_denial: Step 1 of 5',
                startdatetime='2010-06-06T00:20:00Z',
                state='COMPLETED',
            ),
            MockEmrObject(
                name='mr_anger: Step 2 of 5',
                startdatetime='2010-06-06T00:30:00Z',
                state='RUNNING',
            ),
        ]
    ),
    MockEmrObject(
        creationdatetime='2010-06-06T00:00:00Z',
        jobflowid='j-RUNNINGANDPENDING',
        name='mr_grieving',
        readydatetime='2010-06-06T00:15:00Z',
        state='RUNNING',
        steps=[
            MockEmrObject(
                enddatetime='2010-06-06T00:25:00Z',
                name='mr_denial: Step 1 of 5',
                startdatetime='2010-06-06T00:20:00Z',
                state='COMPLETED',
            ),
            MockEmrObject(
                name='mr_anger: Step 2 of 5',
                startdatetime='2010-06-06T00:30:00Z',
                state='RUNNING',
            ),
            MockEmrObject(
                name='mr_bargaining: Step 3 of 5',
                state='PENDING',
            ),
        ]
    ),
    MockEmrObject(
        creationdatetime='2010-06-06T00:00:00Z',
        jobflowid='j-PENDING1STEP',
        name='mr_grieving',
        readydatetime='2010-06-06T00:15:00Z',
        state='RUNNING',
        steps=[
            MockEmrObject(
                name='mr_bargaining: Step 3 of 5',
                state='PENDING',
            ),
        ]
    ),
    MockEmrObject(
        creationdatetime='2010-06-06T00:00:00Z',
        jobflowid='j-PENDING2STEPS',
        name='mr_grieving',
        readydatetime='2010-06-06T00:15:00Z',
        state='RUNNING',
        steps=[
            MockEmrObject(
                enddatetime='2010-06-06T00:35:00Z',
                name='mr_bargaining: Step 3 of 5',
                state='COMPLETED',
                startdatetime='2010-06-06T00:20:00Z',
            ),
            MockEmrObject(
                name='mr_depression: Step 4 of 5',
                state='PENDING',
            ),
        ]
    ),
    MockEmrObject(
        creationdatetime='2010-06-06T00:00:00Z',
        jobflowid='j-COMPLETED',
        name='mr_grieving',
        readydatetime='2010-06-06T00:15:00Z',
        state='COMPLETED',
        steps=[
            MockEmrObject(
                enddatetime='2010-06-06T00:40:00Z',
                startdatetime='2010-06-06T00:20:00Z',
                name='mr_acceptance: Step 5 of 5',
                state='COMPLETED',
            ),
        ]
    ),
]

JOB_FLOWS_BY_ID = dict((jf.jobflowid, jf) for jf in JOB_FLOWS)


class ReportLongJobsTestCase(MockEMRAndS3TestCase):

    def setUp(self):
        super(ReportLongJobsTestCase, self).setUp()
        # redirect print statements to self.stdout
        self._real_stdout = sys.stdout
        self.stdout = StringIO()
        sys.stdout = self.stdout

    def tearDown(self):
        sys.stdout = self._real_stdout
        super(ReportLongJobsTestCase, self).tearDown()

    def test_with_no_job_flows(self):
        main(['-q', '--no-conf'])  # just make sure it doesn't crash

    def test_with_all_job_flows(self):
        self.mock_emr_job_flows.update(JOB_FLOWS_BY_ID)
        emr_conn = EMRJobRunner(conf_paths=[]).make_emr_conn()
        emr_conn.run_jobflow('no name', log_uri=None)
        main(['-q', '--no-conf'])
        lines = [line for line in StringIO(self.stdout.getvalue())]
        self.assertEqual(len(lines), len(JOB_FLOWS_BY_ID) - 1)


class FindLongRunningJobsTestCase(unittest.TestCase):

    maxDiff = None  # show whole diff when tests fail

    def test_bootstrapping(self):
        self.assertEqual(
            list(find_long_running_jobs(
                [JOB_FLOWS_BY_ID['j-BOOTSTRAPPING']],
                min_time=timedelta(hours=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [{'job_flow_id': 'j-BOOTSTRAPPING',
              'name': 'mr_grieving',
              'step_state': '',
              'time': timedelta(hours=3, minutes=55)}])

    def test_running_one_step(self):
        self.assertEqual(
            list(find_long_running_jobs(
                [JOB_FLOWS_BY_ID['j-RUNNING1STEP']],
                min_time=timedelta(hours=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [{'job_flow_id': 'j-RUNNING1STEP',
              'name': 'mr_denial: Step 1 of 5',
              'step_state': 'RUNNING',
              'time': timedelta(hours=3, minutes=40)}])

        # job hasn't been running for 1 day
        self.assertEqual(
            list(find_long_running_jobs(
                [JOB_FLOWS_BY_ID['j-RUNNING1STEP']],
                min_time=timedelta(days=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [])

    def test_running_two_steps(self):
        self.assertEqual(
            list(find_long_running_jobs(
                [JOB_FLOWS_BY_ID['j-RUNNING2STEPS']],
                min_time=timedelta(hours=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [{'job_flow_id': 'j-RUNNING2STEPS',
              'name': 'mr_anger: Step 2 of 5',
              'step_state': 'RUNNING',
              'time': timedelta(hours=3, minutes=30)}])

        # job hasn't been running for 1 day
        self.assertEqual(
            list(find_long_running_jobs(
                [JOB_FLOWS_BY_ID['j-RUNNING2STEPS']],
                min_time=timedelta(days=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [])

    def test_running_and_pending(self):
        self.assertEqual(
            list(find_long_running_jobs(
                [JOB_FLOWS_BY_ID['j-RUNNINGANDPENDING']],
                min_time=timedelta(hours=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [{'job_flow_id': 'j-RUNNINGANDPENDING',
              'name': 'mr_anger: Step 2 of 5',
              'step_state': 'RUNNING',
              'time': timedelta(hours=3, minutes=30)}])

        # job hasn't been running for 1 day
        self.assertEqual(
            list(find_long_running_jobs(
                [JOB_FLOWS_BY_ID['j-RUNNINGANDPENDING']],
                min_time=timedelta(days=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [])

    def test_pending_one_step(self):
        self.assertEqual(
            list(find_long_running_jobs(
                [JOB_FLOWS_BY_ID['j-PENDING1STEP']],
                min_time=timedelta(hours=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [{'job_flow_id': 'j-PENDING1STEP',
              'name': 'mr_bargaining: Step 3 of 5',
              'step_state': 'PENDING',
              'time': timedelta(hours=3, minutes=45)}])

        # job hasn't been running for 1 day
        self.assertEqual(
            list(find_long_running_jobs(
                [JOB_FLOWS_BY_ID['j-PENDING1STEP']],
                min_time=timedelta(days=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [])

    def test_pending_two_steps(self):
        self.assertEqual(
            list(find_long_running_jobs(
                [JOB_FLOWS_BY_ID['j-PENDING2STEPS']],
                min_time=timedelta(hours=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [{'job_flow_id': 'j-PENDING2STEPS',
              'name': 'mr_depression: Step 4 of 5',
              'step_state': 'PENDING',
              'time': timedelta(hours=3, minutes=25)}])

        # job hasn't been running for 1 day
        self.assertEqual(
            list(find_long_running_jobs(
                [JOB_FLOWS_BY_ID['j-PENDING2STEPS']],
                min_time=timedelta(days=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [])

    def test_completed(self):
        self.assertEqual(
            list(find_long_running_jobs(
                [JOB_FLOWS_BY_ID['j-COMPLETED']],
                min_time=timedelta(hours=1),
                now=datetime(2010, 6, 6, 4)
            )),
            []
        )

    def test_all_together(self):
        self.assertEqual(
            list(find_long_running_jobs(
                JOB_FLOWS,
                min_time=timedelta(hours=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [{'job_flow_id': 'j-BOOTSTRAPPING',
              'name': 'mr_grieving',
              'step_state': '',
              'time': timedelta(hours=3, minutes=55)},
             {'job_flow_id': 'j-RUNNING1STEP',
              'name': 'mr_denial: Step 1 of 5',
              'step_state': 'RUNNING',
              'time': timedelta(hours=3, minutes=40)},
             {'job_flow_id': 'j-RUNNING2STEPS',
              'name': 'mr_anger: Step 2 of 5',
              'step_state': 'RUNNING',
              'time': timedelta(hours=3, minutes=30)},
             {'job_flow_id': 'j-RUNNINGANDPENDING',
              'name': 'mr_anger: Step 2 of 5',
              'step_state': 'RUNNING',
              'time': timedelta(hours=3, minutes=30)},
             {'job_flow_id': 'j-PENDING1STEP',
              'name': 'mr_bargaining: Step 3 of 5',
              'step_state': 'PENDING',
              'time': timedelta(hours=3, minutes=45)},
             {'job_flow_id': 'j-PENDING2STEPS',
              'name': 'mr_depression: Step 4 of 5',
              'step_state': 'PENDING',
              'time': timedelta(hours=3, minutes=25)}])
