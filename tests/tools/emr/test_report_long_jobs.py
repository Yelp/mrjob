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
except ImportError:
    import unittest


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

    def test_with_one_job_flow(self):
        emr_conn = EMRJobRunner(conf_path=False).make_emr_conn()
        emr_conn.run_jobflow('no name', log_uri=None)
        main(['-q', '--no-conf'])  # just make sure it doesn't crash


JOB_FLOWS = [
    MockEmrObject(
    	jobflowid='j-RUNNING1STEP',
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
    	jobflowid='j-RUNNING2STEPS',
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
    	jobflowid='j-RUNNINGANDPENDING',
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
    	jobflowid='j-PENDING1STEP',
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
    	jobflowid='j-PENDING2STEPS',
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
    	jobflowid='j-COMPLETED',
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

class FindLongRunningJobsTestCase(unittest.TestCase):

    maxDiff = None  # show whole diff when tests fail

    def test_running_one_step(self):
        self.assertEqual(
            list(find_long_running_jobs(
            	[JOB_FLOWS_BY_ID['j-RUNNING1STEP']],
                min_time=timedelta(hours=1),
                now=datetime(2010, 6, 6, 4)
            )),
            [{'job_flow_id': 'j-RUNNING1STEP',
              'step_name': 'mr_denial: Step 1 of 5',
              'step_num': 0,
              'step_state': 'RUNNING',
              'total_steps': 1,
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
              'step_name': 'mr_anger: Step 2 of 5',
              'step_num': 1,
              'step_state': 'RUNNING',
              'total_steps': 2,
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
              'step_name': 'mr_anger: Step 2 of 5',
              'step_num': 1,
              'step_state': 'RUNNING',
              'total_steps': 3,
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
              'step_name': 'mr_bargaining: Step 3 of 5',
              'step_num': 0,
              'step_state': 'PENDING',
              'total_steps': 1,
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
              'step_name': 'mr_depression: Step 4 of 5',
              'step_num': 1,
              'step_state': 'PENDING',
              'total_steps': 2,
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
