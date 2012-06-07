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
from datetime import date
from datetime import datetime
from datetime import timedelta
from StringIO import StringIO
import sys

import boto.emr.connection
from mrjob.tools.emr.audit_usage import job_flow_to_full_summary
from mrjob.tools.emr.audit_usage import subdivide_interval_by_date
from mrjob.tools.emr.audit_usage import subdivide_interval_by_hour
from mrjob.tools.emr.audit_usage import main
from mrjob.tools.emr.audit_usage import percent
from tests.mockboto import MockEmrObject
from tests.test_emr import MockEMRAndS3TestCase

try:
    import unittest2 as unittest
    unittest  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import unittest


class AuditUsageTestCase(MockEMRAndS3TestCase):

    def setUp(self):
        super(AuditUsageTestCase, self).setUp()
        # redirect print statements to self.stdout
        self._real_stdout = sys.stdout
        self.stdout = StringIO()
        sys.stdout = self.stdout

    def tearDown(self):
        sys.stdout = self._real_stdout
        super(AuditUsageTestCase, self).tearDown()

    def test_with_no_job_flows(self):
        main(['-q', '--no-conf'])  # just make sure it doesn't crash

    def test_with_one_job_flow(self):
        emr_conn = boto.emr.connection.EmrConnection()
        emr_conn.run_jobflow('no name', log_uri=None)

        main(['-q', '--no-conf'])
        self.assertIn('j-MOCKJOBFLOW0', self.stdout.getvalue())


class JobFlowToFullSummaryTestCase(unittest.TestCase):

    maxDiff = None  # show whole diff when tests fail

    def test_basic_job_flow_with_no_steps(self):
        job_flow = MockEmrObject(
            creationdatetime='2010-06-05T23:59:00Z',
            enddatetime='2010-06-06T00:30:00Z',
            jobflowid='j-ISFORJAGUAR',
            name='mr_exciting.woo.20100605.235850.000000',
            normalizedinstancehours='10',
            readydatetime='2010-06-06T00:15:00Z',
            startdatetime='2010-06-06T00:00:00Z',
            state='COMPLETED',
        )

        summary = job_flow_to_full_summary(job_flow)

        self.assertEqual(summary, {
            'created': datetime(2010, 6, 5, 23, 59),
            'end': datetime(2010, 6, 6, 0, 30),
            'id': 'j-ISFORJAGUAR',
            'label': 'mr_exciting',
            'name': 'mr_exciting.woo.20100605.235850.000000',
            'nih': 10.0,
            'nih_bbnu': 7.5,
            'nih_billed': 10.0,
            'nih_used': 2.5,  # only a quarter of time billed was used
            'num_steps': 0,
            'owner': 'woo',
            'pool': None,
            'ran': timedelta(minutes=30),
            'ready': datetime(2010, 6, 6, 0, 15),
            'start': datetime(2010, 6, 6, 0, 0),
            'state': 'COMPLETED',
            'usage': [{
                'date_to_nih_bbnu': {date(2010, 6, 6): 7.5},
                'date_to_nih_billed': {date(2010, 6, 6): 10.0},
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'end': datetime(2010, 6, 6, 0, 15),
                'end_billing': datetime(2010, 6, 6, 1, 0),
                'label': 'mr_exciting',
                'hour_to_nih_bbnu': {datetime(2010, 6, 6, 0): 7.5},
                'hour_to_nih_billed': {datetime(2010, 6, 6, 0): 10.0},
                'hour_to_nih_used': {datetime(2010, 6, 6, 0): 2.5},
                'nih_bbnu': 7.5,
                'nih_billed': 10.0,
                'nih_used': 2.5,
                'owner': 'woo',
                'start': datetime(2010, 6, 6, 0, 0),
                'step_num': None,
            }],
        })

    def test_still_running_job_flow_with_no_steps(self):
        job_flow = MockEmrObject(
            creationdatetime='2010-06-05T23:59:00Z',
            jobflowid='j-ISFORJUICE',
            name='mr_exciting.woo.20100605.235850.000000',
            normalizedinstancehours='10',
            readydatetime='2010-06-06T00:15:00Z',
            startdatetime='2010-06-06T00:00:00Z',
            state='WAITING',
        )

        summary = job_flow_to_full_summary(
            job_flow, now=datetime(2010, 6, 6, 0, 30))

        self.assertEqual(summary, {
            'created': datetime(2010, 6, 5, 23, 59),
            'end': None,
            'id': 'j-ISFORJUICE',
            'label': 'mr_exciting',
            'name': 'mr_exciting.woo.20100605.235850.000000',
            'nih': 10.0,
            'nih_bbnu': 2.5,
            'nih_billed': 5.0,
            'nih_used': 2.5,
            'num_steps': 0,
            'owner': 'woo',
            'pool': None,
            'ran': timedelta(minutes=30),
            'ready': datetime(2010, 6, 6, 0, 15),
            'start': datetime(2010, 6, 6, 0, 0),
            'state': 'WAITING',
            'usage': [{
                'date_to_nih_bbnu': {date(2010, 6, 6): 2.5},
                'date_to_nih_billed': {date(2010, 6, 6): 5.0},
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'end': datetime(2010, 6, 6, 0, 15),
                'end_billing': datetime(2010, 6, 6, 0, 30),
                'hour_to_nih_bbnu': {datetime(2010, 6, 6, 0): 2.5},
                'hour_to_nih_billed': {datetime(2010, 6, 6, 0): 5.0},
                'hour_to_nih_used': {datetime(2010, 6, 6, 0): 2.5},
                'label': 'mr_exciting',
                'nih_bbnu': 2.5,
                'nih_billed': 5.0,
                'nih_used': 2.5,
                'owner': 'woo',
                'start': datetime(2010, 6, 6, 0, 0),
                'step_num': None,
            }],
        })

    def test_still_bootstrapping_job_flow_with_no_steps(self):
        job_flow = MockEmrObject(
            creationdatetime='2010-06-05T23:59:00Z',
            jobflowid='j-ISFORJOKE',
            name='mr_exciting.woo.20100605.235850.000000',
            normalizedinstancehours='10',
            startdatetime='2010-06-06T00:00:00Z',
            state='BOOTSTRAPPING',
        )

        summary = job_flow_to_full_summary(
            job_flow, now=datetime(2010, 6, 6, 0, 30))

        self.assertEqual(summary, {
            'created': datetime(2010, 6, 5, 23, 59),
            'end': None,
            'id': 'j-ISFORJOKE',
            'label': 'mr_exciting',
            'name': 'mr_exciting.woo.20100605.235850.000000',
            'nih': 10.0,
            'nih_bbnu': 0.0,
            'nih_billed': 5.0,
            'nih_used': 5.0,
            'num_steps': 0,
            'owner': 'woo',
            'pool': None,
            'ran': timedelta(minutes=30),
            'ready': None,
            'start': datetime(2010, 6, 6, 0, 0),
            'state': 'BOOTSTRAPPING',
            'usage': [{
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 6): 5.0},
                'date_to_nih_used': {date(2010, 6, 6): 5.0},
                'end': datetime(2010, 6, 6, 0, 30),
                'end_billing': datetime(2010, 6, 6, 0, 30),
                'hour_to_nih_bbnu': {},
                'hour_to_nih_billed': {datetime(2010, 6, 6, 0): 5.0},
                'hour_to_nih_used': {datetime(2010, 6, 6, 0): 5.0},
                'label': 'mr_exciting',
                'nih_bbnu': 0.0,
                'nih_billed': 5.0,
                'nih_used': 5.0,
                'owner': 'woo',
                'start': datetime(2010, 6, 6, 0, 0),
                'step_num': None,
            }],
        })

    def test_job_flow_that_hasnt_yet_started(self):
        job_flow = MockEmrObject(
            creationdatetime='2010-06-05T23:59:00Z',
            jobflowid='j-ISFORJUMP',
            name='mr_exciting.woo.20100605.235850.000000',
            normalizedinstancehours='10',
            state='STARTING',
        )

        summary = job_flow_to_full_summary(
            job_flow, now=datetime(2010, 6, 6, 0, 30))

        self.assertEqual(summary, {
            'created': datetime(2010, 6, 5, 23, 59),
            'end': None,
            'id': 'j-ISFORJUMP',
            'label': 'mr_exciting',
            'name': 'mr_exciting.woo.20100605.235850.000000',
            'nih': 10.0,
            'nih_bbnu': 0.0,
            'nih_billed': 0.0,
            'nih_used': 0.0,
            'num_steps': 0,
            'owner': 'woo',
            'pool': None,
            'ran': timedelta(0),
            'ready': None,
            'start': None,
            'state': 'STARTING',
            'usage': [],
        })

    def test_job_flow_that_was_terminated_before_starting(self):
        job_flow = MockEmrObject(
            creationdatetime='2010-06-05T23:59:00Z',
            enddatetime='2010-06-06T00:01:00Z',
            jobflowid='j-ISFORJOURNEY',
            name='mr_exciting.woo.20100605.235850.000000',
            normalizedinstancehours='0',
            state='TERMINATED',
        )

        summary = job_flow_to_full_summary(
            job_flow, now=datetime(2010, 6, 6, 0, 30))

        self.assertEqual(summary, {
            'created': datetime(2010, 6, 5, 23, 59),
            'end': datetime(2010, 6, 6, 0, 1),
            'id': 'j-ISFORJOURNEY',
            'label': 'mr_exciting',
            'name': 'mr_exciting.woo.20100605.235850.000000',
            'nih': 0.0,
            'nih_bbnu': 0.0,
            'nih_billed': 0.0,
            'nih_used': 0.0,
            'num_steps': 0,
            'owner': 'woo',
            'pool': None,
            'ran': timedelta(0),
            'ready': None,
            'start': None,
            'state': 'TERMINATED',
            'usage': [],
        })

    def test_job_flow_with_no_fields(self):
        # this shouldn't happen in practice; just a robustness check
        job_flow = MockEmrObject()

        summary = job_flow_to_full_summary(job_flow)

        self.assertEqual(summary, {
            'created': None,
            'end': None,
            'id': None,
            'label': None,
            'name': None,
            'nih': 0.0,
            'nih_bbnu': 0.0,
            'nih_billed': 0.0,
            'nih_used': 0.0,
            'num_steps': 0,
            'owner': None,
            'pool': None,
            'ran': timedelta(0),
            'ready': None,
            'start': None,
            'state': None,
            'usage': [],
        })

    def test_job_flow_with_no_steps_split_over_midnight(self):
        job_flow = MockEmrObject(
            creationdatetime='2010-06-05T23:59:00Z',
            enddatetime='2010-06-06T01:15:00Z',  # 2 hours are billed
            jobflowid='j-ISFORJOY',
            name='mr_exciting.woo.20100605.232950.000000',
            normalizedinstancehours='20',
            readydatetime='2010-06-05T23:45:00Z',  # only 15 minutes "used"
            startdatetime='2010-06-05T23:30:00Z',
            state='COMPLETED',
        )

        summary = job_flow_to_full_summary(job_flow)

        self.assertEqual(summary, {
            'created': datetime(2010, 6, 5, 23, 59),
            'end': datetime(2010, 6, 6, 1, 15),
            'id': 'j-ISFORJOY',
            'label': 'mr_exciting',
            'name': 'mr_exciting.woo.20100605.232950.000000',
            'nih': 20.0,
            'nih_bbnu': 17.5,
            'nih_billed': 20.0,
            'nih_used': 2.5,
            'num_steps': 0,
            'owner': 'woo',
            'pool': None,
            'ran': timedelta(hours=1, minutes=45),
            'ready': datetime(2010, 6, 5, 23, 45),
            'start': datetime(2010, 6, 5, 23, 30),
            'state': 'COMPLETED',
            'usage': [{
                'date_to_nih_bbnu': {date(2010, 6, 5): 2.5,
                                     date(2010, 6, 6): 15.0},
                'date_to_nih_billed': {date(2010, 6, 5): 5.0,
                                       date(2010, 6, 6): 15.0},
                'date_to_nih_used': {date(2010, 6, 5): 2.5},
                'end': datetime(2010, 6, 5, 23, 45),
                'end_billing': datetime(2010, 6, 6, 1, 30),
                'hour_to_nih_bbnu': {datetime(2010, 6, 5, 23): 2.5,
                                     datetime(2010, 6, 6, 0): 10.0,
                                     datetime(2010, 6, 6, 1): 5.0},
                'hour_to_nih_billed': {datetime(2010, 6, 5, 23): 5.0,
                                       datetime(2010, 6, 6, 0): 10.0,
                                       datetime(2010, 6, 6, 1): 5.0},
                'hour_to_nih_used': {datetime(2010, 6, 5, 23): 2.5},
                'label': 'mr_exciting',
                'nih_bbnu': 17.5,
                'nih_billed': 20.0,
                'nih_used': 2.5,
                'owner': 'woo',
                'start': datetime(2010, 6, 5, 23, 30),
                'step_num': None,
            }],
        })

    def test_job_flow_with_one_still_running_step(self):
        job_flow = MockEmrObject(
            creationdatetime='2010-06-06T03:59:00Z',
            jobflowid='j-ISFORJUNGLE',
            name='mr_exciting.woo.20100606.035855.000000',
            normalizedinstancehours='20',
            readydatetime='2010-06-06T04:15:00Z',
            startdatetime='2010-06-06T04:00:00Z',
            state='RUNNING',
            steps=[
                MockEmrObject(
                    name='mr_exciting.woo.20100606.035855.000000: Step 1 of 3',
                    startdatetime='2010-06-06T04:15:00Z',
                ),
            ]
        )

        summary = job_flow_to_full_summary(
            job_flow, now=datetime(2010, 6, 6, 5, 30))

        self.assertEqual(summary, {
            'created': datetime(2010, 6, 6, 3, 59),
            'end': None,
            'id': 'j-ISFORJUNGLE',
            'label': 'mr_exciting',
            'name': 'mr_exciting.woo.20100606.035855.000000',
            'nih': 20.0,
            'nih_bbnu': 0.0,
            'nih_billed': 15.0,
            'nih_used': 15.0,
            'num_steps': 1,
            'owner': 'woo',
            'pool': None,
            'ran': timedelta(hours=1, minutes=30),
            'ready': datetime(2010, 6, 6, 4, 15),
            'start': datetime(2010, 6, 6, 4, 0),
            'state': 'RUNNING',
            'usage': [{
            # bootstrapping
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 6): 2.5},
                'end': datetime(2010, 6, 6, 4, 15),
                'end_billing': datetime(2010, 6, 6, 4, 15),
                'hour_to_nih_used': {datetime(2010, 6, 6, 4): 2.5},
                'hour_to_nih_bbnu': {},
                'hour_to_nih_billed': {datetime(2010, 6, 6, 4): 2.5},
                'label': 'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': 'woo',
                'start': datetime(2010, 6, 6, 4, 0),
                'step_num': None,
            }, {
            # mr_exciting, step 1
                'date_to_nih_used': {date(2010, 6, 6): 12.5},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 6): 12.5},
                'end': datetime(2010, 6, 6, 5, 30),
                'end_billing': datetime(2010, 6, 6, 5, 30),
                'hour_to_nih_used': {datetime(2010, 6, 6, 4): 7.5,
                                     datetime(2010, 6, 6, 5): 5.0},
                'hour_to_nih_bbnu': {},
                'hour_to_nih_billed': {datetime(2010, 6, 6, 4): 7.5,
                                       datetime(2010, 6, 6, 5): 5.0},
                'label': 'mr_exciting',
                'nih_used': 12.5,
                'nih_bbnu': 0.0,
                'nih_billed': 12.5,
                'owner': 'woo',
                'start': datetime(2010, 6, 6, 4, 15),
                'step_num': 1,
            }],
        })

    def test_job_flow_with_one_cancelled_step(self):
        job_flow = MockEmrObject(
            creationdatetime='2010-06-06T03:59:00Z',
            enddatetime='2010-06-06T05:30:00Z',
            jobflowid='j-ISFORJACUZZI',
            name='mr_exciting.woo.20100606.035855.000000',
            normalizedinstancehours='20',
            readydatetime='2010-06-06T04:15:00Z',
            startdatetime='2010-06-06T04:00:00Z',
            state='RUNNING',
            # step doesn't have end time even though job flow does
            steps=[
                MockEmrObject(
                    name='mr_exciting.woo.20100606.035855.000000: Step 1 of 3',
                    startdatetime='2010-06-06T04:15:00Z',
                ),
            ]
        )

        summary = job_flow_to_full_summary(job_flow)

        self.assertEqual(summary, {
            'created': datetime(2010, 6, 6, 3, 59),
            'end': datetime(2010, 6, 6, 5, 30),
            'id': 'j-ISFORJACUZZI',
            'label': 'mr_exciting',
            'name': 'mr_exciting.woo.20100606.035855.000000',
            'nih': 20.0,
            'nih_bbnu': 17.5,
            'nih_billed': 20.0,
            'nih_used': 2.5,
            'num_steps': 1,
            'owner': 'woo',
            'pool': None,
            'ran': timedelta(hours=1, minutes=30),
            'ready': datetime(2010, 6, 6, 4, 15),
            'start': datetime(2010, 6, 6, 4, 0),
            'state': 'RUNNING',
            'usage': [{
            # bootstrapping
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 6): 2.5},
                'end': datetime(2010, 6, 6, 4, 15),
                'end_billing': datetime(2010, 6, 6, 4, 15),
                'hour_to_nih_used': {datetime(2010, 6, 6, 4): 2.5},
                'hour_to_nih_bbnu': {},
                'hour_to_nih_billed': {datetime(2010, 6, 6, 4): 2.5},
                'label': 'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': 'woo',
                'start': datetime(2010, 6, 6, 4, 0),
                'step_num': None,
            }, {
            # mr_exciting, step 1 (cancelled)
                'date_to_nih_used': {},
                'date_to_nih_bbnu': {date(2010, 6, 6): 17.5},
                'date_to_nih_billed': {date(2010, 6, 6): 17.5},
                'end': datetime(2010, 6, 6, 4, 15),
                'end_billing': datetime(2010, 6, 6, 6, 0),
                'hour_to_nih_used': {},
                'hour_to_nih_bbnu': {datetime(2010, 6, 6, 4): 7.5,
                                     datetime(2010, 6, 6, 5): 10.0},
                'hour_to_nih_billed': {datetime(2010, 6, 6, 4): 7.5,
                                       datetime(2010, 6, 6, 5): 10.0},
                'label': 'mr_exciting',
                'nih_used': 0.0,
                'nih_bbnu': 17.5,
                'nih_billed': 17.5,
                'owner': 'woo',
                'start': datetime(2010, 6, 6, 4, 15),
                'step_num': 1,
            }],
        })

    def test_multi_step_job_flow(self):
        job_flow = MockEmrObject(
            creationdatetime='2010-06-05T23:29:00Z',
            enddatetime='2010-06-06T01:15:00Z',  # 2 hours are billed
            jobflowid='j-ISFORJOB',
            name='mr_exciting.woo.20100605.232850.000000',
            normalizedinstancehours='20',
            readydatetime='2010-06-05T23:45:00Z',
            startdatetime='2010-06-05T23:30:00Z',
            state='COMPLETED',
            steps=[
                MockEmrObject(
                    name='mr_exciting.woo.20100605.232850.000000: Step 1 of 3',
                    startdatetime='2010-06-05T23:45:00Z',
                    enddatetime='2010-06-06T00:15:00Z',
                ),
                MockEmrObject(
                    name='mr_exciting.woo.20100605.232850.000000: Step 2 of 3',
                    startdatetime='2010-06-06T00:30:00Z',
                    enddatetime='2010-06-06T00:45:00Z',
                ),
                MockEmrObject(
                    name='mr_exciting.woo.20100605.232850.000000: Step 3 of 3',
                    startdatetime='2010-06-06T00:45:00Z',
                    enddatetime='2010-06-06T01:00:00Z',
                ),
            ],
        )

        summary = job_flow_to_full_summary(job_flow)

        self.assertEqual(summary, {
            'created': datetime(2010, 6, 5, 23, 29),
            'end': datetime(2010, 6, 6, 1, 15),
            'id': 'j-ISFORJOB',
            'label': 'mr_exciting',
            'name': 'mr_exciting.woo.20100605.232850.000000',
            'nih': 20.0,
            'nih_bbnu': 7.5,
            'nih_billed': 20.0,
            'nih_used': 12.5,
            'num_steps': 3,
            'owner': 'woo',
            'pool': None,
            'ran': timedelta(hours=1, minutes=45),
            'ready': datetime(2010, 6, 5, 23, 45),
            'start': datetime(2010, 6, 5, 23, 30),
            'state': 'COMPLETED',
            'usage': [{
            # bootstrapping
                'date_to_nih_used': {date(2010, 6, 5): 2.5},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 5): 2.5},
                'end': datetime(2010, 6, 5, 23, 45),
                'end_billing': datetime(2010, 6, 5, 23, 45),
                'hour_to_nih_bbnu': {},
                'hour_to_nih_billed': {datetime(2010, 6, 5, 23): 2.5},
                'hour_to_nih_used': {datetime(2010, 6, 5, 23): 2.5},
                'label': 'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': 'woo',
                'start': datetime(2010, 6, 5, 23, 30),
                'step_num': None,
            }, {
            # step 1 (and idle time after)
                'date_to_nih_used': {date(2010, 6, 5): 2.5,
                                     date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {date(2010, 6, 6): 2.5},
                'date_to_nih_billed': {date(2010, 6, 5): 2.5,
                                       date(2010, 6, 6): 5.0},
                'end': datetime(2010, 6, 6, 0, 15),
                'end_billing': datetime(2010, 6, 6, 0, 30),
                'hour_to_nih_bbnu': {datetime(2010, 6, 6, 0): 2.5},
                'hour_to_nih_billed': {datetime(2010, 6, 5, 23): 2.5,
                                       datetime(2010, 6, 6, 0): 5.0},
                'hour_to_nih_used': {datetime(2010, 6, 5, 23): 2.5,
                                     datetime(2010, 6, 6, 0): 2.5},
                'label': 'mr_exciting',
                'nih_used': 5.0,
                'nih_bbnu': 2.5,
                'nih_billed': 7.5,
                'owner': 'woo',
                'start': datetime(2010, 6, 5, 23, 45),
                'step_num': 1,
            }, {
            # step 2
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 6): 2.5},
                'end': datetime(2010, 6, 6, 0, 45),
                'end_billing': datetime(2010, 6, 6, 0, 45),
                'hour_to_nih_bbnu': {},
                'hour_to_nih_billed': {datetime(2010, 6, 6, 0): 2.5},
                'hour_to_nih_used': {datetime(2010, 6, 6, 0): 2.5},
                'label': 'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': 'woo',
                'start': datetime(2010, 6, 6, 0, 30),
                'step_num': 2,
            },
            # step 3 (and idle time after)
            {
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {date(2010, 6, 6): 5.0},
                'date_to_nih_billed': {date(2010, 6, 6): 7.5},
                'end': datetime(2010, 6, 6, 1, 0),
                'end_billing': datetime(2010, 6, 6, 1, 30),
                'hour_to_nih_bbnu': {datetime(2010, 6, 6, 1): 5.0},
                'hour_to_nih_billed': {datetime(2010, 6, 6, 0): 2.5,
                                       datetime(2010, 6, 6, 1): 5.0},
                'hour_to_nih_used': {datetime(2010, 6, 6, 0): 2.5},
                'label': 'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 5.0,
                'nih_billed': 7.5,
                'owner': 'woo',
                'start': datetime(2010, 6, 6, 0, 45),
                'step_num': 3,
            }],
        })

    def test_pooled_job_flow(self):
        # same as test case above with different job names
        job_flow = MockEmrObject(
            bootstrapactions=[
                MockEmrObject(args=[]),
                MockEmrObject(args=[
                    MockEmrObject(
                        value='pool-0123456789abcdef0123456789abcdef'),
                    MockEmrObject(value='reflecting'),
                ]),
            ],
            creationdatetime='2010-06-05T23:29:00Z',
            enddatetime='2010-06-06T01:15:00Z',  # 2 hours are billed
            jobflowid='j-ISFORJOB',
            name='mr_exciting.woo.20100605.232850.000000',
            normalizedinstancehours='20',
            readydatetime='2010-06-05T23:45:00Z',
            startdatetime='2010-06-05T23:30:00Z',
            state='COMPLETED',
            steps=[
                MockEmrObject(
                    name='mr_exciting.woo.20100605.232950.000000: Step 1 of 1',
                    startdatetime='2010-06-05T23:45:00Z',
                    enddatetime='2010-06-06T00:15:00Z',
                ),
                MockEmrObject(
                    name='mr_whatever.meh.20100606.002000.000000: Step 1 of 2',
                    startdatetime='2010-06-06T00:30:00Z',
                    enddatetime='2010-06-06T00:45:00Z',
                ),
                MockEmrObject(
                    name='mr_whatever.meh.20100606.002000.000000: Step 2 of 2',
                    startdatetime='2010-06-06T00:45:00Z',
                    enddatetime='2010-06-06T01:00:00Z',
                ),
            ],
        )

        summary = job_flow_to_full_summary(job_flow)

        self.assertEqual(summary, {
            'created': datetime(2010, 6, 5, 23, 29),
            'end': datetime(2010, 6, 6, 1, 15),
            'id': 'j-ISFORJOB',
            'label': 'mr_exciting',
            'name': 'mr_exciting.woo.20100605.232850.000000',
            'nih': 20.0,
            'nih_bbnu': 7.5,
            'nih_billed': 20.0,
            'nih_used': 12.5,
            'num_steps': 3,
            'owner': 'woo',
            'pool': 'reflecting',
            'ran': timedelta(hours=1, minutes=45),
            'ready': datetime(2010, 6, 5, 23, 45),
            'start': datetime(2010, 6, 5, 23, 30),
            'state': 'COMPLETED',
            'usage': [{
            # bootstrapping
                'date_to_nih_used': {date(2010, 6, 5): 2.5},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 5): 2.5},
                'end': datetime(2010, 6, 5, 23, 45),
                'end_billing': datetime(2010, 6, 5, 23, 45),
                'hour_to_nih_bbnu': {},
                'hour_to_nih_billed': {datetime(2010, 6, 5, 23): 2.5},
                'hour_to_nih_used': {datetime(2010, 6, 5, 23): 2.5},
                'label': 'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': 'woo',
                'start': datetime(2010, 6, 5, 23, 30),
                'step_num': None,
            }, {
            # mr_exciting, step 1 (and idle time after)
                'date_to_nih_used': {date(2010, 6, 5): 2.5,
                                     date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {date(2010, 6, 6): 2.5},
                'date_to_nih_billed': {date(2010, 6, 5): 2.5,
                                       date(2010, 6, 6): 5.0},
                'end': datetime(2010, 6, 6, 0, 15),
                'end_billing': datetime(2010, 6, 6, 0, 30),
                'hour_to_nih_bbnu': {datetime(2010, 6, 6, 0): 2.5},
                'hour_to_nih_billed': {datetime(2010, 6, 5, 23): 2.5,
                                       datetime(2010, 6, 6, 0): 5.0},
                'hour_to_nih_used': {datetime(2010, 6, 5, 23): 2.5,
                                     datetime(2010, 6, 6, 0): 2.5},
                'label': 'mr_exciting',
                'nih_used': 5.0,
                'nih_bbnu': 2.5,
                'nih_billed': 7.5,
                'owner': 'woo',
                'start': datetime(2010, 6, 5, 23, 45),
                'step_num': 1,
            }, {
            # mr whatever, step 1
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 6): 2.5},
                'end': datetime(2010, 6, 6, 0, 45),
                'end_billing': datetime(2010, 6, 6, 0, 45),
                'hour_to_nih_bbnu': {},
                'hour_to_nih_billed': {datetime(2010, 6, 6, 0): 2.5},
                'hour_to_nih_used': {datetime(2010, 6, 6, 0): 2.5},
                'label': 'mr_whatever',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': 'meh',
                'start': datetime(2010, 6, 6, 0, 30),
                'step_num': 1,
            },
            # mr whatever, step 2 (and idle time after)
            {
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {date(2010, 6, 6): 5.0},
                'date_to_nih_billed': {date(2010, 6, 6): 7.5},
                'end': datetime(2010, 6, 6, 1, 0),
                'end_billing': datetime(2010, 6, 6, 1, 30),
                'hour_to_nih_bbnu': {datetime(2010, 6, 6, 1): 5.0},
                'hour_to_nih_billed': {datetime(2010, 6, 6, 0): 2.5,
                                       datetime(2010, 6, 6, 1): 5.0},
                'hour_to_nih_used': {datetime(2010, 6, 6, 0): 2.5},
                'label': 'mr_whatever',
                'nih_used': 2.5,
                'nih_bbnu': 5.0,
                'nih_billed': 7.5,
                'owner': 'meh',
                'start': datetime(2010, 6, 6, 0, 45),
                'step_num': 2,
            }],
        })


class SubdivideIntervalByDateTestCase(unittest.TestCase):

    def test_zero_interval(self):
        self.assertEqual(
            subdivide_interval_by_date(
                datetime(2010, 6, 6, 4, 26),
                datetime(2010, 6, 6, 4, 26),
            ),
            {}
        )

    def test_same_day(self):
        self.assertEqual(
            subdivide_interval_by_date(
                datetime(2010, 6, 6, 4, 0),
                datetime(2010, 6, 6, 6, 0),
            ),
            {date(2010, 6, 6): 7200.0}
        )

    def test_start_at_midnight(self):
        self.assertEqual(
            subdivide_interval_by_date(
                datetime(2010, 6, 6, 0, 0),
                datetime(2010, 6, 6, 5, 0),
            ),
            {date(2010, 6, 6): 18000.0}
        )

    def test_end_at_midnight(self):
        self.assertEqual(
            subdivide_interval_by_date(
                datetime(2010, 6, 5, 23, 0),
                datetime(2010, 6, 6, 0, 0),
            ),
            {date(2010, 6, 5): 3600.0}
        )

    def test_split_over_midnight(self):
        self.assertEqual(
            subdivide_interval_by_date(
                datetime(2010, 6, 5, 23, 0),
                datetime(2010, 6, 6, 5, 0),
            ),
            {date(2010, 6, 5): 3600.0,
             date(2010, 6, 6): 18000.0}
        )

    def test_full_days(self):
        self.assertEqual(
            subdivide_interval_by_date(
                datetime(2010, 6, 5, 23, 0),
                datetime(2010, 6, 10, 5, 0),
            ),
            {date(2010, 6, 5): 3600.0,
             date(2010, 6, 6): 86400.0,
             date(2010, 6, 7): 86400.0,
             date(2010, 6, 8): 86400.0,
             date(2010, 6, 9): 86400.0,
             date(2010, 6, 10): 18000.0}
        )


class SubdivideIntervalByHourTestCase(unittest.TestCase):

    def test_zero_interval(self):
        self.assertEqual(
            subdivide_interval_by_hour(
                datetime(2010, 6, 6, 4, 26),
                datetime(2010, 6, 6, 4, 26),
            ),
            {}
        )

    def test_same_hour(self):
        self.assertEqual(
            subdivide_interval_by_hour(
                datetime(2010, 6, 6, 4, 24),
                datetime(2010, 6, 6, 4, 26),
            ),
            {datetime(2010, 6, 6, 4): 120.0}
        )

    def test_start_at_midnight(self):
        self.assertEqual(
            subdivide_interval_by_hour(
                datetime(2010, 6, 6, 0, 0),
                datetime(2010, 6, 6, 0, 3),
            ),
            {datetime(2010, 6, 6, 0): 180.0}
        )

    def test_end_at_midnight(self):
        self.assertEqual(
            subdivide_interval_by_hour(
                datetime(2010, 6, 5, 23, 55),
                datetime(2010, 6, 6, 0, 0),
            ),
            {datetime(2010, 6, 5, 23): 300.0}
        )

    def test_split_over_midnight(self):
        self.assertEqual(
            subdivide_interval_by_hour(
                datetime(2010, 6, 5, 23, 55),
                datetime(2010, 6, 6, 0, 3),
            ),
            {datetime(2010, 6, 5, 23): 300.0,
             datetime(2010, 6, 6, 0): 180.0}
        )

    def test_full_hours(self):
        self.assertEqual(
            subdivide_interval_by_hour(
                datetime(2010, 6, 5, 23, 40),
                datetime(2010, 6, 6, 2, 10),
            ),
            {datetime(2010, 6, 5, 23): 1200.0,
             datetime(2010, 6, 6, 0): 3600.0,
             datetime(2010, 6, 6, 1): 3600.0,
             datetime(2010, 6, 6, 2): 600.0}
        )


class PercentTestCase(unittest.TestCase):

    def test_basic(self):
        self.assertEqual(62.5, percent(5, 8))

    def test_default(self):
        self.assertEqual(0.0, percent(1, 0))
        self.assertEqual(0.0, percent(0, 0))
        self.assertEqual(None, percent(0, 0, default=None))
