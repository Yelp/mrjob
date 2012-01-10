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
from StringIO import StringIO
import sys

from mrjob import boto_2_1_1_83aae37b
from mrjob.emr import EMRJobRunner
from mrjob.tools.emr.audit_usage import job_flow_to_intervals
from mrjob.tools.emr.audit_usage import subdivide_interval_by_date
from mrjob.tools.emr.audit_usage import parse_label_and_owner
from mrjob.tools.emr.audit_usage import main
from tests.mockboto import MockEmrObject
from tests.test_emr import MockEMRAndS3TestCase

try:
    import unittest2 as unittest
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
        emr_conn = boto_2_1_1_83aae37b.EmrConnection()
        emr_conn.run_jobflow('no name', log_uri=None)

        main(['-q', '--no-conf'])
        self.assertIn('j-MOCKJOBFLOW0', self.stdout.getvalue())


class JobFlowToIntervalsTestCase(unittest.TestCase):

    maxDiff = None  # show whole diff when tests fail

    def test_basic_job_flow_with_no_steps(self):
        job_flow = MockEmrObject(
            enddatetime='2010-06-06T00:30:00Z',
            name='mr_exciting.woo.20100605.235950.000000',
            normalizedinstancehours='10',
            readydatetime='2010-06-06T00:15:00Z',
            startdatetime='2010-06-06T00:00:00Z',
            steps=[],
        )

        intervals = job_flow_to_intervals(job_flow)

        self.assertEqual(
            intervals,
            [{
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {date(2010, 6, 6): 7.5},
                'date_to_nih_billed': {date(2010, 6, 6): 10.0},
                'end': datetime(2010, 6, 6, 0, 15),
                'end_billing': datetime(2010, 6, 6, 1, 0),
                'label': 'mr_exciting',
                'nih_used': 2.5,  # only a quarter of time billed was used
                'nih_bbnu': 7.5,
                'nih_billed': 10.0,
                'owner': 'woo',
                'pool': None,
                'start': datetime(2010, 6, 6, 0, 0),
            }])

    def test_job_flow_with_no_steps_still_running(self):
        job_flow = MockEmrObject(
            name='mr_exciting.woo.20100605.235950.000000',
            normalizedinstancehours='10',
            readydatetime='2010-06-06T00:15:00Z',
            startdatetime='2010-06-06T00:00:00Z',
            steps=[],
        )

        intervals = job_flow_to_intervals(
            job_flow, now=datetime(2010, 6, 6, 0, 30))

        self.assertEqual(
            intervals,
            [{
                'date_to_nih_used': {date(2010, 6, 6): 5.0},
                'date_to_nih_bbnu': {date(2010, 6, 6): 5.0},
                'date_to_nih_billed': {date(2010, 6, 6): 10.0},
                'end': datetime(2010, 6, 6, 0, 15),
                'end_billing': datetime(2010, 6, 6, 0, 30),
                'label': 'mr_exciting',
                'nih_used': 5.0,
                'nih_bbnu': 5.0,
                'nih_billed': 10.0,
                'owner': 'woo',
                'pool': None,
                'start': datetime(2010, 6, 6, 0, 0),
            }])

    def test_job_flow_with_no_steps_still_bootstrapping(self):
        job_flow = MockEmrObject(
            name='mr_exciting.woo.20100605.235950.000000',
            normalizedinstancehours='10',
            startdatetime='2010-06-06T00:00:00Z',
            steps=[],
        )

        intervals = job_flow_to_intervals(
            job_flow, now=datetime(2010, 6, 6, 0, 30))

        self.assertEqual(
            intervals,
            [{
                'date_to_nih_used': {date(2010, 6, 6): 10.0},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 6): 10.0},
                'end': datetime(2010, 6, 6, 0, 30),
                'end_billing': datetime(2010, 6, 6, 0, 30),
                'label': 'mr_exciting',
                'nih_used': 10.0,
                'nih_bbnu': 0.0,
                'nih_billed': 10.0,
                'owner': 'woo',
                'pool': None,
                'start': datetime(2010, 6, 6, 0, 0),
            }])

    def test_job_flow_with_no_steps_split_over_midnight(self):
        job_flow = MockEmrObject(
            enddatetime='2010-06-06T01:15:00Z',  # 2 hours are billed
            name='mr_exciting.woo.20100605.232950.000000',
            normalizedinstancehours='20',
            readydatetime='2010-06-05T23:45:00Z',  # only 15 minutes "used"
            startdatetime='2010-06-05T23:30:00Z',
            steps=[],
        )

        intervals = job_flow_to_intervals(job_flow)

        self.assertEqual(
            intervals,
            [{
                'date_to_nih_used': {date(2010, 6, 5): 2.5},
                'date_to_nih_bbnu': {date(2010, 6, 5): 2.5,
                                     date(2010, 6, 6): 15.0},
                'date_to_nih_billed': {date(2010, 6, 5): 5.0,
                                       date(2010, 6, 6): 15.0},
                'end': datetime(2010, 6, 5, 23, 45),
                'end_billing': datetime(2010, 6, 6, 1, 30),
                'label': 'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 17.5,
                'nih_billed': 20.0,
                'owner': 'woo',
                'pool': None,
                'start': datetime(2010, 6, 5, 23, 30),
            }])

    def test_job_flow_with_step_still_running(self):
        job_flow = MockEmrObject(
            name='mr_exciting.woo.20100605.000359.000000',
            normalizedinstancehours='15',
            readydatetime='2010-06-06T04:15:00Z',  # only 15 minutes "used"
            startdatetime='2010-06-06T04:00:00Z',
            steps=[
                MockEmrObject(
                    name='mr_exciting.woo.20100606.000359.000000: Step 1 of 3',
                    startdatetime='2010-06-06T04:15:00Z',
                ),
            ]
        )

        intervals = job_flow_to_intervals(
            job_flow, now=datetime(2010, 6, 6, 5, 30))

        self.assertEqual(
            intervals, [
            # bootstrapping
            {
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 6): 2.5},
                'end': datetime(2010, 6, 6, 4, 15),
                'end_billing': datetime(2010, 6, 6, 4, 15),
                'label': 'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': 'woo',
                'pool': None,
                'start': datetime(2010, 6, 6, 4, 0),
            },
            # mr_exciting, step 1
            {
                'date_to_nih_used': {date(2010, 6, 6): 12.5},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 6): 12.5},
                'end': datetime(2010, 6, 6, 5, 30),
                'end_billing': datetime(2010, 6, 6, 5, 30),
                'label': 'mr_exciting',
                'nih_used': 12.5,
                'nih_bbnu': 0.0,
                'nih_billed': 12.5,
                'owner': 'woo',
                'pool': None,
                'start': datetime(2010, 6, 6, 4, 15),
            },
        ])

    def test_multi_step_job_flow(self):
        job_flow = MockEmrObject(
            enddatetime='2010-06-06T01:15:00Z',  # 2 hours are billed
            name='mr_exciting.woo.20100605.232950.000000',
            normalizedinstancehours='20',
            readydatetime='2010-06-05T23:45:00Z',  # only 15 minutes "used"
            startdatetime='2010-06-05T23:30:00Z',
            steps=[
                MockEmrObject(
                    name='mr_exciting.woo.20100605.232950.000000: Step 1 of 3',
                    startdatetime='2010-06-05T23:45:00Z',
                    enddatetime='2010-06-06T00:15:00Z',
                ),
                MockEmrObject(
                    name='mr_exciting.woo.20100605.232950.000000: Step 2 of 3',
                    startdatetime='2010-06-06T00:30:00Z',
                    enddatetime='2010-06-06T00:45:00Z',
                ),
                MockEmrObject(
                    name='mr_exciting.woo.20100605.232950.000000: Step 3 of 3',
                    startdatetime='2010-06-06T00:45:00Z',
                    enddatetime='2010-06-06T01:00:00Z',
                ),
            ],
        )

        intervals = job_flow_to_intervals(job_flow)

        self.assertEqual(len(intervals), 4)

        self.assertEqual(
            intervals, [
            # bootstrapping
            {
                'date_to_nih_used': {date(2010, 6, 5): 2.5},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 5): 2.5},
                'end': datetime(2010, 6, 5, 23, 45),
                'end_billing': datetime(2010, 6, 5, 23, 45),
                'label': 'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': 'woo',
                'pool': None,
                'start': datetime(2010, 6, 5, 23, 30),
            },
            # step 1 (and idle time after)
            {
                'date_to_nih_used': {date(2010, 6, 5): 2.5,
                                     date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {date(2010, 6, 6): 2.5},
                'date_to_nih_billed': {date(2010, 6, 5): 2.5,
                                       date(2010, 6, 6): 5.0},
                'end': datetime(2010, 6, 6, 0, 15),
                'end_billing': datetime(2010, 6, 6, 0, 30),
                'label': 'mr_exciting',
                'nih_used': 5.0,
                'nih_bbnu': 2.5,
                'nih_billed': 7.5,
                'owner': 'woo',
                'pool': None,
                'start': datetime(2010, 6, 5, 23, 45),
            },
            # step 2
            {
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 6): 2.5},
                'end': datetime(2010, 6, 6, 0, 45),
                'end_billing': datetime(2010, 6, 6, 0, 45),
                'label': 'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': 'woo',
                'pool': None,
                'start': datetime(2010, 6, 6, 0, 30),
            },
            # step 3 (and idle time after)
            {
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {date(2010, 6, 6): 5.0},
                'date_to_nih_billed': {date(2010, 6, 6): 7.5},
                'end': datetime(2010, 6, 6, 1, 0),
                'end_billing': datetime(2010, 6, 6, 1, 30),
                'label': 'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 5.0,
                'nih_billed': 7.5,
                'owner': 'woo',
                'pool': None,
                'start': datetime(2010, 6, 6, 0, 45),
            },
            ])

    def test_pooled_job_flow(self):
        # same as test case above with different job names
        job_flow = MockEmrObject(
            bootstrapactions=[
                MockEmrObject(args=[]),
                MockEmrObject(args=[
                    MockEmrObject(value='pool-1234567890abcdef'),
                    MockEmrObject(value='reflecting'),
                ]),
            ],
            enddatetime='2010-06-06T01:15:00Z',  # 2 hours are billed
            name='mr_exciting.woo.20100605.232950.000000',
            normalizedinstancehours='20',
            readydatetime='2010-06-05T23:45:00Z',  # only 15 minutes "used"
            startdatetime='2010-06-05T23:30:00Z',
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

        intervals = job_flow_to_intervals(job_flow)

        self.assertEqual(
            intervals, [
            # bootstrapping
            {
                'date_to_nih_used': {date(2010, 6, 5): 2.5},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 5): 2.5},
                'end': datetime(2010, 6, 5, 23, 45),
                'end_billing': datetime(2010, 6, 5, 23, 45),
                'label': 'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': 'woo',
                'pool': 'reflecting',
                'start': datetime(2010, 6, 5, 23, 30),
            },
            # mr_exciting, step 1 (and idle time after)
            {
                'date_to_nih_used': {date(2010, 6, 5): 2.5,
                                     date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {date(2010, 6, 6): 2.5},
                'date_to_nih_billed': {date(2010, 6, 5): 2.5,
                                       date(2010, 6, 6): 5.0},
                'end': datetime(2010, 6, 6, 0, 15),
                'end_billing': datetime(2010, 6, 6, 0, 30),
                'label': 'mr_exciting',
                'nih_used': 5.0,
                'nih_bbnu': 2.5,
                'nih_billed': 7.5,
                'owner': 'woo',
                'pool': 'reflecting',
                'start': datetime(2010, 6, 5, 23, 45),
            },
            # mr whatever, step 1
            {
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 6): 2.5},
                'end': datetime(2010, 6, 6, 0, 45),
                'end_billing': datetime(2010, 6, 6, 0, 45),
                'label': 'mr_whatever',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': 'meh',
                'pool': 'reflecting',
                'start': datetime(2010, 6, 6, 0, 30),
            },
            # mr whatever, step 2 (and idle time after)
            {
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {date(2010, 6, 6): 5.0},
                'date_to_nih_billed': {date(2010, 6, 6): 7.5},
                'end': datetime(2010, 6, 6, 1, 0),
                'end_billing': datetime(2010, 6, 6, 1, 30),
                'label': 'mr_whatever',
                'nih_used': 2.5,
                'nih_bbnu': 5.0,
                'nih_billed': 7.5,
                'owner': 'meh',
                'pool': 'reflecting',
                'start': datetime(2010, 6, 6, 0, 45),
            },
            ])


class ParseLabelAndOwnerTestCase(unittest.TestCase):

    def test_empty(self):
        self.assertEqual(parse_label_and_owner(''), (None, None))
        self.assertRaises(TypeError, parse_label_and_owner, None)

    def test_job_flow_name(self):
        self.assertEqual(
            parse_label_and_owner('mr_exciting.woo.20100605.235950.000000'),
            ('mr_exciting', 'woo'))

    def test_step_name(self):
        self.assertEqual(
            parse_label_and_owner(
                'mr_exciting.woo.20100605.235950.000000: Step 7 of 13'),
            ('mr_exciting', 'woo'))

    def test_non_mrjob_name(self):
        self.assertEqual(
            parse_label_and_owner('Development Job Flow'),
            (None, None))


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
