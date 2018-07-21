# Copyright 2011 Yelp
# Copyright 2012 Yelp and Contributors
# Copyright 2015-2016 Yelp
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
from datetime import date
from datetime import datetime
from datetime import timedelta
from unittest import TestCase

import boto.emr.connection
from mrjob.tools.emr.audit_usage import _cluster_to_full_summary
from mrjob.tools.emr.audit_usage import _percent
from mrjob.tools.emr.audit_usage import _subdivide_interval_by_date
from mrjob.tools.emr.audit_usage import _subdivide_interval_by_hour
from mrjob.tools.emr.audit_usage import main

from tests.mockboto import MockEmrObject
from tests.py2 import patch
from tests.tools.emr import ToolTestCase


class AuditUsageTestCase(ToolTestCase):

    def setUp(self):
        super(AuditUsageTestCase, self).setUp()

        self.repeat_sleep = self.start(patch('time.sleep'))
        # this is called once per cluster (no pagination), so we can
        # test quantity as well as whether it was called
        self.describe_cluster_sleep = self.start(
            patch('mrjob.tools.emr.audit_usage.sleep'))

    def test_with_no_clusters(self):
        self.monkey_patch_stdout()
        main(['-q', '--no-conf'])  # make sure it doesn't crash

        self.assertTrue(self.repeat_sleep.called)
        self.assertFalse(self.describe_cluster_sleep.called)

    def test_with_one_cluster(self):
        emr_conn = boto.emr.connection.EmrConnection()
        emr_conn.run_jobflow('no name', job_flow_role='fake-instance-profile',
                             service_role='fake-service-role')

        self.monkey_patch_stdout()
        main(['-q', '--no-conf'])
        self.assertIn(b'j-MOCKCLUSTER0', sys.stdout.getvalue())

        self.assertTrue(self.repeat_sleep.called)
        self.assertEqual(self.describe_cluster_sleep.call_count, 1)


class ClusterToFullSummaryTestCase(TestCase):

    maxDiff = None  # show whole diff when tests fail

    def test_basic_cluster_with_no_steps(self):
        cluster = MockEmrObject(
            id='j-ISFORJAGUAR',
            name='mr_exciting.woo.20100605.235850.000000',
            normalizedinstancehours='10',
            status=MockEmrObject(
                state='TERMINATED',
                timeline=MockEmrObject(
                    creationdatetime='2010-06-06T00:00:00Z',
                    enddatetime='2010-06-06T00:30:00Z',
                    readydatetime='2010-06-06T00:15:00Z',
                ),
            ),
        )

        summary = _cluster_to_full_summary(cluster)

        self.assertEqual(summary, {
            'created': datetime(2010, 6, 6, 0, 0),
            'end': datetime(2010, 6, 6, 0, 30),
            'id': u'j-ISFORJAGUAR',
            'label': u'mr_exciting',
            'name': u'mr_exciting.woo.20100605.235850.000000',
            'nih': 10.0,
            'nih_bbnu': 7.5,
            'nih_billed': 10.0,
            'nih_used': 2.5,  # only a quarter of time billed was used
            'num_steps': 0,
            'owner': u'woo',
            'pool': None,
            'ran': timedelta(minutes=30),
            'ready': datetime(2010, 6, 6, 0, 15),
            'state': u'TERMINATED',
            'usage': [{
                'date_to_nih_bbnu': {date(2010, 6, 6): 7.5},
                'date_to_nih_billed': {date(2010, 6, 6): 10.0},
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'end': datetime(2010, 6, 6, 0, 15),
                'end_billing': datetime(2010, 6, 6, 1, 0),
                'label': u'mr_exciting',
                'hour_to_nih_bbnu': {datetime(2010, 6, 6, 0): 7.5},
                'hour_to_nih_billed': {datetime(2010, 6, 6, 0): 10.0},
                'hour_to_nih_used': {datetime(2010, 6, 6, 0): 2.5},
                'nih_bbnu': 7.5,
                'nih_billed': 10.0,
                'nih_used': 2.5,
                'owner': u'woo',
                'start': datetime(2010, 6, 6, 0, 0),
                'step_num': None,
            }],
        })

    def test_still_running_cluster_with_no_steps(self):

        cluster = MockEmrObject(
            id='j-ISFORJUICE',
            name='mr_exciting.woo.20100605.235850.000000',
            normalizedinstancehours='10',
            status=MockEmrObject(
                state='WAITING',
                timeline=MockEmrObject(
                    creationdatetime='2010-06-06T00:00:00Z',
                    readydatetime='2010-06-06T00:15:00Z',
                ),
            ),
        )

        summary = _cluster_to_full_summary(
            cluster, now=datetime(2010, 6, 6, 0, 30))

        self.assertEqual(summary, {
            'created': datetime(2010, 6, 6, 0, 0),
            'end': None,
            'id': u'j-ISFORJUICE',
            'label': u'mr_exciting',
            'name': u'mr_exciting.woo.20100605.235850.000000',
            'nih': 10.0,
            'nih_bbnu': 2.5,
            'nih_billed': 5.0,
            'nih_used': 2.5,
            'num_steps': 0,
            'owner': u'woo',
            'pool': None,
            'ran': timedelta(minutes=30),
            'ready': datetime(2010, 6, 6, 0, 15),
            'state': u'WAITING',
            'usage': [{
                'date_to_nih_bbnu': {date(2010, 6, 6): 2.5},
                'date_to_nih_billed': {date(2010, 6, 6): 5.0},
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'end': datetime(2010, 6, 6, 0, 15),
                'end_billing': datetime(2010, 6, 6, 0, 30),
                'hour_to_nih_bbnu': {datetime(2010, 6, 6, 0): 2.5},
                'hour_to_nih_billed': {datetime(2010, 6, 6, 0): 5.0},
                'hour_to_nih_used': {datetime(2010, 6, 6, 0): 2.5},
                'label': u'mr_exciting',
                'nih_bbnu': 2.5,
                'nih_billed': 5.0,
                'nih_used': 2.5,
                'owner': u'woo',
                'start': datetime(2010, 6, 6, 0, 0),
                'step_num': None,
            }],
        })

    def test_still_bootstrapping_cluster_with_no_steps(self):
        cluster = MockEmrObject(
            id='j-ISFORJOKE',
            name='mr_exciting.woo.20100605.235850.000000',
            normalizedinstancehours='10',
            status=MockEmrObject(
                state='BOOTSTRAPPING',
                timeline=MockEmrObject(
                    creationdatetime='2010-06-06T00:00:00Z',
                ),
            ),
        )

        summary = _cluster_to_full_summary(
            cluster, now=datetime(2010, 6, 6, 0, 30))

        self.assertEqual(summary, {
            'created': datetime(2010, 6, 6, 0, 0),
            'end': None,
            'id': u'j-ISFORJOKE',
            'label': u'mr_exciting',
            'name': u'mr_exciting.woo.20100605.235850.000000',
            'nih': 10.0,
            'nih_bbnu': 0.0,
            'nih_billed': 5.0,
            'nih_used': 5.0,
            'num_steps': 0,
            'owner': u'woo',
            'pool': None,
            'ran': timedelta(minutes=30),
            'ready': None,
            'state': u'BOOTSTRAPPING',
            'usage': [{
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 6): 5.0},
                'date_to_nih_used': {date(2010, 6, 6): 5.0},
                'end': datetime(2010, 6, 6, 0, 30),
                'end_billing': datetime(2010, 6, 6, 0, 30),
                'hour_to_nih_bbnu': {},
                'hour_to_nih_billed': {datetime(2010, 6, 6, 0): 5.0},
                'hour_to_nih_used': {datetime(2010, 6, 6, 0): 5.0},
                'label': u'mr_exciting',
                'nih_bbnu': 0.0,
                'nih_billed': 5.0,
                'nih_used': 5.0,
                'owner': u'woo',
                'start': datetime(2010, 6, 6, 0, 0),
                'step_num': None,
            }],
        })

    def test_cluster_that_was_terminated_before_ready(self):
        cluster = MockEmrObject(
            id='j-ISFORJOURNEY',
            name='mr_exciting.woo.20100605.235850.000000',
            normalizedinstancehours='1',
            status=MockEmrObject(
                state='TERMINATED',
                timeline=MockEmrObject(
                    creationdatetime='2010-06-06T00:00:00Z',
                    enddatetime='2010-06-06T00:30:00Z',
                ),
            ),
        )

        summary = _cluster_to_full_summary(
            cluster, now=datetime(2010, 6, 6, 1))

        self.assertEqual(summary, {
            'created': datetime(2010, 6, 6, 0, 0),
            'end': datetime(2010, 6, 6, 0, 30),
            'id': u'j-ISFORJOURNEY',
            'label': u'mr_exciting',
            'name': u'mr_exciting.woo.20100605.235850.000000',
            'nih': 1.0,
            'nih_bbnu': 0.5,
            'nih_billed': 1.0,
            'nih_used': 0.5,
            'num_steps': 0,
            'owner': u'woo',
            'pool': None,
            'ran': timedelta(minutes=30),
            'ready': None,
            'state': u'TERMINATED',
            'usage': [{
                'date_to_nih_bbnu': {date(2010, 6, 6): 0.5},
                'date_to_nih_billed': {date(2010, 6, 6): 1.0},
                'date_to_nih_used': {date(2010, 6, 6): 0.5},
                'end': datetime(2010, 6, 6, 0, 30),
                'end_billing': datetime(2010, 6, 6, 1, 0),
                'hour_to_nih_bbnu': {datetime(2010, 6, 6, 0, 0): 0.5},
                'hour_to_nih_billed': {datetime(2010, 6, 6, 0, 0): 1.0},
                'hour_to_nih_used': {datetime(2010, 6, 6, 0, 0): 0.5},
                'label': u'mr_exciting',
                'nih_bbnu': 0.5,
                'nih_billed': 1.0,
                'nih_used': 0.5,
                'owner': u'woo',
                'start': datetime(2010, 6, 6, 0, 0),
                'step_num': None,
            }],
        })

    def test_cluster_with_no_fields(self):
        # this shouldn't happen in practice; just a robustness check
        cluster = MockEmrObject()

        summary = _cluster_to_full_summary(cluster)

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
            'state': None,
            'usage': [],
        })

    def test_cluster_with_no_steps_split_over_midnight(self):
        cluster = MockEmrObject(
            id='j-ISFORJOY',
            name='mr_exciting.woo.20100605.232950.000000',
            normalizedinstancehours='20',
            status=MockEmrObject(
                state='TERMINATED',
                timeline=MockEmrObject(
                    creationdatetime='2010-06-05T23:30:00Z',
                    enddatetime='2010-06-06T01:15:00Z',  # 2 hours billed
                    readydatetime='2010-06-05T23:45:00Z',  # 15 minutes "used"
                ),
            ),
        )

        summary = _cluster_to_full_summary(cluster)

        self.assertEqual(summary, {
            'created': datetime(2010, 6, 5, 23, 30),
            'end': datetime(2010, 6, 6, 1, 15),
            'id': u'j-ISFORJOY',
            'label': u'mr_exciting',
            'name': u'mr_exciting.woo.20100605.232950.000000',
            'nih': 20.0,
            'nih_bbnu': 17.5,
            'nih_billed': 20.0,
            'nih_used': 2.5,
            'num_steps': 0,
            'owner': u'woo',
            'pool': None,
            'ran': timedelta(hours=1, minutes=45),
            'ready': datetime(2010, 6, 5, 23, 45),
            'state': u'TERMINATED',
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
                'label': u'mr_exciting',
                'nih_bbnu': 17.5,
                'nih_billed': 20.0,
                'nih_used': 2.5,
                'owner': u'woo',
                'start': datetime(2010, 6, 5, 23, 30),
                'step_num': None,
            }],
        })

    def test_cluster_with_one_still_running_step(self):
        cluster = MockEmrObject(
            id='j-ISFORJUNGLE',
            name='mr_exciting.woo.20100606.035855.000000',
            normalizedinstancehours='20',
            status=MockEmrObject(
                state='RUNNING',
                timeline=MockEmrObject(
                    creationdatetime='2010-06-06T04:00:00Z',
                    readydatetime='2010-06-06T04:15:00Z',
                ),
            ),
            steps=[
                MockEmrObject(
                    name='mr_exciting.woo.20100606.035855.000000: Step 1 of 3',
                    status=MockEmrObject(
                        timeline=MockEmrObject(
                            startdatetime='2010-06-06T04:15:00Z',
                        ),
                    ),
                ),
            ],
        )

        summary = _cluster_to_full_summary(
            cluster, now=datetime(2010, 6, 6, 5, 30))

        self.assertEqual(summary, {
            'created': datetime(2010, 6, 6, 4, 0),
            'end': None,
            'id': u'j-ISFORJUNGLE',
            'label': u'mr_exciting',
            'name': u'mr_exciting.woo.20100606.035855.000000',
            'nih': 20.0,
            'nih_bbnu': 0.0,
            'nih_billed': 15.0,
            'nih_used': 15.0,
            'num_steps': 1,
            'owner': u'woo',
            'pool': None,
            'ran': timedelta(hours=1, minutes=30),
            'ready': datetime(2010, 6, 6, 4, 15),
            'state': u'RUNNING',
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
                'label': u'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': u'woo',
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
                'label': u'mr_exciting',
                'nih_used': 12.5,
                'nih_bbnu': 0.0,
                'nih_billed': 12.5,
                'owner': u'woo',
                'start': datetime(2010, 6, 6, 4, 15),
                'step_num': 1,
            }],
        })

    def test_cluster_with_one_cancelled_step(self):
        cluster = MockEmrObject(
            id='j-ISFORJACUZZI',
            name='mr_exciting.woo.20100606.035855.000000',
            normalizedinstancehours='20',
            status=MockEmrObject(
                state='RUNNING',
                timeline=MockEmrObject(
                    creationdatetime='2010-06-06T04:00:00Z',
                    enddatetime='2010-06-06T05:30:00Z',
                    readydatetime='2010-06-06T04:15:00Z',
                ),
            ),
            # step doesn't have end time even though cluster does
            steps=[
                MockEmrObject(
                    name='mr_exciting.woo.20100606.035855.000000: Step 1 of 3',
                    status=MockEmrObject(
                        timeline=MockEmrObject(
                            startdatetime='2010-06-06T04:15:00Z',
                        ),
                    ),
                ),
            ]
        )

        summary = _cluster_to_full_summary(cluster)

        self.assertEqual(summary, {
            'created': datetime(2010, 6, 6, 4, 0),
            'end': datetime(2010, 6, 6, 5, 30),
            'id': u'j-ISFORJACUZZI',
            'label': u'mr_exciting',
            'name': u'mr_exciting.woo.20100606.035855.000000',
            'nih': 20.0,
            'nih_bbnu': 17.5,
            'nih_billed': 20.0,
            'nih_used': 2.5,
            'num_steps': 1,
            'owner': u'woo',
            'pool': None,
            'ran': timedelta(hours=1, minutes=30),
            'ready': datetime(2010, 6, 6, 4, 15),
            'state': u'RUNNING',
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
                'label': u'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': u'woo',
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
                'label': u'mr_exciting',
                'nih_used': 0.0,
                'nih_bbnu': 17.5,
                'nih_billed': 17.5,
                'owner': u'woo',
                'start': datetime(2010, 6, 6, 4, 15),
                'step_num': 1,
            }],
        })

    def test_multi_step_cluster(self):
        cluster = MockEmrObject(
            id='j-ISFORJOB',
            name='mr_exciting.woo.20100605.232850.000000',
            normalizedinstancehours='20',
            status=MockEmrObject(
                state='TERMINATED',
                timeline=MockEmrObject(
                    creationdatetime='2010-06-05T23:30:00Z',
                    enddatetime='2010-06-06T01:15:00Z',  # 2 hours are billed
                    readydatetime='2010-06-05T23:45:00Z',
                ),
            ),
            steps=[
                MockEmrObject(
                    name='mr_exciting.woo.20100605.232850.000000: Step 1 of 3',
                    status=MockEmrObject(
                        timeline=MockEmrObject(
                            startdatetime='2010-06-05T23:45:00Z',
                            enddatetime='2010-06-06T00:15:00Z',
                        ),
                    ),
                ),
                MockEmrObject(
                    name='mr_exciting.woo.20100605.232850.000000: Step 2 of 3',
                    status=MockEmrObject(
                        timeline=MockEmrObject(
                            startdatetime='2010-06-06T00:30:00Z',
                            enddatetime='2010-06-06T00:45:00Z',
                        ),
                    ),
                ),
                MockEmrObject(
                    name='mr_exciting.woo.20100605.232850.000000: Step 3 of 3',
                    status=MockEmrObject(
                        timeline=MockEmrObject(
                            startdatetime='2010-06-06T00:45:00Z',
                            enddatetime='2010-06-06T01:00:00Z',
                        ),
                    ),
                ),
            ],
        )

        summary = _cluster_to_full_summary(cluster)

        self.assertEqual(summary, {
            'created': datetime(2010, 6, 5, 23, 30),
            'end': datetime(2010, 6, 6, 1, 15),
            'id': u'j-ISFORJOB',
            'label': u'mr_exciting',
            'name': u'mr_exciting.woo.20100605.232850.000000',
            'nih': 20.0,
            'nih_bbnu': 7.5,
            'nih_billed': 20.0,
            'nih_used': 12.5,
            'num_steps': 3,
            'owner': u'woo',
            'pool': None,
            'ran': timedelta(hours=1, minutes=45),
            'ready': datetime(2010, 6, 5, 23, 45),
            'state': u'TERMINATED',
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
                'label': u'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': u'woo',
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
                'label': u'mr_exciting',
                'nih_used': 5.0,
                'nih_bbnu': 2.5,
                'nih_billed': 7.5,
                'owner': u'woo',
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
                'label': u'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': u'woo',
                'start': datetime(2010, 6, 6, 0, 30),
                'step_num': 2,
            }, {
                # step 3 (and idle time after)
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {date(2010, 6, 6): 5.0},
                'date_to_nih_billed': {date(2010, 6, 6): 7.5},
                'end': datetime(2010, 6, 6, 1, 0),
                'end_billing': datetime(2010, 6, 6, 1, 30),
                'hour_to_nih_bbnu': {datetime(2010, 6, 6, 1): 5.0},
                'hour_to_nih_billed': {datetime(2010, 6, 6, 0): 2.5,
                                       datetime(2010, 6, 6, 1): 5.0},
                'hour_to_nih_used': {datetime(2010, 6, 6, 0): 2.5},
                'label': u'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 5.0,
                'nih_billed': 7.5,
                'owner': u'woo',
                'start': datetime(2010, 6, 6, 0, 45),
                'step_num': 3,
            }],
        })

    def test_pooled_cluster(self):
        # same as test case above with different job keys
        cluster = MockEmrObject(
            bootstrapactions=[
                MockEmrObject(args=[]),
                MockEmrObject(args=[
                    MockEmrObject(
                        value='pool-0123456789abcdef0123456789abcdef'),
                    MockEmrObject(value='reflecting'),
                ]),
            ],
            id='j-ISFORJOB',
            name='mr_exciting.woo.20100605.232850.000000',
            normalizedinstancehours='20',
            state='TERMINATED',
            status=MockEmrObject(
                state='TERMINATED',
                timeline=MockEmrObject(
                    creationdatetime='2010-06-05T23:30:00Z',
                    enddatetime='2010-06-06T01:15:00Z',  # 2 hours are billed
                    readydatetime='2010-06-05T23:45:00Z',
                ),
            ),
            steps=[
                MockEmrObject(
                    name='mr_exciting.woo.20100605.232950.000000: Step 1 of 1',
                    status=MockEmrObject(
                        timeline=MockEmrObject(
                            startdatetime='2010-06-05T23:45:00Z',
                            enddatetime='2010-06-06T00:15:00Z',
                        ),
                    ),
                ),
                MockEmrObject(
                    name='mr_whatever.meh.20100606.002000.000000: Step 1 of 2',
                    status=MockEmrObject(
                        timeline=MockEmrObject(
                            startdatetime='2010-06-06T00:30:00Z',
                            enddatetime='2010-06-06T00:45:00Z',
                        ),
                    ),
                ),
                MockEmrObject(
                    name='mr_whatever.meh.20100606.002000.000000: Step 2 of 2',
                    status=MockEmrObject(
                        timeline=MockEmrObject(
                            startdatetime='2010-06-06T00:45:00Z',
                            enddatetime='2010-06-06T01:00:00Z',
                        ),
                    ),
                ),
            ],
        )

        summary = _cluster_to_full_summary(cluster)

        self.assertEqual(summary, {
            'created': datetime(2010, 6, 5, 23, 30),
            'end': datetime(2010, 6, 6, 1, 15),
            'id': u'j-ISFORJOB',
            'label': u'mr_exciting',
            'name': u'mr_exciting.woo.20100605.232850.000000',
            'nih': 20.0,
            'nih_bbnu': 7.5,
            'nih_billed': 20.0,
            'nih_used': 12.5,
            'num_steps': 3,
            'owner': u'woo',
            'pool': u'reflecting',
            'ran': timedelta(hours=1, minutes=45),
            'ready': datetime(2010, 6, 5, 23, 45),
            'state': 'TERMINATED',
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
                'label': u'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': u'woo',
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
                'label': u'mr_exciting',
                'nih_used': 5.0,
                'nih_bbnu': 2.5,
                'nih_billed': 7.5,
                'owner': u'woo',
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
                'label': u'mr_whatever',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': u'meh',
                'start': datetime(2010, 6, 6, 0, 30),
                'step_num': 1,
            }, {
                # mr whatever, step 2 (and idle time after)
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {date(2010, 6, 6): 5.0},
                'date_to_nih_billed': {date(2010, 6, 6): 7.5},
                'end': datetime(2010, 6, 6, 1, 0),
                'end_billing': datetime(2010, 6, 6, 1, 30),
                'hour_to_nih_bbnu': {datetime(2010, 6, 6, 1): 5.0},
                'hour_to_nih_billed': {datetime(2010, 6, 6, 0): 2.5,
                                       datetime(2010, 6, 6, 1): 5.0},
                'hour_to_nih_used': {datetime(2010, 6, 6, 0): 2.5},
                'label': u'mr_whatever',
                'nih_used': 2.5,
                'nih_bbnu': 5.0,
                'nih_billed': 7.5,
                'owner': u'meh',
                'start': datetime(2010, 6, 6, 0, 45),
                'step_num': 2,
            }],
        })


class SubdivideIntervalByDateTestCase(TestCase):

    def test_zero_interval(self):
        self.assertEqual(
            _subdivide_interval_by_date(
                datetime(2010, 6, 6, 4, 26),
                datetime(2010, 6, 6, 4, 26),
            ),
            {}
        )

    def test_same_day(self):
        self.assertEqual(
            _subdivide_interval_by_date(
                datetime(2010, 6, 6, 4, 0),
                datetime(2010, 6, 6, 6, 0),
            ),
            {date(2010, 6, 6): 7200.0}
        )

    def test_start_at_midnight(self):
        self.assertEqual(
            _subdivide_interval_by_date(
                datetime(2010, 6, 6, 0, 0),
                datetime(2010, 6, 6, 5, 0),
            ),
            {date(2010, 6, 6): 18000.0}
        )

    def test_end_at_midnight(self):
        self.assertEqual(
            _subdivide_interval_by_date(
                datetime(2010, 6, 5, 23, 0),
                datetime(2010, 6, 6, 0, 0),
            ),
            {date(2010, 6, 5): 3600.0}
        )

    def test_split_over_midnight(self):
        self.assertEqual(
            _subdivide_interval_by_date(
                datetime(2010, 6, 5, 23, 0),
                datetime(2010, 6, 6, 5, 0),
            ),
            {date(2010, 6, 5): 3600.0,
             date(2010, 6, 6): 18000.0}
        )

    def test_full_days(self):
        self.assertEqual(
            _subdivide_interval_by_date(
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


class SubdivideIntervalByHourTestCase(TestCase):

    def test_zero_interval(self):
        self.assertEqual(
            _subdivide_interval_by_hour(
                datetime(2010, 6, 6, 4, 26),
                datetime(2010, 6, 6, 4, 26),
            ),
            {}
        )

    def test_same_hour(self):
        self.assertEqual(
            _subdivide_interval_by_hour(
                datetime(2010, 6, 6, 4, 24),
                datetime(2010, 6, 6, 4, 26),
            ),
            {datetime(2010, 6, 6, 4): 120.0}
        )

    def test_start_at_midnight(self):
        self.assertEqual(
            _subdivide_interval_by_hour(
                datetime(2010, 6, 6, 0, 0),
                datetime(2010, 6, 6, 0, 3),
            ),
            {datetime(2010, 6, 6, 0): 180.0}
        )

    def test_end_at_midnight(self):
        self.assertEqual(
            _subdivide_interval_by_hour(
                datetime(2010, 6, 5, 23, 55),
                datetime(2010, 6, 6, 0, 0),
            ),
            {datetime(2010, 6, 5, 23): 300.0}
        )

    def test_split_over_midnight(self):
        self.assertEqual(
            _subdivide_interval_by_hour(
                datetime(2010, 6, 5, 23, 55),
                datetime(2010, 6, 6, 0, 3),
            ),
            {datetime(2010, 6, 5, 23): 300.0,
             datetime(2010, 6, 6, 0): 180.0}
        )

    def test_full_hours(self):
        self.assertEqual(
            _subdivide_interval_by_hour(
                datetime(2010, 6, 5, 23, 40),
                datetime(2010, 6, 6, 2, 10),
            ),
            {datetime(2010, 6, 5, 23): 1200.0,
             datetime(2010, 6, 6, 0): 3600.0,
             datetime(2010, 6, 6, 1): 3600.0,
             datetime(2010, 6, 6, 2): 600.0}
        )


class PercentTestCase(TestCase):

    def test_basic(self):
        self.assertEqual(62.5, _percent(5, 8))

    def test_default(self):
        self.assertEqual(0.0, _percent(1, 0))
        self.assertEqual(0.0, _percent(0, 0))
        self.assertEqual(None, _percent(0, 0, default=None))
