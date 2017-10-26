# Copyright 2011 Yelp
# Copyright 2012 Yelp and Contributors
# Copyright 2015-2016 Yelp
# Copyright 2017 Yelp
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

import boto3
from dateutil.parser import parse
from dateutil.tz import tzutc

from mrjob.tools.emr.audit_usage import _cluster_to_full_summary
from mrjob.tools.emr.audit_usage import _percent
from mrjob.tools.emr.audit_usage import _subdivide_interval_by_date
from mrjob.tools.emr.audit_usage import _subdivide_interval_by_hour
from mrjob.tools.emr.audit_usage import main

from tests.py2 import patch
from tests.tools.emr import ToolTestCase


# this test used to use naive datetimes
def utc(*args):
    return datetime(*args, tzinfo=tzutc())


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
        emr_client = boto3.client('emr')
        emr_client.run_job_flow(
            Name='no name',
            Instances=dict(
                MasterInstanceType='m1.medium',
                InstanceCount=1),
            JobFlowRole='fake-instance-profile',
            ServiceRole='fake-service-role',
            ReleaseLabel='emr-5.0.0')

        self.monkey_patch_stdout()
        main(['-q', '--no-conf'])
        self.assertIn(b'j-MOCKCLUSTER0', sys.stdout.getvalue())

        self.assertTrue(self.repeat_sleep.called)
        self.assertEqual(self.describe_cluster_sleep.call_count, 1)


class ClusterToFullSummaryTestCase(TestCase):

    maxDiff = None  # show whole diff when tests fail

    def test_basic_cluster_with_no_steps(self):
        cluster = dict(
            BootstrapActions=[],
            Id='j-ISFORJAGUAR',
            Name='mr_exciting.woo.20100605.235850.000000',
            NormalizedInstanceHours=10,
            Steps=[],
            Status=dict(
                State='TERMINATED',
                Timeline=dict(
                    CreationDateTime=parse('2010-06-06T00:00:00Z'),
                    EndDateTime=parse('2010-06-06T00:30:00Z'),
                    ReadyDateTime=parse('2010-06-06T00:15:00Z'),
                ),
            ),
        )

        summary = _cluster_to_full_summary(cluster)

        self.assertEqual(summary, {
            'created': utc(2010, 6, 6, 0, 0),
            'end': utc(2010, 6, 6, 0, 30),
            'id': u'j-ISFORJAGUAR',
            'label': u'mr_exciting',
            'name': u'mr_exciting.woo.20100605.235850.000000',
            'nih': 10.0,
            'nih_bbnu': 2.5,
            'nih_billed': 5.0,
            'nih_used': 2.5,  # everything after bootstrapping was wasted
            'num_steps': 0,
            'owner': u'woo',
            'pool': None,
            'ran': timedelta(minutes=30),
            'ready': utc(2010, 6, 6, 0, 15),
            'state': u'TERMINATED',
            'usage': [{
                'date_to_nih_bbnu': {date(2010, 6, 6): 2.5},
                'date_to_nih_billed': {date(2010, 6, 6): 5.0},
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'end': utc(2010, 6, 6, 0, 15),
                'end_billing': utc(2010, 6, 6, 0, 30),
                'label': u'mr_exciting',
                'hour_to_nih_bbnu': {utc(2010, 6, 6, 0): 2.5},
                'hour_to_nih_billed': {utc(2010, 6, 6, 0): 5.0},
                'hour_to_nih_used': {utc(2010, 6, 6, 0): 2.5},
                'nih_bbnu': 2.5,
                'nih_billed': 5.0,
                'nih_used': 2.5,
                'owner': u'woo',
                'start': utc(2010, 6, 6, 0, 0),
                'step_num': None,
            }],
        })

    def test_still_running_cluster_with_no_steps(self):

        cluster = dict(
            BootstrapActions=[],
            Id='j-ISFORJUICE',
            Name='mr_exciting.woo.20100605.235850.000000',
            NormalizedInstanceHours=10,
            Steps=[],
            Status=dict(
                State='WAITING',
                Timeline=dict(
                    CreationDateTime=parse('2010-06-06T00:00:00Z'),
                    ReadyDateTime=parse('2010-06-06T00:15:00Z'),
                ),
            ),
        )

        summary = _cluster_to_full_summary(
            cluster, now=utc(2010, 6, 6, 0, 30))

        self.assertEqual(summary, {
            'created': utc(2010, 6, 6, 0, 0),
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
            'ready': utc(2010, 6, 6, 0, 15),
            'state': u'WAITING',
            'usage': [{
                'date_to_nih_bbnu': {date(2010, 6, 6): 2.5},
                'date_to_nih_billed': {date(2010, 6, 6): 5.0},
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'end': utc(2010, 6, 6, 0, 15),
                'end_billing': utc(2010, 6, 6, 0, 30),
                'hour_to_nih_bbnu': {utc(2010, 6, 6, 0): 2.5},
                'hour_to_nih_billed': {utc(2010, 6, 6, 0): 5.0},
                'hour_to_nih_used': {utc(2010, 6, 6, 0): 2.5},
                'label': u'mr_exciting',
                'nih_bbnu': 2.5,
                'nih_billed': 5.0,
                'nih_used': 2.5,
                'owner': u'woo',
                'start': utc(2010, 6, 6, 0, 0),
                'step_num': None,
            }],
        })

    def test_still_bootstrapping_cluster_with_no_steps(self):
        cluster = dict(
            BootstrapActions=[],
            Id='j-ISFORJOKE',
            Name='mr_exciting.woo.20100605.235850.000000',
            NormalizedInstanceHours=10,
            Status=dict(
                State='BOOTSTRAPPING',
                Timeline=dict(
                    CreationDateTime=parse('2010-06-06T00:00:00Z'),
                ),
            ),
            Steps=[],
        )

        summary = _cluster_to_full_summary(
            cluster, now=utc(2010, 6, 6, 0, 30))

        self.assertEqual(summary, {
            'created': utc(2010, 6, 6, 0, 0),
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
                'end': utc(2010, 6, 6, 0, 30),
                'end_billing': utc(2010, 6, 6, 0, 30),
                'hour_to_nih_bbnu': {},
                'hour_to_nih_billed': {utc(2010, 6, 6, 0): 5.0},
                'hour_to_nih_used': {utc(2010, 6, 6, 0): 5.0},
                'label': u'mr_exciting',
                'nih_bbnu': 0.0,
                'nih_billed': 5.0,
                'nih_used': 5.0,
                'owner': u'woo',
                'start': utc(2010, 6, 6, 0, 0),
                'step_num': None,
            }],
        })

    def test_cluster_that_was_terminated_before_ready(self):
        cluster = dict(
            BootstrapActions=[],
            Id='j-ISFORJOURNEY',
            Name='mr_exciting.woo.20100605.235850.000000',
            NormalizedInstanceHours=1,
            Steps=[],
            Status=dict(
                State='TERMINATED',
                Timeline=dict(
                    CreationDateTime=parse('2010-06-06T00:00:00Z'),
                    EndDateTime=parse('2010-06-06T00:30:00Z'),
                ),
            ),
        )

        summary = _cluster_to_full_summary(
            cluster, now=utc(2010, 6, 6, 1))

        self.assertEqual(summary, {
            'created': utc(2010, 6, 6, 0, 0),
            'end': utc(2010, 6, 6, 0, 30),
            'id': u'j-ISFORJOURNEY',
            'label': u'mr_exciting',
            'name': u'mr_exciting.woo.20100605.235850.000000',
            'nih': 1.0,
            'nih_bbnu': 0.0,
            'nih_billed': 0.5,
            'nih_used': 0.5,
            'num_steps': 0,
            'owner': u'woo',
            'pool': None,
            'ran': timedelta(minutes=30),
            'ready': None,
            'state': u'TERMINATED',
            'usage': [{
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 6): 0.5},
                'date_to_nih_used': {date(2010, 6, 6): 0.5},
                'end': utc(2010, 6, 6, 0, 30),
                'end_billing': utc(2010, 6, 6, 0, 30),
                'hour_to_nih_bbnu': {},
                'hour_to_nih_billed': {utc(2010, 6, 6, 0, 0): 0.5},
                'hour_to_nih_used': {utc(2010, 6, 6, 0, 0): 0.5},
                'label': u'mr_exciting',
                'nih_bbnu': 0.0,
                'nih_billed': 0.5,
                'nih_used': 0.5,
                'owner': u'woo',
                'start': utc(2010, 6, 6, 0, 0),
                'step_num': None,
            }],
        })

    def test_cluster_with_no_steps_split_over_midnight(self):
        cluster = dict(
            BootstrapActions=[],
            Id='j-ISFORJOY',
            Name='mr_exciting.woo.20100605.232950.000000',
            NormalizedInstanceHours=20,
            Status=dict(
                State='TERMINATED',
                Timeline=dict(
                    CreationDateTime=parse('2010-06-05T23:30:00Z'),
                    EndDateTime=parse('2010-06-06T01:15:00Z'),
                    ReadyDateTime=parse('2010-06-05T23:45:00Z'),
                ),
            ),
            Steps=[],
        )

        summary = _cluster_to_full_summary(cluster)

        self.assertEqual(summary, {
            'created': utc(2010, 6, 5, 23, 30),
            'end': utc(2010, 6, 6, 1, 15),
            'id': u'j-ISFORJOY',
            'label': u'mr_exciting',
            'name': u'mr_exciting.woo.20100605.232950.000000',
            'nih': 20.0,
            'nih_bbnu': 15.0,
            'nih_billed': 17.5,
            'nih_used': 2.5,
            'num_steps': 0,
            'owner': u'woo',
            'pool': None,
            'ran': timedelta(hours=1, minutes=45),
            'ready': utc(2010, 6, 5, 23, 45),
            'state': u'TERMINATED',
            'usage': [{
                'date_to_nih_bbnu': {date(2010, 6, 5): 2.5,
                                     date(2010, 6, 6): 12.5},
                'date_to_nih_billed': {date(2010, 6, 5): 5.0,
                                       date(2010, 6, 6): 12.5},
                'date_to_nih_used': {date(2010, 6, 5): 2.5},
                'end': utc(2010, 6, 5, 23, 45),
                'end_billing': utc(2010, 6, 6, 1, 15),
                'hour_to_nih_bbnu': {utc(2010, 6, 5, 23): 2.5,
                                     utc(2010, 6, 6, 0): 10.0,
                                     utc(2010, 6, 6, 1): 2.5},
                'hour_to_nih_billed': {utc(2010, 6, 5, 23): 5.0,
                                       utc(2010, 6, 6, 0): 10.0,
                                       utc(2010, 6, 6, 1): 2.5},
                'hour_to_nih_used': {utc(2010, 6, 5, 23): 2.5},
                'label': u'mr_exciting',
                'nih_bbnu': 15.0,
                'nih_billed': 17.5,
                'nih_used': 2.5,
                'owner': u'woo',
                'start': utc(2010, 6, 5, 23, 30),
                'step_num': None,
            }],
        })

    def test_cluster_with_one_still_running_step(self):
        cluster = dict(
            BootstrapActions=[],
            Id='j-ISFORJUNGLE',
            Name='mr_exciting.woo.20100606.035855.000000',
            NormalizedInstanceHours=20,
            Status=dict(
                State='RUNNING',
                Timeline=dict(
                    CreationDateTime=parse('2010-06-06T04:00:00Z'),
                    ReadyDateTime=parse('2010-06-06T04:15:00Z'),
                ),
            ),
            Steps=[
                dict(
                    Name='mr_exciting.woo.20100606.035855.000000: Step 1 of 3',
                    Status=dict(
                        Timeline=dict(
                            StartDateTime=parse('2010-06-06T04:15:00Z'),
                        ),
                    ),
                ),
            ],
        )

        summary = _cluster_to_full_summary(
            cluster, now=utc(2010, 6, 6, 5, 30))

        self.assertEqual(summary, {
            'created': utc(2010, 6, 6, 4, 0),
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
            'ready': utc(2010, 6, 6, 4, 15),
            'state': u'RUNNING',
            'usage': [{
                # bootstrapping
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 6): 2.5},
                'end': utc(2010, 6, 6, 4, 15),
                'end_billing': utc(2010, 6, 6, 4, 15),
                'hour_to_nih_used': {utc(2010, 6, 6, 4): 2.5},
                'hour_to_nih_bbnu': {},
                'hour_to_nih_billed': {utc(2010, 6, 6, 4): 2.5},
                'label': u'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': u'woo',
                'start': utc(2010, 6, 6, 4, 0),
                'step_num': None,
            }, {
                # mr_exciting, step 1
                'date_to_nih_used': {date(2010, 6, 6): 12.5},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 6): 12.5},
                'end': utc(2010, 6, 6, 5, 30),
                'end_billing': utc(2010, 6, 6, 5, 30),
                'hour_to_nih_used': {utc(2010, 6, 6, 4): 7.5,
                                     utc(2010, 6, 6, 5): 5.0},
                'hour_to_nih_bbnu': {},
                'hour_to_nih_billed': {utc(2010, 6, 6, 4): 7.5,
                                       utc(2010, 6, 6, 5): 5.0},
                'label': u'mr_exciting',
                'nih_used': 12.5,
                'nih_bbnu': 0.0,
                'nih_billed': 12.5,
                'owner': u'woo',
                'start': utc(2010, 6, 6, 4, 15),
                'step_num': 1,
            }],
        })

    def test_cluster_with_one_cancelled_step(self):
        cluster = dict(
            BootstrapActions=[],
            Id='j-ISFORJACUZZI',
            Name='mr_exciting.woo.20100606.035855.000000',
            NormalizedInstanceHours=20,
            Status=dict(
                State='RUNNING',
                Timeline=dict(
                    CreationDateTime=parse('2010-06-06T04:00:00Z'),
                    EndDateTime=parse('2010-06-06T05:30:00Z'),
                    ReadyDateTime=parse('2010-06-06T04:15:00Z'),
                ),
            ),
            # step doesn't have end time even though cluster does
            Steps=[
                dict(
                    Name='mr_exciting.woo.20100606.035855.000000: Step 1 of 3',
                    Status=dict(
                        Timeline=dict(
                            StartDateTime=parse('2010-06-06T04:15:00Z'),
                        ),
                    ),
                ),
            ]
        )

        summary = _cluster_to_full_summary(cluster)

        self.assertEqual(summary, {
            'created': utc(2010, 6, 6, 4, 0),
            'end': utc(2010, 6, 6, 5, 30),
            'id': u'j-ISFORJACUZZI',
            'label': u'mr_exciting',
            'name': u'mr_exciting.woo.20100606.035855.000000',
            'nih': 20.0,
            'nih_bbnu': 12.5,
            'nih_billed': 15.0,
            'nih_used': 2.5,
            'num_steps': 1,
            'owner': u'woo',
            'pool': None,
            'ran': timedelta(hours=1, minutes=30),
            'ready': utc(2010, 6, 6, 4, 15),
            'state': u'RUNNING',
            'usage': [{
                # bootstrapping
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 6): 2.5},
                'end': utc(2010, 6, 6, 4, 15),
                'end_billing': utc(2010, 6, 6, 4, 15),
                'hour_to_nih_used': {utc(2010, 6, 6, 4): 2.5},
                'hour_to_nih_bbnu': {},
                'hour_to_nih_billed': {utc(2010, 6, 6, 4): 2.5},
                'label': u'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': u'woo',
                'start': utc(2010, 6, 6, 4, 0),
                'step_num': None,
            }, {
                # mr_exciting, step 1 (cancelled)
                'date_to_nih_used': {},
                'date_to_nih_bbnu': {date(2010, 6, 6): 12.5},
                'date_to_nih_billed': {date(2010, 6, 6): 12.5},
                'end': utc(2010, 6, 6, 4, 15),
                'end_billing': utc(2010, 6, 6, 5, 30),
                'hour_to_nih_used': {},
                'hour_to_nih_bbnu': {utc(2010, 6, 6, 4): 7.5,
                                     utc(2010, 6, 6, 5): 5.0},
                'hour_to_nih_billed': {utc(2010, 6, 6, 4): 7.5,
                                       utc(2010, 6, 6, 5): 5.0},
                'label': u'mr_exciting',
                'nih_used': 0.0,
                'nih_bbnu': 12.5,
                'nih_billed': 12.5,
                'owner': u'woo',
                'start': utc(2010, 6, 6, 4, 15),
                'step_num': 1,
            }],
        })

    def test_multi_step_cluster(self):
        cluster = dict(
            BootstrapActions=[],
            Id='j-ISFORJOB',
            Name='mr_exciting.woo.20100605.232850.000000',
            NormalizedInstanceHours=20,
            Status=dict(
                State='TERMINATED',
                Timeline=dict(
                    CreationDateTime=parse('2010-06-05T23:30:00Z'),
                    EndDateTime=parse('2010-06-06T01:15:00Z'),
                    ReadyDateTime=parse('2010-06-05T23:45:00Z'),
                ),
            ),
            Steps=[
                dict(
                    Name='mr_exciting.woo.20100605.232850.000000: Step 1 of 3',
                    Status=dict(
                        Timeline=dict(
                            StartDateTime=parse('2010-06-05T23:45:00Z'),
                            EndDateTime=parse('2010-06-06T00:15:00Z'),
                        ),
                    ),
                ),
                dict(
                    Name='mr_exciting.woo.20100605.232850.000000: Step 2 of 3',
                    Status=dict(
                        Timeline=dict(
                            StartDateTime=parse('2010-06-06T00:30:00Z'),
                            EndDateTime=parse('2010-06-06T00:45:00Z'),
                        ),
                    ),
                ),
                dict(
                    Name='mr_exciting.woo.20100605.232850.000000: Step 3 of 3',
                    Status=dict(
                        Timeline=dict(
                            StartDateTime=parse('2010-06-06T00:45:00Z'),
                            EndDateTime=parse('2010-06-06T01:00:00Z'),
                        ),
                    ),
                ),
            ],
        )

        summary = _cluster_to_full_summary(cluster)

        self.assertEqual(summary, {
            'created': utc(2010, 6, 5, 23, 30),
            'end': utc(2010, 6, 6, 1, 15),
            'id': u'j-ISFORJOB',
            'label': u'mr_exciting',
            'name': u'mr_exciting.woo.20100605.232850.000000',
            'nih': 20.0,
            'nih_bbnu': 5.0,
            'nih_billed': 17.5,
            'nih_used': 12.5,
            'num_steps': 3,
            'owner': u'woo',
            'pool': None,
            'ran': timedelta(hours=1, minutes=45),
            'ready': utc(2010, 6, 5, 23, 45),
            'state': u'TERMINATED',
            'usage': [{
                # bootstrapping
                'date_to_nih_used': {date(2010, 6, 5): 2.5},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 5): 2.5},
                'end': utc(2010, 6, 5, 23, 45),
                'end_billing': utc(2010, 6, 5, 23, 45),
                'hour_to_nih_bbnu': {},
                'hour_to_nih_billed': {utc(2010, 6, 5, 23): 2.5},
                'hour_to_nih_used': {utc(2010, 6, 5, 23): 2.5},
                'label': u'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': u'woo',
                'start': utc(2010, 6, 5, 23, 30),
                'step_num': None,
            }, {
                # step 1 (and idle time after)
                'date_to_nih_used': {date(2010, 6, 5): 2.5,
                                     date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {date(2010, 6, 6): 2.5},
                'date_to_nih_billed': {date(2010, 6, 5): 2.5,
                                       date(2010, 6, 6): 5.0},
                'end': utc(2010, 6, 6, 0, 15),
                'end_billing': utc(2010, 6, 6, 0, 30),
                'hour_to_nih_bbnu': {utc(2010, 6, 6, 0): 2.5},
                'hour_to_nih_billed': {utc(2010, 6, 5, 23): 2.5,
                                       utc(2010, 6, 6, 0): 5.0},
                'hour_to_nih_used': {utc(2010, 6, 5, 23): 2.5,
                                     utc(2010, 6, 6, 0): 2.5},
                'label': u'mr_exciting',
                'nih_used': 5.0,
                'nih_bbnu': 2.5,
                'nih_billed': 7.5,
                'owner': u'woo',
                'start': utc(2010, 6, 5, 23, 45),
                'step_num': 1,
            }, {
                # step 2
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 6): 2.5},
                'end': utc(2010, 6, 6, 0, 45),
                'end_billing': utc(2010, 6, 6, 0, 45),
                'hour_to_nih_bbnu': {},
                'hour_to_nih_billed': {utc(2010, 6, 6, 0): 2.5},
                'hour_to_nih_used': {utc(2010, 6, 6, 0): 2.5},
                'label': u'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': u'woo',
                'start': utc(2010, 6, 6, 0, 30),
                'step_num': 2,
            }, {
                # step 3 (and idle time after)
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {date(2010, 6, 6): 2.5},
                'date_to_nih_billed': {date(2010, 6, 6): 5.0},
                'end': utc(2010, 6, 6, 1, 0),
                'end_billing': utc(2010, 6, 6, 1, 15),
                'hour_to_nih_bbnu': {utc(2010, 6, 6, 1): 2.5},
                'hour_to_nih_billed': {utc(2010, 6, 6, 0): 2.5,
                                       utc(2010, 6, 6, 1): 2.5},
                'hour_to_nih_used': {utc(2010, 6, 6, 0): 2.5},
                'label': u'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 2.5,
                'nih_billed': 5.0,
                'owner': u'woo',
                'start': utc(2010, 6, 6, 0, 45),
                'step_num': 3,
            }],
        })

    def test_pooled_cluster(self):
        self._test_new_or_legacy_pooled_cluster(Tags=[
            dict(Key='__mrjob_pool_hash',
                 Value='0123456789abcdef0123456789abcdef'),
            dict(Key='__mrjob_pool_name',
                 Value='reflecting'),
        ])

    def test_legacy_pooled_cluster(self):
        # audit clusters from previous versions of mrjob
        self._test_new_or_legacy_pooled_cluster(
            BootstrapActions=[
                dict(Args=[], Name='empty'),
                dict(Args=['pool-0123456789abcdef0123456789abcdef',
                           'reflecting'], Name='master'),
            ],
        )

    def _test_new_or_legacy_pooled_cluster(self, **kwargs):
        # same as test case above with different job keys
        cluster = dict(
            Id='j-ISFORJOB',
            Name='mr_exciting.woo.20100605.232850.000000',
            NormalizedInstanceHours=20,
            State='TERMINATED',
            Status=dict(
                State='TERMINATED',
                Timeline=dict(
                    CreationDateTime=parse('2010-06-05T23:30:00Z'),
                    EndDateTime=parse('2010-06-06T01:15:00Z'),
                    ReadyDateTime=parse('2010-06-05T23:45:00Z'),
                ),
            ),
            Steps=[
                dict(
                    Name='mr_exciting.woo.20100605.232950.000000: Step 1 of 1',
                    Status=dict(
                        Timeline=dict(
                            StartDateTime=parse('2010-06-05T23:45:00Z'),
                            EndDateTime=parse('2010-06-06T00:15:00Z'),
                        ),
                    ),
                ),
                dict(
                    Name='mr_whatever.meh.20100606.002000.000000: Step 1 of 2',
                    Status=dict(
                        Timeline=dict(
                            StartDateTime=parse('2010-06-06T00:30:00Z'),
                            EndDateTime=parse('2010-06-06T00:45:00Z'),
                        ),
                    ),
                ),
                dict(
                    Name='mr_whatever.meh.20100606.002000.000000: Step 2 of 2',
                    Status=dict(
                        Timeline=dict(
                            StartDateTime=parse('2010-06-06T00:45:00Z'),
                            EndDateTime=parse('2010-06-06T01:00:00Z'),
                        ),
                    ),
                ),
            ],
            **kwargs
        )

        summary = _cluster_to_full_summary(cluster)

        self.assertEqual(summary, {
            'created': utc(2010, 6, 5, 23, 30),
            'end': utc(2010, 6, 6, 1, 15),
            'id': u'j-ISFORJOB',
            'label': u'mr_exciting',
            'name': u'mr_exciting.woo.20100605.232850.000000',
            'nih': 20.0,
            'nih_bbnu': 5.0,
            'nih_billed': 17.5,
            'nih_used': 12.5,
            'num_steps': 3,
            'owner': u'woo',
            'pool': u'reflecting',
            'ran': timedelta(hours=1, minutes=45),
            'ready': utc(2010, 6, 5, 23, 45),
            'state': 'TERMINATED',
            'usage': [{
                # bootstrapping
                'date_to_nih_used': {date(2010, 6, 5): 2.5},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 5): 2.5},
                'end': utc(2010, 6, 5, 23, 45),
                'end_billing': utc(2010, 6, 5, 23, 45),
                'hour_to_nih_bbnu': {},
                'hour_to_nih_billed': {utc(2010, 6, 5, 23): 2.5},
                'hour_to_nih_used': {utc(2010, 6, 5, 23): 2.5},
                'label': u'mr_exciting',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': u'woo',
                'start': utc(2010, 6, 5, 23, 30),
                'step_num': None,
            }, {
                # mr_exciting, step 1 (and idle time after)
                'date_to_nih_used': {date(2010, 6, 5): 2.5,
                                     date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {date(2010, 6, 6): 2.5},
                'date_to_nih_billed': {date(2010, 6, 5): 2.5,
                                       date(2010, 6, 6): 5.0},
                'end': utc(2010, 6, 6, 0, 15),
                'end_billing': utc(2010, 6, 6, 0, 30),
                'hour_to_nih_bbnu': {utc(2010, 6, 6, 0): 2.5},
                'hour_to_nih_billed': {utc(2010, 6, 5, 23): 2.5,
                                       utc(2010, 6, 6, 0): 5.0},
                'hour_to_nih_used': {utc(2010, 6, 5, 23): 2.5,
                                     utc(2010, 6, 6, 0): 2.5},
                'label': u'mr_exciting',
                'nih_used': 5.0,
                'nih_bbnu': 2.5,
                'nih_billed': 7.5,
                'owner': u'woo',
                'start': utc(2010, 6, 5, 23, 45),
                'step_num': 1,
            }, {
                # mr whatever, step 1
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {},
                'date_to_nih_billed': {date(2010, 6, 6): 2.5},
                'end': utc(2010, 6, 6, 0, 45),
                'end_billing': utc(2010, 6, 6, 0, 45),
                'hour_to_nih_bbnu': {},
                'hour_to_nih_billed': {utc(2010, 6, 6, 0): 2.5},
                'hour_to_nih_used': {utc(2010, 6, 6, 0): 2.5},
                'label': u'mr_whatever',
                'nih_used': 2.5,
                'nih_bbnu': 0.0,
                'nih_billed': 2.5,
                'owner': u'meh',
                'start': utc(2010, 6, 6, 0, 30),
                'step_num': 1,
            }, {
                # mr whatever, step 2 (and idle time after)
                'date_to_nih_used': {date(2010, 6, 6): 2.5},
                'date_to_nih_bbnu': {date(2010, 6, 6): 2.5},
                'date_to_nih_billed': {date(2010, 6, 6): 5.0},
                'end': utc(2010, 6, 6, 1, 0),
                'end_billing': utc(2010, 6, 6, 1, 15),
                'hour_to_nih_bbnu': {utc(2010, 6, 6, 1): 2.5},
                'hour_to_nih_billed': {utc(2010, 6, 6, 0): 2.5,
                                       utc(2010, 6, 6, 1): 2.5},
                'hour_to_nih_used': {utc(2010, 6, 6, 0): 2.5},
                'label': u'mr_whatever',
                'nih_used': 2.5,
                'nih_bbnu': 2.5,
                'nih_billed': 5.0,
                'owner': u'meh',
                'start': utc(2010, 6, 6, 0, 45),
                'step_num': 2,
            }],
        })


class SubdivideIntervalByDateTestCase(TestCase):

    def test_zero_interval(self):
        self.assertEqual(
            _subdivide_interval_by_date(
                utc(2010, 6, 6, 4, 26),
                utc(2010, 6, 6, 4, 26),
            ),
            {}
        )

    def test_same_day(self):
        self.assertEqual(
            _subdivide_interval_by_date(
                utc(2010, 6, 6, 4, 0),
                utc(2010, 6, 6, 6, 0),
            ),
            {date(2010, 6, 6): 7200.0}
        )

    def test_start_at_midnight(self):
        self.assertEqual(
            _subdivide_interval_by_date(
                utc(2010, 6, 6, 0, 0),
                utc(2010, 6, 6, 5, 0),
            ),
            {date(2010, 6, 6): 18000.0}
        )

    def test_end_at_midnight(self):
        self.assertEqual(
            _subdivide_interval_by_date(
                utc(2010, 6, 5, 23, 0),
                utc(2010, 6, 6, 0, 0),
            ),
            {date(2010, 6, 5): 3600.0}
        )

    def test_split_over_midnight(self):
        self.assertEqual(
            _subdivide_interval_by_date(
                utc(2010, 6, 5, 23, 0),
                utc(2010, 6, 6, 5, 0),
            ),
            {date(2010, 6, 5): 3600.0,
             date(2010, 6, 6): 18000.0}
        )

    def test_full_days(self):
        self.assertEqual(
            _subdivide_interval_by_date(
                utc(2010, 6, 5, 23, 0),
                utc(2010, 6, 10, 5, 0),
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
                utc(2010, 6, 6, 4, 26),
                utc(2010, 6, 6, 4, 26),
            ),
            {}
        )

    def test_same_hour(self):
        self.assertEqual(
            _subdivide_interval_by_hour(
                utc(2010, 6, 6, 4, 24),
                utc(2010, 6, 6, 4, 26),
            ),
            {utc(2010, 6, 6, 4): 120.0}
        )

    def test_start_at_midnight(self):
        self.assertEqual(
            _subdivide_interval_by_hour(
                utc(2010, 6, 6, 0, 0),
                utc(2010, 6, 6, 0, 3),
            ),
            {utc(2010, 6, 6, 0): 180.0}
        )

    def test_end_at_midnight(self):
        self.assertEqual(
            _subdivide_interval_by_hour(
                utc(2010, 6, 5, 23, 55),
                utc(2010, 6, 6, 0, 0),
            ),
            {utc(2010, 6, 5, 23): 300.0}
        )

    def test_split_over_midnight(self):
        self.assertEqual(
            _subdivide_interval_by_hour(
                utc(2010, 6, 5, 23, 55),
                utc(2010, 6, 6, 0, 3),
            ),
            {utc(2010, 6, 5, 23): 300.0,
             utc(2010, 6, 6, 0): 180.0}
        )

    def test_full_hours(self):
        self.assertEqual(
            _subdivide_interval_by_hour(
                utc(2010, 6, 5, 23, 40),
                utc(2010, 6, 6, 2, 10),
            ),
            {utc(2010, 6, 5, 23): 1200.0,
             utc(2010, 6, 6, 0): 3600.0,
             utc(2010, 6, 6, 1): 3600.0,
             utc(2010, 6, 6, 2): 600.0}
        )


class PercentTestCase(TestCase):

    def test_basic(self):
        self.assertEqual(62.5, _percent(5, 8))

    def test_default(self):
        self.assertEqual(0.0, _percent(1, 0))
        self.assertEqual(0.0, _percent(0, 0))
        self.assertEqual(None, _percent(0, 0, default=None))
