# Copyright 2009-2011 Yelp
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

"""Test the idle job flow terminator"""

from __future__ import with_statement

from StringIO import StringIO
from datetime import datetime, timedelta
import sys
from testify import TestCase, assert_equal, assert_raises, setup, teardown

from mrjob.tools.emr.terminate_idle_job_flows import *
from tests.emr_test import MockEMRAndS3TestCase
from tests.mockboto import MockEmrObject, to_iso8601


class JobFlowInspectionTestCase(MockEMRAndS3TestCase):

    @setup
    def create_fake_job_flows(self):
        self.now = datetime.utcnow().replace(microsecond=0)

        # empty job
        self.mock_emr_job_flows['j-EMPTY'] = MockEmrObject(
            state='WAITING',
            creationdatetime=to_iso8601(self.now - timedelta(hours=10)),
            steps=[],
        )

        # currently running job
        self.mock_emr_job_flows['j-CURRENTLY_RUNNING'] = MockEmrObject(
            state='RUNNING',
            creationdatetime=to_iso8601(self.now - timedelta(hours=6)),
            startdatetime=to_iso8601(self.now - timedelta(hours=5)),
            steps=[MockEmrObject(
                startdatetime=to_iso8601(self.now - timedelta(hours=4)),
                jar='/home/hadoop/contrib/streaming/hadoop-0.18-streaming.jar',
                state='RUNNING',
            )],
        )

        # finished job flow
        self.mock_emr_job_flows['j-DONE'] = MockEmrObject(
            state='COMPLETE',
            creationdatetime=to_iso8601(self.now - timedelta(hours=10)),
            startdatetime=to_iso8601(self.now - timedelta(hours=9)),
            enddatetime=to_iso8601(self.now - timedelta(hours=5)),
            steps=[MockEmrObject(
                startdatetime=to_iso8601(self.now - timedelta(hours=8)),
                enddatetime=to_iso8601(self.now - timedelta(hours=6)),
                jar='/home/hadoop/contrib/streaming/hadoop-0.18-streaming.jar',
                state='COMPLETE',
            )],
        )

        # idle job flow
        self.mock_emr_job_flows['j-DONE_AND_IDLE'] = MockEmrObject(
            state='WAITING',
            creationdatetime=to_iso8601(self.now - timedelta(hours=6)),
            startdatetime=to_iso8601(self.now - timedelta(hours=5)),
            steps=[MockEmrObject(
                startdatetime=to_iso8601(self.now - timedelta(hours=4)),
                enddatetime=to_iso8601(self.now - timedelta(hours=2)),
                jar='/home/hadoop/contrib/streaming/hadoop-0.18-streaming.jar',
                state='COMPLETE',
            )],
        )

        # hive job flow (looks completed but isn't)
        self.mock_emr_job_flows['j-HIVE'] = MockEmrObject(
            state='WAITING',
            creationdatetime=to_iso8601(self.now - timedelta(hours=6)),
            startdatetime=to_iso8601(self.now - timedelta(hours=5)),
            steps=[MockEmrObject(
                startdatetime=to_iso8601(self.now - timedelta(hours=4)),
                enddatetime=to_iso8601(self.now - timedelta(hours=4)),
                jar='s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar',
                state='COMPLETE',
            )],
        )

        # hadoop debugging + actual job
        # hadoop debugging looks the same to us as Hive (they use the same
        # jar). The difference is that there's also a streaming step.
        self.mock_emr_job_flows['j-HADOOP_DEBUGGING'] = MockEmrObject(
            state='WAITING',
            creationdatetime=to_iso8601(self.now - timedelta(hours=6)),
            startdatetime=to_iso8601(self.now - timedelta(hours=5)),
            steps=[
                MockEmrObject(
                    startdatetime=to_iso8601(self.now - timedelta(hours=5)),
                    enddatetime=to_iso8601(self.now - timedelta(hours=5)),
                    jar='s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar',
                    state='COMPLETE',
                ),
                MockEmrObject(
                    startdatetime=to_iso8601(self.now - timedelta(hours=4)),
                    enddatetime=to_iso8601(self.now - timedelta(hours=2)),
                    jar='/home/hadoop/contrib/streaming/hadoop-0.18-streaming.jar',
                    state='COMPLETE',
                )
            ],
        )

        # skip cancelled steps
        self.mock_emr_job_flows['j-IDLE_AND_FAILED'] = MockEmrObject(
            state='WAITING',
            creationdatetime=to_iso8601(self.now - timedelta(hours=6)),
            startdatetime=to_iso8601(self.now - timedelta(hours=5)),
            steps=[

                MockEmrObject(
                    startdatetime=to_iso8601(self.now - timedelta(hours=4)),
                    enddatetime=to_iso8601(self.now - timedelta(hours=3)),
                    jar='/home/hadoop/contrib/streaming/hadoop-0.18-streaming.jar',
                    state='FAILED',
                ),
                MockEmrObject(
                    jar='/home/hadoop/contrib/streaming/hadoop-0.18-streaming.jar',
                    state='CANCELLED',
                )
            ],
        )

        # add job flow IDs and fake names to the mock job flows
        for jfid, jf in self.mock_emr_job_flows.iteritems():
            jf.jobflowid = jfid
            jf.name = jfid[2:].replace('_', ' ').title() + ' Job Flow'

    def test_empty(self):
        jf = self.mock_emr_job_flows['j-EMPTY']

        assert_equal(is_job_flow_done(jf), False)
        assert_equal(is_job_flow_running(jf), False)
        assert_equal(is_job_flow_non_streaming(jf), False)
        assert_equal(time_job_flow_idle(jf, self.now), timedelta(hours=10))

    def test_currently_running(self):
        now = datetime.utcnow().replace(microsecond=0)

        jf = self.mock_emr_job_flows['j-CURRENTLY_RUNNING']
        assert_equal(is_job_flow_done(jf), False)
        assert_equal(is_job_flow_running(jf), True)
        assert_equal(is_job_flow_non_streaming(jf), False)
        assert_equal(time_job_flow_idle(jf, self.now), timedelta(0))

    def test_done_and_idle(self):
        jf = self.mock_emr_job_flows['j-DONE_AND_IDLE']

        assert_equal(is_job_flow_done(jf), False)
        assert_equal(is_job_flow_running(jf), False)
        assert_equal(is_job_flow_non_streaming(jf), False)
        assert_equal(time_job_flow_idle(jf, self.now), timedelta(hours=2))

    def test_done(self):
        jf = self.mock_emr_job_flows['j-DONE']

        assert_equal(is_job_flow_done(jf), True)
        assert_equal(is_job_flow_running(jf), False)
        assert_equal(is_job_flow_non_streaming(jf), False)
        assert_equal(time_job_flow_idle(jf, self.now), timedelta(0))

    def test_hive_job_flow(self):
        jf = self.mock_emr_job_flows['j-HIVE']

        assert_equal(is_job_flow_done(jf), False)
        assert_equal(is_job_flow_running(jf), False)
        assert_equal(is_job_flow_non_streaming(jf), True)
        assert_equal(time_job_flow_idle(jf, self.now), timedelta(hours=4))

    def test_hadoop_debugging_job_flow(self):
        jf = self.mock_emr_job_flows['j-HADOOP_DEBUGGING']

        assert_equal(is_job_flow_done(jf), False)
        assert_equal(is_job_flow_running(jf), False)
        assert_equal(is_job_flow_non_streaming(jf), False)
        assert_equal(time_job_flow_idle(jf, self.now), timedelta(hours=2))

    def test_idle_and_failed(self):
        jf = self.mock_emr_job_flows['j-IDLE_AND_FAILED']

        assert_equal(is_job_flow_done(jf), False)
        assert_equal(is_job_flow_running(jf), False)
        assert_equal(is_job_flow_non_streaming(jf), False)
        assert_equal(time_job_flow_idle(jf, self.now), timedelta(hours=3))

    def test_inspect_and_maybe_terminate_job_flows(self):

        def terminated_jfs():
            return sorted(jf.jobflowid
                          for jf in self.mock_emr_job_flows.itervalues()
                          if jf.state in ('SHUTTING_DOWN', 'TERMINATED'))

        def inspect_and_maybe_terminate_quietly(*args, **kwargs):
            # don't print anything out
            real_stdout = sys.stdout
            sys.stdout = StringIO()
            try:
                return inspect_and_maybe_terminate_job_flows(*args, **kwargs)
            finally:
                sys.stdout = real_stdout

        assert_equal(terminated_jfs(), [])

        # dry run shouldn't do anything
        inspect_and_maybe_terminate_quietly(
            conf_path=False, max_hours_idle=0.01,
            now=self.now, dry_run=True)

        assert_equal(terminated_jfs(), [])

        # no job flows are 20 hours old
        inspect_and_maybe_terminate_quietly(
            conf_path=False, max_hours_idle=20,
            now=self.now, dry_run=False)

        # terminate 5-hour-old jobs
        inspect_and_maybe_terminate_quietly(
            conf_path=False, max_hours_idle=5,
            now=self.now, dry_run=False)

        # j-HIVE is old enough to terminate, but it doesn't have streaming
        # steps, so we leave it alone
        assert_equal(terminated_jfs(), ['j-EMPTY'])

        # terminate 2-hour-old jobs
        inspect_and_maybe_terminate_quietly(
            conf_path=False, max_hours_idle=2,
            now=self.now, dry_run=False)

        # picky edge case: two jobs are EXACTLY 2 hours old, so they're
        # not over the maximum

        assert_equal(terminated_jfs(), ['j-EMPTY', 'j-IDLE_AND_FAILED'])

        # all the job flows we can terminate are at least 1 hour old
        inspect_and_maybe_terminate_quietly(
            conf_path=False, max_hours_idle=1,
            now=self.now, dry_run=False)

        assert_equal(terminated_jfs(),
                     ['j-DONE_AND_IDLE', 'j-EMPTY', 'j-HADOOP_DEBUGGING',
                      'j-IDLE_AND_FAILED'])

        # just to prove our point
        inspect_and_maybe_terminate_quietly(
            conf_path=False, max_hours_idle=0,
            now=self.now, dry_run=False)

        assert_equal(terminated_jfs(),
                     ['j-DONE_AND_IDLE', 'j-EMPTY', 'j-HADOOP_DEBUGGING',
                      'j-IDLE_AND_FAILED'])


