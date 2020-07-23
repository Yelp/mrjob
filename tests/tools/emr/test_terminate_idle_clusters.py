# Copyright 2009-2012 Yelp
# Copyright 2013 Lyft
# Copyright 2014 Marc Abramowitz
# Copyright 2015-2019 Yelp
# Copyright 2020 Affirm, Inc.
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
"""Test the idle cluster terminator"""
import sys
from datetime import timedelta

from mrjob.aws import _boto3_now
from mrjob.pool import _pool_name
from mrjob.py2 import StringIO
from mrjob.tools.emr.terminate_idle_clusters import _maybe_terminate_clusters
from mrjob.tools.emr.terminate_idle_clusters import _is_cluster_bootstrapping
from mrjob.tools.emr.terminate_idle_clusters import _is_cluster_done
from mrjob.tools.emr.terminate_idle_clusters import _is_cluster_running
from mrjob.tools.emr.terminate_idle_clusters import _is_cluster_starting
from mrjob.tools.emr.terminate_idle_clusters import _cluster_has_pending_steps
from mrjob.tools.emr.terminate_idle_clusters import _time_last_active

from tests.mock_boto3 import MockBoto3TestCase
from tests.py2 import patch
from tests.sandbox import SandboxedTestCase


class ClusterTerminationTestCase(MockBoto3TestCase):

    maxDiff = None

    _DEFAULT_STEP_ARGS = ['-mapper', 'my_job.py --mapper',
                          '-reducer', 'my_job.py --reducer']

    def setUp(self):
        super(ClusterTerminationTestCase, self).setUp()

        # don't delete locks, so we can test they were created
        self.start(patch('mrjob.fs.s3.S3Filesystem.rm'))

        self.create_fake_clusters()

    def create_fake_clusters(self):
        self.now = _boto3_now().replace(microsecond=0)
        self.add_mock_s3_data({'my_bucket': {}})

        # create a timestamp the given number of *hours*, *minutes*, etc.
        # in the past
        def ago(**kwargs):
            return self.now - timedelta(**kwargs)

        # Build a step object easily
        # also make it respond to .args()
        def step(jar='/home/hadoop/contrib/streaming/hadoop-streaming.jar',
                 args=self._DEFAULT_STEP_ARGS,
                 state='COMPLETED',
                 created=None,
                 started=None,
                 ended=None,
                 name='Streaming Step',
                 action_on_failure='TERMINATE_CLUSTER',
                 **kwargs):

            timeline = dict()
            if created:
                timeline['CreationDateTime'] = created
            if started:
                timeline['StartDateTime'] = started
            if ended:
                timeline['EndDateTime'] = ended

            return dict(
                Config=dict(
                    ActionOnFailure=action_on_failure,
                    Args=args,
                    Jar=jar,
                ),
                Status=dict(
                    State=state,
                    Timeline=timeline,
                )
            )

        # empty job
        self.add_mock_emr_cluster(
            dict(
                Id='j-EMPTY',
                TerminationProtected=False,
                Status=dict(
                    State='STARTING',
                    Timeline=dict(
                        CreationDateTime=ago(hours=10)
                    ),
                ),
            )
        )

        # job that's bootstrapping
        self.add_mock_emr_cluster(dict(
            Id='j-BOOTSTRAPPING',
            TerminationProtected=False,
            Status=dict(
                State='BOOTSTRAPPING',
                Timeline=dict(
                    CreationDateTime=ago(hours=10),
                ),
            ),
            _Steps=[step(created=ago(hours=10), state='PENDING')],
        ))

        # currently running job
        self.add_mock_emr_cluster(
            dict(
                Id='j-CURRENTLY_RUNNING',
                TerminationProtected=False,
                Status=dict(
                    State='RUNNING',
                    Timeline=dict(
                        CreationDateTime=ago(hours=4, minutes=15),
                        ReadyDateTime=ago(hours=4, minutes=10)
                    )
                ),
                _Steps=[step(started=ago(hours=4), state='RUNNING')]
            )
        )

        # finished cluster
        self.add_mock_emr_cluster(dict(
            Id='j-DONE',
            TerminationProtected=False,
            Status=dict(
                State='TERMINATED',
                Timeline=dict(
                    CreationDateTime=ago(hours=10),
                    ReadyDateTime=ago(hours=8),
                    EndDateTime=ago(hours=5),
                ),
            ),
            _Steps=[step(started=ago(hours=8), ended=ago(hours=6))],
        ))

        # idle cluster
        self.add_mock_emr_cluster(dict(
            Id='j-DONE_AND_IDLE',
            TerminationProtected=False,
            Status=dict(
                State='WAITING',
                Timeline=dict(
                    CreationDateTime=ago(hours=6),
                    ReadyDateTime=ago(hours=5, minutes=5),
                ),
            ),
            _Steps=[step(started=ago(hours=4), ended=ago(hours=2))],
        ))

        # idle cluster with 4.x step format. should still be
        # recognizable as a streaming step
        self.add_mock_emr_cluster(dict(
            Id='j-DONE_AND_IDLE_4_X',
            TerminationProtected=False,
            Status=dict(
                State='WAITING',
                Timeline=dict(
                    CreationDateTime=ago(hours=6),
                    ReadyDateTime=ago(hours=5, minutes=5),
                ),
            ),
            _Steps=[step(started=ago(hours=4), ended=ago(hours=2),
                         jar='command-runner.jar',
                         args=['hadoop-streaming'] + self._DEFAULT_STEP_ARGS)],
        ))

        # idle cluster with an expired lock
        self.add_mock_emr_cluster(dict(
            Id='j-IDLE_BUT_INCOMPLETE_STEPS',
            TerminationProtected=False,
            Status=dict(
                State='WAITING',
                Timeline=dict(
                    CreationDateTime=ago(hours=6),
                    ReadyDateTime=ago(hours=5, minutes=5),
                ),
            ),
            _Steps=[step(started=ago(hours=4), end_hours_ago=None)],
        ))

        # custom hadoop streaming jar
        self.add_mock_emr_cluster(dict(
            Id='j-CUSTOM_DONE_AND_IDLE',
            TerminationProtected=False,
            Status=dict(
                State='WAITING',
                Timeline=dict(
                    CreationDateTime=ago(hours=6),
                    ReadyDateTime=ago(hours=5, minutes=5),
                ),
            ),
            _Steps=[step(
                started=ago(hours=4),
                ended=ago(hours=4),
                jar=('s3://my_bucket/tmp/somejob/files/'
                     'oddjob-0.0.3-SNAPSHOT-standalone.jar'),
                args=[],
            )],
        ))

        # idle cluster, termination protected
        self.add_mock_emr_cluster(dict(
            Id='j-IDLE_AND_PROTECTED',
            TerminationProtected=True,
            Status=dict(
                State='WAITING',
                Timeline=dict(
                    CreationDateTime=ago(hours=6),
                    ReadyDateTime=ago(hours=5, minutes=5),
                ),
            ),
            _Steps=[step(started=ago(hours=4), ended=ago(hours=2))],
        ))

        # hadoop debugging without any other steps
        self.add_mock_emr_cluster(dict(
            Id='j-DEBUG_ONLY',
            TerminationProtected=False,
            Status=dict(
                State='WAITING',
                Timeline=dict(
                    CreationDateTime=ago(hours=3),
                    ReadyDateTime=ago(hours=2, minutes=55),
                ),
            ),
            _Steps=[
                step(jar='command-runner.jar',
                     name='Setup Hadoop Debugging',
                     args=['state-pusher-script'],
                     started=ago(hours=3),
                     ended=ago(hours=2))
            ],
        ))

        # hadoop debugging + actual job
        self.add_mock_emr_cluster(dict(
            Id='j-HADOOP_DEBUGGING',
            TerminationProtected=False,
            Status=dict(
                State='WAITING',
                Timeline=dict(
                    CreationDateTime=ago(hours=6),
                    ReadyDateTime=ago(hours=5, minutes=55),
                ),
            ),
            _Steps=[
                step(jar='command-runner.jar',
                     name='Setup Hadoop Debugging',
                     args=['state-pusher-script'],
                     started=ago(hours=5),
                     ended=ago(hours=4)),
                step(started=ago(hours=4), ended=ago(hours=2)),
            ],
        ))

        # should skip cancelled steps
        self.add_mock_emr_cluster(dict(
            Id='j-IDLE_AND_FAILED',
            TerminationProtected=False,
            Status=dict(
                State='WAITING',
                Timeline=dict(
                    CreationDateTime=ago(hours=6),
                    ReadyDateTime=ago(hours=5, minutes=5),
                ),
            ),
            _Steps=[
                step(started=ago(hours=4), ended=ago(hours=3), state='FAILED'),
                step(state='CANCELLED'),
            ],
        ))

        # pooled cluster reaching end of full hour
        self.add_mock_emr_cluster(dict(
            _BootstrapActions=[
                dict(Args=[], Name='action 0'),
                dict(
                    Args=['pool-0123456789abcdef0123456789abcdef',
                          'reflecting'],
                    Name='main',
                ),
            ],
            Id='j-POOLED',
            TerminationProtected=False,
            Status=dict(
                State='WAITING',
                Timeline=dict(
                    CreationDateTime=ago(minutes=55),
                    ReadyDateTime=ago(minutes=50),
                ),
            ),
            Tags=[
                dict(Key='__mrjob_pool_name',
                     Value='reflecting'),
                dict(Key='__mrjob_pool_hash',
                     Value='0123456789abcdef0123456789abcdef'),
            ],
        ))

        # cluster that has had pending jobs but hasn't run them
        self.add_mock_emr_cluster(dict(
            Id='j-PENDING_BUT_IDLE',
            TerminationProtected=False,
            Status=dict(
                State='RUNNING',
                Timeline=dict(
                    CreationDateTime=ago(hours=3),
                    ReadyDateTime=ago(hours=2, minutes=50),
                ),
            ),
            _Steps=[step(created=ago(hours=3), state='PENDING')],
        ))

    def ids_of_terminated_clusters(self):
        return sorted(
            str(cluster_id)
            for cluster_id, cluster in self.mock_emr_clusters.items()
            if cluster_id != 'j-DONE' and
            cluster['Status']['State'] in (
                'TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS'))

    def maybe_terminate_quietly(self, stdout=None, **kwargs):
        if 'conf_paths' not in kwargs:
            kwargs['conf_paths'] = []

        if 'now' not in kwargs:
            kwargs['now'] = self.now

        kwargs['cloud_tmp_dir'] = 's3://my_bucket/'
        kwargs['cloud_fs_sync_secs'] = 0
        kwargs['max_mins_locked'] = 1

        # don't print anything out
        real_stdout = sys.stdout
        sys.stdout = stdout or StringIO()
        try:
            return _maybe_terminate_clusters(**kwargs)
        finally:
            sys.stdout = real_stdout

    def time_mock_cluster_idle(self, mock_cluster):
        if (_is_cluster_starting(mock_cluster) or
                _is_cluster_bootstrapping(mock_cluster) or
                _is_cluster_running(mock_cluster['_Steps']) or
                _is_cluster_done(mock_cluster)):
            return timedelta(0)
        else:
            return self.now - _time_last_active(
                mock_cluster, mock_cluster['_Steps'])

    def assert_mock_cluster_is(
            self, mock_cluster,
            starting=False,
            bootstrapping=False,
            done=False,
            has_pending_steps=False,
            idle_for=timedelta(0),
            pool_name=None,
            running=False):

        self.assertEqual(starting,
                         _is_cluster_starting(mock_cluster))
        self.assertEqual(bootstrapping,
                         _is_cluster_bootstrapping(mock_cluster))
        self.assertEqual(done,
                         _is_cluster_done(mock_cluster))
        self.assertEqual(has_pending_steps,
                         _cluster_has_pending_steps(mock_cluster['_Steps']))
        self.assertEqual(idle_for,
                         self.time_mock_cluster_idle(mock_cluster))
        self.assertEqual(pool_name,
                         _pool_name(mock_cluster))
        self.assertEqual(running,
                         _is_cluster_running(mock_cluster['_Steps']))

    def test_empty(self):
        self.assert_mock_cluster_is(
            self.mock_emr_clusters['j-EMPTY'],
            starting=True,
        )

    def test_currently_running(self):
        self.assert_mock_cluster_is(
            self.mock_emr_clusters['j-CURRENTLY_RUNNING'],
            running=True,
        )

    def test_done(self):
        self.assert_mock_cluster_is(
            self.mock_emr_clusters['j-DONE'],
            done=True,
        )

    def test_debug_only(self):
        self.assert_mock_cluster_is(
            self.mock_emr_clusters['j-DEBUG_ONLY'],
            idle_for=timedelta(hours=2),
        )

    def test_done_and_idle(self):
        self.assert_mock_cluster_is(
            self.mock_emr_clusters['j-DONE_AND_IDLE'],
            idle_for=timedelta(hours=2),
        )

    def test_done_and_idle_4_x(self):
        self.assert_mock_cluster_is(
            self.mock_emr_clusters['j-DONE_AND_IDLE_4_X'],
            idle_for=timedelta(hours=2),
        )

    def test_hadoop_debugging_cluster(self):
        self.assert_mock_cluster_is(
            self.mock_emr_clusters['j-HADOOP_DEBUGGING'],
            idle_for=timedelta(hours=2),
        )

    def test_idle_and_failed(self):
        self.assert_mock_cluster_is(
            self.mock_emr_clusters['j-IDLE_AND_FAILED'],
            idle_for=timedelta(hours=3),
        )

    def test_pooled(self):
        self.assert_mock_cluster_is(
            self.mock_emr_clusters['j-POOLED'],
            idle_for=timedelta(minutes=50),
            pool_name='reflecting',
        )

    def test_pending_but_idle(self):
        self.assert_mock_cluster_is(
            self.mock_emr_clusters['j-PENDING_BUT_IDLE'],
            has_pending_steps=True,
            idle_for=timedelta(hours=2, minutes=50),
        )

    def test_dry_run_does_nothing(self):
        self.maybe_terminate_quietly(max_mins_idle=0.6, dry_run=True)

        self.assertEqual(self.ids_of_terminated_clusters(), [])

    def test_increasing_idle_time(self):
        self.assertEqual(self.ids_of_terminated_clusters(), [])

        # no clusters are 20 hours old
        self.maybe_terminate_quietly(
            conf_paths=[], max_mins_idle=1200, now=self.now)

        self.assertEqual(self.ids_of_terminated_clusters(), [])

        # terminate 5-hour-old jobs
        self.maybe_terminate_quietly(
            conf_paths=[], max_mins_idle=300, now=self.now)

        # terminate 2-hour-old jobs
        self.maybe_terminate_quietly(
            conf_paths=[], max_mins_idle=120, now=self.now)

        # picky edge case: two jobs are EXACTLY 2 hours old, so they're
        # not over the maximum

        self.assertEqual(self.ids_of_terminated_clusters(),
                         ['j-CUSTOM_DONE_AND_IDLE',
                          'j-IDLE_AND_FAILED',
                          'j-PENDING_BUT_IDLE'])

        self.maybe_terminate_quietly(max_mins_idle=60)

        self.assertEqual(self.ids_of_terminated_clusters(),
                         ['j-CUSTOM_DONE_AND_IDLE',
                          'j-DEBUG_ONLY',
                          'j-DONE_AND_IDLE', 'j-DONE_AND_IDLE_4_X',
                          'j-HADOOP_DEBUGGING',
                          'j-IDLE_AND_FAILED', 'j-PENDING_BUT_IDLE'])

    def test_one_hour_is_the_default(self):
        self.assertEqual(self.ids_of_terminated_clusters(), [])

        self.maybe_terminate_quietly()

        self.assertEqual(self.ids_of_terminated_clusters(),
                         ['j-CUSTOM_DONE_AND_IDLE',
                          'j-DEBUG_ONLY',
                          'j-DONE_AND_IDLE', 'j-DONE_AND_IDLE_4_X',
                          'j-HADOOP_DEBUGGING',
                          'j-IDLE_AND_FAILED', 'j-PENDING_BUT_IDLE'])

    def test_zero_idle_time(self):
        self.assertEqual(self.ids_of_terminated_clusters(), [])

        self.maybe_terminate_quietly(max_mins_idle=0)

        self.assertEqual(self.ids_of_terminated_clusters(),
                         ['j-CUSTOM_DONE_AND_IDLE',
                          'j-DEBUG_ONLY',
                          'j-DONE_AND_IDLE', 'j-DONE_AND_IDLE_4_X',
                          'j-HADOOP_DEBUGGING',
                          'j-IDLE_AND_FAILED', 'j-PENDING_BUT_IDLE',
                          'j-POOLED'])

    def test_terminate_pooled_only(self):
        self.assertEqual(self.ids_of_terminated_clusters(), [])

        self.maybe_terminate_quietly(pooled_only=True)

        # pooled job was not idle for an hour (the default)
        self.assertEqual(self.ids_of_terminated_clusters(), [])

        self.maybe_terminate_quietly(pooled_only=True, max_mins_idle=0.6)

        self.assertEqual(self.ids_of_terminated_clusters(), ['j-POOLED'])

    def test_terminate_unpooled_only(self):
        self.assertEqual(self.ids_of_terminated_clusters(), [])

        self.maybe_terminate_quietly(unpooled_only=True)

        self.assertEqual(self.ids_of_terminated_clusters(),
                         ['j-CUSTOM_DONE_AND_IDLE',
                          'j-DEBUG_ONLY',
                          'j-DONE_AND_IDLE', 'j-DONE_AND_IDLE_4_X',
                          'j-HADOOP_DEBUGGING',
                          'j-IDLE_AND_FAILED', 'j-PENDING_BUT_IDLE'])

        self.maybe_terminate_quietly(unpooled_only=True, max_mins_idle=0.6)

        self.assertEqual(self.ids_of_terminated_clusters(),
                         ['j-CUSTOM_DONE_AND_IDLE',
                          'j-DEBUG_ONLY',
                          'j-DONE_AND_IDLE', 'j-DONE_AND_IDLE_4_X',
                          'j-HADOOP_DEBUGGING',
                          'j-IDLE_AND_FAILED', 'j-PENDING_BUT_IDLE'])

    def test_terminate_by_pool_name(self):
        self.assertEqual(self.ids_of_terminated_clusters(), [])

        # wrong pool name
        self.maybe_terminate_quietly(pool_name='default', max_mins_idle=0.6)

        self.assertEqual(self.ids_of_terminated_clusters(), [])

        # right pool name
        self.maybe_terminate_quietly(pool_name='reflecting', max_mins_idle=0.6)

        self.assertEqual(self.ids_of_terminated_clusters(), ['j-POOLED'])

    def test_its_quiet_too_quiet(self):
        stdout = StringIO()
        self.maybe_terminate_quietly(
            stdout=stdout, max_mins_idle=0.6, quiet=True)
        self.assertEqual(stdout.getvalue(), '')

    EXPECTED_STDOUT_LINES = [
        'Terminated cluster j-POOLED (POOLED);'
        ' was idle for 0:50:00',
        'Terminated cluster j-PENDING_BUT_IDLE (PENDING_BUT_IDLE);'
        ' was pending for 2:50:00',
        'Terminated cluster j-DEBUG_ONLY (DEBUG_ONLY);'
        ' was idle for 2:00:00',
        'Terminated cluster j-DONE_AND_IDLE (DONE_AND_IDLE);'
        ' was idle for 2:00:00',
        'Terminated cluster j-DONE_AND_IDLE_4_X (DONE_AND_IDLE_4_X);'
        ' was idle for 2:00:00',
        'Terminated cluster j-IDLE_AND_FAILED (IDLE_AND_FAILED);'
        ' was idle for 3:00:00',
        'Terminated cluster j-HADOOP_DEBUGGING (HADOOP_DEBUGGING);'
        ' was idle for 2:00:00',
        'Terminated cluster j-CUSTOM_DONE_AND_IDLE (CUSTOM_DONE_AND_IDLE);'
        ' was idle for 4:00:00',
    ]

    def test_its_not_very_quiet(self):
        stdout = StringIO()
        self.maybe_terminate_quietly(stdout=stdout, max_mins_idle=0.6)

        self.assertEqual(set(stdout.getvalue().splitlines()),
                         set(self.EXPECTED_STDOUT_LINES))

        # should have actually terminated clusters
        self.assertEqual(self.ids_of_terminated_clusters(), [
            'j-CUSTOM_DONE_AND_IDLE',
            'j-DEBUG_ONLY',
            'j-DONE_AND_IDLE',
            'j-DONE_AND_IDLE_4_X',
            'j-HADOOP_DEBUGGING',
            'j-IDLE_AND_FAILED',
            'j-PENDING_BUT_IDLE',
            'j-POOLED',
        ])

    def test_dry_run(self):
        stdout = StringIO()
        self.maybe_terminate_quietly(
            stdout=stdout, max_mins_idle=0.6, dry_run=True)

        # shouldn't *actually* terminate clusters
        self.assertEqual(self.ids_of_terminated_clusters(), [])


class DeprecatedSwitchesTestCase(SandboxedTestCase):

    def setUp(self):
        super(DeprecatedSwitchesTestCase, self).setUp()

        self._maybe_terminate_clusters = self.start(patch(
            'mrjob.tools.emr.terminate_idle_clusters.'
            '_maybe_terminate_clusters'))

        self.log = self.start(
            patch('mrjob.tools.emr.terminate_idle_clusters.log'))
