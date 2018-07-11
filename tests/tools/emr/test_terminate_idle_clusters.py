# Copyright 2009-2012 Yelp
# Copyright 2013 Lyft
# Copyright 2014 Marc Abramowitz
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
"""Test the idle cluster terminator"""
import sys
from datetime import datetime
from datetime import timedelta

from mrjob.pool import _est_time_to_hour
from mrjob.pool import _pool_hash_and_name
from mrjob.py2 import StringIO
from mrjob.tools.emr.terminate_idle_clusters import _maybe_terminate_clusters
from mrjob.tools.emr.terminate_idle_clusters import _is_cluster_bootstrapping
from mrjob.tools.emr.terminate_idle_clusters import _is_cluster_done
from mrjob.tools.emr.terminate_idle_clusters import _is_cluster_running
from mrjob.tools.emr.terminate_idle_clusters import _is_cluster_starting
from mrjob.tools.emr.terminate_idle_clusters import _cluster_has_pending_steps
from mrjob.tools.emr.terminate_idle_clusters import _time_last_active

from tests.mockboto import MockBotoTestCase
from tests.mockboto import MockEmrObject
from tests.mockboto import to_iso8601


class ClusterTerminationTestCase(MockBotoTestCase):

    maxDiff = None

    _DEFAULT_STEP_ARGS = ['-mapper', 'my_job.py --mapper',
                          '-reducer', 'my_job.py --reducer']

    def setUp(self):
        super(ClusterTerminationTestCase, self).setUp()
        self.create_fake_clusters()

    def create_fake_clusters(self):
        self.now = datetime.utcnow().replace(microsecond=0)
        self.add_mock_s3_data({'my_bucket': {}})

        # create a timestamp the given number of *hours*, *minutes*, etc.
        # in the past. If any *kwargs* are None, return None.
        def ago(**kwargs):
            if any(v is None for v in kwargs.values()):
                return None
            return to_iso8601(self.now - timedelta(**kwargs))

        # Build a step object easily
        # also make it respond to .args()
        def step(jar='/home/hadoop/contrib/streaming/hadoop-streaming.jar',
                 args=self._DEFAULT_STEP_ARGS,
                 state='COMPLETED',
                 create_hours_ago=None,
                 start_hours_ago=None,
                 end_hours_ago=None,
                 name='Streaming Step',
                 action_on_failure='TERMINATE_CLUSTER',
                 **kwargs):

            return MockEmrObject(
                config=MockEmrObject(
                    actiononfailure=action_on_failure,
                    args=[MockEmrObject(value=a) for a in args],
                    jar=jar,
                ),
                status=MockEmrObject(
                    state=state,
                    timeline=MockEmrObject(
                        creationdatetime=ago(hours=create_hours_ago),
                        enddatetime=ago(hours=end_hours_ago),
                        startdatetime=ago(hours=start_hours_ago),
                    ),
                )
            )

        # empty job
        self.add_mock_emr_cluster(
            MockEmrObject(
                id='j-EMPTY',
                status=MockEmrObject(
                    state='STARTING',
                    timeline=MockEmrObject(
                        creationdatetime=ago(hours=10)
                    ),
                ),
            )
        )

        # job that's bootstrapping
        self.add_mock_emr_cluster(MockEmrObject(
            id='j-BOOTSTRAPPING',
            status=MockEmrObject(
                state='BOOTSTRAPPING',
                timeline=MockEmrObject(
                    creationdatetime=ago(hours=10),
                ),
            ),
            _steps=[step(create_hours_ago=10, state='PENDING')],
        ))

        # currently running job
        self.add_mock_emr_cluster(
            MockEmrObject(
                id='j-CURRENTLY_RUNNING',
                status=MockEmrObject(
                    state='RUNNING',
                    timeline=MockEmrObject(
                        creationdatetime=ago(hours=4, minutes=15),
                        readydatetime=ago(hours=4, minutes=10)
                    )
                ),
                _steps=[step(start_hours_ago=4, state='RUNNING')]
            )
        )

        # finished cluster
        self.add_mock_emr_cluster(MockEmrObject(
            id='j-DONE',
            status=MockEmrObject(
                state='TERMINATED',
                timeline=MockEmrObject(
                    creationdatetime=ago(hours=10),
                    readydatetime=ago(hours=8),
                    enddatetime=ago(hours=5),
                ),
            ),
            _steps=[step(start_hours_ago=8, end_hours_ago=6)],
        ))

        # idle cluster
        self.add_mock_emr_cluster(MockEmrObject(
            id='j-DONE_AND_IDLE',
            status=MockEmrObject(
                state='WAITING',
                timeline=MockEmrObject(
                    creationdatetime=ago(hours=6),
                    readydatetime=ago(hours=5, minutes=5),
                ),
            ),
            _steps=[step(start_hours_ago=4, end_hours_ago=2)],
        ))

        # idle cluster with 4.x step format. should still be
        # recognizable as a streaming step
        self.add_mock_emr_cluster(MockEmrObject(
            id='j-DONE_AND_IDLE_4_X',
            status=MockEmrObject(
                state='WAITING',
                timeline=MockEmrObject(
                    creationdatetime=ago(hours=6),
                    readydatetime=ago(hours=5, minutes=5),
                ),
            ),
            _steps=[step(start_hours_ago=4, end_hours_ago=2,
                         jar='command-runner.jar',
                         args=['hadoop-streaming'] + self._DEFAULT_STEP_ARGS)],
        ))

        # idle cluster with an active lock
        self.add_mock_emr_cluster(MockEmrObject(
            id='j-IDLE_AND_LOCKED',
            status=MockEmrObject(
                state='WAITING',
                timeline=MockEmrObject(
                    creationdatetime=ago(hours=6),
                    readydatetime=ago(hours=5, minutes=5),
                ),
            ),
            _steps=[step(start_hours_ago=4, end_hours_ago=2)],
        ))
        self.add_mock_s3_data({
            'my_bucket': {
                'locks/j-IDLE_AND_LOCKED/2': b'not_you',
            },
        }, time_modified=self.now)

        # idle cluster with an expired lock
        self.add_mock_emr_cluster(MockEmrObject(
            id='j-IDLE_AND_EXPIRED',
            status=MockEmrObject(
                state='WAITING',
                timeline=MockEmrObject(
                    creationdatetime=ago(hours=6),
                    readydatetime=ago(hours=5, minutes=5),
                ),
            ),
            _steps=[step(start_hours_ago=4, end_hours_ago=2)],
        ))
        self.add_mock_s3_data({
            'my_bucket': {
                'locks/j-IDLE_AND_EXPIRED/2': b'not_you',
            },
        }, time_modified=(self.now - timedelta(minutes=5)))

        # idle cluster with an expired lock
        self.add_mock_emr_cluster(MockEmrObject(
            id='j-IDLE_BUT_INCOMPLETE_STEPS',
            status=MockEmrObject(
                state='WAITING',
                timeline=MockEmrObject(
                    creationdatetime=ago(hours=6),
                    readydatetime=ago(hours=5, minutes=5),
                ),
            ),
            _steps=[step(start_hours_ago=4, end_hours_ago=None)],
        ))

        # custom hadoop streaming jar
        self.add_mock_emr_cluster(MockEmrObject(
            id='j-CUSTOM_DONE_AND_IDLE',
            status=MockEmrObject(
                state='WAITING',
                timeline=MockEmrObject(
                    creationdatetime=ago(hours=6),
                    readydatetime=ago(hours=5, minutes=5),
                ),
            ),
            _steps=[step(
                start_hours_ago=4,
                end_hours_ago=4,
                jar=('s3://my_bucket/tmp/somejob/files/'
                     'oddjob-0.0.3-SNAPSHOT-standalone.jar'),
                args=[],
            )],
        ))

        # idle, but termination protected. this cluster can never actually be
        # terminated, but the job must handle this case gracefully
        self.add_mock_emr_cluster(MockEmrObject(
            id='j-IDLE_AND_PROTECTED',
            _TerminationProtected=True,
            status=MockEmrObject(
                state='WAITING',
                timeline=MockEmrObject(
                    creationdatetime=ago(hours=6),
                    readydatetime=ago(hours=5, minutes=5),
                ),
            ),
            _steps=[step(start_hours_ago=4, end_hours_ago=2)],
        ))

        mock_emr_conn = self.connect_emr()

        # hadoop debugging without any other steps
        mock_emr_conn.run_jobflow(_id='j-DEBUG_ONLY',
                                  name='DEBUG_ONLY',
                                  enable_debugging=True,
                                  now=self.now - timedelta(hours=3),
                                  job_flow_role='fake-instance-profile',
                                  service_role='fake-service-role')

        j_debug_only = self.mock_emr_clusters['j-DEBUG_ONLY']
        j_debug_only.status.state = 'WAITING'
        j_debug_only.status.timeline.readydatetime = ago(hours=2, minutes=55)
        j_debug_only._steps[0].status.state = 'COMPLETED'
        j_debug_only._steps[0].status.timeline.enddatetime = ago(hours=2)

        # hadoop debugging + actual job
        mock_emr_conn.run_jobflow(_id='j-HADOOP_DEBUGGING',
                                  name='HADOOP_DEBUGGING',
                                  enable_debugging=True,
                                  now=self.now - timedelta(hours=6),
                                  job_flow_role='fake-instance-profile',
                                  service_role='fake-service-role')

        j_hadoop_debugging = self.mock_emr_clusters['j-HADOOP_DEBUGGING']
        j_hadoop_debugging._steps.append(step())
        j_hadoop_debugging.status.state = 'WAITING'
        j_hadoop_debugging.status.timeline.readydatetime = ago(
            hours=4, minutes=55)

        # Need to reset times manually because mockboto resets them
        j_hadoop_debugging._steps[0].status.state = 'COMPLETED'
        j_hadoop_debugging._steps[0].status.timeline.enddatetime = ago(hours=5)
        j_hadoop_debugging._steps[1].status.timeline.startdatetime = ago(
            hours=4)
        j_hadoop_debugging._steps[1].status.timeline.enddatetime = ago(hours=2)

        # should skip cancelled steps
        self.add_mock_emr_cluster(MockEmrObject(
            id='j-IDLE_AND_FAILED',
            status=MockEmrObject(
                state='WAITING',
                timeline=MockEmrObject(
                    creationdatetime=ago(hours=6),
                    readydatetime=ago(hours=5, minutes=5),
                ),
            ),
            _steps=[
                step(start_hours_ago=4, end_hours_ago=3, state='FAILED'),
                step(
                    state='CANCELLED',
                )
            ],
        ))

        # pooled cluster reaching end of full hour
        self.add_mock_emr_cluster(MockEmrObject(
            _bootstrapactions=[
                MockEmrObject(args=[], name='action 0'),
                MockEmrObject(args=[
                    MockEmrObject(
                        value='pool-0123456789abcdef0123456789abcdef'),
                    MockEmrObject(value='reflecting'),
                ], name='master'),
            ],
            id='j-POOLED',
            status=MockEmrObject(
                state='WAITING',
                timeline=MockEmrObject(
                    creationdatetime=ago(minutes=55),
                    readydatetime=ago(minutes=50),
                ),
            ),
        ))

        # cluster that has had pending jobs but hasn't run them
        self.add_mock_emr_cluster(MockEmrObject(
            id='j-PENDING_BUT_IDLE',
            status=MockEmrObject(
                state='RUNNING',
                timeline=MockEmrObject(
                    creationdatetime=ago(hours=3),
                    readydatetime=ago(hours=2, minutes=50),
                ),
            ),
            _steps=[step(create_hours_ago=3, state='PENDING')],
        ))

    def ids_of_terminated_clusters(self):
        return sorted(
            str(cluster_id)
            for cluster_id, cluster in self.mock_emr_clusters.items()
            if cluster_id != 'j-DONE' and
            cluster.status.state in (
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
                _is_cluster_running(mock_cluster._steps) or
                _is_cluster_done(mock_cluster)):
            return timedelta(0)
        else:
            return self.now - _time_last_active(
                mock_cluster, mock_cluster._steps)

    def assert_mock_cluster_is(
            self, mock_cluster,
            starting=False,
            bootstrapping=False,
            done=False,
            from_end_of_hour=timedelta(hours=1),
            has_pending_steps=False,
            idle_for=timedelta(0),
            pool_hash=None,
            pool_name=None,
            running=False):

        self.assertEqual(starting,
                         _is_cluster_starting(mock_cluster))
        self.assertEqual(bootstrapping,
                         _is_cluster_bootstrapping(mock_cluster))
        self.assertEqual(done,
                         _is_cluster_done(mock_cluster))
        self.assertEqual(from_end_of_hour,
                         _est_time_to_hour(mock_cluster, self.now))
        self.assertEqual(has_pending_steps,
                         _cluster_has_pending_steps(mock_cluster._steps))
        self.assertEqual(idle_for,
                         self.time_mock_cluster_idle(mock_cluster))
        self.assertEqual((pool_hash, pool_name),
                         _pool_hash_and_name(mock_cluster._bootstrapactions))
        self.assertEqual(running,
                         _is_cluster_running(mock_cluster._steps))

    def _lock_contents(self, mock_cluster, steps_ahead=0):
        conn = self.connect_s3()
        bucket = conn.get_bucket('my_bucket')
        lock_key_name = 'locks/%s/%d' % (
            mock_cluster.id, len(mock_cluster._steps) + steps_ahead)
        key = bucket.get_key(lock_key_name)
        if key is None:
            return None
        else:
            return key.get_contents_as_string()

    def assert_locked_by_terminate(self, mock_cluster, steps_ahead=1):
        contents = self._lock_contents(mock_cluster, steps_ahead=steps_ahead)
        self.assertIsNotNone(contents)
        self.assertIn(b'terminate', contents)

    def assert_locked_by_something_else(self, mock_cluster, steps_ahead=1):
        contents = self._lock_contents(mock_cluster, steps_ahead=steps_ahead)
        self.assertIsNotNone(contents)
        self.assertNotIn(b'terminate', contents)

    def assert_not_locked(self, mock_cluster, steps_ahead=1):
        self.assertIsNone(
            self._lock_contents(mock_cluster, steps_ahead=steps_ahead))

    def assert_terminated_clusters_locked_by_terminate(self):
        for cluster_id in self.ids_of_terminated_clusters():
            self.assert_locked_by_terminate(self.mock_emr_clusters[cluster_id])

    def test_empty(self):
        self.assert_mock_cluster_is(
            self.mock_emr_clusters['j-EMPTY'],
            starting=True,
        )

    def test_currently_running(self):
        self.assert_mock_cluster_is(
            self.mock_emr_clusters['j-CURRENTLY_RUNNING'],
            from_end_of_hour=timedelta(minutes=45),
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

    def test_idle_and_expired(self):
        self.assert_mock_cluster_is(
            self.mock_emr_clusters['j-IDLE_AND_EXPIRED'],
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
            from_end_of_hour=timedelta(minutes=5),
            idle_for=timedelta(minutes=50),
            pool_hash='0123456789abcdef0123456789abcdef',
            pool_name='reflecting',
        )

    def test_pending_but_idle(self):
        self.assert_mock_cluster_is(
            self.mock_emr_clusters['j-PENDING_BUT_IDLE'],
            has_pending_steps=True,
            idle_for=timedelta(hours=2, minutes=50),
        )

    def test_dry_run_does_nothing(self):
        self.maybe_terminate_quietly(
            max_hours_idle=0.01, dry_run=True)

        unlocked_ids = [
            'j-BOOTSTRAPPING',
            'j-CURRENTLY_RUNNING',
            'j-CUSTOM_DONE_AND_IDLE',
            'j-DEBUG_ONLY',
            'j-DONE',
            'j-DONE_AND_IDLE',
            'j-DONE_AND_IDLE_4_X',
            'j-EMPTY',
            'j-HADOOP_DEBUGGING',
            'j-IDLE_AND_FAILED',
            'j-IDLE_BUT_INCOMPLETE_STEPS',
            'j-PENDING_BUT_IDLE',
            'j-POOLED'
        ]
        for cluster_id in unlocked_ids:
            self.assert_not_locked(self.mock_emr_clusters[cluster_id])

        self.assertEqual(self.ids_of_terminated_clusters(), [])

    def test_increasing_idle_time(self):
        self.assertEqual(self.ids_of_terminated_clusters(), [])

        # no clusters are 20 hours old
        self.maybe_terminate_quietly(
            conf_paths=[], max_hours_idle=20,
            now=self.now)

        self.assertEqual(self.ids_of_terminated_clusters(), [])

        # terminate 5-hour-old jobs
        self.maybe_terminate_quietly(
            conf_paths=[], max_hours_idle=5,
            now=self.now)

        # terminate 2-hour-old jobs
        self.maybe_terminate_quietly(
            conf_paths=[], max_hours_idle=2,
            now=self.now)

        # picky edge case: two jobs are EXACTLY 2 hours old, so they're
        # not over the maximum

        self.assertEqual(self.ids_of_terminated_clusters(),
                         ['j-CUSTOM_DONE_AND_IDLE',
                          'j-IDLE_AND_FAILED',
                          'j-PENDING_BUT_IDLE'])

        self.maybe_terminate_quietly(max_hours_idle=1)

        self.assert_terminated_clusters_locked_by_terminate()
        self.assertEqual(self.ids_of_terminated_clusters(),
                         ['j-CUSTOM_DONE_AND_IDLE',
                          'j-DEBUG_ONLY',
                          'j-DONE_AND_IDLE', 'j-DONE_AND_IDLE_4_X',
                          'j-HADOOP_DEBUGGING', 'j-IDLE_AND_EXPIRED',
                          'j-IDLE_AND_FAILED', 'j-PENDING_BUT_IDLE'])

    def test_one_hour_is_the_default(self):
        self.assertEqual(self.ids_of_terminated_clusters(), [])

        self.maybe_terminate_quietly()

        self.assert_terminated_clusters_locked_by_terminate()
        self.assertEqual(self.ids_of_terminated_clusters(),
                         ['j-CUSTOM_DONE_AND_IDLE',
                          'j-DEBUG_ONLY',
                          'j-DONE_AND_IDLE', 'j-DONE_AND_IDLE_4_X',
                          'j-HADOOP_DEBUGGING', 'j-IDLE_AND_EXPIRED',
                          'j-IDLE_AND_FAILED', 'j-PENDING_BUT_IDLE'])

    def test_zero_idle_time(self):
        self.assertEqual(self.ids_of_terminated_clusters(), [])

        self.maybe_terminate_quietly(max_hours_idle=0)

        self.assert_terminated_clusters_locked_by_terminate()
        self.assertEqual(self.ids_of_terminated_clusters(),
                         ['j-CUSTOM_DONE_AND_IDLE',
                          'j-DEBUG_ONLY',
                          'j-DONE_AND_IDLE', 'j-DONE_AND_IDLE_4_X',
                          'j-HADOOP_DEBUGGING', 'j-IDLE_AND_EXPIRED',
                          'j-IDLE_AND_FAILED', 'j-PENDING_BUT_IDLE',
                          'j-POOLED'])

    def test_mins_to_end_of_hour(self):

        self.maybe_terminate_quietly(mins_to_end_of_hour=2)

        self.assertEqual(self.ids_of_terminated_clusters(), [])

        # edge case: it's exactly 5 minutes to end of hour
        self.maybe_terminate_quietly(mins_to_end_of_hour=5)

        self.assertEqual(self.ids_of_terminated_clusters(), [])

        self.maybe_terminate_quietly(mins_to_end_of_hour=6)

        self.assert_terminated_clusters_locked_by_terminate()

        # j-PENDING_BUT_IDLE is also 5 mins from end of hour, but
        # is skipped because it has pending jobs.
        self.assertEqual(self.ids_of_terminated_clusters(), ['j-POOLED'])

    def test_mins_to_end_of_hour_excludes_pending(self):
        # the filters are ANDed togther, and mins_to_end_of_hour excludes
        # jobs with pending steps.
        self.maybe_terminate_quietly(mins_to_end_of_hour=61,
                                     max_hours_idle=0.01)

        self.assert_terminated_clusters_locked_by_terminate()

        self.assertEqual(self.ids_of_terminated_clusters(),
                         ['j-CUSTOM_DONE_AND_IDLE',
                          'j-DEBUG_ONLY',
                          'j-DONE_AND_IDLE', 'j-DONE_AND_IDLE_4_X',
                          'j-HADOOP_DEBUGGING', 'j-IDLE_AND_EXPIRED',
                          'j-IDLE_AND_FAILED', 'j-POOLED'])

    def test_terminate_pooled_only(self):
        self.assertEqual(self.ids_of_terminated_clusters(), [])

        self.maybe_terminate_quietly(pooled_only=True)

        self.assert_terminated_clusters_locked_by_terminate()

        # pooled job was not idle for an hour (the default)
        self.assertEqual(self.ids_of_terminated_clusters(), [])

        self.maybe_terminate_quietly(
            pooled_only=True, max_hours_idle=0.01)

        self.assertEqual(self.ids_of_terminated_clusters(), ['j-POOLED'])

    def test_terminate_unpooled_only(self):
        self.assertEqual(self.ids_of_terminated_clusters(), [])

        self.maybe_terminate_quietly(unpooled_only=True)

        self.assert_terminated_clusters_locked_by_terminate()

        self.assertEqual(self.ids_of_terminated_clusters(),
                         ['j-CUSTOM_DONE_AND_IDLE',
                          'j-DEBUG_ONLY',
                          'j-DONE_AND_IDLE', 'j-DONE_AND_IDLE_4_X',
                          'j-HADOOP_DEBUGGING', 'j-IDLE_AND_EXPIRED',
                          'j-IDLE_AND_FAILED', 'j-PENDING_BUT_IDLE'])

        self.maybe_terminate_quietly(
            unpooled_only=True, max_hours_idle=0.01)

        self.assertEqual(self.ids_of_terminated_clusters(),
                         ['j-CUSTOM_DONE_AND_IDLE',
                          'j-DEBUG_ONLY',
                          'j-DONE_AND_IDLE', 'j-DONE_AND_IDLE_4_X',
                          'j-HADOOP_DEBUGGING', 'j-IDLE_AND_EXPIRED',
                          'j-IDLE_AND_FAILED', 'j-PENDING_BUT_IDLE'])

    def test_terminate_by_pool_name(self):
        self.assertEqual(self.ids_of_terminated_clusters(), [])

        # wrong pool name
        self.maybe_terminate_quietly(
            pool_name='default', max_hours_idle=0.01)

        self.assertEqual(self.ids_of_terminated_clusters(), [])

        # right pool name
        self.maybe_terminate_quietly(
            pool_name='reflecting', max_hours_idle=0.01)

        self.assert_terminated_clusters_locked_by_terminate()

        self.assertEqual(self.ids_of_terminated_clusters(), ['j-POOLED'])

    def test_its_quiet_too_quiet(self):
        stdout = StringIO()
        self.maybe_terminate_quietly(
            stdout=stdout, max_hours_idle=0.01, quiet=True)
        self.assertEqual(stdout.getvalue(), '')

    EXPECTED_STDOUT_LINES = [
        'Terminated cluster j-POOLED (POOLED);'
        ' was idle for 0:50:00, 0:05:00 to end of hour',
        'Terminated cluster j-PENDING_BUT_IDLE (PENDING_BUT_IDLE);'
        ' was pending for 2:50:00, 1:00:00 to end of hour',
        'Terminated cluster j-DEBUG_ONLY (DEBUG_ONLY);'
        ' was idle for 2:00:00, 1:00:00 to end of hour',
        'Terminated cluster j-DONE_AND_IDLE (DONE_AND_IDLE);'
        ' was idle for 2:00:00, 1:00:00 to end of hour',
        'Terminated cluster j-DONE_AND_IDLE_4_X (DONE_AND_IDLE_4_X);'
        ' was idle for 2:00:00, 1:00:00 to end of hour',
        'Terminated cluster j-IDLE_AND_EXPIRED (IDLE_AND_EXPIRED);'
        ' was idle for 2:00:00, 1:00:00 to end of hour',
        'Terminated cluster j-IDLE_AND_FAILED (IDLE_AND_FAILED);'
        ' was idle for 3:00:00, 1:00:00 to end of hour',
        'Terminated cluster j-HADOOP_DEBUGGING (HADOOP_DEBUGGING);'
        ' was idle for 2:00:00, 1:00:00 to end of hour',
        'Terminated cluster j-CUSTOM_DONE_AND_IDLE (CUSTOM_DONE_AND_IDLE);'
        ' was idle for 4:00:00, 1:00:00 to end of hour',
    ]

    def test_its_not_very_quiet(self):
        stdout = StringIO()
        self.maybe_terminate_quietly(
            stdout=stdout, max_hours_idle=0.01)

        self.assertEqual(set(stdout.getvalue().splitlines()),
                         set(self.EXPECTED_STDOUT_LINES))

        # should have actually terminated clusters
        self.assertEqual(self.ids_of_terminated_clusters(), [
            'j-CUSTOM_DONE_AND_IDLE',
            'j-DEBUG_ONLY',
            'j-DONE_AND_IDLE',
            'j-DONE_AND_IDLE_4_X',
            'j-HADOOP_DEBUGGING',
            'j-IDLE_AND_EXPIRED',
            'j-IDLE_AND_FAILED',
            'j-PENDING_BUT_IDLE',
            'j-POOLED',
        ])

    def test_dry_run(self):
        stdout = StringIO()
        self.maybe_terminate_quietly(
            stdout=stdout, max_hours_idle=0.01, dry_run=True)

        expected_stdout_lines = self.EXPECTED_STDOUT_LINES + [
            # dry_run doesn't actually try to lock
            'Terminated cluster j-IDLE_AND_LOCKED (IDLE_AND_LOCKED);'
            ' was idle for 2:00:00, 1:00:00 to end of hour',
            # dry_run also can't tell when clusters are termination protected
            'Terminated cluster j-IDLE_AND_PROTECTED (IDLE_AND_PROTECTED);'
            ' was idle for 2:00:00, 1:00:00 to end of hour']

        self.assertEqual(set(stdout.getvalue().splitlines()),
                         set(expected_stdout_lines))

        # shouldn't *actually* terminate clusters
        self.assertEqual(self.ids_of_terminated_clusters(), [])
