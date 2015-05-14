# Copyright 2009-2012 Yelp and Contributors
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
"""Tests for mrjob.ssh"""
from subprocess import PIPE

from mrjob import ssh

from tests.py2 import Mock
from tests.py2 import TestCase
from tests.py2 import call
from tests.py2 import patch



class HadoopJobKillTestCase(TestCase):

    SSH_ARGS = [
        'ssh_bin', '-i', 'key.pem', '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null', 'hadoop@address',
    ]


    EXPECTED_LIST_CALL = SSH_ARGS + ['hadoop', 'job', '-list']

    GOOD_LIST_OUTPUT = (
        b"1 jobs currently running\n"
        b"JobId   State   StartTime   UserName    Priority    SchedulingInfo\n"
        b"job_201205162225_0003   4   1337208155510   hadoop  NORMAL  NA\n")

    EXPECTED_KILL_CALL = SSH_ARGS + [
        'hadoop', 'job', '-kill', 'job_201205162225_0003',
    ]

    GOOD_KILL_OUTPUT = b"Killed job job_201205162225_0003\n"

    def test_expected(self):

        values = [self.GOOD_LIST_OUTPUT, self.GOOD_KILL_OUTPUT]

        def fake_popen(*args, **kwargs):
            m = Mock()
            m.communicate.return_value = (values.pop(0), b'')
            return m

        with patch.object(ssh, 'Popen', side_effect=fake_popen) as m:
            ssh.ssh_terminate_single_job(['ssh_bin'], 'address', 'key.pem')
            self.assertEqual(m.call_args_list[0],
                             call(self.EXPECTED_LIST_CALL,
                                  stdin=PIPE, stdout=PIPE, stderr=PIPE))
            self.assertEqual(m.call_args_list[1],
                             call(self.EXPECTED_KILL_CALL,
                                  stdin=PIPE, stdout=PIPE, stderr=PIPE))

    def test_too_many_jobs_on_the_dance_floor(self):

        def fake_popen(*args, **kwargs):
            m = Mock()
            m.communicate.return_value = (b"2 jobs currently running\n", b'')
            return m

        with patch.object(ssh, 'Popen', side_effect=fake_popen):
            self.assertRaises(IOError, ssh.ssh_terminate_single_job,
                              ['ssh_bin'], 'address', 'key.pem')

    def test_dance_floor_is_empty(self):

        def fake_popen(*args, **kwargs):
            m = Mock()
            m.communicate.return_value = (b"0 jobs currently running\n", b'')
            return m

        with patch.object(ssh, 'Popen', side_effect=fake_popen):
            self.assertEqual(
                None, ssh.ssh_terminate_single_job(
                    ['ssh_bin'], 'address', 'key.pem'))

    def test_junk_list_output(self):

        def fake_popen(*args, **kwargs):
            m = Mock()
            m.communicate.return_value = (b"yah output, its gahbage\n", b'')
            return m

        with patch.object(ssh, 'Popen', side_effect=fake_popen):
            self.assertRaises(IOError, ssh.ssh_terminate_single_job,
                              ['ssh_bin'], 'address', 'key.pem')

    def test_junk_kill_output(self):

        values = [self.GOOD_LIST_OUTPUT, b"yah output, its gahbage\n"]

        def fake_popen(*args, **kwargs):
            m = Mock()
            m.communicate.return_value = (values.pop(0), b'')
            return m

        with patch.object(ssh, 'Popen', side_effect=fake_popen):
            self.assertEqual(
                ssh.ssh_terminate_single_job(
                    ['ssh_bin'], 'address', 'key.pem'),
                'yah output, its gahbage\n')
