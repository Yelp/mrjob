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

from __future__ import with_statement

from subprocess import PIPE

from mock import Mock
from mock import call
from mock import patch

try:
    import unittest2 as unittest
    unittest  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import unittest


from mrjob import ssh


class HadoopJobKillTestCase(unittest.TestCase):

    EXPECTED_LIST_CALL = [
        'ssh_bin', '-i', 'key.pem', '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null', 'hadoop@address',
        'hadoop', 'job', '-list',
    ]

    GOOD_LIST_OUTPUT = (
        "1 jobs currently running\n"
        "JobId   State   StartTime   UserName    Priority    SchedulingInfo\n"
        "job_201205162225_0003   4   1337208155510   hadoop  NORMAL  NA\n")

    EXPECTED_KILL_CALL = [
        'ssh_bin', '-i', 'key.pem', '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null', 'hadoop@address',
        'hadoop', 'job', '-kill', 'job_201205162225_0003',
    ]

    GOOD_KILL_OUTPUT = "Killed job job_201205162225_0003\n"

    def test_hadoop_job_kill(self):

        values = [self.GOOD_LIST_OUTPUT, self.GOOD_KILL_OUTPUT]

        def fake_popen(*args, **kwargs):
            m = Mock()
            m.communicate.return_value = (values.pop(0), '')
            return m

        with patch.object(ssh, 'Popen', side_effect=fake_popen) as m:
            ssh.ssh_terminate_single_job(['ssh_bin'], 'address', 'key.pem')
            self.assertEqual(m.call_args_list[0],
                             call(self.EXPECTED_LIST_CALL,
                                  stdin=PIPE, stdout=PIPE, stderr=PIPE))
            self.assertEqual(m.call_args_list[1],
                             call(self.EXPECTED_KILL_CALL,
                                  stdin=PIPE, stdout=PIPE, stderr=PIPE))
