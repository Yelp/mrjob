# Copyright 2012 Yelp
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
try:
    from unittest2 import TestCase
    TestCase  # pyflakes
except ImportError:
    from unittest import TestCase

from mock import patch

from mrjob import cmd
from mrjob import launch
from mrjob.tools.emr import (
    audit_usage,
    create_job_flow,
    fetch_logs,
    report_long_jobs,
    s3_tmpwatch,
    terminate_idle_job_flows,
    terminate_job_flow)


class CommandTestCase(TestCase):

    def setUp(self):
        def error(msg=None):
            if msg:
                raise ValueError(msg)
            else:
                raise ValueError

        p = patch.object(cmd, 'error', side_effect=error)
        p.start()
        self.addCleanup(p.stop)

    def test_run(self):
        with patch.object(launch, 'MRJobLauncher') as m_launcher:
            cmd.main(args=['mrjob', 'run', 'script.py'])
            m_launcher.assert_called_once_with(
                args=['script.py'], from_cl=True)

    def test_audit_usage(self):
        with patch.object(audit_usage, 'main') as m_main:
            cmd.main(args=['mrjob', 'audit-emr-usage'])
            m_main.assert_called_once_with([])

    def test_create_job_flow(self):
        with patch.object(create_job_flow, 'main') as m_main:
            cmd.main(args=['mrjob', 'create-job-flow'])
            m_main.assert_called_once_with([])

    def test_fetch_logs(self):
        with patch.object(fetch_logs, 'main') as m_main:
            cmd.main(args=['mrjob', 'fetch-logs'])
            m_main.assert_called_once_with([])

    def test_report_long_jobs(self):
        with patch.object(report_long_jobs, 'main') as m_main:
            cmd.main(args=['mrjob', 'report-long-jobs'])
            m_main.assert_called_once_with([])

    def test_s3_tmpwatch(self):
        with patch.object(s3_tmpwatch, 'main') as m_main:
            cmd.main(args=['mrjob', 's3-tmpwatch'])
            m_main.assert_called_once_with([])

    def test_terminate_idle_job_flows(self):
        with patch.object(terminate_idle_job_flows, 'main') as m_main:
            cmd.main(args=['mrjob', 'terminate-idle-job-flows'])
            m_main.assert_called_once_with([])

    def test_terminate_job_flow(self):
        with patch.object(terminate_job_flow, 'main') as m_main:
            cmd.main(args=['mrjob', 'terminate-job-flow'])
            m_main.assert_called_once_with([])
