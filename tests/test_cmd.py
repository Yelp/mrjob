# Copyright 2012 Yelp
# Copyright 2013 David Marin
# Copyright 2014-2016 Yelp
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
from unittest import TestCase

from mrjob import cmd
from mrjob import launch
from mrjob.tools.emr import audit_usage
from mrjob.tools.emr import create_cluster
from mrjob.tools.emr import create_job_flow
from mrjob.tools.emr import report_long_jobs
from mrjob.tools.emr import s3_tmpwatch
from mrjob.tools.emr import terminate_cluster
from mrjob.tools.emr import terminate_idle_clusters
from mrjob.tools.emr import terminate_idle_job_flows
from mrjob.tools.emr import terminate_job_flow

from tests.py2 import patch


class CommandTestCase(TestCase):

    def setUp(self):
        def error(msg=None):
            if msg:
                raise ValueError(msg)
            else:
                raise ValueError

        p = patch.object(cmd, '_error', side_effect=error)
        p.start()
        self.addCleanup(p.stop)

    def _test_main_call(self, module, cmd_name):
        with patch.object(module, 'main') as m_main:
            cmd.main(args=['mrjob', cmd_name])
            m_main.assert_called_once_with([])

    def test_run(self):
        with patch.object(launch, 'MRJobLauncher') as m_launcher:
            cmd.main(args=['mrjob', 'run', 'script.py'])
            m_launcher.assert_called_once_with(
                args=['script.py'], from_cl=True)

    def test_audit_usage(self):
        self._test_main_call(audit_usage, 'audit-emr-usage')

    def test_create_cluster(self):
        self._test_main_call(create_cluster, 'create-cluster')

    def test_report_long_jobs(self):
        self._test_main_call(report_long_jobs, 'report-long-jobs')

    def test_s3_tmpwatch(self):
        self._test_main_call(s3_tmpwatch, 's3-tmpwatch')

    def test_terminate_idle_clusters(self):
        self._test_main_call(terminate_idle_clusters,
                             'terminate-idle-clusters')

    def test_terminate_cluster(self):
        self._test_main_call(terminate_cluster, 'terminate-cluster')

    def test_deprecated_create_job_flow(self):
        self._test_main_call(create_job_flow, 'create-job-flow')

    def test_deprecated_terminate_idle_job_flows(self):
        self._test_main_call(terminate_idle_job_flows,
                             'terminate-idle-job-flows')

    def test_deprecated_terminate_job_flow(self):
        self._test_main_call(terminate_job_flow, 'terminate-job-flow')
