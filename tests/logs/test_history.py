# -*- encoding: utf-8 -*-
# Copyright 2015 Yelp
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

from mrjob.logs.history import _match_history_log

from tests.py2 import TestCase


class MatchHistoryLogTestCase(TestCase):

    def test_empty(self):
        self.assertEqual(_match_history_log(''), None)

    def test_pre_yarn(self):
        history_path = (
            '/logs/history/done/version-1/host_1451590133273_/2015/12/31'
            '/000000/job_201512311928_0001_1451590317008_hadoop'
            '_streamjob8025762403845318969.jar')

        self.assertEqual(
            _match_history_log(history_path),
            dict(job_id='job_201512311928_0001', yarn=False))

        conf_path = (
            '/logs/history/done/version-1/host_1451590133273_/2015/12/31'
            '/000000/job_201512311928_0001_conf.xml')

        self.assertEqual(
            _match_history_log(conf_path),
            None)

    def test_pre_yarn_filter_by_job_id(self):
        history_path = (
            '/logs/history/done/version-1/host_1451590133273_/2015/12/31'
            '/000000/job_201512311928_0001_1451590317008_hadoop'
            '_streamjob8025762403845318969.jar')

        self.assertEqual(
            _match_history_log(history_path, job_id='job_201512311928_0001'),
            dict(job_id='job_201512311928_0001', yarn=False))

        self.assertEqual(
            _match_history_log(history_path, job_id='job_201512311928_0002'),
            None)

    def test_yarn(self):
        history_path = (
            'hdfs:///tmp/hadoop-yarn/staging/history/done/2015/12/31/000000/'
            'job_1451592123989_0001-1451592605470-hadoop-QuasiMonteCarlo'
            '-1451592786882-10-1-SUCCEEDED-default-1451592631082.jhist')

        self.assertEqual(
            _match_history_log(history_path),
            dict(job_id='job_1451592123989_0001', yarn=True))

        conf_path = (
            'hdfs:///tmp/hadoop-yarn/staging/history/done/2015/12/31/000000/'
            'job_1451592123989_0001_conf.xml')

        self.assertEqual(
            _match_history_log(conf_path),
            None)

    def test_yarn_filter_by_job_id(self):
        history_path = (
            'hdfs:///tmp/hadoop-yarn/staging/history/done/2015/12/31/000000/'
            'job_1451592123989_0001-1451592605470-hadoop-QuasiMonteCarlo'
            '-1451592786882-10-1-SUCCEEDED-default-1451592631082.jhist')

        self.assertEqual(
            _match_history_log(history_path, job_id='job_1451592123989_0001'),
            dict(job_id='job_1451592123989_0001', yarn=True))

        self.assertEqual(
            _match_history_log(history_path, job_id='job_1451592123989_0002'),
            None)
