
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

"""Test compatibility switching between different Hadoop versions"""

import os

from testify import TestCase
from testify import assert_equal
from testify import setup
from testify import teardown

from mrjob.compat import get_jobconf_value
from mrjob.compat import supports_combiners_in_hadoop_streaming
from mrjob.compat import translate_jobconf
from mrjob.compat import uses_generic_jobconf


class EnvVarTestCase(TestCase):

    @setup
    def store_env(self):
        self._old_env = os.environ.copy()

    @teardown
    def replace_env(self):
        os.environ.clear()
        os.environ.update(self._old_env)

    def test_get_jobconf_value_1(self):
        os.environ['user_name'] = 'Edsger W. Dijkstra'
        assert_equal(get_jobconf_value('user.name'),
                     'Edsger W. Dijkstra')
        assert_equal(get_jobconf_value('mapreduce.job.user.name'),
                     'Edsger W. Dijkstra')

    def test_get_jobconf_value_2(self):
        os.environ['mapreduce_job_user_name'] = 'Edsger W. Dijkstra'
        assert_equal(get_jobconf_value('user.name'),
                     'Edsger W. Dijkstra')
        assert_equal(get_jobconf_value('mapreduce.job.user.name'),
                     'Edsger W. Dijkstra')


class CompatTestCase(TestCase):

    def test_translate_jobconf(self):
        assert_equal(translate_jobconf('user.name', '0.18'),
                     'user.name')
        assert_equal(translate_jobconf('mapreduce.job.user.name', '0.18'),
                     'user.name')
        assert_equal(translate_jobconf('user.name', '0.19'),
                     'user.name')
        assert_equal(translate_jobconf('mapreduce.job.user.name', '0.19.2'),
                     'user.name')
        assert_equal(translate_jobconf('user.name', '0.21'),
                     'mapreduce.job.user.name')

    def test_supports_combiners(self):
        assert_equal(supports_combiners_in_hadoop_streaming('0.19'),
                     False)
        assert_equal(supports_combiners_in_hadoop_streaming('0.19.2'),
                     False)
        assert_equal(supports_combiners_in_hadoop_streaming('0.20'),
                     True)
        assert_equal(supports_combiners_in_hadoop_streaming('0.20.203'),
                     True)

    def test_uses_generic_jobconf(self):
        assert_equal(uses_generic_jobconf('0.18'), False)
        assert_equal(uses_generic_jobconf('0.20'), True)
        assert_equal(uses_generic_jobconf('0.21'), True)
