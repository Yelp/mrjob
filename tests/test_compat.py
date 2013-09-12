# Copyright 2009-2012 Yelp
# Copyright 2013 Lyft
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

try:
    import unittest2 as unittest
    unittest  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import unittest

from mock import patch

from mrjob.compat import jobconf_from_env
from mrjob.compat import jobconf_from_dict
from mrjob.compat import supports_combiners_in_hadoop_streaming
from mrjob.compat import supports_new_distributed_cache_options
from mrjob.compat import translate_jobconf
from mrjob.compat import uses_generic_jobconf


class GetJobConfValueTestCase(unittest.TestCase):

    def setUp(self):
        p = patch.object(os, 'environ', {})
        p.start()
        self.addCleanup(p.stop)

    def test_get_old_hadoop_jobconf(self):
        os.environ['user_name'] = 'Edsger W. Dijkstra'
        self.assertEqual(jobconf_from_env('user.name'),
                         'Edsger W. Dijkstra')
        self.assertEqual(jobconf_from_env('mapreduce.job.user.name'),
                         'Edsger W. Dijkstra')

    def test_get_new_hadoop_jobconf(self):
        os.environ['mapreduce_job_user_name'] = 'Edsger W. Dijkstra'
        self.assertEqual(jobconf_from_env('user.name'),
                         'Edsger W. Dijkstra')
        self.assertEqual(jobconf_from_env('mapreduce.job.user.name'),
                         'Edsger W. Dijkstra')

    def test_default(self):
        self.assertEqual(jobconf_from_env('user.name'), None)
        self.assertEqual(jobconf_from_env('user.name', 'dave'), 'dave')

    def test_get_missing_jobconf_not_in_table(self):
        # there was a bug where defaults didn't work for jobconf
        # variables that we don't know about
        self.assertEqual(jobconf_from_env('user.defined'), None)
        self.assertEqual(jobconf_from_env('user.defined', 'beauty'), 'beauty')


class JobConfFromDictTestCase(unittest.TestCase):

    def test_get_old_hadoop_jobconf(self):
        jobconf = {'user.name': 'Edsger W. Dijkstra'}
        self.assertEqual(jobconf_from_dict(jobconf, 'user.name'),
                         'Edsger W. Dijkstra')
        self.assertEqual(jobconf_from_dict(jobconf, 'mapreduce.job.user.name'),
                         'Edsger W. Dijkstra')

    def test_get_new_hadoop_jobconf(self):
        jobconf = {'mapreduce.job.user.name': 'Edsger W. Dijkstra'}
        self.assertEqual(jobconf_from_dict(jobconf, 'user.name'),
                         'Edsger W. Dijkstra')
        self.assertEqual(jobconf_from_dict(jobconf, 'mapreduce.job.user.name'),
                         'Edsger W. Dijkstra')

    def test_default(self):
        self.assertEqual(jobconf_from_dict({}, 'user.name'), None)
        self.assertEqual(jobconf_from_dict({}, 'user.name', 'dave'), 'dave')

    def test_get_missing_jobconf_not_in_table(self):
        # there was a bug where defaults didn't work for jobconf
        # variables that we don't know about
        self.assertEqual(jobconf_from_dict({}, 'user.defined'), None)
        self.assertEqual(
            jobconf_from_dict({}, 'user.defined', 'beauty'), 'beauty')


class CompatTestCase(unittest.TestCase):

    def test_translate_jobconf(self):
        self.assertEqual(translate_jobconf('user.name', '0.18'),
                         'user.name')
        self.assertEqual(translate_jobconf('mapreduce.job.user.name', '0.18'),
                         'user.name')
        self.assertEqual(translate_jobconf('user.name', '0.19'),
                         'user.name')
        self.assertEqual(
            translate_jobconf('mapreduce.job.user.name', '0.19.2'),
            'user.name')
        self.assertEqual(translate_jobconf('user.name', '0.21'),
                         'mapreduce.job.user.name')

        self.assertEqual(translate_jobconf('user.name', '1.0'),
                         'user.name')
        self.assertEqual(translate_jobconf('user.name', '2.0'),
                         'mapreduce.job.user.name')

    def test_supports_combiners(self):
        self.assertEqual(supports_combiners_in_hadoop_streaming('0.19'),
                         False)
        self.assertEqual(supports_combiners_in_hadoop_streaming('0.19.2'),
                         False)
        self.assertEqual(supports_combiners_in_hadoop_streaming('0.20'),
                         True)
        self.assertEqual(supports_combiners_in_hadoop_streaming('0.20.203'),
                         True)

    def test_uses_generic_jobconf(self):
        self.assertEqual(uses_generic_jobconf('0.18'), False)
        self.assertEqual(uses_generic_jobconf('0.20'), True)
        self.assertEqual(uses_generic_jobconf('0.21'), True)

    def test_cache_opts(self):
        self.assertEqual(supports_new_distributed_cache_options('0.18'), False)
        self.assertEqual(supports_new_distributed_cache_options('0.20'), False)
        self.assertEqual(
            supports_new_distributed_cache_options('0.20.203'), True)
