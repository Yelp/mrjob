# Copyright 2009-2012 Yelp
# Copyright 2013 Lyft
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
"""Test compatibility switching between different Hadoop versions"""
from unittest import TestCase

import os
from distutils.version import LooseVersion

from mrjob.compat import jobconf_from_dict
from mrjob.compat import jobconf_from_env
from mrjob.compat import map_version
from mrjob.compat import translate_jobconf
from mrjob.compat import translate_jobconf_dict
from mrjob.compat import translate_jobconf_for_all_versions
from mrjob.compat import uses_yarn

from tests.py2 import patch
from tests.sandbox import PatcherTestCase


class JobConfFromEnvTestCase(TestCase):

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


class JobConfFromDictTestCase(TestCase):

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


class TranslateJobConfTestCase(TestCase):

    def test_translate_jobconf(self):
        self.assertEqual(translate_jobconf('user.name', '0.20'),
                         'user.name')
        self.assertEqual(translate_jobconf('mapreduce.job.user.name', '0.20'),
                         'user.name')
        self.assertEqual(
            translate_jobconf('mapreduce.job.user.name', '0.20.2'),
            'user.name')
        self.assertEqual(translate_jobconf('user.name', '0.21'),
                         'mapreduce.job.user.name')

        self.assertEqual(translate_jobconf('user.name', '1.0'),
                         'user.name')
        self.assertEqual(translate_jobconf('user.name', '2.0'),
                         'mapreduce.job.user.name')

        self.assertEqual(translate_jobconf('foo.bar', '2.0'), 'foo.bar')

    def test_version_may_not_be_None(self):
        self.assertRaises(TypeError, translate_jobconf, 'user.name', None)
        # test unknown variables too, since they don't go through map_version()
        self.assertRaises(TypeError, translate_jobconf, 'foo.bar', None)


class TranslateJobConfDictTestCase(PatcherTestCase):

    # jobconf with spooooky mix of Hadoop 1 and Hadoop 2 variables
    JOBCONF = {
        'foo.bar': 'baz',                   # unknown jobconf
        'mapred.jar': 'a.jar',              # Hadoop 1 jobconf
        'mapreduce.job.user.name': 'dave',  # Hadoop 2 jobconf
    }

    def setUp(self):
        super(TranslateJobConfDictTestCase, self).setUp()
        self.log = self.start(patch('mrjob.compat.log'))

    def test_empty(self):
        self.assertEqual(translate_jobconf_dict({}), {})
        self.assertFalse(self.log.warning.called)

    def test_no_version(self):
        self.assertEqual(
            translate_jobconf_dict({
                'foo.bar': 'baz',                   # unknown jobconf
                'mapred.jar': 'a.jar',              # Hadoop 1 jobconf
                'mapreduce.job.user.name': 'dave',  # Hadoop 2 jobconf
            }),
            {
                'foo.bar': 'baz',
                'mapred.jar': 'a.jar',
                'mapreduce.job.jar': 'a.jar',
                'mapreduce.job.user.name': 'dave',
                'user.name': 'dave',
            })

        self.assertFalse(self.log.warning.called)

    def test_hadoop_1(self):
        self.assertEqual(
            translate_jobconf_dict({
                'foo.bar': 'baz',
                'mapred.jar': 'a.jar',
                'mapreduce.job.user.name': 'dave',
            }, hadoop_version='1.0'),
            {
                'foo.bar': 'baz',
                'mapred.jar': 'a.jar',
                'mapreduce.job.user.name': 'dave',
                'user.name': 'dave',
            })

        self.assertTrue(self.log.warning.called)

    def test_hadoop_2(self):
        self.assertEqual(
            translate_jobconf_dict({
                'foo.bar': 'baz',
                'mapred.jar': 'a.jar',
                'mapreduce.job.user.name': 'dave',
            }, hadoop_version='2.0'),
            {
                'foo.bar': 'baz',
                'mapred.jar': 'a.jar',
                'mapreduce.job.jar': 'a.jar',
                'mapreduce.job.user.name': 'dave',
            })

        self.assertTrue(self.log.warning.called)

    def test_dont_overwrite(self):
        # this jobconf contains two versions of the same variable
        jobconf = {
            'mapred.jar': 'a.jar',
            'mapreduce.job.jar': 'a.jar',
        }

        self.assertEqual(translate_jobconf_dict(jobconf, '1.0'), jobconf)
        self.assertFalse(self.log.warning.called)


class TranslateJobConfForAllVersionsTestCase(TestCase):

    def test_translate_jobconf_for_all_versions(self):
        self.assertEqual(translate_jobconf_for_all_versions('user.name'),
                         ['mapreduce.job.user.name', 'user.name'])
        self.assertEqual(translate_jobconf_for_all_versions('foo.bar'),
                         ['foo.bar'])


class UsesYarnTestCase(TestCase):

    def test_uses_yarn(self):
        self.assertEqual(uses_yarn('0.22'), False)
        self.assertEqual(uses_yarn('0.23'), True)
        self.assertEqual(uses_yarn('0.24'), True)
        self.assertEqual(uses_yarn('1.0.0'), False)
        self.assertEqual(uses_yarn('2.0.0'), True)


class MapVersionTestCase(TestCase):

    def test_empty(self):
        self.assertRaises(ValueError, map_version, '0.5.0', None)
        self.assertRaises(ValueError, map_version, '0.5.0', {})
        self.assertRaises(ValueError, map_version, '0.5.0', [])

    def test_version_may_not_be_None(self):
        self.assertEqual(map_version('1', {'1': 'foo'}), 'foo')
        self.assertRaises(TypeError, map_version, None, {'1': 'foo'})

    def test_dict(self):
        version_map = {
            '1': 'foo',
            '2': 'bar',
            '3': 'baz',
        }

        self.assertEqual(map_version('1.1', version_map), 'foo')
        # test exact match
        self.assertEqual(map_version('2', version_map), 'bar')
        # versions are just minimums
        self.assertEqual(map_version('4.5', version_map), 'baz')
        # compare versions, not strings
        self.assertEqual(map_version('11.11', version_map), 'baz')
        # fall back to lowest version
        self.assertEqual(map_version('0.1', version_map), 'foo')

    def test_list_of_tuples(self):
        version_map = [
            (LooseVersion('1'), 'foo'),
            (LooseVersion('2'), 'bar'),
            (LooseVersion('3'), 'baz'),
        ]

        self.assertEqual(map_version('1.1', version_map), 'foo')
        self.assertEqual(map_version('2', version_map), 'bar')
        self.assertEqual(map_version('4.5', version_map), 'baz')
        self.assertEqual(map_version('11.11', version_map), 'baz')
        self.assertEqual(map_version('0.1', version_map), 'foo')
