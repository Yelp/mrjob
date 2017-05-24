# -*- encoding: utf-8 -*-
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
from unittest import TestCase

from mrjob.logs.counters import _format_counters
from mrjob.logs.step import _parse_indented_counters


class FormatCountersTestCase(TestCase):

    COUNTERS = {
        'File System Counters': {
            'FILE: Number of bytes read': 8,
            'FILE: Number of bytes written': 359982,
        },
        'Job Counters': {
            'Launched map tasks': 2,
        },
    }

    def test_empty(self):
        self.assertEqual(_format_counters({}), 'Counters: 0')

    def test_basic(self):
        self.assertEqual(
            _format_counters(self.COUNTERS),
            ('Counters: 3\n'
             '\tFile System Counters\n'
             '\t\tFILE: Number of bytes read=8\n'
             '\t\tFILE: Number of bytes written=359982\n'
             '\tJob Counters\n'
             '\t\tLaunched map tasks=2'))

    def test_indent(self):
        self.assertEqual(
            _format_counters(self.COUNTERS, indent='  '),
            ('Counters: 3\n'
             '  File System Counters\n'
             '    FILE: Number of bytes read=8\n'
             '    FILE: Number of bytes written=359982\n'
             '  Job Counters\n'
             '    Launched map tasks=2'))

    def test_empty_group(self):
        # counter groups should always have at least one counter
        self.assertEqual(
            _format_counters({
                'File System Counters': {},
                'Job Counters': {
                    'Launched map tasks': 2,
                },
            }),
            ('Counters: 1\n'
             '\tJob Counters\n'
             '\t\tLaunched map tasks=2'))

    def test_round_trip(self):
        # are we outputting counters in the same format as the Hadoop binary?
        self.assertEqual(
            _parse_indented_counters(
                _format_counters(self.COUNTERS).splitlines()),
            self.COUNTERS)
