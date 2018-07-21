# -*- coding: utf-8 -*-
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
import json
from unittest import TestCase

from mrjob.iam import _MRJOB_SERVICE_ROLE
from mrjob.iam import _unwrap_response
from mrjob.iam import _yield_instance_profiles
from mrjob.iam import _yield_roles

from tests.mockboto import MockIAMConnection


# IAM stuff is mostly tested by test_emr.py, but we don't test what happens if
# there are already enough IAM objects to cause pagination

class PaginationTestCase(TestCase):

    def test_many_instance_profiles(self):
        conn = MockIAMConnection()
        max_items = conn.DEFAULT_MAX_ITEMS

        for i in range(2 * max_items):
            conn.create_instance_profile('ip-%03d' % i)

        instance_profiles_page = _unwrap_response(
            conn.list_instance_profiles())['instance_profiles']
        self.assertEqual(len(instance_profiles_page), max_items)

        instance_profiles = list(_yield_instance_profiles(conn))
        self.assertEqual(len(instance_profiles), 2 * max_items)

    def test_many_roles(self):
        conn = MockIAMConnection()
        max_items = conn.DEFAULT_MAX_ITEMS

        for i in range(2 * max_items):
            conn.create_role('r-%03d' % i, json.dumps(_MRJOB_SERVICE_ROLE))

        roles_page = _unwrap_response(conn.list_roles())['roles']
        self.assertEqual(len(roles_page), max_items)

        roles = list(_yield_roles(conn))
        self.assertEqual(len(roles), 2 * max_items)
