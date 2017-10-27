# -*- coding: utf-8 -*-
# Copyright 2015-2016 Yelp
# Copyright 2017 Yelp
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

from tests.mock_boto3.iam import MockIAMClient


# IAM stuff is mostly tested by test_emr.py, but we don't test what happens if
# there are already enough IAM objects to cause pagination

class PaginationTestCase(TestCase):

    def test_many_instance_profiles(self):
        client = MockIAMClient()
        max_items = client.DEFAULT_MAX_ITEMS

        for i in range(2 * max_items):
            client.create_instance_profile(InstanceProfileName=('ip-%03d' % i))

        paginator = client.get_paginator('list_instance_profiles')
        pages = list(paginator.paginate())

        self.assertEqual(len(pages), 2)
        self.assertEqual(len(pages[0]['InstanceProfiles']), max_items)
        self.assertEqual(len(pages[1]['InstanceProfiles']), max_items)

    def test_many_roles(self):
        client = MockIAMClient()
        max_items = client.DEFAULT_MAX_ITEMS

        for i in range(2 * max_items + 1):
            client.create_role(
                AssumeRolePolicyDocument=json.dumps(_MRJOB_SERVICE_ROLE),
                RoleName=('r-%03d' % i),)

        paginator = client.get_paginator('list_roles')
        pages = list(paginator.paginate())

        self.assertEqual(len(pages), 3)
        self.assertEqual(len(pages[0]['Roles']), max_items)
        self.assertEqual(len(pages[1]['Roles']), max_items)
        self.assertEqual(len(pages[2]['Roles']), 1)
