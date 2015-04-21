import json
try:
    from unittest2 import TestCase
    TestCase  # pyflakes
except ImportError:
    from unittest import TestCase

from mrjob.iam import MRJOB_SERVICE_ROLE
from mrjob.iam import MRJOB_SERVICE_ROLE_POLICY
from mrjob.iam import _unwrap_response
from mrjob.iam import yield_instance_profiles_with_policies
from mrjob.iam import yield_roles_with_policies
from mrjob.iam import yield_policies_for_role

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

        instance_profiles = list(yield_instance_profiles_with_policies(conn))
        self.assertEqual(len(instance_profiles), 2 * max_items)

    def test_many_roles(self):
        conn = MockIAMConnection()
        max_items = conn.DEFAULT_MAX_ITEMS

        for i in range(2 * max_items):
            conn.create_role('r-%03d' % i, MRJOB_SERVICE_ROLE)

        roles_page = _unwrap_response(
            conn.list_roles())['roles']
        self.assertEqual(len(roles_page), max_items)

        roles = list(yield_roles_with_policies(conn))
        self.assertEqual(len(roles), 2 * max_items)

    def test_many_role_policies(self):
        conn = MockIAMConnection()
        max_items = conn.DEFAULT_MAX_ITEMS

        conn.create_role('r', MRJOB_SERVICE_ROLE)

        for i in range(2 * max_items):
            conn.put_role_policy('r', 'rp-%03d' % i,
                                 json.dumps(MRJOB_SERVICE_ROLE_POLICY))

        policies_page = _unwrap_response(
            conn.list_role_policies('r'))['policy_names']
        self.assertEqual(len(policies_page), max_items)

        policies = list(yield_policies_for_role(conn, 'r'))
        self.assertEqual(len(policies), 2 * max_items)
