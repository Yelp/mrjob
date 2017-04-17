# Copyright 2015-2017 Yelp and Contributors
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
"""Mock boto3 IAM support."""
import json

from botocore.exceptions import ClientError

from mrjob.aws import _boto3_now
from mrjob.conf import combine_values

from .util import MockClientMeta
from .util import MockPaginator


class MockIAMClient(object):

    DEFAULT_PATH = '/'

    DEFAULT_MAX_ITEMS = 100

    OPERATION_NAME_TO_RESULT_KEY = dict(
        list_attached_role_policies='AttachedPolicies',
        list_instance_profiles='InstanceProfiles',
        list_roles='Roles',
    )

    def __init__(self, region_name=None, api_version=None,
                 use_ssl=True, verify=None, endpoint_url=None,
                 aws_access_key_id=None, aws_secret_access_key=None,
                 aws_session_token=None, config=None,
                 mock_iam_instance_profiles=None, mock_iam_roles=None,
                 mock_iam_role_attached_policies=None):
        """Mock out connection to IAM.

        mock_iam_instance_profiles maps profile name to a dict containing:
            create_date -- ISO creation datetime
            path -- IAM path
            role_name -- name of single role for this instance profile, or None

        mock_iam_roles maps role name to a dict containing:
            assume_role_policy_document -- a JSON-then-URI-encoded policy doc
            create_date -- ISO creation datetime
            path -- IAM path

        mock_iam_role_attached_policies maps role to a list of ARNs for
        attached (managed) policies.

        We don't track which managed policies exist or what their contents are.
        We also don't support role IDs.
        """
        # if not passed dictionaries for these mock values, create our own
        self.mock_iam_instance_profiles = combine_values(
            {}, mock_iam_instance_profiles)
        self.mock_iam_roles = combine_values({}, mock_iam_roles)
        self.mock_iam_role_attached_policies = combine_values(
            {}, mock_iam_role_attached_policies)

        endpoint_url = endpoint_url or 'https://iam.amazonaws.com'
        region_name = region_name or 'aws-global'

        self.meta = MockClientMeta(
            endpoint_url=endpoint_url,
            region_name=region_name)

    def get_paginator(self, operation_name):
        return MockPaginator(
            getattr(self, operation_name),
            self.OPERATION_NAME_TO_RESULT_KEY[operation_name],
            self.DEFAULT_MAX_ITEMS)

    # roles

    def create_role(self, AssumeRolePolicyDocument, RoleName):
        # Path not supported
        # mock RoleIds are all the same

        self._check_role_does_not_exist(RoleName, 'CreateRole')

        role = dict(
            Arn=('arn:aws:iam::012345678901:role/%s' % RoleName),
            AssumeRolePolicyDocument=json.loads(AssumeRolePolicyDocument),
            CreateDate=_boto3_now(),
            Path='/',
            RoleId='AROAMOCKMOCKMOCKMOCK',
            RoleName=RoleName,
        )
        self.mock_iam_roles[RoleName] = role

        return dict(Role=role)

    def list_roles(self):
        # PathPrefix not supported

        roles = list(data for name, data in
                     sorted(self.mock_iam_roles.items()))

        return dict(Roles=roles)

    def _check_role_does_not_exist(self, RoleName, OperationName):
        if RoleName in self.mock_iam_roles:
            raise ClientError(
                dict(Error=dict(
                    Code='EntityAlreadyExists',
                    Message=('Role with name %s already exists' % RoleName))),
                OperationName)

    def _check_role_exists(self, RoleName, OperationName):
        if RoleName not in self.mock_iam_roles:
            raise ClientError(
                dict(Error=dict(
                    Code='NoSuchEntity',
                    Message=('Role not found for %s' % RoleName))),
                OperationName)

    # attached role policies

    def attach_role_policy(self, PolicyArn, RoleName):
        self._check_role_exists(RoleName, 'AttachRolePolicy')

        arns = self.mock_iam_role_attached_policies.setdefault(RoleName, [])
        if PolicyArn not in arns:
            arns.append(PolicyArn)

        return {}

    def list_attached_role_policies(self, RoleName):
        self._check_role_exists(RoleName, 'ListAttachedRolePolicies')

        arns = self.mock_iam_role_attached_policies.get(RoleName, [])

        return dict(AttachedPolicies=[
            dict(PolicyArn=arn, PolicyName=arn.split('/')[-1])
            for arn in arns
        ])

    # instance profiles

    def create_instance_profile(self, InstanceProfileName):
        # Path not implemented
        # mock InstanceProfileIds are all the same

        self._check_instance_profile_does_not_exist(InstanceProfileName,
                                                    'CreateInstanceProfile')

        profile = dict(
            Arn=('arn:aws:iam::012345678901:instance-profile/%s' %
                 InstanceProfileName),
            CreateDate=_boto3_now(),
            InstanceProfileId='AIPAMOCKMOCKMOCKMOCK',
            InstanceProfileName=InstanceProfileName,
            Path='/',
            Roles=[],
        )
        self.mock_iam_instance_profiles[InstanceProfileName] = profile

        return dict(InstanceProfile=profile)

    def add_role_to_instance_profile(self, InstanceProfileName, RoleName):
        self._check_role_exists(RoleName, 'AddRoleToInstanceProfile')
        self._check_instance_profile_exists(
            InstanceProfileName, 'AddRoleToInstanceProfile')

        profile = self.mock_iam_instance_profiles[InstanceProfileName]
        if profile['Roles']:
            raise ClientError(
                dict(Error=dict(
                    Code='LimitExceeded',
                    Message=('Cannot exceed quota for'
                             ' InstanceSessionsPerInstanceProfile: 1'),
                )),
                'AddRoleToInstanceProfile',
            )

        # just point straight at the mock role; never going to redefine it
        profile['Roles'] = [self.mock_iam_roles[RoleName]]

    def list_instance_profiles(self):
        # PathPrefix not implemented

        profiles = list(data for name, data in
                        sorted(self.mock_iam_instance_profiles.items()))

        return dict(InstanceProfiles=profiles)

    def _check_instance_profile_does_not_exist(
            self, InstanceProfileName, OperationName):

        if InstanceProfileName in self.mock_iam_instance_profiles:
            raise ClientError(
                dict(Error=dict(
                    Code='EntityAlreadyExists',
                    Message=('Instance Profile %s already exists' %
                             InstanceProfileName))),
                OperationName)

    def _check_instance_profile_exists(
            self, InstanceProfileName, OperationName):

        if InstanceProfileName not in self.mock_iam_instance_profiles:
            raise ClientError(
                dict(Error=dict(
                    Code='NoSuchEntity',
                    Message=('Instance Profile %s cannot be found' %
                             InstanceProfileName))),
                OperationName)
