# -*- coding: utf-8 -*-
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
"""Utilities for dealing with AWS IAM.

The main purpose of this code is to create the needed IAM instance profiles,
roles, and policies to make EMR work with a new account.

This works somewhat similarly to the AWS CLI (aws emr create-default-roles),
except they are assigned random mrjob-* names.

There isn't really versioning; mrjob will simply check if there are IAM objects
idential to the ones it needs before attempting to create them.
"""
import json
from logging import getLogger
from urllib import unquote

from mrjob.aws import random_identifier

# Working IAM roles and policies for EMR; these should be identical
# to the ones created by AWS CLI (`aws emr create-default-roles`), at least as
# of 2015-04-20.
#
# AWS has recommended roles in their documentation, but they don't quite
# work as-is:
#
# http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-iam-roles-defaultroles.html  # noqa

# use this for service_role
MRJOB_SERVICE_ROLE = {
    "Version": "2008-10-17",
    "Statement": [{
        "Sid": "",
        "Effect": "Allow",
        "Principal": {
            "Service": "elasticmapreduce.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
    }]
}

# Role to wrap in an instance profile
MRJOB_INSTANCE_PROFILE_ROLE = {
    "Version": "2008-10-17",
    "Statement": [{
        "Sid": "",
        "Effect": "Allow",
        "Principal": {
            "Service": "ec2.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
    }]
}

# the built-in, managed policy to attach to MRJOB_SERVICE_ROLE
EMR_SERVICE_ROLE_POLICY_ARN = (
    'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole')

# the built-in, managed policy to attach to MRJOB_INSTANCE_PROFILE_ROLE
EMR_INSTANCE_PROFILE_POLICY_ARN = (
    'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role')

# if we can't create or find our own service role, use the one
# created by the AWS console and CLI
FALLBACK_SERVICE_ROLE = 'EMR_DefaultRole'

# if we can't create or find our own instance profile, use the one
# created by the AWS console and CLI
FALLBACK_INSTANCE_PROFILE = 'EMR_EC2_DefaultRole'


log = getLogger(__name__)


# Utilities for all API calls

def _unquote_json(quoted_json_document):
    """URI-decode and then JSON-decode the given document."""
    json_document = unquote(quoted_json_document)
    return json.loads(json_document)


def _unwrap_response(resp):
    """Get the actual result from an IAM API response."""
    for resp_key, resp_data in resp.items():
        if resp_key.endswith('_response'):
            for result_key, result in resp_data.items():
                if result_key.endswith('_result'):
                    return result

            return {}  # PutRolePolicy has no response, for example

    raise ValueError(resp)


def _get_response(conn, action, params, path='/', parent=None,
                  verb='POST', list_marker='Set', **kwargs):
    """Replacement for conn.get_response(...) that unwraps the response
    from its containing elements."""
    # verb was GET in earlier versions of boto; always default to POST
    wrapped_resp = conn.get_response(
        action, params, path='/', parent=parent, verb=verb,
        list_marker=list_marker, **kwargs)
    return _unwrap_response(wrapped_resp)


def _get_responses(conn, action, params, *args, **kwargs):
    """_get_reponse(), with pagination. Yields one or more unwrapped responses.
    """
    params = params.copy()

    while True:
        resp = _get_response(conn, action, params, *args, **kwargs)
        yield resp

        # go to next page, if any
        marker = resp.get('marker')
        if marker:
            params['Marker'] = marker
        else:
            return


# Auto-created roles/profiles

def get_or_create_mrjob_service_role(conn):
    """Look for a usable service role for EMR, and if there is none,
    create one."""

    for role_name, role_document in _yield_roles(conn):
        if role_document != MRJOB_SERVICE_ROLE:
            continue

        policy_arns = list(_yield_attached_role_policies(conn, role_name))
        if policy_arns == [EMR_SERVICE_ROLE_POLICY_ARN]:
            return role_name

    name = _create_mrjob_role_with_attached_policy(
        conn, MRJOB_SERVICE_ROLE, EMR_SERVICE_ROLE_POLICY_ARN)

    log.info('Auto-created service role %s' % name)

    return name


def get_or_create_mrjob_instance_profile(conn):
    """Look for a usable instance profile for EMR, and if there is none,
    create one."""

    for profile_name, role_name, role_document in (
            _yield_instance_profiles(conn)):

        if role_document != MRJOB_INSTANCE_PROFILE_ROLE:
            continue

        policy_arns = list(_yield_attached_role_policies(conn, role_name))
        if policy_arns == [EMR_INSTANCE_PROFILE_POLICY_ARN]:
            return role_name

    name = _create_mrjob_role_with_attached_policy(
        conn, MRJOB_INSTANCE_PROFILE_ROLE, EMR_INSTANCE_PROFILE_POLICY_ARN)

    # create an instance profile with the same name as the role
    _get_response(conn, 'CreateInstanceProfile', {'InstanceProfileName': name})

    _get_response(conn, 'AddRoleToInstanceProfile', {
        'InstanceProfileName': name, 'RoleName': name})

    log.info('Auto-created instance profile %s' % name)

    return name


def _yield_roles(conn):
    """Yield (role_name, role_document)."""
    resps = _get_responses(conn, 'ListRoles', {}, list_marker='Roles')

    for resp in resps:
        for role_data in resp['roles']:
            yield _get_role_name_and_document(role_data)


def _yield_instance_profiles(conn):
    """Yield (profile_name, role_name, role_document).

    role_name and role_document are None for empty instance profiles
    """
    resps = _get_responses(conn, 'ListInstanceProfiles', {},
                           list_marker='InstanceProfiles')

    for resp in resps:
        for profile_data in resp['instance_profiles']:
            profile_name = profile_data['instance_profile_name']

            if profile_data['roles']:
                # doesn't look like boto can handle two list markers, hence
                # the extra "member" layer
                role_data = profile_data['roles']['member']
                role_name, role_document = _get_role_name_and_document(
                    role_data)
            else:
                role_name = None
                role_document = None

            yield (profile_name, role_name, role_document)


def _yield_attached_role_policies(conn, role_name):
    """Yield the ARNs for policies attached to the given role."""
    # allowing for multiple responses might be overkill, as currently
    # (2015-05-29) only two policies are allowed per role.
    resps = _get_responses(conn, 'ListAttachedRolePolicies',
                           {'RoleName': role_name},
                           list_marker='AttachedPolicies')

    for resp in resps:
        for policy_data in resp['attached_policies']:
            yield policy_data['policy_arn']


def _get_role_name_and_document(role_data):
    role_name = role_data['role_name']
    role_document = _unquote_json(role_data['assume_role_policy_document'])

    return (role_name, role_document)


def _create_mrjob_role_with_attached_policy(conn, role_document, policy_arn):
    # create role
    role_name = 'mrjob-' + random_identifier()

    _get_response(conn, 'CreateRole', {
        'AssumeRolePolicyDocument': json.dumps(role_document),
        'RoleName': role_name})

    _get_response(conn, 'AttachRolePolicy', {
        'PolicyArn': policy_arn,
        'RoleName': role_name})

    return role_name


# DEPRECATED STUFF

# all this is deprecated as of v0.4.5, to be removed in v0.5.0

# v0.4.4 used built-in role policies, but the built-in managed ones
# are a better idea. See #1026

# policy to add to MRJOB_SERVICE_ROLE
MRJOB_SERVICE_ROLE_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{
        "Action": [
            "ec2:AuthorizeSecurityGroupIngress",
            "ec2:CancelSpotInstanceRequests",
            "ec2:CreateSecurityGroup",
            "ec2:CreateTags",
            "ec2:Describe*",
            "ec2:DeleteTags",
            "ec2:ModifyImageAttribute",
            "ec2:ModifyInstanceAttribute",
            "ec2:RequestSpotInstances",
            "ec2:RunInstances",
            "ec2:TerminateInstances",
            "iam:PassRole",
            "iam:ListRolePolicies",
            "iam:GetRole",
            "iam:GetRolePolicy",
            "iam:ListInstanceProfiles",
            "s3:Get*",
            "s3:List*",
            "s3:CreateBucket",
            "sdb:BatchPutAttributes",
            "sdb:Select"
        ],
        "Effect": "Allow",
        "Resource": "*"
    }]
}

# policy to attach to MRJOB_INSTANCE_PROFILE_ROLE
MRJOB_INSTANCE_PROFILE_POLICY = {
    "Statement": [{
        "Action": [
            "cloudwatch:*",
            "dynamodb:*",
            "ec2:Describe*",
            "elasticmapreduce:Describe*",
            "rds:Describe*",
            "s3:*",
            "sdb:*",
            "sns:*",
            "sqs:*"
        ],
        "Effect": "Allow",
        "Resource": ["*"]
    }]
}


def yield_roles_with_policies(conn, path=None):
    """Yield (role_name, (role_document, [policy_documents])).

    path is an exact path to match.
    """
    log.warning('yield_roles_with_policies() is deprecated'
                ' and will be removed in v0.5.0')

    # could support path_prefix here, but mrjob isn't using it, and EMR
    # role policies apparently have to have a path of / anyways

    resps = _get_responses(conn, 'ListRoles', {}, list_marker='Roles')

    for resp in resps:
        for role_data in resp['roles']:
            if path and role_data['path'] != path:
                continue

            yield _get_role_with_policies(conn, role_data)


def yield_policies_for_role(conn, role_name):
    """Given a role name, yield (policy_name, policy_document)

    conn should be a boto.iam.IAMConnection
    """
    log.warning('yield_policies_for_role() is deprecated'
                ' and will be removed in v0.5.0')

    resps = _get_responses(conn,
                           'ListRolePolicies',
                           {'RoleName': role_name},
                           list_marker='PolicyNames')

    for resp in resps:
        policy_names = resp['policy_names']

        for policy_name in policy_names:
            resp = _get_response(conn,
                                 'GetRolePolicy',
                                 {'RoleName': role_name,
                                  'PolicyName': policy_name})

            policy = _unquote_json(resp['policy_document'])
            yield (policy_name, policy)


def yield_instance_profiles_with_policies(conn):
    """Yield (instance_proile_name, (role_document, [policy_documents])).

    This works just like yield_roles_with_policies(), except it
    gives the instance profile's name rather than the role name
    (instance profiles are just thin wrappers for roles).
    """
    log.warning('yield_instance_profiles_with_policies() is deprecated'
                ' and will be removed in v0.5.0')

    # could support path_prefix here, but mrjob isn't using it
    resps = _get_responses(conn, 'ListInstanceProfiles', {},
                           list_marker='InstanceProfiles')

    for resp in resps:
        for profile_data in resp['instance_profiles']:
            profile_name = profile_data['instance_profile_name']

            if profile_data['roles']:
                # doesn't look like boto can handle two list markers, hence
                # the extra "member" layer
                role_data = profile_data['roles']['member']
                _, role_with_policies = _get_role_with_policies(
                    conn, role_data)
            else:
                role_with_policies = None

            yield (profile_name, role_with_policies)


def _get_role_with_policies(conn, role_data):
    """Returns (role_name, (role, policies))."""
    role_name = role_data['role_name']
    role = _unquote_json(role_data['assume_role_policy_document'])

    policies = [policy for policy_name, policy in
                yield_policies_for_role(conn, role_name)]

    return (role_name, (role, policies))


def role_with_policies_matches(rp1, rp2):
    log.warning('role_with_policies_matches() is deprecated'
                ' and will be removed in v0.5.0')

    role1, policies1 = rp1
    role2, policies2 = rp2

    # JSON-encode to allow sorting in Python 3
    def dumps(x):
        return json.dumps(x, sort_keys=True)

    # Could make this a bit more lenient. For example, it doesn't look
    # like the ordering of lists in any of these documents matters. For
    # now, I'm just making mrjob's defaults the same as the ones created
    # by awscli
    return (role1 == role2 and
            sorted(dumps(p1) for p1 in policies1) ==
            sorted(dumps(p2) for p2 in policies2))


def _create_mrjob_role_with_policies(conn, role, policies):
    # create role
    role_name = 'mrjob-' + random_identifier()

    _get_response(conn, 'CreateRole', {
        'AssumeRolePolicyDocument': json.dumps(role),
        'RoleName': role_name})

    for i, policy in enumerate(policies):
        # each policy needs a unique name
        if len(policies) == 1:
            policy_name = role_name
        else:
            policy_name = '%s-%d' % (role_name, i)

        _get_response(conn, 'PutRolePolicy', {
            'PolicyDocument': json.dumps(policy),
            'PolicyName': policy_name,
            'RoleName': role_name})

    return role_name
