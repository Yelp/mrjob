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

from mrjob.py2 import unquote
from mrjob.util import random_identifier

# Working IAM roles and policies for EMR; these should be identical
# to the ones created by AWS CLI (`aws emr create-default-roles`), at least as
# of 2015-04-20.
#
# AWS has recommended roles in their documentation, but they don't quite
# work as-is:
#
# http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-iam-roles-defaultroles.html  # noqa

# use this for service_role
_MRJOB_SERVICE_ROLE = {
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
_MRJOB_INSTANCE_PROFILE_ROLE = {
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

# the built-in, managed policy to attach to _MRJOB_SERVICE_ROLE
_EMR_SERVICE_ROLE_POLICY_ARN = (
    'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole')

# the built-in, managed policy to attach to _MRJOB_INSTANCE_PROFILE_ROLE
_EMR_INSTANCE_PROFILE_POLICY_ARN = (
    'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role')

# if we can't create or find our own service role, use the one
# created by the AWS console and CLI
_FALLBACK_SERVICE_ROLE = 'EMR_DefaultRole'

# if we can't create or find our own instance profile, use the one
# created by the AWS console and CLI
_FALLBACK_INSTANCE_PROFILE = 'EMR_EC2_DefaultRole'


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


def _repeat(api_call, *args, **kwargs):
    """Make the same API call repeatedly until we've seen every page
    of the response (sets *marker* automatically).

    Yields one or more unwrapped responses.
    """
    marker = None

    while True:
        raw_resp = api_call(*args, marker=marker, **kwargs)
        resp = _unwrap_response(raw_resp)
        yield resp

        # go to next page, if any
        marker = resp.get('marker')
        if not marker:
            return


# Auto-created roles/profiles

def get_or_create_mrjob_service_role(conn):
    """Look for a usable service role for EMR, and if there is none,
    create one."""

    for role_name, role_document in _yield_roles(conn):
        if role_document != _MRJOB_SERVICE_ROLE:
            continue

        policy_arns = list(_yield_attached_role_policies(conn, role_name))
        if policy_arns == [_EMR_SERVICE_ROLE_POLICY_ARN]:
            return role_name

    name = _create_mrjob_role_with_attached_policy(
        conn, _MRJOB_SERVICE_ROLE, _EMR_SERVICE_ROLE_POLICY_ARN)

    log.info('Auto-created service role %s' % name)

    return name


def get_or_create_mrjob_instance_profile(conn):
    """Look for a usable instance profile for EMR, and if there is none,
    create one."""
    for profile_name, role_name, role_document in (
            _yield_instance_profiles(conn)):

        if role_document != _MRJOB_INSTANCE_PROFILE_ROLE:
            continue

        policy_arns = list(_yield_attached_role_policies(conn, role_name))
        if policy_arns == [_EMR_INSTANCE_PROFILE_POLICY_ARN]:
            return role_name

    name = _create_mrjob_role_with_attached_policy(
        conn, _MRJOB_INSTANCE_PROFILE_ROLE, _EMR_INSTANCE_PROFILE_POLICY_ARN)

    # create an instance profile with the same name as the role
    conn.create_instance_profile(name)
    conn.add_role_to_instance_profile(name, name)

    log.info('Auto-created instance profile %s' % name)

    return name


def _yield_roles(conn):
    """Yield (role_name, role_document)."""
    for resp in _repeat(conn.list_roles):
        for role_data in resp['roles']:
            yield _get_role_name_and_document(role_data)


def _yield_instance_profiles(conn):
    """Yield (profile_name, role_name, role_document).

    role_name and role_document are None for empty instance profiles
    """
    for resp in _repeat(conn.list_instance_profiles):
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
    for resp in _repeat(_list_attached_role_policies, conn, role_name):
        for policy_data in resp['attached_policies']:
            yield policy_data['policy_arn']


def _get_role_name_and_document(role_data):
    role_name = role_data['role_name']
    role_document = _unquote_json(role_data['assume_role_policy_document'])

    return (role_name, role_document)


def _create_mrjob_role_with_attached_policy(conn, role_document, policy_arn):
    # create role
    role_name = 'mrjob-' + random_identifier()

    conn.create_role(role_name, json.dumps(role_document))
    _attach_role_policy(conn, role_name, policy_arn)

    return role_name


# methods which should eventually be added to boto's IAMConnection

def _list_attached_role_policies(conn, role_name, marker=None, max_items=None):
    params = {'RoleName': role_name}
    if marker is not None:
        params['Marker'] = marker
    if max_items is not None:
        params['MaxItems'] = max_items
    return conn.get_response('ListAttachedRolePolicies', params,
                             list_marker='AttachedPolicies')


def _attach_role_policy(conn, role_name, policy_arn):
    params = {'PolicyArn': policy_arn,
              'RoleName': role_name}

    return conn.get_response('AttachRolePolicy', params)
