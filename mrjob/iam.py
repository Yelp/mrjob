"""Utilities for dealing with AWS IAM.

The main purpose of this code is to create the needed IAM instance profiles,
roles, and policies to make EMR work with a new account.

This works somewhat similarly to the AWS CLI (aws emr create-default-roles),
except they are assigned random mrjob-* names.

There isn't really versioning; mrjob will simply check if there are IAM objects
idential to the ones it needs before attempting to create them.
"""

# recommended IAM roles and policies for EMR, from:
# http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-iam-roles-defaultroles.html
#
# These didn't work as-is; had to change "Resource": "*" to "Resource": ["*"]
import json
from urllib import quote
from urllib import unquote

from mrjob.aws import random_identifier

# where to store roles created by mrjob
MRJOB_SERVICE_ROLE_PATH = '/mrjob/service_role/'
MRJOB_INSTANCE_PROFILE_PATH = '/mrjob/instance_profile/'

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

# policy to add to MRJOB_SERVICE_ROLE
MRJOB_SERVICE_ROLE_POLICY = {
    "Statement": [{
        "Action": [
            "ec2:AuthorizeSecurityGroupIngress",
            "ec2:CancelSpotInstanceRequests",
            "ec2:CreateSecurityGroup",
            "ec2:CreateTags",
            "ec2:DeleteTags",
            "ec2:Describe*",
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
            "s3:CreateBucket",
            "s3:List*",
            "sdb:BatchPutAttributes",
            "sdb:Select"
        ],
    "Effect": "Allow",
    "Resource": ["*"]
    }]
}

# Role to wrap in an instance profile
MRJOB_INSTANCE_PROFILE_ROLE = {
  "Version": "2008-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}

# policies to attach to MRJOB_EMR_EC2_ROLE
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


# TODO: handle paginated results

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


def _get_response(conn, action, params, *args, **kwargs):
    """Replacement for conn.get_response(...) that unwraps the response
    from its containing elements."""
    wrapped_resp = conn.get_response(action, params, *args, **kwargs)
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


def yield_instance_profiles_with_policies(conn, path_prefix=None):
    """Yield (instance_proile_name, (role_document, [policy_documents])).

    This works just like yield_roles_with_policies(), except it
    gives the instance profile's name rather than the role name
    (instance profiles are just thin wrappers for roles).
    """
    params = {}
    if path_prefix is not None:
        params['PathPrefix'] = path_prefix
    resps = _get_responses(conn, 'ListInstanceProfiles', params,
                           list_marker='InstanceProfiles')

    for resp in resps:
        for profile_data in resp['instance_profiles']:
            profile_name = profile_data['instance_profile_name']
            # doesn't look like boto can handle two list markers, hence
            # the extra "member" layer
            role_data = profile_data['roles']['member']

            _, role_with_policies = _get_role_with_policies(conn, role_data)

            yield (profile_name, role_with_policies)


def yield_roles_with_policies(conn, path_prefix=None):
    """Yield (role_name, (role_document, [policy_documents]))."""
    params = {}
    if path_prefix is not None:
        params['PathPrefix'] = path_prefix
    resps = _get_responses(conn, 'ListRoles', params, list_marker='Roles')

    for resp in resps:
        for role_data in resp['roles']:
            yield _get_role_with_policies(conn, role_data)



def _get_role_with_policies(conn, role_data):
    """Returns (role_name, (role, policies))."""
    role_name = role_data['role_name']
    role = _unquote_json(role_data['assume_role_policy_document'])

    policies = [policy for policy_name, policy in
                yield_policies_for_role(conn, role_name)]

    return (role_name, (role, policies))



def yield_policies_for_role(conn, role_name):
    """Given a role name, yield (policy_name, policy_document)

    conn should be a boto.iam.IAMConnection
    """
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


def role_with_policies_matches(rp1, rp2):
    role1, policies1 = rp1
    role2, policies2 = rp2

    # JSON-encode to allow sorting in Python 3
    def dumps(x):
        json.dumps(x, sort_keys=True)

    return (role1 == role2 and
            sorted(dumps(p) for p in policies1) ==
            sorted(dumps(p) for p in policies2))


def get_or_create_mrjob_service_role(conn):
    target_role_with_policies = (
        MRJOB_SERVICE_ROLE, [MRJOB_SERVICE_ROLE_POLICY])

    for role_name, role_with_policies in (
            yield_roles_with_policies(
                conn, path_prefix=MRJOB_SERVICE_ROLE_PATH)):

        if role_with_policies_matches(role_with_policies,
                                     target_role_with_policies):
            return role_name

    return _create_mrjob_role_with_policies(
        conn, MRJOB_SERVICE_ROLE_PATH, *target_role_with_policies)


def get_or_create_mrjob_instance_profile(conn):
    target_role_with_policies = (
        MRJOB_INSTANCE_PROFILE_ROLE, [MRJOB_INSTANCE_PROFILE_POLICY])

    for profile_name, role_with_policies in (
            yield_roles_with_policies(
                conn, path_prefix=MRJOB_INSTANCE_PROFILE_PATH)):

        if role_with_policies_matches(role_with_policies,
                                     target_role_with_policies):
            return profile_name

    # role and instance profile will have same randomly generated name
    name = _create_mrjob_role_with_policies(
        conn, MRJOB_INSTANCE_PROFILE_PATH, *target_role_with_policies)

    # create an instance profile with the same name as the role
    _get_response(conn, 'CreateInstanceProfile', {
        'InstanceProfileName': name,
        'Path': MRJOB_INSTANCE_PROFILE_PATH})

    _get_response(conn, 'AddRoleToInstanceProfile', {
        'InstanceProfileName': name, 'RoleName': name})

    return profile_name


def _create_mrjob_role_with_policies(conn, path, role, policies):
    # create role
    role_name = 'mrjob-' + random_identifier()

    resp = _get_response(conn, 'CreateRole', {
        'AssumeRolePolicyDocument': json.dumps(role),
        'Path': path,
        'RoleName': role_name})

    for i, policy in enumerate(policies):
        # each policy needs a unique name
        if len(policies) == 1:
            policy_name = role_name
        else:
            policy_name = '%s-%d' % (role_name, i)

        resp = _get_response(conn, 'PutRolePolicy', {
            'PolicyDocument': json.dumps(policy),
            'PolicyName': policy_name,
            'RoleName': role_name})

    return role_name
