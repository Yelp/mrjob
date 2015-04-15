"""Utilities for dealing with AWS IAM, mostly creating roles."""

# recommended IAM roles and policies for EMR, from:
# http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-iam-roles-defaultroles.html
import json
from urllib import quote
from urllib import unquote

# where to store roles created by mrjob
MRJOB_EMR_ROLE_PATH = '/mrjob/emr/'
MRJOB_EMR_EC2_ROLE_PATH = '/mrjob/ec2/'

# use this for service_role
MRJOB_EMR_ROLE = {
    "Version": "2008-10-17",
    "Statement": [{
        "Sid": "",
        "Effect": "Allow",
        "Principal": {
            "Service": ["elasticmapreduce.amazonaws.com"]
        },
        "Action": "sts:AssumeRole"
    }]
}

# policy to add to MRJOB_EMR_ROLE
MRJOB_EMR_ROLE_POLICY = {
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
    "Resource": "*"
    }]
}

# use this for job_flow_role
MRJOB_EMR_EC2_ROLE = {
  "Version": "2008-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "ec2.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}

# policies to attach to MRJOB_EMR_EC2_ROLE
MRJOB_EMR_EC2_ROLE_POLICY = {
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
        "Resource": "*"
    }]
}


# TODO: handle paginated results

def _unquote_json(quoted_json_document):
    """URI-decode and then JSON-decode the given document."""
    json_document = unquote(quoted_json_document)
    return json.loads(json_document)

def _quote_json(document):
    """JSON-encode and then URI-encode the given document."""
    json_document = json.dumps(document)
    return quote(json_document)

def _get_result(resp):
    """Get the actual result from an IAM API response."""
    for response_key, response in resp.items():
        if response_key.endswith('_response'):
            for result_key, result in response.items():
                if result_key.endswith('_result'):
                    return result

    raise ValueError


def get_policies_for_role(conn, role_name):
    """Given a role name, return a list of policy documents.

    conn should be a boto.iam.IAMConnection
    """
    # just using raw IAMConnection.get_response(); the role stuff didn't exist
    # in boto 2.2.0, and newer boto adds very little value (you still have to
    # unpack the response like we do below).
    resp = conn.get_response('ListRolePolicies',
                             {'RoleName': role_name},
                             list_marker='PolicyNames')

    policy_names = _get_result(resp)['policy_names']

    policies = []

    for policy_name in policy_names:
        resp = conn.get_response('GetRolePolicy',
                                 {'RoleName': role_name,
                                  'PolicyName': policy_name})

        policy = _unquote_json(_get_result(resp)['policy_document'])

        policies.append(policy)

    return policies
