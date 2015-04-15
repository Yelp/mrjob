"""Utilities for dealing with AWS IAM, mostly creating roles."""

# recommended IAM roles and policies for EMR, from:
# http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-iam-roles-defaultroles.html
import json
from urllib import unquote_plus


# where to store roles created by mrjob
MRJOB_EMR_ROLE_PATH = '/mrjob/emr/'
MRJOB_EMR_EC2_ROLE_PATH = '/mrjob/ec2/'

# use this for service_role
DEFAULT_EMR_ROLE = {
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

# policy to add to DEFAULT_EMR_ROLE
DEFAULT_EMR_ROLE_POLICY = {
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
DEFAULT_EMR_EC2_ROLE = {
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

# policies to attach to DEFAULT_EMR_EC2_ROLE
DEFAULT_EMR_EC2_ROLE_POLICY = {
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


def get_policies_for_role(conn, role_name):
    """Given a role name, return a map from role policy name to the (decoded)
    policy document.

    conn should be an boto.iam.IAMConnection
    """
    # just using raw IAMConnection.get_response(); the role stuff didn't exist
    # in boto 2.2.0, and newer boto adds very little value (you still have to
    # unpack the response like we do below).
    resp = conn.get_response('ListRolePolicies',
                             {'RoleName': role_name},
                             list_marker='PolicyNames')

    policy_names = resp['list_role_policies_response'][
            'list_role_policies_result']['policy_names']

    policies_by_name = {}

    for policy_name in policy_names:
        resp = conn.get_response('GetRolePolicy',
                                 {'RoleName': role_name,
                                  'PolicyName': policy_name})
        policy_document_quoted = resp['get_role_policy_response'][
            'get_role_policy_result']['policy_document']
        policy_document_json = unquote_plus(policy_document_quoted)
        policy_document = json.loads(policy_document_json)

        policies_by_name[policy_name] = policy_document

    return policies_by_name
