# -*- coding: utf-8 -*-
# Copyright 2013 Lyft
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
"""General information about Amazon Web Services, such as region-to-endpoint
mappings.
"""
from __future__ import with_statement

### EC2 Instances ###

# map from instance type to number of compute units
# from http://aws.amazon.com/ec2/instance-types/
EC2_INSTANCE_TYPE_TO_COMPUTE_UNITS = {
    't1.micro': 2,
    'm1.small': 1,
    'm1.large': 4,
    'm1.xlarge': 8,
    'm2.xlarge': 6.5,
    'm2.2xlarge': 13,
    'm2.4xlarge': 26,
    'c1.medium': 5,
    'c1.xlarge': 20,
    'cc1.4xlarge': 33.5,
    'cg1.4xlarge': 33.5,
}

# map from instance type to GB of memory
# from http://aws.amazon.com/ec2/instance-types/
EC2_INSTANCE_TYPE_TO_MEMORY = {
    't1.micro': 0.6,
    'm1.small': 1.7,
    'm1.large': 7.5,
    'm1.xlarge': 15,
    'm2.xlarge': 17.5,
    'm2.2xlarge': 34.2,
    'm2.4xlarge': 68.4,
    'c1.medium': 1.7,
    'c1.xlarge': 7,
    'cc1.4xlarge': 23,
    'cg1.4xlarge': 22,
}


### EMR ###

# EMR's hard limit on number of steps in a job flow
MAX_STEPS_PER_JOB_FLOW = 256


### Regions ###

# Based on http://docs.aws.amazon.com/general/latest/gr/rande.html

# See Issue #658 for why we don't just let boto handle this.


# where to connect to EMR. The docs say
# elasticmapreduce.<region>.amazonaws.com, but the SSL certificates,
# they tell a different story. See Issue #621.

# where the AWS docs say to connect to EMR
_EMR_REGION_ENDPOINT = 'elasticmapreduce.%(region)s.amazonaws.com'
# the host that currently works with EMR's SSL certificate
_EMR_REGION_SSL_HOST = '%(region)s.elasticmapreduce.amazonaws.com'
# the regionless endpoint doesn't have SSL issues
_EMR_REGIONLESS_ENDPOINT = 'elasticmapreduce.amazonaws.com'

# where to connect to S3
_S3_REGION_ENDPOINT = 's3-%(region)s.amazonaws.com'
_S3_REGIONLESS_ENDPOINT = 's3.amazonaws.com'

# us-east-1 doesn't have its own endpoint or need bucket location constraints
_S3_REGIONS_WITH_NO_LOCATION_CONSTRAINT = ['us-east-1']


# "EU" is an alias for the eu-west-1 region
_ALIAS_TO_REGION = {
    'eu': 'eu-west-1',
}

# The region to assume if none is specified
_DEFAULT_REGION = 'us-east-1'


def _fix_region(region):
    """Convert "EU" to "eu-west-1", None to '', and convert to lowercase."""
    region = (region or '').lower()
    return _ALIAS_TO_REGION.get(region) or region


def emr_endpoint_for_region(region):
    """Get the host for Elastic MapReduce in the given AWS region."""
    region = _fix_region(region)

    if not region:
        return _EMR_REGIONLESS_ENDPOINT
    else:
        return _EMR_REGION_ENDPOINT % {'region': region}


def emr_ssl_host_for_region(region):
    """Get the host for Elastic MapReduce that matches their SSL cert
    for the given region. (See Issue #621.)"""
    region = _fix_region(region)

    if not region:
        return _EMR_REGIONLESS_ENDPOINT
    else:
        return _EMR_REGION_SSL_HOST % {'region': region}


def s3_endpoint_for_region(region):
    """Get the host for S3 in the given AWS region."""
    region = _fix_region(region)

    if not region or region in _S3_REGIONS_WITH_NO_LOCATION_CONSTRAINT:
        return _S3_REGIONLESS_ENDPOINT
    else:
        return _S3_REGION_ENDPOINT % {'region': region}


def s3_location_constraint_for_region(region):
    """Get the location constraint an S3 bucket needs so that other AWS
    services can connect to it in the given region."""
    region = _fix_region(region)

    if not region or region in _S3_REGIONS_WITH_NO_LOCATION_CONSTRAINT:
        return ''
    else:
        return region
