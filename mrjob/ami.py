# -*- coding: utf-8 -*-
# Copyright 2018 Yelp
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
"""Utilities for creating custom AMIs."""
import logging
import re


log = logging.getLogger(__name__)

# used to match the name of AMIs that we want to use on EMR
_EMR_BASE_AMI_NAME_RE = re.compile(r'^amzn-ami-hvm-[\d\.]*-x86_64-ebs$')

# default instance type for building AMI images
_DEFAULT_AMI_INSTANCE_TYPE = 'm1.medium'



def _make_custom_emr_ami_image(ec2_client, sh_content, **params):
    """Make a custom AMI image. This will launch an EC2 image that runs
    commands and then shuts itself down.

    :param ec2_client: a boto3 EC2 client
    :param sh_content: a list of lines for a shell script
    :param params: a dictionary of request parameters for the RunInstances
                   API. Some that are most relevant:

    :param ImageId: the base AMI (defaults to latest Amazon Linux)
    :param InstanceType: what instance to build the AMI on (defaults to
                         ``m1.medium``)
    :param KeyName: name of SSH key to use (default is None, i.e. no SSH)
    :param Placement: availablility zone etc. (default is None)
    :param SubnetId: subnet to launch in (default is None)
    """
    pass

#if not base_ami_id:
#        log.info('looking up latest Amazon Linux AMI...')
#        image = get_latest_emr_base_ami(ec2_client)
#        log.info('  found %s (%s)' % (image['ImageId'], image['Description']))

#    if not instance_type:
#        instance_type = _DEFAULT_AMI_INSTANCE_TYPE

def _get_latest_emr_base_ami(ec2_client):
    """Fetch the latest Amazon Linux AMI image that's usable as a base
    image for EMR. This can take several seconds.

    For the sake of consistency, we have somewhat stricter requirements
    than `the AWS documentation <https://docs.aws.amazon.com/emr/latest/\
    ManagementGuide/emr-custom-ami.html#emr-custom-ami-considerations>`_.
    Specifically:

    * Amazon Linux (not Amazon Linux 2)
    * HVM virtualization
    * x86_64 architecture
    * single EBS volume
      * standard volume type (not GP2)
    * stable version (no "testing" or "rc", only numbers and dots)

    This returns the entire dictionary representing the image. The
    *ImageId* field contains the AMI ID, and *Description* contains
    a human-readable description.
    """
    # DescribeImages' filtering is imperfect and slow, but this helps a bit
    images = ec2_client.describe_images(
        Owners=['amazon'],
        Filters=[
            dict(Name='architecture', Values=['x86_64']),
            dict(Name='virtualization-type',Values=['hvm']),
            dict(Name='root-device-type',Values=['ebs']),
        ],
    )['Images']

    # perform further filtering by name
    images = [img for img in images
              if _EMR_BASE_AMI_NAME_RE.match(img['Name'])]

    # sort by creation datetime
    images.sort(key=lambda img: img['CreationDate'])

    return images[-1]
