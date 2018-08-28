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
"""Mock boto3 EC2 support."""
from copy import deepcopy

from botocore.exceptions import ParamValidationError

from mrjob.aws import _DEFAULT_AWS_REGION

from .util import MockClientMeta


class MockEC2Client(object):
    """Mock out boto3 EC2 client

    :param mock_ec2_images: A list of image dictionaries to be returned
                            by :py:meth:`describe_images`
    """
    def __init__(self,
                 mock_ec2_images,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 aws_session_token=None,
                 endpoint_url=None,
                 region_name=None):

        self.mock_ec2_images = mock_ec2_images

        region_name = region_name or _DEFAULT_AWS_REGION
        if not endpoint_url:
            endpoint_url = 'https://ec2.%s.amazonaws.com' % region_name

        self.meta = MockClientMeta(
            endpoint_url=endpoint_url,
            region_name=region_name)

    def describe_images(self, Filters=None, Owners=None):
        images = []

        for image in self.mock_ec2_images:
            if not (Owners is None or image.get('ImageOwnerAlias') in Owners):
                continue

            if Filters:
                for Filter in Filters:
                    if set(Filter) != {'Name', 'Values'}:
                        raise ParamValidationError(
                            report='Unknown parameter in Filters')

                    field = _hyphen_to_camel(Filter['Name'])
                    if not image.get(field) in Filter['Values']:
                        continue

            images.append(deepcopy(image))

        return dict(Images=images)


def _hyphen_to_camel(s):
    """Convert a string like ``root-device-type`` to ``RootDeviceType``"""
    return ''.join(part[0].upper() + part[1:] for part in s.split('-'))
