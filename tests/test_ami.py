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
"""Tests for AMI utilities"""
from mrjob.ami import describe_base_emr_images

from tests.mock_boto3 import MockBoto3TestCase


class DescribeBaseEMRImagesTestCase(MockBoto3TestCase):

    # a valid base EMR image. we can make variants of this for testing
    BASE_EMR_IMAGE = {
        'Architecture': 'x86_64',
        'BlockDeviceMappings': [
            {
                'DeviceName': '/dev/xvda',
                'Ebs': {
                    'DeleteOnTermination': True,
                    'Encrypted': False,
                    'SnapshotId': 'snap-0ceb5dfba7c0cbd4c',
                    'VolumeSize': 8,
                    'VolumeType': 'standard'
                }
            }
        ],
        'CreationDate': '2018-08-11T02:33:53.000Z',
        'Description': 'Amazon Linux AMI 2018.03.0.20180811 x86_64 HVM EBS',
        'EnaSupport': True,
        'Hypervisor': 'xen',
        'ImageId': 'ami-09c6e771',
        'ImageLocation': 'amazon/amzn-ami-hvm-2018.03.0.20180811-x86_64-ebs',
        'ImageOwnerAlias': 'amazon',
        'ImageType': 'machine',
        'Name': 'amzn-ami-hvm-2018.03.0.20180811-x86_64-ebs',
        'OwnerId': '137112412989',
        'Public': True,
        'RootDeviceName': '/dev/xvda',
        'RootDeviceType': 'ebs',
        'SriovNetSupport': 'simple',
        'State': 'available',
        'VirtualizationType': 'hvm',
    }

    def make_image(self, **kwargs):
        """Return a copy of BASE_EMR_IMAGE with the given fields added.

        You can blank out fields by setting them to None."""
        image = dict(self.BASE_EMR_IMAGE, **kwargs)

        return {k: v for k, v in image.items() if v is not None}

    def test_no_images(self):
        self.assertEqual(describe_base_emr_images(self.client('ec2')), [])

    def test_base_emr_image(self):
        image = self.make_image()
        self.add_mock_ec2_image(image)

        self.assertEqual(describe_base_emr_images(self.client('ec2')),
                         [image])

    def test_most_recent_image_first(self):
        image_old = self.make_image(ImageId='ami-old',
                                    CreationDate='2010-06-06T00:00:00.000Z')
        image_new = self.make_image(ImageId='ami-new',
                                    CreationDate='2015-05-06T00:00:00.000Z')

        self.add_mock_ec2_image(image_old)
        self.add_mock_ec2_image(image_new)

        self.assertEqual(describe_base_emr_images(self.client('ec2')),
                         [image_new, image_old])

    def test_filter_and_sort(self):
        image_old = self.make_image(ImageId='ami-old',
                                    CreationDate='2010-06-06T00:00:00.000Z')
        image_new = self.make_image(ImageId='ami-new',
                                    CreationDate='2015-05-06T00:00:00.000Z')
        image_null = {}

        self.add_mock_ec2_image(image_null)
        self.add_mock_ec2_image(image_old)
        self.add_mock_ec2_image(image_null)
        self.add_mock_ec2_image(image_new)
        self.add_mock_ec2_image(image_null)

        self.assertEqual(describe_base_emr_images(self.client('ec2')),
                         [image_new, image_old])

    def assert_rejects_image(self, **kwargs):
        image = self.make_image(**kwargs)
        self.add_mock_ec2_image(image)

        self.assertNotIn(image, describe_base_emr_images(self.client('ec2')))

    def test_owner_must_be_amazon(self):
        self.assert_rejects_image(ImageOwnerAlias='aws-marketplace',
                                  OwnerId='679593333241')

    def test_architecture_must_be_x86_64(self):
        self.assert_rejects_image(Architecture='i386')

    def test_root_device_type_must_be_ebs(self):
        self.assert_rejects_image(RootDeviceType='instance-store')

    def test_virtualization_type_must_be_hvm(self):
        self.assert_rejects_image(VirtualizationType='paravirtual')

    def test_amazon_linux_1_only(self):
        self.assert_rejects_image(
            Name='amzn2-ami-hvm-2017.12.0.20180109-x86_64-ebs')

    def test_stable_amazon_linux_versions_only(self):
        # no "testing" or "rc," only dots and numbers, please
        self.assert_rejects_image(
            Name='amzn-ami-hvm-2017.03.rc-1.20170327-x86_64-ebs')

    def test_one_volume_only(self):
        self.assert_rejects_image(
            BlockDeviceMappings=[
                self.BASE_EMR_IMAGE['BlockDeviceMappings'][0],
                {
                    'DeviceName': 'xvdca',
                    'VirtualName': 'ephemeral0',
                }
            ]
        )

    def test_dont_crash_on_missing_name(self):
        # shouldn't happen in practice, but just in case
        self.assert_rejects_image(Name=None)

    def test_dont_crash_on_missing_block_device_mappings(self):
        # shouldn't happen in practice, but just in case
        self.assert_rejects_image(BlockDeviceMappings=None)

    def test_dont_crash_on_missing_creation_date(self):
        self.assert_rejects_image(CreationDate=None)
