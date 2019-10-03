# -*- coding: utf-8 -*-
# Copyright 2016-2017 Yelp
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
import socket
from ssl import SSLError

from botocore.exceptions import ClientError

from mrjob.aws import _AWS_MAX_TRIES
from mrjob.aws import _boto3_paginate
from mrjob.aws import _wrap_aws_client
from mrjob.aws import EC2_INSTANCE_TYPE_TO_COMPUTE_UNITS
from mrjob.aws import EC2_INSTANCE_TYPE_TO_MEMORY

from tests.mock_boto3 import MockBoto3TestCase
from tests.py2 import patch
from tests.sandbox import BasicTestCase


class EC2InstanceTypeTestCase(BasicTestCase):

    def test_ec2_instance_dicts_match(self):
        self.assertEqual(
            set(EC2_INSTANCE_TYPE_TO_COMPUTE_UNITS),
            set(EC2_INSTANCE_TYPE_TO_MEMORY))


class WrapAWSClientTestCase(MockBoto3TestCase):
    """Test that _wrap_aws_client() works as expected.

    We're going to wrap S3Client.list_buckets(), but it should work the
    same on any method on any AWS client/resource."""

    def setUp(self):
        super(WrapAWSClientTestCase, self).setUp()

        # don't actually wait between retries
        self.sleep = self.start(patch('time.sleep'))

        self.log = self.start(patch('mrjob.retry.log'))

        self.bucket_names = []

        self.list_buckets = self.start(patch(
            'tests.mock_boto3.s3.MockS3Client.list_buckets',
            side_effect=[dict(Buckets=self.bucket_names)]))

        self.client = self.client('s3')
        self.wrapped_client = _wrap_aws_client(self.client)

    def add_transient_error(self, ex):
        self.list_buckets.side_effect = (
            [ex] + list(self.list_buckets.side_effect))

    def test_unwrapped_no_errors(self):
        # just a sanity check of our mocks
        self.assertEqual(self.client.list_buckets(), dict(Buckets=[]))

    def test_unwrapped_with_errors(self):
        self.add_transient_error(socket.error(104, 'Connection reset by peer'))

        self.assertRaises(socket.error, self.client.list_buckets)

    def test_wrapped_no_errors(self):
        self.assertEqual(self.wrapped_client.list_buckets(), dict(Buckets=[]))

        self.assertFalse(self.log.info.called)

    def assert_retry(self, ex):
        self.add_transient_error(ex)
        self.assertEqual(self.wrapped_client.list_buckets(), dict(Buckets=[]))
        self.assertTrue(self.log.info.called)

    def assert_no_retry(self, ex):
        self.add_transient_error(ex)
        self.assertRaises(ex.__class__, self.wrapped_client.list_buckets)
        self.assertFalse(self.log.info.called)

    def test_socket_connection_reset_by_peer(self):
        self.assert_retry(socket.error(104, 'Connection reset by peer'))

    def test_socket_connection_timed_out(self):
        self.assert_retry(socket.error(110, 'Connection timed out'))

    def test_other_socket_errors(self):
        self.assert_no_retry(socket.error(111, 'Connection refused'))

    def test_ssl_read_op_timed_out_error(self):
        self.assert_retry(SSLError('The read operation timed out'))

    def test_ssl_write_op_timed_out_error(self):
        self.assert_retry(SSLError('The write operation timed out'))

    def test_other_ssl_error(self):
        self.assert_no_retry(SSLError('certificate verify failed'))

    def test_throttling_error(self):
        self.assert_retry(ClientError(
            dict(
                Error=dict(
                    Code='ThrottlingError'
                )
            ),
            'ListBuckets'))

    def test_aws_505_error(self):
        self.assert_retry(ClientError(
            dict(
                ResponseMetadata=dict(
                    HTTPStatusCode=505
                )
            ),
            'ListBuckets'))

    def test_other_client_error(self):
        self.assert_no_retry(ClientError(
            dict(
                Error=dict(
                    Code='AccessDenied',
                    Message='Access Denied',
                ),
                ResponseMetadata=dict(
                    HTTPStatusCode=403
                ),
            ),
            'GetBucketLocation'))

    def test_other_error(self):
        self.assert_no_retry(ValueError())

    def test_two_retriable_errors(self):
        self.add_transient_error(socket.error(110, 'Connection timed out'))
        self.add_transient_error(SSLError('The read operation timed out'))
        self.assertEqual(self.wrapped_client.list_buckets(), dict(Buckets=[]))
        self.assertTrue(self.log.info.called)

    def test_retriable_and_no_retriable_error(self):
        # errors are prepended to side effects
        # we want socket.error to happen first
        self.add_transient_error(ValueError())
        self.add_transient_error(socket.error(110, 'Connection timed out'))

        self.assertRaises(ValueError, self.wrapped_client.list_buckets)
        self.assertTrue(self.log.info.called)

    def test_eventually_give_up(self):
        for _ in range(_AWS_MAX_TRIES + 1):
            self.add_transient_error(socket.error(110, 'Connection timed out'))

        self.assertRaises(socket.error, self.wrapped_client.list_buckets)
        self.assertTrue(self.log.info.called)

    def test_min_backoff(self):
        self.wrapped_client = _wrap_aws_client(self.client, min_backoff=1000)

        self.add_transient_error(socket.error(110, 'Connection timed out'))

        self.assertEqual(self.wrapped_client.list_buckets(), dict(Buckets=[]))

        self.sleep.assert_called_with(1000)

        self.assertTrue(self.log.info.called)

    def test_pagination(self):
        self.add_transient_error(socket.error(110, 'Connection timed out'))

        self.bucket_names = ['walrus%02d' % i for i in range(100)]

        self.assertEqual(list(_boto3_paginate(
            'Buckets', self.wrapped_client, 'list_buckets')),
            self.bucket_names)
