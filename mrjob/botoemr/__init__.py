# Copyright (c) 2010 Spotify AB
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.

"""
This module provies an interface to the Elastic MapReduce (EMR)
service from AWS.
"""
from connection import EmrConnection
from step import Step, StreamingStep, JarStep
from bootstrap_action import BootstrapAction

from boto import handler
from boto.s3.bucket import Bucket
from boto.resultset import ResultSet
import xml.sax

def get_bucket_location(bucket):
    """
    Returns the LocationConstraint for the bucket.

    :rtype: str
    :return: The LocationConstraint for the bucket or the empty
             string if no constraint was specified when bucket
             was created.
    """
    response = bucket.connection.make_request('GET', bucket.name,
                                            query_args='location')
    body = response.read()
    if response.status == 200:
        rs = ResultSet(bucket)
        h = handler.XmlHandler(rs, bucket)
        xml.sax.parseString(body, h)
        return rs.LocationConstraint
    else:
        raise bucket.connection.provider.storage_response_error(
            response.status, response.reason, body)

if not hasattr(Bucket, 'get_location'):
    Bucket.get_location = get_bucket_location

