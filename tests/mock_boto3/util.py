# Copyright 2015-2017 Yelp and Contributors
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
"""Common utilities for mock boto3 support"""
from mrjob.conf import combine_dicts


class MockClientMeta(object):
    """Mock out the *meta* field for various boto3 objects."""

    def __init__(self, client=None, endpoint_url=None, region_name=None):
        self.client = client
        self.endpoint_url = endpoint_url
        self.region_name = region_name


class MockPaginator(object):
    """Mock botocore paginators.

    Rather than mocking pagination, markers, etc. in every mock API call,
    we have our API calls return the full results, and make our paginators
    break them into pages.
    """
    def __init__(self, method, result_key, page_size):
        self.result_key = result_key
        self.method = method
        self.page_size = page_size

    def paginate(self, **kwargs):
        result = self.method(**kwargs)

        values = result[self.result_key]

        for page_start in range(0, len(values), self.page_size):
            page = values[page_start:page_start + self.page_size]
            yield combine_dicts(result, {self.result_key: page})
