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


# TODO: move this to top level of tests? Or rework as MockClientMeta?
class MockObject(object):
    """A generic object that you can set any attribute on."""

    def __init__(self, **kwargs):
        """Intialize with the given attributes, ignoring fields set to None."""
        for key, value in kwargs.items():
            if value is not None:
                setattr(self, key, value)

    def __setattr__(self, key, value):
        if isinstance(value, bytes):
            value = value.decode('utf_8')

        self.__dict__[key] = value

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        if len(self.__dict__) != len(other.__dict__):
            return False

        for k, v in self.__dict__.items():
            if not k in other.__dict__:
                return False
            else:
                if v != other.__dict__[k]:
                    return False

        return True

    # useful for hand-debugging tests
    def __repr__(self):
        return('%s.%s(%s)' % (
            self.__class__.__module__,
            self.__class__.__name__,
            ', '.join('%s=%r' % (k, v)
                      for k, v in sorted(self.__dict__.items()))))


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
