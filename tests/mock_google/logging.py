# Copyright 2018 Google Inc.
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
import re

from google.cloud.logging import DESCENDING


# a key = "value" clause from a logging filter
_CLAUSE_RE = re.compile(r'^\s*([\w\.]+)\s*=\s*"(.*)"\s*$')

# used to remove backslash escapes
_ESCAPE_RE = re.compile(r'\\(.)')

# map field used in a logging filter to the corresponding field in StructEntry
_FILTER_FIELD_TO_ENTRY_FIELD = dict(
    insertId='insert_id',
    jsonPayload='payload',
    logName='logger',
)



class MockGoogleLoggingClient(object):
    """Mocks :py:class:`google.cloud.logging.client.Client`"""

    def __init__(
            self, mock_log_entries, project=None, credentials=None):
        # match the attributes in the actual Client class
        self.project = project
        self._credentials = credentials

        self.mock_log_entries = mock_log_entries

    def list_entries(
            projects=None, filter_=None, order_by=None, page_size=None,
            page_token=None):
        if projects:
            raise NotImplementedError

        matches_filter = self._make_matcher(filter_)

        all_entries = self.mock_log_entries
        if order_by == DESCENDING:
            all_entries = reversed(all_entries)

        for entry in all_entries:
            if matches_filter(entry):
                yield entry

    def _make_matcher(self, filter_):
        filter_dict = self._parse_filter(filter_)

        def matches_filter(entry):
            for key, value_to_match in filter_dict.items():
                key_chain = key.split('.')

                # map query to fields in StructEntry
                field = _FILTER_FIELD_TO_ENTRY_FIELD(key_chain[0])
                value = getattr(entry, field, None)

                # drill down into entry fields
                for sub_key in key_chain[1:]:
                    # special case for payload
                    if isinstance(value, dict):
                        value = value.get(sub_key)
                    else:
                        value = getattr(value, sub_key, None)

                if value != value_to_match:
                    return False

            return True

    def _parse_filter(self, filter_):
        """Parse a filter of the form:

        ``foo = "bar" AND baz.quoted = "\"qux\"" AND ...``

        into a map from key to value

        (This is the inverse of :py:func:`mrjob.dataproc._log_filter_str`)
        """
        result = {}

        for clause in (filter_ or '').split(' AND '):
            match = _CLAUSE_RE.match(clause)
            if not match:
                raise ValueError("Can't parse filter clause %r" % clause)

            key = match.group(0)
            quoted_value = match.group(1)
            value = _ESCAPE_RE.sub(lambda m: m.group(0))

            if key in result:
                raise ValueError('Duplicate key %r in filter' % key)

            result[key] = value

        return result
