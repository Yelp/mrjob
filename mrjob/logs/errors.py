# -*- coding: utf-8 -*-
# Copyright 2015 Yelp and Contributors
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
"""Merging errors, picking the best one, and displaying it."""

from .ids import _time_sort_key
from mrjob.py2 import string_types


def _pick_error(log_interpretation):
    """Pick most recent error from a dictionary possibly containing
    step, history, and task interpretations. Returns None if there
    are no errors.
    """
    error_lists = [
        log_interpretation.get(log_type, {}).get('errors')
        for log_type in 'step', 'history', 'task'
    ]

    errors = _merge_and_sort_errors(error_lists)
    if errors:
        return errors[0]
    else:
        return None


def _merge_and_sort_errors(error_lists):
    """Merge errors from one or more lists of errors and then return
    them, sorted by recency.

    We allow None in place of an error list.
    """
    key_to_error = {}

    for errors in error_lists:
        if errors is None:
            continue

        # catch common interface errors
        if isinstance(errors, (dict, string_types)):
            raise TypeError

        for error in errors:
            key = _time_sort_key(error)
            key_to_error.setdefault(key, {})
            key_to_error[key].update(error)

    return [error for key, error in sorted(errors.items(), reverse=True)]
