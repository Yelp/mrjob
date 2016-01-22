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
"""Utility methods for dealing with counters (not including parsers)."""

def _pick_counters(log_interpretation):
    """Pick counters from a dictionary possibly containing
    step and history interpretations."""
    for log_type in 'step', 'history':
        counters = log_interpretation.get(log_type, {}).get('counters')
        if counters:
            return counters
    else:
        return {}


def _sum_counters(*counters_list):
    """Combine many maps from group to counter to amount."""
    result = {}

    for counters in counters_list:
        for group, counter_to_amount in counters.items():
            for counter, amount in counter_to_amount.items():
                result.setdefault(group, {})
                result[group].setdefault(counter, 0)
                result[group][counter] += amount

    return result
