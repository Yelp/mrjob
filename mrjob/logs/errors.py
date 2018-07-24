# -*- coding: utf-8 -*-
# Copyright 2015-2016 Yelp
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
import json

from .ids import _time_sort_key


def _pick_error(log_interpretation):
    """Pick most recent error from a dictionary possibly containing
    step, history, and task interpretations. Returns None if there
    are no errors.
    """
    def yield_errors():
        for log_type in ('step', 'history', 'task'):
            errors = log_interpretation.get(log_type, {}).get('errors')
            for error in errors or ():
                yield error

    errors = _merge_and_sort_errors(yield_errors())
    if errors:
        return errors[0]
    else:
        return None


def _merge_and_sort_errors(errors):
    """Merge errors from one or more lists of errors and then return
    them, sorted by recency.

    We allow None in place of an error list.
    """
    key_to_error = {}

    for error in errors:
        key = _time_sort_key(error)
        key_to_error.setdefault(key, {})
        key_to_error[key].update(error)

    # wrap sort key to prioritize task errors. See #1429
    def sort_key(key_and_error):
        key, error = key_and_error

        # key[0] is step number
        return (key[0], bool(error.get('task_error')), key[1:])

    return [error for _, error in
            sorted(key_to_error.items(), key=sort_key, reverse=True)]


def _format_error(error):
    """Return string to log/print explaining the given error."""
    # it's just sad if we error while trying to explain an error
    try:
        return _format_error_helper(error)
    except:
        return json.dumps(error, indent=2, sort_keys=True)


def _format_error_helper(error):
    """Return string to log/print explaining the given error."""
    result = ''

    hadoop_error = error.get('hadoop_error')
    if hadoop_error:
        result += hadoop_error.get('message', '')

        if hadoop_error.get('path'):
            result += '\n\n(from %s)' % _describe_source(hadoop_error)

    # for practical purposes, there's always a hadoop error with a message,
    # so don't worry too much about spacing.

    task_error = error.get('task_error')
    if task_error:
        if hadoop_error:
            result += '\n\ncaused by:\n\n%s' % (task_error.get('message', ''))
        else:
            result += task_error.get('message', '')

        if task_error.get('path'):
            result += '\n\n(from %s)' % _describe_source(task_error)

    split = error.get('split')
    if split and split.get('path'):
        result += '\n\nwhile reading input from %s' % _describe_source(split)

    return result


def _describe_source(d):
    """return either '<path>' or 'line N of <path>' or 'lines M-N of <path>'.
    """
    path = d.get('path') or ''

    if 'num_lines' in d and 'start_line' in d:
        if d['num_lines'] == 1:
            return 'line %d of %s' % (d['start_line'] + 1, path)
        else:
            return 'lines %d-%d of %s' % (
                d['start_line'] + 1, d['start_line'] + d['num_lines'], path)
    else:
        return path
