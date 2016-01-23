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
"""Utility for handling IDs, especially sorting by recency."""


# TODO: test these!

def _sort_by_recency(ds):
    """Sort the given list/sequence of dicts containing IDs so that the
    most recent ones come first (e.g. to find the best error, or the best
    log file to look for an error in).
    """
    return _sort_by_recency(ds, key=_time_sort_key, reverse=True)


def _time_sort_key(d):
    """Sort key to sort the given dictionaries containing IDs roughly by time
    (earliest first).

    We consider higher attempt_nums "later" than higher task_nums (of the
    same step type) because fatal errors usually occur on the final
    attempt of a task.
    """
    # break ID like
    # {application,attempt,task,job}_201601081945_0005[_m[_000005[_0]]] into
    # its component parts
    attempt_parts = (d.get('attempt_id') or d.get('task_id')
                     or d.get('job_id') or d.get('application_id')
                     or '').split('_')

    timestamp_and_step = '_'.join(attempt_parts[1:3])
    task_type = '_'.join(attempt_parts[3:4])
    task_num = '_'.join(attempt_parts[4:5])
    attempt_num = '_'.join(attempt_parts[5:6])

    # numbers are 0-padded, so no need to convert anything to int
    # also, 'm' (task_type in attempt_id) sorts before 'r', which is
    # what we want
    return (
        d.get('application_id') or '',
        d.get('container_id') or '',
        timestamp_and_step,
        task_type,
        attempt_num,
        task_num)


def _add_implied_task_id(d):
    """If *d* (a dictionary) has *attempt_id* but not *task_id*, add it.

    Use this on errors.
    """
    # NOTE: container IDs look similar to task IDs, but they're actually
    # different. Each container contains one task attempt, so there are
    # actually more container IDs than task IDs.
    if d.get('attempt_id') and not d.get('task_id'):
        d['task_id'] = _attempt_id_to_task_id(
            d['attempt_id'])


# TODO: pretty sure that application and job IDs match, but if not,
# our code could probably live with that
def _add_implied_job_id(d):
    """If *d* has *task_id* or *application_id* but not *job_id*,
    add it.

    (We don't infer application_id from job_id because application_id
    only exists on YARN)
    """
    if not d.get('job_id'):
        if d.get('task_id'):
            d['job_id'] = _to_job_id(d['task_id'])
        elif d.get('application_id'):
            d['job_id'] = _to_job_id(d['application_id'])



def _attempt_id_to_task_id(attempt_id):
    """Convert e.g. ``'attempt_201601081945_0005_m_000005_0'``
    to ``'task_201601081945_0005_m_000005'``"""
    return 'task_' + '_'.join(attempt_id.split('_')[1:5])


def _to_job_id(task_id):
    """Convert e.g. ``'task_201601081945_0005_m_000005'``
    or ``'application_201601081945_0005'`` to
    to ``'job_201601081945_0005'``."""
    return 'job_' + '_'.join(task_id.split('_')[1:3])
