# -*- coding: utf-8 -*-
# Copyright 2016 Yelp and Contributors
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
"""Runner mixin for counters and probable cause of failure.

This relies on passing around a *log_interpretation* dictionary, which
is described in detail in :py:mod:`mrjob.logs`.

This mixin doesn't yet handle step logs because the EMR and Hadoop runners
handle them so differently. It's up to you to fill in the 'step' field
of your log interpretation; the mixin can't do much without it because
it needs it for the job/application ID.

Your runner should generally have one log interpretation per step,
though the mixin doesn't care how or where you store them.
"""
from logging import getLogger

from mrjob.compat import uses_yarn
from mrjob.logs.counters import _pick_counters
from mrjob.logs.errors import _pick_error
from mrjob.logs.history import _interpret_history_log
from mrjob.logs.history import _ls_history_logs
from mrjob.logs.task import _interpret_task_logs
from mrjob.logs.task import _interpret_spark_task_logs
from mrjob.logs.task import _ls_task_logs
from mrjob.logs.task import _ls_spark_task_logs
from mrjob.step import _is_spark_step_type

log = getLogger(__name__)


# a callback for _interpret_task_logs(). Breaking it out to make
# testing easier
def _log_parsing_task_log(log_path):
    log.info('  Parsing task log: %s' % log_path)


class LogInterpretationMixin(object):
    """Mix this in to your runner class to simplify log interpretation."""
    # this mixin is meant to be tightly bound to MRJobRunner, but
    # currently it only relies on self.fs and self.get_hadoop_version()

    ### stuff to redefine ###

    def _stream_history_log_dirs(self, output_dir=None):
        """Yield lists of directories (usually, URIs) to search for history
        logs in.

        Usually, you'll want to add logging messages (e.g.
        'Searching for history logs in ...'

        :param output_dir: Output directory for step (optional), to look
            for logs (e.g. on Cloudera).
        """
        return ()

    def _stream_task_log_dirs(self, application_id=None, output_dir=None):
        """Yield lists of directories (usually, URIs) to search for task
        logs in.

        Usually, you'll want to add logging messages (e.g.
        'Searching for task syslogs in...')

        :param application_id: YARN application ID (optional), so we can ls
            the relevant subdirectory of `userlogs/` rather than the whole
            thing
        :param output_dir: Output directory for step (optional), to look
            for logs (e.g. on Cloudera).
        """
        # sometimes pre-YARN logs are organized by job ID, but not always,
        # so we don't bother with job_id; just ls() the entire userlogs
        # dir and depend on regexes to find the right subdir.
        return ()

    def _get_step_log_interpretation(self, log_interpretation, step_type):
        """Return interpretation of the step log. Either implement
        this, or fill ``'step'`` yourself (e.g. from Hadoop binary's
        output."""
        return None

    ### stuff to call ###

    def _pick_counters(self, log_interpretation, step_type):
        """Pick counters from our log interpretation, interpreting
        history logs if need be."""
        if _is_spark_step_type(step_type):
            return {}

        counters = _pick_counters(log_interpretation)

        if not counters:
            log.info('Attempting to fetch counters from logs...')
            self._interpret_step_logs(log_interpretation, step_type)
            counters = _pick_counters(log_interpretation)

        if not counters:
            self._interpret_history_log(log_interpretation)
            counters = _pick_counters(log_interpretation)

        return counters

    def _pick_error(self, log_interpretation, step_type):
        """Pick probable cause of failure (only call this if job fails)."""
        if not all(log_type in log_interpretation for
                   log_type in ('job', 'step', 'task')):
            log.info('Scanning logs for probable cause of failure...')
            self._interpret_step_logs(log_interpretation, step_type)
            self._interpret_history_log(log_interpretation)
            self._interpret_task_logs(log_interpretation, step_type)

        return _pick_error(log_interpretation)

    ### stuff that should just work ###

    def _interpret_history_log(self, log_interpretation):
        """Fetch history log and add 'history' to log_interpretation."""
        if 'history' in log_interpretation:
            return   # already interpreted

        step_interpretation = log_interpretation.get('step') or {}

        job_id = step_interpretation.get('job_id')
        if not job_id:
            if not log_interpretation.get('no_job'):
                log.warning("Can't fetch history log; missing job ID")
            return

        output_dir = step_interpretation.get('output_dir')

        log_interpretation['history'] = _interpret_history_log(
            self.fs, self._ls_history_logs(
                job_id=job_id, output_dir=output_dir))

    def _ls_history_logs(self, job_id=None, output_dir=None):
        """Yield history log matches, logging a message for each one."""
        for match in _ls_history_logs(
                self.fs,
                self._stream_history_log_dirs(output_dir=output_dir),
                job_id=job_id):
            log.info('  Parsing history log: %s' % match['path'])
            yield match

    def _interpret_step_logs(self, log_interpretation, step_type):
        """Add *step* to the log interpretation, if it's not already there."""
        if 'step' in log_interpretation:
            return

        step_interpretation = self._get_step_log_interpretation(
            log_interpretation, step_type)
        if step_interpretation:
            log_interpretation['step'] = step_interpretation

    def _interpret_task_logs(
            self, log_interpretation, step_type, partial=True):
        """Fetch task syslogs and stderr, and add 'task' to interpretation."""
        if 'task' in log_interpretation and (
                partial or not log_interpretation['task'].get('partial')):
            return   # already interpreted

        step_interpretation = log_interpretation.get('step') or {}

        application_id = step_interpretation.get('application_id')
        job_id = step_interpretation.get('job_id')
        output_dir = step_interpretation.get('output_dir')

        yarn = uses_yarn(self.get_hadoop_version())

        if yarn:
            if not application_id:
                if not log_interpretation.get('no_job'):
                    log.warning(
                        "Can't fetch task logs; missing application ID")
                return
        else:
            if not job_id:
                if not log_interpretation.get('no_job'):
                    log.warning("Can't fetch task logs; missing job ID")
                return

        if _is_spark_step_type(step_type):
            interpret_func = _interpret_spark_task_logs
        else:
            interpret_func = _interpret_task_logs

        log_interpretation['task'] = interpret_func(
            self.fs,
            self._ls_task_logs(
                step_type,
                application_id=application_id,
                job_id=job_id,
                output_dir=output_dir),
            partial=partial,
            log_callback=_log_parsing_task_log)

    def _ls_task_logs(self, step_type,
                      application_id=None, job_id=None, output_dir=None):
        """Yield task log matches."""
        if _is_spark_step_type(step_type):
            ls_func = _ls_spark_task_logs
        else:
            ls_func = _ls_task_logs

        # logging messages are handled by a callback in _interpret_task_logs()
        for match in ls_func(
                self.fs,
                self._stream_task_log_dirs(
                    application_id=application_id, output_dir=output_dir),
                application_id=application_id,
                job_id=job_id):
            yield match
