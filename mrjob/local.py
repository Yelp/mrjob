# -*- coding: utf-8 -*-
# Copyright 2009-2013 Yelp and Contributors
# Copyright 2015-2017 Yelp
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
"""Run an MRJob locally by forking off a bunch of processes and piping
them together. Useful for testing, not terrible for running medium-sized
jobs on all CPUs."""
import logging
import os
import platform
from multiprocessing import Pool
from subprocess import CalledProcessError
from subprocess import check_call

from mrjob.logs.errors import _format_error
from mrjob.logs.task import _parse_task_stderr
from mrjob.options import _allowed_keys
from mrjob.options import _combiners
from mrjob.options import _deprecated_aliases
from mrjob.runner import RunnerOptionStore
from mrjob.sim import SimMRJobRunner
from mrjob.step import StepFailedException
from mrjob.util import cmd_line

log = logging.getLogger(__name__)


class _TaskFailedException(StepFailedException):
    """Extension of :py:class:`~mrjob.step.StepFailedException` that
    blames one particular task."""
    _FIELDS = StepFailedException._FIELDS + ('task_type', 'task_num')

    def __init__(
            self, reason=None, step_num=None, num_steps=None, step_desc=None,
            task_type=None, task_num=None):
        super(_TaskFailedException, self).__init__(
            reason=reason, step_num=step_num,
            num_steps=num_steps, step_desc=step_desc)

        self.task_type = task_type
        self.task_num = task_num


class LocalRunnerOptionStore(RunnerOptionStore):
    ALLOWED_KEYS = _allowed_keys('local')
    COMBINERS = _combiners('local')
    DEPRECATED_ALIASES = _deprecated_aliases('local')


class LocalMRJobRunner(SimMRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` locally, for testing purposes.
    Invoked when you run your job with ``-r local``.

    Unlike :py:class:`~mrjob.job.InlineMRJobRunner`, this actually spawns
    multiple subprocesses for each task.

    It's rare to need to instantiate this class directly (see
    :py:meth:`~LocalMRJobRunner.__init__` for details).

    """
    alias = 'local'

    OPTION_STORE_CLASS = LocalRunnerOptionStore

    def __init__(self, **kwargs):
        """Arguments to this constructor may also appear in :file:`mrjob.conf`
        under ``runners/local``.

        :py:class:`~mrjob.local.LocalMRJobRunner`'s constructor takes the
        same keyword args as
        :py:class:`~mrjob.runner.MRJobRunner`. However, please note:

        * *cmdenv* is combined with :py:func:`~mrjob.conf.combine_local_envs`
        * *python_bin* defaults to ``sys.executable`` (the current python
          interpreter)
        * *hadoop_input_format*, *hadoop_output_format*,
          and *partitioner* are ignored because they
          require Java. If you need to test these, consider starting up a
          standalone Hadoop instance and running your job with ``-r hadoop``.
        """
        super(LocalMRJobRunner, self).__init__(**kwargs)

        # should we fall back to sorting in memory?
        self._bad_sort_bin = False

    def _invoke_task(self, task_type, step_num, task_num,
                     stdin, stdout, stderr, wd, env):

        args = self._substep_args(step_num, task_type)

        #for key, value in sorted(env.items()):
        #    log.debug('> export %s=%s' % (key, value))
        log.debug('> %s' % cmd_line(args))

        try:
            check_call(args, stdin=stdin, stdout=stdout, stderr=stderr,
                       cwd=wd, env=env)
        except Exception as ex:
            raise _TaskFailedException(
                reason=str(ex),
                step_num=step_num,
                num_steps=self._num_steps(),
                task_type=task_type,
                task_num=task_num,
            )

    def _run_multiple(self, tasks, num_processes=None):
        """Use multiprocessing to run in parallel."""
        pool = Pool(processes=num_processes)

        try:
            results = [pool.apply_async(*task) for task in tasks]
            for result in results:
                result.get()

        # make sure that the pool (and its file descriptors, etc.)
        # don't stay open. This doesn't matter much for individual jobs,
        # but it makes our automated tasks run out of file descriptors.

            pool.close()
        except:
            # if there's an error in one task, terminate all others
            pool.terminate()
            raise
        finally:
            pool.join()

    def _log_cause_of_error(self, ex):
        if not isinstance(ex, _TaskFailedException):
            return

        input_path = self._task_input_path(
            ex.task_type, ex.step_num, ex.task_num)
        stderr_path = self._task_stderr_path(
            ex.task_type, ex.step_num, ex.task_num)

        if self.fs.exists(stderr_path):  # it should, but just to be safe
            with open(stderr_path, 'rb') as stderr:
                task_error = _parse_task_stderr(stderr)
                if task_error:
                    task_error['path'] = stderr_path
                    log.error('Cause of failure:\n\n%s\n\n' %
                              _format_error(dict(
                                  split=dict(path=input_path),
                                  task_error=task_error)))
                    return

        # fallback if we can't find the error (e.g. the job does something
        # weird to stderr or stack traces)
        log.error('Error while reading from %s:\n' % input_path)

    def _sort_input(self, input_paths, output_path):
        """Try sorting with the :command:`sort` binary before falling
        back to in-memory sort."""
        if input_paths and not (
                self._bad_sort_bin or platform.system() == 'Windows'):
            env = os.environ.copy()

            # ignore locale when sorting
            env['LC_ALL'] = 'C'

            # Make sure that the tmp dir environment variables are changed if
            # the default is changed.
            env['TMP'] = self._opts['local_tmp_dir']
            env['TMPDIR'] = self._opts['local_tmp_dir']

            err_path = os.path.join(self._get_local_tmp_dir(), 'sort-stderr')

            with open(output_path, 'wb') as output:
                with open(err_path, 'wb') as err:
                    args = self._sort_bin() + list(input_paths)
                    log.debug('> %s' % cmd_line(args))
                    try:
                        check_call(args, stdout=output, stderr=err, env=env)
                        return
                    except CalledProcessError:
                        log.error(
                            '`%s` failed, falling back to in-memory sort' %
                            cmd_line(self._sort_bin()))
                        with open(err_path) as read_err:
                            for line in read_err:
                                log.error('STDERR: %s' % line.rstrip('\r\n'))
                    except OSError:
                        log.error(
                            'no sort binary, falling back to in-memory sort')

        self._bad_sort_bin = True

        # in-memory sort
        super(LocalMRJobRunner, self)._sort_input(input_paths, output_path)

    def _sort_bin(self):
        """The binary to use to sort input.

        (On Windows, we go straight to sorting in memory.)
        """
        if self._opts['sort_bin']:
            return self._opts['sort_bin']
        elif self._sort_values:
            return ['sort']
        else:
            # only sort on the reducer key (see #660)
            return ['sort', '-t', '\t', '-k', '1,1', '-s']
