# -*- coding: utf-8 -*-
# Copyright 2009-2017 Yelp and Contributors
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
"""Abstract base class for all runners that execute binaries/scripts
(that is, everything but inline mode).
"""
import json
import logging
import os
import sys
from subprocess import Popen
from subprocess import PIPE

from mrjob.conf import combine_local_envs
from mrjob.py2 import PY2
from mrjob.runner import MRJobRunner
from mrjob.step import STEP_TYPES
from mrjob.util import cmd_line

log = logging.getLogger(__name__)


class MRJobBinRunner(MRJobRunner):

    OPT_NAMES = MRJobRunner.OPT_NAMES | {
        'interpreter',
        'python_bin',
        'steps',
        'steps_interpreter',
        'steps_python_bin',
        'task_python_bin',
    }

    def _load_steps(self):
        if not self._script_path:
            return []

        args = (self._executable(True) + ['--steps'] +
        self._mr_job_extra_args(local=True))
        log.debug('> %s' % cmd_line(args))
        # add . to PYTHONPATH (in case mrjob isn't actually installed)
        env = combine_local_envs(os.environ,
                                 {'PYTHONPATH': os.path.abspath('.')})
        steps_proc = Popen(args, stdout=PIPE, stderr=PIPE, env=env)
        stdout, stderr = steps_proc.communicate()

        if steps_proc.returncode != 0:
            raise Exception(
                'error getting step information: \n%s' % stderr)

        # on Python 3, convert stdout to str so we can json.loads() it
        if not isinstance(stdout, str):
            stdout = stdout.decode('utf_8')

        try:
            steps = json.loads(stdout)
        except ValueError:
            raise ValueError("Bad --steps response: \n%s" % stdout)

        # verify that this is a proper step description
        if not steps or not stdout:
            raise ValueError('step description is empty!')
        for step in steps:
            if step['type'] not in STEP_TYPES:
                raise ValueError(
                    'unexpected step type %r in steps %r' % (
                        step['type'], stdout))

        return steps

    ### interpreter/python binary ###

    def _interpreter(self, steps=False):
        if steps:
            return (self._opts['steps_interpreter'] or
                    self._opts['interpreter'] or
                    self._steps_python_bin())
        else:
            return (self._opts['interpreter'] or
                    self._task_python_bin())

    def _executable(self, steps=False):
        if steps:
            return self._interpreter(steps=True) + [self._script_path]
        else:
            return self._interpreter() + [
                self._working_dir_mgr.name('file', self._script_path)]

    def _python_bin(self):
        """Python binary used for everything other than invoking the job.
        For invoking jobs with ``--steps``, see :py:meth:`_steps_python_bin`,
        and for everything else (e.g. ``--mapper``, ``--spark``), see
        :py:meth:`_task_python_bin`, which defaults to this method if
        :mrjob-opt:`task_python_bin` isn't set.

        Other ways mrjob uses Python:
         * file locking in setup wrapper scripts
         * finding site-packages dir to bootstrap mrjob on clusters
         * invoking ``cat.py`` in local mode
         * the Python binary for Spark (``$PYSPARK_PYTHON``)
        """
        # python_bin isn't an option for inline runners
        return self._opts['python_bin'] or self._default_python_bin()

    def _steps_python_bin(self):
        """Python binary used to invoke job with ``--steps``"""
        return (self._opts['steps_python_bin'] or
                self._default_python_bin(local=True))

    def _task_python_bin(self):
        """Python binary used to invoke job with ``--mapper``,
        ``--reducer``, ``--spark``, etc."""
        return (self._opts['task_python_bin'] or
                self._python_bin())

    def _default_python_bin(self, local=False):
        """The default python command. If local is true, try to use
        sys.executable. Otherwise use 'python' or 'python3' as appropriate.

        This returns a single-item list (because it's a command).
        """
        if local and sys.executable:
            return [sys.executable]
        elif PY2:
            return ['python']
        else:
            # e.g. python3
            return ['python%d' % sys.version_info[0]]
