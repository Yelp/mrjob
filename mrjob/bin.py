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
from subprocess import Popen
from subprocess import PIPE

from mrjob.conf import combine_local_envs
from mrjob.runner import MRJobRunner
from mrjob.step import STEP_TYPES
from mrjob.util import cmd_line

log = logging.getLogger(__name__)


class MRJobBinRunner(MRJobRunner):

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
