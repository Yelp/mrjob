# Copyright 2009-2012 Yelp and Contributors
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
from __future__ import with_statement

import os
from tempfile import mkdtemp
from shutil import rmtree

try:
    import unittest2 as unittest
    unittest  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import unittest

from mock import patch

from mrjob import runner


# simple config that also silences 'no config options for runner' logging
EMPTY_MRJOB_CONF = {'runners': {
    'local': {
        'label': 'test_job'
    },
    'emr': {
        'check_emr_status_every': 0.00,
        's3_sync_wait_time': 0.00,
    },
    'hadoop': {
        'label': 'test_job'
    },
    'inline': {
        'label': 'test_job'
    },
}}


def mrjob_conf_patcher(substitute_conf=EMPTY_MRJOB_CONF):
    def mock_load_opts_from_mrjob_confs(runner_alias, conf_paths=None):
        return [(None, substitute_conf['runners'][runner_alias])]

    return patch.object(runner, 'load_opts_from_mrjob_confs',
                        mock_load_opts_from_mrjob_confs)


class EmptyMrjobConfTestCase(unittest.TestCase):

    # set to None if you don't want load_opts_from_mrjob_confs patched
    MRJOB_CONF_CONTENTS = EMPTY_MRJOB_CONF

    def setUp(self):
        super(EmptyMrjobConfTestCase, self).setUp()

        if self.MRJOB_CONF_CONTENTS is not None:
            patcher = mrjob_conf_patcher(self.MRJOB_CONF_CONTENTS)
            patcher.start()
            self.addCleanup(patcher.stop)


class SandboxedTestCase(EmptyMrjobConfTestCase):
    """Patch mrjob.conf, create a temp directory, and save the environment for
    each test
    """

    def setUp(self):
        super(SandboxedTestCase, self).setUp()

        # tmp dir
        self.tmp_dir = mkdtemp()
        self.addCleanup(rmtree, self.tmp_dir)

        # environment
        self._old_environ = os.environ.copy()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._old_environ)

    def makedirs(self, path):
        abs_path = os.path.join(self.tmp_dir, path)
        if not os.path.isdir(abs_path):
            os.makedirs(abs_path)
        return abs_path

    def makefile(self, path, contents):
        self.makedirs(os.path.split(path)[0])
        abs_path = os.path.join(self.tmp_dir, path)
        with open(abs_path, 'w') as f:
            f.write(contents)
        return abs_path

    def abs_paths(self, *paths):
        return [os.path.join(self.tmp_dir, path) for path in paths]
