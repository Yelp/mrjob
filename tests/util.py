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
        'boostrap_mrjob': False,
    }
}}


def mrjob_conf_patcher(substitute_conf=EMPTY_MRJOB_CONF):
    def mock_load_opts_from_mrjob_confs(runner_alias, conf_paths=None):
        return [(None, substitute_conf['runners'][runner_alias])]

    return patch.object(runner, 'load_opts_from_mrjob_confs',
                        mock_load_opts_from_mrjob_confs)


class EmptyMrjobConfTestCase(unittest.TestCase):

    MRJOB_CONF_CONTENTS = EMPTY_MRJOB_CONF

    def setUp(self):
        super(EmptyMrjobConfTestCase, self).setUp()
        patcher = mrjob_conf_patcher(self.MRJOB_CONF_CONTENTS)
        patcher.start()
        self.addCleanup(patcher.stop)


class SandboxedTestCase(EmptyMrjobConfTestCase):

    def setUp(self):
        super(SandboxedTestCase, self).setUp()
        self.tmp_dir = mkdtemp()
        self.addCleanup(rmtree, self.tmp_dir)
