# Copyright 2009-2012 Yelp
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

import os

from mrjob.fs.multi import MultiFilesystem
from mrjob.fs.ssh import SSHFilesystem
from mrjob.fs import ssh as fs_ssh

from tests.fs import MockSubprocessTestCase
from tests.mockssh import main as mock_ssh_main


class SSHFSTestCase(MockSubprocessTestCase):

    def setUp(self):
        super(SSHFSTestCase, self).setUp()
        # wrap SSHFilesystem so it gets cat()
        self.fs = MultiFilesystem(SSHFilesystem(['hadoop']))
        self.set_up_mock_ssh()
        self.mock_popen(fs_ssh, mock_hadoop_main)
