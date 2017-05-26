# -*- coding: utf-8 -*-
# Copyright 2017 Yelp
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
import logging
import os
import pipes
from mrjob.runner import MRJobRunner
from mrjob.runner import RunnerOptionStore
from mrjob.util import cmd_line

log = logging.getLogger(__name__)


class HadoopInTheCloudOptionStore(RunnerOptionStore):
    pass


class HadoopInTheCloudJobRunner(MRJobRunner):
    """Abstract base class for all Hadoop-in-the-cloud services."""

    # so far, every service provides the ability to run bootstrap scripts
    BOOTSTRAP_MRJOB_IN_SETUP = False

    # init: mentions
    # _cluster_id
    # _bootstrap
    # _bootstrap_dir_mgr
    # _master_bootstrap_script_path

    ### Bootstrapping ###

    def _cp_to_local_cmd(self):
        """Command to copy files from the cloud to the local directory
        (usually via Hadoop). Redefine this as needed"""
        return 'hadoop fs -copyToLocal'

    def _create_master_bootstrap_script_if_needed(self):
        """Helper for :py:meth:`_add_bootstrap_files_for_upload`.

        Create the master bootstrap script and write it into our local
        temp directory. Set self._master_bootstrap_script_path.

        This will do nothing if there are no bootstrap scripts or commands,
        or if it has already been called."""
        if self._master_bootstrap_script_path:
            return

        # don't bother if we're not starting a cluster
        if self._cluster_id:
            return

        # Also don't bother if we're not bootstrapping
        if not (self._bootstrap or self._bootstrap_mrjob()):
            return

        # create mrjob.zip if we need it, and add commands to install it
        mrjob_bootstrap = []
        if self._bootstrap_mrjob():
            assert self._mrjob_zip_path
            path_dict = {
                'type': 'file', 'name': None, 'path': self._mrjob_zip_path}
            self._bootstrap_dir_mgr.add(**path_dict)

            # find out where python keeps its libraries
            mrjob_bootstrap.append([
                "__mrjob_PYTHON_LIB=$(%s -c "
                "'from distutils.sysconfig import get_python_lib;"
                " print(get_python_lib())')" %
                cmd_line(self._python_bin())])

            # remove anything that might be in the way (see #1567)
            mrjob_bootstrap.append(['sudo rm -rf $__mrjob_PYTHON_LIB/mrjob'])

            # unzip mrjob.zip
            mrjob_bootstrap.append(
                ['sudo unzip ', path_dict, ' -d $__mrjob_PYTHON_LIB'])

            # re-compile pyc files now, since mappers/reducers can't
            # write to this directory. Don't fail if there is extra
            # un-compileable crud in the tarball (this would matter if
            # sh_bin were 'sh -e')
            mrjob_bootstrap.append(
                ['sudo %s -m compileall -q'
                 ' -f $__mrjob_PYTHON_LIB/mrjob && true' %
                 cmd_line(self._python_bin())])

        path = os.path.join(self._get_local_tmp_dir(), 'b.sh')
        log.info('writing master bootstrap script to %s' % path)

        contents = self._master_bootstrap_script_content(
            self._bootstrap + mrjob_bootstrap)
        for line in contents:
            log.debug('BOOTSTRAP: ' + line.rstrip('\r\n'))

        with open(path, 'w') as f:
            for line in contents:
                f.write(line)

        self._master_bootstrap_script_path = path

    def _master_bootstrap_script_content(self, bootstrap):
        """Return a list containing the lines of the master bootstrap script.
        """
        out = []

        def writeln(line=''):
            out.append(line + '\n')

        # shebang, precommands
        self._write_start_of_sh_script(writeln)

        # store $PWD
        writeln('# store $PWD')
        writeln('__mrjob_PWD=$PWD')
        writeln()

        # special case for PWD being in /, which happens on Dataproc
        # (really we should cd to tmp or something)
        writeln('if [ $__mrjob_PWD = "/" ]; then')
        writeln('  __mrjob_PWD=""')
        writeln('fi')
        writeln()

        # run commands in a block so we can redirect stdout to stderr
        # (e.g. to catch errors from compileall). See #370
        writeln('{')

        # download files
        writeln('  # download files and mark them executable')

        cp_to_local = self._cp_to_local_cmd()

        # TODO: why bother with $__mrjob_PWD here, since we're already in it?
        for name, path in sorted(
                self._bootstrap_dir_mgr.name_to_path('file').items()):
            uri = self._upload_mgr.uri(path)
            writeln('  %s %s $__mrjob_PWD/%s' %
                    (cp_to_local, pipes.quote(uri), pipes.quote(name)))
            # make everything executable, like Hadoop Distributed Cache
            writeln('  chmod a+x $__mrjob_PWD/%s' % pipes.quote(name))
        writeln()

        # run bootstrap commands
        writeln('  # bootstrap commands')
        for cmd in bootstrap:
            # reconstruct the command line, substituting $__mrjob_PWD/<name>
            # for path dicts
            line = '  '
            for token in cmd:
                if isinstance(token, dict):
                    # it's a path dictionary
                    line += '$__mrjob_PWD/'
                    line += pipes.quote(self._bootstrap_dir_mgr.name(**token))
                else:
                    # it's raw script
                    line += token
            writeln(line)

        writeln('} 1>&2')  # stdout -> stderr for ease of error log parsing

        return out

    def _write_start_of_sh_script(self, writeln):
        """Write shebang and pre-commands."""
        # shebang
        sh_bin = self._sh_bin()
        if not sh_bin[0].startswith('/'):
            sh_bin = ['/usr/bin/env'] + sh_bin
        writeln('#!' + cmd_line(sh_bin))

        # hook for 'set -e', etc. (see #1549)
        for cmd in self._sh_pre_commands():
            writeln(cmd)

        writeln()
