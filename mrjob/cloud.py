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
from mrjob.options import _allowed_keys
from mrjob.options import _combiners
from mrjob.options import _deprecated_aliases
from mrjob.runner import MRJobRunner
from mrjob.runner import RunnerOptionStore
from mrjob.setup import BootstrapWorkingDirManager
from mrjob.setup import parse_setup_cmd
from mrjob.util import cmd_line
from mrjob.util import file_ext

log = logging.getLogger(__name__)


# map archive file extensions to the command used to unarchive them
_EXT_TO_UNARCHIVE_CMD = {
    '.zip': 'unzip -o %(file)s -d %(dir)s',
    '.tar': 'mkdir %(dir)s; tar xf %(file)s -C %(dir)s',
    '.tar.gz': 'mkdir %(dir)s; tar xfz %(file)s -C %(dir)s',
    '.tgz': 'mkdir %(dir)s; tar xfz %(file)s -C %(dir)s',
}


class HadoopInTheCloudOptionStore(RunnerOptionStore):

    ALLOWED_KEYS = _allowed_keys('_cloud')
    COMBINERS = _combiners('_cloud')
    DEPRECATED_ALIASES = _deprecated_aliases('_cloud')





class HadoopInTheCloudJobRunner(MRJobRunner):
    """Abstract base class for all Hadoop-in-the-cloud services."""

    alias = '_cloud'

    OPTION_STORE_CLASS = HadoopInTheCloudOptionStore

    # so far, every service provides the ability to run bootstrap scripts
    BOOTSTRAP_MRJOB_IN_SETUP = False


    def __init__(self, **kwargs):
        super(HadoopInTheCloudJobRunner, self).__init__(**kwargs)

        # if *cluster_id* is not set, ``self._cluster_id`` will be
        # set when we create or join a cluster
        self._cluster_id = self._opts['cluster_id']

        # bootstrapping
        self._bootstrap = self._bootstrap_python() + self._parse_bootstrap()

        # add files to manager
        self._bootstrap_dir_mgr = BootstrapWorkingDirManager()

        for cmd in self._bootstrap:
            for maybe_path_dict in cmd:
                if isinstance(maybe_path_dict, dict):
                    self._bootstrap_dir_mgr.add(**maybe_path_dict)

        # we'll create this script later, as needed
        self._master_bootstrap_script_path = None

    ### Bootstrapping ###

    def _bootstrap_python(self):
        """Redefine this to return a (possibly empty) list of parsed commands
        (in the same format as returned by parse_setup_cmd())' to make sure a
        compatible version of Python is installed

        If the *bootstrap_python* option is false, should always return ``[]``.
        """
        return []

    def _cp_to_local_cmd(self):
        """Command to copy files from the cloud to the local directory
        (usually via Hadoop). Redefine this as needed; for example, on EMR,
        we sometimes have to use ``aws s3 cp`` because ``hadoop`` isn't
        installed at bootstrap time."""
        return 'hadoop fs -copyToLocal'

    def _parse_bootstrap(self):
        """Parse the *bootstrap* option with
        :py:func:`mrjob.setup.parse_setup_cmd()`.
        """
        return [parse_setup_cmd(cmd) for cmd in self._opts['bootstrap']]

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
            log.debug('BOOTSTRAP: ' + line)

        with open(path, 'wb') as f:
            for line in contents:
                f.write(line.encode('utf-8') + b'\n')

        self._master_bootstrap_script_path = path

    def _master_bootstrap_script_content(self, bootstrap):
        """Return a list containing the lines of the master bootstrap script.
        (without trailing newlines)
        """
        out = []

        # shebang, precommands
        out.extend(self._start_of_sh_script())
        out.append('')

        # store $PWD
        out.append('# store $PWD')
        out.append('__mrjob_PWD=$PWD')
        out.append('')

        # special case for PWD being in /, which happens on Dataproc
        # (really we should cd to tmp or something)
        out.append('if [ $__mrjob_PWD = "/" ]; then')
        out.append('  __mrjob_PWD=""')
        out.append('fi')
        out.append('')

        # run commands in a block so we can redirect stdout to stderr
        # (e.g. to catch errors from compileall). See #370
        out.append('{')

        # download files
        out.append('  # download files and mark them executable')

        cp_to_local = self._cp_to_local_cmd()

        # TODO: why bother with $__mrjob_PWD here, since we're already in it?
        for name, path in sorted(
                self._bootstrap_dir_mgr.name_to_path('file').items()):
            uri = self._upload_mgr.uri(path)
            out.append('  %s %s $__mrjob_PWD/%s' %
                    (cp_to_local, pipes.quote(uri), pipes.quote(name)))
            # make everything executable, like Hadoop Distributed Cache
            out.append('  chmod a+x $__mrjob_PWD/%s' % pipes.quote(name))
        out.append('')

        # download and unarchive archives
        archive_names_and_paths = sorted(
            self._bootstrap_dir_mgr.name_to_path('archive').items())
        if archive_names_and_paths:
            # make tmp dir if needed
            out.append('__mrjob_TMP=$(mktemp -d)')
            out.append('')

            for name, path in archive_names_and_paths:
                uri = self._upload_mgr.uri(path)
                ext = file_ext(path)

                # copy file to tmp dir
                quoted_archive_path = (
                    '$__mrjob_TMP/' + pipes.quote(name + ext))

                out.append('  %s %s %s' % (
                    cp_to_local, pipes.quote(uri), quoted_archive_path))

                # unarchive file
                if ext not in _EXT_TO_UNARCHIVE_CMD:
                    raise KeyError('unknown archive file extension: %s' % path)
                unarchive_cmd = _EXT_TO_UNARCHIVE_CMD[ext]

                out.append('  ' + unarchive_cmd % dict(
                    file=quoted_archive_path,
                    dir='$__mrjob_PWD/' + pipes.quote(name)))

                out.append('')

        # run bootstrap commands
        out.append('  # bootstrap commands')
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
            out.append(line)

        out.append('} 1>&2')  # stdout -> stderr for ease of error log parsing

        return out

    def _start_of_sh_script(self):
        """Return a list of lines (without trailing newlines) containing the
        shell script shebang and pre-commands."""
        out = []

        # shebang
        sh_bin = self._sh_bin()
        if not sh_bin[0].startswith('/'):
            sh_bin = ['/usr/bin/env'] + sh_bin
        out.append('#!' + cmd_line(sh_bin))

        # hook for 'set -e', etc. (see #1549)
        out.extend(self._sh_pre_commands())

        return out
