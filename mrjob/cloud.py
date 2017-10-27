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
from os.path import basename

from mrjob.bin import MRJobBinRunner
from mrjob.setup import WorkingDirManager
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

# issue a warning if max_mins_idle is set to less than this
_DEFAULT_MAX_MINS_IDLE = 10.0


class HadoopInTheCloudJobRunner(MRJobBinRunner):
    """Abstract base class for all Hadoop-in-the-cloud services."""

    alias = '_cloud'

    OPT_NAMES = MRJobBinRunner.OPT_NAMES | {
        'bootstrap',
        'bootstrap_python',
        'check_cluster_every',
        'cloud_fs_sync_secs',
        'cloud_tmp_dir',
        'cluster_id',
        'core_instance_type',
        'extra_cluster_params',
        'image_version',
        'instance_type',
        'master_instance_type',
        'max_mins_idle',
        'max_hours_idle',
        'num_core_instances',
        'num_task_instances',
        'region',
        'task_instance_type',
        'zone',
    }

    # so far, every service provides the ability to run bootstrap scripts
    _BOOTSTRAP_MRJOB_IN_SETUP = False

    def __init__(self, **kwargs):
        super(HadoopInTheCloudJobRunner, self).__init__(**kwargs)

        # if *cluster_id* is not set, ``self._cluster_id`` will be
        # set when we create or join a cluster
        self._cluster_id = self._opts['cluster_id']

        # bootstrapping
        self._bootstrap = self._bootstrap_python() + self._parse_bootstrap()

        # add files to manager
        self._bootstrap_dir_mgr = WorkingDirManager()

        for cmd in self._bootstrap:
            for token in cmd:
                if isinstance(token, dict):
                    # convert dir archive tokens to archives
                    if token['type'] == 'dir':
                        token['path'] = self._dir_archive_path(token['path'])
                        token['type'] = 'archive'

                    self._bootstrap_dir_mgr.add(**token)

        # we'll create this script later, as needed
        self._master_bootstrap_script_path = None

    ### Options ###

    def _fix_opts(self, opts, source=None):
        opts = super(HadoopInTheCloudJobRunner, self)._fix_opts(
            opts, source=source)

        # patch max_hours_idle into max_mins_idle (see #1663)
        if opts.get('max_hours_idle') is not None:
            log.warning(
                'max_hours_idle is deprecated and will be removed in v0.7.0.' +
                (' Please use max_mins_idle instead'
                 if opts.get('max_mins_idle') is None else ''))

        if opts.get('max_mins_idle') is None:
            if opts.get('max_hours_idle') is not None:
                opts['max_mins_idle'] = opts['max_hours_idle'] * 60
            else:
                opts['max_mins_idle'] = _DEFAULT_MAX_MINS_IDLE

        # warn about issues with
        if opts['max_mins_idle'] < _DEFAULT_MAX_MINS_IDLE:
            log.warning('Setting max_mins_idle to less than %.1f may result'
                        ' in cluster shutting down before job can run' %
                        _DEFAULT_MAX_MINS_IDLE)

        return opts

    def _combine_opts(self, opt_list):
        """Propagate *instance_type* to other instance type opts, if not
        already set.

        Also propagate core instance type to task instance type, if it's
        not already set.
        """
        opts = super(HadoopInTheCloudJobRunner, self)._combine_opts(opt_list)

        if opts['instance_type']:
            # figure out how late in the configs opt was set (setting
            # --instance_type on the command line overrides core_instance_type
            # set in configs)
            opt_priority = {k: -1 for k in opts}

            for i, sub_opts in enumerate(opt_list):
                for k, v in sub_opts.items():
                    if v == opts[k]:
                        opt_priority[k] = i

            # instance_type only affects master_instance_type if there are
            # no other instances
            if opts['num_core_instances'] or opts['num_task_instances']:
                propagate_to = ['core_instance_type', 'task_instance_type']
            else:
                propagate_to = ['master_instance_type']

            for k in propagate_to:
                if opts[k] is None or (
                        opt_priority[k] < opt_priority['instance_type']):
                    opts[k] = opts['instance_type']

        if not opts['task_instance_type']:
            opts['task_instance_type'] = opts['core_instance_type']

        return opts

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
            out.append('')
            out.append('  %s %s $__mrjob_PWD/%s' %
                       (cp_to_local, pipes.quote(uri), pipes.quote(name)))
            # imitate Hadoop Distributed Cache (see #1602)
            out.append('  chmod u+rx $__mrjob_PWD/%s' % pipes.quote(name))
        out.append('')

        # download and unarchive archives
        archive_names_and_paths = sorted(
            self._bootstrap_dir_mgr.name_to_path('archive').items())
        if archive_names_and_paths:
            # make tmp dir if needed
            out.append('  # download and unpack archives')
            out.append('  __mrjob_TMP=$(mktemp -d)')
            out.append('')

            for name, path in archive_names_and_paths:
                uri = self._upload_mgr.uri(path)
                ext = file_ext(basename(path))

                # copy file to tmp dir
                quoted_archive_path = '$__mrjob_TMP/%s' % pipes.quote(name)

                out.append('  %s %s %s' % (
                    cp_to_local, pipes.quote(uri), quoted_archive_path))

                # unarchive file
                if ext not in _EXT_TO_UNARCHIVE_CMD:
                    raise KeyError('unknown archive file extension: %s' % path)
                unarchive_cmd = _EXT_TO_UNARCHIVE_CMD[ext]

                out.append('  ' + unarchive_cmd % dict(
                    file=quoted_archive_path,
                    dir='$__mrjob_PWD/' + pipes.quote(name)))

                # imitate Hadoop Distributed Cache (see #1602)
                out.append(
                    '  chmod u+rx -R $__mrjob_PWD/%s' % pipes.quote(name))

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

    ### Launching Clusters ###

    def _add_extra_cluster_params(self, params):
        """Return a dict with the *extra_cluster_params* opt patched into
        *params*, and ``None`` values removed."""
        params = params.copy()
        params.update(self._opts['extra_cluster_params'])
        params = {k: v for k, v in params.items() if v is not None}

        return params
