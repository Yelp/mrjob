# Copyright 2009-2011 Yelp
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

""""mrjob.conf" is the name of both this module, and the global config file
for :py:mod:`mrjob`.

We look for :file:`mrjob.conf` in these locations:

- The location specified by :envvar:`MRJOB_CONF`
- :file:`~/.mrjob.conf`
- :file:`~/.mrjob` **(deprecated)**
- :file:`mrjob.conf` in any directory in :envvar:`PYTHONPATH` **(deprecated)**
- :file:`/etc/mrjob.conf`

If your :file:`mrjob.conf` path is deprecated, use this table to fix it:

================================= ===============================
Old Location                      New Location
================================= ===============================
:file:`~/.mrjob`                  :file:`~/.mrjob.conf`
somewhere in :envvar:`PYTHONPATH` Specify in :envvar:`MRJOB_CONF`
================================= ===============================

The point of :file:`mrjob.conf` is to let you set up things you want every
job to have access to so that you don't have to think about it. For example:

- libraries and source code you want to be available for your jobs
- where temp directories and logs should go
- security credentials

:file:`mrjob.conf` is just a `YAML <http://www.yaml.org>`_-encoded dictionary
containing default values to pass in to the constructors of the various runner
classes. Here's a minimal :file:`mrjob.conf`:

.. code-block:: yaml

    runners:
      emr:
        cmdenv:
          TZ: America/Los_Angeles

Now whenever you run ``mr_your_script.py -r emr``,
:py:class:`~mrjob.emr.EMRJobRunner` will automatically set :envvar:`TZ` to
``America/Los_Angeles`` in your job's environment when it runs on EMR.

Options specified on the command-line take precedence over
:file:`mrjob.conf`. Usually this means simply overriding the option in
:file:`mrjob.conf`. However, we know that *cmdenv* contains environment
variables, so we do the right thing. For example, if your :file:`mrjob.conf`
contained:

.. code-block:: yaml

    runners:
      emr:
        cmdenv:
          PATH: /usr/local/bin
          TZ: America/Los_Angeles

and you ran your job as::

    mr_your_script.py -r emr --cmdenv TZ=Europe/Paris --cmdenv PATH=/usr/sbin

We'd automatically handle the :envvar:`PATH`
variables and your job's environment would be::

    {'TZ': 'Europe/Paris', 'PATH': '/usr/sbin:/usr/local/bin'}

What's going on here is that *cmdenv* is associated with
:py:func:`combine_envs`. Each option is associated with an appropriate
combiner function that that combines options in an appropriate way.

Combiners can also do useful things like expanding environment variables and
globs in paths. For example, you could set:

.. code-block:: yaml

    runners:
      local:
        upload_files: &upload_files
        - $DATA_DIR/*.db
      hadoop:
        upload_files: *upload_files
      emr:
        upload_files: *upload_files

and every time you ran a job, every job in your ``.db`` file in ``$DATA_DIR``
would automatically be loaded into your job's current working directory.

Also, if you specified additional files to upload with :option:`--file`, those
files would be uploaded in addition to the ``.db`` files, rather than instead
of them.

See :doc:`configs-runners` for the entire dizzying array of configurable
options.
"""

from __future__ import with_statement

import glob
import logging
import os
import shlex

from mrjob.util import expand_path

try:
    import simplejson as json  # preferred because of C speedups
except ImportError:
    import json  # built in to Python 2.6 and later

# yaml is nice to have, but we can fall back on JSON if need be
try:
    import yaml
except ImportError:
    yaml = None

log = logging.getLogger('mrjob.conf')


### READING AND WRITING mrjob.conf ###

def find_mrjob_conf():
    """Look for :file:`mrjob.conf`, and return its path. Places we look:

    - The location specified by :envvar:`MRJOB_CONF`
    - :file:`~/.mrjob.conf`
    - :file:`~/.mrjob` (deprecated)
    - :file:`mrjob.conf` in any directory in :envvar:`PYTHONPATH` (deprecated)
    - :file:`/etc/mrjob.conf`

    Return ``None`` if we can't find it. Print a warning if its location is
    deprecated.
    """
    def candidates():
        """Return (path, deprecation_warning)"""
        if 'MRJOB_CONF' in os.environ:
            yield (expand_path(os.environ['MRJOB_CONF']), None)

        # $HOME isn't necessarily set on Windows, but ~ works
        yield (expand_path('~/.mrjob.conf'), None)

        # DEPRECATED:
        yield (expand_path('~/.mrjob'), 'use ~/.mrjob.conf instead.')
        if os.environ.get('PYTHONPATH'):
            for dirname in os.environ['PYTHONPATH'].split(os.pathsep):
                yield (os.path.join(dirname, 'mrjob.conf'),
                      'Use $MRJOB_CONF to explicitly specify the path'
                       ' instead.')

        yield ('/etc/mrjob.conf', None)

    for path, deprecation_message in candidates():
        log.debug('looking for configs in %s' % path)
        if os.path.exists(path):
            log.info('using configs in %s' % path)
            if deprecation_message:
                log.warning('This config path is deprecated and will stop'
                            ' working in mrjob 0.4. %s' % deprecation_message)
            return path
    else:
        log.info("no configs found; falling back on auto-configuration")
        return None


def load_mrjob_conf(conf_path=None):
    """Load the entire data structure in :file:`mrjob.conf`, which should
    look something like this::

        {'runners':
            'local': {'OPTION': VALUE, ...}
            'emr': {'OPTION': VALUE, ...}
            'hadoop: {'OPTION': VALUE, ...}
        }

    Returns ``None`` if we can't find :file:`mrjob.conf`.

    :type conf_path: str
    :param conf_path: an alternate place to look for mrjob.conf. If this is
                      ``False``, we'll always return ``None``.
    """
    if conf_path is False:
        return None
    elif conf_path is None:
        conf_path = find_mrjob_conf()
        if conf_path is None:
            return None

    with open(conf_path) as f:
        if yaml:
            return yaml.safe_load(f)
        else:
            return json.load(f)


def load_opts_from_mrjob_conf(runner_alias, conf_path=None):
    """Load the options to initialize a runner from mrjob.conf, or return
    ``{}`` if we can't find them.

    :type conf_path: str
    :param conf_path: an alternate place to look for mrjob.conf. If this is
                      ``False``, we'll always return ``{}``.
    """
    conf = load_mrjob_conf(conf_path=conf_path)
    if conf is None:
        return {}

    try:
        return conf['runners'][runner_alias] or {}
    except (KeyError, TypeError, ValueError):
        log.warning('no configs for runner type %r; returning {}' %
                    runner_alias)
        return {}


def dump_mrjob_conf(conf, f):
    """Write out configuration options to a file.

    Useful if you don't want to bother to figure out YAML.

    *conf* should look something like this:

        {'runners':
            'local': {'OPTION': VALUE, ...}
            'emr': {'OPTION': VALUE, ...}
            'hadoop: {'OPTION': VALUE, ...}
        }

    :param f: a file object to write to (e.g. ``open('mrjob.conf', 'w')``)
    """
    if yaml:
        yaml.safe_dump(conf, f, default_flow_style=False)
    else:
        json.dump(conf, f, indent=2)
    f.flush()


### COMBINING OPTIONS ###

# combiners generally consider earlier values to be defaults, and later
# options to override or add on to them.

def combine_values(*values):
    """Return the last value in *values* that is not ``None``.

    The default combiner; good for simple values (booleans, strings, numbers).
    """
    for v in reversed(values):
        if v is not None:
            return v
    else:
        return None


def combine_lists(*seqs):
    """Concatenate the given sequences into a list. Ignore ``None`` values.

    Generally this is used for a list of commands we want to run; the
    "default" commands get run before any commands specific to your job.
    """
    result = []

    for seq in seqs:
        if seq:
            result.extend(seq)

    return result


def combine_cmds(*cmds):
    """Take zero or more commands to run on the command line, and return
    the last one that is not ``None``. Each command should either be a list
    containing the command plus switches, or a string, which will be parsed
    with :py:func:`shlex.split`

    Returns either ``None`` or a list containing the command plus arguments.
    """
    cmd = combine_values(*cmds)

    if cmd is None:
        return None
    elif isinstance(cmd, basestring):
        return shlex.split(cmd)
    else:
        return list(cmd)


def combine_cmd_lists(*seqs_of_cmds):
    """Concatenate the given commands into a list. Ignore ``None`` values,
    and parse strings with :py:func:`shlex.split`.

    Returns a list of lists (each sublist contains the command plus arguments).
    """
    seq_of_cmds = combine_lists(*seqs_of_cmds)
    return [combine_cmds(cmd) for cmd in seq_of_cmds]


def combine_dicts(*dicts):
    """Combine zero or more dictionaries. Values from dicts later in the list
    take precedence over values earlier in the list.

    If you pass in ``None`` in place of a dictionary, it will be ignored.
    """
    result = {}

    for d in dicts:
        if d:
            result.update(d)

    return result


def combine_envs(*envs):
    """Combine zero or more dictionaries containing environment variables.

    Environment variables later from dictionaries later in the list take
    priority over those earlier in the list. For variables ending with
    ``PATH``, we prepend (and add a colon) rather than overwriting.

    If you pass in ``None`` in place of a dictionary, it will be ignored.
    """
    return _combine_envs_helper(envs, local=False)


def combine_local_envs(*envs):
    """Same as :py:func:`combine_envs`, except that paths are combined
    using the local path separator (e.g ``;`` on Windows rather than ``:``).
    """
    return _combine_envs_helper(envs, local=True)


def _combine_envs_helper(envs, local):
    if local:
        pathsep = os.pathsep
    else:
        pathsep = ':'

    result = {}
    for env in envs:
        if env:
            for key, value in env.iteritems():
                if key.endswith('PATH') and result.get(key):
                    result[key] = value + pathsep + result[key]
                else:
                    result[key] = value

    return result


def combine_paths(*paths):
    """Returns the last value in *paths* that is not ``None``.
    Resolve ``~`` (home dir) and environment variables."""
    return expand_path(combine_values(*paths))


def combine_path_lists(*path_seqs):
    """Concatenate the given sequences into a list. Ignore None values.
    Resolve ``~`` (home dir) and environment variables, and expand globs
    that refer to the local filesystem."""
    results = []

    for path in combine_lists(*path_seqs):
        expanded = expand_path(path)
        # if we can't expand a glob, leave as-is (maybe it refers to
        # S3 or HDFS)
        paths = sorted(glob.glob(expanded)) or [expanded]

        results.extend(paths)

    return results


def combine_opts(combiners, *opts_list):
    """The master combiner, used to combine dictionaries of options with
    appropriate sub-combiners.

    :param combiners: a map from option name to a combine_*() function to
                      combine options by that name. By default, we combine
                      options using :py:func:`combine_values`.
    :param opts_list: one or more dictionaries to combine
    """
    final_opts = {}

    keys = set()
    for opts in opts_list:
        if opts:
            keys.update(opts)

    for key in keys:
        values = []
        for opts in opts_list:
            if opts and key in opts:
                values.append(opts[key])

        combine_func = combiners.get(key) or combine_values
        final_opts[key] = combine_func(*values)

    return final_opts
