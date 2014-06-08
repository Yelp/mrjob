# Copyright 2009-2012 Yelp
# Copyright 2013 David Marin
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
"""

from __future__ import with_statement

import glob
from itertools import chain
import logging
import os

from mrjob.util import expand_path

try:
    import simplejson as json  # preferred because of C speedups
    json  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import json  # built in to Python 2.6 and later

# yaml is nice to have, but we can fall back on JSON if need be
try:
    import yaml
    yaml  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    yaml = None


from mrjob.util import shlex_split


log = logging.getLogger(__name__)


class OptionStore(dict):
    """Encapsulates logic about a configuration. With the exception of the
    constructor, it can be accessed like a dictionary."""

    #: Set of valid keys for this type of configuration
    ALLOWED_KEYS = set()

    #: Mapping of key to function used to combine multiple values to override,
    #: augment, etc. Leave blank for :py:func:`combine_values()`.
    COMBINERS = dict()

    def __init__(self):
        super(OptionStore, self).__init__()
        self.cascading_dicts = [
            dict((key, None) for key in self.ALLOWED_KEYS),
            self.default_options(),
        ]

    def default_options(self):
        """Default options for this :py:class:`OptionStore`"""
        return {}

    def validated_options(self, opts, error_fmt):
        unrecognized_opts = set(opts) - self.ALLOWED_KEYS
        if unrecognized_opts:
            log.warn(error_fmt % ', '.join(sorted(unrecognized_opts)))
            return dict((k, v) for k, v in opts.iteritems()
                        if k in self.ALLOWED_KEYS)
        else:
            return opts

    def populate_values_from_cascading_dicts(self):
        """When ``cascading_dicts`` has been built, use it to populate the
        dictionary with the ultimate values.
        """
        self.update(combine_opts(self.COMBINERS, *self.cascading_dicts))
        self._opt_priority = calculate_opt_priority(self, self.cascading_dicts)

    def is_default(self, key):
        return self._opt_priority[key] < 2

    def __getitem__(self, key):
        if key in self.ALLOWED_KEYS:
            return super(OptionStore, self).__getitem__(key)
        else:
            raise KeyError(key)

    def __setitem__(self, key, value):
        if key in self.ALLOWED_KEYS:
            return super(OptionStore, self).__setitem__(key, value)
        else:
            raise KeyError(key)


### READING AND WRITING mrjob.conf ###


def find_mrjob_conf():
    """Look for :file:`mrjob.conf`, and return its path. Places we look:

    - The location specified by :envvar:`MRJOB_CONF`
    - :file:`~/.mrjob.conf`
    - :file:`/etc/mrjob.conf`

    Return ``None`` if we can't find it.
    """
    def candidates():
        if 'MRJOB_CONF' in os.environ:
            yield expand_path(os.environ['MRJOB_CONF'])

        # $HOME isn't necessarily set on Windows, but ~ works
        # use os.path.join() so we don't end up mixing \ and /
        yield expand_path(os.path.join('~', '.mrjob.conf'))

        # this only really makes sense on Unix, so no os.path.join()
        yield '/etc/mrjob.conf'

    for path in candidates():
        log.debug('looking for configs in %s' % path)
        if os.path.exists(path):
            log.info('using configs in %s' % path)
            return path
    else:
        log.info("no configs found; falling back on auto-configuration")
        return None


def real_mrjob_conf_path(conf_path=None):
    if conf_path is False:
        return None
    elif conf_path is None:
        return find_mrjob_conf()
    else:
        return expand_path(conf_path)


def conf_object_at_path(conf_path):
    if conf_path is None:
        return None

    with open(conf_path) as f:
        if yaml:
            return yaml.safe_load(f)
        else:
            try:
                return json.load(f)
            except ValueError as e:
                msg = ('If your mrjob.conf is in YAML, you need to install'
                       ' yaml; see http://pypi.python.org/pypi/PyYAML/')
                # Use msg attr if it's set
                if hasattr(e, 'msg'):
                    e.msg = '%s (%s)' % (e.msg, msg)
                else:
                    e.msg = msg
                raise e


def load_opts_from_mrjob_conf(runner_alias, conf_path=None,
                              already_loaded=None):
    """Load a list of dictionaries representing the options in a given
    mrjob.conf for a specific runner. Returns ``[(path, values)]``. If
    conf_path is not found, return [(None, {})].

    :type runner_alias: str
    :param runner_alias: String identifier of the runner type, e.g. ``emr``,
                         ``local``, etc.
    :type conf_path: str
    :param conf_path: location of the file to load
    :type already_loaded: list
    :param already_loaded: list of :file:`mrjob.conf` paths that have already
                           been loaded
    """
    conf_path = real_mrjob_conf_path(conf_path)
    conf = conf_object_at_path(conf_path)

    if conf is None:
        return [(None, {})]

    if already_loaded is None:
        already_loaded = []

    already_loaded.append(conf_path)

    try:
        values = conf['runners'][runner_alias] or {}
    except (KeyError, TypeError, ValueError):
        values = {}

    inherited = []
    if conf.get('include', None):
        includes = conf['include']
        if isinstance(includes, basestring):
            includes = [includes]

        for include in includes:
            if include in already_loaded:
                log.warn('%s tries to recursively include %s! (Already'
                         ' included:  %s)' % (
                             conf_path, conf['include'],
                             ', '.join(already_loaded)))
            else:
                inherited.extend(
                    load_opts_from_mrjob_conf(
                        runner_alias, include, already_loaded))
    return inherited + [(conf_path, values)]


def load_opts_from_mrjob_confs(runner_alias, conf_paths=None):
    """Load a list of dictionaries representing the options in a given
    list of mrjob config files for a specific runner. Returns
    ``[(path, values)]``. If a path is not found, use (None, {}) as its value.
    If *conf_paths* is ``None``, look for a config file in the default
    locations.

    :type runner_alias: str
    :param runner_alias: String identifier of the runner type, e.g. ``emr``,
                         ``local``, etc.
    :type conf_paths: list or ``None``
    :param conf_path: locations of the files to load
    """
    if conf_paths is None:
        return load_opts_from_mrjob_conf(runner_alias, find_mrjob_conf())
    else:
        return chain(*[
            load_opts_from_mrjob_conf(runner_alias, path)
            for path in conf_paths])


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
    with :py:func:`shlex.split`. The string must either be a byte string or a
    unicode string containing no non-ASCII characters.

    Returns either ``None`` or a list containing the command plus arguments.
    """
    cmd = combine_values(*cmds)

    if cmd is None:
        return None
    elif isinstance(cmd, basestring):
        return shlex_split(cmd)
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


### PRIORITY ###


# TODO 0.4: Move inside OptionStore
def calculate_opt_priority(opts, opt_dicts):
    """Keep track of where in the order opts were specified,
    to handle opts that affect the same thing (e.g. ec2_*instance_type).

    Here is a rough guide to the values set by this function. They are

        Where specified     Priority
        unset everywhere    -1
        blank               0
        non-blank default   1
        base conf file      2
        inheriting conf     [3-n]
        command line        n+1

    :type opts: iterable
    :type opt_dicts: list of dicts with keys also appearing in **opts**
    """
    opt_priority = dict((opt, -1) for opt in opts)
    for priority, opt_dict in enumerate(opt_dicts):
        if opt_dict:
            for opt, value in opt_dict.iteritems():
                if value is not None:
                    opt_priority[opt] = priority
    return opt_priority
