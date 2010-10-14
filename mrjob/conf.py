# Copyright 2009-2010 Yelp
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
"""Logic for reading and writing mrjob.conf config files, and automatically
setting up configs (e.g. finding the Hadoop streaming JAR).

Configs contain keyword options to pass to the __init__() methods of the
various job runners (referred to as "opts" throughout our code). This module
provides a way of programmatically combining options from various sources
(e.g. mrjob.conf, the command line, defaults) through combine_*() methods.
"""
from __future__ import with_statement

import glob
import logging
import os

try:
    import simplejson as json # preferred because of C speedups
except ImportError:
    import json # built in to Python 2.6 and later

# yaml is nice to have, but we can fall back on json if need be
try:
    import yaml
except ImportError:
    yaml = None

log = logging.getLogger('mrjob.emr')

### READING AND WRITING mrjob.conf ###

def find_mrjob_conf():
    """Look for the mrjob.conf file, and return its path, or return
    None if we can't find it.

    We look for:
    - ~/.mrjob
    - mrjob.conf in any directory in $PYTHONPATH
    - /etc/mrjob.conf
    """
    def candidates():
        if 'HOME' in os.environ:
            yield os.path.join(os.environ['HOME'], '.mrjob')

        if os.environ.get('PYTHONPATH'):
            for dirname in os.environ['PYTHONPATH'].split(os.pathsep):
                yield os.path.join(dirname, 'mrjob.conf')

        yield '/etc/mrjob.conf'

    for path in candidates():
        log.debug('looking for configs in %s' % path)
        if os.path.exists(path):
            log.info('using configs in %s' % path)
            return path
    else:
        log.info("no configs found; falling back on auto-configuration")
        return None

def load_mrjob_conf(conf_path=None):
    """Load the entire data structure in mrjob.conf. Returns None
    if we can't find it.

    Args:
    conf_path -- an alternate place to look for mrjob.conf.

    If conf_path is False, we'll always return None.
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
            return json.loads(f)

def load_opts_from_mrjob_conf(runner_type, conf_path=None):
    """Load the options to initialize a runner from mrjob.conf, or return {}
    if we can't find them.

    Args:
    conf_path -- an alternate place to look for mrjob.conf
    """
    conf = load_mrjob_conf(conf_path=conf_path)
    if conf is None:
        return {}

    try:
        return conf['runners'][runner_type] or {}
    except (KeyError, TypeError, ValueError):
        log.warning('no configs for runner type %r; returning {}' %
                    runner_type)
        return {}

def dump_mrjob_conf(conf, f):
    """Write a configuration out to the given file object."""
    if yaml:
        yaml.safe_dump(conf, f, default_flow_style=False)
    else:
        json.dumps(conf, f, indent=2)
    f.flush()

### COMBINING OPTIONS ###

# combiners generally consider earlier values to be defaults, and later
# options to override or add on to them.

def expand_path(path):
    """Resolve ~ (home dir) and environment variables in paths. If path is None,
    return None.
    """
    if path is None:
        return None
    else:
        return os.path.expanduser(os.path.expandvars(path))

def combine_dicts(*dicts):
    """Combine zero or more dictionaries. Values from dicts later in the list
    take precedence over values earlier in the list.

    If you pass in None in place of a dictionary, it will be ignored.
    """
    result = {}

    for d in dicts:
        if d:
            result.update(d)

    return result

def combine_envs(*envs):
    """Combine zero or more dictionaries containing environment variables.

    Environment variables later from dictionaries later in the list take
    priority over those earlier in the list. For variables ending with 'PATH',
    we prepend (and add a colon) rather than overwriting.

    If you pass in None in place of a dictionary, it will be ignored.
    """
    result = {}
    for env in envs:
        if env:
            for key, value in env.iteritems():
                if key.endswith('PATH') and result.get(key):
                    result[key] = '%s:%s' % (value, result[key])
                else:
                    result[key] = value

    return result

def combine_lists(*seqs):
    """Concatenate the given sequences into a list. Ignore None values."""
    result = []

    for seq in seqs:
        if seq:
            result.extend(seq)

    return result

def combine_opts(combiners, *opts_list):
    """Utility function to combine options used to initialize
    a job runner, e.g. to combine default opts with opts specified on
    the command line. opts later in the list take precedence.

    Args:
    combiners -- a dictionary option name to a combine_*() function to
        use to combine options by that name. By default, we combine options
        using combine_values()
    opts_list -- one or more dictionaries (None is not allowed)
    """
    final_opts = {}
    
    keys = set()
    for opts in opts_list:
        keys.update(opts)

    for key in keys:
        values = []
        for opts in opts_list:
            if key in opts:
                values.append(opts[key])

        combine_func = combiners.get(key) or combine_values
        final_opts[key] = combine_func(*values)

    return final_opts

def combine_paths(*paths):
    """Returns the last value in *paths that is not None.
    Resolve ~ (home dir) and environment variables."""
    return expand_path(combine_values(*paths))

def combine_path_lists(*path_seqs):
    """Concatenate the given sequences into a list. Ignore None values.
    Resolve ~ (home dir) and environment variables, and expand globs
    that refer to the local filesystem."""
    results = []

    for path in combine_lists(*path_seqs):
        expanded = expand_path(path)
        # if we can't expand a glob, leave as-is (maybe it refers to
        # S3 or HDFS)
        paths = sorted(glob.glob(expanded)) or [expanded]

        results.extend(paths)

    return results

def combine_values(*values):
    """Return the last value in values that is not None"""
    for v in reversed(values):
        if v is not None:
            return v
    else:
        return None

