# Copyright 2015-2016 Yelp
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
"""Parse logs from EMR bootstrap actions (and, eventually, Dataproc
initialization actions)."""
import re

from .task import _parse_task_stderr
from .wrap import _cat_log
from .wrap import _ls_logs

# match cause of failure when there's a problem with bootstrap script. Example:
#
# On the master instance (i-96c21a39), bootstrap action 1 returned a non-zero
# return code
#
# This correponds to a path like
# <s3_log_dir>/node/i-96c21a39/bootstrap-actions/1/stderr.gz
#
# (may or may not actually be gzipped)
_BOOTSTRAP_NONZERO_RETURN_CODE_RE = re.compile(
    r'^.*\((?P<node_id>i-[0-9a-f]+)\)'
    r'.*bootstrap action (?P<action_num>\d+)'
    r'.*non-zero return code'
    r'.*$')


def _check_for_nonzero_return_code(reason):
    m = _BOOTSTRAP_NONZERO_RETURN_CODE_RE.match(reason)

    if m:
        return dict(action_num=int(m.group('action_num')),
                    node_id=m.group('node_id'))
    else:
        return None


def _ls_emr_bootstrap_stderr_paths(
        fs, log_dir_stream, action_num=None, node_id=None):

    matches = _ls_logs(fs, log_dir_stream, _match_emr_bootstrap_stderr_path,
                       action_num=None, node_id=None)

    return sorted(matches, key=lambda m: (-m['action_num'], m['node_id']))


def _match_emr_bootstrap_stderr_path(path, node_id=None, action_num=None):
    # write a regex for this
    pass


def _interpret_emr_bootstrap_stderr(fs, matches):
    # cat logs, pass them to _parse_task_stderr
    pass
