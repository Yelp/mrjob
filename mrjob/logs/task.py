# -*- coding: utf-8 -*-
# Copyright 2015 Yelp and Contributors
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
"""Parse "task" logs, which are the syslog and stderr for each individual
task and typically appear in the userlogs/ directory."""




def _parse_task_stderr(lines):
    """Attempt to explain any error in task stderr, be it a Python
    exception or a problem with a setup command (see #1203).

    Currently this only works with tasks run with the setup wrapper script;
    it looks for '+ ' followed by a command line, and then the command's
    stderr.

    Either returns None or a task error dictionary with the following keys:

    message: a string (e.g. Python command line followed by Python traceback)
    start_line: where in lines message appears (0-indexed)
    num_lines: how may lines the message takes up
    """
    task_error = None

    for line_num, line in enumerate(lines):
        line = line.rstrip('\r\n')

        if line.startswith('+ '):
            task_error = dict(
                message=line,
                start_line=line_num)
        elif task_error:
            # explain what wrong!
            task_error['message'] += '\n' + line

    if task_error:
        task_error['num_lines'] = line_num + 1 - task_error['start_line']
        return task_error
    else:
        return None
