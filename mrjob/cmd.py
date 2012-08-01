# Copyright 2012 Yelp
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

from __future__ import with_statement

from sys import argv
from sys import stderr


commands = {}

descriptions = {}


def command(name, description):
    def decorator(f):
        commands[name] = f
        descriptions[name] = description
        return f
    return decorator


def main():
    if not argv[1:] or argv[1] not in commands:
        print >> stderr, (
            "usage: mrjob %s [args]" % '|'.join(sorted(commands)))
    else:
        commands[argv[1]].run(argv[2:])


@command('run', 'Run a job')
def run(args):
    from mrjob.launch import MRJobLauncher
    MRJobLauncher.run(args=args)
