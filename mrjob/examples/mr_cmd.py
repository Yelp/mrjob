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
"""Write MapReduce jobs entirely on the command line. For example, to do a word
count::

    python -m mrjob.examples.mr_cmd -r local input.txt \
            --step mr "grep mrjob | sed -e 's/^/x	/'" "wc -l"

The ``sed`` command is to ensure that every line has the same key and therefore
goes to the same reducer. Otherwise, lines will be interpreted as having
unpredictable keys and you may not get a single number as output.

"""
from optparse import OptionValueError

from mrjob.job import MRJob
from mrjob.util import bash_wrap


class CmdJob(MRJob):

    def configure_options(self):
        super(CmdJob, self).configure_options()

        def register_substep(option, opt, value, parser):
            if (len(value) > 3 or
                not all(c in 'mcr' for c in value) or
                len(set(value)) != len(value)):
                raise OptionValueError(
                    "First argument to --step must be of the form m?c?r?")

            d = dict((c, parser.rargs.pop(0)) for c in value)

            parser.values.steps.append(d)

        self.add_passthrough_option(
            '-s', '--step', dest='steps', action='callback', type='str',
            callback=register_substep, default=[],
            help=(
                'First argument is any subset of the string "mcr", where each '
                ' letter represents a mapper, combiner, or reducer for the'
                ' step. Pass one additional argument for each specified'
                ' substep. Example: "-s mr [mapper cmd] [reducer cmd]".'))

    def steps(self):
        steps = []
        for step in self.options.steps:
            step_kwargs = {}
            if 'm' in step:
                step_kwargs['mapper_cmd'] = bash_wrap(step['m'])
            if 'c' in step:
                step_kwargs['combiner_cmd'] = bash_wrap(step['c'])
            if 'r' in step:
                step_kwargs['reducer_cmd'] = bash_wrap(step['r'])
            steps.append(self.mr(**step_kwargs))
        return steps


if __name__ == '__main__':
    CmdJob().run()
