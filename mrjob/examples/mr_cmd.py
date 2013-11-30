# Copyright 2012 Yelp
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
"""Write MapReduce jobs entirely on the command line. For example, to do a word
count::

    python -m mrjob.examples.mr_cmd -r local input.txt \
            --step mr "grep mrjob | sed -e 's/^/x	/'" "wc -l"

The ``sed`` command is to ensure that every line has the same key and therefore
goes to the same reducer. Otherwise, lines will be interpreted as having
unpredictable keys and you may not get a single number as output.

"""
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.util import bash_wrap


class MRCmdJob(MRJob):

    def configure_options(self):
        super(MRCmdJob, self).configure_options()

        def register_substep(option, opt, value, parser, mrc):
            def last_step_has(*args):
                return any(arg in parser.values.steps[-1] for arg in args)
            # always add a new step if no steps
            # or new substep is a mapper
            # or new substep is a combiner and we already have a combiner or
            #   reducer
            # or new substep is a reducer and we already have a reducer
            if (len(parser.values.steps) == 0 or
                mrc == 'mapper' or
                (mrc == 'combiner' and last_step_has('combiner', 'reducer')) or
                (mrc == 'reducer' and last_step_has('reducer'))):
                parser.values.steps.append({})

            parser.values.steps[-1][mrc] = value

        self.add_passthrough_option(
            '-M', dest='steps', action='callback', type='str',
            callback=register_substep, default=[], callback_args=('mapper',),
            help='Define a map step')

        self.add_passthrough_option(
            '-C', dest='steps', action='callback', type='str',
            callback=register_substep, callback_args=('combiner',),
            help='Define a combine step')

        self.add_passthrough_option(
            '-R', dest='steps', action='callback', type='str',
            callback=register_substep, callback_args=('reducer',),
            help='Define a reduce step')

    def steps(self):
        steps = []
        for step in self.options.steps:
            step_kwargs = {}
            if 'mapper' in step:
                step_kwargs['mapper_cmd'] = bash_wrap(step['mapper'])
            if 'combiner' in step:
                step_kwargs['combiner_cmd'] = bash_wrap(step['combiner'])
            if 'reducer' in step:
                step_kwargs['reducer_cmd'] = bash_wrap(step['reducer'])
            steps.append(MRStep(**step_kwargs))
        return steps


if __name__ == '__main__':
    MRCmdJob().run()
