# Copyright 2012 Yelp and Contributors
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

from mrjob.util import cmd_line


# Constants for --steps data structure, not to be used for anything else (like
# function names, hence the explicit 'mapper', etc. in _MAPPER_FUNCS)
MAPPER = 'mapper'

COMBINER = 'combiner'

REDUCER = 'reducer'

STREAMING_STEP = 'streaming'

JAR_STEP = 'jar'

STEP_TYPES = (STREAMING_STEP, JAR_STEP)

SCRIPT_SUBSTEP = 'script'

COMMAND_SUBSTEP = 'command'

# Function names mapping to mapper, reducer, and combiner operations
_MAPPER_FUNCS = ('mapper', 'mapper_init', 'mapper_final', 'mapper_cmd',
                 'mapper_filter')
_COMBINER_FUNCS = ('combiner', 'combiner_init', 'combiner_final',
                   'combiner_cmd')
_REDUCER_FUNCS = ('reducer', 'reducer_init', 'reducer_final', 'reducer_cmd',
                  'reducer_filter')

# All allowable step keyword arguments, also happens to be all function names
# for jobs whose step objects are created automatically
_JOB_STEP_PARAMS = _MAPPER_FUNCS + _COMBINER_FUNCS + _REDUCER_FUNCS

log = logging.getLogger('mrjob.step')

# used by MRJobStep below, to fake no mapper
def _IDENTITY_MAPPER(key, value):
    yield key, value

# used by MRJobStep below, to fake no reducer
def _IDENTITY_REDUCER(key, values):
    for value in values:
        yield key, value


class MRJobStep(object):

    def __init__(self, mapper=None, reducer=None, **kwargs):
        # limit which keyword args can be specified
        bad_kwargs = sorted(set(kwargs) - set(_JOB_STEP_PARAMS))
        if bad_kwargs:
            raise TypeError(
                'mr() got an unexpected keyword argument %r' % bad_kwargs[0])

        if not (any(kwargs.itervalues()) or mapper or reducer):
            raise ValueError("Step has no mappers and no reducers")

        self.has_explicit_mapper = any(
            name for name in kwargs if name in _MAPPER_FUNCS)

        self.has_explicit_combiner = any(
            name for name in kwargs if name in _COMBINER_FUNCS)

        self.has_explicit_reducer = any(
            name for name in kwargs if name in _REDUCER_FUNCS)

        steps = dict((f, None) for f in _JOB_STEP_PARAMS)

        if mapper:
            steps['mapper'] = mapper
            self.has_explicit_mapper = True

        if reducer:
            steps['reducer'] = reducer
            self.has_explicit_reducer = True

        steps.update(kwargs)

        def _prefix_set(prefix):
            return set(k for k in steps if k.startswith(prefix) and steps[k])

        def _check_cmd(cmd, prefix_set):
            if len(prefix_set) > 1 and cmd in prefix_set:
                raise ValueError("Can't specify both %s and %s" % (
                    cmd, prefix_set))

        _check_cmd('mapper_cmd', _prefix_set('mapper'))
        _check_cmd('combiner_cmd', _prefix_set('combiner'))
        _check_cmd('reducer_cmd', _prefix_set('reducer'))

        self._steps = steps

    def __eq__(self, other):
        return (isinstance(other, MRJobStep) and self._steps == other._steps)

    def __getitem__(self, key):
        # always be prepared to run a mapper, since Hadoop Streaming requires
        # it
        if key == 'mapper' and self._steps['mapper'] is None:
            return _IDENTITY_MAPPER
        # identity reducer should only show up if you specified 'reducer_init',
        # 'reducer_final', or 'reducer_filter', but not 'reducer' itself
        if (key == 'reducer' and self._steps['reducer'] is None and
            self.has_explicit_reducer):
            return _IDENTITY_REDUCER
        return self._steps[key]

    def _render_substep(self, cmd_key, filter_key=None):
        if self._steps[cmd_key]:
            cmd = self._steps[cmd_key]
            if not isinstance(cmd, basestring):
                cmd = cmd_line(cmd)
            if (filter_key and self._steps[filter_key]):
                raise ValueError('Cannot specify both %s and %s' % (
                    cmd_key, filter_key))
            return {'type': COMMAND_SUBSTEP, 'command': cmd}
        else:
            substep = {'type': SCRIPT_SUBSTEP}
            if (filter_key and
                self._steps[filter_key]):
                substep['filter'] = self._steps[filter_key]
            return substep

    def render_mapper(self):
        return self._render_substep('mapper_cmd', 'mapper_filter')

    def render_combiner(self):
        # combiners can't have filters
        return self._render_substep('combiner_cmd')

    def render_reducer(self):
        return self._render_substep('reducer_cmd', 'reducer_filter')

    def description(self, step_num):
        substep_descs = {'type': STREAMING_STEP}
        # Use a mapper if:
        #   - the user writes one
        #   - it is the first step and we don't want to mess up protocols
        #   - there are only combiners and we don't want to mess up protocols
        if (step_num == 0 or
            self.has_explicit_mapper or
            self.has_explicit_combiner):
            substep_descs[MAPPER] = self.render_mapper()
        if self.has_explicit_combiner:
            substep_descs[COMBINER] = self.render_combiner()
        if self.has_explicit_reducer:
            substep_descs[REDUCER] = self.render_reducer()
        return substep_descs


class JarStep(object):

    def __init__(self, name, jar, main_class=None, step_args=None):
        self.name = name
        self.jar = jar
        self.main_class = main_class
        self.step_args = step_args

    def __eq__(self, other):
        return (isinstance(other, JarStep) and
                self.name == other.name and
                self.jar == other.jar and
                self.main_class == other.main_class and
                self.step_args == other.step_args)

    def description(self, step_num):
        return {
            'type': 'jar',
            'name': self.name,
            'jar': self.jar,
            'main_class': self.main_class,
            'step_args': self.step_args,
        }
