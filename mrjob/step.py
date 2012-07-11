# Copyright 2009-2012 Yelp and Contributors
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


_MAPPER_FUNCS = ('mapper', 'mapper_init', 'mapper_final', 'mapper_cmd',
                 'mapper_filter')
_COMBINER_FUNCS = ('combiner', 'combiner_init', 'combiner_final',
                   'combiner_cmd', 'combiner_filter')
_REDUCER_FUNCS = ('reducer', 'reducer_init', 'reducer_final', 'reducer_cmd',
                  'reducer_filter')

_JOB_STEP_PARAMS = _MAPPER_FUNCS + _COMBINER_FUNCS + _REDUCER_FUNCS

STEP_TYPES = ('script', 'streaming', 'jar')

log = logging.getLogger('mrjob.step')

# used by MRJobStep below, to fake no mapper
def _IDENTITY_MAPPER(key, value):
    yield key, value


class MRJobStep(object):

    def __init__(self, mapper=None, reducer=None, **kwargs):
        # limit which keyword args can be specified
        bad_kwargs = sorted(set(kwargs) - set(_JOB_STEP_PARAMS))
        if bad_kwargs:
            raise TypeError(
                'mr() got an unexpected keyword argument %r' % bad_kwargs[0])

        if not (any(kwargs.itervalues()) or mapper or reducer):
            raise Exception("Step has no mappers and no reducers")

        self.has_explicit_mapper = any(
            name for name in kwargs if name in _MAPPER_FUNCS)

        steps = dict((f, None) for f in _JOB_STEP_PARAMS)

        if mapper:
            steps['mapper'] = mapper
            self.has_explicit_mapper = True

        if reducer:
            steps['reducer'] = reducer

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

    def __getitem__(self, key):
        # easy access to functions stored in self._steps
        if key == 'mapper' and self._steps['mapper'] is None:
            return _IDENTITY_MAPPER
        return self._steps[key]

    @property
    def has_explicit_combiner(self):
        return any(name in self._steps for name in _COMBINER_FUNCS)

    @property
    def has_explicit_reducer(self):
        return any(name in self._steps for name in _REDUCER_FUNCS)

    def _render_substep(self, cmd_key, filter_key):
        if self._steps[cmd_key]:
            cmd = self._steps[cmd_key]
            if not isinstance(cmd, basestring):
                cmd = cmd_line(cmd)
            return {'type': 'streaming', 'command': cmd}
        else:
            substep = {'type': 'script'}
            if filter_key in self._steps:
                substep['filter'] = self._steps[filter_key]
            return substep

    def render_mapper(self):
        return self._render_substep('mapper_cmd', 'mapper_filter')

    def render_combiner(self):
        return self._render_substep('combiner_cmd', 'combiner_filter')

    def render_reducer(self):
        return self._render_substep('reducer_cmd', 'reducer_filter')

    def description(self, step_num):
        substep_descs = {'type': 'streaming'}
        # Use a mapper if:
        #   - the user writes one
        #   - it is the first step and we don't want to mess up protocols
        #   - there are only combiners
        if (step_num == 0 or
            self.has_explicit_mapper or
            self.has_explicit_reducer):
            substep_descs['mapper'] = self.render_mapper()
        if self.has_explicit_combiner:
            substep_descs['combiner'] = self.render_combiner()
        if self.has_explicit_reducer:
            substep_descs['reducer'] = self.render_reducer()
        return substep_descs


class JarStep(object):

    def __init__(self, name, jar, main_class=None, step_args=None):
        self.name = name
        self.jar = jar
        self.main_class = main_class
        self.step_args = step_args

    def description(self, step_num):
        return {
            'type': 'jar',
            'name': self.name,
            'jar': self.jar,
            'main_class': self.main_class,
            'step_args': self.step_args,
        }
