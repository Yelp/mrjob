# Copyright 2012 Yelp and Contributors
# Copyright 2013 David Marin and Contributors
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
import logging

from mrjob.py2 import string_types
from mrjob.util import cmd_line


STEP_TYPES = ('streaming', 'jar')

# Function names mapping to mapper, reducer, and combiner operations
_MAPPER_FUNCS = ('mapper', 'mapper_init', 'mapper_final', 'mapper_cmd',
                 'mapper_pre_filter')
_COMBINER_FUNCS = ('combiner', 'combiner_init', 'combiner_final',
                   'combiner_cmd', 'combiner_pre_filter')
_REDUCER_FUNCS = ('reducer', 'reducer_init', 'reducer_final', 'reducer_cmd',
                  'reducer_pre_filter')
_HADOOP_OPTS = ('jobconf',)

# params to specify how to run the step. need at least one of these
_JOB_STEP_FUNC_PARAMS = _MAPPER_FUNCS + _COMBINER_FUNCS + _REDUCER_FUNCS
# all allowable MRStep params
_JOB_STEP_PARAMS = _JOB_STEP_FUNC_PARAMS + _HADOOP_OPTS

# all allowable JarStep constructor keyword args
_JAR_STEP_KWARGS = ['args', 'main_class']


log = logging.getLogger(__name__)


# used by MRStep below, to fake no mapper
def _IDENTITY_MAPPER(key, value):
    yield key, value


# used by MRStep below, to fake no reducer
def _IDENTITY_REDUCER(key, values):
    for value in values:
        yield key, value


class StepFailedException(Exception):
    """Exception to throw when a step fails.

    This will automatically be caught
    and converted to an error message by :py:meth:`mrjob.job.MRJob.run`, but
    you may wish to catch it if you
    :ref:`run your job programatically <runners-programmatically>`.
    """

    def __init__(
            self, reason=None, step_num=None, num_steps=None, step_desc=None):
        """Initialize a reason for step failure.

        :param string reason: brief explanation of which step failed
        :param int step_num: which step failed (0-indexed)
        :param int num_steps: number of steps in the job
        :param string step_desc: description of step (if we don't like the
                                 default "Step X of Y")

        *reason* should not be several lines long; use ``log.error(...)``
        for that.
        """
        self.reason = reason
        self.step_num = step_num
        self.num_steps = num_steps
        self.step_desc = step_desc

    def __str__(self):
        """Human-readable version of the exception. Note that this 1-indexes
        *step_num*."""
        return '%s failed%s' % (
            (self.step_desc or 'Step%s%s' % (
                '' if self.step_num is None else ' %d' % (self.step_num + 1),
                '' if (self.step_num is None or self.num_steps is None) else (
                    ' of %d' % self.num_steps))),
            '' if self.reason is None else ': %s' % self.reason)

    def __repr__(self):
        return '%s(%s)' % (
            self.__class__.__name__,
            ', '.join(('%s=%r' % (k, getattr(self, k))
                       for k in ('reason', 'step_num',
                                 'num_steps', 'step_desc')
                       if getattr(self, k) is not None)))


class MRStep(object):
    """Represents steps handled by the script containing your job.

    Used by :py:meth:`MRJob.steps <mrjob.job.MRJob.steps>`.
    See :ref:`writing-multi-step-jobs` for sample usage.

    Accepts the following keyword arguments:

    :param mapper: function with same function signature as
                   :py:meth:`mapper`, or ``None`` for an identity mapper.
    :param reducer: function with same function signature as
                    :py:meth:`reducer`, or ``None`` for no reducer.
    :param combiner: function with same function signature as
                     :py:meth:`combiner`, or ``None`` for no combiner.
    :param mapper_init: function with same function signature as
                        :py:meth:`mapper_init`, or ``None`` for no initial
                        mapper action.
    :param mapper_final: function with same function signature as
                         :py:meth:`mapper_final`, or ``None`` for no final
                         mapper action.
    :param reducer_init: function with same function signature as
                         :py:meth:`reducer_init`, or ``None`` for no
                         initial reducer action.
    :param reducer_final: function with same function signature as
                          :py:meth:`reducer_final`, or ``None`` for no
                          final reducer action.
    :param combiner_init: function with same function signature as
                          :py:meth:`combiner_init`, or ``None`` for no
                          initial combiner action.
    :param combiner_final: function with same function signature as
                           :py:meth:`combiner_final`, or ``None`` for no
                           final combiner action.
    :param jobconf: dictionary with custom jobconf arguments to pass to
                    hadoop.
    """
    def __init__(self, **kwargs):
        # limit which keyword args can be specified
        bad_kwargs = sorted(set(kwargs) - set(_JOB_STEP_PARAMS))
        if bad_kwargs:
            raise TypeError('MRStep() got an unexpected keyword argument %r' %
                            bad_kwargs[0])

        if not set(kwargs) & set(_JOB_STEP_FUNC_PARAMS):
            raise ValueError("Step has no mappers and no reducers")

        self.has_explicit_mapper = any(
            value for name, value in kwargs.items()
            if name in _MAPPER_FUNCS)

        self.has_explicit_combiner = any(
            value for name, value in kwargs.items()
            if name in _COMBINER_FUNCS)

        self.has_explicit_reducer = any(
            value for name, value in kwargs.items()
            if name in _REDUCER_FUNCS)

        steps = dict((f, None) for f in _JOB_STEP_PARAMS)

        steps.update(kwargs)

        def _prefix_set(prefix):
            return set(k for k in steps if k.startswith(prefix) and steps[k])

        def _check_cmd(cmd, prefix_set):
            if len(prefix_set) > 1 and cmd in prefix_set:
                prefix_set.remove(cmd)
                raise ValueError("Can't specify both %s and %s" % (
                    cmd, prefix_set))

        _check_cmd('mapper_cmd', _prefix_set('mapper'))
        _check_cmd('combiner_cmd', _prefix_set('combiner'))
        _check_cmd('reducer_cmd', _prefix_set('reducer'))

        self._steps = steps

    def __repr__(self):
        not_none = dict((k, v) for k, v in self._steps.items()
                        if v is not None)
        return '%s(%s)' % (
            self.__class__.__name__,
            ', '.join('%s=%r' % (k, v) for k, v in not_none.items()))

    def __eq__(self, other):
        return (isinstance(other, MRStep) and self._steps == other._steps)

    def __getitem__(self, key):
        # always be prepared to run a mapper, since Hadoop Streaming requires
        # it
        if key == 'mapper' and self._steps['mapper'] is None:
            return _IDENTITY_MAPPER
        # identity reducer should only show up if you specified 'reducer_init',
        # 'reducer_final', or 'reducer_pre_filter', but not 'reducer' itself
        if (key == 'reducer' and self._steps['reducer'] is None and
                self.has_explicit_reducer):
            return _IDENTITY_REDUCER
        # identity combiner should only show up if you specified
        # 'combiner_init', 'combiner_final', or 'combiner_pre_filter', but not
        # 'combiner' itself
        if (key == 'combiner' and self._steps['combiner'] is None and
                self.has_explicit_combiner):
            return _IDENTITY_REDUCER
        return self._steps[key]

    def _render_substep(self, cmd_key, pre_filter_key=None):
        if self._steps[cmd_key]:
            cmd = self._steps[cmd_key]
            if not isinstance(cmd, string_types):
                cmd = cmd_line(cmd)
            if (pre_filter_key and self._steps[pre_filter_key]):
                raise ValueError('Cannot specify both %s and %s' % (
                    cmd_key, pre_filter_key))
            return {'type': 'command', 'command': cmd}
        else:
            substep = {'type': 'script'}
            if (pre_filter_key and
                    self._steps[pre_filter_key]):
                substep['pre_filter'] = self._steps[pre_filter_key]
            return substep

    def render_mapper(self):
        return self._render_substep('mapper_cmd', 'mapper_pre_filter')

    def render_combiner(self):
        return self._render_substep('combiner_cmd', 'combiner_pre_filter')

    def render_reducer(self):
        return self._render_substep('reducer_cmd', 'reducer_pre_filter')

    def description(self, step_num):
        """Returns a dictionary representation of this step:

        .. code-block:: js

            {
                'type': 'streaming',
                'mapper': { ... },
                'combiner': { ... },
                'reducer': { ... },
                'jobconf': dictionary of Hadoop configuration properties
            }

        ``jobconf`` is optional, and only one of ``mapper``, ``combiner``,
        and ``reducer`` need be included.

        ``mapper``, ``combiner``, and ``reducer`` are either handled by
        the script containing your job definition:

        .. code-block:: js

           {
               'type': 'script',
               'pre_filter': (optional) cmd to pass input through, as a string
           }

        or they simply run a command:

        .. code-block:: js

            {
                'type': 'command',
                'command': command to run, as a string
            }

        See :ref:`steps-format` for examples.
        """
        substep_descs = {'type': 'streaming'}
        # Use a mapper if:
        #   - the user writes one
        #   - it is the first step and we don't want to mess up protocols
        #   - there are only combiners and we don't want to mess up protocols
        if (step_num == 0 or
                self.has_explicit_mapper or
                self.has_explicit_combiner):
            substep_descs['mapper'] = self.render_mapper()
        if self.has_explicit_combiner:
            substep_descs['combiner'] = self.render_combiner()
        if self.has_explicit_reducer:
            substep_descs['reducer'] = self.render_reducer()
        # TODO: verify this is a dict, convert booleans to strings
        if self._steps['jobconf']:
            substep_descs['jobconf'] = self._steps['jobconf']
        return substep_descs


class JarStep(object):
    """Represents a running a custom Jar as a step.

    Accepts the following keyword arguments:

    :param jar: The local path to the Jar. On EMR, this can also be an
                ``s3://`` URI, or ``file://`` to reference a jar on
                the local filesystem of your EMR instance(s).
    :param main_class: (optional) The main class to run from the jar. If
                       not specified, Hadoop will use the main class
                       in the jar's manifest file.
    :param args: (optional) A list of arguments to the jar

    *jar* can also be passed as a positional argument

    See :ref:`non-hadoop-streaming-jar-steps` for sample usage.
    """
    #: If this is passed as one of the step's arguments, it'll be replaced
    #: with the step's input paths (if there are multiple paths, they'll
    #: be joined with commas)
    INPUT = '<input>'
    #: If this is passed as one of the step's arguments, it'll be replaced
    #: with the step's output path
    OUTPUT = '<output>'

    def __init__(self, jar, **kwargs):
        bad_kwargs = sorted(set(kwargs) - set(_JAR_STEP_KWARGS))
        if bad_kwargs:
            raise TypeError('JarStep() got an unexpected keyword argument %r' %
                            bad_kwargs[0])

        self.jar = jar

        self.args = kwargs.get('args') or []
        self.main_class = kwargs.get('main_class')

    def __repr__(self):
        repr_args = []
        repr_args.append(repr(self.jar))
        if self.args:
            repr_args.append('args=' + repr(self.args))
        if self.main_class:
            repr_args.append('main_class=' + repr(self.main_class))

        return 'JarStep(%s)' % ', '.join(repr_args)

    def __eq__(self, other):
        return (isinstance(other, JarStep) and
                all(getattr(self, key) == getattr(other, key)
                    for key in ('jar', 'args', 'main_class')))

    def description(self, step_num):
        """Returns a dictionary representation of this step:

        .. code-block:: js

            {
                'type': 'jar',
                'jar': path of the jar,
                'main_class': string, name of the main class,
                'args': list of strings, args to the main class,
            }

        See :ref:`steps-format` for examples.
        """
        return {
            'type': 'jar',
            'args': self.args,
            'jar': self.jar,
            'main_class': self.main_class,
        }
