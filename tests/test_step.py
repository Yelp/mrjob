# Copyright 2012 Yelp and Contributors
# Copyright 2013 David Marin
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
"""Tests for mrjob.step"""
from mrjob.step import _IDENTITY_MAPPER
from mrjob.step import JarStep
from mrjob.step import MRStep
from mrjob.step import StepFailedException

from tests.py2 import TestCase


# functions we don't really care about the values of

def identity_mapper(k=None, v=None):
    yield k, v


def identity_reducer(k, vals):
    for v in vals:
        yield k, v


class StepFailedExceptionTestCase(TestCase):

    def test_empty(self):
        ex = StepFailedException()
        self.assertEqual(str(ex), 'Step failed')
        self.assertEqual(repr(ex), 'StepFailedException()')

    def test_reason(self):
        ex = StepFailedException('Hadoop is feeling sad today')
        self.assertEqual(str(ex), 'Step failed: Hadoop is feeling sad today')
        self.assertEqual(
            repr(ex),
            "StepFailedException(reason='Hadoop is feeling sad today')")

    def test_step_num(self):
        ex = StepFailedException(step_num=0)
        self.assertEqual(str(ex), 'Step 1 failed')
        self.assertEqual(repr(ex), 'StepFailedException(step_num=0)')

    def test_step_num_with_num_steps(self):
        ex = StepFailedException(step_num=0, num_steps=4)
        self.assertEqual(str(ex), 'Step 1 of 4 failed')
        self.assertEqual(repr(ex),
                         'StepFailedException(step_num=0, num_steps=4)')

    def test_num_steps_with_no_step_num(self):
        ex = StepFailedException(num_steps=4)
        self.assertEqual(str(ex), 'Step failed')
        self.assertEqual(repr(ex), 'StepFailedException(num_steps=4)')

    def test_step_desc(self):
        ex = StepFailedException(step_desc='Master node setup step')
        self.assertEqual(str(ex), 'Master node setup step failed')
        self.assertEqual(
            repr(ex),
            "StepFailedException(step_desc='Master node setup step')")


class JarStepTestCase(TestCase):

    def test_empty(self):
        self.assertRaises(TypeError, JarStep)

    def test_all(self):
        kwargs = {
            'jar': 'binks.jar.jar',
            'main_class': 'MyMainMan',
            'args': ['argh', 'argh'],
        }
        expected = kwargs.copy()
        expected['type'] = 'jar'
        self.assertEqual(JarStep(**kwargs).description(0), expected)

    def test_some(self):
        kwargs = {
            'jar': 'binks.jar.jar',
        }
        expected = kwargs.copy()
        expected.update({
            'type': 'jar',
            'main_class': None,
            'args': [],
        })
        self.assertEqual(JarStep(**kwargs).description(0), expected)


class MRStepInitTestCase(TestCase):

    ### Basic behavior ###

    def test_nothing_specified(self):
        self.assertRaises(ValueError, MRStep)

    def test_only_hadoop_opts(self):
        self.assertRaises(ValueError, MRStep,
                          jobconf={'dfs.block.size': '134217728'})

    def _test_explicit(self, m=False, c=False, r=False, **kwargs):
        s = MRStep(**kwargs)
        self.assertEqual(s.has_explicit_mapper, m)
        self.assertEqual(s.has_explicit_combiner, c)
        self.assertEqual(s.has_explicit_reducer, r)

    # normal

    def test_explicit_mapper(self):
        self._test_explicit(mapper=identity_mapper, m=True)

    def test_explicit_combiner(self):
        self._test_explicit(combiner=identity_reducer, c=True)

    def test_explicit_reducer(self):
        self._test_explicit(reducer=identity_reducer, r=True)

    def test_no_explicit_mapper(self):
        self._test_explicit(mapper=None, m=False)

    def test_no_explicit_combiner(self):
        self._test_explicit(combiner=None, c=False)

    def test_no_explicit_reducer(self):
        self._test_explicit(reducer=None, r=False)

    # final

    def test_explicit_mapper_final(self):
        self._test_explicit(mapper_final=identity_mapper, m=True)

    def test_explicit_combiner_final(self):
        self._test_explicit(combiner_final=identity_reducer, c=True)

    def test_explicit_reducer_final(self):
        self._test_explicit(reducer_final=identity_reducer, r=True)

    def test_no_explicit_mapper_final(self):
        self._test_explicit(mapper_final=None, m=False)

    def test_no_explicit_combiner_final(self):
        self._test_explicit(combiner_final=None, c=False)

    def test_no_explicit_reducer_final(self):
        self._test_explicit(reducer_final=None, r=False)

    # init

    def test_explicit_mapper_init(self):
        self._test_explicit(mapper_init=identity_mapper, m=True)

    def test_explicit_combiner_init(self):
        self._test_explicit(combiner_init=identity_reducer, c=True)

    def test_explicit_reducer_init(self):
        self._test_explicit(reducer_init=identity_reducer, r=True)

    def test_no_explicit_mapper_init(self):
        self._test_explicit(mapper_init=None, m=False)

    def test_no_explicit_combiner_init(self):
        self._test_explicit(combiner_init=None, c=False)

    def test_no_explicit_reducer_init(self):
        self._test_explicit(reducer_init=None, r=False)

    # cmd

    def test_explicit_mapper_cmd(self):
        self._test_explicit(mapper_cmd='cat', m=True)

    def test_explicit_combiner_cmd(self):
        self._test_explicit(combiner_cmd='cat', c=True)

    def test_explicit_reducer_cmd(self):
        self._test_explicit(reducer_cmd='cat', r=True)

    # pre-filter

    def test_explicit_mapper_pre_filter(self):
        self._test_explicit(mapper_pre_filter='cat', m=True)

    def test_explicit_combiner_pre_filter(self):
        self._test_explicit(combiner_pre_filter='cat', c=True)

    def test_explicit_reducer_pre_filter(self):
        self._test_explicit(reducer_pre_filter='cat', r=True)

    ### Conflicts ###

    def _test_conflict(self, **kwargs):
        self.assertRaises(ValueError, MRStep, **kwargs)

    def test_conflict_mapper(self):
        self._test_conflict(mapper_cmd='cat', mapper=identity_mapper)

    def test_conflict_combiner(self):
        self._test_conflict(combiner_cmd='cat', combiner=identity_reducer)

    def test_conflict_reducer(self):
        self._test_conflict(reducer_cmd='cat', reducer=identity_reducer)


class MRStepGetItemTestCase(TestCase):

    def test_get_identity_mapper(self):
        # this is the weird behavior
        self.assertEqual(MRStep(mapper_final=identity_mapper)['mapper'],
                         _IDENTITY_MAPPER)

    def test_get_regular_mapper(self):
        # this is the normal behavior
        self.assertEqual(MRStep(mapper=identity_mapper)['mapper'],
                         identity_mapper)


class MRStepDescriptionTestCase(TestCase):

    def test_render_mapper(self):
        self.assertEqual(
            MRStep(mapper=identity_mapper).description(0),
            {
                'type': 'streaming',
                'mapper': {
                    'type': 'script',
                },
            }
        )

    def test_render_reducer_first_mapper_implied(self):
        self.assertEqual(
            MRStep(reducer=identity_reducer).description(0),
            {
                'type': 'streaming',
                'mapper': {
                    'type': 'script',
                },
                'reducer': {
                    'type': 'script',
                },
            })

    def test_render_reducer_first_mapper_not_implied(self):
        self.assertEqual(MRStep(
            reducer=identity_reducer).description(1),
            {
                'type': 'streaming',
                'reducer': {
                    'type': 'script',
                },
            })

    def test_render_combiner(self):
        self.assertEqual(
            MRStep(combiner=identity_reducer).description(1),
            {
                'type': 'streaming',
                'mapper': {
                    'type': 'script',
                },
                'combiner': {
                    'type': 'script',
                },
            })

    def test_render_mapper_pre_filter(self):
        self.assertEqual(
            MRStep(
                mapper=identity_mapper,
                mapper_pre_filter='cat').description(0),
            {
                'type': 'streaming',
                'mapper': {
                    'type': 'script',
                    'pre_filter': 'cat',
                },
            }
        )

    def test_render_reducer_pre_filter(self):
        self.assertEqual(
            MRStep(
                reducer=identity_reducer,
                reducer_pre_filter='cat').description(1),
            {
                'type': 'streaming',
                'reducer': {
                    'type': 'script',
                    'pre_filter': 'cat',
                },
            })

    def test_render_mapper_cmd(self):
        self.assertEqual(
            MRStep(mapper_cmd='cat').description(0),
            {
                'type': 'streaming',
                'mapper': {
                    'type': 'command',
                    'command': 'cat',
                },
            }
        )

    def test_render_reducer_cmd_first_mapper_implied(self):
        self.assertEqual(
            MRStep(reducer_cmd='cat').description(0),
            {
                'type': 'streaming',
                'mapper': {
                    'type': 'script',
                },
                'reducer': {
                    'type': 'command',
                    'command': 'cat',
                },
            })

    def test_render_reducer_cmd_first_mapper_not_implied(self):
        self.assertEqual(
            MRStep(reducer_cmd='cat').description(1),
            {
                'type': 'streaming',
                'reducer': {
                    'type': 'command',
                    'command': 'cat',
                },
            })

    def test_render_combiner_cmd(self):
        self.assertEqual(
            MRStep(combiner_cmd='cat').description(1),
            {
                'type': 'streaming',
                'mapper': {
                    'type': 'script',
                },
                'combiner': {
                    'type': 'command',
                    'command': 'cat',
                },
            })

    def test_render_jobconf(self):
        step = MRStep(mapper=identity_mapper,
                      jobconf={'dfs.block.size': '134217728'})

        self.assertEqual(
            step.description(0),
            {
                'type': 'streaming',
                'mapper': {
                    'type': 'script',
                },
                'jobconf': {
                    'dfs.block.size': '134217728',
                }
            }
        )
