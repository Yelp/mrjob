try:
    from unittest2 import TestCase
    TestCase  # silency pyflakes
except ImportError:
    from unittest import TestCase

from mrjob.step import _IDENTITY_MAPPER
from mrjob.step import JarStep
from mrjob.step import MRJobStep


# functions we don't really care about the values of


def identity_mapper(k=None, v=None):
    yield k, v


def identity_reducer(k, vals):
    for v in vals:
        yield k, v


class JarStepTestCase(TestCase):

    def test_all(self):
        kwargs = {
            'name': 'step_name',
            'jar': 'binks.jar.jar',
            'main_class': 'MyMainMan',
            'step_args': ['argh', 'argh'],
        }
        expected = kwargs.copy()
        expected['type'] = 'jar'
        self.assertEqual(JarStep(**kwargs).description(0), expected)

    def test_some(self):
        kwargs = {
            'name': 'step_name',
            'jar': 'binks.jar.jar',
        }
        expected = kwargs.copy()
        expected.update({
            'type': 'jar',
            'main_class': None,
            'step_args': None,
        })
        self.assertEqual(JarStep(**kwargs).description(0), expected)


class MRJobStepInitTestCase(TestCase):

    ### Basic behavior ###

    def test_nothing_specified(self):
        self.assertRaises(ValueError, MRJobStep)

    def _test_explicit(self, m=False, c=False, r=False, **kwargs):
        s = MRJobStep(**kwargs)
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

    # final

    def test_explicit_mapper_final(self):
        self._test_explicit(mapper_final=identity_mapper, m=True)

    def test_explicit_combiner_final(self):
        self._test_explicit(combiner_final=identity_reducer, c=True)

    def test_explicit_reducer_final(self):
        self._test_explicit(reducer_final=identity_reducer, r=True)

    # init

    def test_explicit_mapper_init(self):
        self._test_explicit(mapper_init=identity_mapper, m=True)

    def test_explicit_combiner_init(self):
        self._test_explicit(combiner_init=identity_reducer, c=True)

    def test_explicit_reducer_init(self):
        self._test_explicit(reducer_init=identity_reducer, r=True)

    # cmd

    def test_explicit_mapper_cmd(self):
        self._test_explicit(mapper_cmd='cat', m=True)

    def test_explicit_combiner_cmd(self):
        self._test_explicit(combiner_cmd='cat', c=True)

    def test_explicit_reducer_cmd(self):
        self._test_explicit(reducer_cmd='cat', r=True)

    # filter

    def test_explicit_mapper_filter(self):
        self._test_explicit(mapper_filter='cat', m=True)

    def test_explicit_combiner_filter(self):
        # combiners aren't currently allowed to have filters
        self.assertRaises(TypeError, MRJobStep, combiner_filter='cat')

    def test_explicit_reducer_filter(self):
        self._test_explicit(reducer_filter='cat', r=True)

    ### Conflicts ###

    def _test_conflict(self, **kwargs):
        self.assertRaises(ValueError, MRJobStep, **kwargs)

    def test_conflict_mapper(self):
        self._test_conflict(mapper_cmd='cat', mapper=identity_mapper)

    def test_conflict_combiner(self):
        self._test_conflict(combiner_cmd='cat', combiner=identity_reducer)

    def test_conflict_reducer(self):
        self._test_conflict(reducer_cmd='cat', reducer=identity_reducer)


class MRJobStepGetItemTestCase(TestCase):

    def test_get_identity_mapper(self):
        # this is the weird behavior
        self.assertEqual(MRJobStep(mapper_final=identity_mapper)['mapper'],
                         _IDENTITY_MAPPER)

    def test_get_regular_mapper(self):
        # this is the normal behavior
        self.assertEqual(MRJobStep(mapper=identity_mapper)['mapper'],
                         identity_mapper)


class MRJobStepDescriptionTestCase(TestCase):

    def test_render_mapper(self):
        self.assertEqual(
            MRJobStep(mapper=identity_mapper).description(0),
            {
            'type': 'streaming',
            'mapper': {
                'type': 'script',
            },
        })

    def test_render_reducer_first_mapper_implied(self):
        self.assertEqual(
            MRJobStep(reducer=identity_reducer).description(0),
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
        self.assertEqual(MRJobStep(
            reducer=identity_reducer).description(1),
            {
                'type': 'streaming',
                'reducer': {
                    'type': 'script',
                },
            })

    def test_render_combiner_mapper_not_implied(self):
        self.assertEqual(
            MRJobStep(combiner=identity_reducer).description(1),
            {
                'type': 'streaming',
                'mapper': {
                    'type': 'script',
                },
                'combiner': {
                    'type': 'script',
                },
            })

    def test_render_mapper_filter(self):
        self.assertEqual(
            MRJobStep(
                mapper=identity_mapper, mapper_filter='cat').description(0),
            {
            'type': 'streaming',
            'mapper': {
                'type': 'script',
                'filter': 'cat',
            },
        })

    def test_render_reducer_filter(self):
        self.assertEqual(
            MRJobStep(
                reducer=identity_reducer, reducer_filter='cat').description(1),
            {
                'type': 'streaming',
                'reducer': {
                    'type': 'script',
                    'filter': 'cat',
                },
            })
